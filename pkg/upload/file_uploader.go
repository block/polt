package upload

import (
	"bytes"
	"context"
	"os"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	f "github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

type FileUploader struct {
	filePath string
}

func NewFileUploader(filePath string) *FileUploader {
	return &FileUploader{
		filePath: filePath,
	}
}

func (u *FileUploader) Upload(ctx context.Context, prefix string, data []byte, schema *arrow.Schema) error {
	writerProps := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Snappy))
	// Create a file to write the Parquet data
	file, err := os.Create(prefix + u.filePath)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			// Log error but don't override the main error
			_ = closeErr
		}
	}()

	reader, err := f.NewParquetReader(bytes.NewReader(data))
	if err != nil {
		return err
	}
	writer, err := pqarrow.NewFileWriter(schema, file, writerProps, pqarrow.DefaultWriterProps())
	if err != nil {
		return err
	}

	fileReader, err := pqarrow.NewFileReader(reader, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)

	if err != nil {
		return err
	}
	rr, err := fileReader.GetRecordReader(ctx, nil, nil)
	if err != nil {
		return err
	}
	for rr.Next() {
		record := rr.Record()
		// Strip metadata from the record before writing it as reader.GetRecordReader() returns a record with metadata
		newRecord := stripMetadata(record)
		if err := writer.WriteBuffered(newRecord); err != nil {
			return err
		}
		record.Release()
		newRecord.Release()
	}
	if closeErr := writer.Close(); closeErr != nil {
		return closeErr
	}

	return err
}

func stripMetadata(record arrow.Record) arrow.Record {
	fields := make([]arrow.Field, len(record.Schema().Fields()))
	for i, field := range record.Schema().Fields() {
		fields[i] = arrow.Field{Name: field.Name, Type: field.Type}
	}
	newSchema := arrow.NewSchema(fields, nil)

	// Create a new record with the new schema and the same data
	newRecord := array.NewRecord(newSchema, record.Columns(), record.NumRows())

	return newRecord
}
