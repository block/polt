package parquet

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/block/polt/pkg/random"
	"github.com/block/polt/pkg/upload"
	"github.com/cashapp/spirit/pkg/table"
)

const (
	defaultFileSize = 1 * 1024 * 1024 // 1MB
	// TODO: Due to parquet format and SNAPPY compression, the actual size of the written data can be smaller than the buffer size
	// so we need to adjust the compression factor to be able to flush the buffer before it reaches the outputFileSize.
	// Currently, we are using the default compression factor of 1, but we can adjust it based on what we see in the real data.
	compressionFactor = 1
)

type WriteBuffer struct {
	wm     sync.Mutex // protects the writer
	writer *pqarrow.FileWriter

	rbm           sync.Mutex // protects the recordBuilder
	recordBuilder *array.RecordBuilder

	schema *arrow.Schema
	buffer *bytes.Buffer

	bufferedChunks []*BufferedChunk
	bcm            sync.Mutex // protects the bufferedChunks

	chunker              table.Chunker
	uploader             upload.Uploader
	outputFileSize       uint64
	RowsWritten          uint64
	currentEstBufferSize atomic.Int64
}

type BufferedChunk struct {
	Chunk    *table.Chunk
	Duration time.Duration
}

func NewWriteBuffer(schema *arrow.Schema, chunker table.Chunker, uploader upload.Uploader, outputFileSize uint64) *WriteBuffer {
	pool := memory.NewGoAllocator()
	recordBuilder := array.NewRecordBuilder(pool, schema)
	buffer := bytes.NewBuffer(nil)
	props := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Snappy))
	writer, _ := pqarrow.NewFileWriter(schema, buffer, props, pqarrow.DefaultWriterProps())

	if outputFileSize == 0 {
		outputFileSize = defaultFileSize
	}

	return &WriteBuffer{
		schema:         schema,
		recordBuilder:  recordBuilder,
		buffer:         buffer,
		writer:         writer,
		chunker:        chunker,
		uploader:       uploader,
		outputFileSize: outputFileSize,
	}
}

func (wb *WriteBuffer) writeRecord(record arrow.Record) error {
	wb.wm.Lock()
	defer wb.wm.Unlock()
	wb.RowsWritten++
	// We use `WriteBuffered` to write the record on the writer instead of `Write` to
	// make sure all the records for the current batch are added to same row group.
	if err := wb.writer.WriteBuffered(record); err != nil {
		return err
	}

	return nil
}

func (wb *WriteBuffer) reset() {
	wb.buffer.Reset()
	wb.RowsWritten = 0
	wb.currentEstBufferSize.Store(0)
	props := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Snappy))
	wb.writer, _ = pqarrow.NewFileWriter(wb.schema, wb.buffer, props, pqarrow.DefaultWriterProps())
	wb.resetChunks()
}

func (wb *WriteBuffer) Flush(ctx context.Context) (uint64, error) {
	wb.wm.Lock()
	defer wb.wm.Unlock()
	if err := wb.writer.Close(); err != nil {
		return 0, err
	}
	// Upload the buffer via the uploader
	data := wb.buffer.Bytes()
	if err := wb.uploader.Upload(ctx, random.ID(), data, wb.schema); err != nil {
		return 0, err
	}
	wb.feedbackChunks()
	rowsWritten := wb.RowsWritten
	// reset the buffer, writer and bufferedChunks, rowsWritten
	wb.reset()

	return rowsWritten, nil
}

func (wb *WriteBuffer) AddChunk(chunk *table.Chunk, duration time.Duration) {
	wb.bcm.Lock()
	defer wb.bcm.Unlock()
	wb.bufferedChunks = append(wb.bufferedChunks, &BufferedChunk{Chunk: chunk, Duration: duration})
}

func (wb *WriteBuffer) feedbackChunks() {
	wb.bcm.Lock()
	defer wb.bcm.Unlock()
	for _, pChunk := range wb.bufferedChunks {
		wb.chunker.Feedback(pChunk.Chunk, pChunk.Duration)
	}
}

func (wb *WriteBuffer) resetChunks() {
	wb.bcm.Lock()
	defer wb.bcm.Unlock()
	wb.bufferedChunks = nil
}

func (wb *WriteBuffer) CheckAndFlushBuffer(ctx context.Context) (uint64, error) {
	if uint64(wb.currentEstBufferSize.Load()/compressionFactor) >= wb.outputFileSize {
		return wb.Flush(ctx)
	}

	return 0, nil
}

func (wb *WriteBuffer) WriteGenericRowValues(values []interface{}) error { //nolint:cyclop
	wb.rbm.Lock()
	defer wb.rbm.Unlock()
	var err error
	for i, field := range wb.recordBuilder.Fields() {
		switch b := field.(type) {
		case *array.BinaryBuilder:
			byteVal, ok := values[i].([]byte)
			if !ok {
				return errors.New("cannot convert to []byte")
			}
			b.Append(byteVal)
		case *array.StringBuilder:
			strVal, err2 := strValue(values[i])
			if err2 != nil {
				return err2
			}
			if err = b.AppendValueFromString(strVal); err != nil {
				return err
			}
		case *array.Int32Builder:
			intValue, err := int32Val(values[i])
			if err != nil {
				return err
			}
			b.Append(int32(intValue))
		case *array.Int64Builder:
			intValue, err := int64Val(values[i])
			if err != nil {
				return err
			}
			b.Append(intValue)
		case *array.Float64Builder:
			floatValue, err := float64Val(values[i])
			if err != nil {
				return err
			}
			b.Append(floatValue)

		case *array.TimestampBuilder:
			timeS, err := arrow.TimestampFromString(string(values[i].([]uint8)), arrow.Microsecond)
			if err != nil {
				return err
			}
			b.Append(timeS)
		}
	}
	err2 := wb.addRecordToBuffer()
	if err2 != nil {
		return err2
	}

	return nil
}

func float64Val(i interface{}) (float64, error) {
	switch v := i.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	default:
		return 0, fmt.Errorf("cannot convert %v to float64", i)
	}
}

func strValue(value interface{}) (string, error) {
	var strVal []uint8
	var str string
	var ok bool
	strVal, ok = value.([]uint8)
	if !ok {
		str, ok = value.(string)
		if !ok {
			return "", fmt.Errorf("%v is not a string", value)
		}

		return str, nil
	}

	return string(strVal), nil
}

func estimateSize(record arrow.Record) int64 {
	var totalSize int64
	for _, col := range record.Columns() {
		for _, buf := range col.Data().Buffers() {
			if buf != nil {
				totalSize += int64(buf.Len())
			}
		}
	}

	return totalSize
}
func (wb *WriteBuffer) addRecordToBuffer() error {
	record := wb.recordBuilder.NewRecord()
	if err := wb.writeRecord(record); err != nil {
		return err
	}
	// Update the currentEstBufferSize with the size of the record
	wb.currentEstBufferSize.Add(estimateSize(record))
	record.Release()

	return nil
}

func int64Val(value interface{}) (int64, error) {
	var intValue int64
	var err error
	if val, ok := value.(int64); ok {
		intValue = val
	} else if val, ok := value.(int32); ok {
		intValue = int64(val)
	} else {
		intValue, err = strconv.ParseInt(string(value.([]uint8)), 10, 64)
		if err != nil {
			return 0, err
		}
	}

	return intValue, nil
}

func int32Val(value interface{}) (int64, error) {
	var intValue int64
	var err error
	if val, ok := value.(int64); ok {
		intValue = val
	} else if val, ok := value.(int32); ok {
		intValue = int64(val)
	} else {
		intValue, err = strconv.ParseInt(string(value.([]uint8)), 10, 32)
		if err != nil {
			return 0, err
		}
	}

	return intValue, nil
}
