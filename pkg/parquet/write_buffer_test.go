package parquet

import (
	"context"
	"database/sql"
	"log/slog"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/block/polt/pkg/test"
	"github.com/block/spirit/pkg/table"
	"github.com/stretchr/testify/require"
)

type NullUploader struct{}

func (n *NullUploader) Upload(_ context.Context, _ string, _ []byte, _ *arrow.Schema) error {
	return nil
}

func TestNewParquetManagerWriteGenericRowValues(t *testing.T) {
	// Define schema
	fields := []arrow.Field{
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "age", Type: arrow.PrimitiveTypes.Int32},
	}
	schema := arrow.NewSchema(fields, nil)

	pm := NewWriteBuffer(schema, nil, nil, 0)
	// Create a sample record
	pool := memory.NewGoAllocator()
	builder := array.NewRecordBuilder(pool, schema)
	name, ok := builder.Field(0).(*array.StringBuilder)
	require.True(t, ok)
	name.Append("John Doe")
	age, ok := builder.Field(1).(*array.Int32Builder)
	require.True(t, ok)
	age.Append(30)
	record := builder.NewRecord()
	defer record.Release()

	// Write the row values
	err := pm.WriteGenericRowValues([]interface{}{"John Doe", int32(30)})
	require.NoError(t, err)

	// Verify the row was written as record correctly
	require.EqualValues(t, 1, pm.RowsWritten)
	numRows, err := pm.writer.RowGroupNumRows()
	require.NoError(t, err)
	require.Equal(t, 1, numRows)
	require.Positive(t, pm.buffer.Len(), "Buffer should not be empty after writing a record")
}

func TestNewParquetManagerAddChunk(t *testing.T) {
	// Define schema
	fields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32},
	}
	schema := arrow.NewSchema(fields, nil)

	pm := NewWriteBuffer(schema, nil, &NullUploader{}, 0)
	// Create a sample chunk
	chunk := &table.Chunk{
		Key: []string{"id"},
		LowerBound: &table.Boundary{
			Value:     nil,
			Inclusive: true,
		},
		UpperBound: &table.Boundary{
			Value:     nil,
			Inclusive: false,
		},
	}
	// Add the chunk
	pm.AddChunk(chunk, 1*time.Second, 0)
	// Verify the chunk was added correctly
	require.Len(t, pm.bufferedChunks, 1, "BufferedChunks should have one element after adding a chunk")
	require.Equal(t, chunk, pm.bufferedChunks[0].Chunk, "Chunk should be the same as the one added")
}

func TestNewParquetManagerFlush(t *testing.T) {
	test.RunSQL(t, "DROP TABLE IF EXISTS test.t_flush")
	tbl := `CREATE TABLE t_flush (
		id int(11) NOT NULL AUTO_INCREMENT,
        PRIMARY KEY (id)
	)`
	test.RunSQL(t, tbl)

	test.RunSQL(t, "INSERT INTO t_flush (id) VALUES (1)")
	test.RunSQL(t, "INSERT INTO t_flush (id) VALUES (2)")
	test.RunSQL(t, "INSERT INTO t_flush (id) VALUES (3)")

	db, err := sql.Open("mysql", test.DSN())
	require.NoError(t, err)
	ti := table.NewTableInfo(db, "test", "t_flush")
	err = ti.SetInfo(context.Background())
	require.NoError(t, err)

	schema, err := MysqlToArrowSchema(db, "t_flush")
	require.NoError(t, err)

	chunker, err := table.NewChunker(ti, nil, 1*time.Second, slog.Default())
	require.NoError(t, err)

	pm := NewWriteBuffer(schema, chunker, &NullUploader{}, 0)

	err = chunker.Open()
	require.NoError(t, err)
	defer chunker.Close()

	chunk, err := chunker.Next()
	require.NoError(t, err)

	// Add the chunk
	pm.AddChunk(chunk, 1*time.Second, 0)
	// Flush the chunks
	pm.Flush(context.Background())

	// Verify the chunks were flushed correctly
	require.Empty(t, pm.bufferedChunks, "BufferedChunks should be empty after flushing")

	// Verify the buffer was reset
	require.Equal(t, 4, pm.buffer.Len(), "Buffer should be empty after flushing")
	require.Equal(t, "PAR1", pm.buffer.String(), "Buffer should only contain the parquet magic number")
}
