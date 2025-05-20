package archive

import (
	"context"
	"database/sql"
	"os"
	"path"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/block/polt/pkg/test"
	"github.com/block/polt/pkg/upload"
	"github.com/cashapp/spirit/pkg/table"
	"github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	test.DeleteAllParquetFiles()
	exitCode := m.Run()
	test.DeleteAllParquetFiles()
	os.Exit(exitCode)
}

func TestNewFileArchiver(t *testing.T) {
	var parquetFileSuffix = "file.parquet"
	test.RunSQL(t, `DROP TABLE IF EXISTS _t1_ia_stage_file_runid`)

	tbl := `CREATE TABLE _t1_ia_stage_file_runid (
		id int NOT NULL AUTO_INCREMENT,
		created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (id)
	)`
	test.RunSQL(t, tbl)

	test.RunSQL(t, `INSERT INTO _t1_ia_stage_file_runid (id, created_at) VALUES (6,'2025-01-05 00:27:21')`)
	test.RunSQL(t, `INSERT INTO _t1_ia_stage_file_runid (id, created_at) VALUES (4, '2025-01-06 20:53:59')`)
	test.RunSQL(t, `INSERT INTO _t1_ia_stage_file_runid (id, created_at) VALUES (3,'2025-01-10 10:15:58')`)

	cfg, err := mysql.ParseDSN(test.DSN())
	require.NoError(t, err)
	db, err := sql.Open("mysql", test.DSN())
	require.NoError(t, err)

	stgTableCnt := test.GetCount(t, db, "_t1_ia_stage_file_runid", "1=1")
	stgTbl := table.NewTableInfo(db, cfg.DBName, "_t1_ia_stage_file_runid")
	err = stgTbl.SetInfo(context.Background())

	require.NoError(t, err)

	fileArchiver, err := NewFileArchiver(
		&FileConfig{
			DB:       db,
			SrcTbl:   stgTbl,
			Logger:   logrus.New(),
			ChkPt:    nil,
			Uploader: upload.NewFileUploader(parquetFileSuffix),
			Threads:  2,
		},
	)
	require.NoError(t, err)

	err = fileArchiver.Move(context.Background(), false)
	require.NoError(t, err)
	props := parquet.NewReaderProperties(memory.DefaultAllocator)

	fileReader, err := file.OpenParquetFile(path.Clean(test.FindParquetFile(t, parquetFileSuffix, true)[0]),
		false, file.WithReadProps(props))
	require.NoError(t, err)
	defer fileReader.Close()

	require.Equal(t, 1, fileReader.NumRowGroups())
	rgr := fileReader.RowGroup(0)
	require.EqualValues(t, 3, rgr.NumRows())
	require.EqualValues(t, stgTableCnt, rgr.NumRows())

	rdr, err := rgr.Column(0)
	require.NoError(t, err)
	require.NotNil(t, rdr)
	rowsInt64, ok := rdr.(*file.Int32ColumnChunkReader)
	require.True(t, ok)

	valsInt32 := make([]int32, 3)
	total, read, err := rowsInt64.ReadBatch(int64(3), valsInt32, nil, nil)
	require.NoError(t, err)
	require.Equal(t, int64(3), total)
	require.Equal(t, 3, read)
	require.ElementsMatch(t, []int32{6, 4, 3}, valsInt32)
}

func TestNewFileArchiver_BinaryKey(t *testing.T) {
	var parquetFileSuffix = "file2.parquet"
	test.RunSQL(t, `DROP TABLE IF EXISTS _t2_ia_stage_file_runid`)

	tbl := `CREATE TABLE _t2_ia_stage_file_runid (
		pk varbinary(40) NOT NULL,
		a int NOT NULL,
		b varchar(255) NOT NULL,
		PRIMARY KEY (pk)
	)`
	test.RunSQL(t, tbl)

	// Test data with UUID as primary key
	test.RunSQL(t, `INSERT INTO _t2_ia_stage_file_runid (pk, a, b) SELECT "edc9a31a-fe04-11ef-8cc8-66d7ea1d41ac", 1, "hai" FROM dual`)
	test.RunSQL(t, `INSERT INTO _t2_ia_stage_file_runid (pk, a, b) SELECT "7d4aada4-fe05-11ef-96eb-66d7ea1d41ac", 2, "hello" FROM dual`)
	test.RunSQL(t, `INSERT INTO _t2_ia_stage_file_runid (pk, a, b) SELECT "87e306bc-fe05-11ef-aba9-66d7ea1d41ac", 3, "bye" FROM dual`)

	cfg, err := mysql.ParseDSN(test.DSN())
	require.NoError(t, err)
	db, err := sql.Open("mysql", test.DSN())
	require.NoError(t, err)

	stgTableCnt := test.GetCount(t, db, "_t2_ia_stage_file_runid", "1=1")
	stgTbl := table.NewTableInfo(db, cfg.DBName, "_t2_ia_stage_file_runid")
	err = stgTbl.SetInfo(context.Background())

	require.NoError(t, err)

	fileArchiver, err := NewFileArchiver(
		&FileConfig{
			DB:       db,
			SrcTbl:   stgTbl,
			Logger:   logrus.New(),
			ChkPt:    nil,
			Uploader: upload.NewFileUploader(parquetFileSuffix),
			Threads:  2,
		},
	)
	require.NoError(t, err)

	err = fileArchiver.Move(context.Background(), false)
	require.NoError(t, err)
	props := parquet.NewReaderProperties(memory.DefaultAllocator)

	fileReader, err := file.OpenParquetFile(path.Clean(test.FindParquetFile(t, parquetFileSuffix, true)[0]),
		false, file.WithReadProps(props))
	require.NoError(t, err)
	defer fileReader.Close()

	require.Equal(t, 1, fileReader.NumRowGroups())
	rgr := fileReader.RowGroup(0)
	require.EqualValues(t, 3, rgr.NumRows())
	require.EqualValues(t, stgTableCnt, rgr.NumRows())

	// Test the values of the primary key
	rdr, err := rgr.Column(0)
	require.NoError(t, err)
	require.NotNil(t, rdr)
	rowsByteArray, ok := rdr.(*file.ByteArrayColumnChunkReader)
	require.True(t, ok)

	valBytes := make([]parquet.ByteArray, 3)
	total, read, err := rowsByteArray.ReadBatch(int64(3), valBytes, nil, nil)
	require.NoError(t, err)
	require.Equal(t, int64(3), total)
	require.Equal(t, 3, read)

	require.ElementsMatch(t, []parquet.ByteArray{[]byte("edc9a31a-fe04-11ef-8cc8-66d7ea1d41ac"), []byte("87e306bc-fe05-11ef-aba9-66d7ea1d41ac"), []byte("7d4aada4-fe05-11ef-96eb-66d7ea1d41ac")}, valBytes)

	// Test the values of column a
	rdr, err = rgr.Column(1)
	require.NoError(t, err)
	require.NotNil(t, rdr)
	rowsInt32, ok := rdr.(*file.Int32ColumnChunkReader)
	require.True(t, ok)

	valInts := make([]int32, 3)
	total, read, err = rowsInt32.ReadBatch(int64(3), valInts, nil, nil)
	require.NoError(t, err)
	require.Equal(t, int64(3), total)
	require.Equal(t, 3, read)

	require.ElementsMatch(t, []int32{1, 3, 2}, valInts)

	// Test the values of column b
	rdr, err = rgr.Column(2)
	require.NoError(t, err)
	require.NotNil(t, rdr)
	rowsString, ok := rdr.(*file.ByteArrayColumnChunkReader)
	require.True(t, ok)

	valBytes = make([]parquet.ByteArray, 3)
	total, read, err = rowsString.ReadBatch(int64(3), valBytes, nil, nil)
	require.NoError(t, err)
	require.Equal(t, int64(3), total)
	require.Equal(t, 3, read)
	require.ElementsMatch(t, []parquet.ByteArray{[]byte("hai"), []byte("bye"), []byte("hello")}, valBytes)
}
