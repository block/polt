package archive

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/block/polt/pkg/parquet"
	"github.com/block/polt/pkg/stage"
	"github.com/block/polt/pkg/test"
	"github.com/cashapp/spirit/pkg/table"
	"github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func TestNewBufferStager(t *testing.T) {
	test.RunSQL(t, `DROP TABLE IF EXISTS _t1_ia_stage_buffer_runid`)

	cfg, err := mysql.ParseDSN(test.DSN())
	require.NoError(t, err)
	db, err := sql.Open("mysql", test.DSN())
	require.NoError(t, err)

	tbl := `CREATE TABLE _t1_ia_stage_buffer_runid (
		id int NOT NULL AUTO_INCREMENT,
		created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (id)
	)`
	test.RunSQL(t, tbl)

	test.RunSQL(t, `INSERT INTO _t1_ia_stage_buffer_runid (id, created_at) VALUES (6,'2025-01-05 00:27:21')`)
	test.RunSQL(t, `INSERT INTO _t1_ia_stage_buffer_runid (id, created_at) VALUES (4, '2025-01-06 20:53:59')`)
	test.RunSQL(t, `INSERT INTO _t1_ia_stage_buffer_runid (id, created_at) VALUES (3,'2025-01-10 10:15:58')`)

	stgTbl := table.NewTableInfo(db, cfg.DBName, "_t1_ia_stage_buffer_runid")
	err = stgTbl.SetInfo(context.Background())
	require.NoError(t, err)
	sconfig := &stage.StagerConfig{
		DB:            db,
		Threads:       1,
		SrcTbl:        stgTbl,
		Logger:        logrus.New(),
		ChunkDuration: time.Second,
	}
	schema, err := parquet.MysqlToArrowSchema(db, "_t1_ia_stage_buffer_runid")
	require.NoError(t, err)
	bufferSize := uint64(1024)

	stager, err := NewBufferStager(sconfig, nil, schema, nil, bufferSize)
	require.NoError(t, err)
	require.NotNil(t, stager)

	err = stager.Stage(context.Background())
	require.NoError(t, err)

	// Test that the BufferStager staged the correct number of rows into the buffer
	require.EqualValues(t, 3, stager.writeBuffer.RowsWritten)
}
