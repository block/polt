package stage

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/block/polt/pkg/test"
	"github.com/cashapp/spirit/pkg/table"
	"github.com/cashapp/spirit/pkg/throttler"
	"github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var stgtbl = `CREATE TABLE _t1_st_stage_run_id (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		idxed_column int(11) NOT NULL,
		try_num int NOT NULL,
		PRIMARY KEY (id),
        INDEX(idxed_column)
	)`

func setup(t *testing.T) {
	t.Helper()
	test.RunSQL(t, `DROP TABLE IF EXISTS t1_st`)
	test.RunSQL(t, `DROP TABLE IF EXISTS _t1_st_stage_run_id`)
	tbl := `CREATE TABLE t1_st (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		idxed_column int(11) NOT NULL,
		PRIMARY KEY (id),
        INDEX(idxed_column)
	)`

	test.RunSQL(t, stgtbl)

	// Insert random values between 20 and 30
	test.RunSQL(t, tbl)
	test.RunSQL(t, `INSERT INTO t1_st (name, idxed_column) SELECT REPEAT('a', 100), FLOOR(RAND()*(30-20+1))+20 FROM dual`)
	test.RunSQL(t, `INSERT INTO t1_st (name, idxed_column) SELECT REPEAT('a', 100), FLOOR(RAND()*(30-20+1))+20 FROM t1_st`)
	test.RunSQL(t, `INSERT INTO t1_st (name, idxed_column) SELECT REPEAT('a', 100), FLOOR(RAND()*(30-20+1))+20 FROM t1_st a JOIN t1_st b JOIN t1_st c`)
	test.RunSQL(t, `INSERT INTO t1_st (name, idxed_column) SELECT REPEAT('a', 100), FLOOR(RAND()*(30-20+1))+20 FROM t1_st a JOIN t1_st b JOIN t1_st c JOIN t1_st d LIMIT 100000`)
}

func TestStager_DryRun_Stage(t *testing.T) {
	setup(t)
	cfg, err := mysql.ParseDSN(test.DSN())
	require.NoError(t, err)
	db, err := sql.Open("mysql", test.DSN())
	require.NoError(t, err)

	srcTbl := table.NewTableInfo(db, cfg.DBName, "t1_st")
	err = srcTbl.SetInfo(context.Background())
	require.NoError(t, err)

	stageTbl := table.NewTableInfo(db, cfg.DBName, "_t1_st_stage_run_id")
	err = stageTbl.SetInfo(context.Background())
	require.NoError(t, err)

	// Range Query
	test.RunSQL(t, `DROP TABLE IF EXISTS _t1_st_stage_run_id`)
	test.RunSQL(t, stgtbl)

	whereQuery2 := "idxed_column > 21 AND idxed_column <=28"
	s, err := NewStager(&StagerConfig{
		DB:            db,
		Key:           "idxed_column",
		Where:         whereQuery2,
		Logger:        logrus.New(),
		StageTbl:      stageTbl,
		SrcTbl:        srcTbl,
		DryRun:        true,
		ChunkDuration: 50 * time.Second,
		Threads:       4,
	}, nil)
	require.NoError(t, err)
	s.SetThrottler(&throttler.Noop{})

	// Get the matching count before staging
	count1 := test.GetCount(t, db, "t1_st", whereQuery2)
	require.NoError(t, s.Stage(context.Background()))
	count2 := test.GetCount(t, db, "_t1_st_stage_run_id", whereQuery2)
	assert.Equal(t, count1, count2)

	// Test that all matching rows are still on src table.
	assert.Equal(t, count1, test.GetCount(t, db, "t1_st", whereQuery2))
}

func TestStager_Stage(t *testing.T) {
	setup(t)
	cfg, err := mysql.ParseDSN(test.DSN())
	require.NoError(t, err)

	db, err := sql.Open("mysql", test.DSN())
	require.NoError(t, err)

	srcTbl := table.NewTableInfo(db, cfg.DBName, "t1_st")
	err = srcTbl.SetInfo(context.Background())
	require.NoError(t, err)

	stageTbl := table.NewTableInfo(db, cfg.DBName, "_t1_st_stage_run_id")
	err = stageTbl.SetInfo(context.Background())
	require.NoError(t, err)

	// Equal to query
	whereQuery := "`idxed_column` = 23"
	s, err := NewStager(&StagerConfig{
		DB:            db,
		Key:           "idxed_column",
		Where:         whereQuery,
		Logger:        logrus.New(),
		StageTbl:      stageTbl,
		SrcTbl:        srcTbl,
		ChunkDuration: 50 * time.Second,
		Threads:       4,
	}, nil)
	require.NoError(t, err)
	s.SetThrottler(&throttler.Noop{})

	var count1, count2 int
	// Get the matching count before staging
	count1 = test.GetCount(t, db, "t1_st", whereQuery)
	require.NoError(t, s.Stage(context.Background()))
	count2 = test.GetCount(t, db, "_t1_st_stage_run_id", whereQuery)
	assert.Equal(t, count1, count2)

	// Test that all matching rows are deleted on src table.
	assert.Equal(t, 0, test.GetCount(t, db, "t1_st", whereQuery))

	// Range Query
	test.RunSQL(t, `DROP TABLE IF EXISTS _t1_st_stage_run_id`)
	test.RunSQL(t, stgtbl)

	whereQuery2 := "idxed_column > 21 AND idxed_column <=28"
	s, err = NewStager(&StagerConfig{
		DB:            db,
		Key:           "idxed_column",
		Where:         whereQuery2,
		Logger:        logrus.New(),
		StageTbl:      stageTbl,
		SrcTbl:        srcTbl,
		ChunkDuration: 50 * time.Second,
		Threads:       4,
	}, nil)
	require.NoError(t, err)
	s.SetThrottler(&throttler.Noop{})

	// Get the matching count before staging
	count1 = test.GetCount(t, db, "t1_st", whereQuery2)
	require.NoError(t, s.Stage(context.Background()))
	count2 = test.GetCount(t, db, "_t1_st_stage_run_id", whereQuery2)
	assert.Equal(t, count1, count2)

	// Test that all matching rows are deleted on src table.
	assert.Equal(t, 0, test.GetCount(t, db, "t1_st", whereQuery2))
}

func TestETA(t *testing.T) {
	setup(t)
	cfg, err := mysql.ParseDSN(test.DSN())
	require.NoError(t, err)

	db, err := sql.Open("mysql", test.DSN())
	require.NoError(t, err)

	srcTbl := table.NewTableInfo(db, cfg.DBName, "t1_st")
	err = srcTbl.SetInfo(context.Background())
	require.NoError(t, err)

	stageTbl := table.NewTableInfo(db, cfg.DBName, "_t1_st_stage_run_id")
	err = stageTbl.SetInfo(context.Background())
	require.NoError(t, err)

	// Equal to query
	whereQuery := "`idxed_column` = 23"
	stager1, err := NewStager(&StagerConfig{
		DB:            db,
		Key:           "idxed_column",
		Where:         whereQuery,
		Logger:        logrus.New(),
		StageTbl:      stageTbl,
		SrcTbl:        srcTbl,
		ChunkDuration: 50 * time.Second,
		Threads:       4,
	}, nil)
	require.NoError(t, err)
	// The estimates are clear because the query has a covering index.
	require.True(t, stager1.HasClearEstimates())
	stager1.totalRows = 1000

	// Ask for the ETA, it should be "TBD" because the perSecond estimate is not set yet.
	assert.Equal(t, "TBD", stager1.GetETA())
	assert.Equal(t, "0/1000 0.00%", stager1.GetProgress())

	// Imply we staged 90 rows (in a chunk of 100)
	stager1.StagedRowsCount = 90

	staged, estimated, pct := stager1.getStageStats()
	assert.Equal(t, uint64(90), staged)
	assert.Equal(t, uint64(1000), estimated)
	assert.Equal(t, 9, int(pct))

	stager1.rowsPerSecond = 10

	assert.Equal(t, "1m31s", stager1.GetETA())
	assert.Equal(t, "90/1000 9.00%", stager1.GetProgress())

	// Test with non covering index query
	whereQuery2 := "`idxed_column` = 23 AND name = 'a'"
	stager2, err := NewStager(&StagerConfig{
		DB:            db,
		Key:           "idxed_column",
		Where:         whereQuery2,
		Logger:        logrus.New(),
		StageTbl:      stageTbl,
		SrcTbl:        srcTbl,
		ChunkDuration: 50 * time.Second,
		Threads:       4,
	}, nil)
	require.NoError(t, err)
	// The estimates are not clear because the query doesn't have a covering index.
	require.False(t, stager2.HasClearEstimates())

	// Ask for the ETA, it should be "No ETA" because the query doesn't have a covering index and it would be
	// very expensive to perform count(*).
	assert.Equal(t, "NO ETA", stager2.GetETA())
	assert.Equal(t, "Not possible to calculate progress", stager2.GetProgress())
}
