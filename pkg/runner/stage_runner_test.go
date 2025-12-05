package runner

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/block/polt/pkg/test"
	"github.com/go-sql-driver/mysql"
	"github.com/siddontang/loggers"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var runID = "runid"
var checkpointsTbl = "checkpoints_runid"
var startedBy = "cash_app_service"
var auditDB = "polt"

func NewStageRunnerForTest(scfg *StageRunnerConfig, logger loggers.Advanced) *StageRunner {
	return &StageRunner{
		runID:           scfg.RunID,
		startedBy:       scfg.StartedBy,
		auditDB:         scfg.AuditDB,
		table:           scfg.Table,
		query:           scfg.Query,
		database:        scfg.Database,
		host:            scfg.Host,
		username:        scfg.Username,
		password:        scfg.Password,
		replicaDSN:      scfg.ReplicaDSN,
		replicaMaxLag:   scfg.ReplicaMaxLag,
		targetChunkTime: scfg.TargetChunkTime,
		threads:         scfg.Threads,
		logger:          logger,
	}
}

func TestNewStageRunner(t *testing.T) {
	stgRunid := "runid_same"
	test.RunSQL(t, `DROP TABLE IF EXISTS t1_sr_new, _t1_sr_new_stage_runid_same`)
	test.RunSQL(t, "DROP TABLE IF EXISTS polt.runs")
	test.RunSQL(t, "DROP TABLE IF EXISTS polt.checkpoints_runid_same")

	table := `CREATE TABLE t1_sr_new (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		KEY name_idx (name),
		PRIMARY KEY (id)
	)`
	test.RunSQL(t, table)
	cfg, err := mysql.ParseDSN(test.DSN())
	require.NoError(t, err)

	srConfig := &StageRunnerConfig{
		Table:           "t1_sr_new",
		Threads:         16,
		RunID:           stgRunid,
		TargetChunkTime: 10 * time.Millisecond,
		StartedBy:       startedBy,
		Host:            cfg.Addr,
		Username:        cfg.User,
		Password:        cfg.Passwd,
		Database:        cfg.DBName,
		AuditDB:         "polt",
		Query:           "SELECT * FROM t1_sr_new WHERE name ='harry'",
	}

	s := NewStageRunnerForTest(srConfig, logrus.New())
	stgTbl, err := s.Run(context.Background())
	require.NoError(t, err)
	require.Equal(t, "_t1_sr_new_stage_runid_same", stgTbl)

	assert.Equal(t, 17, s.db.Stats().MaxOpenConnections) // Test that connection pool max connections set to number of threads + 1.

	// Test that run entry is created and status is set to Succeeded.
	successful, err := checkIfSuccessfullyRan(context.Background(), &Run{
		mode:       stageMode,
		ID:         s.runID,
		auditDB:    s.auditDB,
		db:         s.db,
		inputQuery: s.query,
	})
	require.NoError(t, err)
	assert.True(t, successful)

	err = s.Close()
	require.NoError(t, err)
	// Test that DB connection pool is closed after closing the StageRunner.
	require.Error(t, s.db.Ping())

	// Test that running already staged data does not error out and is idempotent.
	srConfig1 := &StageRunnerConfig{
		Table:           "t1_sr_new",
		Threads:         16,
		RunID:           stgRunid,
		TargetChunkTime: 10 * time.Millisecond,
		StartedBy:       startedBy,
		Host:            cfg.Addr,
		Username:        cfg.User,
		Password:        cfg.Passwd,
		Database:        cfg.DBName,
		AuditDB:         "polt",
		Query:           "SELECT * FROM t1_sr_new WHERE name ='harry'",
	}
	s1 := NewStageRunnerForTest(srConfig1, logrus.New())
	stgTbl, err = s1.Run(context.Background())
	require.NoError(t, err)
	require.Equal(t, "_t1_sr_new_stage_runid_same", stgTbl)

	// Returns early without setting up stager as data is already staged.
	assert.Nil(t, s1.stager)
	err = s1.Close()
	require.NoError(t, err)
}

func TestStageRunnerPrepare(t *testing.T) {
	cfg, err := mysql.ParseDSN(test.DSN())
	require.NoError(t, err)

	srConfig := &StageRunnerConfig{
		Table:           "t1_sr_pk",
		Threads:         16,
		TargetChunkTime: 10 * time.Millisecond,
		StartedBy:       startedBy,
		Host:            cfg.Addr,
		Username:        cfg.User,
		Password:        cfg.Passwd,
		Database:        cfg.DBName,
		AuditDB:         "polt",
		Query:           "SELECT * FROM t1_sr_pk WHERE id = 1",
	}

	s := NewStageRunnerForTest(srConfig, logrus.New())

	// Prepare generates a new runID if not provided.
	assert.Equal(t, s.Prepare(), s.runID)
	srConfig.RunID = runID

	// Prepare uses the provided runID.
	s = NewStageRunnerForTest(srConfig, logrus.New())
	assert.Equal(t, s.Prepare(), runID)
}

func TestNewStageRunnerWithReplica(t *testing.T) {
	if test.ReplicaDSN() == "" {
		t.Skip("skipping replica tests because REPLICA_DSN not set")
	}
	test.RunSQL(t, `DROP TABLE IF EXISTS t1_st_rep`)
	test.RunSQL(t, `DROP TABLE IF EXISTS _t1_st_rep_stage_runid`)
	tbl := `CREATE TABLE t1_st_rep (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		idxed_column int(11) NOT NULL,
		PRIMARY KEY (id),
        INDEX(idxed_column)
	)`

	// Insert random values between 20 and 30
	test.RunSQL(t, tbl)
	test.RunSQL(t, `INSERT INTO t1_st_rep (name, idxed_column) SELECT REPEAT('a', 100), FLOOR(RAND()*(30-20+1))+20 FROM dual`)
	test.RunSQL(t, `INSERT INTO t1_st_rep (name, idxed_column) SELECT REPEAT('a', 100), FLOOR(RAND()*(30-20+1))+20 FROM t1_st_rep`)
	test.RunSQL(t, `INSERT INTO t1_st_rep (name, idxed_column) SELECT REPEAT('a', 100), FLOOR(RAND()*(30-20+1))+20 FROM t1_st_rep a JOIN t1_st_rep b JOIN t1_st_rep c`)
	test.RunSQL(t, `INSERT INTO t1_st_rep (name, idxed_column) SELECT REPEAT('a', 100), FLOOR(RAND()*(30-20+1))+20 FROM t1_st_rep a JOIN t1_st_rep b JOIN t1_st_rep c JOIN t1_st_rep d LIMIT 100000`)
	test.RunSQL(t, "DROP TABLE IF EXISTS polt.runs")
	test.RunSQL(t, "DROP TABLE IF EXISTS polt.checkpoints_runid")

	cfg, err := mysql.ParseDSN(test.DSN())
	require.NoError(t, err)
	whereQuery := "select * from t1_st_rep where idxed_column > 21 AND idxed_column <=28"

	srConfig := &StageRunnerConfig{
		Table:           "t1_st_rep",
		Threads:         16,
		RunID:           runID,
		TargetChunkTime: 10 * time.Millisecond,
		StartedBy:       startedBy,
		Host:            cfg.Addr,
		Username:        cfg.User,
		Password:        cfg.Passwd,
		Database:        cfg.DBName,
		AuditDB:         "polt",
		Query:           whereQuery,
		ReplicaMaxLag:   5 * time.Millisecond,
	}
	srConfig.ReplicaDSN = test.ReplicaDSN()
	require.NoError(t, err)

	s := NewStageRunnerForTest(srConfig, logrus.New())

	stgTbl, err := s.Run(context.Background())
	require.NoError(t, err)
	require.Equal(t, "_t1_st_rep_stage_runid", stgTbl)

	assert.Equal(t, 17, s.db.Stats().MaxOpenConnections) // Test that connection pool max connections set to number of threads + 1.

	err = s.Close()
	require.NoError(t, err)
	// Test that DB connection pool is closed after closing the StageRunner.
	require.Error(t, s.db.Ping())
}

func TestNewStageRunnerOnPrimaryKey(t *testing.T) {
	test.RunSQL(t, `DROP TABLE IF EXISTS t1_sr_pk, _t1_sr_pk_stage_runid`)
	test.RunSQL(t, "DROP TABLE IF EXISTS polt.runs")
	test.RunSQL(t, "DROP TABLE IF EXISTS polt.checkpoints_runid")

	table := `CREATE TABLE t1_sr_pk (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		KEY name_idx (name),
		PRIMARY KEY (id)
	)`
	test.RunSQL(t, table)
	cfg, err := mysql.ParseDSN(test.DSN())
	require.NoError(t, err)

	srConfig := &StageRunnerConfig{
		Table:           "t1_sr_pk",
		Threads:         16,
		RunID:           runID,
		TargetChunkTime: 10 * time.Millisecond,
		StartedBy:       startedBy,
		Database:        cfg.DBName,
		Host:            cfg.Addr,
		Username:        cfg.User,
		Password:        cfg.Passwd,
		AuditDB:         "polt",
		Query:           "SELECT * FROM t1_sr_pk WHERE id = 1",
	}

	// test inputQuery after adding data to the table
	test.RunSQL(t, "INSERT INTO t1_sr_pk (name) VALUES ('foo')")
	s := NewStageRunnerForTest(srConfig, logrus.New())

	stgTbl, err := s.Run(context.Background())
	require.NoError(t, err)
	require.Equal(t, "_t1_sr_pk_stage_runid", stgTbl)

	err = s.Close()
	require.NoError(t, err)
	// Test that DB connection pool is closed after closing the StageRunner.
	require.Error(t, s.db.Ping())
}

func TestCheckpoint(t *testing.T) {
	// Lower the checkpoint interval for testing.
	checkpointDumpInterval = 300 * time.Nanosecond
	defer func() {
		checkpointDumpInterval = 50 * time.Second
	}()

	test.RunSQL(t, `DROP TABLE IF EXISTS sr_cpt1, _sr_cpt1_stage_runid`)
	test.RunSQL(t, "DROP TABLE IF EXISTS polt.runs, polt.checkpoints_runid")

	tbl := `CREATE TABLE sr_cpt1 (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		name varchar(255) NOT NULL,
	    idxed_column int(11) NOT NULL,
		INDEX(idxed_column)
    )`

	test.RunSQL(t, tbl)

	test.RunSQL(t, `INSERT INTO sr_cpt1 (name, idxed_column) SELECT REPEAT('a', 100), FLOOR(RAND()*(30-20+1))+20 FROM dual`)
	test.RunSQL(t, `INSERT INTO sr_cpt1 (name, idxed_column) SELECT REPEAT('a', 100), FLOOR(RAND()*(30-20+1))+20 FROM sr_cpt1`)
	test.RunSQL(t, `INSERT INTO sr_cpt1 (name, idxed_column) SELECT REPEAT('a', 100), FLOOR(RAND()*(30-20+1))+20 FROM sr_cpt1 a JOIN sr_cpt1 b JOIN sr_cpt1 c`)
	test.RunSQL(t, `INSERT INTO sr_cpt1 (name, idxed_column) SELECT REPEAT('a', 100), FLOOR(RAND()*(30-20+1))+20 FROM sr_cpt1 a JOIN sr_cpt1 b JOIN sr_cpt1 c JOIN sr_cpt1 d`)
	test.RunSQL(t, `INSERT INTO sr_cpt1 (name, idxed_column) SELECT REPEAT('a', 100), FLOOR(RAND()*(30-20+1))+20 FROM sr_cpt1 a JOIN sr_cpt1 b JOIN sr_cpt1 c JOIN sr_cpt1 d LIMIT 100000`)

	cfg, err := mysql.ParseDSN(test.DSN())
	require.NoError(t, err)

	srConfig := &StageRunnerConfig{
		Table:           "sr_cpt1",
		Threads:         2,
		RunID:           runID,
		TargetChunkTime: 1 * time.Second,
		StartedBy:       startedBy,
		Host:            cfg.Addr,
		Username:        cfg.User,
		Password:        cfg.Passwd,
		Database:        cfg.DBName,
		AuditDB:         "polt",
		Query:           "SELECT * FROM sr_cpt1 WHERE idxed_column > 21 AND idxed_column <=28",
	}

	require.NoError(t, err)
	s := NewStageRunnerForTest(srConfig, logrus.New())

	stgTbl, err := s.Run(context.Background())
	require.NoError(t, err)
	require.Equal(t, "_sr_cpt1_stage_runid", stgTbl)

	// Test that there are some entries in checkpoint table.
	assert.NotZero(t, test.GetCount(t, s.db, "polt.checkpoints_runid", "1=1"))
	require.NoError(t, s.Close())
}

func TestResumeFromCheckpoint(t *testing.T) {
	// Lower the checkpoint interval for testing.
	checkpointDumpInterval = 600 * time.Nanosecond
	defer func() {
		checkpointDumpInterval = 50 * time.Second
	}()

	test.RunSQL(t, `DROP TABLE IF EXISTS sr_cpt1, _sr_cpt1_stage_runid`)
	test.RunSQL(t, "DROP TABLE IF EXISTS polt.runs, polt.checkpoints_runid")

	tbl := `CREATE TABLE sr_cpt1 (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		name varchar(255) NOT NULL,
	    idxed_column int(11) NOT NULL,
		INDEX(idxed_column)
    )`

	db, err := sql.Open("mysql", test.DSN())
	require.NoError(t, err)

	test.RunSQL(t, tbl)

	test.RunSQL(t, `INSERT INTO sr_cpt1 (name, idxed_column) SELECT REPEAT('a', 100), FLOOR(RAND()*(30-20+1))+20 FROM dual`)
	test.RunSQL(t, `INSERT INTO sr_cpt1 (name, idxed_column) SELECT REPEAT('a', 100), FLOOR(RAND()*(30-20+1))+20 FROM sr_cpt1`)
	test.RunSQL(t, `INSERT INTO sr_cpt1 (name, idxed_column) SELECT REPEAT('a', 100), FLOOR(RAND()*(30-20+1))+20 FROM sr_cpt1 a JOIN sr_cpt1 b JOIN sr_cpt1 c`)
	test.RunSQL(t, `INSERT INTO sr_cpt1 (name, idxed_column) SELECT REPEAT('a', 100), FLOOR(RAND()*(30-20+1))+20 FROM sr_cpt1 a JOIN sr_cpt1 b JOIN sr_cpt1 c JOIN sr_cpt1 d`)
	test.RunSQL(t, `INSERT INTO sr_cpt1 (name, idxed_column) SELECT REPEAT('a', 100), FLOOR(RAND()*(30-20+1))+20 FROM sr_cpt1 a JOIN sr_cpt1 b JOIN sr_cpt1 c JOIN sr_cpt1 d LIMIT 100000`)

	cfg, err := mysql.ParseDSN(test.DSN())
	require.NoError(t, err)

	srConfig := &StageRunnerConfig{
		Table:           "sr_cpt1",
		Threads:         2,
		RunID:           runID,
		TargetChunkTime: 1 * time.Second,
		StartedBy:       startedBy,
		Database:        cfg.DBName,
		Host:            cfg.Addr,
		Username:        cfg.User,
		Password:        cfg.Passwd,
		AuditDB:         "polt",
		Query:           "SELECT * FROM sr_cpt1 WHERE idxed_column > 21 AND idxed_column <=28",
	}

	s := NewStageRunnerForTest(srConfig, logrus.New())

	// Get the total number of matching rows that will be staged
	rowsToBeDeleted1 := test.GetCount(t, db, "sr_cpt1", "idxed_column > 21 AND idxed_column <=28")

	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		for {
			// Cancel the stager forcefully as soon as we notice data in checkpoints table
			if test.TableExists(t, auditDB, checkpointsTbl, db) && test.GetCount(t, db, "polt.checkpoints_runid", "1=1") > 0 &&
				test.GetCount(t, db, "polt.runs", "run_id='runid'") > 0 {
				// Check that the metadata lock with table name exists while stage runner is running.
				assert.True(t, test.LockExists(t, db, "sr_cpt1"))
				cancel()

				break
			}
		}
	}()
	time.Sleep(1 * time.Second)
	stgTbl, err := s.Run(ctx)
	assert.Equal(t, "_sr_cpt1_stage_runid", stgTbl)
	require.Error(t, err) // it gets interrupted as soon as there is a checkpoint saved.

	// Test that lock is released after runner finishes.
	assert.False(t, test.LockExists(t, db, "sr_cpt1"))

	// Test that run status changes to `failed`
	status, err := getRunEntryStatus(context.Background(), &Run{
		mode:       stageMode,
		ID:         s.runID,
		auditDB:    auditDB,
		db:         db,
		inputQuery: s.query,
	})
	require.NoError(t, err)
	require.Equal(t, "failed", status)

	s1 := NewStageRunnerForTest(srConfig, logrus.New())
	stgTbl, err = s1.Run(context.Background())
	require.NoError(t, err)
	require.Equal(t, "_sr_cpt1_stage_runid", stgTbl)
	// Check that checkpoint table exists after stage phase runs successfully.
	// It will only be deleted only after the corresponding archive phase finishes.
	require.True(t, test.TableExists(t, auditDB, checkpointsTbl, db))
	// Test that runs table exists after the stage phase finishes successfully.
	require.True(t, test.TableExists(t, "polt", "runs", db))

	var tryNum int
	err = db.QueryRowContext(context.Background(), "SELECT try_num FROM polt.runs WHERE run_id='runid'").Scan(&tryNum)
	require.NoError(t, err)
	require.Equal(t, 2, tryNum)

	// Test that all matching rows have been deleted from source table.
	assert.Zero(t, test.GetCount(t, db, "sr_cpt1", "idxed_column > 21 AND idxed_column <=28"))

	// Test that the total rows staged are equal to original total row count
	assert.Equal(t, rowsToBeDeleted1, test.GetCount(t, db, "_sr_cpt1_stage_runid", "idxed_column > 21 AND idxed_column <=28"))
	require.NoError(t, s1.Close())
}

func TestStageRunnerWithGeneratedColumns(t *testing.T) {
	test.RunSQL(t, `DROP TABLE IF EXISTS t1_sr_gen, _t1_sr_gen_stage_runid`)
	test.RunSQL(t, "DROP TABLE IF EXISTS polt.runs")
	test.RunSQL(t, "DROP TABLE IF EXISTS polt.checkpoints_runid")

	// Create table with generated columns (both VIRTUAL and STORED)
	table := `CREATE TABLE t1_sr_gen (
		id int(11) NOT NULL AUTO_INCREMENT,
		first_name varchar(100) NOT NULL,
		last_name varchar(100) NOT NULL,
		full_name varchar(201) AS (CONCAT(first_name, ' ', last_name)) VIRTUAL,
		name_length int AS (LENGTH(CONCAT(first_name, ' ', last_name))) STORED,
		created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
		PRIMARY KEY (id),
		KEY idx_name_length (name_length)
	)`
	test.RunSQL(t, table)

	// Insert test data
	test.RunSQL(t, `INSERT INTO t1_sr_gen (first_name, last_name) VALUES ('John', 'Doe')`)
	test.RunSQL(t, `INSERT INTO t1_sr_gen (first_name, last_name) VALUES ('Jane', 'Smith')`)
	test.RunSQL(t, `INSERT INTO t1_sr_gen (first_name, last_name) VALUES ('Bob', 'Johnson')`)
	test.RunSQL(t, `INSERT INTO t1_sr_gen (first_name, last_name) VALUES ('Alice', 'Williams')`)

	cfg, err := mysql.ParseDSN(test.DSN())
	require.NoError(t, err)

	srConfig := &StageRunnerConfig{
		Table:           "t1_sr_gen",
		Threads:         16,
		RunID:           runID,
		TargetChunkTime: 10 * time.Millisecond,
		StartedBy:       startedBy,
		Host:            cfg.Addr,
		Username:        cfg.User,
		Password:        cfg.Passwd,
		Database:        cfg.DBName,
		AuditDB:         "polt",
		Query:           "SELECT * FROM t1_sr_gen WHERE name_length > 8",
	}

	s := NewStageRunnerForTest(srConfig, logrus.New())
	stgTbl, err := s.Run(context.Background())
	require.NoError(t, err)
	require.Equal(t, "_t1_sr_gen_stage_runid", stgTbl)

	// Verify that the staging table was created and data was copied
	db, err := sql.Open("mysql", test.DSN())
	require.NoError(t, err)
	defer func() {
		if closeErr := db.Close(); closeErr != nil {
			t.Errorf("error closing db: %v", closeErr)
		}
	}()

	// Check that rows matching the query were staged
	stagedCount := test.GetCount(t, db, "_t1_sr_gen_stage_runid", "name_length > 8")
	require.Greater(t, stagedCount, 0)

	// Verify that generated columns exist in the staging table
	var columnCount int
	err = db.QueryRow("SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = 'test' AND TABLE_NAME = '_t1_sr_gen_stage_runid' AND COLUMN_NAME IN ('full_name', 'name_length')").Scan(&columnCount)
	require.NoError(t, err)
	require.Equal(t, 2, columnCount, "Generated columns should exist in staging table")

	// Verify the generated column values are correct
	var fullName string
	var nameLength int
	err = db.QueryRow("SELECT full_name, name_length FROM _t1_sr_gen_stage_runid WHERE first_name = 'Alice' LIMIT 1").Scan(&fullName, &nameLength)
	require.NoError(t, err)
	require.Equal(t, "Alice Williams", fullName)
	require.Equal(t, 14, nameLength)

	// Test that run entry is created and status is set to Succeeded
	successful, err := checkIfSuccessfullyRan(context.Background(), &Run{
		mode:       stageMode,
		ID:         s.runID,
		auditDB:    s.auditDB,
		db:         s.db,
		inputQuery: s.query,
	})
	require.NoError(t, err)
	assert.True(t, successful)

	err = s.Close()
	require.NoError(t, err)
}

func TestResumeFromDBFailure(t *testing.T) {
	// Lower the checkpoint interval for testing.
	checkpointDumpInterval = 600 * time.Nanosecond
	defer func() {
		checkpointDumpInterval = 50 * time.Second
	}()
	var runID = "runid2"
	var checkpointsTbl = "checkpoints_runid2"

	test.RunSQL(t, `DROP TABLE IF EXISTS sr_cpt2, _sr_cpt2_stage_runid2`)
	test.RunSQL(t, "DROP TABLE IF EXISTS polt.runs, polt.checkpoints_runid2")

	tbl := `CREATE TABLE sr_cpt2 (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		name varchar(255) NOT NULL,
	    idxed_column int(11) NOT NULL,
		INDEX(idxed_column)
    )`

	db, err := sql.Open("mysql", test.DSN())
	require.NoError(t, err)

	test.RunSQL(t, tbl)

	test.RunSQL(t, `INSERT INTO sr_cpt2 (name, idxed_column) SELECT REPEAT('a', 100), FLOOR(RAND()*(30-20+1))+20 FROM dual`)
	test.RunSQL(t, `INSERT INTO sr_cpt2 (name, idxed_column) SELECT REPEAT('a', 100), FLOOR(RAND()*(30-20+1))+20 FROM sr_cpt2`)
	test.RunSQL(t, `INSERT INTO sr_cpt2 (name, idxed_column) SELECT REPEAT('a', 100), FLOOR(RAND()*(30-20+1))+20 FROM sr_cpt2 a JOIN sr_cpt2 b JOIN sr_cpt2 c`)
	test.RunSQL(t, `INSERT INTO sr_cpt2 (name, idxed_column) SELECT REPEAT('a', 100), FLOOR(RAND()*(30-20+1))+20 FROM sr_cpt2 a JOIN sr_cpt2 b JOIN sr_cpt2 c JOIN sr_cpt2 d`)
	test.RunSQL(t, `INSERT INTO sr_cpt2 (name, idxed_column) SELECT REPEAT('a', 100), FLOOR(RAND()*(30-20+1))+20 FROM sr_cpt2 a JOIN sr_cpt2 b JOIN sr_cpt2 c JOIN sr_cpt2 d LIMIT 100000`)

	cfg, err := mysql.ParseDSN(test.DSN())
	require.NoError(t, err)

	srConfig := &StageRunnerConfig{
		Table:           "sr_cpt2",
		Threads:         2,
		RunID:           runID,
		TargetChunkTime: 1 * time.Second,
		StartedBy:       startedBy,
		Database:        cfg.DBName,
		Host:            cfg.Addr,
		Username:        cfg.User,
		Password:        cfg.Passwd,
		AuditDB:         "polt",
		Query:           "SELECT * FROM sr_cpt2 WHERE idxed_column > 21 AND idxed_column <=28",
	}

	s := NewStageRunnerForTest(srConfig, logrus.New())

	// Get the total number of matching rows that will be staged
	rowsToBeDeleted1 := test.GetCount(t, db, "sr_cpt2", "idxed_column > 21 AND idxed_column <=28")

	require.NoError(t, err)
	ctx := context.Background()

	go func() {
		for {
			// Fail the stager forcefully as soon as we notice data in checkpoints table
			if test.TableExists(t, auditDB, checkpointsTbl, db) && test.GetCount(t, db, "polt.checkpoints_runid2", "1=1") > 0 &&
				test.GetCount(t, db, "polt.runs", "run_id='runid2'") > 0 {
				// Check that the metadata lock with table name exists while stage runner is running.
				assert.True(t, test.LockExists(t, db, "sr_cpt2"))
				// Simulate DB failure and stage failure by closing the connection.
				_ = s.db.Close() // Ignore error during cleanup

				break
			}
		}
	}()
	time.Sleep(1 * time.Second)
	stgTbl, err := s.Run(ctx)
	assert.Equal(t, "_sr_cpt2_stage_runid2", stgTbl)
	require.Error(t, err) // it gets interrupted as soon as there is a checkpoint saved.

	// Test that lock is released after runner finishes.
	assert.False(t, test.LockExists(t, db, "sr_cpt2"))

	// Test that run status remains in `running` state as the DB connection closed and the run was interrupted.
	status, err := getRunEntryStatus(context.Background(), &Run{
		mode:       stageMode,
		ID:         s.runID,
		auditDB:    auditDB,
		db:         db,
		inputQuery: s.query,
	})
	require.NoError(t, err)
	require.Equal(t, "running", status)

	// Test that the resume from checkpoint works and the run finishes successfully after the DB connection is restored.
	s1 := NewStageRunnerForTest(srConfig, logrus.New())
	stgTbl, err = s1.Run(context.Background())
	require.NoError(t, err)
	require.Equal(t, "_sr_cpt2_stage_runid2", stgTbl)
	// Check that checkpoint table exists after stage phase runs successfully.
	// It will only be deleted only after the corresponding archive phase finishes.
	require.True(t, test.TableExists(t, auditDB, checkpointsTbl, db))
	// Test that runs table exists after the stage phase finishes successfully.
	require.True(t, test.TableExists(t, "polt", "runs", db))

	var tryNum int
	err = db.QueryRowContext(context.Background(), "SELECT try_num FROM polt.runs WHERE run_id='runid2'").Scan(&tryNum)
	require.NoError(t, err)
	require.Equal(t, 2, tryNum)

	// Test that all matching rows have been deleted from source table.
	assert.Zero(t, test.GetCount(t, db, "sr_cpt2", "idxed_column > 21 AND idxed_column <=28"))

	// Test that the total rows staged are equal to original total row count
	assert.Equal(t, rowsToBeDeleted1, test.GetCount(t, db, "_sr_cpt2_stage_runid2", "idxed_column > 21 AND idxed_column <=28"))
	require.NoError(t, s1.Close())
}
