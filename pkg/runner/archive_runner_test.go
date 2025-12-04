package runner

import (
	"context"
	"database/sql"
	"os"
	"path"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/block/polt/pkg/destinations"
	"github.com/block/polt/pkg/test"
	"github.com/go-sql-driver/mysql"
	"github.com/siddontang/loggers"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	test.DeleteAllParquetFiles()
	exitCode := m.Run()
	test.DeleteAllParquetFiles()
	os.Exit(exitCode)
}

func NewArchiveRunnerForTest(acfg *ArchiveRunnerConfig, logger loggers.Advanced) *ArchiveRunner {
	return &ArchiveRunner{
		runID:           acfg.RunID,
		stgTbl:          acfg.StgTbl,
		dstType:         acfg.DstType,
		dstPath:         acfg.DstPath,
		startedBy:       acfg.StartedBy,
		database:        acfg.Database,
		auditDB:         acfg.AuditDB,
		threads:         acfg.Threads,
		lockWaitTimeout: acfg.LockWaitTimeout,
		host:            acfg.Host,
		username:        acfg.Username,
		password:        acfg.Password,
		logger:          logger,
		fileSizeInBytes: acfg.FileSizeInBytes,
	}
}

var arRunID = "ar_runid"

func TestNewArchiveRunner(t *testing.T) {
	test.RunSQL(t, `DROP TABLE IF EXISTS t1_ar_archived, _t1_ar_stage_runid`)
	test.RunSQL(t, "DROP TABLE IF EXISTS polt.runs")
	test.RunSQL(t, "DROP TABLE IF EXISTS polt.checkpoints_ar_runid")
	table := `CREATE TABLE _t1_ar_stage_runid (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		try_num int NOT NULL,
		KEY name_idx (name),
		PRIMARY KEY (id)
	)`
	test.RunSQL(t, table)

	cfg, err := mysql.ParseDSN(test.DSN())
	require.NoError(t, err)

	archive := &ArchiveRunnerConfig{
		DstPath:         "t1_ar_archived",
		DstType:         "table",
		StartedBy:       startedBy,
		RunID:           arRunID,
		StgTbl:          "_t1_ar_stage_runid",
		Database:        cfg.DBName,
		Host:            cfg.Addr,
		Username:        cfg.User,
		Password:        cfg.Passwd,
		AuditDB:         "polt",
		LockWaitTimeout: 30 * time.Second,
		Threads:         16,
	}

	require.NoError(t, err)
	a := NewArchiveRunnerForTest(archive, logrus.New())

	require.NoError(t, a.Run(context.Background()))

	assert.Equal(t, 17, a.db.Stats().MaxOpenConnections) // Test that connection pool max connections set to number of threads + 1.

	// Test that the archiverunner is successful
	successful, err := checkIfSuccessfullyRan(context.Background(), &Run{
		mode:       archiveMode,
		ID:         arRunID,
		db:         a.db,
		auditDB:    a.auditDB,
		inputQuery: "",
	})
	require.NoError(t, err)
	require.True(t, successful)

	// Test that checkpoints table is deleted after the archive phase finishes successfully.
	require.False(t, test.TableExists(t, "polt", "checkpoints_"+arRunID, a.db))
	// Test that runs table exists after the archive phase finishes successfully.
	require.True(t, test.TableExists(t, "polt", "runs", a.db))

	a.Close()
	// Test that DB connection pool is closed after closing the ArchiveRunner.
	require.Error(t, a.db.Ping())

	// Test that re-running the same archive again will be idempotent and doesn't error out.
	archive1 := &ArchiveRunnerConfig{
		DstPath:         "t1_ar_archived",
		DstType:         "table",
		StartedBy:       startedBy,
		RunID:           arRunID,
		StgTbl:          "_t1_ar_stage_runid",
		Database:        cfg.DBName,
		Host:            cfg.Addr,
		Username:        cfg.User,
		Password:        cfg.Passwd,
		AuditDB:         "polt",
		Threads:         16,
		LockWaitTimeout: 30 * time.Second,
	}

	require.NoError(t, err)
	a1 := NewArchiveRunnerForTest(archive1, logrus.New())

	require.NoError(t, a1.Run(context.Background()))
	// Test that it doesn't create/leave new checkpoint table for a successful re-run.
	require.False(t, test.TableExists(t, "polt", "checkpoints_"+arRunID, a1.db))
	require.Nil(t, a1.archiver)
}

func TestNewArchiveRunner_ParquetFile(t *testing.T) {
	parquetFile := "t1_ar_archived.parquet"
	test.DeleteFile(t, parquetFile)

	test.RunSQL(t, `DROP TABLE IF EXISTS _t1_ia_stage_file_runid`)
	test.RunSQL(t, "DROP TABLE IF EXISTS polt.runs")
	test.RunSQL(t, "DROP TABLE IF EXISTS polt.checkpoints_ar_runid")

	tbl := `CREATE TABLE _t1_ia_stage_file_runid (
		id int NOT NULL AUTO_INCREMENT,
		created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (id)
	)`
	test.RunSQL(t, tbl)

	// Insert 3 rows into the staging table
	test.RunSQL(t, `INSERT INTO _t1_ia_stage_file_runid (id, created_at) VALUES (6,'2025-01-05 00:27:21')`)
	test.RunSQL(t, `INSERT INTO _t1_ia_stage_file_runid (id, created_at) VALUES (4, '2025-01-06 20:53:59')`)
	test.RunSQL(t, `INSERT INTO _t1_ia_stage_file_runid (id, created_at) VALUES (3,'2025-01-10 10:15:58')`)

	cfg, err := mysql.ParseDSN(test.DSN())
	require.NoError(t, err)

	archive := &ArchiveRunnerConfig{
		DstPath:         parquetFile,
		DstType:         destinations.LocalParquetFile.String(),
		StartedBy:       startedBy,
		RunID:           arRunID,
		StgTbl:          "_t1_ia_stage_file_runid",
		Database:        cfg.DBName,
		Host:            cfg.Addr,
		Username:        cfg.User,
		Password:        cfg.Passwd,
		AuditDB:         "polt",
		LockWaitTimeout: 30 * time.Second,
		Threads:         2,
	}

	require.NoError(t, err)
	a := NewArchiveRunnerForTest(archive, logrus.New())

	require.NoError(t, a.Run(context.Background()))

	assert.Equal(t, 3, a.db.Stats().MaxOpenConnections) // Test that connection pool max connections set to number of threads + 1.

	// Test the data in parquet file
	var totalRowsArchived int
	parquetFiles := test.FindParquetFile(t, "t1_ar_archived.parquet", true)
	require.Len(t, parquetFiles, 1)
	props := parquet.NewReaderProperties(memory.DefaultAllocator)
	fileReader, err := file.OpenParquetFile(path.Clean(parquetFiles[0]), false, file.WithReadProps(props))
	require.NoError(t, err)
	defer fileReader.Close()

	require.Equal(t, 1, fileReader.NumRowGroups())
	rgr := fileReader.RowGroup(0)
	totalRowsArchived += int(rgr.NumRows())
	require.Equal(t, 3, totalRowsArchived)

	// Test that the archiverunner is successful
	successful, err := checkIfSuccessfullyRan(context.Background(), &Run{
		mode:       archiveMode,
		ID:         arRunID,
		db:         a.db,
		auditDB:    a.auditDB,
		inputQuery: "",
	})
	require.NoError(t, err)
	require.True(t, successful)

	// Test that checkpoints table is deleted after the archive phase finishes successfully.
	require.False(t, test.TableExists(t, "polt", "checkpoints_"+arRunID, a.db))
	// Test that runs table exists after the archive phase finishes successfully.
	require.True(t, test.TableExists(t, "polt", "runs", a.db))

	a.Close()
	// Test that DB connection pool is closed after closing the ArchiveRunner.
	require.Error(t, a.db.Ping())

	// Test that re-running the same archive again will be idempotent and doesn't error out.
	archive1 := &ArchiveRunnerConfig{
		DstPath:         parquetFile,
		DstType:         destinations.LocalParquetFile.String(),
		StartedBy:       startedBy,
		RunID:           arRunID,
		StgTbl:          "_t1_ia_stage_file_runid",
		Database:        cfg.DBName,
		Host:            cfg.Addr,
		Username:        cfg.User,
		Password:        cfg.Passwd,
		AuditDB:         "polt",
		Threads:         2,
		LockWaitTimeout: 30 * time.Second,
	}

	require.NoError(t, err)
	a1 := NewArchiveRunnerForTest(archive1, logrus.New())

	require.NoError(t, a1.Run(context.Background()))
	// Test that it doesn't create/leave new checkpoint table for a successful re-run.
	require.False(t, test.TableExists(t, "polt", "checkpoints_"+arRunID, a1.db))
	require.Nil(t, a1.archiver)
}

func TestArchiveRunnerFailure(t *testing.T) {
	test.RunSQL(t, `DROP TABLE IF EXISTS t1_ar_archived, _t1_ar_stage_runid`)
	test.RunSQL(t, "DROP TABLE IF EXISTS polt.runs")
	test.RunSQL(t, "DROP TABLE IF EXISTS polt.checkpoints_ar_runid")
	table := `CREATE TABLE _t1_ar_stage_runid (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		try_num int NOT NULL,
		KEY name_idx (name),
		PRIMARY KEY (id)
	)`
	test.RunSQL(t, table)

	cfg, err := mysql.ParseDSN(test.DSN())
	require.NoError(t, err)

	archive := &ArchiveRunnerConfig{
		DstPath:         "t1_ar_archived",
		DstType:         "table",
		StartedBy:       startedBy,
		RunID:           arRunID,
		StgTbl:          "_t1_ar_stage_runid",
		Database:        cfg.DBName,
		Host:            cfg.Addr,
		Username:        cfg.User,
		Password:        cfg.Passwd,
		AuditDB:         "polt",
		LockWaitTimeout: 30 * time.Second,
		Threads:         16,
	}

	require.NoError(t, err)
	a := NewArchiveRunnerForTest(archive, logrus.New())

	// Create pre-existing archive table to simulate failure
	test.RunSQL(t, "CREATE TABLE t1_ar_archived (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY)")

	// Test that the archiverunner fails
	require.ErrorContains(t, a.Run(context.Background()), "Table 't1_ar_archived' already exists")
}

func TestArchiveMultipleParquetFiles(t *testing.T) {
	// Lower the checkpoint interval for testing.
	checkpointDumpInterval = 1 * time.Second
	statusInterval = 5 * time.Second
	defer func() {
		statusInterval = 30 * time.Second
		checkpointDumpInterval = 30 * time.Second
	}()

	test.RunSQL(t, `DROP TABLE IF EXISTS _ar_cpt3_stage_runid`)
	test.RunSQL(t, "DROP TABLE IF EXISTS polt.runs, polt.checkpoints_runid")

	tbl := `CREATE TABLE _ar_cpt3_stage_runid (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		name varchar(255) NOT NULL,
	    name2 varchar(255) NOT NULL
    )`

	db, err := sql.Open("mysql", test.DSN())
	require.NoError(t, err)

	test.RunSQL(t, tbl)

	test.RunSQL(t, `INSERT INTO _ar_cpt3_stage_runid (name, name2) SELECT hex(RANDOM_BYTES(2)), hex(RANDOM_BYTES(2)) FROM dual`)
	test.RunSQL(t, `INSERT INTO _ar_cpt3_stage_runid (name, name2) SELECT hex(RANDOM_BYTES(2)), hex(RANDOM_BYTES(2)) FROM _ar_cpt3_stage_runid`)
	test.RunSQL(t, `INSERT INTO _ar_cpt3_stage_runid (name, name2) SELECT hex(RANDOM_BYTES(2)), hex(RANDOM_BYTES(2)) FROM _ar_cpt3_stage_runid a JOIN _ar_cpt3_stage_runid b`)
	test.RunSQL(t, `INSERT INTO _ar_cpt3_stage_runid (name, name2) SELECT hex(RANDOM_BYTES(2)), hex(RANDOM_BYTES(2)) FROM _ar_cpt3_stage_runid a JOIN _ar_cpt3_stage_runid b JOIN _ar_cpt3_stage_runid c`)
	test.RunSQL(t, `INSERT INTO _ar_cpt3_stage_runid (name, name2) SELECT hex(RANDOM_BYTES(2)), hex(RANDOM_BYTES(2)) FROM _ar_cpt3_stage_runid a JOIN _ar_cpt3_stage_runid b JOIN _ar_cpt3_stage_runid c JOIN _ar_cpt3_stage_runid d LIMIT 200000`)

	cfg, err := mysql.ParseDSN(test.DSN())
	require.NoError(t, err)

	arConfig := &ArchiveRunnerConfig{
		StgTbl:          "_ar_cpt3_stage_runid",
		Threads:         2,
		RunID:           runID,
		FileSizeInBytes: 10 * 1024, // 10KB files
		StartedBy:       startedBy,
		DstType:         destinations.LocalParquetFile.String(),
		DstPath:         "_ar_cpt3_stage_runid.parquet",
		Database:        cfg.DBName,
		Host:            cfg.Addr,
		Username:        cfg.User,
		Password:        cfg.Passwd,
		AuditDB:         "polt",
	}

	a := NewArchiveRunnerForTest(arConfig, logrus.New())

	// Get the total number of matching rows that will be archived
	rowsToBeArchived := test.GetCount(t, db, "_ar_cpt3_stage_runid", "1=1")
	require.Equal(t, 200222, rowsToBeArchived)
	require.NoError(t, err)
	err = a.Run(context.Background())

	require.NoError(t, err)

	// Add up the rows in all parquet files
	var totalRowsArchived int
	parquetFiles := test.FindParquetFile(t, "_ar_cpt3_stage_runid.parquet", true)
	for _, f := range parquetFiles {
		props := parquet.NewReaderProperties(memory.DefaultAllocator)
		fileReader, err := file.OpenParquetFile(path.Clean(f), false, file.WithReadProps(props))
		require.NoError(t, err)
		defer fileReader.Close()

		require.Equal(t, 1, fileReader.NumRowGroups())
		rgr := fileReader.RowGroup(0)
		totalRowsArchived += int(rgr.NumRows())
	}

	// Sum of rows in all parquet files should be equal to the total rows that will be archived.
	require.EqualValues(t, rowsToBeArchived, totalRowsArchived)
	a.Close()
}

func TestArchiveMultipleParquetFiles_ResumeFromCheckpt(t *testing.T) {
	var runID = "p_ar_runid"
	var checkpointsTbl = "checkpoints_p_ar_runid"
	// Lower the checkpoint interval for testing.
	checkpointDumpInterval = 100 * time.Millisecond
	statusInterval = 5 * time.Second
	defer func() {
		statusInterval = 30 * time.Second
		checkpointDumpInterval = 30 * time.Second
	}()

	test.RunSQL(t, `DROP TABLE IF EXISTS _ar_cpt4_stage_runid`)
	test.RunSQL(t, "DROP TABLE IF EXISTS polt.runs, polt.checkpoints_p_ar_runid")

	tbl := `CREATE TABLE _ar_cpt4_stage_runid (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		name varchar(255) NOT NULL,
	    name2 varchar(255) NOT NULL
    )`

	db, err := sql.Open("mysql", test.DSN())
	require.NoError(t, err)

	test.RunSQL(t, tbl)

	test.RunSQL(t, `INSERT INTO _ar_cpt4_stage_runid (name, name2) SELECT hex(RANDOM_BYTES(2)), hex(RANDOM_BYTES(2)) FROM dual`)
	test.RunSQL(t, `INSERT INTO _ar_cpt4_stage_runid (name, name2) SELECT hex(RANDOM_BYTES(2)),hex(RANDOM_BYTES(2)) FROM _ar_cpt4_stage_runid`)
	test.RunSQL(t, `INSERT INTO _ar_cpt4_stage_runid (name, name2) SELECT hex(RANDOM_BYTES(2)), hex(RANDOM_BYTES(2)) FROM _ar_cpt4_stage_runid a JOIN _ar_cpt4_stage_runid b`)
	test.RunSQL(t, `INSERT INTO _ar_cpt4_stage_runid (name, name2) SELECT hex(RANDOM_BYTES(2)), hex(RANDOM_BYTES(2)) FROM _ar_cpt4_stage_runid a JOIN _ar_cpt4_stage_runid b JOIN _ar_cpt4_stage_runid c`)
	test.RunSQL(t, `INSERT INTO _ar_cpt4_stage_runid (name, name2) SELECT hex(RANDOM_BYTES(2)), hex(RANDOM_BYTES(2)) FROM _ar_cpt4_stage_runid a JOIN _ar_cpt4_stage_runid b JOIN _ar_cpt4_stage_runid c JOIN _ar_cpt4_stage_runid d LIMIT 200000`)

	cfg, err := mysql.ParseDSN(test.DSN())
	require.NoError(t, err)

	arConfig := &ArchiveRunnerConfig{
		StgTbl:          "_ar_cpt4_stage_runid",
		Threads:         2,
		RunID:           runID,
		FileSizeInBytes: 500 * 1024, // 500KB files
		StartedBy:       startedBy,
		DstType:         destinations.LocalParquetFile.String(),
		DstPath:         "_ar_cpt4_stage_runid.parquet",
		Database:        cfg.DBName,
		Host:            cfg.Addr,
		Username:        cfg.User,
		Password:        cfg.Passwd,
		AuditDB:         "polt",
	}

	a := NewArchiveRunnerForTest(arConfig, logrus.New())

	// Get the total number of matching rows that will be archived
	rowsToBeArchived := test.GetCount(t, db, "_ar_cpt4_stage_runid", "1=1")
	require.Equal(t, 200222, rowsToBeArchived)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		for {
			// Cancel the archiver forcefully as soon as we notice data in checkpoints table
			if test.TableExists(t, auditDB, checkpointsTbl, db) && test.GetCount(t, db, "polt.checkpoints_p_ar_runid", "1=1") > 0 &&
				test.TableExists(t, auditDB, "runs", db) && test.GetCount(t, db, "polt.runs", "run_id='p_ar_runid'") > 0 {
				cancel()

				break
			}
		}
	}()
	time.Sleep(2 * time.Second)
	err = a.Run(ctx)
	require.Error(t, err)

	// Test that run status changes to `failed`
	status, err := getRunEntryStatus(context.Background(), &Run{
		mode:    archiveMode,
		ID:      runID,
		auditDB: auditDB,
		db:      db,
	})
	require.NoError(t, err)
	require.Equal(t, "failed", status)

	a1 := NewArchiveRunnerForTest(arConfig, logrus.New())
	err = a1.Run(context.Background())
	require.NoError(t, err)

	// Add up the rows in all parquet files
	var totalRowsArchived int
	var allRowsIDs []int32
	parquetFiles := test.FindParquetFile(t, "_ar_cpt4_stage_runid.parquet", true)
	for _, f := range parquetFiles {
		props := parquet.NewReaderProperties(memory.DefaultAllocator)
		fileReader, err := file.OpenParquetFile(path.Clean(f), false, file.WithReadProps(props))
		require.NoError(t, err)
		defer fileReader.Close()

		require.Equal(t, 1, fileReader.NumRowGroups())
		rgr := fileReader.RowGroup(0)
		totalRowsArchived += int(rgr.NumRows())

		// Read the primary key column from the parquet file
		rdr, err := rgr.Column(0)
		require.NoError(t, err)
		require.NotNil(t, rdr)
		rowsInt32, ok := rdr.(*file.Int32ColumnChunkReader)
		require.True(t, ok)
		valsInt32 := make([]int32, rgr.NumRows())

		total, read, err := rowsInt32.ReadBatch(rgr.NumRows(), valsInt32, nil, nil)
		require.NoError(t, err)
		require.Equal(t, rgr.NumRows(), total)
		require.EqualValues(t, rgr.NumRows(), read)
		allRowsIDs = append(allRowsIDs, valsInt32...)
	}

	// Sum of unique rows in all parquet files should be equal to the total rows that will be archived.
	// We do unique over primary key column to avoid duplicates. There will be duplicates due to the fact that
	// we are resuming from checkpoint.
	require.Len(t, uniqueInts(allRowsIDs), rowsToBeArchived)
	a.Close()
}

func uniqueInts(input []int32) []int32 {
	u := make([]int32, 0)
	m := make(map[int32]bool)

	for _, val := range input {
		if _, ok := m[val]; !ok {
			m[val] = true
			u = append(u, val)
		}
	}

	return u
}

func TestArchiveRunnerWithGeneratedColumns(t *testing.T) {
	test.RunSQL(t, `DROP TABLE IF EXISTS t1_ar_gen_archived, _t1_ar_gen_stage_runid`)
	test.RunSQL(t, "DROP TABLE IF EXISTS polt.runs")
	test.RunSQL(t, "DROP TABLE IF EXISTS polt.checkpoints_ar_runid")

	// Create staging table with generated columns (both VIRTUAL and STORED)
	table := `CREATE TABLE _t1_ar_gen_stage_runid (
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
	test.RunSQL(t, `INSERT INTO _t1_ar_gen_stage_runid (first_name, last_name) VALUES ('John', 'Doe')`)
	test.RunSQL(t, `INSERT INTO _t1_ar_gen_stage_runid (first_name, last_name) VALUES ('Jane', 'Smith')`)
	test.RunSQL(t, `INSERT INTO _t1_ar_gen_stage_runid (first_name, last_name) VALUES ('Bob', 'Johnson')`)
	test.RunSQL(t, `INSERT INTO _t1_ar_gen_stage_runid (first_name, last_name) VALUES ('Alice', 'Williams')`)

	cfg, err := mysql.ParseDSN(test.DSN())
	require.NoError(t, err)

	archive := &ArchiveRunnerConfig{
		DstPath:         "t1_ar_gen_archived",
		DstType:         "table",
		StartedBy:       startedBy,
		RunID:           arRunID,
		StgTbl:          "_t1_ar_gen_stage_runid",
		Database:        cfg.DBName,
		Host:            cfg.Addr,
		Username:        cfg.User,
		Password:        cfg.Passwd,
		AuditDB:         "polt",
		LockWaitTimeout: 30 * time.Second,
		Threads:         16,
	}

	a := NewArchiveRunnerForTest(archive, logrus.New())
	require.NoError(t, a.Run(context.Background()))

	// Verify that the archive table was created
	db, err := sql.Open("mysql", test.DSN())
	require.NoError(t, err)
	defer db.Close()

	// Check that all rows were archived
	archivedCount := test.GetCount(t, db, "t1_ar_gen_archived", "1=1")
	require.Equal(t, 4, archivedCount)

	// Verify that generated columns exist in the archive table
	var columnCount int
	err = db.QueryRow("SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = 'test' AND TABLE_NAME = 't1_ar_gen_archived' AND COLUMN_NAME IN ('full_name', 'name_length')").Scan(&columnCount)
	require.NoError(t, err)
	require.Equal(t, 2, columnCount, "Generated columns should exist in archive table")

	// Verify the generated column values are correct in the archive table
	var fullName string
	var nameLength int
	err = db.QueryRow("SELECT full_name, name_length FROM t1_ar_gen_archived WHERE first_name = 'Alice' LIMIT 1").Scan(&fullName, &nameLength)
	require.NoError(t, err)
	require.Equal(t, "Alice Williams", fullName)
	require.Equal(t, 14, nameLength)

	// Test that the archiverunner is successful
	successful, err := checkIfSuccessfullyRan(context.Background(), &Run{
		mode:       archiveMode,
		ID:         arRunID,
		db:         a.db,
		auditDB:    a.auditDB,
		inputQuery: "",
	})
	require.NoError(t, err)
	require.True(t, successful)

	// Test that checkpoints table is deleted after the archive phase finishes successfully
	require.False(t, test.TableExists(t, "polt", "checkpoints_"+arRunID, a.db))

	a.Close()
}

func TestArchiveRunnerWithGeneratedColumns_ParquetFile(t *testing.T) {
	parquetFile := "t1_ar_gen_archived.parquet"
	test.DeleteFile(t, parquetFile)

	test.RunSQL(t, `DROP TABLE IF EXISTS _t1_ar_gen_pq_stage_runid`)
	test.RunSQL(t, "DROP TABLE IF EXISTS polt.runs")
	test.RunSQL(t, "DROP TABLE IF EXISTS polt.checkpoints_ar_runid")

	// Create staging table with generated columns
	tbl := `CREATE TABLE _t1_ar_gen_pq_stage_runid (
		id int NOT NULL AUTO_INCREMENT,
		price decimal(10,2) NOT NULL,
		tax_rate decimal(5,4) NOT NULL DEFAULT 0.0825,
		tax_amount decimal(10,2) AS (price * tax_rate) STORED,
		total_price decimal(10,2) AS (price + (price * tax_rate)) VIRTUAL,
		created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
		PRIMARY KEY (id)
	)`
	test.RunSQL(t, tbl)

	// Insert test data
	test.RunSQL(t, `INSERT INTO _t1_ar_gen_pq_stage_runid (price, tax_rate) VALUES (100.00, 0.0825)`)
	test.RunSQL(t, `INSERT INTO _t1_ar_gen_pq_stage_runid (price, tax_rate) VALUES (250.50, 0.0900)`)
	test.RunSQL(t, `INSERT INTO _t1_ar_gen_pq_stage_runid (price, tax_rate) VALUES (75.25, 0.0750)`)

	cfg, err := mysql.ParseDSN(test.DSN())
	require.NoError(t, err)

	archive := &ArchiveRunnerConfig{
		DstPath:         parquetFile,
		DstType:         destinations.LocalParquetFile.String(),
		StartedBy:       startedBy,
		RunID:           arRunID,
		StgTbl:          "_t1_ar_gen_pq_stage_runid",
		Database:        cfg.DBName,
		Host:            cfg.Addr,
		Username:        cfg.User,
		Password:        cfg.Passwd,
		AuditDB:         "polt",
		LockWaitTimeout: 30 * time.Second,
		Threads:         2,
	}

	a := NewArchiveRunnerForTest(archive, logrus.New())
	require.NoError(t, a.Run(context.Background()))

	// Test the data in parquet file
	var totalRowsArchived int
	parquetFiles := test.FindParquetFile(t, "t1_ar_gen_archived.parquet", true)
	require.Len(t, parquetFiles, 1)
	props := parquet.NewReaderProperties(memory.DefaultAllocator)
	fileReader, err := file.OpenParquetFile(path.Clean(parquetFiles[0]), false, file.WithReadProps(props))
	require.NoError(t, err)
	defer fileReader.Close()

	require.Equal(t, 1, fileReader.NumRowGroups())
	rgr := fileReader.RowGroup(0)
	totalRowsArchived += int(rgr.NumRows())
	require.Equal(t, 3, totalRowsArchived)

	// Verify that all columns including generated ones are in the parquet file
	require.Equal(t, 6, fileReader.MetaData().Schema.NumColumns(), "Should have 6 columns including generated columns")

	// Test that the archiverunner is successful
	successful, err := checkIfSuccessfullyRan(context.Background(), &Run{
		mode:       archiveMode,
		ID:         arRunID,
		db:         a.db,
		auditDB:    a.auditDB,
		inputQuery: "",
	})
	require.NoError(t, err)
	require.True(t, successful)

	// Test that checkpoints table is deleted after the archive phase finishes successfully
	require.False(t, test.TableExists(t, "polt", "checkpoints_"+arRunID, a.db))

	a.Close()
}
