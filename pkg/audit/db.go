package audit

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
)

const RunsTblName = "runs"

var checkPointTblCreateStmt = `CREATE TABLE IF NOT EXISTS %n.%n (
    id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
    run_id varchar(255), 
    mode ENUM('stage', 'archive'), 
    created_at timestamp DEFAULT CURRENT_TIMESTAMP , 
    updated_at timestamp DEFAULT CURRENT_TIMESTAMP ,
    rows_copied BIGINT UNSIGNED NOT NULL, 
    copied_until TEXT
    )`

var RunsTblCreateStmt = `CREATE TABLE IF NOT EXISTS %n.%n (
    id int NOT NULL AUTO_INCREMENT PRIMARY KEY, 
    run_id varchar(255), 
    try_num int NOT NULL,  
    mode ENUM('stage', 'archive'), 
    created_at timestamp DEFAULT CURRENT_TIMESTAMP , 
    updated_at timestamp DEFAULT CURRENT_TIMESTAMP, 
    started_by varchar(255), 
    input_query TEXT, 
    folded_query TEXT, 
    src_table varchar(255), 
    dst_type varchar(255), 
    dst_path varchar(255), 
    dry_run tinyint, 
    status ENUM('started', 'running', 'errored', 'failed', 'succeeded'), UNIQUE KEY run_id_with_mode (run_id, mode)
    )`

func CheckPtsTblName(runID string) string {
	return "checkpoints_" + runID
}

func StgTblName(tableName string, runID string) string {
	return fmt.Sprintf("_%s_stage_%s", tableName, runID)
}

func CreateDB(ctx context.Context, db *sql.DB, auditDB string) error {
	return dbconn.Exec(ctx, db, "CREATE DATABASE IF NOT EXISTS %n", auditDB)
}

func CreateRunsTbl(ctx context.Context, db *sql.DB, auditDB string) error {
	err := dbconn.Exec(ctx, db, RunsTblCreateStmt, auditDB, RunsTblName)
	if err != nil {
		return err
	}
	runsTable := table.NewTableInfo(db, auditDB, RunsTblName)

	return runsTable.SetInfo(ctx)
}

func CreateCheckpointTbl(ctx context.Context, db *sql.DB, auditDB string, runID string) error {
	err := dbconn.Exec(ctx, db, checkPointTblCreateStmt, auditDB, CheckPtsTblName(runID))
	if err != nil {
		return err
	}
	chkptTable := table.NewTableInfo(db, auditDB, CheckPtsTblName(runID))

	return chkptTable.SetInfo(ctx)
}
