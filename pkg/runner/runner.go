// Package runner contains the logic for running Polt tool in stage and archive modes.
package runner

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/squareup/polt/pkg/audit"
	"github.com/squareup/polt/pkg/boot"
)

type Status int64

const (
	Started Status = iota
	Running
	Failed
	Errored
	Succeeded
)

var statusInterval = 30 * time.Second

func (s Status) String() string {
	switch s {
	case Started:
		return "started"
	case Running:
		return "running"
	case Failed:
		return "failed"
	case Errored:
		return "errored"
	case Succeeded:
		return "succeeded"
	}

	return "unknown"
}

type Run struct {
	mode        string
	ID          string
	status      string
	auditDB     string
	db          *sql.DB
	startedBy   string
	inputQuery  string
	foldedQuery string
	srcTbl      string
	dstType     string
	dstPath     string
	dryRun      bool
}

func createRunEntry(ctx context.Context, run *Run) error {
	err := dbconn.Exec(ctx,
		run.db,
		"INSERT INTO %n.%n (run_id, try_num, mode, created_at, updated_at, started_by, input_query, folded_query, src_table, dst_type, dst_path, dry_run, status) VALUES(%?,%?,%?,%?,%?,%?,%?,%?,%?,%?,%?,%?,%?)",
		run.auditDB,
		audit.RunsTblName,
		run.ID,
		1,
		run.mode,
		time.Now(),
		time.Now(),
		run.startedBy,
		run.inputQuery,
		run.foldedQuery,
		run.srcTbl,
		run.dstType,
		run.dstPath,
		run.dryRun,
		Started.String(),
	)

	return err
}
func restartRunWithNewTryNum(ctx context.Context, run *Run) (int, error) {
	runTblName := audit.RunsTblName
	// Increment the run try_num
	err := dbconn.Exec(ctx, run.db, "UPDATE %n.%n SET try_num = try_num + 1, status = 'started' WHERE run_id=%? AND mode=%?", run.auditDB, runTblName, run.ID, run.mode)
	if err != nil {
		return -1, err
	}
	var tryNum int
	tryNumQuery := fmt.Sprintf("SELECT %s from %s.%s WHERE run_id=? AND mode=?", boot.TryNumColName, run.auditDB, runTblName)
	err = run.db.QueryRowContext(ctx, tryNumQuery, run.ID, run.mode).Scan(&tryNum)
	if err != nil {
		return -1, err
	}

	return tryNum, nil
}

func runTblExists(ctx context.Context, auditDB string, db *sql.DB) (bool, error) {
	var tblExists bool
	err := db.QueryRowContext(ctx, "SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_schema = ? AND table_name = ?)", auditDB, audit.RunsTblName).Scan(&tblExists)
	if err != nil {
		return false, err
	}

	return tblExists, nil
}

// getRunEntryStatus returns the status of the run entry with a given id and input_query.
func getRunEntryStatus(ctx context.Context, run *Run) (string, error) {
	var runStatus = ""
	runTblExists, err := runTblExists(ctx, run.auditDB, run.db)
	if err != nil {
		return "", err
	}
	if runTblExists {
		query := fmt.Sprintf("SELECT status FROM %s.%s WHERE run_id=? AND input_query=? AND mode='%s'", run.auditDB, audit.RunsTblName, run.mode)
		err := run.db.QueryRowContext(ctx, query, run.ID, run.inputQuery).Scan(&runStatus)
		if err != nil && err != sql.ErrNoRows {
			return "", err
		}
	}

	return runStatus, nil
}

// setRunEntryStatus sets the status of the run entry.
func setRunEntryStatus(ctx context.Context, run *Run) error {
	// We want to update the status of the run even if the context is cancelled.
	ctx = context.WithoutCancel(ctx)

	return dbconn.Exec(ctx, run.db, "UPDATE %n.%n SET status = %?, updated_at = %? WHERE run_id = %? AND mode=%?", run.auditDB, audit.RunsTblName, run.status, time.Now(), run.ID, run.mode)
}

func checkIfSuccessfullyRan(ctx context.Context, run *Run) (bool, error) {
	runStatus, err := getRunEntryStatus(ctx, run)
	if err != nil {
		return false, err
	}

	if runStatus == Succeeded.String() {
		return true, nil
	}

	return false, nil
}

type Runner interface {
	Prepare() error
	Run(ctx context.Context) error
	Close() error
}
