package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/alecthomas/kong"
	"github.com/block/polt/pkg/runner"
	"github.com/sirupsen/logrus"
)

var cli struct {
	Archive ArchiveCmd `cmd:"archive" help:"Archive staged table to final destination"`
	Stage   StageCmd   `cmd:"stage"   help:"Stage the table for archive"`
}

// StageCmd holds the arguments required for staging a source table.
type StageCmd struct {
	RunID           string        `name:"run-id" help:"RunID for the stage job/task" optional:""`
	AuditDB         string        `name:"audit-db" help:"Database to store auditing information" optional:"" default:"polt"`
	DryRun          bool          `name:"dry-run" help:"DryRun mode which only copies data to staging table and doesn't delete from source table"`
	Table           string        `name:"table" help:"Name of the source table" optional:"" default:"stock"`
	Query           string        `name:"query" help:"The WHERE query to run on the table"`
	ReplicaMaxLag   time.Duration `name:"replica-max-lag" help:"The maximum lag allowed on the replica before the archive throttles." optional:"" default:"120s"`
	ReplicaDSN      string        `name:"replica-dsn" help:"A DSN for a replica which (if specified) will be used for lag checking." optional:""`
	StartedBy       string        `name:"started-by" help:"Name of the system/user who started the stage job/run."`
	TargetChunkTime time.Duration `name:"target-chunk-time" help:"The target copy time for each chunk" optional:"" default:"500ms"`
	DBConfig
	DBCreds
}

// ArchiveCmd holds the arguments required for archiving a staging table to the
// final destination.
type ArchiveCmd struct {
	RunID     string `name:"run-id" help:"RunID for the archive job/task" optional:""`
	AuditDB   string `name:"audit-db" help:"Database to store auditing information" optional:"" default:"polt"`
	StgTbl    string `name:"staged_table" help:"StagedTbl" optional:"" default:"stock"`
	DstType   string `name:"destination-type" help:"DstType" optional:"" default:"table"`
	DstPath   string `name:"destination-path" help:"DstPath"`
	StartedBy string `name:"started-by" help:"Name of the system/user who started the stage job/run."`
	DBConfig
	DBCreds
}

// DBCreds..
type DBCreds struct {
	Host     string `name:"host" help:"Hostname" optional:"" default:"127.0.0.1:3306"`
	Username string `name:"username" help:"User" optional:"" default:"msandbox"`
	Password string `name:"password" help:"Password" optional:"" default:"msandbox"`
	Database string `name:"database" help:"Database" optional:"" default:"test"`
}

// DBConfig .
type DBConfig struct {
	Threads         int           `name:"threads" help:"Number of concurrent threads for copy and checksum tasks" optional:"" default:"4"`
	LockWaitTimeout time.Duration `name:"lock-wait-timeout" help:"The DDL lock_wait_timeout required for checksum and cutover" optional:"" default:"30s"`
}

// Run invokes the archiving process. Blocks until completion.
func (a *ArchiveCmd) Run() error {
	logger := logrus.New()
	logger.SetOutput(os.Stdout)

	archiveRunner, err := runner.NewArchiveRunner(&runner.ArchiveRunnerConfig{
		RunID:           a.RunID,
		StgTbl:          a.StgTbl,
		DstType:         a.DstType,
		DstPath:         a.DstPath,
		StartedBy:       a.StartedBy,
		Database:        a.Database,
		Threads:         a.Threads,
		LockWaitTimeout: a.LockWaitTimeout,
		Host:            a.Host,
		Username:        a.Username,
		Password:        a.Password,
		AuditDB:         a.AuditDB,
	}, logger)
	if err != nil {
		return fmt.Errorf("error creating archive runner: %w", err)
	}
	defer archiveRunner.Close()

	return archiveRunner.Run(context.Background())
}

func (s *StageCmd) Run() error {
	logger := logrus.New()
	logger.SetOutput(os.Stdout)

	stageRunner, err := runner.NewStageRunner(&runner.StageRunnerConfig{
		RunID:           s.RunID,
		Query:           s.Query,
		Table:           s.Table,
		StartedBy:       s.StartedBy,
		AuditDB:         s.AuditDB,
		DryRun:          s.DryRun,
		Threads:         s.DBConfig.Threads,
		LockWaitTimeout: s.DBConfig.LockWaitTimeout,
		TargetChunkTime: s.TargetChunkTime,
		Host:            s.Host,
		Username:        s.Username,
		Password:        s.Password,
		Database:        s.Database,
		ReplicaDSN:      s.ReplicaDSN,
		ReplicaMaxLag:   s.ReplicaMaxLag,
	}, logger)
	if err != nil {
		return fmt.Errorf("error creating stage runner: %w", err)
	}
	defer stageRunner.Close()

	_, err = stageRunner.Run(context.Background())

	return err
}

func main() {
	parsedCmd := kong.Parse(&cli)
	parsedCmd.FatalIfErrorf(parsedCmd.Run())
}
