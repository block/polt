package runner

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/siddontang/loggers"
	"github.com/sirupsen/logrus"
	"github.com/squareup/polt/pkg/archive"
	"github.com/squareup/polt/pkg/audit"
	"github.com/squareup/polt/pkg/boot"
	"github.com/squareup/polt/pkg/destinations"
	"github.com/squareup/polt/pkg/random"
	"github.com/squareup/polt/pkg/upload"
)

const archiveMode = "archive"

type ArchiveRunner struct {
	archiver archive.Archiver
	db       *sql.DB
	lock     *dbconn.MetadataLock
	// Attached logger
	logger loggers.Advanced

	auditDB         string
	runID           string
	stgTbl          string
	dstType         string
	dstPath         string
	startedBy       string
	database        string
	dsn             string
	threads         int
	lockWaitTimeout time.Duration
	host            string
	username        string
	password        string
	fileSizeInBytes uint64
	startTime       time.Time
	dryRun          bool
}

type ArchiveRunnerConfig struct {
	RunID           string
	StgTbl          string
	DstType         string
	DstPath         string
	StartedBy       string
	Database        string
	AuditDB         string
	LockWaitTimeout time.Duration
	Threads         int
	Host            string
	Username        string
	Password        string
	FileSizeInBytes uint64
	DryRun          bool
}
type ConfigLoader func(ctx context.Context, optFns ...func(*config.LoadOptions) error) (aws.Config, error)

func NewArchiveRunner(acfg *ArchiveRunnerConfig, logger *logrus.Logger) (*ArchiveRunner, error) {
	return &ArchiveRunner{
		runID:           acfg.RunID,
		stgTbl:          acfg.StgTbl,
		dstType:         acfg.DstType,
		dstPath:         acfg.DstPath,
		startedBy:       acfg.StartedBy,
		database:        acfg.Database,
		auditDB:         acfg.AuditDB,
		host:            acfg.Host,
		username:        acfg.Username,
		password:        acfg.Password,
		logger:          logger,
		threads:         acfg.Threads,
		lockWaitTimeout: acfg.LockWaitTimeout,
		fileSizeInBytes: acfg.FileSizeInBytes,
		dryRun:          acfg.DryRun,
	}, nil
}

// Prepare generates a new runID for the stage if not present already.
func (ar *ArchiveRunner) Prepare() string {
	if ar.runID == "" {
		ar.runID = random.ID()
	}

	return ar.runID
}

func (ar *ArchiveRunner) Close() error {
	var err error
	if ar.lock != nil {
		if err = ar.lock.Close(); err != nil {
			return err
		}
	}
	if ar.db != nil {
		if err = ar.db.Close(); err != nil {
			return err
		}
	}

	return nil
}

func (ar *ArchiveRunner) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var err error

	successful, err := ar.setupDBAndCheckIfRunSucceeded(ctx)
	if err != nil {
		return fmt.Errorf("error setting up db: %w", err)
	}
	// Return early if the archive has already been successful
	if successful {
		ar.logger.Infof("Archive with run-id:%s already successful, skipping", ar.runID)

		return nil
	}

	ar.startTime = time.Now()
	ar.logger.Infof("Starting archive phase of archive: run-id=%s concurrency=%d table=%s.%s",
		ar.runID, ar.threads, ar.database, ar.stgTbl)
	ab, err := ar.boot(ctx)
	if err != nil {
		return err
	}

	if err = ar.setArchiver(ctx, ab.SrcTbl); err != nil {
		return err
	}

	runStatus := Succeeded.String()
	if err2 := ar.trackRunProgress(ctx); err2 != nil {
		return err2
	}

	// This is where the real work / moving happens.
	// Moving involves copying the matching rows from staging table to final
	// destination and deleting the staging table.
	archiveErr := ar.archiver.Move(ctx, ar.dryRun)
	if archiveErr != nil {
		runStatus = Failed.String()
	}

	if err = setRunEntryStatus(ctx, &Run{
		mode:    archiveMode,
		ID:      ar.runID,
		status:  runStatus,
		auditDB: ar.auditDB,
		db:      ar.db,
	}); err != nil {
		return err
	}

	if err = ar.cleanup(ctx); err != nil {
		return err
	}

	if archiveErr == nil {
		ar.logger.Infof("Successfully archived run-id=%s", ar.runID)
	} else {
		ar.logger.Errorf("Failed to archive run-id=%s: %v", ar.runID, archiveErr)
	}

	return archiveErr
}

func (ar *ArchiveRunner) trackRunProgress(ctx context.Context) error {
	if err := setRunEntryStatus(ctx, &Run{
		mode:    archiveMode,
		ID:      ar.runID,
		status:  Running.String(),
		auditDB: ar.auditDB,
		db:      ar.db,
	}); err != nil {
		return err
	}

	if ar.archiver.IsResumable() {
		ra, ok := ar.archiver.(archive.ResumableArchiver)
		if !ok {
			return errors.New("archiver is not resumable")
		}

		go ar.writeStatus(ctx, ra)

		go ar.writeCheckpoint(ctx, ra, checkpointDumpInterval)
	}

	return nil
}

func (ar *ArchiveRunner) setupDBAndCheckIfRunSucceeded(ctx context.Context) (bool, error) {
	var err error
	ar.dsn = dsnFromCreds(&DBCreds{
		Host:     ar.host,
		Username: ar.username,
		Password: ar.password,
		Database: ar.database,
	})
	dbconfig := setupDBConfig(&DBConfig{Threads: ar.threads, LockWaitTimeout: ar.lockWaitTimeout})
	ar.db, err = setupDB(ar.dsn, dbconfig)
	if err != nil {
		return false, err
	}

	successful, err := checkIfSuccessfullyRan(ctx, &Run{
		mode:       archiveMode,
		ID:         ar.runID,
		auditDB:    ar.auditDB,
		db:         ar.db,
		inputQuery: "",
	})

	if err != nil {
		return false, fmt.Errorf("failed to check if successfully ran: %w", err)
	}

	return successful, nil
}

func (ar *ArchiveRunner) boot(ctx context.Context) (*boot.ArchiveBooter, error) {
	var err error
	// Prepare the runID
	ar.Prepare()

	// run the booter that sets up audit tables
	ab := boot.NewArchiveBooter(&boot.ArchiveBooterConfig{
		DB:       ar.db,
		SrcTbl:   ar.stgTbl,
		AuditDB:  ar.auditDB,
		RunID:    ar.runID,
		Database: ar.database,
	})

	if err = ab.PreflightChecks(ctx); err != nil {
		return nil, fmt.Errorf("failed preflight checks: %w", err)
	}

	if err = ab.Setup(ctx); err != nil {
		return nil, fmt.Errorf("failed booter setup: %w", err)
	}

	if err = ab.PostSetupChecks(ctx); err != nil {
		return nil, fmt.Errorf("failed post-setup checks: %w", err)
	}
	// Get advisory lock on the table, this will be unlocked in Close() method.
	dbConfig := dbconn.NewDBConfig()
	ar.lock, err = dbconn.NewMetadataLock(ctx, ar.dsn, []*table.TableInfo{ab.SrcTbl}, dbConfig, slog.Default())
	if err != nil {
		return nil, fmt.Errorf("failed to acquire advisory lock for staging: %w", err)
	}

	return ab, nil
}
func (ar *ArchiveRunner) handleRunStatus(ctx context.Context, run *Run) (*audit.Checkpoint, error) {
	var chkpnt *audit.Checkpoint
	var tryNum int
	runStatus, err := getRunEntryStatus(ctx, run)
	if err != nil {
		return nil, err
	}
	switch runStatus {
	case "":
		err = createRunEntry(ctx, run)
		if err != nil {
			return nil, err
		}
	case Failed.String(), Running.String(), Started.String():
		chkpnt, tryNum, err = ar.getChkPtAndTryNumForResume(ctx, *run)
		if err != nil {
			return nil, err
		}
		ar.logger.Warnf("archive with given run-id: %s already failed, resuming with new try_num: %d", ar.runID, tryNum)
	case Errored.String():
		return nil, fmt.Errorf("archive with given run-id: %s already errored, can't resume. Kick off a new archive job with new runid", ar.runID)
	default:
		return nil, fmt.Errorf("unknown status: %s", runStatus)
	}

	return chkpnt, nil
}

func (ar *ArchiveRunner) setArchiver(ctx context.Context, stgTbl *table.TableInfo) error {
	var run = Run{
		mode:       archiveMode,
		ID:         ar.runID,
		auditDB:    ar.auditDB,
		db:         ar.db,
		inputQuery: "",
	}
	var chkPt *audit.Checkpoint
	var err error
	chkPt, err = ar.handleRunStatus(ctx, &run)
	if err != nil {
		return err
	}
	switch ar.dstType {
	case destinations.ArchiveTable.String():
		ar.archiver = archive.NewTblArchiver(&archive.TblArchiverConfig{
			DB:     ar.db,
			StgTbl: stgTbl,
			DstTbl: ar.dstPath,
		})

	case destinations.S3File.String(), destinations.LocalParquetFile.String():
		uploader, err := upload.NewUploader(ctx, ar.dstType, ar.dstPath, awsConfigLoader)
		if err != nil {
			return err
		}
		ar.archiver, err = archive.NewFileArchiver(&archive.FileConfig{
			DB:              ar.db,
			SrcTbl:          stgTbl,
			Logger:          ar.logger,
			ChkPt:           chkPt,
			Uploader:        uploader,
			FileSizeInBytes: ar.fileSizeInBytes,
			Threads:         ar.threads,
		})
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown destination type %s", ar.dstType)
	}

	return nil
}

func awsConfigLoader(ctx context.Context, _ ...func(*config.LoadOptions) error) (aws.Config, error) {
	return config.LoadDefaultConfig(ctx)
}

func (ar *ArchiveRunner) getChkPtAndTryNumForResume(ctx context.Context, run Run) (*audit.Checkpoint, int, error) {
	chkpnt, err := ar.getCheckpoint(ctx)
	if err != nil {
		return chkpnt, -1, err
	}
	tryNum, err := restartRunWithNewTryNum(ctx, &run)
	if err != nil {
		return nil, tryNum, err
	}
	if chkpnt == nil {
		ar.logger.Warnf("no checkpoint found for failed archive on run ID %s", ar.runID)
	} else {
		ar.logger.Warnf("Resuming the failed archive from checkpoint %v with try_num: %d", *chkpnt, tryNum)
	}

	return chkpnt, tryNum, nil
}

func (ar *ArchiveRunner) getCheckpoint(ctx context.Context) (*audit.Checkpoint, error) {
	checkPtTblName := audit.CheckPtsTblName(ar.runID)
	query := fmt.Sprintf("SELECT copied_until, rows_copied FROM %s.%s WHERE run_id=? ORDER BY id DESC LIMIT 1", ar.auditDB, checkPtTblName)
	var copiedUntil string
	var rowsCopied uint64
	err := ar.db.QueryRowContext(ctx, query, ar.runID).Scan(&copiedUntil, &rowsCopied)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil //nolint:nilnil // caller correctly handles nil pointer
	} else if err != nil {
		return nil, fmt.Errorf("could not read from table '%s'", checkPtTblName)
	}

	return &audit.Checkpoint{
		CopiedUntil: copiedUntil,
		RowsCopied:  rowsCopied,
	}, nil
}

func (ar *ArchiveRunner) cleanup(ctx context.Context) error {
	// Drop the checkpoint table
	if err := dbconn.Exec(ctx, ar.db, "DROP TABLE IF EXISTS %n.%n", ar.auditDB, audit.CheckPtsTblName(ar.runID)); err != nil {
		return err
	}

	return nil
}

func (ar *ArchiveRunner) writeCheckpoint(ctx context.Context, ra archive.ResumableArchiver, checkPointInterval time.Duration) {
	chkPtTblName := audit.CheckPtsTblName(ar.runID)
	ticker := time.NewTicker(checkPointInterval)
	defer ticker.Stop()
	write := func() {
		copiedUntil, err := ra.GetLowWatermark()
		if err != nil {
			ar.logger.Errorf("error retrieving low watermark: %v", err)
		}
		// No need to write checkpoint if copiedUntil is empty
		if copiedUntil == "" {
			return
		}

		copyRows := ra.NumCopiedRows()
		// Note: when we write the copiedUntil to the log, we are
		// exposing the PK values, when using the composite chunker are
		// based on actual user-data.
		// We believe this is OK but may change it in the future.
		// Please do not add any other fields to this log line.
		ar.logger.Infof("checkpoint: low-watermark=%s rows-copied=%d", copiedUntil, copyRows)

		if err = dbconn.Exec(context.WithoutCancel(ctx), ar.db, "INSERT INTO %n.%n (run_id, mode, created_at, updated_at, copied_until, rows_copied) VALUES (%?, %?, %?, %?, %?, %?)", ar.auditDB, chkPtTblName, ar.runID, archiveMode, time.Now(), time.Now(), copiedUntil, copyRows); err != nil {
			ar.logger.Errorf("error writing checkpoint: %v", err)
		}
	}
	defer write()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			write()
		}
	}
}

func (ar *ArchiveRunner) writeStatus(ctx context.Context, ra archive.ResumableArchiver) {
	ticker := time.NewTicker(statusInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ar.logger.Infof("archive status: state='copying data' archive-progress=%s total-time=%s conns-in-use=%d",
				ra.GetProgress(),
				time.Since(ar.startTime).Round(time.Second),
				ar.db.Stats().InUse,
			)
		}
	}
}
