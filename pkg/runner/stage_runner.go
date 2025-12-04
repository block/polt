package runner

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/throttler"
	"github.com/siddontang/loggers"
	"github.com/squareup/polt/pkg/audit"
	"github.com/squareup/polt/pkg/boot"
	"github.com/squareup/polt/pkg/destinations"
	"github.com/squareup/polt/pkg/random"
	"github.com/squareup/polt/pkg/stage"
)

const stageMode = "stage"

const (
	stateInitial int32 = iota
	stateBooterSetup
	stateBooterPreflight
	stateStage
	stateComplete
)

var checkpointDumpInterval = 50 * time.Second

// defaultTargetChunkTime is useful for services that embed polt and don't set the target chunk time.
var defaultTargetChunkTime = 2 * time.Second

type StageRunner struct {
	db            *sql.DB
	replicaDSN    string
	replicaDB     *sql.DB
	stager        *stage.Stager
	lock          *dbconn.MetadataLock
	runID         string
	startedBy     string
	table         string
	database      string
	auditDB       string
	query         string
	currentState  int32 // must use atomic to get/set
	replicaMaxLag time.Duration

	// Attached logger
	logger          loggers.Advanced
	dryrun          bool
	targetChunkTime time.Duration
	threads         int
	lockWaitTimeout time.Duration
	startTime       time.Time
	dsn             string
	host            string
	username        string
	password        string
}

type StageRunnerConfig struct {
	RunID           string
	StartedBy       string
	Database        string
	Table           string
	AuditDB         string
	Query           string
	DryRun          bool
	Threads         int
	TargetChunkTime time.Duration
	ReplicaDSN      string
	ReplicaMaxLag   time.Duration
	LockWaitTimeout time.Duration
	Host            string
	Username        string
	Password        string
}

func NewStageRunner(s *StageRunnerConfig, logger loggers.Advanced) (*StageRunner, error) {
	if s.TargetChunkTime == 0 {
		logger.Warnf("target-chunk-time not set, using default value of %s", defaultTargetChunkTime)
		s.TargetChunkTime = defaultTargetChunkTime
	}

	return &StageRunner{
		currentState:    stateInitial,
		runID:           s.RunID,
		startedBy:       s.StartedBy,
		auditDB:         s.AuditDB,
		table:           s.Table,
		host:            s.Host,
		username:        s.Username,
		password:        s.Password,
		database:        s.Database,
		replicaDSN:      s.ReplicaDSN,
		replicaMaxLag:   s.ReplicaMaxLag,
		query:           s.Query,
		threads:         s.Threads,
		lockWaitTimeout: s.LockWaitTimeout,
		targetChunkTime: s.TargetChunkTime,
		logger:          logger,
		dryrun:          s.DryRun,
	}, nil
}

// Prepare generates a new runID for the stage if not present already.
func (sr *StageRunner) Prepare() string {
	if sr.runID == "" {
		sr.runID = random.ID()
	}

	return sr.runID
}

func (sr *StageRunner) Run(ctx context.Context) (string, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Return early if the stage has already been successful
	successful, err := sr.setupDBAndCheckIfRunSucceeded(ctx)
	if err != nil {
		return "", fmt.Errorf("error setting up db: %w", err)
	}
	if successful {
		sr.logger.Infof("Stage with run-id:%s already successful, skipping", sr.runID)

		return audit.StgTblName(sr.table, sr.runID), nil
	}

	sr.startTime = time.Now()
	sr.logger.Infof("Starting stage phase of archive: run-id=%s concurrency=%d target-chunk-size=%s table=%s.%s query=\"%s\"",
		sr.runID, sr.threads, sr.targetChunkTime, sr.database, sr.table, sr.query,
	)
	// run the booter that sets up audit tables and stage table
	sb, err := sr.boot(ctx)
	if err != nil {
		return "", err
	}

	if err = sr.setStager(ctx, sb); err != nil {
		return "", err
	}

	thtlr, err := sr.getThrottler()
	if err != nil {
		return "", err
	}
	sr.stager.SetThrottler(thtlr)

	go sr.writeStatus(ctx)
	// Write checkpoint every checkpointDumpInterval seconds in the background
	// while (in parallel with) staging data. Only stops when ctx is canceled;
	// errors writing checksums are logged but ignored/retried at next interval.
	go sr.writeCheckpoint(ctx, checkpointDumpInterval)

	if err = setRunEntryStatus(ctx, &Run{
		mode:    stageMode,
		ID:      sr.runID,
		status:  Running.String(),
		auditDB: sr.auditDB,
		db:      sr.db,
	}); err != nil {
		return "", err
	}
	sr.setCurrentState(stateStage)

	runStatus := Succeeded.String()

	// This is where the real work / staging happens.
	// Staging involves copying the matching rows from source table to staging
	// table and deleting them from source table.
	stageErr := sr.stager.Stage(ctx)
	if stageErr != nil {
		runStatus = Failed.String()
	}

	if err = setRunEntryStatus(ctx, &Run{
		mode:    stageMode,
		ID:      sr.runID,
		status:  runStatus,
		auditDB: sr.auditDB,
		db:      sr.db,
	}); err != nil {
		return audit.StgTblName(sr.table, sr.runID), err
	}

	sr.setCurrentState(stateComplete)

	if stageErr == nil {
		sr.logger.Infof("Stage phase of archive with run-id: %s completed successfully", sr.runID)
	} else {
		sr.logger.Errorf("Stage phase of archive with run-id: %s failed: %v", sr.runID, stageErr)
	}

	return audit.StgTblName(sr.table, sr.runID), stageErr
}

func (sr *StageRunner) Close() error {
	var err error

	if sr.lock != nil {
		if err = sr.lock.Close(); err != nil {
			return fmt.Errorf("error unlocking the advisory lock on the table: %w", err)
		}
	}

	if sr.db != nil {
		if err = sr.db.Close(); err != nil {
			return fmt.Errorf("error closing the db connection: %w", err)
		}
	}

	return nil
}

func (sr *StageRunner) writeCheckpoint(ctx context.Context, checkPointInterval time.Duration) {
	chkPtTblName := audit.CheckPtsTblName(sr.runID)
	ticker := time.NewTicker(checkPointInterval)
	defer ticker.Stop()
	write := func() {
		if sr.getCurrentState() > stateBooterPreflight && sr.stager.ChunkerOpen.Load() {
			copiedUntil, err := sr.stager.GetLowWatermark()
			if err != nil {
				sr.logger.Errorf("error retrieving low watermark: %v", err)
			}
			// No need to write checkpoint if copiedUntil is empty
			if copiedUntil == "" {
				return
			}
			copyRows := atomic.LoadUint64(&sr.stager.StagedRowsCount)
			// Note: when we write the copiedUntil to the log, we are
			// exposing the PK values, when using the composite chunker are
			// based on actual user-data.
			// We believe this is OK but may change it in the future.
			// Please do not add any other fields to this log line.
			sr.logger.Infof("checkpoint: low-watermark=%s rows-copied=%d", copiedUntil, copyRows)

			if err = dbconn.Exec(context.WithoutCancel(ctx), sr.db, "INSERT INTO %n.%n (run_id, mode, created_at, updated_at, copied_until, rows_copied) VALUES (%?, %?, %?, %?, %?, %?)", sr.auditDB, chkPtTblName, sr.runID, stageMode, time.Now(), time.Now(), copiedUntil, copyRows); err != nil {
				sr.logger.Errorf("error writing checkpoint: %v", err)
			}
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

func (sr *StageRunner) getCheckpoint(ctx context.Context) (*audit.Checkpoint, error) {
	checkPtTblName := audit.CheckPtsTblName(sr.runID)
	query := fmt.Sprintf("SELECT copied_until, rows_copied FROM %s.%s WHERE run_id=? ORDER BY id DESC LIMIT 1", sr.auditDB, checkPtTblName)
	var copiedUntil string
	var rowsCopied uint64
	err := sr.db.QueryRowContext(ctx, query, sr.runID).Scan(&copiedUntil, &rowsCopied)
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

func (sr *StageRunner) setStager(ctx context.Context, sb *boot.StageBooter) error {
	var err error
	var runStatus string
	var tryNum int
	var chkpnt *audit.Checkpoint
	var run = Run{
		mode:       stageMode,
		ID:         sr.runID,
		auditDB:    sr.auditDB,
		db:         sr.db,
		inputQuery: sr.query,
	}
	runStatus, err = getRunEntryStatus(ctx, &run)
	if err != nil {
		return err
	}
	switch runStatus {
	case "":
		err = createRunEntry(ctx, &Run{
			inputQuery:  sr.query,
			foldedQuery: sb.FoldedQuery,
			mode:        stageMode,
			ID:          sr.runID,
			auditDB:     sr.auditDB,
			db:          sr.db,
			startedBy:   sr.startedBy,
			srcTbl:      sb.SrcTbl.TableName,
			dstType:     destinations.StageTable.String(),
			dstPath:     sb.StageTbl.TableName,
			dryRun:      sr.dryrun,
		})
		if err != nil {
			return err
		}
		tryNum = 1
		// Valid case to try to resume the stage job/run

		// It could be in running state if the previous run failed before marking the run as failed, possibly due to
		// DB connection failure.
		// As we acquire the metadata lock on the table prior to this, we can safely assume that the previous run is not running.
	case Failed.String(), Running.String(), Started.String():
		chkpnt, tryNum, err = sr.getChkPtAndTryNumForResume(ctx, run)
		if err != nil {
			return err
		}
	case Errored.String():
		return fmt.Errorf("stage with given run-id: %s already errored, can't resume. Kick off a new stage job with new runid", sr.runID)
	default:
		return fmt.Errorf("unknown status: %s", runStatus)
	}
	sr.stager, err = stage.NewStager(&stage.StagerConfig{
		StageTbl:      sb.StageTbl,
		Key:           sb.IndexForChunker,
		Where:         sb.WhereConditionForChunker,
		TryNum:        tryNum,
		Logger:        sr.logger,
		DryRun:        sr.dryrun,
		SrcTbl:        sb.SrcTbl,
		ChunkDuration: sr.targetChunkTime,
		Threads:       sr.threads,
		DB:            sr.db,
	}, chkpnt)

	return err
}

func (sr *StageRunner) getChkPtAndTryNumForResume(ctx context.Context, run Run) (*audit.Checkpoint, int, error) {
	chkpnt, err := sr.getCheckpoint(ctx)
	if err != nil {
		return chkpnt, -1, err
	}
	tryNum, err := restartRunWithNewTryNum(ctx, &run)
	if err != nil {
		return nil, tryNum, err
	}
	if chkpnt == nil {
		sr.logger.Warnf("no checkpoint found for failed stage on run ID %s", sr.runID)
	} else {
		sr.logger.Warnf("Resuming the failed stage from checkpoint %v with try_num: %d", *chkpnt, tryNum)
	}

	return chkpnt, tryNum, nil
}

func (sr *StageRunner) getCurrentState() int32 {
	return atomic.LoadInt32(&sr.currentState)
}

func (sr *StageRunner) setCurrentState(s int32) {
	atomic.StoreInt32(&sr.currentState, s)
}

// getThrottler returns a throttler based on the replica connection. Returns a Noop throttler if the replica connection is nil.
func (sr *StageRunner) getThrottler() (throttler.Throttler, error) {
	var err error
	var thtl throttler.Throttler
	if sr.replicaDB != nil {
		// An error here means the connection to the replica is not valid, or it can't be detected
		// This is fatal because if a user specifies a replica throttler, and it can't be used,
		// we should not proceed.
		thtl, err = throttler.NewReplicationThrottler(sr.replicaDB, sr.replicaMaxLag, slog.Default())
		if err != nil {
			sr.logger.Warnf("could not create replication throttler: %v", err)

			return nil, err
		}
	} else {
		thtl = &throttler.Noop{}
	}

	err = thtl.Open(context.Background())
	if err != nil {
		sr.logger.Warnf("could not open throttler: %v", err)

		return nil, err
	}

	return thtl, nil
}

// boot sets up the stage booter and runs the preflight checks and setup.
func (sr *StageRunner) boot(ctx context.Context) (*boot.StageBooter, error) {
	var err error

	// Get SrcTbl Info
	srcTbl := table.NewTableInfo(sr.db, sr.database, sr.table)
	if err = srcTbl.SetInfo(ctx); err != nil {
		return nil, err
	}

	// Get advisory lock on the table, this will be unlocked in Close() method.
	// This is the same lock acquired by Spirit when running migrations, so that we prevent migrations and archives running at same time on the same table.
	// see: https://github.com/block/spirit/blob/c3968034254052c56a4ee7f07eced13cabfa2b1a/pkg/migration/runner.go#L205
	dbConfig := dbconn.NewDBConfig()
	sr.lock, err = dbconn.NewMetadataLock(ctx, sr.dsn, []*table.TableInfo{srcTbl}, dbConfig, slog.Default())
	if err != nil {
		return nil, err
	}

	// Set runID if not set
	sr.Prepare()

	sb := boot.NewStageBooter(&boot.StageBooterConfig{
		DB:      sr.db,
		SrcTbl:  srcTbl,
		AuditDB: sr.auditDB,
		Query:   sr.query,
		RunID:   sr.runID,
		Replica: sr.replicaDB,
	})

	sr.setCurrentState(stateBooterPreflight)

	if err := sb.PreflightChecks(ctx); err != nil {
		return nil, err
	}
	sr.setCurrentState(stateBooterSetup)

	if err := sb.Setup(ctx); err != nil {
		return nil, err
	}

	return sb, nil
}

func (sr *StageRunner) writeStatus(ctx context.Context) {
	ticker := time.NewTicker(statusInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			state := sr.getCurrentState()
			if state == stateStage {
				if sr.stager.HasClearEstimates() {
					sr.logger.Infof("stage status: state='staging data' stage-progress=%s total-time=%s stager-time=%s stager-remaining-time=%v stager-is-throttled=%v conns-in-use=%d",
						sr.stager.GetProgress(),
						time.Since(sr.startTime).Round(time.Second),
						time.Since(sr.stager.StartTime()).Round(time.Second),
						sr.stager.GetETA(),
						sr.stager.Throttler.IsThrottled(),
						sr.db.Stats().InUse,
					)
				} else {
					sr.logger.Infof("stage status: state='staging data' total-time=%s stager-time=%s staged-rows-per-sec=%d stager-is-throttled=%v conns-in-use=%d",
						time.Since(sr.startTime).Round(time.Second),
						time.Since(sr.stager.StartTime()).Round(time.Second),
						sr.stager.RowsPerSecond(),
						sr.stager.Throttler.IsThrottled(),
						sr.db.Stats().InUse,
					)
				}
			}
		}
	}
}

func (sr *StageRunner) setupDBAndCheckIfRunSucceeded(ctx context.Context) (bool, error) {
	var err error
	// Setup DB and replicaDB
	sr.dsn = dsnFromCreds(&DBCreds{
		Host:     sr.host,
		Username: sr.username,
		Password: sr.password,
		Database: sr.database,
	})
	dbconfig := setupDBConfig(&DBConfig{Threads: sr.threads, LockWaitTimeout: sr.lockWaitTimeout})
	sr.db, err = setupDB(sr.dsn, dbconfig)
	if err != nil {
		return false, fmt.Errorf("error setting up db: %w", err)
	}
	sr.replicaDB, err = setupReplicaDB(dbconfig, sr.replicaDSN)
	if err != nil {
		return false, fmt.Errorf("error setting up replica-db: %w", err)
	}
	successful, err := checkIfSuccessfullyRan(ctx, &Run{
		mode:       stageMode,
		ID:         sr.runID,
		auditDB:    sr.auditDB,
		db:         sr.db,
		inputQuery: sr.query,
	})

	if err != nil {
		return false, fmt.Errorf("failed to check if successfully ran: %w", err)
	}

	return successful, nil
}
