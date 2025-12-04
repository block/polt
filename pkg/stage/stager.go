package stage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/block/polt/pkg/audit"
	"github.com/block/polt/pkg/boot"
	"github.com/block/polt/pkg/query"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/throttler"
	"github.com/siddontang/loggers"
	"golang.org/x/sync/errgroup"
)

const backOffMultiple = 10
const stageETAInitialWaitTime = 1 * time.Minute // how long to wait before first estimating staging speed
const stageEstimateInterval = 10 * time.Second  // how often to estimate staging speed

var defaultMaxRetries = 5

type Stager struct {
	sync.Mutex
	db          *sql.DB
	chunker     table.Chunker
	key         string
	concurrency int
	isInvalid   bool
	srcTbl      *table.TableInfo
	stageTbl    *table.TableInfo
	dryRun      bool
	logger      loggers.Advanced

	StagedRowsCount   uint64 // used for estimates: the exact number of rows staged
	StagedChunksCount uint64

	totalRows     uint64
	ChunkerOpen   atomic.Bool
	tryNum        int
	Throttler     throttler.Throttler
	startTime     time.Time
	rowsPerSecond uint64
	estimates     bool
}

type StagerConfig struct {
	DB            *sql.DB
	DBConfig      *dbconn.DBConfig
	DryRun        bool
	Key           string
	Where         string
	TryNum        int
	Logger        loggers.Advanced
	SrcTbl        *table.TableInfo
	StageTbl      *table.TableInfo
	ChunkDuration time.Duration
	Threads       int
}

// NewStager creates a new stager, from a checkpoint (copyRowsAt, copyRows).
func NewStager(sconfig *StagerConfig, chk *audit.Checkpoint) (*Stager, error) {
	// Use slog.Default() for the chunker since spirit now uses slog.Logger
	chunker, err := table.NewCompositeChunker(sconfig.SrcTbl, sconfig.ChunkDuration, slog.Default(), sconfig.Key, sconfig.Where)
	if err != nil {
		return &Stager{}, err
	}
	s := &Stager{
		db:          sconfig.DB,
		chunker:     chunker,
		key:         sconfig.Key,
		tryNum:      sconfig.TryNum,
		logger:      sconfig.Logger,
		concurrency: sconfig.Threads,
		srcTbl:      sconfig.SrcTbl,
		stageTbl:    sconfig.StageTbl,
		dryRun:      sconfig.DryRun,
	}
	_, isCovIdx, err := query.GetIndex("SELECT COUNT(*) FROM "+s.srcTbl.QuotedName+" WHERE "+sconfig.Where, sconfig.DB)
	if err != nil {
		return s, err
	}
	if isCovIdx {
		s.estimates = true
		err = s.db.QueryRow("SELECT COUNT(*) FROM " + s.srcTbl.QuotedName + " WHERE " + sconfig.Where).Scan(&s.totalRows)
		if err != nil {
			return s, err
		}
	}

	if chk != nil {
		// Overwrite the previously attached chunker with one at a specific watermark.
		if err := s.chunker.OpenAtWatermark(chk.CopiedUntil); err != nil {
			return s, err
		}
		// Success from this point on
		// Overwrite copy-rows
		s.ChunkerOpen.Store(true)
		atomic.StoreUint64(&s.StagedRowsCount, chk.RowsCopied)

		// Add the rows that were copied in the checkpoint to the total rows remaining
		atomic.AddUint64(&s.totalRows, chk.RowsCopied)
	}

	return s, nil
}

// isValid checks if the context is valid and the stager is not marked invalid.
// Stager is marked invalid if there is an error while staging a chunk.
func (s *Stager) isValid(ctx context.Context) bool {
	s.Lock()
	defer s.Unlock()
	if ctx.Err() != nil {
		return false
	}

	return !s.isInvalid
}

func (s *Stager) setInvalid(newVal bool) {
	s.Lock()
	defer s.Unlock()
	s.isInvalid = newVal
}

// Stage stages the rows matching in src table to the staging table.
// Staging means copying rows from source table to staging table and deleting
// them from source table.
func (s *Stager) Stage(ctx context.Context) error {
	s.startTime = time.Now()
	if !s.ChunkerOpen.Load() {
		if err := s.chunker.Open(); err != nil {
			return err
		}
		s.ChunkerOpen.Store(true)
	}
	if s.dryRun {
		s.logger.Warnf("Running in dry-run mode so not going to delete data on source table.")
	}
	go s.estimateRowsPerSecond(ctx)
	g, errGrpCtx := errgroup.WithContext(ctx)
	g.SetLimit(s.concurrency)
	for !s.chunker.IsRead() && s.isValid(errGrpCtx) {
		g.Go(func() error {
			chunk, err := s.chunker.Next()
			if err != nil {
				if errors.Is(err, table.ErrTableIsRead) {
					return nil
				}
				s.setInvalid(true)

				return err
			}
			if err := s.RetryableStageChunk(errGrpCtx, chunk); err != nil {
				s.setInvalid(true)

				return err
			}

			return nil
		})
	}
	err := g.Wait()
	s.ChunkerOpen.Store(false)

	return err
}

// backoff sleeps a few milliseconds before retrying.
func backoff(i int) {
	randFactor := i * rand.Intn(backOffMultiple) * int(time.Millisecond)
	time.Sleep(time.Duration(randFactor))
}

func (s *Stager) stageChunkWithTx(ctx context.Context, i int, chunk *table.Chunk, stagingFn func(ctx context.Context, tx *sql.Tx, chunk *table.Chunk) (int64, error)) (int64, error) {
	var err error
	var tx *sql.Tx
	var copiedRows int64
	tx, _, err = dbconn.BeginStandardTrx(ctx, s.db, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return -1, err
	}
	// If there was an error as we exit
	// then rollback before finishing up and then backoff.
	defer func() {
		if err != nil {
			_ = tx.Rollback()
			backoff(i)
		}
	}()

	// Set the error explicitly for the defer to pick up
	copiedRows, err = stagingFn(ctx, tx, chunk)

	return copiedRows, err
}

func (s *Stager) stageChunk(ctx context.Context, tx *sql.Tx, chunk *table.Chunk) (int64, error) {
	var copiedRows int64
	var res sql.Result
	var err error

	// We acquire shared read lock on the rows matching the chunk at the
	// beginning and keep/upgrade the lock until we are done staging the
	// chunk. Ideally the rows meant for archive shouldn't even be actively
	// being updated by another process, so this seems like a fair thing
	// to do. We do however have a locktimeout set, so if we fail to acquire
	// the lock after a timeout with retry for any reason, we will fail the
	// stage job/run.

	// INSERT INGORE because we can have duplicate rows in the chunk because
	// in resuming from checkpoint we will be re-applying some of the
	// previous executed work.

	insertChunkQuery := fmt.Sprintf("INSERT IGNORE INTO %s (%s,%s) SELECT %s,%d FROM %s FORCE INDEX (%s) WHERE %s LOCK IN SHARE MODE",
		s.stageTbl.QuotedName,
		strings.Join(s.srcTbl.Columns, ", "),
		boot.TryNumColName,
		strings.Join(s.srcTbl.Columns, ", "),
		s.tryNum,
		s.srcTbl.QuotedName,
		s.key,
		chunk.String(),
	)
	s.logger.Debugf("\nrunning chunk: %s, insertChunkQuery: %s", chunk.String(), insertChunkQuery)
	res, err = tx.ExecContext(ctx, insertChunkQuery)

	if err != nil {
		s.logger.Errorf("error inserting chunk: %s into staging table. err: %w", chunk.String(), err)

		return -1, err
	}

	copiedRows, err = res.RowsAffected()

	if err != nil {
		s.logger.Errorf("error retreiving number of inserted rows: %w", err)

		return copiedRows, err
	}

	err = s.checksumMatch(ctx, tx, chunk)
	if err != nil {
		s.logger.Errorf("Checksum mismatch %s", err)

		return copiedRows, err
	}

	// Delete the chunk, if not dry-run
	if !s.dryRun {
		deleteChunkQuery := fmt.Sprintf("DELETE FROM %s WHERE %s",
			s.srcTbl.QuotedName,
			chunk.String(),
		)
		_, err = tx.ExecContext(ctx, deleteChunkQuery)
		if err != nil {
			s.logger.Errorf("error deleting chunk: %s from staging table. err: %w", chunk.String(), err)

			return copiedRows, err
		}
	}
	err = tx.Commit()

	return copiedRows, err
}

// RetryableStageChunk copies the chunk to staging table, compares checksum
// between staging and source table and deletes chunk from source table,
// retrying the process until it succeeds or runs out of max configured retries.
func (s *Stager) RetryableStageChunk(ctx context.Context, chunk *table.Chunk) error {
	startTime := time.Now()
	var copiedRows int64
	var err error
	s.Throttler.BlockWait()

	for i := range defaultMaxRetries {
		copiedRows, err = s.stageChunkWithTx(ctx, i, chunk, s.stageChunk)
		if err == nil {
			break
		}
	}
	if err != nil {
		s.logger.Errorf("Error staging the chunk after max %d retries %v", defaultMaxRetries, err)

		return err
	}

	atomic.AddUint64(&s.StagedRowsCount, uint64(copiedRows))
	atomic.AddUint64(&s.StagedChunksCount, 1)

	chunkProcessingTime := time.Since(startTime)
	s.logger.Debugf("ChunkProcessing time %v for chunk %v", chunkProcessingTime, chunk.String())
	s.chunker.Feedback(chunk, chunkProcessingTime, uint64(copiedRows))

	return nil
}

func (s *Stager) checksumMatch(ctx context.Context, tx *sql.Tx, chunk *table.Chunk) error {
	source := fmt.Sprintf("SELECT BIT_XOR(CRC32(CONCAT(%s))) as checksum FROM %s WHERE %s",
		strings.Join(s.srcTbl.Columns, ", "),
		s.srcTbl.QuotedName,
		chunk.String(),
	)
	// Filter the rows on the target/stage table to only the rows that were
	// copied in this try. Previous errored try could have successfully
	// staged some rows within the chunk boundary that will be present in staged
	// table but not in source table. This extra try_num=s.tryNum allows
	// us to filter the rows that were copied from source table in this try
	// only.
	target := fmt.Sprintf("SELECT BIT_XOR(CRC32(CONCAT(%s))) as checksum FROM %s WHERE %s AND try_num=%d",
		strings.Join(s.srcTbl.Columns, ", "),
		s.stageTbl.QuotedName,
		chunk.String(),
		s.tryNum,
	)
	var sourceChecksum, targetChecksum int64
	err := tx.QueryRowContext(ctx, source).Scan(&sourceChecksum)
	if err != nil {
		return err
	}
	err = tx.QueryRowContext(ctx, target).Scan(&targetChecksum)
	if err != nil {
		return err
	}
	if sourceChecksum != targetChecksum {
		return fmt.Errorf("checksum mismatch: %v != %v, sourceSQL: %s, targetSQL: %s", sourceChecksum, targetChecksum, source, target)
	}

	return nil
}

// GetLowWatermark returns the low watermark of the chunker, i.e. the lowest key that has been
// guaranteed to be written to the staging table.
func (s *Stager) GetLowWatermark() (string, error) {
	return s.chunker.GetLowWatermark()
}

func (s *Stager) SetThrottler(throttler throttler.Throttler) {
	s.Lock()
	defer s.Unlock()
	s.Throttler = throttler
}

// Progress/Status functionality is taken from Spirit's Copier
// Unlike Copier, Stager can only give estimated time of completion only if all the columns used in the where clause are indexed.
// If that's not the case , we can only show progress of how fast we are staging the rows and not how much time is left to finish the stage.

// GetProgress returns the progress of the stager.
func (s *Stager) GetProgress() string {
	s.Lock()
	defer s.Unlock()
	if !s.estimates {
		return "Not possible to calculate progress"
	}
	copied, total, pct := s.getStageStats()

	return fmt.Sprintf("%d/%d %.2f%%", copied, total, pct)
}

func (s *Stager) estimateRowsPerSecond(ctx context.Context) {
	// We take >10 second averages because with parallel stage it bounces around a lot.
	prevRowsCount := atomic.LoadUint64(&s.StagedRowsCount)

	ticker := time.NewTicker(stageEstimateInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !s.isValid(ctx) {
				return
			}
			newRowsCount := atomic.LoadUint64(&s.StagedRowsCount)
			rowsPerInterval := float64(newRowsCount - prevRowsCount)
			intervalsDivisor := float64(stageEstimateInterval / time.Second) // should be something like 10 for 10 seconds
			rowsPerSecond := uint64(rowsPerInterval / intervalsDivisor)
			atomic.StoreUint64(&s.rowsPerSecond, rowsPerSecond)
			prevRowsCount = newRowsCount
		}
	}
}

func (s *Stager) getStageStats() (uint64, uint64, float64) {
	if !s.estimates {
		return 0, 0, 0
	}
	pct := float64(atomic.LoadUint64(&s.StagedRowsCount)) / float64(s.totalRows) * 100

	return atomic.LoadUint64(&s.StagedRowsCount), s.totalRows, pct
}

func (s *Stager) StartTime() time.Time {
	s.Lock()
	defer s.Unlock()

	return s.startTime
}

func (s *Stager) HasClearEstimates() bool {
	return s.estimates
}

func (s *Stager) RowsPerSecond() uint64 {
	return atomic.LoadUint64(&s.rowsPerSecond)
}

func (s *Stager) GetETA() string {
	s.Lock()
	defer s.Unlock()
	if !s.HasClearEstimates() {
		return "NO ETA"
	}
	copiedRows, totalRows, pct := s.getStageStats()
	rowsPerSecond := s.RowsPerSecond()
	if pct > 99.99 {
		return "DUE"
	}
	if rowsPerSecond == 0 || time.Since(s.startTime) < stageETAInitialWaitTime {
		return "TBD"
	}
	// divide the remaining rows by how many rows we staged in the last interval per second
	remainingRows := totalRows - copiedRows
	remainingSeconds := math.Floor(float64(remainingRows) / float64(rowsPerSecond))

	estimate := time.Duration(remainingSeconds * float64(time.Second))

	return estimate.String()
}
