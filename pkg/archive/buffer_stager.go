package archive

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/block/polt/pkg/audit"
	"github.com/block/polt/pkg/parquet"
	"github.com/block/polt/pkg/stage"
	"github.com/block/polt/pkg/upload"
	"github.com/cashapp/spirit/pkg/table"
	"github.com/siddontang/loggers"
	"golang.org/x/sync/errgroup"
)

// BufferStager is a stager that stages data from staing table into a in-memory buffer.
type BufferStager struct {
	sync.Mutex
	chunker           table.Chunker
	chunkerOpen       atomic.Bool
	schema            *arrow.Schema
	writeBuffer       *parquet.WriteBuffer
	logger            loggers.Advanced
	db                *sql.DB
	srcTbl            *table.TableInfo
	concurrency       int
	CopiedRowsCount   uint64
	totalRows         uint64
	isInvalid         bool
	StagedRowsCount   uint64
	StagedChunksCount uint64
	chunkDuration     time.Duration
}

func NewBufferStager(sconfig *stage.StagerConfig, chkPt *audit.Checkpoint, schema *arrow.Schema, uploader upload.Uploader, fileSize uint64) (*BufferStager, error) {
	var err error
	b := &BufferStager{
		db:            sconfig.DB,
		concurrency:   sconfig.Threads,
		srcTbl:        sconfig.SrcTbl,
		logger:        sconfig.Logger,
		schema:        schema,
		chunkDuration: sconfig.ChunkDuration,
	}

	var totalRows int
	err = b.db.QueryRow("SELECT COUNT(*) FROM " + b.srcTbl.QuotedName).Scan(&totalRows)
	if err != nil {
		return nil, err
	}
	atomic.StoreUint64(&b.totalRows, uint64(totalRows))

	b.chunker, err = table.NewChunker(sconfig.SrcTbl, sconfig.ChunkDuration, sconfig.Logger)
	if err != nil {
		return nil, err
	}
	if chkPt != nil {
		// Overwrite the previously attached chunker with one at a specific watermark.
		if err := b.chunker.OpenAtWatermark(chkPt.CopiedUntil, table.Datum{}); err != nil {
			return nil, fmt.Errorf("error creating chunker: %w", err)
		}
		// Success from this point on
		// Overwrite copy-rows
		b.chunkerOpen.Store(true)
		atomic.StoreUint64(&b.CopiedRowsCount, chkPt.RowsCopied)

		// Add the rows that were copied in the checkpoint to the total rows remaining
		atomic.AddUint64(&b.totalRows, chkPt.RowsCopied)
	}
	b.writeBuffer = parquet.NewWriteBuffer(schema, b.chunker, uploader, fileSize)

	return b, nil
}

func (b *BufferStager) Stage(ctx context.Context) error {
	if !b.chunkerOpen.Load() {
		if err := b.chunker.Open(); err != nil {
			return err
		}
		b.chunkerOpen.Store(true)
	}
	// TODO: Implement this
	//go b.EstimateRowsPerSecond(ctx)
	g, errGrpCtx := errgroup.WithContext(ctx)
	g.SetLimit(b.concurrency)
	for !b.chunker.IsRead() && b.IsValid(errGrpCtx) {
		g.Go(func() error {
			chunk, err := b.chunker.Next()
			if err != nil {
				if errors.Is(err, table.ErrTableIsRead) {
					return nil
				}
				b.SetInvalid(true)

				return err
			}
			if err := b.RetryableStageChunk(errGrpCtx, chunk); err != nil {
				b.SetInvalid(true)

				return err
			}

			return nil
		})
	}
	err := g.Wait()
	b.chunkerOpen.Store(false)

	return err
}

func (b *BufferStager) RetryableStageChunk(ctx context.Context, chunk *table.Chunk) error {
	var copiedRows int64
	var err error
	//b.Throttler.BlockWait()
	startTime := time.Now()

	for range defaultMaxRetries {
		copiedRows, err = b.stageChunk(ctx, chunk)
		if err == nil {
			break
		}
	}
	if err != nil {
		b.logger.Errorf("Error staging the chunk after max %d retries %v", defaultMaxRetries, err)

		return err
	}

	atomic.AddUint64(&b.StagedRowsCount, uint64(copiedRows))
	atomic.AddUint64(&b.StagedChunksCount, 1)
	chunkProcessingTime := time.Since(startTime)
	b.logger.Infof("ChunkProcessing time %v for chunk %v", chunkProcessingTime, chunk.String())
	b.writeBuffer.AddChunk(chunk, chunkProcessingTime)

	return nil
}

func (b *BufferStager) stageChunk(ctx context.Context, chunk *table.Chunk) (int64, error) {
	var err error

	copyChunkQuery := fmt.Sprintf("SELECT * FROM %s where %s LOCK IN SHARE MODE", b.srcTbl.QuotedName, chunk.String())
	b.logger.Infof("\nrunning chunk: %s, copyChunkQuery: %s", chunk.String(), copyChunkQuery)
	rows, err := b.db.QueryContext(ctx, copyChunkQuery)
	if err != nil {
		b.logger.Errorf("error retrieving rows from chunk: %s err: %w", chunk.String(), err)

		return -1, err
	}
	defer rows.Close()

	return b.loadRowsToBuffer(rows, chunk)
}

func (b *BufferStager) loadRowsToBuffer(rows *sql.Rows, chunk *table.Chunk) (int64, error) {
	var copiedRows int64
	for rows.Next() {
		err2 := b.loadRowToBuffer(rows)
		if err2 != nil {
			return copiedRows, err2
		}
		copiedRows++
	}
	if err := rows.Err(); err != nil {
		return -1, err
	}
	b.logger.Infof("Successfully copied %d rows from chunk %s", copiedRows, chunk.String())

	return copiedRows, nil
}

func (b *BufferStager) loadRowToBuffer(rows *sql.Rows) error {
	var err error
	values := make([]interface{}, b.schema.NumFields())
	valuePtrs := make([]interface{}, b.schema.NumFields())
	for i := range values {
		valuePtrs[i] = &values[i]
	}
	if err = rows.Scan(valuePtrs...); err != nil {
		return err
	}

	err = b.writeBuffer.WriteGenericRowValues(values)
	if err != nil {
		return err
	}

	return nil
}

// IsValid checks if the context is valid and the stager is not marked invalid.
// BufferStager is marked invalid if there is an error while staging a chunk.
func (b *BufferStager) IsValid(ctx context.Context) bool {
	b.Lock()
	defer b.Unlock()
	if ctx.Err() != nil {
		return false
	}

	return !b.isInvalid
}

func (b *BufferStager) SetInvalid(newVal bool) {
	b.Lock()
	defer b.Unlock()
	b.isInvalid = newVal
}

// GetLowWatermark returns the low watermark of the chunker, i.e. the lowest key that has been
// guaranteed to be written to the staging table.
func (b *BufferStager) GetLowWatermark() (string, error) {
	if !b.chunkerOpen.Load() {
		return "", errors.New("watermark not available")
	}

	return b.chunker.GetLowWatermark()
}
