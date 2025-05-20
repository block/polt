package archive

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/block/polt/pkg/audit"
	"github.com/block/polt/pkg/boot"
	"github.com/block/polt/pkg/parquet"
	"github.com/block/polt/pkg/stage"
	"github.com/block/polt/pkg/upload"
	"github.com/cashapp/spirit/pkg/dbconn"
	"github.com/cashapp/spirit/pkg/table"
	"github.com/siddontang/loggers"
	"golang.org/x/sync/errgroup"
)

var defaultMaxRetries = 5

type File struct {
	sync.Mutex

	db               *sql.DB
	srcTbl           *table.TableInfo
	logger           loggers.Advanced
	bufferStagerOpen atomic.Bool

	CopiedRowsCount   uint64 // used for estimates: the exact number of rows staged
	CopiedChunksCount uint64
	concurrency       int
	schema            *arrow.Schema
	uploader          upload.Uploader

	chkPt *audit.Checkpoint

	bufferStager    *BufferStager
	fileSizeInBytes uint64
}

func (f *File) GetProgress() string {
	var pct = 0.0
	var totalRows uint64
	if f.bufferStagerOpen.Load() {
		totalRows = atomic.LoadUint64(&f.bufferStager.totalRows)
	}
	copiedRows := atomic.LoadUint64(&f.CopiedRowsCount)
	if totalRows > 0 {
		pct = 100 * float64(copiedRows) / float64(totalRows)
	}

	return fmt.Sprintf("%d/%d %.2f%%", copiedRows, totalRows, pct)
}

type FileConfig struct {
	DB              *sql.DB
	SrcTbl          *table.TableInfo
	Logger          loggers.Advanced
	ChkPt           *audit.Checkpoint
	Uploader        upload.Uploader
	FileSizeInBytes uint64
	Threads         int
}

func NewFileArchiver(f *FileConfig) (Archiver, error) {
	fa := &File{
		db:              f.DB,
		srcTbl:          f.SrcTbl,
		concurrency:     f.Threads,
		logger:          f.Logger,
		uploader:        f.Uploader,
		fileSizeInBytes: f.FileSizeInBytes,
		chkPt:           f.ChkPt,
	}

	return fa, nil
}

const defaultChunkDuration = 2 * time.Second

// Move moves the rows from the staging table to the destination file path.
func (f *File) Move(ctx context.Context, dryRun bool) error {
	// Drop the try_num column
	var err error
	if slices.Contains(f.srcTbl.Columns, boot.TryNumColName) {
		dropColumnQuery := fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", f.srcTbl.TableName, boot.TryNumColName)
		err = dbconn.Exec(ctx, f.db, dropColumnQuery)
		if err != nil {
			return err
		}
	}

	// Get schema from MySQL table
	f.schema, err = parquet.MysqlToArrowSchema(f.db, f.srcTbl.TableName)
	if err != nil {
		return err
	}

	f.bufferStager, err = NewBufferStager(&stage.StagerConfig{
		Threads:       f.concurrency,
		DB:            f.db,
		SrcTbl:        f.srcTbl,
		Logger:        f.logger,
		ChunkDuration: defaultChunkDuration,
	}, f.chkPt, f.schema, f.uploader, f.fileSizeInBytes)
	if err != nil {
		return err
	}
	f.bufferStagerOpen.Store(true)
	err = f.moveFromBufferToFile(ctx)
	if err != nil {
		return err
	}
	// Drop the staging table only if dryRun is false
	if !dryRun {
		err = dbconn.Exec(ctx, f.db, "DROP TABLE %n", f.srcTbl.TableName)
	}

	return err
}

func (f *File) moveFromBufferToFile(ctx context.Context) error {
	cancelCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	g, errGrpCtx := errgroup.WithContext(cancelCtx)

	// 2 goroutines: one for staging to the in-memory buffer and one for flushing from buffer to file
	// Staging itself will be done in parallel using multiple goroutines
	g.SetLimit(2)

	// Set the error explicitly for the defer to pick up
	g.Go(func() error {
		defer cancel()
		f.logger.Infof("Starting staging to the buffer")
		err := f.bufferStager.Stage(errGrpCtx)
		if err != nil {
			f.logger.Errorf("Error staging: %v", err)
		} else {
			f.logger.Infof("Staging to the buffer completed")
		}

		return err
	})

	// Start a goroutine to flush the buffer to File
	g.Go(func() error {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-errGrpCtx.Done():
				return nil
			case <-ticker.C:
				rowsWritten, err := f.bufferStager.writeBuffer.CheckAndFlushBuffer(ctx)
				if err != nil {
					return err
				}
				atomic.AddUint64(&f.CopiedRowsCount, rowsWritten)
			}
		}
	})

	err := g.Wait()
	if err != nil {
		return err
	}

	f.logger.Infof("Performing final flush from buffer to file")

	// Final flush
	rowsWritten, err := f.bufferStager.writeBuffer.Flush(ctx)
	atomic.AddUint64(&f.CopiedRowsCount, rowsWritten)
	if err != nil {
		return err
	}

	return nil
}

func (f *File) IsResumable() bool {
	return true
}

// GetLowWatermark returns the low watermark of the chunker, i.e. the lowest key that has been
// guaranteed to be written to the file.
func (f *File) GetLowWatermark() (string, error) {
	if !f.bufferStagerOpen.Load() {
		return "", errors.New("watermark not available")
	}

	return f.bufferStager.GetLowWatermark()
}

func (f *File) NumCopiedRows() uint64 {
	return atomic.LoadUint64(&f.CopiedRowsCount)
}
