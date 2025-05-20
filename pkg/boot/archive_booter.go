package boot

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/block/polt/pkg/audit"
	"github.com/cashapp/spirit/pkg/table"
)

type ArchiveBooter struct {
	db         *sql.DB
	SrcTblName string
	runID      string
	auditDB    string
	RunsTbl    *table.TableInfo
	CheckPtTbl *table.TableInfo
	database   string
	SrcTbl     *table.TableInfo
}

type ArchiveBooterConfig struct {
	DB       *sql.DB
	AuditDB  string
	RunID    string
	SrcTbl   string
	Database string
}

func NewArchiveBooter(abc *ArchiveBooterConfig) *ArchiveBooter {
	return &ArchiveBooter{
		db:         abc.DB,
		SrcTblName: abc.SrcTbl,
		runID:      abc.RunID,
		auditDB:    abc.AuditDB,
		database:   abc.Database,
	}
}

func (ab *ArchiveBooter) PreflightChecks(_ context.Context) error {
	if !isMySQLVersionCompatible(ab.db) {
		return errors.New("MySQL 8.0 is required")
	}

	return nil
}

func (ab *ArchiveBooter) PostSetupChecks(ctx context.Context) error {
	// Get StageTbl Info
	ab.SrcTbl = table.NewTableInfo(ab.db, ab.database, ab.SrcTblName)
	if err := ab.SrcTbl.SetInfo(ctx); err != nil {
		return fmt.Errorf("failed to set info on staging table: %w", err)
	}
	// Check that src table name matches the staging table naming convention
	if !strings.Contains(ab.SrcTbl.TableName, "_stage_") {
		return fmt.Errorf("%s is not a valid staging table to archive", ab.SrcTbl.TableName)
	}

	return nil
}

func (ab *ArchiveBooter) Setup(ctx context.Context) error {
	// Create audit db and checkpoint, runs tables in it.
	if err := audit.CreateDB(ctx, ab.db, ab.auditDB); err != nil {
		return err
	}
	if err := audit.CreateRunsTbl(ctx, ab.db, ab.auditDB); err != nil {
		return err
	}

	return audit.CreateCheckpointTbl(ctx, ab.db, ab.auditDB, ab.runID)
}
