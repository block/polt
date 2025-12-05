package archive

import (
	"context"
	"database/sql"
	"fmt"
	"slices"

	"github.com/block/polt/pkg/boot"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
)

type Table struct {
	db       *sql.DB
	stgTbl   *table.TableInfo
	dstTable string
}

func (t *Table) IsResumable() bool {
	return false
}

type TblArchiverConfig struct {
	DB     *sql.DB
	StgTbl *table.TableInfo
	DstTbl string
}

func NewTblArchiver(iac *TblArchiverConfig) Archiver {
	return &Table{
		db:       iac.DB,
		stgTbl:   iac.StgTbl,
		dstTable: iac.DstTbl,
	}
}

// Move moves the rows from the staging table to the destination table.
func (t *Table) Move(ctx context.Context, _ bool) error {
	var err error
	if slices.Contains(t.stgTbl.Columns, boot.TryNumColName) {
		dropColumnQuery := fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", t.stgTbl.TableName, boot.TryNumColName)
		err = dbconn.Exec(ctx, t.db, dropColumnQuery)
		if err != nil {
			return err
		}
	}
	renameQuery := fmt.Sprintf("RENAME TABLE %s TO %s", t.stgTbl.TableName, t.dstTable)

	return dbconn.Exec(ctx, t.db, renameQuery)
}
