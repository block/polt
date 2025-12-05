package archive

import (
	"context"
	"database/sql"
	"testing"

	"github.com/block/spirit/pkg/table"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"github.com/block/polt/pkg/test"
	"github.com/stretchr/testify/assert"
)

func TestTableArchiver_Move(t *testing.T) {
	test.RunSQL(t, `DROP TABLE IF EXISTS _t1_ia_stage_runid`)
	test.RunSQL(t, `DROP TABLE IF EXISTS _t1_ia_archived`)

	tbl := `CREATE TABLE _t1_ia_stage_runid (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		idxed_column int(11) NOT NULL,
		try_num int NOT NULL,
		INDEX(idxed_column),
		PRIMARY KEY (id)
	)`
	test.RunSQL(t, tbl)
	test.RunSQL(t, `INSERT INTO _t1_ia_stage_runid (name, idxed_column, try_num) SELECT REPEAT('a', 100), FLOOR(RAND()*(30-20+1))+20, 1 FROM dual`)
	test.RunSQL(t, `INSERT INTO _t1_ia_stage_runid (name, idxed_column, try_num) SELECT REPEAT('a', 100), FLOOR(RAND()*(30-20+1))+20, 1 FROM _t1_ia_stage_runid`)

	cfg, err := mysql.ParseDSN(test.DSN())
	require.NoError(t, err)
	db, err := sql.Open("mysql", test.DSN())
	require.NoError(t, err)

	stagedCount := test.GetCount(t, db, "_t1_ia_stage_runid", "1=1")
	stgTbl := table.NewTableInfo(db, cfg.DBName, "_t1_ia_stage_runid")
	err = stgTbl.SetInfo(context.Background())

	require.NoError(t, err)
	tblArchiver := &Table{db: db, stgTbl: stgTbl, dstTable: "_t1_ia_archived"}
	err = tblArchiver.Move(context.Background(), false)
	require.NoError(t, err)

	// Test that final destination have same number of rows as source/staged table.
	assert.Equal(t, stagedCount, test.GetCount(t, db, "_t1_ia_archived", "1=1"))

	// Test that move works even if try_num column is not present in the table.
	// This could happen if archive failed after try_num column was dropped and
	// before renaming the staged table to final destination and it was retried.
	test.RunSQL(t, `DROP TABLE _t1_ia_archived`)
	test.RunSQL(t, tbl)
	test.RunSQL(t, "ALTER TABLE _t1_ia_stage_runid DROP COLUMN try_num")
	stgTbl1 := table.NewTableInfo(db, cfg.DBName, "_t1_ia_stage_runid")
	err = stgTbl1.SetInfo(context.Background())
	require.NoError(t, err)
	tblArchiver1 := &Table{db: db, stgTbl: stgTbl1, dstTable: "_t1_ia_archived"}
	err = tblArchiver1.Move(context.Background(), false)
	require.NoError(t, err)
}
