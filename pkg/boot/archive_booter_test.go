package boot

import (
	"context"
	"database/sql"
	"testing"

	"github.com/block/polt/pkg/test"
	"github.com/cashapp/spirit/pkg/table"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestArchiveBooter_Setup(t *testing.T) {
	test.RunSQL(t, `DROP TABLE IF EXISTS _t1_sb_stage_abid`)

	tbl := `CREATE TABLE _t1_sb_stage_abid (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	test.RunSQL(t, tbl)
	cfg, err := mysql.ParseDSN(test.DSN())
	require.NoError(t, err)

	db, err := sql.Open("mysql", test.DSN())
	require.NoError(t, err)
	srcTbl := table.NewTableInfo(db, cfg.DBName, "_t1_sb_stage_abid")
	err = srcTbl.SetInfo(context.Background())
	require.NoError(t, err)
	s := NewArchiveBooter(&ArchiveBooterConfig{AuditDB: "polt", RunID: "abid", DB: db, SrcTbl: srcTbl.TableName})

	err = s.Setup(context.Background())
	require.NoError(t, err)

	// Test that checkpoint, runs tables are created
	assert.True(t, test.TableExists(t, "polt", "runs", db))
	assert.True(t, test.TableExists(t, "polt", "checkpoints_abid", db))
}

func TestArchiveBooter_PostSetupChecks(t *testing.T) {
	test.RunSQL(t, `DROP TABLE IF EXISTS t1_ab`)
	test.RunSQL(t, `DROP TABLE IF EXISTS _t1_ab_stage_runid`)

	tbl := `CREATE TABLE t1_ab (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		age int(11) NOT NULL,
		KEY name_idx (name),
		PRIMARY KEY (id)
	)`

	tbl2 := `CREATE TABLE _t1_ab_stage_runid (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		age int(11) NOT NULL,
		KEY name_idx (name),
		PRIMARY KEY (id)
	)`
	test.RunSQL(t, tbl)
	test.RunSQL(t, tbl2)
	cfg, err := mysql.ParseDSN(test.DSN())
	require.NoError(t, err)

	db, err := sql.Open("mysql", test.DSN())
	require.NoError(t, err)
	srcTbl := table.NewTableInfo(db, cfg.DBName, "t1_ab")
	err = srcTbl.SetInfo(context.Background())
	require.NoError(t, err)
	s := NewArchiveBooter(&ArchiveBooterConfig{AuditDB: "polt", RunID: "runid", DB: db, SrcTbl: srcTbl.TableName, Database: cfg.DBName})
	// PostSetupChecks returns  error for invalid table.
	require.ErrorContains(t, s.PostSetupChecks(context.Background()), "t1_ab is not a valid staging table to archive")

	stgTbl := table.NewTableInfo(db, cfg.DBName, "_t1_ab_stage_runid")
	err = srcTbl.SetInfo(context.Background())
	require.NoError(t, err)
	s = NewArchiveBooter(&ArchiveBooterConfig{AuditDB: "polt", RunID: "runid", DB: db, SrcTbl: stgTbl.TableName, Database: cfg.DBName})

	// PostSetupChecks returns no error for stage table.
	require.NoError(t, s.PostSetupChecks(context.Background()))
}
