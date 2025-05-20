package boot

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"testing"

	"github.com/block/polt/pkg/query"
	"github.com/block/polt/pkg/test"
	"github.com/cashapp/spirit/pkg/table"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStageBooter_Setup(t *testing.T) {
	test.RunSQL(t, `DROP TABLE IF EXISTS t1_sb`)
	test.RunSQL(t, `DROP TABLE IF EXISTS _t1_sb_stage_runid`)

	tbl := `CREATE TABLE t1_sb (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	test.RunSQL(t, tbl)
	cfg, err := mysql.ParseDSN(test.DSN())
	require.NoError(t, err)

	db, err := sql.Open("mysql", test.DSN())
	require.NoError(t, err)
	srcTbl := table.NewTableInfo(db, cfg.DBName, "t1_sb")
	err = srcTbl.SetInfo(context.Background())
	require.NoError(t, err)
	s := NewStageBooter(&StageBooterConfig{AuditDB: "polt", Query: "SELECT * from t1_sb WHERE name = 'harry'", RunID: "runid", DB: db, SrcTbl: srcTbl})

	err = s.Setup(context.Background())
	require.NoError(t, err)

	// Test that checkpoint, runs and stage tables are created
	assert.True(t, test.TableExists(t, "polt", "runs", db))
	assert.True(t, test.TableExists(t, "polt", "checkpoints_runid", db))
	assert.True(t, test.TableExists(t, "test", "_t1_sb_stage_runid", db))
}

func TestStageBooter_PreflightChecks(t *testing.T) {
	test.RunSQL(t, `DROP TABLE IF EXISTS t1_sb`)
	test.RunSQL(t, `DROP TABLE IF EXISTS _t1_sb_stage_runid`)

	tbl := `CREATE TABLE t1_sb (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		age int(11) NOT NULL,
		KEY name_idx (name),
		PRIMARY KEY (id)
	)`
	test.RunSQL(t, tbl)
	cfg, err := mysql.ParseDSN(test.DSN())
	require.NoError(t, err)

	db, err := sql.Open("mysql", test.DSN())
	require.NoError(t, err)
	srcTbl := table.NewTableInfo(db, cfg.DBName, "t1_sb")
	err = srcTbl.SetInfo(context.Background())
	require.NoError(t, err)
	s := NewStageBooter(&StageBooterConfig{AuditDB: "polt", Query: "SELECT * from t1_sb WHERE name = 'harry'", RunID: "runid", DB: db, SrcTbl: srcTbl})
	// PreflightChecks returns no error for valid query.
	require.NoError(t, s.PreflightChecks(context.Background()))

	s = NewStageBooter(&StageBooterConfig{AuditDB: "polt", Query: "SELECT * FROM t1_sb WHERE age > 100", RunID: "runid", DB: db, SrcTbl: srcTbl})

	// PreflightChecks returns  error for query without index.
	require.ErrorIs(t, s.PreflightChecks(context.Background()), query.ErrNoIndexAvb)
}

func TestStageBooter_PreflightChecks_ReplicaHealth(t *testing.T) {
	if test.ReplicaDSN() == "" {
		t.Skip("replicaDSN is not set")
	}
	test.RunSQL(t, `DROP TABLE IF EXISTS t1_sb`)
	test.RunSQL(t, `DROP TABLE IF EXISTS _t1_sb_stage_runid`)

	tbl := `CREATE TABLE t1_sb (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		age int(11) NOT NULL,
		KEY name_idx (name),
		PRIMARY KEY (id)
	)`
	test.RunSQL(t, tbl)
	cfg, err := mysql.ParseDSN(test.DSN())
	require.NoError(t, err)

	db, err := sql.Open("mysql", test.DSN())
	require.NoError(t, err)
	replicadb, err := sql.Open("mysql", test.ReplicaDSN())
	require.NoError(t, err)
	srcTbl := table.NewTableInfo(db, cfg.DBName, "t1_sb")
	err = srcTbl.SetInfo(context.Background())
	require.NoError(t, err)
	s := NewStageBooter(&StageBooterConfig{AuditDB: "polt", Query: "SELECT * from t1_sb WHERE name = 'harry'", RunID: "runid", DB: db, SrcTbl: srcTbl, Replica: replicadb})
	// PreflightChecks returns no error for valid replica.
	require.NoError(t, s.PreflightChecks(context.Background()))

	// use a completely invalid DSN.
	// golang sql.Open lazy loads, so this is possible.
	replicadb, err = sql.Open("mysql", "msandbox:msandbox@tcp(127.0.0.1:22)/test")
	require.NoError(t, err)
	s = NewStageBooter(&StageBooterConfig{AuditDB: "polt", Query: "SELECT * from t1_sb WHERE name = 'harry'", RunID: "runid", DB: db, SrcTbl: srcTbl, Replica: replicadb})

	// PreflightChecks returns  error for invalid replica.
	require.ErrorContains(t, s.PreflightChecks(context.Background()), "connection refused")
}

func TestStageBooter_PreflightChecks_PreEvalQuery(t *testing.T) {
	test.RunSQL(t, `DROP TABLE IF EXISTS t1_sb`)
	test.RunSQL(t, `DROP TABLE IF EXISTS _t1_sb_stage_runid`)

	tbl := `CREATE TABLE t1_sb (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		age int(11) NOT NULL,
		created_at timestamp,
		update_at timestamp,
		KEY name_idx (name),
		PRIMARY KEY (id)
	)`
	test.RunSQL(t, tbl)
	cfg, err := mysql.ParseDSN(test.DSN())
	require.NoError(t, err)

	// For the purpose of this test SET TIMESTAMP = 1696911031 and SET TIME_ZONE='UTC',
	// which returns the fixed value for NOW() as '2023-10-10 04:10:31'
	timeZone := fmt.Sprintf("%s=%s", "time_zone", url.QueryEscape(`"+00:00"`))
	db, err := sql.Open("mysql", test.DSN()+"?timestamp=1696911031&"+timeZone)
	require.NoError(t, err)

	srcTbl := table.NewTableInfo(db, cfg.DBName, "t1_sb")
	err = srcTbl.SetInfo(context.Background())
	require.NoError(t, err)
	s := NewStageBooter(&StageBooterConfig{AuditDB: "polt", Query: "SELECT * FROM t1_sb WHERE name = 'harry' AND created_at < NOW() - INTERVAL 30 day", RunID: "runid", DB: db, SrcTbl: srcTbl})
	require.NoError(t, s.PreflightChecks(context.Background()))

	assert.Equal(t, "SELECT * FROM `t1_sb` WHERE `name`=_UTF8MB4'harry' AND `created_at`<DATE_SUB('2023-10-10 04:10:31', INTERVAL 30 DAY)", s.FoldedQuery)
}
