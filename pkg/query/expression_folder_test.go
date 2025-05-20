package query

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"testing"

	"github.com/block/polt/pkg/test"
	"github.com/cashapp/spirit/pkg/table"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_PreEvaluate(t *testing.T) {
	test.RunSQL(t, `DROP TABLE IF EXISTS t1_evl`)
	tbl := `CREATE TABLE t1_evl (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		created_at timestamp,
		update_at timestamp,
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

	srcTbl := table.NewTableInfo(db, cfg.DBName, "t1_q")
	err = srcTbl.SetInfo(context.Background())
	require.NoError(t, err)

	// Test that occurrences of NOW() is replaced with '2023-10-10 04:10:31'
	fns, err := FoldExpression(context.Background(), "SELECT * FROM t1_evl WHERE created_at <  NOW() - INTERVAL 30 day and name = 'harry' OR updated_at < NOW() - INTERVAL 90 day", db)
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM `t1_evl` WHERE `created_at`<DATE_SUB('2023-10-10 04:10:31', INTERVAL 30 DAY) AND `name`=_UTF8MB4'harry' OR `updated_at`<DATE_SUB('2023-10-10 04:10:31', INTERVAL 90 DAY)", fns)

	// Test that query containing unfoldable function throws error.
	fns, err = FoldExpression(context.Background(), "SELECT * FROM t1_evl where name >  UUID()", db)
	require.ErrorContains(t, err, "found uuid function which isn't foldable")
	assert.Equal(t, "", fns)
}
