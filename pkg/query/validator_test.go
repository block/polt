package query

import (
	"context"
	"database/sql"
	"testing"

	"github.com/block/polt/pkg/test"
	"github.com/block/spirit/pkg/table"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_Validate(t *testing.T) {
	test.RunSQL(t, `DROP TABLE IF EXISTS t1_q`)
	tbl := `CREATE TABLE t1_q (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	test.RunSQL(t, tbl)
	cfg, err := mysql.ParseDSN(test.DSN())
	require.NoError(t, err)

	db, err := sql.Open("mysql", test.DSN())
	require.NoError(t, err)
	srcTbl := table.NewTableInfo(db, cfg.DBName, "t1_q")
	err = srcTbl.SetInfo(context.Background())
	require.NoError(t, err)

	// test valid query
	condition, err := Validate("SELECT * FROM t1_q WHERE name = 'harry'", db)
	require.NoError(t, err)
	assert.Equal(t, "`name` = \"harry\"", condition)

	// test non existent column
	_, err = Validate("SELECT * FROM t1_q WHERE age > 20", db)
	require.ErrorContains(t, err, "Unknown column 'age' in 'where clause'")

	// test invalid sql
	_, err = Validate("SELECT * FROM t1_q WHERE", db)
	require.ErrorContains(t, err, "given query: SELECT * FROM t1_q WHERE is invalid")

	// test non select stmt
	_, err = Validate("INSERT INTO t1_q VALUES('harry')", db)
	require.ErrorContains(t, err, "query: INSERT INTO t1_q VALUES('harry') is not a SELECT statement type")
}
