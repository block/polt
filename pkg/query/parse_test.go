package query

import (
	"testing"

	"github.com/block/polt/pkg/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_Parse(t *testing.T) {
	test.RunSQL(t, `DROP TABLE IF EXISTS t1_qp`)
	tbl := `CREATE TABLE t1_qp (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	test.RunSQL(t, tbl)

	// test valid query
	stmt, err := ParseSelect("SELECT * FROM t1_q WHERE name = 'harry'")
	require.NoError(t, err)
	assert.NotNil(t, stmt)

	// test non select stmt
	_, err = ParseSelect("INSERT INTO t1_qp VALUES ('harry')")
	require.ErrorIs(t, err, errNonSelectStmt)
}
