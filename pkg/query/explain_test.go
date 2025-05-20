package query

import (
	"database/sql"
	"testing"

	"github.com/block/polt/pkg/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_Explain(t *testing.T) {
	test.RunSQL(t, `DROP TABLE IF EXISTS t1_q`)
	tbl := `CREATE TABLE t1_q (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	test.RunSQL(t, tbl)

	db, err := sql.Open("mysql", test.DSN())
	require.NoError(t, err)

	query := "SELECT * FROM t1_q WHERE name = 'harry'"

	// test query without index
	index, isCovIdx, err := GetIndex(query, db)
	require.ErrorIs(t, err, ErrNoIndexAvb)
	require.False(t, isCovIdx)
	assert.Equal(t, "", index)

	// test query with index
	test.RunSQL(t, "ALTER TABLE t1_q ADD INDEX `name_idx` (`name`)")

	index, isCovIdx, err = GetIndex(query, db)
	require.NoError(t, err)
	require.True(t, isCovIdx)
	assert.Equal(t, "name_idx", index)

	// test query without covering index
	test.RunSQL(t, "ALTER TABLE t1_q ADD age int")
	query = "SELECT * FROM t1_q WHERE name = 'harry'"
	index, isCovIdx, err = GetIndex(query, db)
	require.NoError(t, err)
	require.False(t, isCovIdx)
	assert.Equal(t, "name_idx", index)

	// test query wth primary key when there are no matching rows
	query = "SELECT * FROM t1_q WHERE id = 1"
	_, _, err = GetIndex(query, db)
	require.ErrorIs(t, err, ErrNoTable)

	// test query after adding data to the table
	test.RunSQL(t, "INSERT INTO t1_q (name) VALUES ('foo')")
	index, _, err = GetIndex(query, db)
	require.NoError(t, err)
	assert.Equal(t, "PRIMARY", index)

	// test query with LIMIT
	query = "SELECT * FROM t1_q WHERE id = 1 LIMIT 1"
	index, _, err = GetIndex(query, db)
	require.NoError(t, err)
	assert.Equal(t, "PRIMARY", index)

	// test query with ORDER BY
	query = "SELECT * FROM t1_q WHERE id = 1 ORDER BY name"
	index, _, err = GetIndex(query, db)
	require.NoError(t, err)
	assert.Equal(t, "PRIMARY", index)
}
