package query

import (
	"bytes"
	"database/sql"
	"fmt"

	_ "github.com/pingcap/tidb/pkg/parser/test_driver" // required for the tidb parser
)

// Validate validates the query and returns the where clause if it's valid.
func Validate(query string, db *sql.DB) (string, error) {
	stmt, err := ParseSelect(query)
	if err != nil {
		return "", err
	}

	// Test that query is valid by EXPLAINing it
	explainQuery := "EXPLAIN " + query
	_, err = db.Exec(explainQuery)
	if err != nil {
		return "", fmt.Errorf("could not EXPLAIN query: %w", err)
	}

	// Extract the where clause string
	whereStr := bytes.NewBufferString("")
	stmt.Where.Format(whereStr)

	return whereStr.String(), nil
}
