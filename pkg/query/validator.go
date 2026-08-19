package query

import (
	"database/sql"
	"fmt"
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
	whereStr, err := restoreString(stmt.Where)
	if err != nil {
		return "", fmt.Errorf("could not restore WHERE clause: %w", err)
	}

	return whereStr, nil
}
