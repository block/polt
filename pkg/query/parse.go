package query

import (
	"errors"
	"fmt"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
)

var errNonSelectStmt = errors.New("is not a SELECT statement type")

// ParseSelect parses the given SQL query as a SELECT statement and returns
// AST (Abstract syntax tree) node if there is no error.
func ParseSelect(query string) (*ast.SelectStmt, error) {
	p := parser.New()
	p.SetSQLMode(mysql.ModeStrictAllTables)

	nodes, err := p.ParseOneStmt(query, "", "")
	if err != nil {
		return nil, fmt.Errorf("given query: %s is invalid", query)
	}

	if nodes != nil {
		stmt, ok := nodes.(*ast.SelectStmt)
		if !ok {
			return nil, fmt.Errorf("query: %s %w", query, errNonSelectStmt)
		}

		return stmt, nil
	}

	return nil, fmt.Errorf("given query: %s is invalid", query)
}
