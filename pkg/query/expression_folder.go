package query

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
)

// foldableFns is copied from https://github.com/pingcap/tidb/blob/41ba7bfff37703ac8857bc01f5bb82eb8bf3771b/expression/function_traits.go#L138
var foldableFns = []string{
	ast.Now,
	ast.RandomBytes,
	ast.CurrentTimestamp,
	ast.UTCTime,
	ast.Curtime,
	ast.CurrentTime,
	ast.UTCTimestamp,
	ast.UnixTimestamp,
	ast.Curdate,
	ast.CurrentDate,
	ast.UTCDate,
}

// unfoldableFns is copied from https://github.com/pingcap/tidb/blob/ec2731b8f53993987b756ecda000789a364c5064/expression/function_traits.go#L49
var unfoldableFns = []string{
	ast.Sysdate,
	ast.FoundRows,
	ast.Rand,
	ast.UUID,
	ast.Sleep,
	ast.RowFunc,
	ast.Values,
	ast.SetVar,
	ast.GetVar,
	ast.GetParam,
	ast.Benchmark,
	ast.DayName,
	ast.NextVal,
	ast.LastVal,
	ast.SetVal,
	ast.AnyValue,
}

// FoldableFnX extracts foldableFns from AST node of a parsed SQL query.
type FoldableFnX struct {
	foldedFns map[string]bool
	err       error
}

func (v *FoldableFnX) Enter(in ast.Node) (ast.Node, bool) {
	if name, ok := in.(*ast.FuncCallExpr); ok {
		fnName := strings.ToLower(name.FnName.String())
		var foldFn string
		var err error

		if foldFn, err = restoreString(in); err != nil {
			v.err = err
		}
		if slices.Contains(foldableFns, fnName) {
			v.foldedFns[foldFn] = true
		} else if slices.Contains(unfoldableFns, fnName) {
			v.err = fmt.Errorf("found %s function which isn't foldable", fnName)
			// Return early and no need to keep visiting child AST nodes.
			return in, true
		}

		return in, false
	}

	return in, false
}

func (v *FoldableFnX) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}

func restoreString(in ast.Node) (string, error) {
	var sb strings.Builder
	rctx := &format.RestoreCtx{Flags: format.DefaultRestoreFlags, In: &sb, DefaultDB: ""}
	if err := in.Restore(rctx); err != nil {
		return "", fmt.Errorf("error restoring the node %v", in)
	}

	return sb.String(), nil
}

// FoldExpression returns modified query by replacing the occurrences of
// non-deterministic functions with deterministic values in the query.
func FoldExpression(ctx context.Context, query string, db *sql.DB) (string, error) {
	stmt, err := ParseSelect(query)
	if err != nil {
		return "", err
	}
	foldedX := &FoldableFnX{foldedFns: make(map[string]bool, 0)}

	stmt.Accept(foldedX)

	if foldedX.err != nil {
		return "", foldedX.err
	}

	// Return original query early if there are no occurrences of deferred functions
	if len(foldedX.foldedFns) == 0 {
		return query, nil
	}

	var preEvalQuery string
	if preEvalQuery, err = restoreString(stmt); err != nil {
		return "", err
	}

	for k := range foldedX.foldedFns {
		var evalFnValue string
		err := db.QueryRowContext(ctx, "SELECT "+k).Scan(&evalFnValue)
		if err != nil {
			return "", err
		}
		preEvalQuery = strings.ReplaceAll(preEvalQuery, k, fmt.Sprintf("'%s'", evalFnValue))
	}

	return preEvalQuery, nil
}
