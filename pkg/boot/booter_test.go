package boot

import (
	"testing"

	"github.com/block/polt/pkg/test"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

func TestIsMySQLVersionCompatible(t *testing.T) {
	cfg, err := mysql.ParseDSN(test.DSN())
	require.NoError(t, err)

	db, err := test.SetupDB(cfg, 16, 30)
	require.NoError(t, err)

	// Test case: MySQL version is compatible
	isCompatible := isMySQLVersionCompatible(db)
	if !isCompatible {
		t.Errorf("Expected isMySQLVersionCompatible to return true, but got false")
	}
}
