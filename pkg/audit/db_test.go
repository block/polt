package audit

import (
	"context"
	"database/sql"
	"testing"

	"github.com/block/polt/pkg/test"
	"github.com/stretchr/testify/require"
)

func TestCreateAuditDB(t *testing.T) {
	// Create a new audit database
	// Check that the audit database is created correctly
	db, err := sql.Open("mysql", test.DSN())
	require.NoError(t, err)
	defer func() {
		if closeErr := db.Close(); closeErr != nil {
			t.Errorf("error closing db: %v", closeErr)
		}
	}()
	err = CreateDB(context.Background(), db, "auditDB")
	require.NoError(t, err)
	var schemaName string
	err = db.QueryRowContext(context.Background(), "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = ?", "auditDB").Scan(&schemaName)
	require.NoError(t, err)
	require.Equal(t, "auditDB", schemaName)
}

func TestCreateCheckpointTbl(t *testing.T) {
	// Create a new checkpoint table
	// Check that the checkpoint table is created correctly
	db, err := sql.Open("mysql", test.DSN())
	require.NoError(t, err)
	defer func() {
		if closeErr := db.Close(); closeErr != nil {
			t.Errorf("error closing db: %v", closeErr)
		}
	}()
	err = CreateDB(context.Background(), db, "auditDB")
	require.NoError(t, err)
	err = CreateCheckpointTbl(context.Background(), db, "auditDB", "runid")
	require.NoError(t, err)
	var tableName string
	err = db.QueryRowContext(context.Background(), "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ? AND TABLE_SCHEMA = ?", "checkpoints_runid", "auditDB").Scan(&tableName)
	require.NoError(t, err)
	require.Equal(t, "checkpoints_runid", tableName)
}

func TestCreateRunsTbl(t *testing.T) {
	// Create a new runs table
	// Check that the runs table is created correctly
	db, err := sql.Open("mysql", test.DSN())
	require.NoError(t, err)
	defer func() {
		if closeErr := db.Close(); closeErr != nil {
			t.Errorf("error closing db: %v", closeErr)
		}
	}()
	err = CreateDB(context.Background(), db, "auditDB")
	require.NoError(t, err)
	err = CreateRunsTbl(context.Background(), db, "auditDB")
	require.NoError(t, err)
	var tableName string
	err = db.QueryRowContext(context.Background(), "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ? AND TABLE_SCHEMA = ?", "runs", "auditDB").Scan(&tableName)
	require.NoError(t, err)
	require.Equal(t, "runs", tableName)
}
