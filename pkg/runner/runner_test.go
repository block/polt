package runner

import (
	"context"
	"database/sql"
	"testing"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/squareup/polt/pkg/audit"
	"github.com/squareup/polt/pkg/test"
	"github.com/stretchr/testify/require"
)

func TestRunEntry(t *testing.T) {
	// Create a new run entry
	// Check that the run entry is created correctly
	test.RunSQL(t, "DROP TABLE IF EXISTS polt.runs")
	db, err := sql.Open("mysql", test.DSN())
	require.NoError(t, err)
	defer db.Close()

	require.NoError(t, err)
	auditDB := "polt"
	err = audit.CreateRunsTbl(context.Background(), db, auditDB)
	require.NoError(t, err)

	// Create 3 run entries
	err = createRunEntry(context.Background(), &Run{
		ID:      "run1",
		mode:    stageMode,
		db:      db,
		auditDB: auditDB,
	})
	require.NoError(t, err)
	err = createRunEntry(context.Background(), &Run{
		ID:      "run1",
		mode:    archiveMode,
		db:      db,
		auditDB: auditDB,
	})
	require.NoError(t, err)
	err = createRunEntry(context.Background(), &Run{
		ID:      "run2",
		mode:    stageMode,
		db:      db,
		auditDB: auditDB,
	})
	require.NoError(t, err)

	// Get the run entry for stage mode for `run1`
	status, err := getRunEntryStatus(context.Background(), &Run{
		ID:      "run1",
		mode:    stageMode,
		db:      db,
		auditDB: auditDB,
	})
	require.NoError(t, err)
	require.Equal(t, Started.String(), status)

	// Get the run entry for non-existent run, should return empty string
	status, err = getRunEntryStatus(context.Background(), &Run{
		ID:      "non-existent",
		mode:    stageMode,
		db:      db,
		auditDB: auditDB,
	})
	require.NoError(t, err)
	require.Equal(t, "", status)

	// Update the run entry for `run1` to `running`
	err = setRunEntryStatus(context.Background(), &Run{
		ID:      "run1",
		mode:    stageMode,
		db:      db,
		auditDB: auditDB,
		status:  Running.String(),
	})
	require.NoError(t, err)

	// Get the run entry for stage mode for `run1`
	status, err = getRunEntryStatus(context.Background(), &Run{
		ID:      "run1",
		mode:    stageMode,
		db:      db,
		auditDB: auditDB,
	})
	require.NoError(t, err)
	require.Equal(t, Running.String(), status)

	// Update the run entry for `run1` to `failed`
	err = setRunEntryStatus(context.Background(), &Run{
		ID:      "run1",
		mode:    stageMode,
		db:      db,
		auditDB: auditDB,
		status:  Failed.String(),
	})
	require.NoError(t, err)

	// Restart the failed run with a new try number
	tryNum, err := restartRunWithNewTryNum(context.Background(), &Run{
		ID:      "run1",
		mode:    stageMode,
		db:      db,
		auditDB: auditDB,
	})
	require.NoError(t, err)
	require.Equal(t, 2, tryNum)
	status, err = getRunEntryStatus(context.Background(), &Run{
		ID:      "run1",
		mode:    stageMode,
		db:      db,
		auditDB: auditDB,
	})
	require.NoError(t, err)
	require.Equal(t, Started.String(), status)

	// Update the run entry for `run1` to `succeeded`
	err = setRunEntryStatus(context.Background(), &Run{
		ID:      "run1",
		mode:    stageMode,
		db:      db,
		auditDB: auditDB,
		status:  Succeeded.String(),
	})
	require.NoError(t, err)

	// Test that checkIfSuccessfullyRan returns true for a successful run
	ran, err := checkIfSuccessfullyRan(context.Background(), &Run{
		ID:      "run1",
		mode:    stageMode,
		db:      db,
		auditDB: auditDB,
	})
	require.NoError(t, err)
	require.True(t, ran)

	// Test that checkIfSuccessfullyRan returns false for a run
	// with different mode (archive) than the one that was successful
	ran, err = checkIfSuccessfullyRan(context.Background(), &Run{
		ID:      "run1",
		mode:    archiveMode,
		db:      db,
		auditDB: auditDB,
	})
	require.NoError(t, err)
	require.False(t, ran)

	// Update the run entry for `run1` and archive mode to `failed`
	err = setRunEntryStatus(context.Background(), &Run{
		ID:      "run1",
		mode:    archiveMode,
		db:      db,
		auditDB: auditDB,
		status:  Failed.String(),
	})
	require.NoError(t, err)

	// Restart the failed run with a new try number
	tryNum, err = restartRunWithNewTryNum(context.Background(), &Run{
		ID:      "run1",
		mode:    archiveMode,
		db:      db,
		auditDB: auditDB,
	})
	require.NoError(t, err)
	require.Equal(t, 2, tryNum)

	// Test that checkIfSuccessfullyRan returns false for a non existen run
	ran, err = checkIfSuccessfullyRan(context.Background(), &Run{
		ID:      "non-existent",
		mode:    stageMode,
		db:      db,
		auditDB: auditDB,
	})
	require.NoError(t, err)
	require.False(t, ran)

	// Test that checkIfSuccessfullyRan returns false for a run that is not successful
	ran, err = checkIfSuccessfullyRan(context.Background(), &Run{
		ID:      "run2",
		mode:    stageMode,
		db:      db,
		auditDB: auditDB,
	})
	require.NoError(t, err)
	require.False(t, ran)

	// Test that runTblExists returns true for a table that exists
	exists, err := runTblExists(context.Background(), auditDB, db)
	require.NoError(t, err)
	require.True(t, exists)

	// Drop the runs table and test that runTblExists returns false
	err = dbconn.Exec(context.Background(), db, "DROP TABLE polt.runs")
	require.NoError(t, err)
	exists, err = runTblExists(context.Background(), auditDB, db)
	require.NoError(t, err)
	require.False(t, exists)
}
