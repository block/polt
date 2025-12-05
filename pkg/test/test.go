package test

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

func DSN() string {
	dsn := os.Getenv("MYSQL_DSN")
	if dsn == "" {
		return "msandbox:msandbox@tcp(127.0.0.1:8030)/test"
	}

	return dsn
}
func ReplicaDSN() string {
	return os.Getenv("REPLICA_DSN")
}

func RunSQL(t *testing.T, stmt string) {
	t.Helper()
	db, err := sql.Open("mysql", DSN())
	require.NoError(t, err)
	defer func() {
		if closeErr := db.Close(); closeErr != nil {
			t.Errorf("error closing db: %v", closeErr)
		}
	}()
	_, err = db.Exec(stmt)
	require.NoError(t, err)
}

func FindParquetFile(t *testing.T, suffix string, ensureFile bool) []string {
	t.Helper()
	matches, err := filepath.Glob("*" + suffix)
	if err != nil {
		panic(err)
	}
	if ensureFile {
		require.NotEmpty(t, matches)
	}
	if len(matches) > 0 {
		return matches
	}

	return []string{}
}

func DeleteFile(t *testing.T, filePath string) {
	t.Helper()
	if _, err := os.Stat(filePath); err == nil {
		err = os.Remove(filePath)
		require.NoError(t, err)
	} else if !os.IsNotExist(err) {
		t.Error("Error checking file existence", err)
	}
}

func DeleteAllParquetFiles() {
	matches, err := filepath.Glob("*.parquet")
	if err != nil {
		panic(err)
	}
	for _, match := range matches {
		_ = os.Remove(match) // Ignore error during cleanup
	}
}

func SetupDB(cfg *mysql.Config, threads int, seconds int) (*sql.DB, error) {
	dbConfig := dbconn.NewDBConfig()
	// Extra +1 for dedicated transaction to get advisory lock on table name
	// that will be open for the entirety of the archive.
	dbConfig.MaxOpenConnections = threads + 1
	dbConfig.LockWaitTimeout = seconds

	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s", cfg.User, cfg.Passwd, cfg.Addr, cfg.DBName)
	db, err := dbconn.New(dsn, dbConfig)
	if err != nil {
		return nil, fmt.Errorf("error connecting to database: %s as user: %s, err:%w", cfg.DBName, cfg.User, err)
	}

	return db, nil
}

func TableExists(t *testing.T, schema, table string, db *sql.DB) bool {
	t.Helper()
	var count int
	query := "SELECT COUNT(TABLE_NAME) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?;"

	_ = db.QueryRowContext(context.Background(), query, schema, table).Scan(&count)

	return count > 0
}

func LockExists(t *testing.T, db *sql.DB, tableName string) bool {
	t.Helper()
	var lock *dbconn.MetadataLock
	defer func() {
		// Release the lock
		if lock != nil {
			if closeErr := lock.Close(); closeErr != nil {
				t.Errorf("error closing lock: %v", closeErr)
			}
		}
	}()
	srcTbl := table.NewTableInfo(db, "test", tableName)
	err := srcTbl.SetInfo(context.Background())
	require.NoError(t, err)

	// Try to get advisory lock on the table, if there is an error then the lock already exists
	dbConfig := dbconn.NewDBConfig()
	logger := slog.Default()
	lock, err = dbconn.NewMetadataLock(context.Background(), DSN(), []*table.TableInfo{srcTbl}, dbConfig, logger)

	return err != nil
}

func GetCount(t *testing.T, db *sql.DB, tableName string, where string) int {
	t.Helper()
	var count int
	err := db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s", tableName, where)).Scan(&count)
	require.NoError(t, err)

	return count
}
