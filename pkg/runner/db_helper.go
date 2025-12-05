package runner

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/block/spirit/pkg/dbconn"
)

// DBCreds is a struct to hold database credentials that's common to both stage and archive runners.
type DBCreds struct {
	Host     string `name:"host" help:"Hostname" optional:"" default:"127.0.0.1:3306"`
	Username string `name:"username" help:"User" optional:"" default:"msandbox"`
	Password string `name:"password" help:"Password" optional:"" default:"msandbox"`
	Database string `name:"database" help:"Database" optional:"" default:"test"`
}

// DBConfig is a struct to hold database configuration that's common to both stage and archive runners.
type DBConfig struct {
	Threads         int           `name:"threads" help:"Number of concurrent threads for copy and checksum tasks" optional:"" default:"4"`
	LockWaitTimeout time.Duration `name:"lock-wait-timeout" help:"The DDL lock_wait_timeout required for checksum and cutover" optional:"" default:"30s"`
}

func setupDBConfig(cfg *DBConfig) *dbconn.DBConfig {
	dbConfig := dbconn.NewDBConfig()
	// Extra +1 for dedicated transaction to get advisory lock on table name
	// that will be open for the entirety of the archive.
	dbConfig.MaxOpenConnections = cfg.Threads + 1
	dbConfig.LockWaitTimeout = int(cfg.LockWaitTimeout.Seconds())

	return dbConfig
}

func setupDB(dsn string, dbConfig *dbconn.DBConfig) (*sql.DB, error) {
	db, err := dbconn.New(dsn, dbConfig)
	if err != nil {
		return nil, fmt.Errorf("error connecting to database err:%w", err)
	}

	return db, nil
}

func setupReplicaDB(config *dbconn.DBConfig, replicaDSN string) (*sql.DB, error) {
	if replicaDSN == "" {
		return nil, nil //nolint:nilnil
	}
	db, err := dbconn.New(replicaDSN, config)
	if err != nil {
		return nil, fmt.Errorf("error connecting to replica database: %w", err)
	}

	return db, nil
}

func dsnFromCreds(dbCreds *DBCreds) string {
	return fmt.Sprintf("%s:%s@tcp(%s)/%s", dbCreds.Username, dbCreds.Password, dbCreds.Host, dbCreds.Database)
}
