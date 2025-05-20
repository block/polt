// Package boot contains the logic for booting/setup Polt tool in stage and
// archive modes.
// stage mode: Moves data from src tbl to a staging table.
// archive mode: Moves data from staging table to final
// destination(s3, table etc)
package boot

import (
	"database/sql"
	"strconv"
)

const TryNumColName = "try_num"
const minMySQLVersion = 8

type Booter interface {
	PreflightChecks() error
	Setup() error
}

// isMySQLVersionCompatible returns true if we can positively identify this as MySQL 8 or later.
func isMySQLVersionCompatible(db *sql.DB) bool {
	var version string
	if err := db.QueryRow("select substr(version(), 1, 1)").Scan(&version); err != nil {
		return false // can't tell
	}

	intVer, err := strconv.Atoi(version)
	if err != nil {
		return false // can't tell
	}

	return intVer >= minMySQLVersion
}
