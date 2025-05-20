package boot

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/block/polt/pkg/audit"
	"github.com/block/polt/pkg/query"
	"github.com/cashapp/spirit/pkg/dbconn"
	"github.com/cashapp/spirit/pkg/table"
)

const MaxTableLen = 64

type StageBooter struct {
	db      *sql.DB
	auditDB string
	runID   string
	query   string

	SrcTbl                   *table.TableInfo
	StageTbl                 *table.TableInfo
	CheckPtTbl               *table.TableInfo
	RunsTbl                  *table.TableInfo
	IndexForChunker          string
	WhereConditionForChunker string
	FoldedQuery              string
	replica                  *sql.DB
}

type StageBooterConfig struct {
	DB      *sql.DB
	AuditDB string
	RunID   string
	Query   string
	SrcTbl  *table.TableInfo
	Replica *sql.DB
}

func NewStageBooter(sbc *StageBooterConfig) *StageBooter {
	return &StageBooter{
		db:      sbc.DB,
		SrcTbl:  sbc.SrcTbl,
		query:   sbc.Query,
		runID:   sbc.RunID,
		auditDB: sbc.AuditDB,
		replica: sbc.Replica,
	}
}

// Setup sets up the tables required before proceeding to the actual stage phase.
func (sb *StageBooter) Setup(ctx context.Context) error {
	// Create audit db and checkpoint, runs tables in it.
	if err := audit.CreateDB(ctx, sb.db, sb.auditDB); err != nil {
		return err
	}
	if err := audit.CreateRunsTbl(ctx, sb.db, sb.auditDB); err != nil {
		return err
	}
	if err := audit.CreateCheckpointTbl(ctx, sb.db, sb.auditDB, sb.runID); err != nil {
		return err
	}

	// Create stage table
	return sb.createStageTbl(ctx)
}

func (sb *StageBooter) createStageTbl(ctx context.Context) error {
	stageTable := audit.StgTblName(sb.SrcTbl.TableName, sb.runID)
	if len(stageTable) > MaxTableLen {
		return fmt.Errorf("table name is too long: '%s'. new table name will exceed 64 characters", sb.SrcTbl.TableName)
	}

	var existingStagingTbl string
	stageTableQuery := fmt.Sprintf("SHOW TABLES LIKE '%s'", stageTable)
	err := sb.db.QueryRowContext(ctx, stageTableQuery).Scan(&existingStagingTbl)

	if err != nil && err != sql.ErrNoRows {
		return err
	}
	// Create only if the table doesn't exist
	if err == sql.ErrNoRows {
		err := dbconn.Exec(ctx, sb.db, "CREATE TABLE IF NOT EXISTS %n.%n LIKE %n", sb.SrcTbl.SchemaName, stageTable, sb.SrcTbl.TableName)
		if err != nil {
			return err
		}

		// Add extra column try_num to keep track of which try number of a particular run the row was staged (eg: when resuming from an error).
		// This helps to limit the checksum to only the rows that were copied from source table to staging table , making the checksum idempotent.
		err = dbconn.Exec(ctx, sb.db, "ALTER TABLE %n ADD COLUMN try_num INT NOT NULL", stageTable)
		if err != nil {
			return err
		}
	}
	sb.StageTbl = table.NewTableInfo(sb.db, sb.SrcTbl.SchemaName, stageTable)

	return sb.StageTbl.SetInfo(ctx)
}

// PreflightChecks contains the checks that the booter need to pass before proceeding to the stage phase.
func (sb *StageBooter) PreflightChecks(ctx context.Context) error {
	var err error
	if !isMySQLVersionCompatible(sb.db) {
		return errors.New("MySQL 8.0 or later is required")
	}

	if err = sb.replicaisHealthy(ctx); err != nil {
		return err
	}
	sb.FoldedQuery, err = query.FoldExpression(ctx, sb.query, sb.db)
	if err != nil {
		return err
	}
	whereCondition, err := query.Validate(sb.FoldedQuery, sb.db)
	if err != nil {
		return err
	}
	sb.WhereConditionForChunker = whereCondition
	index, _, err := query.GetIndex(sb.query, sb.db)
	if err != nil {
		return err
	}
	sb.IndexForChunker = index

	return err
}

// replicaIsHealthy checks SHOW REPLICA STATUS for Yes and Yes.
// Currently duplicated from Spirit's check package.
func (sb *StageBooter) replicaisHealthy(ctx context.Context) error {
	if sb.replica == nil {
		return nil // The user is not using the replica DSN feature.
	}
	rows, err := sb.replica.QueryContext(ctx, "SHOW REPLICA STATUS") //nolint: execinquery
	if err != nil {
		return err
	}
	defer rows.Close()
	status, err := scanToMap(rows)
	if err != nil {
		return err
	}
	if status["Replica_IO_Running"].String != "Yes" || status["Replica_SQL_Running"].String != "Yes" {
		return errors.New("replica is not healthy")
	}

	return nil
}

func scanToMap(rows *sql.Rows) (map[string]sql.NullString, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	if !rows.Next() {
		err = rows.Err()
		if err != nil {
			return nil, err
		}

		return nil, nil //nolint:nilnil
	}
	values := make([]interface{}, len(columns))
	for index := range values {
		values[index] = new(sql.NullString)
	}
	err = rows.Scan(values...)
	if err != nil {
		return nil, err
	}
	result := make(map[string]sql.NullString)
	for index, columnName := range columns {
		result[columnName] = *values[index].(*sql.NullString) //nolint:forcetypeassert
	}

	return result, nil
}
