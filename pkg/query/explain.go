package query

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
)

type explainOutput struct {
	QueryBlock queryBlock `json:"query_block"`
}
type queryBlock struct {
	// SelectID int `json:"select_id"`
	// CostInfo struct {
	//	QueryCost string `json:"query_cost"`
	// } `json:"cost_info"`
	Table             *tableKey          `json:"table"`
	OrderingOperation *orderingOperation `json:"ordering_operation"`
	Message           string             `json:"message"`
}
type orderingOperation struct {
	Table *tableKey `json:"table"`
}
type tableKey struct {
	TableName    string   `json:"table_name"`
	AccessType   string   `json:"access_type"`
	PossibleKeys []string `json:"possible_keys"`
	Key          string   `json:"key"`
	UsedKeyParts []string `json:"used_key_parts"`
	UsingIndex   bool     `json:"using_index"`
	// KeyLength           string   `json:"key_length"`
	// RowsExaminedPerScan int      `json:"rows_examined_per_scan"`
	// RowsProducedPerJoin int      `json:"rows_produced_per_join"`
	// Filtered            string   `json:"filtered"`
	// IndexCondition      string   `json:"index_condition"`
	// CostInfo            struct {
	//  ReadCost        string `json:"read_cost"`
	//	EvalCost        string `json:"eval_cost"`
	//	PrefixCost      string `json:"prefix_cost"`
	//	DataReadPerJoin string `json:"data_read_per_join"`
	// } `json:"cost_info"`
	// UsedColumns       []string `json:"used_columns"`
	// AttachedCondition string   `json:"attached_condition"`
}

var ErrNoIndexAvb = errors.New("no index available to satisfy the WHERE query")
var ErrNoTable = errors.New("could not identify table in EXPLAIN output")

// GetIndex returns the index used by the query and whether it's a covering index.
func GetIndex(where string, db *sql.DB) (string, bool, error) {
	var explainResult string
	query := "EXPLAIN format=json " + where
	err := db.QueryRow(query).Scan(&explainResult) //nolint:execinquery
	if err != nil {
		return "", false, err
	}

	var eo explainOutput
	err = json.Unmarshal([]byte(explainResult), &eo)
	if err != nil {
		return "", false, err
	}

	// If the query has an ORDER BY clause, the table will be in the OrderingOperation field.
	if eo.QueryBlock.OrderingOperation != nil && eo.QueryBlock.Table == nil {
		eo.QueryBlock.Table = eo.QueryBlock.OrderingOperation.Table
	}
	if eo.QueryBlock.Table == nil {
		return "", false, fmt.Errorf("%w: %s, message: %s", ErrNoTable, where, eo.QueryBlock.Message)
	}

	table := eo.QueryBlock.Table
	keyField := table.Key
	possibleKeys := table.PossibleKeys

	// Use the Key field preferably, otherwise use the first index in the list
	// from PossibleKeys field.
	if keyField != "" {
		return keyField, table.UsingIndex, nil
	} else if len(possibleKeys) != 0 {
		return possibleKeys[0], table.UsingIndex, nil
	}

	return "", false, fmt.Errorf("%w: %s, message: %s", ErrNoIndexAvb, where, eo.QueryBlock.Message)
}
