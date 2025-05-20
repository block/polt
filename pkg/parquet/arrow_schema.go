package parquet

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
)

func mysqlToArrowType(sqlType string) (arrow.DataType, error) {
	// Remove length specification if present
	if idx := strings.Index(sqlType, "("); idx != -1 {
		sqlType = sqlType[:idx]
	}

	switch strings.ToLower(sqlType) {
	case "varbinary", "blob", "binary":
		return arrow.BinaryTypes.Binary, nil
	case "varchar", "text", "char":
		return arrow.BinaryTypes.String, nil
	case "int", "integer", "smallint", "tinyint", "mediumint":
		return arrow.PrimitiveTypes.Int32, nil
	case "bigint":
		return arrow.PrimitiveTypes.Int64, nil
	case "float", "double", "decimal":
		return arrow.PrimitiveTypes.Float64, nil
	case "date", "datetime", "timestamp":
		return arrow.FixedWidthTypes.Timestamp_us, nil
	default:
		return arrow.Null, fmt.Errorf("unsupported type: %s", sqlType)
	}
}

// MysqlToArrowSchema converts a MySQL table schema to an Arrow schema.
func MysqlToArrowSchema(db *sql.DB, tableName string) (*arrow.Schema, error) {
	query := "DESCRIBE " + tableName
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var fields []arrow.Field
	for rows.Next() {
		var field, fieldType, null, key, defaultValue, extra sql.NullString
		if err := rows.Scan(&field, &fieldType, &null, &key, &defaultValue, &extra); err != nil {
			return nil, err
		}

		arrowType, err := mysqlToArrowType(fieldType.String)
		if err != nil {
			return nil, err
		}
		fields = append(fields, arrow.Field{Name: field.String, Type: arrowType})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return arrow.NewSchema(fields, nil), nil
}
