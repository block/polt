package parquet

import (
	"database/sql"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/block/polt/pkg/test"
	"github.com/stretchr/testify/require"
)

func TestMysqlToArrowSchema(t *testing.T) {
	test.RunSQL(t, `DROP TABLE IF EXISTS schema_test`)

	tbl := `CREATE TABLE schema_test (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		idxed_column int(11) NOT NULL,
		created_at datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
		INDEX(idxed_column),
		PRIMARY KEY (id)
	)`
	test.RunSQL(t, tbl)
	db, err := sql.Open("mysql", test.DSN())
	require.NoError(t, err)
	aSchema, err := MysqlToArrowSchema(db, "schema_test")
	require.NoError(t, err)

	require.Len(t, aSchema.Fields(), 4)

	nameFields, ok := aSchema.FieldsByName("name")
	require.True(t, ok)
	require.Len(t, nameFields, 1)
	require.Equal(t, "name", nameFields[0].Name)
	require.Equal(t, arrow.BinaryTypes.String, nameFields[0].Type)

	idFields, ok := aSchema.FieldsByName("id")
	require.True(t, ok)
	require.Len(t, idFields, 1)
	require.Equal(t, "id", idFields[0].Name)
	require.Equal(t, arrow.PrimitiveTypes.Int32, idFields[0].Type)

	idxedColumnFields, ok := aSchema.FieldsByName("idxed_column")
	require.True(t, ok)
	require.Len(t, idxedColumnFields, 1)
	require.Equal(t, "idxed_column", idxedColumnFields[0].Name)
	require.Equal(t, arrow.PrimitiveTypes.Int32, idxedColumnFields[0].Type)

	createdAtFields, ok := aSchema.FieldsByName("created_at")
	require.True(t, ok)
	require.Len(t, createdAtFields, 1)
	require.Equal(t, "created_at", createdAtFields[0].Name)
	require.Equal(t, arrow.FixedWidthTypes.Timestamp_us, createdAtFields[0].Type)
}
