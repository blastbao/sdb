package parser

import (
	"fmt"
	"strings"

	"github.com/dty1er/sdb/schema"
	"github.com/dty1er/sdb/sdb"
)

type validator struct {
	stmt    sdb.Statement
	catalog sdb.Catalog
}

func newValidator(stmt sdb.Statement, catalog sdb.Catalog) *validator {
	return &validator{stmt: stmt, catalog: catalog}
}

func validColName(name string) bool {
	if name == "" {
		return false
	}

	const valid = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_1234567890"

	for _, char := range name {
		if !strings.Contains(valid, string(char)) {
			return false
		}
	}

	if 64 < len(name) {
		return false
	}

	return true
}

func (v *validator) validateCreateTableStmt(stmt *CreateTableStatement) error {
	if len(stmt.PrimaryKeyCol) == 0 {
		return fmt.Errorf("at least one primary key is required")
	}

	if len(stmt.Columns) != len(stmt.Types) {
		return fmt.Errorf("query is invalid")
	}

	if 100 < len(stmt.Columns) {
		return fmt.Errorf("too much columns")
	}

	pKeyInCol := false
	for _, columnName := range stmt.Columns {
		if columnName == stmt.PrimaryKeyCol {
			pKeyInCol = true
			break
		}
	}

	if !pKeyInCol {
		return fmt.Errorf("primary key %s is must be in column", stmt.PrimaryKeyCol)
	}

	for _, columnName := range stmt.Columns {
		if !validColName(columnName) {
			return fmt.Errorf("column name %s is not allowed", columnName)
		}
	}

	for _, typ := range stmt.Types {
		if !schema.IsValidColumnType(typ) {
			return fmt.Errorf("type %s is not allowed", typ)
		}
	}

	if v.catalog.FindTable(stmt.Table) {
		return fmt.Errorf("table %s already exists", stmt.Table)
	}

	return nil
}

func (v *validator) validateInsertStmt(stmt *InsertStatement) error {
	if len(stmt.Rows) > 1000 {
		return fmt.Errorf("Inserting rows number exceeded the limit 1000: %d", len(stmt.Rows))
	}
	colLen := len(stmt.Columns)
	for _, row := range stmt.Rows {
		if len(row) != colLen {
			return fmt.Errorf("row size %d must be the same as column length %d", len(row), colLen)
		}
	}

	if !v.catalog.FindTable(stmt.Table) {
		return fmt.Errorf("table %s does not exist", stmt.Table)
	}

	table := v.catalog.GetTable(stmt.Table)

	for i, col := range stmt.Columns {
		var typ schema.ColumnType
		for _, actualCol := range table.Columns {
			if col == actualCol.Name {
				typ = actualCol.Type
				break
			}
		}
		if typ == 0 {
			return fmt.Errorf("column %s is not defined in the table %s", col, stmt.Table)
		}

		for _, row := range stmt.Rows {
			v := row[i]
			if _, err := schema.ConvertValue(v, typ); err != nil {
				return fmt.Errorf("invalid value %v for column %s, type %s", v, col, typ)
			}
		}
	}

	return nil
}

func (v *validator) validate() error {
	switch s := v.stmt.(type) {
	case *CreateTableStatement:
		return v.validateCreateTableStmt(s)
	case *InsertStatement:
		return v.validateInsertStmt(s)
	default:
		return fmt.Errorf("unexpected statement type")
	}
}
