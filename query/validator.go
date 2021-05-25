package query

import (
	"fmt"
	"strings"

	"github.com/dty1er/sdb/engine"
)

type Validator struct {
	stmt    *Statement
	catalog *engine.Catalog
}

func NewValidator(stmt *Statement, catalog *engine.Catalog) *Validator {
	return &Validator{stmt: stmt, catalog: catalog}
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

func validType(typ string) bool {
	if typ == "" {
		return false
	}

	validTypes := []string{"bool", "int64", "float64", "bytes", "string", "timestamp"}

	for _, validType := range validTypes {
		if strings.ToLower(typ) == validType {
			return true
		}
	}

	return false
}

// TODO: validate types and columns are valid for the table
func (v *Validator) validateCreateTableStmt() error {
	stmt := v.stmt.CreateTable

	if len(stmt.PrimaryKeyCol) == 0 {
		return fmt.Errorf("at least one primary key is required")
	}

	if !v.catalog.FindTable(stmt.Table) {
		return fmt.Errorf("table %s already exists", stmt.Table)
	}

	if len(stmt.Columns) != len(stmt.Types) {
		return fmt.Errorf("query is invalid")
	}

	if 100 < len(stmt.Columns) {
		return fmt.Errorf("too much columns")
	}

	for _, columnName := range stmt.Columns {
		if !validColName(columnName) {
			return fmt.Errorf("column name %s is not allowed", columnName)
		}
	}

	for _, typ := range stmt.Types {
		if !validType(typ) {
			return fmt.Errorf("type %s is not allowed", typ)
		}
	}

	return nil
}

func (v *Validator) Validate() error {
	switch v.stmt.Typ {
	case CREATE_TABLE_STMT:
		return v.validateCreateTableStmt()
	default:
		return fmt.Errorf("unexpected statement type")
	}
}
