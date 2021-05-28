package parser

import (
	"fmt"
	"strings"

	"github.com/dty1er/sdb/engine"
)

type Validator struct {
	stmt   *Statement
	engine *engine.Engine
}

func NewValidator(stmt *Statement, engine *engine.Engine) *Validator {
	return &Validator{stmt: stmt, engine: engine}
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

func (v *Validator) validateCreateTableStmt() error {
	stmt := v.stmt.CreateTable

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
		if !validType(typ) {
			return fmt.Errorf("type %s is not allowed", typ)
		}
	}

	if v.engine.FindTable(stmt.Table) {
		return fmt.Errorf("table %s already exists", stmt.Table)
	}

	return nil
}

func (v *Validator) validateInsertStmt() error {
	stmt := v.stmt.Insert

	if !v.engine.FindTable(stmt.Table) {
		return fmt.Errorf("table %s does not exist", stmt.Table)
	}

	// TODO: make sure the given record matches the table scheme

	return nil
}

func (v *Validator) Validate() error {
	switch v.stmt.Typ {
	case CREATE_TABLE_STMT:
		return v.validateCreateTableStmt()
	case INSERT_STMT:
		return v.validateInsertStmt()
	default:
		return fmt.Errorf("unexpected statement type")
	}
}
