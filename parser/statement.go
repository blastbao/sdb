package parser

import "github.com/dty1er/sdb/sdb"

type StmtType uint8

const (
	CREATE_TABLE_STMT StmtType = iota + 1
	SELECT_STMT
	INSERT_STMT
	UPDATE_STMT
	DELETE_STMT
)

type CreateTableStatement struct {
	Table         string
	Columns       []string
	Types         []string
	PrimaryKeyCol string
}

type SelectStatement struct {
	Columns []string
	Table   []string
	// Conditions []Expression
}

type InsertStatement struct {
	Table   string
	Columns []string
	Rows    [][]string
}

// Statement represents a sql as statement.
// It implements sdb.Statement interface.
type Statement struct {
	sdb.Statement

	Typ         StmtType
	CreateTable *CreateTableStatement
	Select      *SelectStatement
	Insert      *InsertStatement
}
