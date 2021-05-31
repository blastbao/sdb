package parser

import "github.com/dty1er/sdb/sdb"

type CreateTableStatement struct {
	sdb.Statement

	Table         string
	Columns       []string
	Types         []string
	PrimaryKeyCol string
}

type SelectStatement struct {
	sdb.Statement

	Columns []string
	Table   []string
	// Conditions []Expression
}

type InsertStatement struct {
	sdb.Statement

	Table   string
	Columns []string
	Rows    [][]string
}
