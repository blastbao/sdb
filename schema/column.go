package schema

import "strings"

type ColumnType uint8

const (
	ColumnTypeBool ColumnType = iota + 1
	ColumnTypeInt64
	ColumnTypeFloat64
	ColumnTypeBytes
	ColumnTypeString
	ColumnTypeTimestamp
)

var strToColType = map[string]ColumnType{
	"bool":      ColumnTypeBool,
	"int64":     ColumnTypeInt64,
	"float64":   ColumnTypeFloat64,
	"bytes":     ColumnTypeBytes,
	"string":    ColumnTypeString,
	"timestamp": ColumnTypeTimestamp,
}

type ColumnOption uint8

const (
	ColumnOptionNoOption ColumnOption = iota
	ColumnOptionPrimaryKey
	// FUTURE WORK: support more types
	// https://github.com/blastrain/vitess-sqlparser/blob/develop/sqlparser/ast.go#L966-L977
)

type ColumnDef struct {
	Name    string
	Type    ColumnType
	Options []ColumnOption
	// FUTURE WORK: support table options (e.g. encryption, max_rows, charset...)
	// https://dev.mysql.com/doc/refman/8.0/en/create-table.html
}

func IsValidColumnType(typ string) bool {
	_, ok := strToColType[strings.ToLower(typ)]
	return ok
}

func StrToColumnType(typ string) ColumnType {
	return strToColType[strings.ToLower(typ)]
}

type Index struct {
	Table   string
	Name    string
	Columns []string
}

type IndexKey interface {
	Less(than IndexKey)
}

type IndexElement struct {
	Key   IndexKey
	Value interface{}
}

type Table struct {
	Columns         []*ColumnDef
	Indices         []*Index
	PrimaryKeyIndex int
}
