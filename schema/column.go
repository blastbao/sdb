package schema

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

type ColumnType uint8

const (
	ColumnTypeBool ColumnType = iota + 1
	ColumnTypeInt64
	ColumnTypeFloat64
	ColumnTypeBytes
	ColumnTypeString
	ColumnTypeTimestamp
)

var colToStrType = map[ColumnType]string{
	ColumnTypeBool:      "bool",
	ColumnTypeInt64:     "int64",
	ColumnTypeFloat64:   "float64",
	ColumnTypeBytes:     "bytes",
	ColumnTypeString:    "string",
	ColumnTypeTimestamp: "timestamp",
}

var strToColType = map[string]ColumnType{
	"bool":      ColumnTypeBool,
	"int64":     ColumnTypeInt64,
	"float64":   ColumnTypeFloat64,
	"bytes":     ColumnTypeBytes,
	"string":    ColumnTypeString,
	"timestamp": ColumnTypeTimestamp,
}

func (ct ColumnType) String() string {
	return colToStrType[ct]
}

func ConvertValue(v string, t ColumnType) (interface{}, error) {
	switch t {
	case ColumnTypeBool:
		return strconv.ParseBool(v)
	case ColumnTypeInt64:
		return strconv.ParseInt(v, 10, 64) // Error is checked in parse phase in advance
	case ColumnTypeFloat64:
		return strconv.ParseFloat(v, 64) // Error is checked in parse phase in advance
	case ColumnTypeBytes:
		return []byte(v), nil
	case ColumnTypeString:
		return v, nil
	case ColumnTypeTimestamp:
		layouts := []string{ // FUTURE WORK: should support more formats?
			"2006-01-02 15:04:05",
			"2006-01-02",
			time.RFC3339,
		}
		for _, layout := range layouts {
			t, err := time.Parse(layout, v)
			if err != nil {
				return t, nil
			}
		}
		return nil, fmt.Errorf("timestamp format is incorrect")
	}
	return nil, fmt.Errorf("unknown type")
}

type ColumnOption uint8

const (
	ColumnOptionNoOption ColumnOption = iota
	ColumnOptionPrimaryKey
	ColumnOptionDefaultValue
	// FUTURE WORK: support more types
	// https://github.com/blastrain/vitess-sqlparser/blob/develop/sqlparser/ast.go#L966-L977
)

type ColumnDef struct {
	Name       string
	Type       ColumnType
	Options    []ColumnOption
	DefaultVal interface{}
	// FUTURE WORK: support table options (e.g. encryption, max_rows, charset...)
	// https://dev.mysql.com/doc/refman/8.0/en/create-table.html
}

func (cd *ColumnDef) DefaultValue() interface{} {
	for _, opt := range cd.Options {
		if opt == ColumnOptionDefaultValue {
			return cd.DefaultValue
		}
	}
	switch cd.Type {
	case ColumnTypeBool:
		return false
	case ColumnTypeInt64:
		return int64(0)
	case ColumnTypeFloat64:
		return float64(0)
	case ColumnTypeBytes:
		return []byte{}
	case ColumnTypeString:
		return ""
	case ColumnTypeTimestamp:
		return time.Time{}
	}
	return nil
}

func IsValidColumnType(typ string) bool {
	_, ok := strToColType[strings.ToLower(typ)]
	return ok
}

func StrToColumnType(typ string) ColumnType {
	return strToColType[strings.ToLower(typ)]
}

type Index struct {
	Table       string
	Name        string
	ColumnIndex int
}

type IndexKey interface {
	Less(than IndexKey)
}

type IndexElement struct {
	Key   IndexKey
	Value interface{}
}

type Table struct {
	Name            string
	Columns         []*ColumnDef
	Indices         []*Index
	PrimaryKeyIndex int
}
