package sdb

import (
	"io"

	"github.com/dty1er/sdb/schema"
)

// Statement represents the SQL statement like SELECT, INSERT, etc.
type Statement interface {
	isStatement()
}

// Parser can parse the raw SQL and return parsed statement.
// In case syntax error or inconsistent query for the scheme, error must be responded.
type Parser interface {
	Parse(sql string) (Statement, error)
}

// Plan is an execution plan of the given SQL for the database schema.
type Plan interface {
	isPlan()
}

// Planner can read statement and create concrete execution plan for the database.
type Planner interface {
	Plan(stmt Statement) (Plan, error)
}

// Catalog is a set of metadata/information for the database.
type Catalog interface {
	GetTable(table string) *schema.Table
	AddTable(table string, columns []*schema.ColumnDef, indices []*schema.Index) error
	FindTable(table string) bool
	ListIndices() []*schema.Index
	Persist() error
}

// Executor can execute the given execution plan on the database.
type Executor interface {
	// TODO: consider interface
	Execute(plan Plan) (*Result, error)
}

// Tuple is a set of values which represents a row in a table.
type Tuple interface {
	Serializer
	Deserializer
}

type IndexKey interface {
	Less(than IndexKey) bool
}

type StringIndexKey struct {
	val string
}

func NewStringIndexKey(val string) *StringIndexKey {
	return &StringIndexKey{val: val}
}

func (k *StringIndexKey) Less(than IndexKey) bool {
	thanV, ok := than.(*StringIndexKey)
	if !ok {
		return false
	}
	return k.val < thanV.val
}

type Int64IndexKey struct {
	val int64
}

func NewInt64IndexKey(val int64) *Int64IndexKey {
	return &Int64IndexKey{val: val}
}

func (k *Int64IndexKey) Less(than IndexKey) bool {
	thanV, ok := than.(*Int64IndexKey)
	if !ok {
		return false
	}
	return k.val < thanV.val
}

// Engine is a storage engine of sdb.
type Engine interface {
	CreateIndex(table, idxName string)
	InsertTuple(table string, t Tuple) error
	InsertIndex(table, idxName string, key IndexKey, t Tuple) error
	Shutdown() error
}

// Serializer can serialize the object into byte sequence.
type Serializer interface {
	Serialize() ([]byte, error)
}

// Serializer can read r and deserialize the byte sequence into the object.
type Deserializer interface {
	Deserialize(r io.Reader) error
}

// DiskManager can manage actual files on the disk and take care of data persistence.
type DiskManager interface {
	Load(name string, offset int, d Deserializer) error
	Persist(name string, offset int, s Serializer) error
}
