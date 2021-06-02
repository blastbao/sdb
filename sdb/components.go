package sdb

import (
	"io"

	"github.com/dty1er/sdb/schema"
)

type Statement interface {
	isStatement()
}

type Parser interface {
	Parse(sql string) (Statement, error)
}

type Query interface {
	isQuery()
}

type Plan interface {
	isPlan()
}

type Planner interface {
	Plan(stmt Statement) (Plan, error)
}

type Catalog interface {
	AddTable(table string, columns []*schema.ColumnDef, indices []*schema.Index) error
	FindTable(table string) bool
	ListIndices() []*schema.Index
	Persist() error
}

type Executor interface {
	// TODO: consider interface
	Execute(plan Plan) (*Result, error)
}

type Engine interface {
	CreateIndex(table, idxName string)
	Shutdown() error
}

type Serializer interface {
	Serialize() ([]byte, error)
}

type Deserializer interface {
	Deserialize(r io.Reader) error
}

type DiskManager interface {
	Load(name string, offset int, d Deserializer) error
	Persist(name string, offset int, s Serializer) error
}
