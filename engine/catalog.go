package engine

import (
	"fmt"
	"sync"
)

// TODO: (de)serialize and persistent feature
type Table struct {
	Columns         []string
	Types           []Type
	PrimaryKeyIndex int
}

func (t *Table) Has(col, typ string) bool {
	for i := range t.Columns {
		if t.Columns[i] == col && t.Types[i].String() == typ {
			return true
		}
	}

	return false
}

type Catalog struct {
	Tables map[string]*Table
	latch  sync.RWMutex
}

func NewCatalog() *Catalog {
	return &Catalog{
		Tables: map[string]*Table{},
	}
}

func (c *Catalog) AddTable(table string, columns, types []string, pkey string) error {
	c.latch.Lock()
	defer c.latch.Unlock()

	if c.FindTable(table) {
		return fmt.Errorf("table %s already exists", table)
	}

	tps := make([]Type, len(types))
	for i, typ := range types {
		tps[i] = TypeFromString(typ)
	}

	idx := 0
	for i, column := range columns {
		if pkey == column {
			idx = i
		}
	}

	t := &Table{Columns: columns, Types: tps, PrimaryKeyIndex: idx}
	c.Tables[table] = t

	return nil
}

func (c *Catalog) FindTable(table string) bool {
	_, ok := c.Tables[table]
	return ok
}

func (c *Catalog) MatchColumns(table string, columns []string, types []string) bool {
	t, ok := c.Tables[table]
	if !ok {
		return false
	}

	if len(columns) != len(types) {
		return false
	}

	if len(columns) == 0 {
		return false
	}

	for i := range columns {
		if t.Has(columns[i], types[i]) {
			return true
		}

	}

	return false
}
