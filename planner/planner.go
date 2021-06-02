package planner

import (
	"fmt"

	"github.com/dty1er/sdb/parser"
	"github.com/dty1er/sdb/sdb"
)

// Planner is responsible to make the logical and physical
// query execution plan based on the catalog.
type Planner struct {
	catalog sdb.Catalog
}

func New(catalog sdb.Catalog) *Planner {
	return &Planner{catalog: catalog}
}

func (p *Planner) Plan(stmt sdb.Statement) (sdb.Plan, error) {
	switch s := stmt.(type) {
	case *parser.CreateTableStatement:
		return p.PlanCreateTable(s), nil
	case *parser.InsertStatement:
		return p.PlanInsert(s), nil
	}

	return nil, fmt.Errorf("unknown statement")
}
