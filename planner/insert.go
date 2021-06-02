package planner

import (
	"github.com/dty1er/sdb/catalog"
	"github.com/dty1er/sdb/parser"
	"github.com/dty1er/sdb/schema"
	"github.com/dty1er/sdb/sdb"
)

type InsertPlan struct {
	sdb.Plan

	Table   *catalog.Table
	Indices []*schema.Index
	values  []interface{}
}

func (p *Planner) PlanInsert(stmt *parser.InsertStatement) *InsertPlan {
	return &InsertPlan{}
}
