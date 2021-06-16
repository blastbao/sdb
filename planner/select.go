package planner

import (
	"github.com/dty1er/sdb/parser"
	"github.com/dty1er/sdb/sdb"
)

type SelectPlan struct {
	sdb.Plan

	// Plan PhysicalPlan
}

// PlanSelect makes a plan to query data by given SELECT statement.
func (p *Planner) PlanSelect(stmt *parser.SelectStatement) *SelectPlan {
	// PlanSelect consists of 2 parts.
	// In first phase, it makes logical plan. Logical plan shows the sequence of processes how to
	// create the desired result set.
	// In second phase, some optimizations are applied to the logical plan to get better performance.
	// For example, it can convert a logical plan "scan `mytable`" to use index.
	// We call optimizations-applied plan "physical plan".

	var lp LogicalPlan

	cols := []*Column{}
	for _, se := range stmt.SelectExprs {
		switch se.(type) {
		case parser.StarExpr:
		case parser.AliasedExpr:
		}
	}

	switch {
	case stmt.Limit != nil:
		l := &Limit{
			Limit: &NumberExpr{Value: stmt.Limit.Count},
			Input: &Offset{
				Offset: stmt.Limit.Offset,
			},
		}
	}
	// logicalPlan := &Projection{}

	return &SelectPlan{}
}
