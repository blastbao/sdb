package planner

import (
	"time"

	"github.com/dty1er/sdb/parser"
	"github.com/dty1er/sdb/schema"
	"github.com/dty1er/sdb/sdb"
)

type SelectPlan struct {
	sdb.Plan

	LogicalPlan LogicalPlan
}

// PlanSelect makes a plan to query data by given SELECT statement.
func (p *Planner) PlanSelect(stmt *parser.SelectStatement) *SelectPlan {
	// PlanSelect consists of 2 parts.
	// In first phase, it makes logical plan. Logical plan shows the sequence of processes how to
	// create the desired result set.
	// In second phase, some optimizations are applied to the logical plan to get better performance.
	// For example, it can convert a logical plan "scan `mytable`" to use index.
	// We call optimizations-applied plan as "physical plan".

	var list List

	// plan from
	ate := stmt.From.(*parser.AliasedTableExpr)
	tbl := ate.Expr.(*parser.TableName)
	sc := &Scan{
		Table: &Table{Name: tbl.Name, Alias: ate.As},
	}

	list = sc

	// plan where
	if stmt.Where != nil {
		ce := stmt.Where.Expr.(*parser.ComparisonExpr)
		if ce.Operator != parser.Op_EQ {
			// TODO: fix
			panic("where expr must be equality")
		}
		col := ce.Left.(*parser.ColName)
		val := ce.Right.(*parser.Value)
		f := &EqualityFilter{
			Column: &Column{Name: col.Name},
		}

		colDef, _ := p.catalog.GetColumnDef(tbl.Name, col.Name)
		switch colDef.Type {
		case schema.ColumnTypeBool:
			v, _ := schema.ConvertValue(val.Val, schema.ColumnTypeBool)
			f.Value = &BoolExpr{Value: v.(bool)}
		case schema.ColumnTypeInt64:
			v, _ := schema.ConvertValue(val.Val, schema.ColumnTypeInt64)
			f.Value = &Int64Expr{Value: v.(int64)}
		case schema.ColumnTypeFloat64:
			v, _ := schema.ConvertValue(val.Val, schema.ColumnTypeFloat64)
			f.Value = &Float64Expr{Value: v.(float64)}
		case schema.ColumnTypeBytes:
			v, _ := schema.ConvertValue(val.Val, schema.ColumnTypeBytes)
			f.Value = &BytesExpr{Value: v.([]byte)}
		case schema.ColumnTypeString:
			v, _ := schema.ConvertValue(val.Val, schema.ColumnTypeString)
			f.Value = &StringExpr{Value: v.(string)}
		case schema.ColumnTypeTimestamp:
			v, _ := schema.ConvertValue(val.Val, schema.ColumnTypeTimestamp)
			f.Value = &TimestampExpr{Value: v.(time.Time)}
		}

		s := &Selection{Filter: f, Input: sc}
		list = s
	}

	// plan order by
	if stmt.OrderBy != nil {
		ob := &OrderBy{
			Columns:     make([]Expr, len(stmt.OrderBy)),
			Directirons: make([]string, len(stmt.OrderBy)),
		}
		for i, o := range stmt.OrderBy {
			column := o.Expr.(*parser.ColName)
			ob.Columns[i] = &Column{Name: column.Name}
			ob.Directirons[i] = o.Direction.String()
		}

		ob.Input = list
		list = ob
	}

	// plan limit
	if stmt.Limit != nil {
		l := &Limit{Limit: &Int64Expr{Value: int64(stmt.Limit.Count)}}
		o := &Offset{Offset: &Int64Expr{Value: int64(stmt.Limit.Offset)}}

		o.Input = list
		l.Input = o
		list = l
	}

	// plan columns (projection)
	pj := &Projection{Columns: []Expr{}}
	for _, se := range stmt.SelectExprs {
		switch s := se.(type) {
		case *parser.StarExpr:
			for _, colDef := range p.catalog.GetTable(tbl.Name).Columns {
				pj.Columns = append(pj.Columns, &Column{Table: tbl.Name, Name: colDef.Name, Alias: colDef.Name})
			}
			// when * is specified, no other column should be placed
			break
		case *parser.AliasedExpr:
			c := s.Expr.(*parser.ColName)
			pj.Columns = append(pj.Columns, &Column{Table: tbl.Name, Name: c.Name, Alias: s.As})
		}
	}

	pj.Input = list

	// TODO: apply optimizations

	return &SelectPlan{LogicalPlan: pj}
}
