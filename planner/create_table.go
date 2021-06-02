package planner

import (
	"github.com/dty1er/sdb/parser"
	"github.com/dty1er/sdb/schema"
	"github.com/dty1er/sdb/sdb"
)

type CreateTablePlan struct {
	sdb.Plan
	Table   string
	Columns []*schema.ColumnDef
	Indices []*schema.Index
}

func (p *Planner) PlanCreateTable(stmt *parser.CreateTableStatement) *CreateTablePlan {
	columns := make([]*schema.ColumnDef, len(stmt.Columns))
	indices := []*schema.Index{}
	for i, column := range stmt.Columns {
		columns[i] = &schema.ColumnDef{
			Name: column,
			Type: schema.StrToColumnType(stmt.Types[i]),
		}

		if column == stmt.PrimaryKeyCol {
			columns[i].Options = append(columns[i].Options, schema.ColumnOptionPrimaryKey)
			indices = append(indices, &schema.Index{Name: column, Columns: []string{column}})
		}
	}

	return &CreateTablePlan{
		Table:   stmt.Table,
		Columns: columns,
		Indices: indices,
	}
}
