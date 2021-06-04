package planner

import (
	"github.com/dty1er/sdb/parser"
	"github.com/dty1er/sdb/schema"
	"github.com/dty1er/sdb/sdb"
)

type CreateTablePlan struct {
	sdb.Plan
	Table   *schema.Table
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
			indices = append(indices, &schema.Index{Name: column, ColumnIndex: i})
		}
	}

	table := p.catalog.GetTable(stmt.Table)

	// TODO: fill secondary index based on table.Indices to support multiple indices

	return &CreateTablePlan{
		Table:   table,
		Columns: columns,
		Indices: indices,
	}
}
