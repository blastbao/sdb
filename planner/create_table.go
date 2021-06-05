package planner

import (
	"fmt"
	"strings"

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
	table := strings.ToLower(stmt.Table)
	for i, column := range stmt.Columns {
		column = strings.ToLower(column)
		columns[i] = &schema.ColumnDef{
			Name: column,
			Type: schema.StrToColumnType(stmt.Types[i]),
		}

		if column == stmt.PrimaryKeyCol {
			columns[i].Options = append(columns[i].Options, schema.ColumnOptionPrimaryKey)
			idxName := fmt.Sprintf("%s_pkey_%s", table, column)
			indices = append(indices, &schema.Index{Table: table, Name: idxName, ColumnIndex: i})
		}
	}

	return &CreateTablePlan{
		Table:   table,
		Columns: columns,
		Indices: indices,
	}
}
