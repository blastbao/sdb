package planner

import (
	"strings"

	"github.com/dty1er/sdb/parser"
	"github.com/dty1er/sdb/schema"
	"github.com/dty1er/sdb/sdb"
)

type Indices struct {
	Keys []sdb.IndexKey
	Idx  []*schema.Index
}

type InsertPlan struct {
	sdb.Plan

	Table   *schema.Table
	Indices []*Indices
	Values  [][]interface{}
}

func (p *Planner) PlanInsert(stmt *parser.InsertStatement) *InsertPlan {
	tableDef := p.catalog.GetTable(stmt.Table)

	values := [][]interface{}{}
	indices := []*Indices{}

	for _, row := range stmt.Rows {
		vs, is := p.planInsertRow(tableDef, stmt.Columns, row)
		values = append(values, vs)
		indices = append(indices, is)
	}

	return &InsertPlan{Table: tableDef, Indices: indices, Values: values}
}

// planInsertRow creates a complete record and index for the given row to be inserted.
// The given row might be incomplete according to the table schema; for example, the actual schema is
// Students{"id(int64)", "name(string)", "age(int64)"}
// But the statement might be
// (age, id) values (25, 1), (30, 2). In this case, name should be the default value of the column.
// This method converts the given row (25, 1) to (1, "", 25).
func (p *Planner) planInsertRow(table *schema.Table, columns, row []string) ([]interface{}, *Indices) {
	result := make([]interface{}, len(table.Columns))
	for i, columnDef := range table.Columns {
		index := -1
		// Look for the given column from the schema.
		for j, col := range columns {
			if strings.ToLower(col) == columnDef.Name {
				index = j
				break
			}
		}

		if index == -1 {
			// If the given column is not found in the schema, use default value
			result[i] = columnDef.DefaultValue()
		} else {
			// Else, use the value from the row.
			// The type is checked on validate, so ignore error
			result[i], _ = schema.ConvertValue(row[index], columnDef.Type)
		}
	}

	indices := &Indices{
		Keys: make([]sdb.IndexKey, len(table.Indices)),
		Idx:  make([]*schema.Index, len(table.Indices)),
	}

	for i, indexDef := range table.Indices {
		indices.Idx[i] = indexDef
		key := result[i]
		switch k := key.(type) {
		case int64:
			indices.Keys[i] = sdb.NewInt64IndexKey(k)
		default:
			indices.Keys[i] = sdb.NewStringIndexKey(k.(string))
		}
	}

	return result, indices
}
