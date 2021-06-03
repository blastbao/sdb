package planner

import (
	"sort"
	"strconv"

	"github.com/dty1er/sdb/parser"
	"github.com/dty1er/sdb/schema"
	"github.com/dty1er/sdb/sdb"
)

type Index struct {
	Key sdb.IndexKey
	Idx *schema.Index
}

type InsertPlan struct {
	sdb.Plan

	Table   *schema.Table
	Indices []*Index
	Values  [][]interface{}
}

func (p *Planner) PlanInsert(stmt *parser.InsertStatement) *InsertPlan {
	// First, we want to fill the given rows to fit actual table schema correctly.
	// For example, we have table t{ID, Name, Age}
	// Insert statement can be like
	// "Insert into t (Age, ID) Values (20, 1), (30, 2);"
	// In this case, we want to create tuple {(1, "", 20), (2, "", 30)} where "" is the default value of Name column.
	tableDef := p.catalog.GetTable(stmt.Table)

	// space to store final tuples to save on database
	tuples := make([][]interface{}, len(stmt.Rows))
	for i := range tuples {
		tuples[i] = make([]interface{}, len(tableDef.Columns))
	}

	// First, we sort the column and given each row to be ordered.
	// "(Age, ID) VALUES (20, 1), (30, 2)" will be (ID, Age): {(1, 20), (2, 30)}.
	rows, columns := sortRowsAndColumns(stmt.Rows, stmt.Columns, tableDef)

	// Then, we process each row from left to right
	for i, column := range tableDef.Columns {
		col := columns[i]
		if column.Name == col {
			for j := range rows {
				v, _ := schema.ConvertValue(rows[j][i], column.Type)
				tuples[j][i] = v
			}
		} else {
			for j := range rows {
				// the column is not specified in Insert statement. Put default value
				tuples[j][i] = column.DefaultValue()
			}
		}
	}

	indices := make([]*Index, len(tableDef.Indices))
	for _, row := range rows {
		for i := range indices {
			idx := tableDef.Indices[i]
			indices[i] = &Index{Idx: idx}
			if tableDef.Columns[idx.ColumnIndex].Type == schema.ColumnTypeInt64 {
				iv, _ := strconv.ParseInt(row[idx.ColumnIndex], 10, 64)
				indices[i].Key = sdb.NewInt64IndexKey(iv)
			} else {
				indices[i].Key = sdb.NewStringIndexKey(row[idx.ColumnIndex])
			}
		}
	}
	return &InsertPlan{Table: tableDef, Indices: indices, Values: tuples}
}

func sortRowsAndColumns(rows [][]string, columns []string, table *schema.Table) ([][]string, []string) {
	m := map[string]int{}
	for i, col := range table.Columns {
		m[col.Name] = i
	}

	indices := []int{}

	for _, column := range columns {
		index := m[column]
		indices = append(indices, index)
	}

	sort.Slice(columns, func(i, j int) bool {
		return indices[i] < indices[j]
	})

	for _, row := range rows {
		sort.Slice(row, func(i, j int) bool {
			return indices[i] < indices[j]
		})
	}

	return rows, columns
}
