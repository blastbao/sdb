package planner

import (
	"sort"
	"strconv"
	"time"

	"github.com/dty1er/sdb/parser"
	"github.com/dty1er/sdb/schema"
	"github.com/dty1er/sdb/sdb"
)

type InsertPlan struct {
	sdb.Plan

	Table  *schema.Table
	values [][]interface{}
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
				v := convert(rows[j][i], column.Type)
				tuples[j][i] = v
			}
		} else {
			for j := range rows {
				// the column is not specified in Insert statement. Put default value
				tuples[j][i] = column.DefaultValue()
			}
		}
	}

	return &InsertPlan{Table: tableDef, values: tuples}
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

func convert(v string, t schema.ColumnType) interface{} {
	switch t {
	case schema.ColumnTypeBool:
		if v == "true" {
			return true
		} else {
			return false
		}
	case schema.ColumnTypeInt64:
		iv, _ := strconv.ParseInt(v, 10, 64) // Error is checked in parse phase in advance
		return iv
	case schema.ColumnTypeFloat64:
		fv, _ := strconv.ParseFloat(v, 64) // Error is checked in parse phase in advance
		return fv
	case schema.ColumnTypeBytes:
		return []byte(v)
	case schema.ColumnTypeString:
		return v
	case schema.ColumnTypeTimestamp:
		layouts := []string{ // FUTURE WORK: should support more formats?
			"2006-01-02 15:04:05",
			"2006-01-02",
			time.RFC3339,
		}
		for _, layout := range layouts {
			t, err := time.Parse(layout, v)
			if err != nil {
				return t
			}
		}
		return time.Time{}
	}
	return nil
}
