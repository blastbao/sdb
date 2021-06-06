package planner

import (
	"testing"

	"github.com/dty1er/sdb/catalog"
	"github.com/dty1er/sdb/parser"
	"github.com/dty1er/sdb/schema"
	"github.com/dty1er/sdb/sdb"
	"github.com/dty1er/sdb/testutil"
)

func TestPlanner_PlanInsert(t *testing.T) {
	c := &catalog.Catalog{
		Tables: map[string]*schema.Table{
			"students": {
				Name: "students",
				Columns: []*schema.ColumnDef{
					{
						Name:    "id",
						Type:    schema.ColumnTypeInt64,
						Options: []schema.ColumnOption{schema.ColumnOptionPrimaryKey},
					},
					{
						Name:    "name",
						Type:    schema.ColumnTypeString,
						Options: []schema.ColumnOption{},
					},
					{
						Name:    "nickname",
						Type:    schema.ColumnTypeString,
						Options: []schema.ColumnOption{},
					},
					{
						Name:    "age",
						Type:    schema.ColumnTypeInt64,
						Options: []schema.ColumnOption{},
					},
				},
				PrimaryKeyIndex: 0,
				Indices: []*schema.Index{
					{Table: "students", Name: "students_pkey_id", ColumnIndex: 0},
				},
			},
		},
	}
	tests := []struct {
		name     string
		stmt     *parser.InsertStatement
		expected *InsertPlan
	}{
		{
			name: "ok",
			stmt: &parser.InsertStatement{
				Table:   "students",
				Columns: []string{"Id", "Age", "Nickname"},
				Rows: [][]string{
					{"5", "24", "bob"},
					{"6", "25", "nick"},
					{"7", "26", "al"},
				},
			},
			expected: &InsertPlan{
				Table: c.Tables["students"],
				Values: [][]interface{}{
					{int64(5), "", "bob", int64(24)},
					{int64(6), "", "nick", int64(25)},
					{int64(7), "", "al", int64(26)},
				},
				Indices: []*Indices{
					{
						Keys: []sdb.IndexKey{sdb.NewInt64IndexKey(5)},
						Idx:  []*schema.Index{c.Tables["students"].Indices[0]},
					},
					{
						Keys: []sdb.IndexKey{sdb.NewInt64IndexKey(6)},
						Idx:  []*schema.Index{c.Tables["students"].Indices[0]},
					},
					{
						Keys: []sdb.IndexKey{sdb.NewInt64IndexKey(7)},
						Idx:  []*schema.Index{c.Tables["students"].Indices[0]},
					},
				},
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			plan := New(c).PlanInsert(test.stmt)
			testutil.MustEqual(t, plan, test.expected)
		})
	}
}
