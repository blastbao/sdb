package planner

import (
	"testing"

	"github.com/dty1er/sdb/parser"
	"github.com/dty1er/sdb/schema"
	"github.com/dty1er/sdb/testutil"
)

func TestPlanner_PlanCreateTable(t *testing.T) {
	tests := []struct {
		name     string
		stmt     *parser.CreateTableStatement
		expected *CreateTablePlan
	}{
		{
			name: "ok",
			stmt: &parser.CreateTableStatement{
				Table:         "users",
				Columns:       []string{"Id", "Name", "Verified", "Registered"},
				Types:         []string{"INT64", "STRING", "BOOL", "TIMESTAMP"},
				PrimaryKeyCol: "id",
			},
			expected: &CreateTablePlan{
				Table: "users",
				Columns: []*schema.ColumnDef{
					{Name: "id", Type: schema.ColumnTypeInt64, Options: []schema.ColumnOption{schema.ColumnOptionPrimaryKey}},
					{Name: "name", Type: schema.ColumnTypeString},
					{Name: "verified", Type: schema.ColumnTypeBool},
					{Name: "registered", Type: schema.ColumnTypeTimestamp},
				},
				Indices: []*schema.Index{
					{Table: "users", Name: "users_pkey_id", ColumnIndex: 0},
				},
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			plan := New(nil).PlanCreateTable(test.stmt)
			testutil.MustEqual(t, plan, test.expected)
		})
	}
}
