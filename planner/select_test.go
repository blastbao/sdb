package planner

import (
	"testing"

	"github.com/dty1er/sdb/catalog"
	"github.com/dty1er/sdb/parser"
	"github.com/dty1er/sdb/schema"
	"github.com/dty1er/sdb/testutil"
)

func TestPlanner_PlanSelect(t *testing.T) {
	c := &catalog.Catalog{
		Tables: map[string]*schema.Table{
			"users": {
				Name: "users",
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
					{Table: "users", Name: "users_pkey_id", ColumnIndex: 0},
				},
			},
		},
	}
	tests := []struct {
		name     string
		stmt     *parser.SelectStatement
		expected *SelectPlan
	}{
		{
			name: `select * from users`,
			stmt: &parser.SelectStatement{
				SelectExprs: []parser.SelectExpr{
					&parser.StarExpr{},
				},
				From: &parser.AliasedTableExpr{
					Expr: &parser.TableName{
						Name: "users",
					},
				},
			},
			expected: &SelectPlan{
				LogicalPlan: &Projection{
					Columns: []Expr{
						&Column{Table: "users", Name: "id", Alias: "id"},
						&Column{Table: "users", Name: "name", Alias: "name"},
						&Column{Table: "users", Name: "nickname", Alias: "nickname"},
						&Column{Table: "users", Name: "age", Alias: "age"},
					},
					Input: &Scan{
						Table: &Table{Name: "users"},
					},
				},
			},
		},
		{
			name: `select id as i, name as n from users`,
			stmt: &parser.SelectStatement{
				SelectExprs: []parser.SelectExpr{
					&parser.AliasedExpr{Expr: &parser.ColName{Name: "id"}, As: "i"},
					&parser.AliasedExpr{Expr: &parser.ColName{Name: "name"}, As: "n"},
				},
				From: &parser.AliasedTableExpr{
					Expr: &parser.TableName{
						Name: "users",
					},
				},
			},
			expected: &SelectPlan{
				LogicalPlan: &Projection{
					Columns: []Expr{
						&Column{Table: "users", Name: "id", Alias: "i"},
						&Column{Table: "users", Name: "name", Alias: "n"},
					},
					Input: &Scan{
						Table: &Table{Name: "users"},
					},
				},
			},
		},
		{
			name: `select * from users where id = 5`,
			stmt: &parser.SelectStatement{
				SelectExprs: []parser.SelectExpr{
					&parser.StarExpr{},
				},
				From: &parser.AliasedTableExpr{
					Expr: &parser.TableName{
						Name: "users",
					},
				},
				Where: &parser.Where{
					Expr: &parser.ComparisonExpr{
						Left:     &parser.ColName{Name: "id"},
						Operator: parser.Op_EQ,
						Right:    &parser.Value{Val: "5"},
					},
				},
			},
			expected: &SelectPlan{
				LogicalPlan: &Projection{
					Columns: []Expr{
						&Column{Table: "users", Name: "id", Alias: "id"},
						&Column{Table: "users", Name: "name", Alias: "name"},
						&Column{Table: "users", Name: "nickname", Alias: "nickname"},
						&Column{Table: "users", Name: "age", Alias: "age"},
					},
					Input: &Selection{
						Filter: &EqualityFilter{
							Column: &Column{Name: "id"},
							Value:  &Int64Expr{Value: int64(5)},
						},
						Input: &Scan{
							Table: &Table{Name: "users"},
						},
					},
				},
			},
		},
		{
			name: `select * from users where name = "aaa"`,
			stmt: &parser.SelectStatement{
				SelectExprs: []parser.SelectExpr{
					&parser.StarExpr{},
				},
				From: &parser.AliasedTableExpr{
					Expr: &parser.TableName{
						Name: "users",
					},
				},
				Where: &parser.Where{
					Expr: &parser.ComparisonExpr{
						Left:     &parser.ColName{Name: "name"},
						Operator: parser.Op_EQ,
						Right:    &parser.Value{Val: "aaa"},
					},
				},
			},
			expected: &SelectPlan{
				LogicalPlan: &Projection{
					Columns: []Expr{
						&Column{Table: "users", Name: "id", Alias: "id"},
						&Column{Table: "users", Name: "name", Alias: "name"},
						&Column{Table: "users", Name: "nickname", Alias: "nickname"},
						&Column{Table: "users", Name: "age", Alias: "age"},
					},
					Input: &Selection{
						Filter: &EqualityFilter{
							Column: &Column{Name: "name"},
							Value:  &StringExpr{Value: "aaa"},
						},
						Input: &Scan{
							Table: &Table{Name: "users"},
						},
					},
				},
			},
		},
		{
			name: `select * from users order by id, name`,
			stmt: &parser.SelectStatement{
				SelectExprs: []parser.SelectExpr{
					&parser.StarExpr{},
				},
				From: &parser.AliasedTableExpr{
					Expr: &parser.TableName{
						Name: "users",
					},
				},
				OrderBy: []*parser.Order{
					{
						Expr:      &parser.ColName{Name: "id"},
						Direction: parser.OrderDirection_ASC,
					},
					{
						Expr:      &parser.ColName{Name: "name"},
						Direction: parser.OrderDirection_ASC,
					},
				},
			},
			expected: &SelectPlan{
				LogicalPlan: &Projection{
					Columns: []Expr{
						&Column{Table: "users", Name: "id", Alias: "id"},
						&Column{Table: "users", Name: "name", Alias: "name"},
						&Column{Table: "users", Name: "nickname", Alias: "nickname"},
						&Column{Table: "users", Name: "age", Alias: "age"},
					},
					Input: &OrderBy{
						Columns: []Expr{
							&Column{Name: "id"},
							&Column{Name: "name"},
						},
						Directirons: []string{"asc", "asc"},
						Input: &Scan{
							Table: &Table{Name: "users"},
						},
					},
				},
			},
		},
		{
			name: `select * from users limit 5 offset 10`,
			stmt: &parser.SelectStatement{
				SelectExprs: []parser.SelectExpr{
					&parser.StarExpr{},
				},
				From: &parser.AliasedTableExpr{
					Expr: &parser.TableName{
						Name: "users",
					},
				},
				Limit: &parser.Limit{
					Count:  5,
					Offset: 10,
				},
			},
			expected: &SelectPlan{
				LogicalPlan: &Projection{
					Columns: []Expr{
						&Column{Table: "users", Name: "id", Alias: "id"},
						&Column{Table: "users", Name: "name", Alias: "name"},
						&Column{Table: "users", Name: "nickname", Alias: "nickname"},
						&Column{Table: "users", Name: "age", Alias: "age"},
					},
					Input: &Limit{
						Limit: &Int64Expr{Value: 5},
						Input: &Offset{
							Offset: &Int64Expr{Value: 10},
							Input: &Scan{
								Table: &Table{Name: "users"},
							},
						},
					},
				},
			},
		},
		{
			name: `select * from users where id = 5 order by id, name limit 5 offset 10`,
			stmt: &parser.SelectStatement{
				SelectExprs: []parser.SelectExpr{
					&parser.StarExpr{},
				},
				From: &parser.AliasedTableExpr{
					Expr: &parser.TableName{
						Name: "users",
					},
				},
				Where: &parser.Where{
					Expr: &parser.ComparisonExpr{
						Left:     &parser.ColName{Name: "id"},
						Operator: parser.Op_EQ,
						Right:    &parser.Value{Val: "5"},
					},
				},
				OrderBy: []*parser.Order{
					{
						Expr:      &parser.ColName{Name: "id"},
						Direction: parser.OrderDirection_ASC,
					},
					{
						Expr:      &parser.ColName{Name: "name"},
						Direction: parser.OrderDirection_ASC,
					},
				},
				Limit: &parser.Limit{
					Count:  5,
					Offset: 10,
				},
			},
			expected: &SelectPlan{
				LogicalPlan: &Projection{
					Columns: []Expr{
						&Column{Table: "users", Name: "id", Alias: "id"},
						&Column{Table: "users", Name: "name", Alias: "name"},
						&Column{Table: "users", Name: "nickname", Alias: "nickname"},
						&Column{Table: "users", Name: "age", Alias: "age"},
					},
					Input: &Limit{
						Limit: &Int64Expr{Value: 5},
						Input: &Offset{
							Offset: &Int64Expr{Value: 10},
							Input: &OrderBy{
								Columns: []Expr{
									&Column{Name: "id"},
									&Column{Name: "name"},
								},
								Directirons: []string{"asc", "asc"},
								Input: &Selection{
									Filter: &EqualityFilter{
										Column: &Column{Name: "id"},
										Value:  &Int64Expr{Value: int64(5)},
									},
									Input: &Scan{
										Table: &Table{Name: "users"},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			plan := New(c).PlanSelect(test.stmt)
			testutil.MustEqual(t, plan, test.expected)
		})
	}
}
