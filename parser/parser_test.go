package parser

import (
	"testing"

	"github.com/dty1er/sdb/sdb"
	"github.com/dty1er/sdb/testutil"
)

func TestParser_parse_CreateTable(t *testing.T) {
	tests := []struct {
		name      string
		query     string
		expected  sdb.Statement
		wantError bool
	}{
		{
			name:  "ok",
			query: `create table users (id int64 primary key, name string, verified bool, registered timestamp);`,
			expected: &CreateTableStatement{
				Table:         "users",
				Columns:       []string{"id", "name", "verified", "registered"},
				Types:         []string{"int64", "string", "bool", "timestamp"},
				PrimaryKeyCol: "id",
			},
		},
		{
			name:      "failure: composite primary key",
			query:     `create table users (id int64 primary key, name string primary key, verified bool);`,
			wantError: true,
		},
		{
			name:      "failure: no table name",
			query:     `create table (id int64 primary key, name string, verified bool);`,
			wantError: true,
		},
		{
			name:      "failure: no comma",
			query:     `create table (id int64 primary key, name string verified bool);`,
			wantError: true,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			p := New(nil)
			stmt, err := p.parse(test.query)
			testutil.MustEqual(t, err != nil, test.wantError)
			if !test.wantError {
				testutil.MustEqual(t, stmt.(*CreateTableStatement), test.expected)
			}
		})
	}
}

func TestParser_parse_Insert(t *testing.T) {
	tests := []struct {
		name      string
		query     string
		expected  sdb.Statement
		wantError bool
	}{
		{
			name:  "ok: with col names",
			query: `insert into users (id, name, verified, registered) values (1, "bob", true, "2021-05-01 17:59:59"), (2, "alice", false, "2021-05-02 17:59:59");`,
			expected: &InsertStatement{
				Table:   "users",
				Columns: []string{"id", "name", "verified", "registered"},
				Rows: [][]string{
					{"1", "bob", "true", "2021-05-01 17:59:59"},
					{"2", "alice", "false", "2021-05-02 17:59:59"},
				},
			},
		},
		{
			name:  "ok: without col names",
			query: `insert into users values (1, "bob", true, "2021-05-01 17:59:59"), (2, "alice", false, "2021-05-02 17:59:59");`,
			expected: &InsertStatement{
				Table:   "users",
				Columns: []string{},
				Rows: [][]string{
					{"1", "bob", "true", "2021-05-01 17:59:59"},
					{"2", "alice", "false", "2021-05-02 17:59:59"},
				},
			},
		},
		{
			name:      "failure: no table name",
			query:     `insert into values (1, "bob", true, "2021-05-01 17:59:59"), (2, "alice", false, "2021-05-02 17:59:59");`,
			wantError: true,
		},
		{
			name:      "failure: no paren",
			query:     `insert into users (id, name, verified, registered) values (1, "bob", true, "2021-05-01 17:59:59"), 2, "alice", false, "2021-05-02 17:59:59");`,
			wantError: true,
		},
		{
			name:      "failure: no comma",
			query:     `insert into users (id, name, verified, registered) values (1, "bob", true, "2021-05-01 17:59:59"), (2 "alice", false, "2021-05-02 17:59:59");`,
			wantError: true,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			p := New(nil)
			stmt, err := p.parse(test.query)
			testutil.MustEqual(t, err != nil, test.wantError)
			if !test.wantError {
				testutil.MustEqual(t, stmt.(*InsertStatement), test.expected)
			}
		})
	}
}

func TestParser_parse_Select(t *testing.T) {
	tests := []struct {
		name      string
		query     string
		expected  sdb.Statement
		wantError bool
	}{
		{
			name:  "ok: simple",
			query: `select * from users`,
			expected: &SelectStatement{
				SelectExprs: []SelectExpr{
					&StarExpr{},
				},
				From: &AliasedTableExpr{
					Expr: &TableName{
						Name: "users",
					},
				},
			},
		},
		{
			name:  "ok: distinct",
			query: `select distinct id from users`,
			expected: &SelectStatement{
				Distinct: true,
				SelectExprs: []SelectExpr{
					&AliasedExpr{Expr: &ColName{Name: "id"}},
				},
				From: &AliasedTableExpr{
					Expr: &TableName{
						Name: "users",
					},
				},
			},
		},
		{
			name:  "ok: specify columns",
			query: `select id, name from users`,
			expected: &SelectStatement{
				SelectExprs: []SelectExpr{
					&AliasedExpr{Expr: &ColName{Name: "id"}},
					&AliasedExpr{Expr: &ColName{Name: "name"}},
				},
				From: &AliasedTableExpr{
					Expr: &TableName{
						Name: "users",
					},
				},
			},
		},
		{
			name:  "ok: As",
			query: `select id, name as n from users`,
			expected: &SelectStatement{
				SelectExprs: []SelectExpr{
					&AliasedExpr{Expr: &ColName{Name: "id"}},
					&AliasedExpr{Expr: &ColName{Name: "name"}, As: "n"},
				},
				From: &AliasedTableExpr{
					Expr: &TableName{
						Name: "users",
					},
				},
			},
		},
		{
			name:  "ok: simple where",
			query: `select * from users where id = 1`,
			expected: &SelectStatement{
				SelectExprs: []SelectExpr{
					&StarExpr{},
				},
				From: &AliasedTableExpr{
					Expr: &TableName{
						Name: "users",
					},
				},
				Where: &Where{
					Expr: &ComparisonExpr{
						Left: &ColName{
							Name: "id",
						},
						Operator: Op_EQ,
						Right: &Value{
							Val: "1",
						},
					},
				},
			},
		},
		{
			name:  "ok: simple where",
			query: `select * from users where id = 1`,
			expected: &SelectStatement{
				SelectExprs: []SelectExpr{
					&StarExpr{},
				},
				From: &AliasedTableExpr{
					Expr: &TableName{
						Name: "users",
					},
				},
				Where: &Where{
					Expr: &ComparisonExpr{
						Left: &ColName{
							Name: "id",
						},
						Operator: Op_EQ,
						Right: &Value{
							Val: "1",
						},
					},
				},
			},
		},
		{
			name:  "ok: order by 1",
			query: `select * from users order by id`,
			expected: &SelectStatement{
				SelectExprs: []SelectExpr{
					&StarExpr{},
				},
				From: &AliasedTableExpr{
					Expr: &TableName{
						Name: "users",
					},
				},
				OrderBy: []*Order{
					{
						Expr:      &ColName{Name: "id"},
						Direction: OrderDirection_ASC,
					},
				},
			},
		},
		{
			name:  "ok: order by 2",
			query: `select * from users order by id desc`,
			expected: &SelectStatement{
				SelectExprs: []SelectExpr{
					&StarExpr{},
				},
				From: &AliasedTableExpr{
					Expr: &TableName{
						Name: "users",
					},
				},
				OrderBy: []*Order{
					{
						Expr:      &ColName{Name: "id"},
						Direction: OrderDirection_DESC,
					},
				},
			},
		},
		{
			name:  "ok: order by 3",
			query: `select * from users order by id desc, name asc`,
			expected: &SelectStatement{
				SelectExprs: []SelectExpr{
					&StarExpr{},
				},
				From: &AliasedTableExpr{
					Expr: &TableName{
						Name: "users",
					},
				},
				OrderBy: []*Order{
					{
						Expr:      &ColName{Name: "id"},
						Direction: OrderDirection_DESC,
					},
					{
						Expr:      &ColName{Name: "name"},
						Direction: OrderDirection_ASC,
					},
				},
			},
		},
		{
			name:  "ok: order by 4",
			query: `select * from users order by id, name asc`,
			expected: &SelectStatement{
				SelectExprs: []SelectExpr{
					&StarExpr{},
				},
				From: &AliasedTableExpr{
					Expr: &TableName{
						Name: "users",
					},
				},
				OrderBy: []*Order{
					{
						Expr:      &ColName{Name: "id"},
						Direction: OrderDirection_ASC,
					},
					{
						Expr:      &ColName{Name: "name"},
						Direction: OrderDirection_ASC,
					},
				},
			},
		},
		{
			name:  "ok: limit 1",
			query: `select * from users limit 5`,
			expected: &SelectStatement{
				SelectExprs: []SelectExpr{
					&StarExpr{},
				},
				From: &AliasedTableExpr{
					Expr: &TableName{
						Name: "users",
					},
				},
				Limit: &Limit{
					Offset: 0,
					Count:  5,
				},
			},
		},
		{
			name:  "ok: limit 2",
			query: `select * from users limit 2, 5`,
			expected: &SelectStatement{
				SelectExprs: []SelectExpr{
					&StarExpr{},
				},
				From: &AliasedTableExpr{
					Expr: &TableName{
						Name: "users",
					},
				},
				Limit: &Limit{
					Offset: 2,
					Count:  5,
				},
			},
		},
		{
			name:  "ok: limit 3",
			query: `select * from users limit 2 offset 5`,
			expected: &SelectStatement{
				SelectExprs: []SelectExpr{
					&StarExpr{},
				},
				From: &AliasedTableExpr{
					Expr: &TableName{
						Name: "users",
					},
				},
				Limit: &Limit{
					Offset: 5,
					Count:  2,
				},
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			p := New(nil)
			stmt, err := p.parse(test.query)
			testutil.MustEqual(t, err != nil, test.wantError)
			if !test.wantError {
				testutil.MustEqual(t, stmt.(*SelectStatement), test.expected)
			}
		})
	}
}
