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
				Types:         []string{"INT64", "STRING", "BOOL", "TIMESTAMP"},
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
				From: []TableExpr{
					&AliasedTableExpr{
						Expr: &TableName{
							Name: "users",
						},
					},
				},
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.query, func(t *testing.T) {
			p := New(nil)
			stmt, err := p.parse(test.query)
			testutil.MustEqual(t, err != nil, test.wantError)
			if !test.wantError {
				testutil.MustEqual(t, stmt.(*SelectStatement), test.expected)
			}
		})
	}
}
