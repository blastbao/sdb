package parser

import (
	"fmt"
	"testing"

	"github.com/dty1er/sdb/catalog"
	"github.com/dty1er/sdb/schema"
	"github.com/dty1er/sdb/testutil"
)

func TestValidator_Validate_CreateTable(t *testing.T) {
	c := &catalog.Catalog{
		Tables: map[string]*schema.Table{
			"items": {
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
				},
				PrimaryKeyIndex: 0,
			},
		},
	}
	tooManyColumns := []string{}
	for i := 1; i <= 101; i++ {
		tooManyColumns = append(tooManyColumns, fmt.Sprintf("id_%d", i))
	}
	tooManyTypes := []string{}
	for i := 1; i <= 101; i++ {
		tooManyTypes = append(tooManyTypes, "INT64")
	}
	maxColumns := []string{}
	for i := 1; i <= 100; i++ {
		maxColumns = append(maxColumns, fmt.Sprintf("id_%d", i))
	}
	maxTypes := []string{}
	for i := 1; i <= 100; i++ {
		maxTypes = append(maxTypes, "INT64")
	}
	tests := []struct {
		name      string
		stmt      *CreateTableStatement
		catalog   *catalog.Catalog
		wantError bool
	}{
		{
			name: "no pkey",
			stmt: &CreateTableStatement{
				Table:         "users",
				Columns:       []string{"id", "name", "verified", "registered"},
				Types:         []string{"INT64", "STRING", "BOOL", "TIMESTAMP"},
				PrimaryKeyCol: "",
			},
			catalog:   c,
			wantError: true,
		},
		{
			name: "table is not found",
			stmt: &CreateTableStatement{
				Table:         "items",
				Columns:       []string{"id", "name", "verified", "registered"},
				Types:         []string{"INT64", "STRING", "BOOL", "TIMESTAMP"},
				PrimaryKeyCol: "id",
			},
			catalog:   c,
			wantError: true,
		},
		{
			name: "columns and types len mismatch",
			stmt: &CreateTableStatement{
				Table:         "users",
				Columns:       []string{"id", "name", "verified"},
				Types:         []string{"INT64", "STRING", "BOOL", "TIMESTAMP"},
				PrimaryKeyCol: "id",
			},
			catalog:   c,
			wantError: true,
		},
		{
			name: "too many columns",
			stmt: &CreateTableStatement{
				Table:         "users",
				Columns:       tooManyColumns,
				Types:         tooManyTypes,
				PrimaryKeyCol: "id",
			},
			catalog:   c,
			wantError: true,
		},
		{
			name: "pkey is not in columns",
			stmt: &CreateTableStatement{
				Table:         "users",
				Columns:       []string{"id", "name", "verified", "registered"},
				Types:         []string{"INT64", "STRING", "BOOL", "TIMESTAMP"},
				PrimaryKeyCol: "xxx",
			},
			catalog:   c,
			wantError: true,
		},
		{
			name: "invalid col name",
			stmt: &CreateTableStatement{
				Table:         "users",
				Columns:       []string{"id", "last-name", "verified", "registered"},
				Types:         []string{"INT64", "STRING", "BOOL", "TIMESTAMP"},
				PrimaryKeyCol: "id",
			},
			catalog:   c,
			wantError: true,
		},
		{
			name: "invalid type name",
			stmt: &CreateTableStatement{
				Table:         "users",
				Columns:       []string{"id", "name", "verified", "registered"},
				Types:         []string{"INT32", "STRING", "BOOL", "TIMESTAMP"},
				PrimaryKeyCol: "id",
			},
			catalog:   c,
			wantError: true,
		},
		{
			name: "ok",
			stmt: &CreateTableStatement{
				Table:         "users",
				Columns:       maxColumns,
				Types:         maxTypes,
				PrimaryKeyCol: "id_1",
			},
			catalog:   c,
			wantError: false,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			v := newValidator(test.stmt, test.catalog)
			err := v.validate()
			testutil.MustEqual(t, err != nil, test.wantError)
		})
	}
}

func TestValidator_Validate_Insert(t *testing.T) {
	c := &catalog.Catalog{
		Tables: map[string]*schema.Table{
			"students": {
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
			},
		},
	}
	tooManyRows := [][]string{}
	for i := 1; i <= 1001; i++ {
		tooManyRows = append(tooManyRows, []string{fmt.Sprintf("%d", i)})
	}
	tests := []struct {
		name      string
		stmt      *InsertStatement
		catalog   *catalog.Catalog
		wantError bool
	}{
		{
			name: "table not found in the catalog",
			stmt: &InsertStatement{
				Table:   "users",
				Columns: []string{"nickname", "id", "name"},
				Rows: [][]string{
					{"Art", "1", "Arthur"},
					{"Cliff", "3", "Clifford"},
					{"Ed", "2", "Edgar"},
				},
			},
			catalog:   c,
			wantError: true,
		},
		{
			name: "too many rows",
			stmt: &InsertStatement{
				Table:   "students",
				Columns: []string{"id"},
				Rows:    tooManyRows,
			},
			catalog:   c,
			wantError: true,
		},
		{
			name: "columns and rows length invalid",
			stmt: &InsertStatement{
				Table:   "students",
				Columns: []string{"nickname", "id", "name"},
				Rows: [][]string{
					{"Art", "1", "Arthur"},
					{"Cliff", "3", "Clifford"},
					{"Ed", "2"}, // last column value missing
				},
			},
			catalog:   c,
			wantError: true,
		},
		{
			name: "type invalid",
			stmt: &InsertStatement{
				Table:   "students",
				Columns: []string{"nickname", "id", "name"},
				Rows: [][]string{
					{"Art", "1", "Arthur"},
					{"Cliff", "3", "Clifford"},
					{"Ed", "a", "Edgar"}, // invalid id
				},
			},
			catalog:   c,
			wantError: true,
		},
		{
			name: "ok",
			stmt: &InsertStatement{
				Table:   "students",
				Columns: []string{"nickname", "id", "name"},
				Rows: [][]string{
					{"Art", "1", "Arthur"},
					{"Cliff", "3", "Clifford"},
					{"Ed", "2", "Edgar"},
				},
			},
			catalog:   c,
			wantError: false,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			v := newValidator(test.stmt, test.catalog)
			err := v.validate()
			testutil.MustEqual(t, err != nil, test.wantError)
		})
	}
}
