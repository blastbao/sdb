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
		catalog   *engine.Catalog
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
			catalog:   catalog,
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
			catalog:   catalog,
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
			catalog:   catalog,
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
			catalog:   catalog,
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
			catalog:   catalog,
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
			catalog:   catalog,
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
			catalog:   catalog,
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
			catalog:   catalog,
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
