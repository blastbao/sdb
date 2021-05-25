package query

import (
	"fmt"
	"testing"

	"github.com/dty1er/sdb/engine"
	"github.com/dty1er/sdb/testutil"
)

func TestValidator_Validate_CreateTable(t *testing.T) {
	catalog := &engine.Catalog{
		Tables: map[string]*engine.Table{
			"users": {
				Columns: []string{},
				Types:   []engine.Type{},
			},
		},
	}
	tooManyColumns := []string{}
	for i := 1; i <= 101; i++ {
		tooManyColumns = append(tooManyColumns, fmt.Sprintf("id_%d", i))
	}
	tooManyTypes := []string{}
	for i := 1; i <= 101; i++ {
		tooManyColumns = append(tooManyTypes, "INT64")
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
				Table:         "xxx",
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
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			p := NewValidator(&Statement{Typ: CREATE_TABLE_STMT, CreateTable: test.stmt}, test.catalog)
			err := p.Validate()
			testutil.MustEqual(t, err != nil, test.wantError)
		})
	}
}
