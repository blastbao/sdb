package query

import (
	"fmt"

	"github.com/dty1er/sdb/engine"
)

type ExecutionResult struct {
	Message string
	Columns []string
	Values  [][]string
	Count   int
}

type Executor struct {
	engine *engine.Engine
}

func NewExecutor(engine *engine.Engine) *Executor {
	return &Executor{engine: engine}
}

func (e *Executor) execCreateTable(stmt *CreateTableStatement) (*ExecutionResult, error) {
	// TODO lock catalog
	if err := e.engine.AddTable(stmt.Table, stmt.Columns, stmt.Types, stmt.PrimaryKeyCol); err != nil {
		return nil, err
	}

	// pkey index is automatically created by default
	e.engine.CreateIndex(fmt.Sprintf("%s_%s", stmt.Table, stmt.PrimaryKeyCol))

	return &ExecutionResult{}, nil
}

func (e *Executor) Execute(stmt *Statement) (*ExecutionResult, error) {
	switch stmt.Typ {
	case CREATE_TABLE_STMT:
		return e.execCreateTable(stmt.CreateTable)
	default:
		return nil, fmt.Errorf("unexpected statement type")
	}
}
