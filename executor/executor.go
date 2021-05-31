package executor

import (
	"fmt"

	"github.com/dty1er/sdb/engine"
	"github.com/dty1er/sdb/parser"
	"github.com/dty1er/sdb/sdb"
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

func (e *Executor) execCreateTable(stmt *parser.CreateTableStatement) (*ExecutionResult, error) {
	// TODO lock catalog
	if err := e.engine.AddTable(stmt.Table, stmt.Columns, stmt.Types, stmt.PrimaryKeyCol); err != nil {
		return nil, err
	}

	// pkey index is automatically created by default
	e.engine.CreateIndex(fmt.Sprintf("%s_%s", stmt.Table, stmt.PrimaryKeyCol))

	return &ExecutionResult{}, nil
}

// WIP
func (e *Executor) execInsert(stmt *parser.InsertStatement) (*ExecutionResult, error) {
	vals := []interface{}{}
	for i := 0; i < len(stmt.Rows); i++ {
		row := stmt.Rows[i]
		for j := range row {
			vals = append(vals, row[j])
		}
	}
	t := engine.NewTuple(vals, 0)
	if err := e.engine.InsertTuple(stmt.Table, t); err != nil {
		return nil, err
	}

	// TODO: fix to use idx name from plan
	if err := e.engine.InsertIndex(stmt.Table+"_id", t); err != nil {
		return nil, err
	}

	return &ExecutionResult{Message: "a record successfully inserted"}, nil
}

func (e *Executor) Execute(stmt sdb.Statement) (*ExecutionResult, error) {
	switch s := stmt.(type) {
	case *parser.CreateTableStatement:
		return e.execCreateTable(s)
	case *parser.InsertStatement:
		return e.execInsert(s)
	default:
		return nil, fmt.Errorf("unexpected statement type")
	}
}
