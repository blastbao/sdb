package executor

import (
	"fmt"

	"github.com/dty1er/sdb/engine"
	"github.com/dty1er/sdb/planner"
	"github.com/dty1er/sdb/sdb"
)

type ExecutionResult struct {
	Message string
	Columns []string
	Values  [][]string
	Count   int
}

type Executor struct {
	engine  sdb.Engine
	catalog sdb.Catalog
}

func New(engine sdb.Engine, catalog sdb.Catalog) *Executor {
	return &Executor{engine: engine, catalog: catalog}
}

func (e *Executor) execCreateTable(plan *planner.CreateTablePlan) (*sdb.Result, error) {
	if err := e.catalog.AddTable(plan.Table, plan.Columns, plan.Indices); err != nil {
		return nil, err
	}

	for _, index := range plan.Indices {
		e.engine.CreateIndex(index.Name)
	}

	return &sdb.Result{RS: &sdb.ResultSet{Message: "table is successfully created"}}, nil
}

// WIP
func (e *Executor) execInsert(plan *planner.InsertPlan) (*sdb.Result, error) {
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

	return &sdb.Result{RS: &sdb.ResultSet{Message: "a record successfully inserted"}}, nil
}

func (e *Executor) Execute(plan sdb.Plan) (*sdb.Result, error) {
	switch p := plan.(type) {
	case *planner.CreateTablePlan:
		return e.execCreateTable(p)
	case *planner.InsertPlan:
		return e.execInsert(p)
	default:
		return nil, fmt.Errorf("unexpected statement type")
	}
}
