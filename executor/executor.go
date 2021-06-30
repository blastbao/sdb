package executor

import (
	"fmt"

	"github.com/dty1er/sdb/engine"
	"github.com/dty1er/sdb/planner"
	"github.com/dty1er/sdb/sdb"
)

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
		e.engine.CreateIndex(index.Table, index.Name)
	}

	return &sdb.Result{Code: "OK", RS: &sdb.ResultSet{Message: "table is successfully created"}}, nil
}

func (e *Executor) execInsert(plan *planner.InsertPlan) (*sdb.Result, error) {
	for i, v := range plan.Values {
		tuple := engine.NewTuple(v, plan.Table.PrimaryKeyIndex)

		// put in the table
		if err := e.engine.InsertTuple(plan.Table.Name, tuple); err != nil {
			return nil, err
		}

		// save tuple in index
		indices := plan.Indices[i]
		for j := range indices.Keys {
			if err := e.engine.InsertIndex(plan.Table.Name, indices.Idx[j].Name, indices.Keys[j], tuple); err != nil {
				return nil, err
			}
		}
	}

	return &sdb.Result{RS: &sdb.ResultSet{Message: "record successfully inserted"}}, nil
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
