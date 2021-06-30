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

	return &sdb.Result{Code: "OK", RS: &sdb.ResultSet{Message: "record successfully inserted"}}, nil
}

func (e *Executor) execSelect(plan *planner.SelectPlan) (*sdb.Result, error) {
	tbl := "users"
	resultSet := []sdb.Tuple{}
	pj := plan.LogicalPlan.(*planner.Projection)
	for {
		tuple := pj.Input.Next(e.engine)
		if tuple == nil {
			break
		}

		t := pj.Input.Process(tuple)
		if t != nil {
			resultSet = append(resultSet, t)
		}
	}

	// do projection
	projectionCols := []string{}
	for _, col := range pj.Columns {
		c := col.(*planner.Column)
		projectionCols = append(projectionCols, c.Name)
	}

	projectionColIndices := []int{}
	for i, colDef := range e.catalog.GetTable(tbl).Columns {
		for _, col := range projectionCols {
			if colDef.Name == col {
				projectionColIndices = append(projectionColIndices, i)
			}
		}
	}

	rs := []sdb.Tuple{}
	for _, result := range resultSet {
		rs = append(rs, result.Projection(projectionColIndices))
	}

	return &sdb.Result{
		Code: "OK",
		RS: &sdb.ResultSet{
			Message: "successfully fetched records",
			Columns: projectionCols,
			Values:  rs,
			Count:   len(rs),
		},
	}, nil
}

func (e *Executor) Execute(plan sdb.Plan) (*sdb.Result, error) {
	switch p := plan.(type) {
	case *planner.CreateTablePlan:
		return e.execCreateTable(p)
	case *planner.InsertPlan:
		return e.execInsert(p)
	case *planner.SelectPlan:
		return e.execSelect(p)
	default:
		return nil, fmt.Errorf("unexpected statement type")
	}
}
