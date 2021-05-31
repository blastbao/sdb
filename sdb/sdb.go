package sdb

import (
	"fmt"
)

type SDB struct {
	parser   Parser
	planner  Planner
	catalog  Catalog
	executor Executor
	engine   Engine
}

type Statement interface {
	isStatement()
}

type Parser interface {
	Parse(sql string) (Statement, error)
}

type Query interface {
	isQuery()
}

type Plan interface {
	isPlan()
}

type Planner interface {
	Plan(stmt Statement) (Plan, error)
}

type Catalog interface {
	AddTable(table string, columns, types []string, pkey string) error
	FindTable(table string) bool
}

type Executor interface {
	// TODO: consider interface
	Execute(plan Plan) (Result, error)
}

type Parameter struct {
	Query string // Raw SQL
	// TODO: define transaction context etc.
}

type Result struct {
	Code  string     // "OK" or "NG" for now
	RS    *ResultSet // filled when "OK"
	Error *Error     // filled when "NG"
}

type Error struct {
	Message string
}

type ResultSet struct {
	Message string
	Columns []string   // empty when insert, update, delete
	Values  [][]string // empty when insert, update, delete
	Count   int        // empty when insert
}

type Engine interface {
	Shutdown() error
}

func New(parser Parser, planner Planner, catalog Catalog, executor Executor, engine Engine) *SDB {
	return &SDB{
		parser:   parser,
		planner:  planner,
		catalog:  catalog,
		executor: executor,
		engine:   engine,
	}
}

func (sdb *SDB) ExecuteQuery(param *Parameter) *Result {
	stmt, err := sdb.parser.Parse(param.Query)
	if err != nil {
		return &Result{
			Code: "NG",
			// FUTURE WORK: should define and return &ParserError to return
			// which part of the query looks wrong
			Error: &Error{Message: fmt.Sprintf("failed to parse query: %s", err)},
		}
	}

	plan, err := sdb.planner.Plan(stmt)
	if err != nil {
		return &Result{
			Code: "NG",
			// FUTURE WORK: should define and return &ParserError to return
			// which part of the query looks wrong
			Error: &Error{Message: fmt.Sprintf("failed to plan query: %s", err)},
		}
	}

	result, err := sdb.executor.Execute(plan)
	if err != nil {
		return &Result{
			Code:  "NG",
			Error: &Error{Message: fmt.Sprintf("failed to execute query: %s", err)},
		}
	}

	successResp := &Result{
		Code: "OK",
		RS: &ResultSet{
			Message: result.RS.Message,
		},
	}

	// switch stmt.Typ {
	// case parser.SELECT_STMT:
	// 	successResp.RS.Columns = result.Columns
	// 	successResp.RS.Values = result.Values
	// 	successResp.RS.Count = result.Count
	// case parser.UPDATE_STMT, parser.DELETE_STMT:
	// 	successResp.RS.Count = result.Count
	// }

	return successResp
}

func (sdb *SDB) Shutdown() error {
	// TODO: should retry on error?
	return sdb.engine.Shutdown()
}
