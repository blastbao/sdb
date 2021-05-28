package sdb

import (
	"fmt"

	"github.com/dty1er/sdb/executor"
	"github.com/dty1er/sdb/parser"
)

type Request struct {
	Query string // Raw SQL
}

type Response struct {
	Result string     // "OK" or "NG"
	RS     *ResultSet // filled when "OK"
	Error  *Error     // filled when "NG"
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

func (sdb *SDB) ExecQuery(req *Request) *Response {
	// TODO: encapsulate tokenize, parse, and validate into parser
	tokenizer := parser.NewTokenizer(req.Query)
	tokens := tokenizer.Tokenize()

	p := parser.NewParser(tokens)
	stmt, err := p.Parse()
	if err != nil {
		return &Response{
			Result: "NG",
			Error:  &Error{Message: fmt.Sprintf("failed to parse query: %s", err)},
		}
	}

	validator := parser.NewValidator(stmt, sdb.engine)
	if err := validator.Validate(); err != nil {
		return &Response{
			Result: "NG",
			Error:  &Error{Message: fmt.Sprintf("failed to parse query: %s", err)},
		}
	}

	// TODO: make planner pkg and make plan, then pass it to executor

	executor := executor.NewExecutor(sdb.engine)
	result, err := executor.Execute(stmt)
	if err != nil {
		return &Response{
			Result: "NG",
			Error:  &Error{Message: fmt.Sprintf("failed to execute query: %s", err)},
		}
	}

	successResp := &Response{
		Result: "OK",
		RS: &ResultSet{
			Message: result.Message,
		},
	}

	switch stmt.Typ {
	case parser.SELECT_STMT:
		successResp.RS.Columns = result.Columns
		successResp.RS.Values = result.Values
		successResp.RS.Count = result.Count
	case parser.UPDATE_STMT, parser.DELETE_STMT:
		successResp.RS.Count = result.Count
	}

	return successResp
}
