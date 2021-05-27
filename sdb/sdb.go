package sdb

import (
	"fmt"

	"github.com/dty1er/sdb/query"
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
	tokenizer := query.NewTokenizer(req.Query)
	tokens := tokenizer.Tokenize()

	parser := query.NewParser(tokens)
	stmt, err := parser.Parse()
	if err != nil {
		return &Response{
			Result: "NG",
			Error:  &Error{Message: fmt.Sprintf("failed to parse query: %s", err)},
		}
	}

	validator := query.NewValidator(stmt, sdb.engine)
	if err := validator.Validate(); err != nil {
		return &Response{
			Result: "NG",
			Error:  &Error{Message: fmt.Sprintf("failed to parse query: %s", err)},
		}
	}

	executor := query.NewExecutor(sdb.engine)
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
	case query.SELECT_STMT:
		successResp.RS.Columns = result.Columns
		successResp.RS.Values = result.Values
		successResp.RS.Count = result.Count
	case query.UPDATE_STMT, query.DELETE_STMT:
		successResp.RS.Count = result.Count
	}

	return successResp
}
