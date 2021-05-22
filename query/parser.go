package query

import (
	"errors"
	"fmt"
)

type Parser struct {
	tokens []*Token
	index  int
}

func NewParser(tokens []*Token) *Parser {
	return &Parser{tokens: tokens}
}

func (p *Parser) consume(tk TokenKind) bool {
	if p.index >= len(p.tokens) {
		return false
	}

	if p.tokens[p.index].Kind == tk {
		p.index++
		return true
	}

	return false
}

func (p *Parser) mustBeType() *Token {
	types := []TokenKind{BOOL, INT64, FLOAT64, BYTES, STRING, TIMESTAMP}
	cur := p.tokens[p.index]

	for _, typ := range types {
		if cur.Kind == typ {
			p.index++
			return cur
		}
	}

	panic(fmt.Sprintf("any type is expected but got %v", cur.Kind))
}

func (p *Parser) mustBeOr(ks ...TokenKind) *Token {
	cur := p.tokens[p.index]
	for _, k := range ks {
		if cur.Kind == k {
			p.index++
			return cur
		}
	}

	panic(fmt.Sprintf("one of %v is expected but got %v", ks, cur.Kind))
}

func (p *Parser) mustBe(k TokenKind) *Token {
	cur := p.tokens[p.index]
	if cur.Kind != k {
		panic(fmt.Sprintf("%v is expected but got %v", k, cur.Kind))
	}

	p.index++
	return cur
}

func (p *Parser) parseCreateTableStmt() *Statement {
	p.mustBe(TABLE)
	tbl := p.mustBe(STRING_VAL)
	p.mustBe(LPAREN)

	var columns, types []string
	pk := ""
	for {
		column := p.mustBe(STRING_VAL)
		typ := p.mustBeType()

		columns = append(columns, column.Val)
		types = append(types, typ.Kind.String())

		if p.consume(PRIMARY) {
			if pk != "" {
				panic(fmt.Sprintf("composite primary key is not implemented as of now"))
			}

			p.mustBe(KEY)
			pk = column.Val
		}

		if !p.consume(COMMA) {
			break
		}
	}

	p.mustBe(RPAREN)
	p.mustBe(EOF)

	return &Statement{
		Typ: CREATE_TABLE_STMT,
		CreateTable: &CreateTableStatement{
			Table:         tbl.Val,
			Columns:       columns,
			Types:         types,
			PrimaryKeyCol: pk,
		},
	}
}

func (p *Parser) parseInsertStmt() *Statement {
	p.mustBe(INTO)
	tbl := p.mustBe(STRING_VAL)

	columns := []string{}
	if p.consume(LPAREN) {
		// insert with columns e.g. insert into users (id, name) values ...
		for {
			column := p.mustBe(STRING_VAL)
			columns = append(columns, column.Val)

			if !p.consume(COMMA) {
				break
			}
		}

		p.mustBe(RPAREN)

	}

	p.mustBe(VALUES)

	rows := [][]string{}
	for { // for-loop to read multiple rows
		p.mustBe(LPAREN)

		values := []string{}
		for { // for-loop to read multiple values in a row
			val := p.mustBeOr(STRING_VAL, NUMBER_VAL)
			values = append(values, val.Val)

			if !p.consume(COMMA) {
				break
			}
		}

		p.mustBe(RPAREN)

		rows = append(rows, values)

		if !p.consume(COMMA) {
			break
		}
	}

	p.mustBe(EOF)

	return &Statement{
		Typ: INSERT_STMT,
		Insert: &InsertStatement{
			Table:   tbl.Val,
			Columns: columns,
			Rows:    rows,
		},
	}
}

// func (p *Parser) parseSelectStmt() (*Statement, error) {
// }

func (p *Parser) Parse() (stmt *Statement, err error) {
	// Parse() uses panic/recover for non-local exits purpose.
	// Usually it should not be used, but chaining error return makes significantly drops the readability.
	defer func() {
		if r := recover(); r != nil {
			stmt = nil
			switch x := r.(type) {
			case string:
				err = errors.New(x)
			case error:
				err = x
			default:
				err = errors.New("sdb bug: unknown panic happened")
			}
		}
	}()

	switch {
	case p.consume(CREATE):
		return p.parseCreateTableStmt(), nil
	case p.consume(INSERT):
		return p.parseInsertStmt(), nil
	// case p.consume(SELECT):
	// 	return p.parseSelectStmt()
	default:
		return nil, fmt.Errorf("unexpected leading token")
	}
}
