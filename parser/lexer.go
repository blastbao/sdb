package parser

import (
	"errors"
	"fmt"

	"github.com/dty1er/sdb/sdb"
)

type lexer struct {
	tokens []*token
	index  int
}

func newLexer(tokens []*token) *lexer {
	return &lexer{tokens: tokens}
}

func (l *lexer) consume(tk tokenKind) bool {
	if l.index >= len(l.tokens) {
		return false
	}

	if l.tokens[l.index].Kind == tk {
		l.index++
		return true
	}

	return false
}

func (l *lexer) Atoi(s string) int {
	i, err := strconv.Atoi(s)
	if err != nil {
		panic(err)
	}
	return i
}

func (l *lexer) mustBeStringVal() *token {
	cur := l.tokens[l.index]
	if cur.Kind != STRING_VAL {
		panic(fmt.Sprintf("String value is expected but got %v", cur.Kind))
	}

	l.index++
	return cur
}

func (l *lexer) mustBeType() *token {
	types := []tokenKind{BOOL, INT64, FLOAT64, BYTES, STRING, TIMESTAMP}
	cur := l.tokens[l.index]

	for _, typ := range types {
		if cur.Kind == typ {
			l.index++
			return cur
		}
	}

	panic(fmt.Sprintf("any type is expected but got %v", cur.Kind))
}

func (l *lexer) mustBeOr(ks ...tokenKind) *token {
	cur := l.tokens[l.index]
	for _, k := range ks {
		if cur.Kind == k {
			l.index++
			return cur
		}
	}

	panic(fmt.Sprintf("one of %v is expected but got %v", ks, cur.Kind))
}

func (l *lexer) mustBe(k tokenKind) *token {
	cur := l.tokens[l.index]
	if cur.Kind != k {
		// lexer uses panic because returning error makes the code complicated.
		// It must be recovered on caller side.
		panic(fmt.Sprintf("%v is expected but got %v", k, cur.Kind))
	}

	l.index++
	return cur
}

func (l *lexer) lexCreateTableStmt() *CreateTableStatement {
	l.mustBe(TABLE)
	tbl := l.mustBe(STRING_VAL)
	l.mustBe(LPAREN)

	var columns, types []string
	pk := ""
	for {
		column := l.mustBe(STRING_VAL)
		typ := l.mustBeType()

		columns = append(columns, column.Val)
		types = append(types, typ.Kind.String())

		if l.consume(PRIMARY) {
			if pk != "" {
				panic(fmt.Sprintf("composite primary key is not implemented as of now"))
			}

			l.mustBe(KEY)
			pk = column.Val
		}

		if !l.consume(COMMA) {
			break
		}
	}

	l.mustBe(RPAREN)
	l.mustBe(EOF)

	return &CreateTableStatement{
		Table:         tbl.Val,
		Columns:       columns,
		Types:         types,
		PrimaryKeyCol: pk,
	}
}

func (l *lexer) lexInsertStmt() *InsertStatement {
	l.mustBe(INTO)
	tbl := l.mustBe(STRING_VAL)

	columns := []string{}
	if l.consume(LPAREN) {
		// insert with columns e.g. insert into users (id, name) values ...
		for {
			column := l.mustBe(STRING_VAL)
			columns = append(columns, column.Val)

			if !l.consume(COMMA) {
				break
			}
		}

		l.mustBe(RPAREN)

	}

	l.mustBe(VALUES)

	rows := [][]string{}
	for { // for-loop to read multiple rows
		l.mustBe(LPAREN)

		values := []string{}
		for { // for-loop to read multiple values in a row
			val := l.mustBeOr(STRING_VAL, NUMBER_VAL)
			values = append(values, val.Val)

			if !l.consume(COMMA) {
				break
			}
		}

		l.mustBe(RPAREN)

		rows = append(rows, values)

		if !l.consume(COMMA) {
			break
		}
	}

	l.mustBe(EOF)

	return &InsertStatement{
		Table:   tbl.Val,
		Columns: columns,
		Rows:    rows,
	}
}

func (l *lexer) lex() (stmt sdb.Statement, err error) {
	// lex() uses panic/recover for non-local exits purpose.
	// Usually they are not recommended to be used, but chaining error return significantly drops the readability.
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
	case l.consume(CREATE):
		return l.lexCreateTableStmt(), nil
	case l.consume(INSERT):
		return l.lexInsertStmt(), nil
	case l.consume(SELECT):
		return l.lexSelectStmt(), nil
	default:
		return nil, fmt.Errorf("unexpected leading token")
	}
}
