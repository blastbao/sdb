package parser

import (
	"errors"
	"fmt"
	"strconv"

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

func (l *lexer) atoi(s string) int {
	i, err := strconv.Atoi(s)
	if err != nil {
		panic(err)
	}
	return i
}

func (l *lexer) mustBeStringOrNumberVal() *token {
	cur := l.tokens[l.index]
	if cur.Kind != STRING_VAL && cur.Kind != NUMBER_VAL {
		panic(fmt.Sprintf("String value is expected but got %v", cur.Kind))
	}

	l.index++
	return cur
}

func (l *lexer) mustBeNumberVal() *token {
	cur := l.tokens[l.index]
	if cur.Kind != NUMBER_VAL {
		panic(fmt.Sprintf("String value is expected but got %v", cur.Kind))
	}

	l.index++
	return cur
}

func (l *lexer) mustBeStringVal() *token {
	cur := l.tokens[l.index]
	if cur.Kind != STRING_VAL {
		panic(fmt.Sprintf("String value is expected but got %v", cur.Kind))
	}

	l.index++
	return cur
}

func (l *lexer) mustBeOperator() *token {
	types := []tokenKind{EQ, NEQ, LT, LTE, GT, GTE}
	cur := l.tokens[l.index]

	for _, typ := range types {
		if cur.Kind == typ {
			l.index++
			return cur
		}
	}

	panic(fmt.Sprintf("any operator is expected but got %v", cur.Kind))
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

type Expr interface {
	isExpr()
}

type AndExpr struct {
	Expr

	Left, Right Expr
}

type OrExpr struct {
	Expr

	Left, Right Expr
}

type OperatorType uint8

const (
	Op_EQ OperatorType = iota + 1
	Op_NEQ
	Op_LT
	Op_LTE
	Op_GT
	Op_GTE
)

type ComparisonExpr struct {
	Expr

	Left     Expr
	Operator OperatorType
	Right    Expr
}

type Value struct {
	Expr

	Val string
}

type ColName struct {
	Expr

	Name      string
	Qualifier string
}

type SelectExpr interface {
	isSelectExpr()
}

type StarExpr struct {
	SelectExpr

	Table string
}

type AliasedExpr struct {
	SelectExpr

	Expr Expr
	As   string
}

type SimpleTableExpr interface {
	isSimpleTableExpr()
}

type TableName struct {
	SimpleTableExpr

	Name string
}

type TableExpr interface {
	isTableExpr()
}

type AliasedTableExpr struct {
	TableExpr

	Expr SimpleTableExpr
	As   string
}

type Where struct {
	Expr Expr
}

type OrderDirection uint8

const (
	OrderDirection_ASC OrderDirection = iota + 1
	OrderDirection_DESC
)

func (od OrderDirection) String() string {
	if od == OrderDirection_DESC {
		return "desc"
	}

	return "asc"
}

type Order struct {
	Expr      Expr
	Direction OrderDirection
}

type Limit struct {
	Offset int
	Count  int
}

type SelectStatement struct {
	sdb.Statement

	Distinct    bool
	SelectExprs []SelectExpr
	From        TableExpr
	Where       *Where
	OrderBy     []*Order
	Limit       *Limit
}

func (l *lexer) lexComparisonExpr() Expr {
	left := l.mustBeStringVal()
	op := l.mustBeOperator()
	right := l.mustBeStringOrNumberVal()
	c := &ComparisonExpr{
		Left:  &ColName{Name: left.Val},
		Right: &Value{Val: right.Val},
	}
	switch op.Kind {
	case EQ:
		c.Operator = Op_EQ
	case NEQ:
		c.Operator = Op_NEQ
	case LT:
		c.Operator = Op_LT
	case LTE:
		c.Operator = Op_LTE
	case GT:
		c.Operator = Op_GT
	case GTE:
		c.Operator = Op_GTE
	}

	return c
}

func (l *lexer) lexSelectStmt() *SelectStatement {
	stmt := &SelectStatement{}

	if l.consume(DISTINCT) {
		stmt.Distinct = true
	}

	stmt.SelectExprs = []SelectExpr{}
	for {
		switch {
		// FUTURE WORK: support column with qualifier (e.g. SELECT mytable.* or mytable.id)
		case l.consume(ASTERISK):
			stmt.SelectExprs = append(stmt.SelectExprs, &StarExpr{})
		default:
			sv := l.mustBeStringVal()
			e := &AliasedExpr{
				Expr: &ColName{Name: sv.Val},
			}
			if l.consume(AS) {
				sv := l.mustBeStringVal()
				e.As = sv.Val
			}

			stmt.SelectExprs = append(stmt.SelectExprs, e)
		}

		if l.consume(COMMA) {
			continue
		}

		break
	}

	l.mustBe(FROM)

	sv := l.mustBeStringVal()
	stmt.From = &AliasedTableExpr{Expr: &TableName{Name: sv.Val}}

	if l.consume(WHERE) {
		w := &Where{}
		// TODO: Right now it can use only 1 comparison operator as where expression
		e := l.lexComparisonExpr()
		w.Expr = e
		stmt.Where = w
	}

	if l.consume(ORDER) {
		l.mustBe(BY)
		os := []*Order{}
		for {
			o := &Order{}
			col := l.mustBeStringVal()
			o.Expr = &ColName{Name: col.Val}
			if l.consume(ASC) {
				o.Direction = OrderDirection_ASC
			} else if l.consume(DESC) {
				o.Direction = OrderDirection_DESC
			} else {
				o.Direction = OrderDirection_ASC // by default
			}

			os = append(os, o)

			if l.consume(COMMA) {
				continue
			}

			break
		}
		stmt.OrderBy = os
	}

	if l.consume(LIMIT) {
		offset := "0"
		limit := "0"
		offsetOrLimit := l.mustBeNumberVal()
		if l.consume(COMMA) {
			// In case "LIMIT 2, 5", offset is 2, limit is 5
			offset = offsetOrLimit.Val
			limit = l.mustBeNumberVal().Val
		} else if l.consume(OFFSET) {
			// In case "LIMIT 2 OFFSET 5", offset is 5, limit is 2
			limit = offsetOrLimit.Val
			offset = l.mustBeNumberVal().Val
		} else {
			// Just "LIMIT 2", without offset; offset is 0 by default
			limit = offsetOrLimit.Val
		}

		stmt.Limit = &Limit{Offset: l.atoi(offset), Count: l.atoi(limit)}
	}

	return stmt
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
