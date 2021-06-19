package parser

import (
	"github.com/dty1er/sdb/sdb"
)

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
