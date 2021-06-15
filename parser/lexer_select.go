package parser

import "github.com/dty1er/sdb/sdb"

type Expr interface {
	isExpr()
}

type AndExpr struct {
	Left, Right Expr
}

type OrExpr struct {
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

	Expr  SimpleTableExpr
	Table string
	As    string
}

type Where struct {
	Expr Expr
}

type OrderDirection uint8

const (
	OrderDirection_ASC OperatorType = iota + 1
	OrderDirection_DESC
)

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
	From        []TableExpr
	Where       *Where
	OrderBy     []*Order
	Limit       *Limit
}

func (l *lexer) lexSelectStmt() *SelectStatement {
	return nil
}
