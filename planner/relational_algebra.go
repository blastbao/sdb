package planner

import "time"

type LogicalPlan interface {
	List

	isLogicalPlan()
}

type Expr interface {
	isExpr()
}

type BoolExpr struct {
	Expr

	Value bool
}

type Int64Expr struct {
	Expr

	Value int64
}

type Float64Expr struct {
	Expr

	Value float64
}

type BytesExpr struct {
	Expr

	Value []byte
}

type StringExpr struct {
	Expr

	Value string
}

type TimestampExpr struct {
	Expr

	Value time.Time
}

type Scan struct {
	LogicalPlan

	Table *Table
}

type Column struct {
	Expr

	Table string
	Name  string // TODO: This should be Expr because it can be function, operation, etc.
	Alias string
}

// Projection is a Projection relational algebra operator.
type Projection struct {
	LogicalPlan

	// Columns is a set of column to be picked up.
	Columns []Expr
	// Input is a source of data from which this Projection picks data.
	Input List
}

type Limit struct {
	LogicalPlan

	Limit Expr
	Input List
}

type OrderBy struct {
	LogicalPlan

	Columns     []Expr
	Directirons []string
	Input       List
}

type List interface {
	isList()
}

type Table struct {
	List

	Name  string
	Alias string
}

type Offset struct {
	List

	Offset Expr
	Input  List
}

type Filter interface {
	isFilter()
}

type EqualityFilter struct {
	Filter

	Column Expr
	Value  Expr
}

type Selection struct {
	LogicalPlan

	Filter Filter
	Input  List
}
