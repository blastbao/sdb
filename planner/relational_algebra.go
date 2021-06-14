package planner

type LogicalPlan interface {
	isLogicalPlan()
}

type Expr interface {
	isExpr()
}

type List interface {
	isList()
}

type Column struct {
	Table string
	Name  string // TODO: This should be Expr because it can be function, operation, etc.
	Alias string
}

// Projection is a Projection relational algebra operator.
type Projection struct {
	LogicalPlan

	// Columns is a set of column to be picked up.
	Columns []*Column
	// Input is a source of data from which this Projection picks data.
	Input List
}

type Table struct {
	List
	Name  string
	Alias string
}

type Scan struct {
	List
	Table List
}
