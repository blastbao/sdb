package planner

type Column struct {
	Table string
	Name  string // TODO: This should be Expr because it can be function, operation, etc.
	Alias string
}
