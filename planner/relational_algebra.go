package planner

import (
	"time"

	"github.com/dty1er/sdb/sdb"
)

type LogicalPlan interface {
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
	List

	Table *Table

	tuples []sdb.Tuple
	idx    int
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
	List

	Limit Expr
	Input List
}

type OrderBy struct {
	List

	Columns     []Expr
	Directirons []string
	Input       List
}

type List interface {
	isList()
	Next(engine sdb.Engine) sdb.Tuple
	Process(t sdb.Tuple) sdb.Tuple
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
	List

	Filter Filter
	Input  List
}

func (s *Scan) Next(engine sdb.Engine) sdb.Tuple {
	if len(s.tuples) == 0 {
		ts, err := engine.ReadTable(s.Table.Name)
		if err != nil {
			panic(err)
		}

		s.tuples = ts
	}
	if s.idx >= len(s.tuples) {
		return nil
	}
	t := s.tuples[s.idx]
	s.idx++
	return t
}

func (s *Scan) Process(t sdb.Tuple) sdb.Tuple {
	return t
}
