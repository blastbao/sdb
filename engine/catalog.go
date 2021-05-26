package engine

// TODO: (de)serialize and persistent feature
type Table struct {
	Columns []string
	Types   []Type
}

func (t *Table) Has(col, typ string) bool {
	for i := range t.Columns {
		if t.Columns[i] == col && t.Types[i].String() == typ {
			return true
		}
	}

	return false
}

type Catalog struct {
	Tables map[string]*Table
}

func (c *Catalog) FindTable(table string) bool {
	_, ok := c.Tables[table]
	return ok
}

func (c *Catalog) MatchColumns(table string, columns []string, types []string) bool {
	t, ok := c.Tables[table]
	if !ok {
		return false
	}

	if len(columns) != len(types) {
		return false
	}

	if len(columns) == 0 {
		return false
	}

	for i := range columns {
		if t.Has(columns[i], types[i]) {
			return true
		}

	}

	return false
}
