package engine

// TODO: (de)serialize and persistent feature
type Table struct {
	Columns []string
	Types   []Type
}

type Catalog struct {
	Tables map[string]*Table
}

func (c *Catalog) FindTable(table string) bool {
	_, ok := c.Tables[table]
	return ok
}
