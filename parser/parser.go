package parser

import (
	"github.com/dty1er/sdb/sdb"
)

type CreateTableStatement struct {
	sdb.Statement

	Table         string
	Columns       []string
	Types         []string
	PrimaryKeyCol string
}

type InsertStatement struct {
	sdb.Statement

	Table   string
	Columns []string
	Rows    [][]string
}

type Parser struct {
	catalog sdb.Catalog
}

func New(catalog sdb.Catalog) *Parser {
	return &Parser{catalog: catalog}
}

// Parse parses the given query to the statement.
// It implements sdb.Parser interface.
func (p *Parser) Parse(query string) (sdb.Statement, error) {
	tokens := newTokenizer(query).tokenize()
	stmt, err := newLexer(tokens).lex()
	if err != nil {
		return nil, err
	}

	if err := newValidator(stmt, p.catalog).validate(); err != nil {
		return nil, err
	}

	return stmt, nil
}
