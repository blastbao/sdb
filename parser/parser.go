package parser

import (
	"github.com/dty1er/sdb/sdb"
)

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
