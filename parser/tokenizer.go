package parser

import "strings"

type tokenKind string

const (
	EOF tokenKind = "EOF"

	SELECT   = "SELECT"
	DISTINCT = "DISTINCT"
	AS       = "AS"
	FROM     = "FROM"
	WHERE    = "WHERE"
	AND      = "AND"
	OR       = "OR"
	LEFT     = "LEFT"
	JOIN     = "JOIN"
	ON       = "ON"
	ORDER    = "ORDER"
	BY       = "BY"
	ASC      = "ASC"
	DESC     = "DESC"
	LIMIT    = "LIMIT"
	OFFSET   = "OFFSET"

	CREATE = "CREATE"
	TABLE  = "TABLE"

	INSERT = "INSERT"
	INTO   = "INTO"
	VALUES = "VALUES"

	PRIMARY = "PRIMARY"
	KEY     = "KEY"

	BOOL      = "BOOL"
	INT64     = "INT64"
	FLOAT64   = "FLOAT64"
	BYTES     = "BYTES"
	STRING    = "STRING"
	TIMESTAMP = "TIMESTAMP"

	STRING_VAL = "STRING_VAL"
	NUMBER_VAL = "NUMBER_VAL"

	LPAREN   = "LPAREN" // (
	RPAREN   = "RPAREN" // )
	COMMA    = "COMMA"
	EQ       = "="
	LT       = "<"
	LTE      = "<="
	GT       = ">"
	GTE      = ">="
	NEQ      = "<>"
	ASTERISK = "ASTERISK"
)

var Keywords = map[string]tokenKind{
	"select":    SELECT,
	"as":        AS,
	"distinct":  DISTINCT,
	"from":      FROM,
	"where":     WHERE,
	"and":       AND,
	"or":        OR,
	"left":      LEFT,
	"join":      JOIN,
	"on":        ON,
	"order":     ORDER,
	"by":        BY,
	"asc":       ASC,
	"desc":      DESC,
	"limit":     LIMIT,
	"offset":    OFFSET,
	"create":    CREATE,
	"table":     TABLE,
	"insert":    INSERT,
	"into":      INTO,
	"values":    VALUES,
	"primary":   PRIMARY,
	"key":       KEY,
	"bool":      BOOL,
	"int64":     INT64,
	"float64":   FLOAT64,
	"bytes":     BYTES,
	"string":    STRING,
	"timestamp": TIMESTAMP,
	"(":         LPAREN,
	")":         RPAREN,
	",":         COMMA,
	"=":         EQ,
	"<":         LT,
	"<=":        LTE,
	">":         GT,
	">=":        GTE,
	"<>":        NEQ,
	"*":         ASTERISK,
	";":         EOF,
}

func (tk tokenKind) String() string {
	return string(tk)
}

type token struct {
	Kind tokenKind

	Val string
}

type tokenizer struct {
	query string
	pos   int
}

func newTokenizer(query string) *tokenizer {
	return &tokenizer{query: query}
}

func (t *tokenizer) isSpace() bool {
	return t.query[t.pos] == ' ' || t.query[t.pos] == '\n' || t.query[t.pos] == '\t'
}

func (t *tokenizer) isSymbol() bool {
	symbols := []byte{'{', '}', '(', ')', ',', '=', '*', ';'}
	for _, symbol := range symbols {
		if t.query[t.pos] == symbol {
			return true
		}
	}

	return false
}

func (t *tokenizer) isEnd() bool {
	return t.pos >= len(t.query)
}

func (t *tokenizer) isNumber() bool {
	return t.query[t.pos] >= '0' && t.query[t.pos] <= '9'
}

func (t *tokenizer) isPoint() bool {
	return t.query[t.pos] == '.'
}

func (t *tokenizer) isDoubleQuote() bool {
	return t.query[t.pos] == '"'
}

func (t *tokenizer) skipSpaces() {
	for t.isSpace() {
		t.pos++
	}
}

func (t *tokenizer) scanNumber() string {
	var out []uint8
	for !t.isEnd() && !t.isSpace() && (t.isNumber() || t.isPoint()) {
		out = append(out, t.query[t.pos])
		t.pos++
	}
	return string(out)
}

func (t *tokenizer) scanStringVal() string {
	var out []uint8
	for !t.isEnd() && !t.isSymbol() && !t.isSpace() {
		out = append(out, t.query[t.pos])
		t.pos++
	}

	return string(out)
}

func (t *tokenizer) scanQuotedStringVal() string {
	// leading double quote is consumed by t.match()
	var out []uint8
	for !t.isEnd() && !t.isDoubleQuote() {
		out = append(out, t.query[t.pos])
		t.pos++
	}

	if t.isDoubleQuote() { // ignore last double quote
		t.pos++
	}

	return string(out)
}

func (t *tokenizer) match(s string) bool {
	length := len(s)
	// remaining characters length must be longer than s length
	if len(t.query)-t.pos < length {
		return false
	}

	if strings.ToLower(t.query[t.pos:t.pos+length]) == strings.ToLower(s) {
		t.pos += length
		return true
	}

	return false
}

func (t *tokenizer) tokenize() []*token {
	tokens := []*token{}

	for t.pos = 0; t.pos < len(t.query); {
		t.skipSpaces()

		found := false
		for kw, tk := range Keywords {
			if t.match(kw) {
				tokens = append(tokens, &token{Kind: tk})
				found = true
				break
			}
		}

		if found {
			continue
		}

		switch {
		case t.match(`"`):
			s := t.scanQuotedStringVal()
			tokens = append(tokens, &token{Kind: STRING_VAL, Val: s})
		case t.isNumber():
			s := t.scanNumber()
			tokens = append(tokens, &token{Kind: NUMBER_VAL, Val: s})
		default:
			s := t.scanStringVal()
			tokens = append(tokens, &token{Kind: STRING_VAL, Val: s})
		}
	}

	return tokens
}
