package parser

import "strings"

type tokenKind string

const (
	EOF tokenKind = "EOF"

	SELECT = "SELECT"
	FROM   = "FROM"
	WHERE  = "WHERE"
	AND    = "AND"
	LEFT   = "LEFT"
	JOIN   = "JOIN"
	ON     = "ON"
	ORDER  = "ORDER"
	BY     = "BY"
	LIMIT  = "LIMIT"
	OFFSET = "OFFSET"

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

		switch {
		case t.match("select"):
			tokens = append(tokens, &token{Kind: SELECT})
		case t.match("from"):
			tokens = append(tokens, &token{Kind: FROM})
		case t.match("where"):
			tokens = append(tokens, &token{Kind: WHERE})
		case t.match("and"):
			tokens = append(tokens, &token{Kind: AND})
		case t.match("create"):
			tokens = append(tokens, &token{Kind: CREATE})
		case t.match("table"):
			tokens = append(tokens, &token{Kind: TABLE})
		case t.match("insert"):
			tokens = append(tokens, &token{Kind: INSERT})
		case t.match("into"):
			tokens = append(tokens, &token{Kind: INTO})
		case t.match("values"):
			tokens = append(tokens, &token{Kind: VALUES})
		case t.match("primary"):
			tokens = append(tokens, &token{Kind: PRIMARY})
		case t.match("key"):
			tokens = append(tokens, &token{Kind: KEY})

		case t.match("("):
			tokens = append(tokens, &token{Kind: LPAREN})
		case t.match(")"):
			tokens = append(tokens, &token{Kind: RPAREN})
		case t.match(","):
			tokens = append(tokens, &token{Kind: COMMA})
		case t.match("="):
			tokens = append(tokens, &token{Kind: EQ})
		case t.match("*"):
			tokens = append(tokens, &token{Kind: ASTERISK})
		case t.match(";"):
			tokens = append(tokens, &token{Kind: EOF})

		case t.match("bool"):
			tokens = append(tokens, &token{Kind: BOOL})
		case t.match("int64"):
			tokens = append(tokens, &token{Kind: INT64})
		case t.match("float64"):
			tokens = append(tokens, &token{Kind: FLOAT64})
		case t.match("bytes"):
			tokens = append(tokens, &token{Kind: BYTES})
		case t.match("string"):
			tokens = append(tokens, &token{Kind: STRING})
		case t.match("timestamp"):
			tokens = append(tokens, &token{Kind: TIMESTAMP})

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
