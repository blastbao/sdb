package parser

import "strings"

type TokenKind string

const (
	EOF TokenKind = "EOF"

	SELECT = "SELECT"
	FROM   = "FROM"
	WHERE  = "WHERE"
	AND    = "AND"

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
	EQ       = "EQ"
	ASTERISK = "ASTERISK"
)

func (tk TokenKind) String() string {
	return string(tk)
}

type Token struct {
	Kind TokenKind

	Val string
}

type Tokenizer struct {
	query string
	pos   int
}

func NewTokenizer(query string) *Tokenizer {
	return &Tokenizer{query: query}
}

func (t *Tokenizer) isSpace() bool {
	return t.query[t.pos] == ' ' || t.query[t.pos] == '\n' || t.query[t.pos] == '\t'
}

func (t *Tokenizer) isSymbol() bool {
	symbols := []byte{'{', '}', '(', ')', ',', '=', '*', ';'}
	for _, symbol := range symbols {
		if t.query[t.pos] == symbol {
			return true
		}
	}

	return false
}

func (t *Tokenizer) isEnd() bool {
	return t.pos >= len(t.query)
}

func (t *Tokenizer) isNumber() bool {
	return t.query[t.pos] >= '0' && t.query[t.pos] <= '9'
}

func (t *Tokenizer) isPoint() bool {
	return t.query[t.pos] == '.'
}

func (t *Tokenizer) isDoubleQuote() bool {
	return t.query[t.pos] == '"'
}

func (t *Tokenizer) skipSpaces() {
	for t.isSpace() {
		t.pos++
	}
}

func (t *Tokenizer) scanNumber() string {
	var out []uint8
	for !t.isEnd() && !t.isSpace() && (t.isNumber() || t.isPoint()) {
		out = append(out, t.query[t.pos])
		t.pos++
	}
	return string(out)
}

func (t *Tokenizer) scanStringVal() string {
	var out []uint8
	for !t.isEnd() && !t.isSymbol() && !t.isSpace() {
		out = append(out, t.query[t.pos])
		t.pos++
	}

	return string(out)
}

func (t *Tokenizer) scanQuotedStringVal() string {
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

func (t *Tokenizer) match(s string) bool {
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

func (t *Tokenizer) Tokenize() []*Token {
	tokens := []*Token{}

	for t.pos = 0; t.pos < len(t.query); {
		t.skipSpaces()

		switch {
		case t.match("select"):
			tokens = append(tokens, &Token{Kind: SELECT})
		case t.match("from"):
			tokens = append(tokens, &Token{Kind: FROM})
		case t.match("where"):
			tokens = append(tokens, &Token{Kind: WHERE})
		case t.match("and"):
			tokens = append(tokens, &Token{Kind: AND})
		case t.match("create"):
			tokens = append(tokens, &Token{Kind: CREATE})
		case t.match("table"):
			tokens = append(tokens, &Token{Kind: TABLE})
		case t.match("insert"):
			tokens = append(tokens, &Token{Kind: INSERT})
		case t.match("into"):
			tokens = append(tokens, &Token{Kind: INTO})
		case t.match("values"):
			tokens = append(tokens, &Token{Kind: VALUES})
		case t.match("primary"):
			tokens = append(tokens, &Token{Kind: PRIMARY})
		case t.match("key"):
			tokens = append(tokens, &Token{Kind: KEY})

		case t.match("("):
			tokens = append(tokens, &Token{Kind: LPAREN})
		case t.match(")"):
			tokens = append(tokens, &Token{Kind: RPAREN})
		case t.match(","):
			tokens = append(tokens, &Token{Kind: COMMA})
		case t.match("="):
			tokens = append(tokens, &Token{Kind: EQ})
		case t.match("*"):
			tokens = append(tokens, &Token{Kind: ASTERISK})
		case t.match(";"):
			tokens = append(tokens, &Token{Kind: EOF})

		case t.match("bool"):
			tokens = append(tokens, &Token{Kind: BOOL})
		case t.match("int64"):
			tokens = append(tokens, &Token{Kind: INT64})
		case t.match("float64"):
			tokens = append(tokens, &Token{Kind: FLOAT64})
		// case t.match("bytes"):
		// 	tokens = append(tokens, &Token{Kind: BYTES})
		case t.match("string"):
			tokens = append(tokens, &Token{Kind: STRING})
		case t.match("timestamp"):
			tokens = append(tokens, &Token{Kind: TIMESTAMP})

		case t.match(`"`):
			s := t.scanQuotedStringVal()
			tokens = append(tokens, &Token{Kind: STRING_VAL, Val: s})
		case t.isNumber():
			s := t.scanNumber()
			tokens = append(tokens, &Token{Kind: NUMBER_VAL, Val: s})
		default:
			s := t.scanStringVal()
			tokens = append(tokens, &Token{Kind: STRING_VAL, Val: s})
		}
	}

	return tokens
}
