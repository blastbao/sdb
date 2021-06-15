package parser

import (
	"sort"
	"strings"
)

type keyword struct {
	s  string
	tk tokenKind
}

type tokenKind uint8

const (
	EOF tokenKind = iota + 1

	SELECT
	DISTINCT
	AS
	FROM
	WHERE
	AND
	OR
	LEFT
	JOIN
	ON
	ORDER
	BY
	ASC
	DESC
	LIMIT
	OFFSET

	CREATE
	TABLE

	INSERT
	INTO
	VALUES

	PRIMARY
	KEY

	BOOL
	INT64
	FLOAT64
	BYTES
	STRING
	TIMESTAMP

	STRING_VAL
	NUMBER_VAL

	LPAREN
	RPAREN
	COMMA
	EQ
	LT
	LTE
	GT
	GTE
	NEQ
	ASTERISK
)

func (tk tokenKind) String() string {
	for _, kw := range keywords {
		if kw.tk == tk {
			return kw.s
		}
	}

	// must not come here
	return ""
}

var keywords = []*keyword{
	{s: "select", tk: SELECT},
	{s: "as", tk: AS},
	{s: "distinct", tk: DISTINCT},
	{s: "from", tk: FROM},
	{s: "where", tk: WHERE},
	{s: "and", tk: AND},
	{s: "or", tk: OR},
	{s: "left", tk: LEFT},
	{s: "join", tk: JOIN},
	{s: "on", tk: ON},
	{s: "order", tk: ORDER},
	{s: "by", tk: BY},
	{s: "asc", tk: ASC},
	{s: "desc", tk: DESC},
	{s: "limit", tk: LIMIT},
	{s: "offset", tk: OFFSET},
	{s: "create", tk: CREATE},
	{s: "table", tk: TABLE},
	{s: "insert", tk: INSERT},
	{s: "into", tk: INTO},
	{s: "values", tk: VALUES},
	{s: "primary", tk: PRIMARY},
	{s: "key", tk: KEY},
	{s: "bool", tk: BOOL},
	{s: "int64", tk: INT64},
	{s: "float64", tk: FLOAT64},
	{s: "bytes", tk: BYTES},
	{s: "string", tk: STRING},
	{s: "timestamp", tk: TIMESTAMP},
	{s: "(", tk: LPAREN},
	{s: ")", tk: RPAREN},
	{s: ",", tk: COMMA},
	{s: "=", tk: EQ},
	{s: "<", tk: LT},
	{s: "<=", tk: LTE},
	{s: ">", tk: GT},
	{s: ">=", tk: GTE},
	{s: "<>", tk: NEQ},
	{s: "*", tk: ASTERISK},
	{s: ";", tk: EOF},
}

func sortKeywords() []*keyword {
	k := keywords
	sort.Slice(k, func(i, j int) bool {
		return k[j].s < k[i].s
	})

	return k
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
		for _, kw := range sortKeywords() {
			if t.match(kw.s) {
				tokens = append(tokens, &token{Kind: kw.tk})
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
