package parser

import (
	"testing"

	"github.com/dty1er/sdb/testutil"
)

func TestLexer_Lex_CreateTable(t *testing.T) {
	tests := []struct {
		name      string
		tokens    []*token
		expected  *Statement
		wantError bool
	}{
		{
			name: `create table: no error`,
			tokens: []*token{
				{Kind: CREATE},
				{Kind: TABLE},
				{Kind: STRING_VAL, Val: "users"},
				{Kind: LPAREN},
				{Kind: STRING_VAL, Val: "id"},
				{Kind: INT64},
				{Kind: PRIMARY},
				{Kind: KEY},
				{Kind: COMMA},
				{Kind: STRING_VAL, Val: "name"},
				{Kind: STRING},
				{Kind: COMMA},
				{Kind: STRING_VAL, Val: "verified"},
				{Kind: BOOL},
				{Kind: COMMA},
				{Kind: STRING_VAL, Val: "registered"},
				{Kind: TIMESTAMP},
				{Kind: RPAREN},
				{Kind: EOF},
			},
			expected: &Statement{
				Typ: CREATE_TABLE_STMT,
				CreateTable: &CreateTableStatement{
					Table:         "users",
					Columns:       []string{"id", "name", "verified", "registered"},
					Types:         []string{"INT64", "STRING", "BOOL", "TIMESTAMP"},
					PrimaryKeyCol: "id",
				},
			},
		},
		{
			name: `create table: error scenario 1 - composite primary key`,
			tokens: []*token{
				{Kind: CREATE},
				{Kind: TABLE},
				{Kind: STRING_VAL, Val: "users"},
				{Kind: LPAREN},
				{Kind: STRING_VAL, Val: "id"},
				{Kind: INT64},
				{Kind: PRIMARY},
				{Kind: KEY},
				{Kind: COMMA},
				{Kind: STRING_VAL, Val: "name"},
				{Kind: STRING},
				{Kind: PRIMARY},
				{Kind: KEY},
				{Kind: COMMA},
				{Kind: STRING_VAL, Val: "verified"},
				{Kind: BOOL},
				{Kind: RPAREN},
				{Kind: EOF},
			},
			wantError: true,
		},
		{
			name: `create table: error scenario 2 - missing table name`,
			tokens: []*token{
				{Kind: CREATE},
				{Kind: TABLE},
				{Kind: LPAREN},
				{Kind: STRING_VAL, Val: "id"},
				{Kind: INT64},
				{Kind: PRIMARY},
				{Kind: KEY},
				{Kind: COMMA},
				{Kind: STRING_VAL, Val: "name"},
				{Kind: STRING},
				{Kind: COMMA},
				{Kind: STRING_VAL, Val: "verified"},
				{Kind: BOOL},
				{Kind: RPAREN},
				{Kind: EOF},
			},
			wantError: true,
		},
		{
			name: `create table: error scenario 3 - no comma`,
			tokens: []*token{
				{Kind: CREATE},
				{Kind: TABLE},
				{Kind: LPAREN},
				{Kind: STRING_VAL, Val: "id"},
				{Kind: INT64},
				{Kind: PRIMARY},
				{Kind: KEY},
				{Kind: COMMA},
				{Kind: STRING_VAL, Val: "name"},
				{Kind: STRING},
				{Kind: STRING_VAL, Val: "verified"},
				{Kind: BOOL},
				{Kind: RPAREN},
				{Kind: EOF},
			},
			wantError: true,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			l := newLexer(test.tokens)
			stmt, err := l.lex()
			testutil.MustEqual(t, err != nil, test.wantError)
			testutil.MustEqual(t, stmt, test.expected)
		})
	}
}

func TestLexer_Lex_Insert(t *testing.T) {
	tests := []struct {
		name      string
		tokens    []*token
		expected  *Statement
		wantError bool
	}{
		{
			name: `insert: normal path`,
			tokens: []*token{
				{Kind: INSERT},
				{Kind: INTO},
				{Kind: STRING_VAL, Val: "users"},
				{Kind: LPAREN},
				{Kind: STRING_VAL, Val: "id"},
				{Kind: COMMA},
				{Kind: STRING_VAL, Val: "name"},
				{Kind: COMMA},
				{Kind: STRING_VAL, Val: "verified"},
				{Kind: COMMA},
				{Kind: STRING_VAL, Val: "registered"},
				{Kind: RPAREN},
				{Kind: VALUES},
				{Kind: LPAREN},
				{Kind: NUMBER_VAL, Val: "1"},
				{Kind: COMMA},
				{Kind: STRING_VAL, Val: `"bob"`},
				{Kind: COMMA},
				{Kind: STRING_VAL, Val: "true"},
				{Kind: COMMA},
				{Kind: STRING_VAL, Val: `"2021-05-01 17:59:59"`},
				{Kind: RPAREN},
				{Kind: COMMA},
				{Kind: LPAREN},
				{Kind: NUMBER_VAL, Val: "2"},
				{Kind: COMMA},
				{Kind: STRING_VAL, Val: `"alice"`},
				{Kind: COMMA},
				{Kind: STRING_VAL, Val: "false"},
				{Kind: COMMA},
				{Kind: STRING_VAL, Val: `"2021-05-02 17:59:59"`},
				{Kind: RPAREN},
				{Kind: EOF},
			},
			expected: &Statement{
				Typ: INSERT_STMT,
				Insert: &InsertStatement{
					Table:   "users",
					Columns: []string{"id", "name", "verified", "registered"},
					Rows: [][]string{
						{"1", `"bob"`, "true", `"2021-05-01 17:59:59"`},
						{"2", `"alice"`, "false", `"2021-05-02 17:59:59"`},
					},
				},
			},
		},
		{
			name: `insert: normal path2 no columns`,
			tokens: []*token{
				{Kind: INSERT},
				{Kind: INTO},
				{Kind: STRING_VAL, Val: "users"},
				{Kind: VALUES},
				{Kind: LPAREN},
				{Kind: NUMBER_VAL, Val: "1"},
				{Kind: COMMA},
				{Kind: STRING_VAL, Val: `"bob"`},
				{Kind: COMMA},
				{Kind: STRING_VAL, Val: "true"},
				{Kind: COMMA},
				{Kind: STRING_VAL, Val: `"2021-05-01 17:59:59"`},
				{Kind: RPAREN},
				{Kind: COMMA},
				{Kind: LPAREN},
				{Kind: NUMBER_VAL, Val: "2"},
				{Kind: COMMA},
				{Kind: STRING_VAL, Val: `"alice"`},
				{Kind: COMMA},
				{Kind: STRING_VAL, Val: "false"},
				{Kind: COMMA},
				{Kind: STRING_VAL, Val: `"2021-05-02 17:59:59"`},
				{Kind: RPAREN},
				{Kind: EOF},
			},
			expected: &Statement{
				Typ: INSERT_STMT,
				Insert: &InsertStatement{
					Table:   "users",
					Columns: []string{},
					Rows: [][]string{
						{"1", `"bob"`, "true", `"2021-05-01 17:59:59"`},
						{"2", `"alice"`, "false", `"2021-05-02 17:59:59"`},
					},
				},
			},
		},
		{
			name: `insert: error path - no table name`,
			tokens: []*token{
				{Kind: INSERT},
				{Kind: INTO},
				{Kind: LPAREN},
				{Kind: STRING_VAL, Val: "id"},
				{Kind: COMMA},
				{Kind: STRING_VAL, Val: "name"},
				{Kind: COMMA},
				{Kind: STRING_VAL, Val: "verified"},
				{Kind: COMMA},
				{Kind: STRING_VAL, Val: "registered"},
				{Kind: RPAREN},
				{Kind: VALUES},
				{Kind: LPAREN},
				{Kind: NUMBER_VAL, Val: "1"},
				{Kind: COMMA},
				{Kind: STRING_VAL, Val: `"bob"`},
				{Kind: COMMA},
				{Kind: STRING_VAL, Val: "true"},
				{Kind: COMMA},
				{Kind: STRING_VAL, Val: `"2021-05-01 17:59:59"`},
				{Kind: RPAREN},
				{Kind: COMMA},
				{Kind: LPAREN},
				{Kind: NUMBER_VAL, Val: "2"},
				{Kind: COMMA},
				{Kind: STRING_VAL, Val: `"alice"`},
				{Kind: COMMA},
				{Kind: STRING_VAL, Val: "false"},
				{Kind: COMMA},
				{Kind: STRING_VAL, Val: `"2021-05-02 17:59:59"`},
				{Kind: RPAREN},
				{Kind: EOF},
			},
			wantError: true,
		},
		{
			name: `insert: error path - no paren`,
			tokens: []*token{
				{Kind: INSERT},
				{Kind: INTO},
				{Kind: STRING_VAL, Val: "users"},
				{Kind: LPAREN},
				{Kind: STRING_VAL, Val: "id"},
				{Kind: COMMA},
				{Kind: STRING_VAL, Val: "name"},
				{Kind: COMMA},
				{Kind: STRING_VAL, Val: "verified"},
				{Kind: COMMA},
				{Kind: STRING_VAL, Val: "registered"},
				{Kind: RPAREN},
				{Kind: VALUES},
				{Kind: LPAREN},
				{Kind: NUMBER_VAL, Val: "1"},
				{Kind: COMMA},
				{Kind: STRING_VAL, Val: `"bob"`},
				{Kind: COMMA},
				{Kind: STRING_VAL, Val: "true"},
				{Kind: COMMA},
				{Kind: STRING_VAL, Val: `"2021-05-01 17:59:59"`},
				{Kind: RPAREN},
				{Kind: COMMA},
				{Kind: NUMBER_VAL, Val: "2"},
				{Kind: COMMA},
				{Kind: STRING_VAL, Val: `"alice"`},
				{Kind: COMMA},
				{Kind: STRING_VAL, Val: "false"},
				{Kind: COMMA},
				{Kind: STRING_VAL, Val: `"2021-05-02 17:59:59"`},
				{Kind: RPAREN},
				{Kind: EOF},
			},
			wantError: true,
		},
		{
			name: `insert: error path - no comma`,
			tokens: []*token{
				{Kind: INSERT},
				{Kind: INTO},
				{Kind: STRING_VAL, Val: "users"},
				{Kind: LPAREN},
				{Kind: STRING_VAL, Val: "id"},
				{Kind: COMMA},
				{Kind: STRING_VAL, Val: "name"},
				{Kind: COMMA},
				{Kind: STRING_VAL, Val: "verified"},
				{Kind: COMMA},
				{Kind: STRING_VAL, Val: "registered"},
				{Kind: RPAREN},
				{Kind: VALUES},
				{Kind: LPAREN},
				{Kind: NUMBER_VAL, Val: "1"},
				{Kind: STRING_VAL, Val: `"bob"`},
				{Kind: COMMA},
				{Kind: STRING_VAL, Val: "true"},
				{Kind: COMMA},
				{Kind: STRING_VAL, Val: `"2021-05-01 17:59:59"`},
				{Kind: RPAREN},
				{Kind: COMMA},
				{Kind: LPAREN},
				{Kind: NUMBER_VAL, Val: "2"},
				{Kind: COMMA},
				{Kind: STRING_VAL, Val: `"alice"`},
				{Kind: COMMA},
				{Kind: STRING_VAL, Val: "false"},
				{Kind: COMMA},
				{Kind: STRING_VAL, Val: `"2021-05-02 17:59:59"`},
				{Kind: RPAREN},
				{Kind: EOF},
			},
			wantError: true,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			l := newLexer(test.tokens)
			stmt, err := l.lex()
			testutil.MustEqual(t, err != nil, test.wantError)
			testutil.MustEqual(t, stmt, test.expected)
		})
	}
}

func TestParser_Parse_Select(t *testing.T) {
	tests := []struct {
		name      string
		tokens    []*token
		expected  *Statement
		wantError bool
	}{
		// {
		// 	name: "SELECT * FROM users WHERE id = 5;",
		// 	tokens: []*Token{
		// 		{Kind: SELECT},
		// 		{Kind: ASTERISK},
		// 		{Kind: FROM},
		// 		{Kind: STRING_VAL, Val: "users"},
		// 		{Kind: WHERE},
		// 		{Kind: STRING_VAL, Val: "id"},
		// 		{Kind: EQ},
		// 		{Kind: NUMBER_VAL, Val: "5"},
		// 		{Kind: EOF},
		// 	},
		// },
		// {
		// 	name: `SELECT id, name FROM users WHERE name = "bob" AND age = 25;`,
		// 	tokens: []*Token{
		// 		{Kind: SELECT},
		// 		{Kind: STRING_VAL, Val: "id"},
		// 		{Kind: COMMA},
		// 		{Kind: STRING_VAL, Val: "name"},
		// 		{Kind: FROM},
		// 		{Kind: STRING_VAL, Val: "users"},
		// 		{Kind: WHERE},
		// 		{Kind: STRING_VAL, Val: "name"},
		// 		{Kind: EQ},
		// 		{Kind: QUOTED_STRING_VAL, Val: "bob"},
		// 		{Kind: AND},
		// 		{Kind: STRING_VAL, Val: "age"},
		// 		{Kind: EQ},
		// 		{Kind: NUMBER_VAL, Val: "25"},
		// 		{Kind: EOF},
		// 	},
		// },
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			l := newLexer(test.tokens)
			stmt, err := l.lex()
			testutil.MustEqual(t, err != nil, test.wantError)
			testutil.MustEqual(t, stmt, test.expected)
		})
	}
}
