package parser

import (
	"testing"

	"github.com/dty1er/sdb/testutil"
)

func TestTokenizer_Tokenize(t *testing.T) {
	tests := []struct {
		query    string
		expected []*token
	}{
		{
			query: `AAA BBB CCC`,
			expected: []*token{
				{Kind: STRING_VAL, Val: "AAA"},
				{Kind: STRING_VAL, Val: "BBB"},
				{Kind: STRING_VAL, Val: "CCC"},
			},
		},
		{
			query: "SELECT * FROM users WHERE id = 5;",
			expected: []*token{
				{Kind: SELECT},
				{Kind: ASTERISK},
				{Kind: FROM},
				{Kind: STRING_VAL, Val: "users"},
				{Kind: WHERE},
				{Kind: STRING_VAL, Val: "id"},
				{Kind: EQ},
				{Kind: NUMBER_VAL, Val: "5"},
				{Kind: EOF},
			},
		},
		{
			query: `SELECT id, name FROM users WHERE name = "bob" AND age = 25;`,
			expected: []*token{
				{Kind: SELECT},
				{Kind: STRING_VAL, Val: "id"},
				{Kind: COMMA},
				{Kind: STRING_VAL, Val: "name"},
				{Kind: FROM},
				{Kind: STRING_VAL, Val: "users"},
				{Kind: WHERE},
				{Kind: STRING_VAL, Val: "name"},
				{Kind: EQ},
				{Kind: STRING_VAL, Val: "bob"},
				{Kind: AND},
				{Kind: STRING_VAL, Val: "age"},
				{Kind: EQ},
				{Kind: NUMBER_VAL, Val: "25"},
				{Kind: EOF},
			},
		},
		{
			query: `SELECT id, name FROM users WHERE name = "aaa;bbb";`,
			expected: []*token{
				{Kind: SELECT},
				{Kind: STRING_VAL, Val: "id"},
				{Kind: COMMA},
				{Kind: STRING_VAL, Val: "name"},
				{Kind: FROM},
				{Kind: STRING_VAL, Val: "users"},
				{Kind: WHERE},
				{Kind: STRING_VAL, Val: "name"},
				{Kind: EQ},
				{Kind: STRING_VAL, Val: "aaa;bbb"},
				{Kind: EOF},
			},
		},
		{
			query: `CREATE TABLE users (id int64, name string, verified bool, registered timestamp);`,
			expected: []*token{
				{Kind: CREATE},
				{Kind: TABLE},
				{Kind: STRING_VAL, Val: "users"},
				{Kind: LPAREN},
				{Kind: STRING_VAL, Val: "id"},
				{Kind: INT64},
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
		},
		{
			query: `
INSERT INTO users 
  (id, name, verified, registered) 
VALUES 
  (1, "bob", true, "2021-05-01 17:59:59"),
  (2, "alice", false, "2021-05-02 17:59:59");`,
			expected: []*token{
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
				{Kind: STRING_VAL, Val: "bob"},
				{Kind: COMMA},
				{Kind: STRING_VAL, Val: "true"},
				{Kind: COMMA},
				{Kind: STRING_VAL, Val: "2021-05-01 17:59:59"},
				{Kind: RPAREN},
				{Kind: COMMA},
				{Kind: LPAREN},
				{Kind: NUMBER_VAL, Val: "2"},
				{Kind: COMMA},
				{Kind: STRING_VAL, Val: "alice"},
				{Kind: COMMA},
				{Kind: STRING_VAL, Val: "false"},
				{Kind: COMMA},
				{Kind: STRING_VAL, Val: "2021-05-02 17:59:59"},
				{Kind: RPAREN},
				{Kind: EOF},
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.query, func(t *testing.T) {
			tknz := newTokenizer(test.query)
			tokens := tknz.tokenize()
			testutil.MustEqual(t, tokens, test.expected)
		})
	}
}
