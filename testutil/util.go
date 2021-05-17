package testutil

import (
	"testing"

	"github.com/go-test/deep"
)

func MustEqual(t *testing.T, a, b interface{}) {
	t.Helper()
	if diff := deep.Equal(a, b); diff != nil {
		t.Fatal(diff)
	}
}
