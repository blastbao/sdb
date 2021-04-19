package testutil

import (
	"testing"

	"github.com/go-test/deep"
)

func Equal(t *testing.T, a, b interface{}) {
	if diff := deep.Equal(a, b); diff != nil {
		t.Error(diff)
	}
}
