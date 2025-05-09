package mongolink //nolint

import (
	"testing"
)

func TestIsArrayPath(t *testing.T) { //nolint:paralleltest
	tests := []struct {
		path string
		want bool
	}{
		{"a.1", true},
		{"a.b.1", true},
		{"a.b.2.1", true},
		{"a.b.c.d", false},
	}

	for _, test := range tests {
		got := isArrayPath(test.path)
		if got != test.want {
			t.Errorf("got = %v, want %v", got, test.want)
		}
	}
}
