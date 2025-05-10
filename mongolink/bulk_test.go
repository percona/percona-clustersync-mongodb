package mongolink //nolint

import (
	"testing"
)

func TestIsArrayPath(t *testing.T) { //nolint:paralleltest
	tests := []struct {
		field string
		dp    map[string][]any
		want  bool
	}{
		{
			"a.1",
			nil,
			true,
		},
		{
			"a.b.1",
			map[string][]any{"a.b": {"c", "d"}},
			true,
		},
		{
			"a.22.1",
			map[string][]any{"a.22": {"a", "22", 1}},
			true,
		},
		{
			"a.b.22",
			map[string][]any{"a.b.22": {"a", "b", "22"}},
			false,
		},
	}

	for _, test := range tests {
		got := isArrayPath(test.field, test.dp)
		if got != test.want {
			t.Errorf("got = %v, want %v", got, test.want)
		}
	}
}
