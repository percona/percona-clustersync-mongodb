package mongolink //nolint

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestIsArrayPath(t *testing.T) {
	tests := []struct {
		path string
		want bool
	}{
		{"a.1", true},
		{"a.b.1", true},
		{"a.b.c.1", true},
		{"a.b.c.d", false},
	}

	for _, test := range tests {
		got := isArrayPath(test.path)
		if got != test.want {
			t.Errorf("got = %v, want %v", got, test.want)
		}
	}
}

func TestGetArray(t *testing.T) {
	doc, err := bson.Marshal(bson.D{
		{"f1", "v1"},
		{"arr", bson.A{"A", "B", "C", "D", "E"}},
		{"f2", bson.D{
			{"arr", bson.A{"A", "B", "C", "D", "E", "F"}},
		}},
	})
	if err != nil {
		t.Fatalf("failed to marshal BSON: %v", err)
	}

	tests := []struct {
		path   string
		length int
	}{
		{"arr", 5},
		{"f2.arr", 6},
	}

	for _, test := range tests {
		got, _ := getArray(doc, test.path)
		if len(got) != test.length {
			t.Errorf("got = %v, want %v", got, test.length)
		}
	}
}
