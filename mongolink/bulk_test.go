package mongolink

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

	got, _ := getArray(doc, "arr")
	if len(got) != 5 {
		t.Errorf("got = %v, want %v", got, 5)
	}
	got, _ = getArray(doc, "f2.arr")
	if len(got) != 6 {
		t.Errorf("got = %v, want %v", got, 6)
	}
}
