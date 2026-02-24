package repl //nolint:testpackage

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestIsArrayPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		field string
		dp    map[string][]any
		tf    map[string]struct{}
		want  bool
	}{
		{
			name:  "dp nil: depth 2 numeric path returns false",
			field: "a.1",
			dp:    nil,
			tf:    nil,
			want:  false,
		},
		{
			name:  "dp nil: another depth 2 numeric path returns false",
			field: "f2.1",
			dp:    nil,
			tf:    nil,
			want:  false,
		},
		{
			name:  "dp present, field not in dp, Atoi true",
			field: "a.b.1",
			dp:    map[string][]any{"a.b": {"c", "d"}},
			tf:    nil,
			want:  true,
		},
		{
			name:  "dp present, field exists, last is integer",
			field: "a.22.1",
			dp:    map[string][]any{"a.22.1": {"a", "22", 1}},
			tf:    nil,
			want:  true,
		},
		{
			name:  "dp present, field exists, last is string",
			field: "a.b.22",
			dp:    map[string][]any{"a.b.22": {"a", "b", "22"}},
			tf:    nil,
			want:  false,
		},
		{
			name:  "dp present, field exists, last component is integer",
			field: "arr.2",
			dp:    map[string][]any{"arr.2": {"arr", 2}},
			tf:    nil,
			want:  true,
		},
		{
			name:  "dp present, field exists, interior int but last string",
			field: "f2.0.2.0",
			dp:    map[string][]any{"f2.0.2.0": {"f2", "0", 2, "0"}},
			tf:    nil,
			want:  false,
		},
		{
			name:  "single segment",
			field: "field",
			dp:    nil,
			tf:    nil,
			want:  false,
		},
		{
			name:  "dp present, field exists, empty path",
			field: "x.0",
			dp:    map[string][]any{"x.0": {}},
			tf:    nil,
			want:  false,
		},
		{
			name:  "dp present, field not in dp, Atoi false",
			field: "a.b.x",
			dp:    map[string][]any{"other": {"x"}},
			tf:    nil,
			want:  false,
		},
		{
			name:  "dp nil, truncated parent: real array index",
			field: "a.2",
			dp:    nil,
			tf:    map[string]struct{}{"a": {}},
			want:  true,
		},
		{
			name:  "dp nil, depth 2 non-truncated parent: returns false",
			field: "f2.0",
			dp:    nil,
			tf:    map[string]struct{}{"a": {}},
			want:  false,
		},
		{
			name:  "dp nil, deeply nested: numeric path returns true",
			field: "a.2.b.3",
			dp:    nil,
			tf:    map[string]struct{}{"a": {}},
			want:  true,
		},
		{
			name:  "dp nil, nested truncated parent: real array index",
			field: "f2.0.3",
			dp:    nil,
			tf:    map[string]struct{}{"f2.0": {}},
			want:  true,
		},
		{
			name:  "dp nil, empty truncatedFields: depth 2 returns false",
			field: "a.1",
			dp:    nil,
			tf:    map[string]struct{}{},
			want:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := isArrayPath(tt.field, tt.dp, tt.tf)
			if got != tt.want {
				t.Errorf("isArrayPath(%q, %v, %v) = %v, want %v", tt.field, tt.dp, tt.tf, got, tt.want)
			}
		})
	}
}

func TestCollectUpdateOps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                  string
		event                 *UpdateEvent
		expectPipeline        bool
		expectConcatArraysFor []string // Fields that should use $concatArrays
		expectSimpleSetFor    []string // Fields that should use simple $set
	}{
		{
			name: "dp nil: truncated parent uses $concatArrays, depth 2 uses $set",
			event: &UpdateEvent{
				UpdateDescription: UpdateDescription{
					TruncatedArrays: []struct {
						Field   string `bson:"field"`
						NewSize int32  `bson:"newSize"`
					}{
						{Field: "a1", NewSize: 3},
					},
					UpdatedFields: bson.D{
						{Key: "a1.2", Value: "X"},
						{Key: "f2.1", Value: "Y"},
					},
					DisambiguatedPaths: nil, // MongoDB 6.0
				},
			},
			expectPipeline:        true,
			expectConcatArraysFor: []string{"a1"},
			expectSimpleSetFor:    []string{"f2.1"},
		},
		{
			name: "dp present: confirmed array uses $concatArrays",
			event: &UpdateEvent{
				UpdateDescription: UpdateDescription{
					TruncatedArrays: []struct {
						Field   string `bson:"field"`
						NewSize int32  `bson:"newSize"`
					}{
						{Field: "a1", NewSize: 3},
					},
					UpdatedFields: bson.D{
						{Key: "a1.2", Value: "X"},
						{Key: "f2.1", Value: "Y"},
					},
					DisambiguatedPaths: bson.D{
						{Key: "a1.2", Value: bson.A{"a1", 2}},
					},
				},
			},
			expectPipeline:        true,
			expectConcatArraysFor: []string{"a1", "f2"},
			expectSimpleSetFor:    []string{},
		},
		{
			name: "dp present: string key uses $set",
			event: &UpdateEvent{
				UpdateDescription: UpdateDescription{
					UpdatedFields: bson.D{
						{Key: "f2.1", Value: "Y"},
					},
					DisambiguatedPaths: bson.D{
						{Key: "f2.1", Value: bson.A{"f2", "1"}},
					},
				},
			},
			expectPipeline:        false,
			expectConcatArraysFor: []string{},
			expectSimpleSetFor:    []string{"f2.1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := collectUpdateOps(tt.event)

			switch v := result.(type) {
			case bson.A:
				if !tt.expectPipeline {
					t.Errorf("Expected simple update doc (bson.D), got pipeline (bson.A)")

					return
				}

				foundConcatArrays, foundSimpleSet := extractPipelineFields(t, v)
				assertFieldsPresent(t, foundConcatArrays, tt.expectConcatArraysFor, "$concatArrays")
				assertFieldsPresent(t, foundSimpleSet, tt.expectSimpleSetFor, "simple $set")

			case bson.D:
				if tt.expectPipeline {
					t.Errorf("Expected pipeline (bson.A), got simple update doc (bson.D)")

					return
				}

				verifySimpleUpdateDoc(t, v, tt.expectSimpleSetFor)

			default:
				t.Errorf("Unexpected result type: %T", result)
			}
		})
	}
}

func extractPipelineFields(t *testing.T, pipeline bson.A) (map[string]bool, map[string]bool) {
	t.Helper()

	concatArrays := make(map[string]bool)
	simpleSet := make(map[string]bool)

	for _, stage := range pipeline {
		stageDoc, ok := stage.(bson.D)
		if !ok {
			t.Errorf("Pipeline stage is not bson.D: %T", stage)

			continue
		}

		extractSetFields(t, stageDoc, concatArrays, simpleSet)
	}

	return concatArrays, simpleSet
}

func extractSetDocs(t *testing.T, doc bson.D) []bson.D {
	t.Helper()

	docs := make([]bson.D, 0, len(doc))

	for _, elem := range doc {
		if elem.Key != "$set" {
			continue
		}

		setDoc, ok := elem.Value.(bson.D)
		if !ok {
			t.Errorf("$set value is not bson.D: %T", elem.Value)

			continue
		}

		docs = append(docs, setDoc)
	}

	return docs
}

func extractSetFields(t *testing.T, stageDoc bson.D, concatArrays, simpleSet map[string]bool) {
	t.Helper()

	for _, setDoc := range extractSetDocs(t, stageDoc) {
		for _, setField := range setDoc {
			if hasConcatArrays(setField.Value) {
				concatArrays[setField.Key] = true
			} else {
				simpleSet[setField.Key] = true
			}
		}
	}
}

func hasConcatArrays(v any) bool {
	valueDoc, ok := v.(bson.D)
	if !ok {
		return false
	}

	for _, elem := range valueDoc {
		if elem.Key == "$concatArrays" {
			return true
		}
	}

	return false
}

func assertFieldsPresent(t *testing.T, found map[string]bool, expected []string, fieldType string) {
	t.Helper()

	for _, field := range expected {
		if !found[field] {
			t.Errorf("Expected field %q to use %s, but it doesn't", field, fieldType)
		}
	}
}

func verifySimpleUpdateDoc(t *testing.T, doc bson.D, expectedFields []string) {
	t.Helper()

	for _, setDoc := range extractSetDocs(t, doc) {
		foundFields := make(map[string]bool)
		for _, setField := range setDoc {
			foundFields[setField.Key] = true
		}

		assertFieldsPresent(t, foundFields, expectedFields, "$set")
	}
}
