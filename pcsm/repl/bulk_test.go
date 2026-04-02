package repl //nolint:testpackage

import (
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/v2/bson"
)

const setOp = "$set"

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

			ops := collectUpdateOps(tt.event)

			switch v := ops.primary.(type) {
			case bson.A:
				if !tt.expectPipeline {
					t.Errorf("Expected simple update doc (bson.D), got pipeline (bson.A)")

					return
				}

				foundConcatArrays, foundSimpleSet := extractPipelineFields(t, v)

				// Non-array fields may be in follow-up standard $set ops, collect those too.
				for _, fu := range ops.followUp {
					for _, elem := range fu {
						if elem.Key == "$set" {
							if setDoc, ok := elem.Value.(bson.D); ok {
								for _, f := range setDoc {
									foundSimpleSet[f.Key] = true
								}
							}
						}
					}
				}

				assertFieldsPresent(t, foundConcatArrays, tt.expectConcatArraysFor, "$concatArrays")
				assertFieldsPresent(t, foundSimpleSet, tt.expectSimpleSetFor, "simple $set")

			case bson.D:
				if tt.expectPipeline {
					t.Errorf("Expected pipeline (bson.A), got simple update doc (bson.D)")

					return
				}

				verifySimpleUpdateDoc(t, v, tt.expectSimpleSetFor)

			default:
				t.Errorf("Unexpected result type: %T", ops.primary)
			}
		})
	}
}

func TestCollectUpdateOps_NoFalsePositiveConflict(t *testing.T) {
	t.Parallel()

	// "items_count" should NOT conflict with truncated array "items"
	// because "items_count" is a separate field, not a sub-path of "items".
	tests := []struct {
		name           string
		truncField     string
		updateKey      string
		expectPipeline bool
	}{
		{
			name:           "prefix match is not a conflict: items_count vs items",
			truncField:     "items",
			updateKey:      "items_count",
			expectPipeline: false,
		},
		{
			name:           "prefix match is not a conflict: data vs database",
			truncField:     "data",
			updateKey:      "database",
			expectPipeline: false,
		},
		{
			name:           "exact match is a conflict",
			truncField:     "items",
			updateKey:      "items",
			expectPipeline: true,
		},
		{
			name:           "dot-separated sub-path is a conflict",
			truncField:     "items",
			updateKey:      "items.2",
			expectPipeline: true,
		},
		{
			name:           "deeply nested sub-path is a conflict",
			truncField:     "items",
			updateKey:      "items.2.name",
			expectPipeline: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			event := &UpdateEvent{
				UpdateDescription: UpdateDescription{
					TruncatedArrays: []struct {
						Field   string `bson:"field"`
						NewSize int32  `bson:"newSize"`
					}{
						{Field: tt.truncField, NewSize: 3},
					},
					UpdatedFields: bson.D{
						{Key: tt.updateKey, Value: "val"},
					},
				},
			}

			ops := collectUpdateOps(event)

			_, isPipeline := ops.primary.(bson.A)
			if isPipeline != tt.expectPipeline {
				t.Errorf("collectUpdateOps() returned pipeline=%v, want pipeline=%v", isPipeline, tt.expectPipeline)
			}
		})
	}
}

func TestCollectUpdateOpsWithPipeline_ChunksSmallFields(t *testing.T) {
	t.Parallel()

	// Many small fields should be chunked by field count (maxFieldsPerSetOp),
	// since their total byte size is well under maxBytesPerSetOp.
	// With 250 small fields: first 100 go into the primary pipeline, remaining 150
	// are split into follow-up pipeline operations (100 + 50).
	const numFields = 250

	updatedFields := make(bson.D, 0, numFields+1)
	for i := range numFields {
		updatedFields = append(updatedFields, bson.E{
			Key:   "field_" + strconv.Itoa(i),
			Value: "value",
		})
	}

	updatedFields = append(updatedFields, bson.E{Key: "arr.2", Value: "arrval"})

	event := &UpdateEvent{
		UpdateDescription: UpdateDescription{
			TruncatedArrays: []struct {
				Field   string `bson:"field"`
				NewSize int32  `bson:"newSize"`
			}{
				{Field: "arr", NewSize: 5},
			},
			UpdatedFields: updatedFields,
		},
	}

	ops := collectUpdateOpsWithPipeline(event)

	pipeline, ok := ops.primary.(bson.A)
	if !ok {
		t.Fatalf("Expected pipeline (bson.A), got %T", ops.primary)
	}

	concatArraysStages, truncationStages := classifySetStages(t, pipeline)

	assert.Equal(t, 1, concatArraysStages, "$concatArrays stages")
	assert.Equal(t, 1, truncationStages, "truncation stages")

	// Count all $set fields across primary pipeline and follow-ups
	totalFields := countAllSetFields(t, ops)

	assert.Equal(t, numFields, totalFields, "total fields across primary + follow-ups")

	// All batched fields go to follow-ups: 250 / 100 = 3 follow-ups (100, 100, 50)
	expectedFollowUps := (numFields + maxFieldsPerSetOp - 1) / maxFieldsPerSetOp

	assert.Len(t, ops.followUp, expectedFollowUps, "follow-up operations")
}

func TestCollectUpdateOpsWithPipeline_ChunksLargeFields(t *testing.T) {
	t.Parallel()

	// Large fields (~20KB each) should be chunked by byte size (maxBytesPerSetOp),
	// producing follow-up pipeline operations. This is the BufBuilder overflow scenario.
	const numFields = 100
	largeValue := strings.Repeat("X", 20_000)

	updatedFields := make(bson.D, 0, numFields+1)
	for i := range numFields {
		updatedFields = append(updatedFields, bson.E{
			Key:   "arr." + strconv.Itoa(i) + ".d",
			Value: largeValue,
		})
	}

	updatedFields = append(updatedFields, bson.E{Key: "meta.updated", Value: true})

	event := &UpdateEvent{
		UpdateDescription: UpdateDescription{
			TruncatedArrays: []struct {
				Field   string `bson:"field"`
				NewSize int32  `bson:"newSize"`
			}{
				{Field: "arr", NewSize: 250},
			},
			UpdatedFields: updatedFields,
		},
	}

	ops := collectUpdateOpsWithPipeline(event)

	pipeline, ok := ops.primary.(bson.A)
	if !ok {
		t.Fatalf("Expected pipeline (bson.A), got %T", ops.primary)
	}

	concatArraysStages, _ := classifySetStages(t, pipeline)

	assert.Equal(t, 0, concatArraysStages, "$concatArrays stages (all fields should be batched)")

	// Must have follow-ups since 100 × 20KB = 2MB >> 512KB limit
	assert.NotEmpty(t, ops.followUp, "should have follow-up operations due to byte size")

	// Each follow-up must be a standard update (bson.D) with a single $set operator
	for i, fu := range ops.followUp {
		assert.Len(t, fu, 1, "follow-up %d should have exactly 1 operator ($set)", i)
		assert.Equal(t, "$set", fu[0].Key, "follow-up %d operator key", i)
	}

	// Count total fields across everything
	totalFields := countAllSetFields(t, ops)
	assert.Equal(t, numFields+1, totalFields, "total fields across primary + follow-ups (100 arr + 1 meta)")
}

// classifySetStages counts $concatArrays and truncation $set stages in a pipeline.
func classifySetStages(t *testing.T, pipeline bson.A) (int, int) {
	t.Helper()

	var concatArrays, truncation int

	for _, stage := range pipeline {
		stageDoc, ok := stage.(bson.D)
		if !ok {
			continue
		}

		for _, elem := range stageDoc {
			if elem.Key != setOp {
				continue
			}

			setDoc, ok := elem.Value.(bson.D)
			if !ok {
				continue
			}

			switch {
			case len(setDoc) == 1 && hasConcatArrays(setDoc[0].Value):
				concatArrays++
			case len(setDoc) == 1 && hasSlice(setDoc[0].Value):
				truncation++
			}
		}
	}

	return concatArrays, truncation
}

// extractSetFieldsFromPipeline collects non-array $set field keys from a pipeline in order.
func extractSetFieldsFromPipeline(t *testing.T, pipeline bson.A) []string {
	t.Helper()

	var keys []string

	for _, stage := range pipeline {
		stageDoc, ok := stage.(bson.D)
		if !ok {
			continue
		}

		for _, elem := range stageDoc {
			if elem.Key != setOp {
				continue
			}

			setDoc, ok := elem.Value.(bson.D)
			if !ok {
				continue
			}

			for _, f := range setDoc {
				if !hasConcatArrays(f.Value) && !hasSlice(f.Value) {
					keys = append(keys, f.Key)
				}
			}
		}
	}

	return keys
}

// collectSetFieldKeys collects all non-array $set field keys from primary + follow-ups in order.
func collectSetFieldKeys(t *testing.T, ops updateOps) []string {
	t.Helper()

	pipeline, ok := ops.primary.(bson.A)
	if !ok {
		t.Fatalf("Expected pipeline (bson.A), got %T", ops.primary)
	}

	keys := extractSetFieldsFromPipeline(t, pipeline)

	// Follow-ups are standard $set (bson.D), not pipelines
	for _, fu := range ops.followUp {
		for _, elem := range fu {
			if elem.Key == "$set" {
				if setDoc, ok := elem.Value.(bson.D); ok {
					for _, f := range setDoc {
						keys = append(keys, f.Key)
					}
				}
			}
		}
	}

	return keys
}

// countAllSetFields counts all non-array $set fields across primary pipeline + follow-ups.
func countAllSetFields(t *testing.T, ops updateOps) int {
	t.Helper()

	return len(collectSetFieldKeys(t, ops))
}

func TestCollectUpdateOpsWithPipeline_ChunkedSetPreservesOrder(t *testing.T) {
	t.Parallel()

	// Field ordering must be preserved across chunks to avoid breaking
	// exact-match queries on embedded documents (MongoDB compares embedded
	// documents by BSON byte order, so field reordering changes query semantics).
	const numFields = 250 // spans multiple chunks of maxFieldsPerSetStage

	updatedFields := make(bson.D, 0, numFields)
	expectedOrder := make([]string, 0, numFields)

	for i := range numFields {
		key := "field_" + strconv.Itoa(i)
		updatedFields = append(updatedFields, bson.E{Key: key, Value: i})
		expectedOrder = append(expectedOrder, key)
	}

	event := &UpdateEvent{
		UpdateDescription: UpdateDescription{
			TruncatedArrays: []struct {
				Field   string `bson:"field"`
				NewSize int32  `bson:"newSize"`
			}{
				{Field: "arr", NewSize: 5},
			},
			UpdatedFields: updatedFields,
		},
	}

	ops := collectUpdateOpsWithPipeline(event)

	// Collect all non-array $set fields in order: primary pipeline first, then follow-ups
	gotOrder := collectSetFieldKeys(t, ops)

	if len(gotOrder) != len(expectedOrder) {
		t.Fatalf("Expected %d fields, got %d", len(expectedOrder), len(gotOrder))
	}

	for i := range expectedOrder {
		if gotOrder[i] != expectedOrder[i] {
			t.Errorf("Field at position %d: got %q, want %q", i, gotOrder[i], expectedOrder[i])
		}
	}
}

func TestCollectUpdateOpsWithPipeline_SliceUsesMaxForPositive(t *testing.T) {
	t.Parallel()

	// The $slice third argument must always be positive.
	// Verify the pipeline uses $max to guarantee this.
	event := &UpdateEvent{
		UpdateDescription: UpdateDescription{
			TruncatedArrays: []struct {
				Field   string `bson:"field"`
				NewSize int32  `bson:"newSize"`
			}{
				{Field: "arr", NewSize: 0}, // Array truncated to size 0
			},
			UpdatedFields: bson.D{
				{Key: "arr.0", Value: "new_val"},
			},
		},
	}

	ops := collectUpdateOpsWithPipeline(event)

	pipeline, ok := ops.primary.(bson.A)
	if !ok {
		t.Fatalf("Expected pipeline (bson.A), got %T", ops.primary)
	}

	// Find the $concatArrays stage for "arr"
	found := false

	for _, stage := range pipeline {
		stageDoc, ok := stage.(bson.D)
		if !ok {
			continue
		}

		for _, elem := range stageDoc {
			if elem.Key != setOp {
				continue
			}

			setDoc, ok := elem.Value.(bson.D)
			if !ok {
				continue
			}

			for _, setField := range setDoc {
				if setField.Key != "arr" {
					continue
				}

				arrDoc, ok := setField.Value.(bson.D)
				if !ok {
					continue
				}

				for _, arrElem := range arrDoc {
					if arrElem.Key != "$concatArrays" {
						continue
					}

					concatArr, ok := arrElem.Value.(bson.A)
					if !ok || len(concatArr) != 3 {
						t.Fatalf("Expected $concatArrays with 3 elements, got %v", arrElem.Value)
					}

					// Third element is the trailing $slice
					sliceDoc, ok := concatArr[2].(bson.D)
					if !ok {
						t.Fatalf("Expected third $concatArrays element to be bson.D, got %T", concatArr[2])
					}

					for _, sliceElem := range sliceDoc {
						if sliceElem.Key != "$slice" {
							continue
						}

						sliceArgs, ok := sliceElem.Value.(bson.A)
						if !ok || len(sliceArgs) != 3 {
							t.Fatalf("Expected $slice with 3 args, got %v", sliceElem.Value)
						}

						// Third arg should be bson.D{{"$max", bson.A{1, ...}}}
						maxDoc, ok := sliceArgs[2].(bson.D)
						if !ok {
							t.Fatalf("Expected $slice third arg to be bson.D, got %T", sliceArgs[2])
						}

						hasMax := false
						for _, maxElem := range maxDoc {
							if maxElem.Key == "$max" {
								hasMax = true

								maxArr, ok := maxElem.Value.(bson.A)
								if !ok || len(maxArr) != 2 {
									t.Fatalf("Expected $max with 2 elements, got %v", maxElem.Value)
								}

								minVal, ok := maxArr[0].(int)
								if !ok || minVal != 1 {
									t.Errorf("Expected $max minimum to be 1, got %v", maxArr[0])
								}
							}
						}

						if !hasMax {
							t.Error("$slice third argument does not use $max to guarantee positive value")
						}

						found = true
					}
				}
			}
		}
	}

	if !found {
		t.Error("Could not find $concatArrays with $slice in pipeline")
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
		if elem.Key != setOp {
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

func hasSlice(v any) bool {
	valueDoc, ok := v.(bson.D)
	if !ok {
		return false
	}

	for _, elem := range valueDoc {
		if elem.Key == "$slice" {
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
