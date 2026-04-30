package repl //nolint:testpackage

import (
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/percona/percona-clustersync-mongodb/pcsm/catalog"
)

const (
	setOp   = "$set"
	pushOp  = "$push"
	unsetOp = "$unset"
)

func TestCollectUpdateOps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		event  *UpdateEvent
		assert func(t *testing.T, ops updateOps)
	}{
		{
			name: "simple $set with no truncation produces a single classic update",
			event: &UpdateEvent{
				UpdateDescription: UpdateDescription{
					UpdatedFields: bson.D{{Key: "a", Value: 1}, {Key: "b.c", Value: 2}},
				},
			},
			assert: func(t *testing.T, ops updateOps) {
				t.Helper()
				assert.Empty(t, ops.followUp)

				doc, ok := ops.primary.(bson.D)
				if !ok {
					t.Fatalf("expected primary bson.D, got %T", ops.primary)
				}

				keys := topLevelKeys(doc)
				assert.Equal(t, []string{setOp}, keys)
			},
		},
		{
			name: "removed fields produce $unset",
			event: &UpdateEvent{
				UpdateDescription: UpdateDescription{
					RemovedFields: []string{"x", "y.z"},
				},
			},
			assert: func(t *testing.T, ops updateOps) {
				t.Helper()
				assert.Empty(t, ops.followUp)

				doc, ok := ops.primary.(bson.D)
				if !ok {
					t.Fatalf("expected primary bson.D, got %T", ops.primary)
				}

				assert.Equal(t, []string{unsetOp}, topLevelKeys(doc))
			},
		},
		{
			name: "truncation alone produces classic $push with $each:[]/$slice",
			event: &UpdateEvent{
				UpdateDescription: UpdateDescription{
					TruncatedArrays: []struct {
						Field   string `bson:"field"`
						NewSize int32  `bson:"newSize"`
					}{
						{Field: "arr", NewSize: 3},
					},
				},
			},
			assert: func(t *testing.T, ops updateOps) {
				t.Helper()
				assert.Empty(t, ops.followUp)

				doc, ok := ops.primary.(bson.D)
				if !ok {
					t.Fatalf("expected primary bson.D, got %T", ops.primary)
				}

				assertHasTruncationPush(t, doc, "arr", 3)
			},
		},
		{
			name: "truncation conflict splits indexed write into follow-up $set",
			event: &UpdateEvent{
				UpdateDescription: UpdateDescription{
					TruncatedArrays: []struct {
						Field   string `bson:"field"`
						NewSize int32  `bson:"newSize"`
					}{
						{Field: "arr", NewSize: 3},
					},
					UpdatedFields: bson.D{
						{Key: "arr.2", Value: "X"},
						{Key: "meta", Value: "Y"},
					},
				},
			},
			assert: func(t *testing.T, ops updateOps) {
				t.Helper()

				doc, ok := ops.primary.(bson.D)
				if !ok {
					t.Fatalf("expected primary bson.D, got %T", ops.primary)
				}

				// Primary must hold $push (truncation) and $set (non-conflicting).
				assertHasTruncationPush(t, doc, "arr", 3)

				primarySet := findSetDoc(t, doc)
				assert.Equal(t, bson.D{{Key: "meta", Value: "Y"}}, primarySet,
					"non-conflicting field 'meta' must remain in primary $set")

				// arr.2 conflicts with truncation 'arr', must spill to follow-up.
				followUpKeys := collectFollowUpKeys(t, ops.followUp)
				assert.Equal(t, []string{"arr.2"}, followUpKeys,
					"conflicting field 'arr.2' must be in follow-up $set")
			},
		},
		{
			name: "truncation of nested array spills inside-truncated-array writes to follow-up",
			event: &UpdateEvent{
				UpdateDescription: UpdateDescription{
					TruncatedArrays: []struct {
						Field   string `bson:"field"`
						NewSize int32  `bson:"newSize"`
					}{
						{Field: "groups.4.items", NewSize: 1000},
					},
					UpdatedFields: bson.D{
						{Key: "signature", Value: "sig"},
						{Key: "groups.4.count", Value: 1000},
						{Key: "groups.4.items.985", Value: "v0"},
						{Key: "groups.4.items.986", Value: "v1"},
					},
				},
			},
			assert: func(t *testing.T, ops updateOps) {
				t.Helper()

				doc, ok := ops.primary.(bson.D)
				if !ok {
					t.Fatalf("expected primary bson.D, got %T", ops.primary)
				}

				assertHasTruncationPush(t, doc, "groups.4.items", 1000)

				primarySet := findSetDoc(t, doc)
				gotKeys := setKeys(primarySet)
				// signature and groups.4.count do not conflict with truncation.
				assert.ElementsMatch(t, []string{"signature", "groups.4.count"}, gotKeys)

				// Both indexed writes inside the truncated path go to follow-up.
				followUpKeys := collectFollowUpKeys(t, ops.followUp)
				assert.ElementsMatch(t, []string{
					"groups.4.items.985",
					"groups.4.items.986",
				}, followUpKeys)
			},
		},
		{
			name: "non-conflicting prefix-match field stays in primary",
			event: &UpdateEvent{
				UpdateDescription: UpdateDescription{
					TruncatedArrays: []struct {
						Field   string `bson:"field"`
						NewSize int32  `bson:"newSize"`
					}{
						{Field: "items", NewSize: 5},
					},
					UpdatedFields: bson.D{
						{Key: "items_count", Value: 5}, // prefix 'items' but separate field
					},
				},
			},
			assert: func(t *testing.T, ops updateOps) {
				t.Helper()
				assert.Empty(t, ops.followUp,
					"items_count must not be treated as conflict with truncated 'items'")

				doc, ok := ops.primary.(bson.D)
				if !ok {
					t.Fatalf("expected primary bson.D, got %T", ops.primary)
				}

				primarySet := findSetDoc(t, doc)
				assert.Equal(t, []string{"items_count"}, setKeys(primarySet))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ops := collectUpdateOps(tt.event)
			tt.assert(t, ops)
		})
	}
}

// TestCollectUpdateOpsWithConflicts_ChunksConflictingFields verifies that many array-index
// updates conflicting with a truncation are chunked into multiple follow-up $set ops by
// maxBytesPerSetOp / maxFieldsPerSetOp, keeping each individual update bounded.
func TestCollectUpdateOpsWithConflicts_ChunksConflictingFields(t *testing.T) {
	t.Parallel()

	const numIndexed = 250 // > maxFieldsPerSetOp (100) → chunked into multiple follow-ups

	updatedFields := make(bson.D, 0, numIndexed)
	expectedKeys := make([]string, 0, numIndexed)

	for i := range numIndexed {
		key := "arr." + strconv.Itoa(i)
		updatedFields = append(updatedFields, bson.E{Key: key, Value: "v" + strconv.Itoa(i)})
		expectedKeys = append(expectedKeys, key)
	}

	event := &UpdateEvent{
		UpdateDescription: UpdateDescription{
			TruncatedArrays: []struct {
				Field   string `bson:"field"`
				NewSize int32  `bson:"newSize"`
			}{
				{Field: "arr", NewSize: int32(numIndexed)},
			},
			UpdatedFields: updatedFields,
		},
	}

	ops := collectUpdateOpsWithConflicts(event)

	// Primary should have just $push (truncation), no $set (no non-conflicting fields).
	doc, ok := ops.primary.(bson.D)
	if !ok {
		t.Fatalf("expected primary bson.D, got %T", ops.primary)
	}

	assertHasTruncationPush(t, doc, "arr", int32(numIndexed))
	assert.Empty(t, findSetDoc(t, doc), "no non-conflicting $set fields expected")

	// All conflicting fields must spill to follow-ups, chunked by count.
	expectedFollowUps := (numIndexed + maxFieldsPerSetOp - 1) / maxFieldsPerSetOp
	assert.Len(t, ops.followUp, expectedFollowUps)

	gotKeys := collectFollowUpKeys(t, ops.followUp)
	assert.Equal(t, expectedKeys, gotKeys, "conflicting fields must be preserved in source order")
}

// TestCollectUpdateOpsWithConflicts_ChunksLargeFields verifies byte-based chunking when
// individual fields are large (~20 KB each) - chunks split by maxBytesPerSetOp before
// reaching maxFieldsPerSetOp.
func TestCollectUpdateOpsWithConflicts_ChunksLargeFields(t *testing.T) {
	t.Parallel()

	const numFields = 100

	largeValue := strings.Repeat("X", 20_000)

	updatedFields := make(bson.D, 0, numFields)

	for i := range numFields {
		updatedFields = append(updatedFields, bson.E{
			Key:   "arr." + strconv.Itoa(i),
			Value: largeValue,
		})
	}

	event := &UpdateEvent{
		UpdateDescription: UpdateDescription{
			TruncatedArrays: []struct {
				Field   string `bson:"field"`
				NewSize int32  `bson:"newSize"`
			}{
				{Field: "arr", NewSize: int32(numFields)},
			},
			UpdatedFields: updatedFields,
		},
	}

	ops := collectUpdateOpsWithConflicts(event)

	// 100 × 20 KB = 2 MB total; maxBytesPerSetOp is 512 KiB → ~4 follow-up chunks.
	assert.NotEmpty(t, ops.followUp)
	assert.GreaterOrEqual(t, len(ops.followUp), 2)

	for i, fu := range ops.followUp {
		assert.Len(t, fu, 1, "follow-up %d should have exactly 1 operator ($set)", i)
		assert.Equal(t, setOp, fu[0].Key, "follow-up %d operator key", i)
	}

	gotKeys := collectFollowUpKeys(t, ops.followUp)
	assert.Len(t, gotKeys, numFields, "all fields must be preserved across follow-ups")
}

// TestCollectUpdateOpsWithConflicts_NestedArrayTruncation covers a truncated array
// nested inside another array (groups.<idx>.items where groups is itself an array).
// The truncation goes to a primary $push and indexed writes spill to follow-up $set
// ops, which correctly navigate dotted numeric paths through arrays without exhausting
// MongoDB's 125 MB BufBuilder.
func TestCollectUpdateOpsWithConflicts_NestedArrayTruncation(t *testing.T) {
	t.Parallel()

	const numIndexed = 15

	updatedFields := make(bson.D, 0, 3+numIndexed)
	updatedFields = append(updatedFields,
		bson.E{Key: "signature", Value: "sig"},
		bson.E{Key: "updated_at", Value: "now"},
		bson.E{Key: "groups.4.count", Value: 1000},
	)

	for i := range numIndexed {
		key := "groups.4.items." + strconv.Itoa(985+i)
		updatedFields = append(updatedFields, bson.E{Key: key, Value: "v" + strconv.Itoa(i)})
	}

	event := &UpdateEvent{
		UpdateDescription: UpdateDescription{
			TruncatedArrays: []struct {
				Field   string `bson:"field"`
				NewSize int32  `bson:"newSize"`
			}{
				{Field: "groups.4.items", NewSize: 1000},
			},
			UpdatedFields: updatedFields,
		},
	}

	ops := collectUpdateOpsWithConflicts(event)

	doc, ok := ops.primary.(bson.D)
	if !ok {
		t.Fatalf("expected primary bson.D (classic update), got %T", ops.primary)
	}

	assertHasTruncationPush(t, doc, "groups.4.items", 1000)

	primarySet := findSetDoc(t, doc)
	primaryKeys := setKeys(primarySet)
	assert.ElementsMatch(t, []string{"signature", "updated_at", "groups.4.count"}, primaryKeys)

	// Every array-index update key must appear in some follow-up $set.
	followUpKeys := collectFollowUpKeys(t, ops.followUp)
	for i := range numIndexed {
		key := "groups.4.items." + strconv.Itoa(985+i)
		assert.Contains(t, followUpKeys, key, "expected follow-up $set to include %s", key)
	}
}

// TestClientBulkWrite_FullByBytes verifies that the per-bulk byte budget triggers Full()
// before the count cap when individual writes are large.
func TestClientBulkWrite_FullByBytes(t *testing.T) {
	t.Parallel()

	cbw := newClientBulkWriter(10_000, false)
	ns := catalog.Namespace{Database: "db", Collection: "c"}

	largeValue := strings.Repeat("X", 1024*1024)

	for i := range 100 {
		raw, err := bson.Marshal(bson.D{{Key: "_id", Value: i}, {Key: "blob", Value: largeValue}})
		if err != nil {
			t.Fatalf("marshal full document: %v", err)
		}

		cbw.Insert(ns, &InsertEvent{
			DocumentKey:  bson.D{{Key: "_id", Value: i}},
			FullDocument: raw,
		})

		if cbw.Full() {
			break
		}
	}

	assert.True(t, cbw.Full(), "Full() must be true after exceeding maxBulkBytes")
	assert.Less(t, len(cbw.writes), 10_000, "Full() must trigger before reaching count cap")
	assert.GreaterOrEqual(t, cbw.bytes, maxBulkBytes, "byte counter must have reached maxBulkBytes")
}

// TestCollectionBulkWrite_FullByBytes mirrors the client-bulk byte budget test for the
// pre-MongoDB-8.0 collection-level bulk write path.
func TestCollectionBulkWrite_FullByBytes(t *testing.T) {
	t.Parallel()

	cbw := newCollectionBulkWriter(10_000, false)
	ns := catalog.Namespace{Database: "db", Collection: "c"}

	largeValue := strings.Repeat("X", 1024*1024)

	for i := range 100 {
		raw, err := bson.Marshal(bson.D{{Key: "_id", Value: i}, {Key: "blob", Value: largeValue}})
		if err != nil {
			t.Fatalf("marshal full document: %v", err)
		}

		cbw.Insert(ns, &InsertEvent{
			DocumentKey:  bson.D{{Key: "_id", Value: i}},
			FullDocument: raw,
		})

		if cbw.Full() {
			break
		}
	}

	assert.True(t, cbw.Full(), "Full() must be true after exceeding maxBulkBytes")
	assert.Less(t, cbw.count, 10_000, "Full() must trigger before reaching count cap")
	assert.GreaterOrEqual(t, cbw.bytes, maxBulkBytes, "byte counter must have reached maxBulkBytes")
}

// TestClientBulkWrite_FullByCount preserves the existing count-based Full() behavior for
// small writes that don't reach the byte budget.
func TestClientBulkWrite_FullByCount(t *testing.T) {
	t.Parallel()

	cbw := newClientBulkWriter(3, false)
	ns := catalog.Namespace{Database: "db", Collection: "c"}

	for i := range 3 {
		raw, err := bson.Marshal(bson.D{{Key: "_id", Value: i}})
		if err != nil {
			t.Fatalf("marshal full document: %v", err)
		}

		cbw.Insert(ns, &InsertEvent{
			DocumentKey:  bson.D{{Key: "_id", Value: i}},
			FullDocument: raw,
		})
	}

	assert.True(t, cbw.Full(), "Full() must be true at count cap")
	assert.Less(t, cbw.bytes, maxBulkBytes, "byte counter must be well below maxBulkBytes")
}

// TestCollectionBulkWrite_FullByCount preserves the count-based Full() behavior.
func TestCollectionBulkWrite_FullByCount(t *testing.T) {
	t.Parallel()

	cbw := newCollectionBulkWriter(3, false)
	ns := catalog.Namespace{Database: "db", Collection: "c"}

	for i := range 3 {
		raw, err := bson.Marshal(bson.D{{Key: "_id", Value: i}})
		if err != nil {
			t.Fatalf("marshal full document: %v", err)
		}

		cbw.Insert(ns, &InsertEvent{
			DocumentKey:  bson.D{{Key: "_id", Value: i}},
			FullDocument: raw,
		})
	}

	assert.True(t, cbw.Full(), "Full() must be true at count cap")
	assert.Less(t, cbw.bytes, maxBulkBytes, "byte counter must be well below maxBulkBytes")
}

// topLevelKeys returns the top-level keys of a classic update document in source order.
func topLevelKeys(doc bson.D) []string {
	keys := make([]string, len(doc))
	for i, e := range doc {
		keys[i] = e.Key
	}

	return keys
}

// findSetDoc returns the $set sub-document from a classic update doc, or nil if absent.
func findSetDoc(t *testing.T, doc bson.D) bson.D {
	t.Helper()

	for _, elem := range doc {
		if elem.Key != setOp {
			continue
		}

		setDoc, ok := elem.Value.(bson.D)
		if !ok {
			t.Fatalf("$set value is not bson.D: %T", elem.Value)
		}

		return setDoc
	}

	return nil
}

// setKeys returns the keys of a $set sub-document in source order.
func setKeys(setDoc bson.D) []string {
	keys := make([]string, len(setDoc))
	for i, e := range setDoc {
		keys[i] = e.Key
	}

	return keys
}

// assertHasTruncationPush verifies the classic update doc carries
// $push: {field: {$each: [], $slice: newSize}}.
func assertHasTruncationPush(t *testing.T, doc bson.D, field string, newSize int32) {
	t.Helper()

	for _, elem := range doc {
		if elem.Key != pushOp {
			continue
		}

		pushDoc, ok := elem.Value.(bson.D)
		if !ok {
			t.Fatalf("$push value is not bson.D: %T", elem.Value)
		}

		for _, p := range pushDoc {
			if p.Key != field {
				continue
			}

			spec, ok := p.Value.(bson.D)
			if !ok {
				t.Fatalf("$push.%s value is not bson.D: %T", field, p.Value)
			}

			var hasEach, hasSlice bool

			for _, s := range spec {
				switch s.Key {
				case "$each":
					arr, ok := s.Value.(bson.A)
					assert.True(t, ok && len(arr) == 0, "$each must be empty array")

					hasEach = true
				case "$slice":
					assert.Equal(t, newSize, s.Value, "$slice must equal newSize")

					hasSlice = true
				}
			}

			assert.True(t, hasEach, "$push.%s must include $each:[]", field)
			assert.True(t, hasSlice, "$push.%s must include $slice", field)

			return
		}

		t.Fatalf("$push does not contain field %q", field)
	}

	t.Fatalf("classic update doc does not contain $push for %q", field)
}

// collectFollowUpKeys returns every $set key across follow-up updates in source order.
func collectFollowUpKeys(t *testing.T, followUp []bson.D) []string {
	t.Helper()

	keys := make([]string, 0)

	for _, fu := range followUp {
		for _, elem := range fu {
			if elem.Key != setOp {
				continue
			}

			setDoc, ok := elem.Value.(bson.D)
			if !ok {
				t.Fatalf("$set value is not bson.D: %T", elem.Value)
			}

			for _, f := range setDoc {
				keys = append(keys, f.Key)
			}
		}
	}

	return keys
}
