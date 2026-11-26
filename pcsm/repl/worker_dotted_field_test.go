package repl //nolint

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/percona/percona-clustersync-mongodb/pcsm/catalog"
)

// recordingBulkWriter records which bulkWriter method received each event.
type recordingBulkWriter struct {
	mockBulkWriter

	updates  []*UpdateEvent
	replaces []*ReplaceEvent
	deletes  []*DeleteEvent
}

func (r *recordingBulkWriter) Update(ns catalog.Namespace, e *UpdateEvent) {
	r.mockBulkWriter.Update(ns, e)
	r.updates = append(r.updates, e)
}

func (r *recordingBulkWriter) Replace(ns catalog.Namespace, e *ReplaceEvent) {
	r.mockBulkWriter.Replace(ns, e)
	r.replaces = append(r.replaces, e)
}

func (r *recordingBulkWriter) Delete(ns catalog.Namespace, e *DeleteEvent) {
	r.mockBulkWriter.Delete(ns, e)
	r.deletes = append(r.deletes, e)
}

// makeUpdateEvent creates a routedEvent for an update whose updateDescription is given as
// Extended JSON, the way a MongoDB 8.0 change stream reports it with showExpandedEvents.
func makeUpdateEvent(t *testing.T, updateDescription string) *routedEvent {
	t.Helper()

	var desc bson.D
	require.NoError(t, bson.UnmarshalExtJSON([]byte(updateDescription), false, &desc))

	raw, err := bson.Marshal(bson.D{
		{"documentKey", bson.D{{"_id", 1}}},
		{"updateDescription", desc},
	})
	require.NoError(t, err)

	ns := catalog.Namespace{Database: "testdb", Collection: "testcoll"}

	return &routedEvent{
		ns: ns,
		change: &ChangeEvent{
			OperationType: Update,
			Namespace:     ns,
			RawData:       bson.Raw(raw),
		},
	}
}

// The update descriptions below are the ones a MongoDB 8.0 change stream emits for
// aggregation-pipeline updates ($setField, $unsetField, $literal) of the named shapes.
func TestUpdateDescription_HasDottedFieldName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		desc string
		want bool
	}{
		{
			name: "set a dotted key inside a document",
			desc: `{"updatedFields": {"files.main.ts": "A2"}, "removedFields": [], "truncatedArrays": [],
				"disambiguatedPaths": {"files.main.ts": ["files", "main.ts"]}}`,
			want: true,
		},
		{
			name: "remove a dotted key inside a document",
			desc: `{"updatedFields": {}, "removedFields": ["files.b.ts"], "truncatedArrays": [],
				"disambiguatedPaths": {"files.b.ts": ["files", "b.ts"]}}`,
			want: true,
		},
		{
			name: "set a dotted top-level field",
			desc: `{"updatedFields": {"a.b": 2}, "removedFields": [], "truncatedArrays": [],
				"disambiguatedPaths": {"a.b": ["a.b"]}}`,
			want: true,
		},
		{
			name: "set a URL key inside an array element",
			desc: `{"updatedFields": {"m.0.d.https://x/a.png": "cap2"}, "removedFields": [], "truncatedArrays": [],
				"disambiguatedPaths": {"m.0.d.https://x/a.png": ["m", 0, "d", "https://x/a.png"]}}`,
			want: true,
		},
		{
			name: "truncate an array stored under a dotted key",
			desc: `{"updatedFields": {}, "removedFields": [],
				"truncatedArrays": [{"field": "arrs.x.y", "newSize": 2}, {"field": "arrs.z", "newSize": 2}],
				"disambiguatedPaths": {"arrs.x.y": ["arrs", "x.y"]}}`,
			want: true,
		},
		{
			name: "one dotted parent among plain fields",
			desc: `{"updatedFields": {"bp.dn.my.func.files": {}, "bp.dn.plain.files": {}}, "removedFields": [],
				"truncatedArrays": [], "disambiguatedPaths": {"bp.dn.my.func.files": ["bp", "dn", "my.func", "files"]}}`,
			want: true,
		},
		{
			name: "numeric object key is not a dotted name",
			desc: `{"updatedFields": {"arr.0.1": 7, "o.1": "uno"}, "removedFields": [], "truncatedArrays": [],
				"disambiguatedPaths": {"o.1": ["o", "1"]}}`,
			want: false,
		},
		{
			name: "nested path without ambiguity",
			desc: `{"updatedFields": {"a.b.c": 1}, "removedFields": ["a.d"], "truncatedArrays": [],
				"disambiguatedPaths": {}}`,
			want: false,
		},
		{
			name: "no disambiguatedPaths (MongoDB before 6.1)",
			desc: `{"updatedFields": {"files.main.ts": "A2"}}`,
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			parsed, err := parseDMLEvent(makeUpdateEvent(t, tt.desc).change.RawData, Update)
			require.NoError(t, err)

			event, ok := parsed.(UpdateEvent)
			require.True(t, ok)

			assert.Equal(t, tt.want, event.UpdateDescription.hasDottedFieldName())
		})
	}
}

func TestWorker_AddToCurrentBulk_UpdateWithoutDottedFieldNameIsReplayed(t *testing.T) {
	t.Parallel()

	bw := &recordingBulkWriter{}
	w := &worker{currentBulkWrite: bw}

	err := w.addToCurrentBulk(t.Context(), makeUpdateEvent(t,
		`{"updatedFields": {"a.b.c": 1, "o.1": "uno"}, "disambiguatedPaths": {"o.1": ["o", "1"]}}`))
	require.NoError(t, err)

	// No source client is set: a plain update must not need one.
	require.Len(t, bw.updates, 1)
	assert.Empty(t, bw.replaces)
	assert.Empty(t, bw.deletes)
}

func TestWorker_AddToCurrentBulk_UpdateWithDottedFieldNameIsNotReplayed(t *testing.T) {
	t.Parallel()

	bw := &recordingBulkWriter{}
	w := &worker{currentBulkWrite: bw}

	err := w.addToCurrentBulk(t.Context(), makeUpdateEvent(t,
		`{"updatedFields": {"files.main.ts": "A2"}, "disambiguatedPaths": {"files.main.ts": ["files", "main.ts"]}}`))

	// Without a source client the document cannot be refetched. The event must fail
	// rather than be replayed as $set on the ambiguous path "files.main.ts".
	require.ErrorContains(t, err, "requires source client")
	assert.True(t, bw.Empty())
	assert.Empty(t, bw.updates)
}
