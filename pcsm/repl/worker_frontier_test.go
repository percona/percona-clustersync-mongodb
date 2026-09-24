package repl //nolint:testpackage // Exercises worker pool frontier accounting through the real writer path.

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestReportedFrontier(t *testing.T) {
	t.Parallel()

	type routed struct {
		worker int
		ts     bson.Timestamp
		commit bool // commit this event through the real writer path
		fail   bool // the bulk for this event fails
	}

	tests := []struct {
		name   string
		events []routed
		want   bson.Timestamp
	}{
		{
			name: "never routed pool reports nothing",
			want: bson.Timestamp{},
		},
		{
			name: "slower worker with outstanding work bounds a faster one",
			events: []routed{
				{worker: 0, ts: bson.Timestamp{T: 101, I: 1}, commit: true},
				{worker: 0, ts: bson.Timestamp{T: 150, I: 1}},
				{worker: 1, ts: bson.Timestamp{T: 201, I: 1}, commit: true},
				{worker: 1, ts: bson.Timestamp{T: 202, I: 1}},
			},
			want: bson.Timestamp{T: 101, I: 1},
		},
		{
			name: "never committed worker holds the frontier at its first event",
			events: []routed{
				{worker: 0, ts: bson.Timestamp{T: 101, I: 1}},
				{worker: 1, ts: bson.Timestamp{T: 201, I: 1}, commit: true},
			},
			want: bson.Timestamp{T: 101, I: 1},
		},
		{
			name: "failed write is outstanding, not drained",
			events: []routed{
				{worker: 0, ts: bson.Timestamp{T: 101, I: 1}, commit: true},
				{worker: 0, ts: bson.Timestamp{T: 102, I: 1}, fail: true},
				{worker: 1, ts: bson.Timestamp{T: 201, I: 1}, commit: true},
			},
			want: bson.Timestamp{T: 101, I: 1},
		},
		{
			name: "distinct events sharing a timestamp keep the worker busy",
			events: []routed{
				{worker: 0, ts: bson.Timestamp{T: 101, I: 1}, commit: true},
				{worker: 0, ts: bson.Timestamp{T: 101, I: 1}},
				{worker: 1, ts: bson.Timestamp{T: 201, I: 1}, commit: true},
			},
			want: bson.Timestamp{T: 101, I: 1},
		},
		{
			name: "all drained reports the newest commit",
			events: []routed{
				{worker: 0, ts: bson.Timestamp{T: 101, I: 1}, commit: true},
				{worker: 1, ts: bson.Timestamp{T: 201, I: 1}, commit: true},
			},
			want: bson.Timestamp{T: 201, I: 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			pool := makeTestPool(2)
			for _, ev := range tt.events {
				event := makeInsertEventWithTS(docIDForWorker(t, ev.worker, 2), ev.ts)
				pool.Route(event.change, event.ns)
				switch {
				case ev.commit:
					commitRoutedEvent(t, pool.workers[ev.worker])
				case ev.fail:
					failRoutedEvent(t, pool.workers[ev.worker])
				}
			}

			assert.Equal(t, tt.want, pool.ReportedFrontier())
			assert.False(t, pool.ReportedFrontier().Before(pool.Checkpoint()),
				"reporting never trails the resume floor")
		})
	}
}

// failRoutedEvent drains one routed event into a bulk whose write fails, so
// the worker counts it as routed but never committed.
func failRoutedEvent(t *testing.T, w *worker) {
	t.Helper()

	w.errCh = make(chan error, 1)
	w.currentBulkWrite = &mockBulkWriter{doErr: assert.AnError}
	w.newBulkWriter = func() bulkWriter { return &mockBulkWriter{} }
	w.pendingBulkCh = make(chan *pendingBulk, 1)
	w.writerDone = make(chan struct{})

	require.NoError(t, w.addToCurrentBulk(<-w.routedEventCh))
	require.True(t, w.enqueueBulk())
	close(w.pendingBulkCh)
	w.runWriter(t.Context())
	require.Error(t, w.writerErr)
}
