package repl //nolint:testpackage // Exercises the approved cursor and dispatcher seams.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type scriptedDrain struct {
	events   []bson.Raw
	token    bson.Raw
	frontier bson.Timestamp
}

// scriptedChangeCursor ends each drain with one empty getMore, like TryNext.
// onDrained observes the emitted channel before the next drain starts. Everything
// runs synchronously, so observations cannot race the cursor's PBRT updates.
type scriptedChangeCursor struct {
	drains     []scriptedDrain
	drain      int
	nextEvent  int
	atBoundary bool
	onDrained  func(int)
}

func (c *scriptedChangeCursor) TryNext(context.Context) bool {
	if c.atBoundary {
		c.onDrained(c.drain)
		c.drain++
		c.nextEvent = 0
		c.atBoundary = false
	}
	if c.drain == len(c.drains) {
		return false
	}
	if c.nextEvent < len(c.drains[c.drain].events) {
		c.nextEvent++

		return true
	}
	c.atBoundary = true

	return false
}

func (c *scriptedChangeCursor) Current() bson.Raw {
	return c.drains[c.drain].events[c.nextEvent-1]
}

func (c *scriptedChangeCursor) ResumeToken() bson.Raw {
	return c.drains[c.drain].token
}

func (c *scriptedChangeCursor) Err() error {
	if c.drain == len(c.drains) {
		return context.Canceled
	}

	return nil
}

func (*scriptedChangeCursor) ID() int64 { return 1 }

func TestDrainChangeStream_TicksFollowScannedFrontier(t *testing.T) {
	t.Parallel()

	marshal := func(value any) bson.Raw {
		raw, err := bson.Marshal(value)
		require.NoError(t, err)

		return raw
	}
	backlog := make([]bson.Raw, 0, 3)
	for _, ts := range []bson.Timestamp{{T: 101, I: 1}, {T: 102, I: 1}, {T: 103, I: 1}} {
		backlog = append(backlog, marshal(bson.D{
			{"operationType", "insert"},
			{"clusterTime", ts},
			{"ns", bson.D{{"db", "test"}, {"coll", "documents"}}},
			{"documentKey", bson.D{{"_id", ts.T}}},
		}))
	}

	cur := &scriptedChangeCursor{drains: []scriptedDrain{
		{token: marshal(bson.D{{"_data", "820000006400000001"}}), frontier: bson.Timestamp{T: 100, I: 1}},
		{events: backlog, token: marshal(bson.D{{"_data", "820000006700000001"}}), frontier: bson.Timestamp{T: 103, I: 1}},
		{token: marshal(bson.D{{"_data", "82000003E800000005"}}), frontier: bson.Timestamp{T: 1000, I: 5}},
	}}
	changeCh := make(chan *ChangeEvent, 10)
	var timestamps []bson.Timestamp
	var ticksPerDrain []int
	cur.onDrained = func(drain int) {
		ticks := 0
		for len(changeCh) > 0 {
			change := <-changeCh
			timestamps = append(timestamps, change.ClusterTime)
			if change.OperationType == advanceTimePseudoEvent {
				ticks++
				assert.Falsef(t, change.ClusterTime.After(cur.drains[drain].frontier),
					"drain %d: tick %v overtook PBRT %v", drain+1, change.ClusterTime, cur.drains[drain].frontier)
			}
		}
		ticksPerDrain = append(ticksPerDrain, ticks)
	}
	var noteDrains []int
	r := &Repl{}
	err := r.drainChangeStream(t.Context(), cur, changeCh, func(context.Context) (bson.Timestamp, error) {
		noteDrains = append(noteDrains, cur.drain+1)

		return bson.Timestamp{T: 1000, I: 1}, nil
	})
	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, []int{1, 0, 1}, ticksPerDrain, "only empty drains emit ticks")
	assert.Equal(t, []int{1, 3}, noteDrains, "only empty drains stimulate the oplog")
	assert.Equal(t, []bson.Timestamp{
		{T: 100, I: 1}, {T: 101, I: 1}, {T: 102, I: 1}, {T: 103, I: 1}, {T: 1000, I: 5},
	}, timestamps)
	for i := 1; i < len(timestamps); i++ {
		assert.False(t, timestamps[i].Before(timestamps[i-1]), "emitted timestamps must be monotonic")
	}
}

func TestApplyTick_ReportsProgressOnlyWhenPoolIdle(t *testing.T) {
	t.Parallel()

	start := bson.Timestamp{T: 100, I: 1}
	routed := bson.Timestamp{T: 101, I: 1}
	tick := bson.Timestamp{T: 1000, I: 5}
	pool := makeTestPool(1)
	r := &Repl{pool: pool, lastReplicatedOpTime: start, checkpointOpTime: start}
	change := makeInsertEventWithTS("uncommitted", routed)
	pool.Route(change.change, change.ns)

	r.applyTick(tick, routed)
	assert.Equal(t, start, r.Status().LastReplicatedOpTime,
		"tick must not report routed-but-uncommitted events as replicated")
	assert.Equal(t, start, r.Status().CheckpointOpTime)

	// Commit through the real writer path, without a timer or live MongoDB.
	commitRoutedEvent(t, pool.workers[0])
	r.applyTick(tick, routed)
	assert.Equal(t, tick, r.Status().LastReplicatedOpTime, "idle pool may apply the scanned frontier")
	assert.Equal(t, start, r.Status().CheckpointOpTime, "ticks never advance the resume checkpoint")

	r.applyTick(start, routed)
	assert.Equal(t, tick, r.Status().LastReplicatedOpTime, "older ticks cannot regress reported progress")
}

// commitRoutedEvent drains one routed event into a bulk and runs the writer
// to completion so the worker records the commit exactly as production does.
func commitRoutedEvent(t *testing.T, w *worker) {
	t.Helper()

	w.currentBulkWrite = &mockBulkWriter{}
	w.newBulkWriter = func() bulkWriter { return &mockBulkWriter{} }
	w.pendingBulkCh = make(chan *pendingBulk, 1)
	w.writerDone = make(chan struct{})

	require.NoError(t, w.addToCurrentBulk(<-w.routedEventCh))

	require.True(t, w.enqueueBulk())
	close(w.pendingBulkCh)
	w.runWriter(t.Context())
}

// Distinct change events can share a clusterTime (multi-document transactions,
// applyOps). Idleness must be decided by exact event accounting, not by
// comparing the committed and routed timestamps, or a tick and the
// filtered-event checkpoint path both run ahead of a queued same-timestamp event.
func TestPoolIdle_EqualTimestampEventStillQueued(t *testing.T) {
	t.Parallel()

	start := bson.Timestamp{T: 100, I: 1}
	shared := bson.Timestamp{T: 101, I: 1}
	tick := bson.Timestamp{T: 1000, I: 5}
	pool := makeTestPool(1)
	w := pool.workers[0]

	first := makeInsertEventWithTS("txn-first", shared)
	pool.Route(first.change, first.ns)
	commitRoutedEvent(t, w)
	require.Equal(t, shared, *w.lastCommittedTS.Load())

	second := makeInsertEventWithTS("txn-second", shared)
	pool.Route(second.change, second.ns)
	require.Len(t, w.routedEventCh, 1)

	assert.False(t, pool.Idle(), "a queued event sharing the committed timestamp is not idle")

	r := &Repl{pool: pool, lastReplicatedOpTime: start, checkpointOpTime: start}
	r.applyTick(tick, shared)
	assert.Equal(t, start, r.Status().LastReplicatedOpTime,
		"tick must not pass the unapplied second event sharing the committed timestamp")

	// The same predicate guards the filtered-event checkpoint advance in run().
	assert.False(t, r.poolIdle(shared), "checkpoint must not advance past the queued event")

	commitRoutedEvent(t, w)
	assert.True(t, pool.Idle())
	r.applyTick(tick, shared)
	assert.Equal(t, tick, r.Status().LastReplicatedOpTime, "both commits done: tick applies")
	assert.Equal(t, start, r.Status().CheckpointOpTime, "ticks never advance the resume checkpoint")
}

func TestPoolIdle_WriterFailureNeverReportsIdle(t *testing.T) {
	t.Parallel()

	shared := bson.Timestamp{T: 101, I: 1}
	pool := makeTestPool(1)
	w := pool.workers[0]
	w.errCh = make(chan error, 1)

	first := makeInsertEventWithTS("txn-first", shared)
	pool.Route(first.change, first.ns)
	commitRoutedEvent(t, w)

	second := makeInsertEventWithTS("txn-second", shared)
	pool.Route(second.change, second.ns)

	// The second event's bulk fails: it is routed, never committed.
	w.currentBulkWrite = &mockBulkWriter{doErr: assert.AnError}
	w.newBulkWriter = func() bulkWriter { return &mockBulkWriter{} }
	w.pendingBulkCh = make(chan *pendingBulk, 1)
	w.writerDone = make(chan struct{})
	require.NoError(t, w.addToCurrentBulk(<-w.routedEventCh))
	require.True(t, w.enqueueBulk())
	close(w.pendingBulkCh)
	w.runWriter(t.Context())
	require.Error(t, w.writerErr)

	assert.False(t, pool.Idle(), "a failed bulk leaves its events uncommitted")
	assert.Equal(t, shared, *w.lastCommittedTS.Load(), "timestamp equality alone would have looked idle")
}
