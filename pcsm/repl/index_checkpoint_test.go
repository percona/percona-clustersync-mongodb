package repl //nolint:testpackage // Exercises dispatcher, worker barrier and checkpoint acknowledgement together.

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/percona/percona-clustersync-mongodb/errors"
	"github.com/percona/percona-clustersync-mongodb/mdb"
	"github.com/percona/percona-clustersync-mongodb/util"
)

type indexCheckpointCatalog struct {
	mockCatalog

	indexCalls atomic.Int32
}

func (c *indexCheckpointCatalog) CreateIndexes(context.Context, string, string, []*mdb.IndexSpecification) error {
	c.indexCalls.Add(1)

	return nil
}

func (c *indexCheckpointCatalog) DropIndex(context.Context, string, string, string) error {
	c.indexCalls.Add(1)

	return nil
}

func checkpointToken(t *testing.T, id string) bson.Raw {
	t.Helper()
	raw, err := bson.Marshal(bson.D{{Key: "_data", Value: id}})
	require.NoError(t, err)

	return raw
}

func checkpointIndexEvent(t *testing.T, op OperationType, ts bson.Timestamp) *ChangeEvent {
	t.Helper()
	raw, err := bson.Marshal(bson.D{
		{Key: "_id", Value: checkpointToken(t, string(op))},
		{Key: "operationType", Value: op},
		{Key: "clusterTime", Value: ts},
		{Key: "ns", Value: bson.D{{Key: "db", Value: "testdb"}, {Key: "coll", Value: "testcoll"}}},
		{Key: "operationDescription", Value: bson.D{{Key: "indexes", Value: bson.A{
			bson.D{{Key: "name", Value: "x_1"}, {Key: "key", Value: bson.D{{Key: "x", Value: 1}}}},
		}}}},
	})
	require.NoError(t, err)
	change := &ChangeEvent{RawData: raw}
	require.NoError(t, parseEventHeader(raw, &change.EventHeader))

	return change
}

func awaitCheckpointSignal(t *testing.T, ch <-chan struct{}) {
	t.Helper()

	select {
	case <-ch:
	case <-time.After(barrierTimeout):
		require.FailNow(t, "checkpoint operation did not finish")
	}
}

func awaitResumeOptions(
	t *testing.T,
	ch <-chan *options.ChangeStreamOptionsBuilder,
) *options.ChangeStreamOptionsBuilder {
	t.Helper()

	select {
	case result := <-ch:
		return result
	case <-time.After(barrierTimeout):
		require.FailNow(t, "resume options were not delivered")

		return nil
	}
}

func resolvedResumeOptions(t *testing.T, builder *options.ChangeStreamOptionsBuilder) options.ChangeStreamOptions {
	t.Helper()
	var opts options.ChangeStreamOptions
	for _, apply := range builder.List() {
		require.NoError(t, apply(&opts))
	}

	return opts
}

func runIndexCheckpointTest(t *testing.T, fn func(context.Context)) {
	t.Helper()
	require.NoError(t, util.CtxWithTimeout(t.Context(), barrierTimeout, func(ctx context.Context) error {
		fn(ctx)

		return nil
	}))
}

func TestIndexCheckpoint_DrainBeforeAcknowledgementBeforeIndex(t *testing.T) {
	t.Parallel()

	for _, op := range []OperationType{CreateIndexes, DropIndexes} {
		t.Run(string(op), func(t *testing.T) {
			t.Parallel()
			runIndexCheckpointTest(t, func(ctx context.Context) {
				gate := newBlockingGate()
				pool := makeTestPoolLiveWithParent(t, ctx, []bulkWriter{gate.writer()}, func() bulkWriter {
					return &mockBulkWriter{}
				})
				cat := &indexCheckpointCatalog{}
				ts := bson.Timestamp{T: 100, I: 1}
				prior := makeInsertEventWithTS("predecessor", ts).change
				prior.ID = checkpointToken(t, "predecessor")
				pending := checkpointIndexEvent(t, op, ts)
				requested := make(chan *Checkpoint, 1)
				ack := make(chan struct{})
				r := &Repl{
					pool: pool, catalog: cat, startTime: time.Unix(1, 0),
					nsFilter: func(string, string) bool { return true },
					options:  &Options{},
				}
				r.options.Checkpoint = func(ctx context.Context) error {
					requested <- r.Checkpoint()
					select {
					case <-ack:
						return nil
					case <-ctx.Done():
						return errors.Wrap(ctx.Err(), "checkpoint canceled")
					}
				}
				queue := changeStreamQueue{
					events: make(chan *ChangeEvent, 3),
					resume: make(chan *options.ChangeStreamOptionsBuilder, 1),
				}
				queue.events <- prior
				queue.events <- pending
				queue.events <- nil // The retry must not capture an older boundary.
				close(queue.events)
				done := make(chan struct{})
				go func() {
					defer close(done)
					r.dispatchChanges(ctx, queue)
				}()

				gate.awaitParked(t)
				assert.Empty(t, requested, "uncommitted predecessor cannot be checkpointed")
				gate.releaseAll()
				var cp *Checkpoint
				select {
				case cp = <-requested:
				case <-ctx.Done():
					require.FailNow(t, "checkpoint was not requested after drain")
				}
				assert.Equal(t, prior.ID, cp.StartAfter)
				assert.Equal(t, ts, cp.CheckpointOpTime)
				assert.Equal(t, int64(1), cp.EventsApplied, "pending index is not applied")
				assert.Zero(t, cat.indexCalls.Load(), "index must wait for its own acknowledgement")
				assert.Empty(t, queue.resume, "reconnect must wait behind the pending index")

				close(ack)
				awaitCheckpointSignal(t, done)
				assert.Equal(t, int32(1), cat.indexCalls.Load())
				assert.Equal(t, int64(2), r.Status().EventsApplied)
				opts := resolvedResumeOptions(t, awaitResumeOptions(t, queue.resume))
				assert.Equal(t, prior.ID, opts.StartAfter)
				assert.Nil(t, opts.StartAtOperationTime)
				assert.False(t, r.shouldSkipReplay(pending), "pending same-timestamp index must replay")
				assert.False(t, r.shouldSkipReplay(makeInsertEventWithTS("following", ts).change))
			})
		})
	}
}

func TestIndexCheckpoint_RejectsUnacknowledgedBoundary(t *testing.T) {
	t.Parallel()
	failure := errors.New("checkpoint write failed")
	tests := []struct {
		name          string
		receipt       error
		missing       bool
		cancel        bool
		wait          bool
		expectFailure bool
	}{
		{name: "missing writer", missing: true, expectFailure: true},
		{name: "failed write", receipt: failure, expectFailure: true},
		{name: "writer contained before epoch cancellation", receipt: errors.Wrap(context.Canceled, "fenced")},
		{name: "writer deadline", receipt: context.DeadlineExceeded},
		{name: "epoch canceled after acknowledgement", cancel: true},
		{name: "epoch canceled while awaiting acknowledgement", wait: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			cat := &indexCheckpointCatalog{}
			requested := make(chan struct{})
			done := make(chan struct{})
			close(done)
			r := &Repl{
				pool:    makeTestPoolLive(t, []bulkWriter{&mockBulkWriter{}}),
				catalog: cat, options: &Options{}, doneCh: done, pauseCh: make(chan struct{}, 1),
				startTime: time.Unix(1, 0),
			}
			if !tt.missing {
				r.options.Checkpoint = func(context.Context) error {
					if tt.wait {
						close(requested)
						<-ctx.Done()

						return errors.Wrap(ctx.Err(), "checkpoint canceled")
					}
					if tt.cancel {
						cancel()
					}

					return tt.receipt
				}
			}
			result := make(chan bool, 1)
			change := checkpointIndexEvent(t, CreateIndexes, bson.Timestamp{T: 100})
			go func() {
				result <- r.applyDDL(ctx, change, nil)
			}()
			if tt.wait {
				awaitCheckpointSignal(t, requested)
				cancel()
			}

			select {
			case applied := <-result:
				assert.False(t, applied)
			case <-time.After(barrierTimeout):
				require.FailNow(t, "index boundary did not stop")
			}
			assert.Zero(t, cat.indexCalls.Load())
			assert.Zero(t, r.Status().EventsApplied)
			if tt.expectFailure {
				require.Error(t, r.Status().Err)
				if !tt.missing {
					require.ErrorIs(t, r.Status().Err, failure)
				}
			} else {
				require.NoError(t, r.Status().Err, "tenure containment must not become persisted failure")
			}
		})
	}
}
