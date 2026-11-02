package main //nolint:testpackage // Exercises the tenure's single checkpoint writer.

import (
	"context"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
)

type requestedSnapshot struct {
	value atomic.Int32
	empty bool
}

func (s *requestedSnapshot) Checkpoint(context.Context) ([]byte, error) {
	if s.empty {
		return nil, nil
	}

	return bson.Marshal(bson.D{{"value", s.value.Load()}}) //nolint:wrapcheck // test snapshot
}

func (*requestedSnapshot) Recover(context.Context, []byte) error { return nil }

func TestCheckpointRequestWaitsForOwnWrite(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		entered := make(chan int32, 2)
		release := make(chan struct{})
		monitor := &event.CommandMonitor{Started: func(ctx context.Context, e *event.CommandStartedEvent) {
			if e.CommandName != "findAndModify" {
				return
			}
			entered <- e.Command.Lookup("update", "$set", "data", "value").Int32()
			select {
			case <-release:
			case <-ctx.Done():
			}
		}}
		target := mockedTarget(t, monitor, claimed(bson.D{{"_id", recoveryID}}), claimed(bson.D{{"_id", recoveryID}}))
		defer func() {
			cancel()
			require.NoError(t, target.Disconnect(t.Context()))
		}()
		rec := &requestedSnapshot{}
		rec.value.Store(1)
		requests := make(checkpointRequests)
		stopped := make(chan error, 1)
		go func() {
			stopped <- RunCheckpointing(ctx, target, rec, 1, "active", time.Second, nil, requests)
		}()

		// The timer write captured the old state and is still on the wire.
		require.Equal(t, int32(1), <-entered)
		rec.value.Store(2)
		receipt := make(chan error, 1)
		go func() { receipt <- requests.request(ctx, ctx) }()
		synctest.Wait()
		require.Empty(t, receipt)

		release <- struct{}{}
		require.Equal(t, int32(2), <-entered, "the request must capture its own snapshot")
		synctest.Wait()
		require.Empty(t, receipt, "the earlier timer write cannot acknowledge this request")

		release <- struct{}{}
		require.NoError(t, <-receipt)
		cancel()
		require.NoError(t, <-stopped)
	})
}

func TestCheckpointRequestFailureIsContainmentBeforeEpochCancellation(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		response bson.D
		empty    bool
		want     error
	}{
		{name: "fenced", response: noDocument(), want: errCheckpointFenced},
		{name: "unsettled", response: bson.D{{"ok", 0}, {"code", 2}, {"errmsg", "rejected write"}}},
		{name: "missing snapshot", empty: true, want: errNoRecoveryData},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				ctx := t.Context()
				target := mockedTarget(t, nil, tc.response)
				defer func() { require.NoError(t, target.Disconnect(ctx)) }()
				requests := make(checkpointRequests)
				stopped := make(chan error, 1)
				go func() {
					stopped <- RunCheckpointing(ctx, target, &requestedSnapshot{empty: tc.empty},
						1, "active", time.Hour, nil, requests)
				}()

				require.ErrorIs(t, requests.request(ctx, ctx), context.Canceled)
				err := <-stopped
				require.Error(t, err)
				if tc.want != nil {
					require.ErrorIs(t, err, tc.want)
				}
				require.NoError(t, ctx.Err(), "classification must not depend on epoch cancellation")
			})
		})
	}
}

func TestCheckpointRequestCancellationDoesNotStrandWriter(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		ctx, stop := context.WithCancel(t.Context())
		defer stop()
		caller, cancel := context.WithCancel(ctx)
		entered := make(chan struct{}, 1)
		release := make(chan struct{})
		monitor := &event.CommandMonitor{Started: func(ctx context.Context, e *event.CommandStartedEvent) {
			if e.CommandName == "findAndModify" {
				entered <- struct{}{}
				select {
				case <-release:
				case <-ctx.Done():
				}
			}
		}}
		target := mockedTarget(t, monitor, claimed(bson.D{{"_id", recoveryID}}), claimed(bson.D{{"_id", recoveryID}}))
		defer func() {
			stop()
			require.NoError(t, target.Disconnect(t.Context()))
		}()
		requests := make(checkpointRequests)
		stopped := make(chan error, 1)
		go func() {
			stopped <- RunCheckpointing(ctx, target, &requestedSnapshot{}, 1, "active", time.Hour, nil, requests)
		}()
		first := make(chan error, 1)
		go func() { first <- requests.request(caller, ctx) }()
		<-entered
		cancel()
		require.ErrorIs(t, <-first, context.Canceled)
		release <- struct{}{}

		// No caller is left receiving the first reply. The writer must still
		// accept and acknowledge another request, then join on cancellation.
		second := make(chan error, 1)
		go func() { second <- requests.request(ctx, ctx) }()
		<-entered
		release <- struct{}{}
		require.NoError(t, <-second)
		stop()
		require.NoError(t, <-stopped)
	})
}

func TestCheckpointRequestStopsDuringAdmissionAndReceipt(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name       string
		admitted   bool
		writerLost bool
	}{
		{name: "admission/caller"},
		{name: "admission/writer", writerLost: true},
		{name: "receipt/caller", admitted: true},
		{name: "receipt/writer", admitted: true, writerLost: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				caller, cancel := context.WithCancel(t.Context())
				defer cancel()
				writer, stop := context.WithCancel(t.Context())
				defer stop()
				requests := make(checkpointRequests)
				result := make(chan error, 1)
				go func() { result <- requests.request(caller, writer) }()
				if tc.admitted {
					<-requests
				}
				synctest.Wait()
				if tc.writerLost {
					stop()
				} else {
					cancel()
				}
				require.ErrorIs(t, <-result, context.Canceled)
			})
		})
	}
}

func TestCheckpointWriterFailureReleasesQueuedRequester(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		epoch := t.Context()
		writer, stop := context.WithCancel(epoch)
		defer stop()
		entered, release := make(chan struct{}), make(chan struct{})
		monitor := &event.CommandMonitor{Started: func(_ context.Context, e *event.CommandStartedEvent) {
			if e.CommandName == "findAndModify" {
				close(entered)
				<-release
			}
		}}
		target := mockedTarget(t, monitor, noDocument())
		defer func() { require.NoError(t, target.Disconnect(epoch)) }()
		requests := make(checkpointRequests)
		stopped := make(chan error, 1)
		go func() {
			err := RunCheckpointing(epoch, target, &requestedSnapshot{}, 1, "active", time.Second, nil, requests)
			stop()
			stopped <- err
		}()
		<-entered
		receipt := make(chan error, 1)
		go func() { receipt <- requests.request(epoch, writer) }()
		synctest.Wait()
		close(release)

		require.ErrorIs(t, <-receipt, context.Canceled)
		require.ErrorIs(t, <-stopped, errCheckpointFenced)
		require.NoError(t, epoch.Err())
	})
}
