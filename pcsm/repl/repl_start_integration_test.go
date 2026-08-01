//go:build integration

package repl

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/percona/percona-clustersync-mongodb/mdb"
	"github.com/percona/percona-clustersync-mongodb/pcsm/catalog"
	"github.com/percona/percona-clustersync-mongodb/sel"
)

func TestRepl_Start_exposesStartAtImmediately(t *testing.T) {
	// Given
	setupCtx, cancelSetup := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancelSetup()

	mongoURI := newReplTestReplicaSet(setupCtx, t)
	aggregateStarted := make(chan struct{})
	releaseAggregate := make(chan struct{})
	var blockAggregate sync.Once
	var releaseAggregateOnce sync.Once
	release := func() {
		releaseAggregateOnce.Do(func() { close(releaseAggregate) })
	}
	commandMonitor := &event.CommandMonitor{
		Started: func(_ context.Context, started *event.CommandStartedEvent) {
			if started.CommandName != "aggregate" {
				return
			}

			blockAggregate.Do(func() {
				close(aggregateStarted)
				<-releaseAggregate
			})
		},
	}

	client, err := mongo.Connect(
		options.Client().
			ApplyURI(mongoURI).
			SetServerSelectionTimeout(30 * time.Second).
			SetMonitor(commandMonitor),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		require.NoError(t, client.Disconnect(cleanupCtx))
	})
	require.NoError(t, client.Ping(setupCtx, nil))

	startAt, err := mdb.AdvanceClusterTime(setupCtx, client)
	require.NoError(t, err)
	require.False(t, startAt.IsZero())
	sourceVersion, err := mdb.Version(setupCtx, client)
	require.NoError(t, err)
	r := NewRepl(
		client,
		client,
		catalog.NewCatalog(client, client, sourceVersion),
		sel.AllowAllFilter,
		&Options{NumWorkers: 1},
		sourceVersion,
		false,
		false,
	)
	replCtx, cancelRepl := context.WithCancel(t.Context())
	var stopReplOnce sync.Once
	stopRepl := func() {
		stopReplOnce.Do(func() {
			cancelRepl()
			release()
			select {
			case <-r.Done():
			case <-time.After(10 * time.Second):
				require.FailNow(t, "Repl did not stop after cancellation")
			}
		})
	}
	t.Cleanup(stopRepl)

	// When
	require.NoError(t, r.Start(replCtx, startAt))
	select {
	case <-aggregateStarted:
	case <-setupCtx.Done():
		require.FailNow(t, "Repl did not start its change stream", setupCtx.Err().Error())
	}
	status := r.Status()

	// Then
	require.Equal(t, startAt, status.LastReplicatedOpTime)
	stopRepl()
}
