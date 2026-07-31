//go:build integration

package repl

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"

	"github.com/percona/percona-clustersync-mongodb/errors"
	"github.com/percona/percona-clustersync-mongodb/mdb"
)

type monitoredWriteOutcome[T any] struct {
	result T
	err    error
}

func TestWatchChangeEvents_EmitsMonotonicTimestampsWhenWritesCommitAroundAppendOplogNote(t *testing.T) {
	// Given
	setupCtx, cancelSetup := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancelSetup()

	mongoVersion := os.Getenv("MONGO_VERSION")
	if mongoVersion == "" {
		mongoVersion = "8.0"
	}

	mongod, err := testcontainers.GenericContainer(setupCtx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "percona/percona-server-mongodb:" + mongoVersion,
			ExposedPorts: []string{"27017/tcp"},
			Cmd: []string{
				"mongod", "--quiet", "--bind_ip_all", "--dbpath", "/data/db",
				"--wiredTigerCacheSizeGB", "0.5",
				"--replSet", "rs0", "--port", "27017",
			},
			WaitingFor: wait.ForLog("Waiting for connections").WithStartupTimeout(60 * time.Second),
		},
		Started: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		require.NoError(t, mongod.Terminate(cleanupCtx))
	})

	exitCode, _, err := mongod.Exec(setupCtx, []string{
		"mongosh", "--quiet", "--eval",
		"rs.initiate({_id:'rs0', members:[{_id:0, host:'localhost:27017'}]})",
	})
	require.NoError(t, err)
	require.Zero(t, exitCode)
	require.Eventually(t, func() bool {
		exitCode, _, execErr := mongod.Exec(setupCtx, []string{
			"mongosh", "--quiet", "--eval", "exit(db.hello().isWritablePrimary ? 0 : 1)",
		})
		return execErr == nil && exitCode == 0
	}, 60*time.Second, 100*time.Millisecond, "replica set did not elect a writable primary")

	host, err := mongod.Host(setupCtx)
	require.NoError(t, err)
	mappedPort, err := mongod.MappedPort(setupCtx, "27017/tcp")
	require.NoError(t, err)
	mongoURI := fmt.Sprintf("mongodb://%s:%s/?directConnection=true", host, mappedPort.Port())

	writeClient, err := mongo.Connect(
		options.Client().
			ApplyURI(mongoURI).
			SetServerSelectionTimeout(30 * time.Second).
			SetWriteConcern(writeconcern.Majority()),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		require.NoError(t, writeClient.Disconnect(cleanupCtx))
	})
	require.NoError(t, writeClient.Ping(setupCtx, nil))

	collection := writeClient.Database("watch_change_events_test").Collection("documents")
	insertResult, err := collection.InsertOne(setupCtx, bson.D{{"_id", "seed"}})
	require.NoError(t, err)
	require.True(t, insertResult.Acknowledged)

	startAt, err := mdb.AdvanceClusterTime(setupCtx, writeClient)
	require.NoError(t, err)

	deleteDone := make(chan monitoredWriteOutcome[*mongo.DeleteResult], 1)
	insertDone := make(chan monitoredWriteOutcome[*mongo.InsertOneResult], 1)
	var deleteCalls atomic.Int64
	var insertCalls atomic.Int64
	var appendStartedCalls atomic.Int64
	var appendSucceededCalls atomic.Int64
	commandMonitor := &event.CommandMonitor{
		Started: func(ctx context.Context, started *event.CommandStartedEvent) {
			if started.CommandName != "appendOplogNote" {
				return
			}

			appendNumber := appendStartedCalls.Add(1)
			if appendNumber != 1 {
				return
			}

			deleteCalls.Add(1)
			result, deleteErr := collection.DeleteOne(ctx, bson.D{{"_id", "seed"}})
			deleteDone <- monitoredWriteOutcome[*mongo.DeleteResult]{result: result, err: deleteErr}
		},
		Succeeded: func(ctx context.Context, succeeded *event.CommandSucceededEvent) {
			if succeeded.CommandName != "appendOplogNote" {
				return
			}

			appendNumber := appendSucceededCalls.Add(1)
			if appendNumber != 2 {
				return
			}

			insertCalls.Add(1)
			result, insertErr := collection.InsertOne(ctx, bson.D{{"_id", "overtakes-pending-tick"}})
			insertDone <- monitoredWriteOutcome[*mongo.InsertOneResult]{result: result, err: insertErr}
		},
	}

	sourceClient, err := mongo.Connect(
		options.Client().
			ApplyURI(mongoURI).
			SetServerSelectionTimeout(30 * time.Second).
			SetMonitor(commandMonitor),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		require.NoError(t, sourceClient.Disconnect(cleanupCtx))
	})
	require.NoError(t, sourceClient.Ping(setupCtx, nil))

	replOptions := &Options{ChangeStreamBatchSize: 1}
	replOptions.applyDefaults()
	r := &Repl{source: sourceClient, options: replOptions}

	watchCtx, cancelWatch := context.WithTimeout(t.Context(), 20*time.Second)
	changeEvents := make(chan *ChangeEvent)
	watchErr := make(chan error, 1)
	go func() {
		watchErr <- r.watchChangeEvents(
			watchCtx,
			options.ChangeStream().SetStartAtOperationTime(&startAt),
			changeEvents,
		)
	}()
	t.Cleanup(func() {
		cancelWatch()
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		for {
			select {
			case <-changeEvents:
			case watchError := <-watchErr:
				require.True(t, watchError == nil || errors.Is(watchError, context.Canceled), watchError)

				return
			case <-cleanupCtx.Done():
				require.FailNow(t, "watchChangeEvents did not stop after cancellation", cleanupCtx.Err().Error())
			}
		}
	})

	// When
	events := make([]*ChangeEvent, 0, 4)
	for len(events) < cap(events) {
		select {
		case change := <-changeEvents:
			events = append(events, change)
		case <-watchCtx.Done():
			require.FailNow(t, "watchChangeEvents did not emit the expected event sequence", watchCtx.Err().Error())
		}
	}

	// Then
	var deleteOutcome monitoredWriteOutcome[*mongo.DeleteResult]
	select {
	case deleteOutcome = <-deleteDone:
	case <-watchCtx.Done():
		require.FailNow(t, "first appendOplogNote callback did not delete the seeded document", watchCtx.Err().Error())
	}
	require.NoError(t, deleteOutcome.err)
	require.NotNil(t, deleteOutcome.result)
	require.True(t, deleteOutcome.result.Acknowledged)
	require.EqualValues(t, 1, deleteOutcome.result.DeletedCount)

	var insertOutcome monitoredWriteOutcome[*mongo.InsertOneResult]
	select {
	case insertOutcome = <-insertDone:
	case <-watchCtx.Done():
		require.FailNow(t, "second appendOplogNote callback did not insert the overtaking document", watchCtx.Err().Error())
	}
	require.NoError(t, insertOutcome.err)
	require.NotNil(t, insertOutcome.result)
	require.True(t, insertOutcome.result.Acknowledged)

	require.EqualValues(t, 1, deleteCalls.Load())
	require.EqualValues(t, 1, insertCalls.Load())
	require.Equal(t, Delete, events[0].OperationType)
	require.Equal(t, OperationType(advanceTimePseudoEvent), events[1].OperationType)
	require.Equal(t, Insert, events[2].OperationType)
	require.Equal(t, OperationType(advanceTimePseudoEvent), events[3].OperationType)
	for i := 1; i < len(events); i++ {
		require.Falsef(
			t,
			events[i-1].ClusterTime.After(events[i].ClusterTime),
			"event %d timestamp %v was followed by older event %d timestamp %v",
			i-1,
			events[i-1].ClusterTime,
			i,
			events[i].ClusterTime,
		)
	}
}
