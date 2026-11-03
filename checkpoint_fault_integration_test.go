//go:build integration

package main //nolint:testpackage // Exercise checkpoint faults through the driver and tenure controller.

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
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

	"github.com/percona/percona-clustersync-mongodb/config"
	"github.com/percona/percona-clustersync-mongodb/ha"
	"github.com/percona/percona-clustersync-mongodb/pcsm"
	"github.com/percona/percona-clustersync-mongodb/pcsm/clone"
)

// checkpointFaultMongo owns a separate replica set: retryable writes require
// sessions, and failCommand must never affect another test's MongoDB.
func checkpointFaultMongo(t *testing.T) (string, *mongo.Client) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), mongodStartupTimeout)
	defer cancel()
	version := os.Getenv("MONGO_VERSION")
	if version == "" {
		version = "8.0.29-13"
	}

	request := testcontainers.ContainerRequest{
		Image:        "percona/percona-server-mongodb:" + version,
		ExposedPorts: []string{"27017/tcp"},
		Cmd: []string{
			"mongod", "--quiet", "--bind_ip_all", "--replSet", "rs0",
			"--wiredTigerCacheSizeGB", "0.5", "--setParameter", "enableTestCommands=1",
		},
		WaitingFor: wait.ForLog("Waiting for connections").WithStartupTimeout(mongodStartupTimeout),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: request,
		Started:          true,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		require.NoError(t, container.Terminate(cleanup))
	})
	host, err := container.Host(ctx)
	require.NoError(t, err)
	port, err := container.MappedPort(ctx, "27017/tcp")
	require.NoError(t, err)
	uri := "mongodb://" + net.JoinHostPort(host, port.Port()) + "/?directConnection=true"

	primary := make(chan struct{})
	notifyPrimary := sync.OnceFunc(func() { close(primary) })
	client, err := mongo.Connect(options.Client().ApplyURI(uri).SetServerMonitor(&event.ServerMonitor{
		ServerDescriptionChanged: func(e *event.ServerDescriptionChangedEvent) {
			if e.NewDescription.Kind == "RSPrimary" {
				notifyPrimary()
			}
		},
	}))
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		require.NoError(t, client.Disconnect(cleanup))
	})
	exitCode, _, err := container.Exec(ctx, []string{
		"mongosh", "--quiet", "--eval",
		"rs.initiate({_id:'rs0', members:[{_id:0, host:'localhost:27017'}]})",
	})
	require.NoError(t, err)
	require.Zero(t, exitCode)
	awaitCheckpointSignal(t, ctx, primary)

	return uri, client
}

func awaitCheckpointSignal(t *testing.T, ctx context.Context, signal <-chan struct{}) {
	t.Helper()

	select {
	case <-signal:
	case <-ctx.Done():
		require.FailNow(t, "checkpoint signal did not arrive", "%v", ctx.Err())
	}
}

func checkpointFailCommand(t *testing.T, ctx context.Context, target *mongo.Client, data bson.D) {
	t.Helper()

	require.NoError(t, target.Database("admin").RunCommand(ctx, bson.D{
		{"configureFailPoint", "failCommand"},
		{"mode", bson.D{{"times", 1}}},
		{"data", append(bson.D{{"failCommands", bson.A{"findAndModify"}}}, data...)},
	}).Err())
}

func TestCheckpointDriverRetryKeepsTenure(t *testing.T) {
	t.Parallel()

	uri, target := checkpointFaultMongo(t)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	_, err := recoveryColl(target).InsertOne(ctx, failedRecord(t, 1))
	require.NoError(t, err)

	var mu sync.Mutex
	var writes []bson.Raw
	saved := make(chan struct{}, 4)
	monitor := &event.CommandMonitor{
		Started: func(_ context.Context, e *event.CommandStartedEvent) {
			if e.CommandName == "findAndModify" && !e.Command.Lookup("update", "$set", "data").IsZero() {
				mu.Lock()
				writes = append(writes, append(bson.Raw(nil), e.Command...))
				mu.Unlock()
			}
		},
		Succeeded: func(_ context.Context, e *event.CommandSucceededEvent) {
			if e.CommandName == "findAndModify" {
				select {
				case saved <- struct{}{}:
				default:
				}
			}
		},
	}
	client, err := mongo.Connect(options.Client().ApplyURI(uri).SetRetryWrites(true).SetMonitor(monitor))
	require.NoError(t, err)
	defer func() { require.NoError(t, client.Disconnect(ctx)) }()
	s := &server{
		cfg:           &config.Config{RecoveryCheckpointInterval: time.Hour},
		sourceCluster: client,
		targetCluster: client,
		membership:    &ha.Membership{},
	}
	defer closeTenure(ctx, s)
	s.observeOwnership(ctx, ha.Ownership{Held: true, Term: 2})
	tn := awaitTenure(t, ctx, s)
	awaitCheckpointSignal(t, ctx, saved) // Claim succeeded.

	// When: MongoDB rejects one attempt with a retryable error.
	checkpointFailCommand(t, ctx, target, bson.D{
		{"errorCode", 112}, {"errorLabels", bson.A{"RetryableWriteError"}},
	})
	// Recover emits a state-change save when an interrupted clone becomes failed.
	data, err := bson.Marshal(bson.D{
		{"state", pcsm.StateRunning}, {"clone", clone.Checkpoint{StartTime: time.Unix(1, 0)}},
	})
	require.NoError(t, err)
	require.NoError(t, tn.pipeline.Recover(ctx, data))
	awaitCheckpointSignal(t, ctx, saved)
	require.NoError(t, tn.pipeline.Recover(ctx, data))
	awaitCheckpointSignal(t, ctx, saved)

	// Then: two wire attempts are one logical retryable write, and the
	// writer saves another snapshot and the tenure still admits requests.
	mu.Lock()
	attempts := append([]bson.Raw(nil), writes...)
	mu.Unlock()
	require.Len(t, attempts, 3)
	require.Equal(t, attempts[0].Lookup("lsid"), attempts[1].Lookup("lsid"))
	require.Equal(t, attempts[0].Lookup("txnNumber"), attempts[1].Lookup("txnNumber"))
	require.False(t, attempts[0].Lookup("txnNumber").IsZero(), "the driver must enable retryable writes")
	require.NotEqual(t, attempts[1].Lookup("txnNumber"), attempts[2].Lookup("txnNumber"))
	require.Equal(t, []int64{2, 2}, []int64{
		attempts[0].Lookup("update", "$set", "term").Int64(),
		attempts[1].Lookup("update", "$set", "term").Int64(),
	})
	require.Same(t, tn, currentTenure(s))
	require.NoError(t, tn.ctx.Err())
	response := httptest.NewRecorder()
	_, pipeline, admitted := s.admit(ctx, response)
	require.True(t, admitted)
	require.Same(t, tn.pipeline, pipeline)
	require.Equal(t, int64(2), readCheckpoint(t, ctx, target).Term)
}

func TestCheckpointOperationTimeoutEndsLiveTenure(t *testing.T) {
	t.Parallel()

	uri, target := checkpointFaultMongo(t)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	_, err := recoveryColl(target).InsertOne(ctx, failedRecord(t, 1))
	require.NoError(t, err)

	failed := make(chan struct{})
	var failure error
	var tenureLive bool
	var tn *tenure
	var writes atomic.Int64
	var firstFailure sync.Once
	client, err := mongo.Connect(options.Client().ApplyURI(uri).SetTimeout(time.Second).
		SetMonitor(&event.CommandMonitor{
			Started: func(_ context.Context, e *event.CommandStartedEvent) {
				if e.CommandName == "findAndModify" && !e.Command.Lookup("update", "$set", "data").IsZero() {
					writes.Add(1)
				}
			},
			Failed: func(_ context.Context, e *event.CommandFailedEvent) {
				if e.CommandName == "findAndModify" {
					firstFailure.Do(func() {
						failure = e.Failure
						tenureLive = tn.ctx.Err() == nil
						close(failed)
					})
				}
			},
		}))
	require.NoError(t, err)
	defer func() { require.NoError(t, client.Disconnect(ctx)) }()
	s := &server{
		cfg:           &config.Config{RecoveryCheckpointInterval: time.Hour},
		sourceCluster: client,
		targetCluster: client,
		membership:    &ha.Membership{},
	}
	defer closeTenure(ctx, s)
	// A caller deadline overrides Client.Timeout, so the tenure must have
	// cancellation but no deadline. Only the test's waits use the bounded ctx.
	tenureCtx, cancelTenure := context.WithCancel(t.Context())
	defer cancelTenure()
	s.observeOwnership(tenureCtx, ha.Ownership{Held: true, Term: 2})
	tn = awaitTenure(t, ctx, s)

	// When: a real socket read exceeds the operation deadline, not the
	// tenure deadline. The server delay is solely for testing timeout behavior.
	checkpointFailCommand(t, ctx, target, bson.D{{"blockConnection", true}, {"blockTimeMS", 10000}})
	data, err := bson.Marshal(bson.D{
		{"state", pcsm.StateRunning}, {"clone", clone.Checkpoint{StartTime: time.Unix(1, 0)}},
	})
	require.NoError(t, err)
	require.NoError(t, tn.pipeline.Recover(ctx, data))
	awaitCheckpointSignal(t, ctx, failed)
	require.ErrorIs(t, failure, context.DeadlineExceeded)
	require.True(t, tenureLive, "the timeout must not be tenure cancellation")
	awaitCheckpointSignal(t, ctx, tn.ctx.Done())

	// Then: the writer ends the tenure without issuing another checkpoint.
	require.Nil(t, currentTenure(s))
	require.NoError(t, ctx.Err())
	role, _ := s.membership.CurrentRole()
	require.Equal(t, ha.RoleStandby, role)
	require.Equal(t, ha.Term(3), s.minTerm)
	require.Equal(t, int64(1), writes.Load())
}

// lostClaimDialer consumes the real server response for one monitored request
// before returning EOF to the driver. The claim has applied, but Restore never
// receives its snapshot. Other wire messages, including handshakes, pass through.
type lostClaimDialer struct {
	request atomic.Int64
	dropped atomic.Bool
}

func (d *lostClaimDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	conn, err := (&net.Dialer{}).DialContext(ctx, network, address)
	if err != nil {
		return nil, err //nolint:wrapcheck // Preserve the network error for driver classification.
	}

	return &lostClaimConn{Conn: conn, fault: d}, nil
}

type lostClaimConn struct {
	net.Conn

	fault   *lostClaimDialer
	pending []byte
}

func (c *lostClaimConn) Read(p []byte) (int, error) {
	if len(c.pending) == 0 {
		header := make([]byte, 16)
		_, err := io.ReadFull(c.Conn, header)
		if err != nil {
			return 0, err //nolint:wrapcheck // Preserve the socket failure.
		}
		message := make([]byte, binary.LittleEndian.Uint32(header[:4]))
		copy(message, header)
		_, err = io.ReadFull(c.Conn, message[16:])
		if err != nil {
			return 0, err //nolint:wrapcheck // Preserve the socket failure.
		}
		if int64(binary.LittleEndian.Uint32(header[8:12])) == c.fault.request.Load() &&
			c.fault.dropped.CompareAndSwap(false, true) {
			return 0, io.ErrUnexpectedEOF
		}
		c.pending = message
	}

	n := copy(p, c.pending)
	c.pending = c.pending[n:]

	return n, nil
}

func TestLostClaimResponseDoesNotRestoreAndHigherTermRecovers(t *testing.T) {
	t.Parallel()

	uri, target := checkpointFaultMongo(t)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	_, err := recoveryColl(target).InsertOne(ctx, failedRecord(t, 1))
	require.NoError(t, err)
	membership, err := ha.JoinMembership(ctx, target, ha.MembershipOptions{InstanceID: "lost-claim"})
	require.NoError(t, err)
	defer func() { require.NoError(t, membership.Stop(ctx)) }()

	var fault lostClaimDialer
	monitor, release := holdFirstClaim(t, &event.CommandMonitor{
		Started: func(_ context.Context, e *event.CommandStartedEvent) {
			if e.CommandName == "findAndModify" {
				fault.request.Store(e.RequestID)
			}
		},
	})
	client, err := mongo.Connect(options.Client().ApplyURI(uri).
		SetRetryWrites(false).SetDialer(&fault).SetMonitor(monitor))
	require.NoError(t, err)
	defer func() { require.NoError(t, client.Disconnect(ctx)) }()
	s := &server{
		cfg:           &config.Config{RecoveryCheckpointInterval: time.Hour},
		sourceCluster: client,
		targetCluster: client,
		membership:    membership,
	}
	defer closeTenure(ctx, s)
	defer release()

	// When: a claim applies but its response is lost after MongoDB sends it.
	s.observeOwnership(ctx, ha.Ownership{Held: true, Term: 2})
	lost := currentTenure(s)
	require.NotNil(t, lost)
	release()
	awaitCheckpointSignal(t, ctx, lost.ctx.Done())

	// Then: no snapshot was restored and no request can use this tenure.
	require.True(t, fault.dropped.Load())
	require.Nil(t, currentTenure(s))
	require.False(t, lost.isReady())
	require.Equal(t, pcsm.State(pcsm.StateIdle), lost.pipeline.Status(ctx).State)
	response := httptest.NewRecorder()
	_, _, admitted := s.admit(ctx, response)
	require.False(t, admitted)
	require.Equal(t, http.StatusConflict, response.Code)
	var body struct {
		OK    bool   `json:"ok"`
		Error string `json:"error"`
	}
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
	require.False(t, body.OK)
	require.Equal(t, "not_active", body.Error)
	cp := readCheckpoint(t, ctx, target)
	require.Equal(t, int64(2), cp.Term, "the lost response must follow an applied claim")
	require.Equal(t, "failed", cp.Data.Lookup("state").StringValue())

	// A higher term claims the same snapshot and opens a fresh pipeline.
	s.observeOwnership(ctx, ha.Ownership{Held: true, Term: 3})
	recovered := awaitTenure(t, ctx, s)
	require.NotSame(t, lost.pipeline, recovered.pipeline)
	require.Equal(t, pcsm.State(pcsm.StateFailed), recovered.pipeline.Status(ctx).State)
	_, pipeline, admitted := s.admit(ctx, httptest.NewRecorder())
	require.True(t, admitted)
	require.Same(t, recovered.pipeline, pipeline)
	require.Equal(t, int64(3), readCheckpoint(t, ctx, target).Term)
}
