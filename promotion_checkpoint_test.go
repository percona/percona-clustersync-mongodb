package main //nolint:testpackage // Exercise the lifecycle controller and its checkpoint writer together.

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/drivertest"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/xoptions"

	"github.com/percona/percona-clustersync-mongodb/config"
	"github.com/percona/percona-clustersync-mongodb/ha"
	"github.com/percona/percona-clustersync-mongodb/mdb"
	"github.com/percona/percona-clustersync-mongodb/pcsm"
)

// commandLog records the writes the controller issues, in command order: the
// terms of its checkpoint claims and of its checkpoint saves, and the lease
// updates.
type commandLog struct {
	mu          sync.Mutex
	claims      []int64
	checkpoints []int64
	leaseWrites []string
}

func (l *commandLog) monitor() *event.CommandMonitor {
	return &event.CommandMonitor{
		Started: func(_ context.Context, e *event.CommandStartedEvent) {
			if e.CommandName != "findAndModify" {
				return
			}

			l.mu.Lock()
			defer l.mu.Unlock()

			switch e.Command.Lookup("findAndModify").StringValue() {
			case config.RecoveryCollection:
				term, _ := e.Command.Lookup("update", "$set", "term").AsInt64OK()
				if e.Command.Lookup("update", "$set", "data").IsZero() {
					l.claims = append(l.claims, term)
				} else {
					l.checkpoints = append(l.checkpoints, term)
				}
			case config.LeaseCollection:
				l.leaseWrites = append(l.leaseWrites, e.Command.String())
			}
		},
	}
}

func (l *commandLog) claimTerms() []int64 {
	l.mu.Lock()
	defer l.mu.Unlock()

	return append([]int64(nil), l.claims...)
}

func (l *commandLog) checkpointTerms() []int64 {
	l.mu.Lock()
	defer l.mu.Unlock()

	return append([]int64(nil), l.checkpoints...)
}

func (l *commandLog) leases() []string {
	l.mu.Lock()
	defer l.mu.Unlock()

	return append([]string(nil), l.leaseWrites...)
}

func failedRecord(t *testing.T, term int64) bson.D {
	t.Helper()

	data, err := bson.Marshal(bson.D{{"state", pcsm.StateFailed}})
	require.NoError(t, err)

	return bson.D{{"_id", recoveryID}, {"term", term}, {"data", bson.Raw(data)}}
}

// Mocked replies of the checkpoint document's commands.
func claimed(record bson.D) bson.D { return bson.D{{"ok", 1}, {"value", record}} }
func noDocument() bson.D           { return bson.D{{"ok", 1}, {"value", nil}} }
func inserted() bson.D             { return bson.D{{"ok", 1}, {"n", 1}} }

func duplicateKey() bson.D {
	return bson.D{{"ok", 1}, {"n", 0}, {"writeErrors", bson.A{
		bson.D{{"index", 0}, {"code", 11000}, {"errmsg", "E11000 duplicate key error"}},
	}}}
}

// holdFirstClaim wraps monitor so the first checkpoint claim blocks before it
// is sent until release is called: a test can capture the tenure that issued
// it before that tenure can end on its outcome.
func holdFirstClaim(t *testing.T, monitor *event.CommandMonitor) (*event.CommandMonitor, func()) {
	t.Helper()

	var first atomic.Bool
	released := make(chan struct{})
	release := sync.OnceFunc(func() { close(released) })
	t.Cleanup(release)

	return &event.CommandMonitor{
		Started: func(ctx context.Context, e *event.CommandStartedEvent) {
			if monitor != nil && monitor.Started != nil {
				monitor.Started(ctx, e)
			}

			if e.CommandName == "findAndModify" &&
				e.Command.Lookup("findAndModify").StringValue() == config.RecoveryCollection &&
				first.CompareAndSwap(false, true) {
				select {
				case <-released:
				case <-ctx.Done():
				case <-t.Context().Done():
				case <-time.After(10 * time.Second):
				}
			}
		},
	}, release
}

func mockedTarget(t *testing.T, monitor *event.CommandMonitor, responses ...bson.D) *mongo.Client {
	t.Helper()

	opts := options.Client().SetMonitor(monitor)
	require.NoError(t, xoptions.SetInternalClientOptions(opts, "deployment", drivertest.NewMockDeployment(responses...)))
	target, err := mongo.Connect(opts)
	require.NoError(t, err)

	return target
}

func currentTenure(s *server) *tenure {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.tenure
}

// TestOwnershipDrivesTenures pins the controller against the ownership
// levels the lease loop reports: ownership opens a tenure that claims the
// checkpoint at its term, restores it and checkpoints under that term; the
// same ownership again is a renewal; ownership at another term ends the
// tenure, and the next report of it opens one at that term; and a lost lease
// ends the tenure.
func TestOwnershipDrivesTenures(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		var log commandLog
		record := failedRecord(t, 2)
		// Responses in command order: the claim and first checkpoint of the
		// tenure at term 3, then those of the tenure at term 4.
		target := mockedTarget(t, log.monitor(), claimed(record), claimed(record), claimed(record), claimed(record))
		t.Cleanup(func() { require.NoError(t, target.Disconnect(t.Context())) })

		s := &server{
			cfg:           &config.Config{RecoveryCheckpointInterval: time.Second},
			targetCluster: target,
			membership:    &ha.Membership{},
		}

		// When: the lease is held at term 3.
		s.observeOwnership(ctx, ha.Ownership{Held: true, Term: 3})
		synctest.Wait()

		// Then: a tenure at term 3 claimed the checkpoint, restored it, and is
		// open for requests and advertised.
		first := currentTenure(s)
		require.NotNil(t, first)
		require.Equal(t, ha.Term(3), first.term)
		require.True(t, first.isReady(), "the tenure did not open for requests")
		require.Equal(t, []int64{3}, log.claimTerms())
		require.Equal(t, pcsm.State(pcsm.StateFailed), first.pipeline.Status(ctx).State, "the checkpoint was not restored")
		role, term := s.membership.CurrentRole()
		require.Equal(t, ha.RoleActive, role)
		require.Equal(t, ha.Term(3), term)
		require.Equal(t, ha.Term(4), s.minTerm, "the next tenure must open above this one's term")

		// And: the same ownership again is a renewal.
		s.observeOwnership(ctx, ha.Ownership{Held: true, Term: 3})
		require.Same(t, first, currentTenure(s))

		// And: its writer checkpoints under term 3.
		time.Sleep(time.Second)
		synctest.Wait()
		require.Equal(t, []int64{3}, log.checkpointTerms())

		// When: the lease is held at term 4 while the tenure at 3 is open.
		s.observeOwnership(ctx, ha.Ownership{Held: true, Term: 4})

		// Then: the tenure at 3 is over, and the report that ended it opened
		// nothing: it joined the tenure's work and may be stale by now.
		require.ErrorIs(t, first.ctx.Err(), context.Canceled)
		require.Nil(t, currentTenure(s), "a report that ended a tenure must not open one")
		role, term = s.membership.CurrentRole()
		require.Equal(t, ha.RoleStandby, role)
		require.Equal(t, ha.Term(4), term)

		// When: the next report still holds the lease at term 4.
		s.observeOwnership(ctx, ha.Ownership{Held: true, Term: 4})
		synctest.Wait()

		// Then: a tenure at 4 claimed the checkpoint and is open.
		second := currentTenure(s)
		require.NotNil(t, second)
		require.NotSame(t, first, second)
		require.Equal(t, ha.Term(4), second.term)
		require.True(t, second.isReady())
		require.Equal(t, []int64{3, 4}, log.claimTerms())
		role, term = s.membership.CurrentRole()
		require.Equal(t, ha.RoleActive, role)
		require.Equal(t, ha.Term(4), term)

		time.Sleep(time.Second)
		synctest.Wait()
		require.Equal(t, []int64{3, 4}, log.checkpointTerms(), "the old writer must not outlive its tenure")

		// When: the lease is lost.
		s.observeOwnership(ctx, ha.Ownership{})

		// Then: no tenure, STANDBY at the last term held.
		require.Nil(t, currentTenure(s))
		require.ErrorIs(t, second.ctx.Err(), context.Canceled)
		role, term = s.membership.CurrentRole()
		require.Equal(t, ha.RoleStandby, role)
		require.Equal(t, ha.Term(4), term)
	})
}

// TestFencedClaimEndsTenure pins a tenure whose claim finds the checkpoint
// owned by a newer term: it ends without restoring anything, and the
// instance stays STANDBY while the lease it still holds is advanced above the
// tenure's term, by an owner-only write, for the tenure that follows. The
// driver is mocked and the clock is virtual, so no tick is waited for.
func TestFencedClaimEndsTenure(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		const instanceID = "pcsm-stale"

		var log commandLog
		lease := bson.D{
			{"_id", ha.LeaseID},
			{"group", "g"},
			{"instanceId", instanceID},
			{"term", int64(2)},
			{"electionDate", time.Now()},
			{"expiresAt", time.Now().Add(config.LeaseTTL)},
		}
		// Responses in command order: join, leave, then the claim of the
		// tenure at term 1: the gated update matches nothing, the bootstrap
		// insert collides with the existing document, and the update matches
		// nothing again; then the lease term advance. The claim is held until
		// the tenure that issues it is captured.
		monitor, release := holdFirstClaim(t, log.monitor())
		target := mockedTarget(t, monitor,
			bson.D{{"ok", 1}, {"n", 1}},
			bson.D{{"ok", 1}, {"n", 1}},
			noDocument(),
			duplicateKey(),
			noDocument(),
			bson.D{{"ok", 1}, {"value", lease}},
		)
		t.Cleanup(func() { require.NoError(t, target.Disconnect(t.Context())) })

		membership, err := ha.JoinMembership(ctx, target, ha.MembershipOptions{InstanceID: instanceID, Group: "g"})
		require.NoError(t, err)
		// The heartbeat loop would consume mocked responses on its own clock.
		require.NoError(t, membership.Stop(ctx))

		s := &server{
			cfg:           &config.Config{RecoveryCheckpointInterval: time.Second},
			targetCluster: target,
			membership:    membership,
		}

		// When: the lease is held at term 1 while the checkpoint is owned by a
		// newer term.
		s.observeOwnership(ctx, ha.Ownership{Held: true, Term: 1})
		stale := currentTenure(s)
		require.NotNil(t, stale)
		release()
		synctest.Wait()

		// Then: the tenure ended without restoring, the role is STANDBY, and
		// the next tenure must open above term 1.
		require.Nil(t, currentTenure(s), "a tenure fenced by the checkpoint must end")
		require.ErrorIs(t, stale.ctx.Err(), context.Canceled)
		require.Equal(t, pcsm.State(pcsm.StateIdle), stale.pipeline.Status(ctx).State)
		require.Equal(t, []int64{1, 1}, log.claimTerms(), "the claim decides once more after the collision")
		role, _ := membership.CurrentRole()
		require.Equal(t, ha.RoleStandby, role)
		require.Equal(t, ha.Term(2), s.minTerm)
		require.Empty(t, log.leases())

		// When: the next attempt still reports the lease held at term 1.
		s.observeOwnership(ctx, ha.Ownership{Held: true, Term: 1})

		// Then: the lease term is advanced, by an owner-only write, and
		// nothing opens.
		require.Nil(t, currentTenure(s))
		advances := log.leases()
		require.Len(t, advances, 1, "exactly one lease write advances the term")
		require.Contains(t, advances[0], `"$max"`)
		require.Contains(t, advances[0], `"instanceId": "`+instanceID+`"`)
		role, _ = membership.CurrentRole()
		require.Equal(t, ha.RoleStandby, role)
	})
}

// TestUnsettledCheckpointWriteEndsTenure pins that a checkpoint write whose
// outcome the server did not settle ends the tenure: its writer never issues
// another write under that term, and the next tenure must open above it.
func TestUnsettledCheckpointWriteEndsTenure(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		var log commandLog
		record := failedRecord(t, 1)
		// Responses in command order: the claim of the tenure at term 2, then
		// its first checkpoint, which the server fails without a verdict.
		target := mockedTarget(t, log.monitor(), claimed(record),
			bson.D{{"ok", 0}, {"code", 1}, {"errmsg", "InternalError"}})
		t.Cleanup(func() { require.NoError(t, target.Disconnect(t.Context())) })

		s := &server{
			cfg:           &config.Config{RecoveryCheckpointInterval: time.Second},
			targetCluster: target,
			membership:    &ha.Membership{},
		}

		// Given: a tenure at term 2 is open.
		s.observeOwnership(ctx, ha.Ownership{Held: true, Term: 2})
		synctest.Wait()
		tn := currentTenure(s)
		require.NotNil(t, tn)
		require.True(t, tn.isReady())

		// When: its first checkpoint write fails without a settled outcome.
		time.Sleep(time.Second)
		synctest.Wait()

		// Then: the tenure is over and the instance is STANDBY.
		require.Equal(t, []int64{2}, log.checkpointTerms())
		require.Nil(t, currentTenure(s), "an unsettled checkpoint write must end the tenure")
		require.ErrorIs(t, tn.ctx.Err(), context.Canceled)
		role, _ := s.membership.CurrentRole()
		require.Equal(t, ha.RoleStandby, role)
		require.Equal(t, ha.Term(3), s.minTerm)

		// And: no further write of term 2 is ever issued.
		time.Sleep(time.Second)
		synctest.Wait()
		require.Equal(t, []int64{2}, log.checkpointTerms())
	})
}

// TestLostCheckpointOfEndedTenureIsIgnored pins that a lost checkpoint
// reported by a tenure that is no longer current ends nothing.
func TestLostCheckpointOfEndedTenureIsIgnored(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		var log commandLog
		// Responses in command order: the claims of two tenures, each
		// bootstrapping an absent document.
		target := mockedTarget(t, log.monitor(), noDocument(), inserted(), noDocument(), inserted())
		t.Cleanup(func() { require.NoError(t, target.Disconnect(t.Context())) })

		s := &server{
			cfg:           &config.Config{RecoveryCheckpointInterval: time.Hour},
			targetCluster: target,
			membership:    &ha.Membership{},
		}

		// Given: a tenure at term 3 ended and one at term 4 is current.
		s.observeOwnership(ctx, ha.Ownership{Held: true, Term: 3})
		synctest.Wait()
		ended := currentTenure(s)
		s.observeOwnership(ctx, ha.Ownership{})
		s.observeOwnership(ctx, ha.Ownership{Held: true, Term: 4})
		synctest.Wait()
		current := currentTenure(s)
		require.NotSame(t, ended, current)

		// When: a lost checkpoint of the ended tenure is reported late.
		s.onCheckpointLost(ctx, ended, errCheckpointFenced)

		// Then: the current tenure stands.
		require.Same(t, current, currentTenure(s))
		require.NoError(t, current.ctx.Err())
		require.Equal(t, ha.Term(5), s.minTerm)
	})
}

// TestRunCheckpointingSavesOnSignal pins that a state change reaches the
// target through the tenure's single writer as soon as it is signaled, not at
// the next tick.
func TestRunCheckpointingSavesOnSignal(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		data, err := bson.Marshal(bson.D{{"state", pcsm.StateFailed}})
		require.NoError(t, err)
		record := bson.D{{"_id", recoveryID}, {"term", int64(1)}, {"data", bson.Raw(data)}}
		deployment := drivertest.NewMockDeployment(claimed(record))
		saves := make(chan struct{}, 1)
		opts := options.Client().SetMonitor(&event.CommandMonitor{
			Succeeded: func(_ context.Context, e *event.CommandSucceededEvent) {
				if e.CommandName == "findAndModify" {
					saves <- struct{}{}
				}
			},
		})
		require.NoError(t, xoptions.SetInternalClientOptions(opts, "deployment", deployment))
		target, err := mongo.Connect(opts)
		require.NoError(t, err)

		pipeline := pcsm.New(ctx, nil, target, mdb.ServerVersion{}, false, false)
		require.NoError(t, pipeline.Recover(ctx, data))

		saveNow := make(chan struct{}, 1)
		done := make(chan struct{})
		go func() {
			defer close(done)
			_ = RunCheckpointing(ctx, target, pipeline, 1, "active", time.Hour, saveNow)
		}()
		t.Cleanup(func() {
			cancel()
			<-done
			require.NoError(t, target.Disconnect(t.Context()))
		})

		// When: a state change is signaled long before the next tick.
		saveNow <- struct{}{}
		synctest.Wait()

		// Then: the checkpoint has already been written.
		select {
		case <-saves:
		default:
			require.FailNow(t, "a signaled save did not reach the target before the next tick")
		}
	})
}
