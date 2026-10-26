package main //nolint:testpackage // Exercise the lifecycle controller and its checkpoint writer together.

import (
	"context"
	"sync"
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

// commandLog records the writes the controller issues: checkpoint terms and
// lease updates, in command order.
type commandLog struct {
	mu          sync.Mutex
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
				l.checkpoints = append(l.checkpoints, term)
			case config.LeaseCollection:
				l.leaseWrites = append(l.leaseWrites, e.Command.String())
			}
		},
	}
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

func recoveryCursor(records ...bson.D) bson.D {
	batch := make(bson.A, 0, len(records))
	for _, r := range records {
		batch = append(batch, r)
	}

	return bson.D{{"ok", 1}, {"cursor", bson.D{
		{"id", int64(0)},
		{"ns", config.PCSMDatabase + "." + config.RecoveryCollection},
		{"firstBatch", batch},
	}}}
}

func failedRecord(t *testing.T, term int64) bson.D {
	t.Helper()

	data, err := bson.Marshal(bson.D{{"state", pcsm.StateFailed}})
	require.NoError(t, err)

	return bson.D{{"_id", recoveryID}, {"term", term}, {"seq", int64(1)}, {"data", bson.Raw(data)}}
}

func mockedTarget(t *testing.T, log *commandLog, responses ...bson.D) *mongo.Client {
	t.Helper()

	opts := options.Client().SetMonitor(log.monitor())
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
// levels the lease loop reports: ownership opens a tenure that restores the
// checkpoint and checkpoints under its term; the same ownership again is a
// renewal; ownership at another term ends the tenure, and the next report of
// it opens one at that term; and a lost lease ends the tenure.
func TestOwnershipDrivesTenures(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		var log commandLog
		record := failedRecord(t, 2)
		saved := bson.D{{"ok", 1}, {"value", record}}
		// Responses in command order: the restore and first checkpoint of the
		// tenure at term 3, then those of the tenure at term 4.
		target := mockedTarget(t, &log, recoveryCursor(record), saved, recoveryCursor(record), saved)
		t.Cleanup(func() { require.NoError(t, target.Disconnect(t.Context())) })

		s := &server{
			cfg:           &config.Config{RecoveryCheckpointInterval: time.Second},
			targetCluster: target,
			membership:    &ha.Membership{},
		}

		// When: the lease is held at term 3.
		s.observeOwnership(ctx, ha.Ownership{Held: true, Term: 3})
		synctest.Wait()

		// Then: a tenure at term 3 is open for requests, restored, and advertised.
		first := currentTenure(s)
		require.NotNil(t, first)
		require.Equal(t, ha.Term(3), first.term)
		require.True(t, first.isReady(), "the tenure did not open for requests")
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

		// Then: a tenure at 4 is open.
		second := currentTenure(s)
		require.NotNil(t, second)
		require.NotSame(t, first, second)
		require.Equal(t, ha.Term(4), second.term)
		require.True(t, second.isReady())
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

// TestFencedCheckpointEndsTenureAndRepairsLease pins the whole path for a
// lease that fell behind the checkpoint's term (the lease document was reset
// while recovery data existed): the tenure is fenced by the stored term and
// ends; ownership reported at the stale term is advanced past the fencing
// term through the owner-only AdvanceTerm, opening nothing; ownership at the
// advanced term opens a tenure that restores the checkpoint and checkpoints
// under a term the checkpoint accepts. The driver is mocked and the clock is
// virtual, so no tick is waited for.
func TestFencedCheckpointEndsTenureAndRepairsLease(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		const instanceID = "pcsm-stale"

		var log commandLog
		fenced := failedRecord(t, 5)
		lease := bson.D{
			{"_id", ha.LeaseID},
			{"group", "g"},
			{"instanceId", instanceID},
			{"term", int64(6)},
			{"electionDate", time.Now()},
			{"expiresAt", time.Now().Add(config.LeaseTTL)},
		}
		// Responses in command order: join, leave, the restore of the tenure
		// at term 1 (fenced by the stored term), the lease term advance, the
		// restore of the tenure at term 6 and its first checkpoint.
		target := mockedTarget(t, &log,
			bson.D{{"ok", 1}, {"n", 1}},
			bson.D{{"ok", 1}, {"n", 1}},
			recoveryCursor(fenced),
			bson.D{{"ok", 1}, {"value", lease}},
			recoveryCursor(fenced),
			bson.D{{"ok", 1}, {"value", fenced}},
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

		// When: the lease is held at term 1 while the checkpoint is owned by term 5.
		s.observeOwnership(ctx, ha.Ownership{Held: true, Term: 1})
		stale := currentTenure(s)
		require.NotNil(t, stale)
		synctest.Wait()

		// Then: the tenure ended without restoring, the role is STANDBY, and no
		// tenure may open below term 6.
		require.Nil(t, currentTenure(s), "a tenure fenced by the checkpoint must end")
		require.ErrorIs(t, stale.ctx.Err(), context.Canceled)
		require.Equal(t, pcsm.State(pcsm.StateIdle), stale.pipeline.Status(ctx).State)
		role, _ := membership.CurrentRole()
		require.Equal(t, ha.RoleStandby, role)
		require.Equal(t, ha.Term(6), s.minTerm)
		require.Empty(t, log.leases())

		// When: the next attempt still reports the lease held at the stale term.
		s.observeOwnership(ctx, ha.Ownership{Held: true, Term: 1})

		// Then: the lease term is advanced past the fencing term, by an
		// owner-only write, and nothing opens.
		require.Nil(t, currentTenure(s))
		advances := log.leases()
		require.Len(t, advances, 1, "exactly one lease write advances the term")
		require.Contains(t, advances[0], `"$max"`)
		require.Contains(t, advances[0], `"instanceId": "`+instanceID+`"`)
		role, _ = membership.CurrentRole()
		require.Equal(t, ha.RoleStandby, role)

		// When: the attempt after that reports the advanced term.
		s.observeOwnership(ctx, ha.Ownership{Held: true, Term: 6})
		synctest.Wait()

		// Then: a tenure at term 6 restored the checkpoint and checkpoints
		// under a term the checkpoint accepts.
		repaired := currentTenure(s)
		require.NotNil(t, repaired)
		require.Equal(t, ha.Term(6), repaired.term)
		require.True(t, repaired.isReady())
		require.Equal(t, pcsm.State(pcsm.StateFailed), repaired.pipeline.Status(ctx).State)
		role, term := membership.CurrentRole()
		require.Equal(t, ha.RoleActive, role)
		require.Equal(t, ha.Term(6), term)

		time.Sleep(time.Second)
		synctest.Wait()
		require.Equal(t, []int64{6}, log.checkpointTerms())
	})
}

// TestLateFenceOfEndedTenureIsIgnored pins that a fenced write reported by
// a tenure that is no longer current ends nothing, while the term that fenced
// it still raises the floor for the next tenure.
func TestLateFenceOfEndedTenureIsIgnored(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		var log commandLog
		target := mockedTarget(t, &log, recoveryCursor(), recoveryCursor())
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

		// When: a fenced write of the ended tenure arrives late.
		s.onFenced(ctx, ended, 9)

		// Then: the current tenure stands, and the floor moved past the fence.
		require.Same(t, current, currentTenure(s))
		require.NoError(t, current.ctx.Err())
		require.Equal(t, ha.Term(10), s.minTerm)
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
		deployment := drivertest.NewMockDeployment(bson.D{{"ok", 1}, {"value", record}})
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
			RunCheckpointing(ctx, target, pipeline, 1, "active", time.Hour, saveNow, nil)
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
