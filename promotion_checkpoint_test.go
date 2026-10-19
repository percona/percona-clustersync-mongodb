package main //nolint:testpackage // Exercise server promotion and its checkpoint loop together.

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

func TestPromotionCheckpointUsesNewTerm(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		// Given: a coalesced demotion leaves the prior checkpoint loop alive.
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		data, err := bson.Marshal(bson.D{{"state", pcsm.StateFailed}})
		require.NoError(t, err)
		record := bson.D{{"_id", recoveryID}, {"term", int64(2)}, {"data", bson.Raw(data)}}
		deployment := drivertest.NewMockDeployment(
			bson.D{{"ok", 1}, {"cursor", bson.D{
				{"id", int64(0)},
				{"ns", config.PCSMDatabase + "." + config.RecoveryCollection},
				{"firstBatch", bson.A{record}},
			}}},
			bson.D{{"ok", 1}, {"value", record}},
			bson.D{{"ok", 1}, {"value", record}},
		)
		checkpoints := make(chan bson.Raw, 1)
		opts := options.Client().SetMonitor(&event.CommandMonitor{
			Started: func(_ context.Context, e *event.CommandStartedEvent) {
				if e.CommandName == "findAndModify" {
					checkpoints <- append(bson.Raw(nil), e.Command...)
				}
			},
		})
		require.NoError(t, xoptions.SetInternalClientOptions(opts, "deployment", deployment))
		target, err := mongo.Connect(opts)
		require.NoError(t, err)

		pipeline := pcsm.New(ctx, nil, target, mdb.ServerVersion{}, false, false)
		require.NoError(t, pipeline.Recover(ctx, data))
		membership := &ha.Membership{}
		membership.SetRole(ha.RoleActive, 3)
		oldCtx, oldCancel := context.WithCancel(ctx)
		s := &server{
			cfg:           &config.Config{RecoveryCheckpointInterval: time.Second},
			targetCluster: target,
			pcsm:          pipeline,
			membership:    membership,
			activeTerm:    1,
			epochCtx:      oldCtx, epochCancel: oldCancel,
		}
		oldDone := make(chan struct{})
		go func() {
			defer close(oldDone)
			RunCheckpointing(oldCtx, target, pipeline, 1, "old-active",
				time.Second, nil, func(int64) { s.onDemote(ctx, 1) })
		}()
		t.Cleanup(func() {
			cancel()
			synctest.Wait()
			<-oldDone
			require.NoError(t, target.Disconnect(t.Context()))
		})
		synctest.Wait()

		// When: the next observed role change is ACTIVE in a newer term.
		s.onPromote(ctx, 3)
		require.Equal(t, ha.Term(3), s.activeTerm, "restore must have succeeded")

		// Then: periodic checkpoints continue with the new ownership term.
		for range 2 {
			select {
			case command := <-checkpoints:
				term, ok := command.Lookup("update", "$set", "term").AsInt64OK()
				require.True(t, ok, "checkpoint update must stamp a term")
				require.Equal(t, int64(3), term)
			case <-time.After(2 * time.Second):
				t.Fatal("no periodic checkpoint after promotion")
			}
		}
	})
}

// TestFencedWriteRepairsStaleLeaseTerm pins the whole fence-driven recovery
// path for a lease that fell behind the checkpoint's term (the lease document
// was reset while recovery data existed): the epoch's writer is fenced by the
// stored term, onFenced records the step-down and advances the lease term past
// it, and the next lease tick is a real promotion at that term, which is what
// restores the checkpoint instead of resuming at the stale term and being
// fenced again. The driver is mocked and the clock is virtual, so no tick is
// waited for.
func TestFencedWriteRepairsStaleLeaseTerm(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		const instanceID = "pcsm-stale"

		data, err := bson.Marshal(bson.D{{"state", pcsm.StateFailed}})
		require.NoError(t, err)
		fenced := bson.D{{"_id", recoveryID}, {"term", int64(5)}, {"seq", int64(1)}, {"data", bson.Raw(data)}}
		lease := func(term int64) bson.D {
			return bson.D{
				{"_id", ha.LeaseID},
				{"group", "g"},
				{"instanceId", instanceID},
				{"term", term},
				{"electionDate", time.Now()},
				{"expiresAt", time.Now().Add(config.LeaseTTL)},
			}
		}
		noMatch := bson.D{{"ok", 1}, {"value", nil}}
		// Responses in command order: join, leave, the fenced write (ordered
		// update, bootstrap insert, retried update, classifying read), the
		// lease term advance, and the lease tick that renews at the new term.
		deployment := drivertest.NewMockDeployment(
			bson.D{{"ok", 1}, {"n", 1}},
			bson.D{{"ok", 1}, {"n", 1}},
			noMatch,
			bson.D{{"ok", 1}, {"n", 0}, {"writeErrors", bson.A{
				bson.D{{"index", 0}, {"code", 11000}, {"errmsg", "E11000 duplicate key error"}},
			}}},
			noMatch,
			bson.D{{"ok", 1}, {"cursor", bson.D{
				{"id", int64(0)},
				{"ns", config.PCSMDatabase + "." + config.RecoveryCollection},
				{"firstBatch", bson.A{fenced}},
			}}},
			bson.D{{"ok", 1}, {"value", lease(6)}},
			bson.D{{"ok", 1}, {"value", lease(6)}},
		)

		var (
			commandsMu  sync.Mutex
			leaseWrites []string
		)
		opts := options.Client().SetMonitor(&event.CommandMonitor{
			Started: func(_ context.Context, e *event.CommandStartedEvent) {
				if e.CommandName != "findAndModify" ||
					e.Command.Lookup("findAndModify").StringValue() != config.LeaseCollection {
					return
				}

				commandsMu.Lock()
				leaseWrites = append(leaseWrites, e.Command.String())
				commandsMu.Unlock()
			},
		})
		require.NoError(t, xoptions.SetInternalClientOptions(opts, "deployment", deployment))
		target, err := mongo.Connect(opts)
		require.NoError(t, err)

		// Given: an ACTIVE instance at term 1 whose checkpoint is owned by term 5.
		membership, err := ha.JoinMembership(ctx, target, ha.MembershipOptions{InstanceID: instanceID, Group: "g"})
		require.NoError(t, err)
		// The heartbeat loop would consume mocked responses on its own clock.
		require.NoError(t, membership.Stop(ctx))
		membership.SetRole(ha.RoleActive, 1)

		pipeline := pcsm.New(ctx, nil, target, mdb.ServerVersion{}, false, false)
		require.NoError(t, pipeline.Recover(ctx, data))

		epochCtx, epochCancel := context.WithCancel(ctx)
		defer epochCancel()
		s := &server{
			cfg:           &config.Config{RecoveryCheckpointInterval: time.Hour},
			targetCluster: target,
			pcsm:          pipeline,
			membership:    membership,
			activeTerm:    1,
			epochCtx:      epochCtx,
			epochCancel:   epochCancel,
		}
		saveNow := make(chan struct{}, 1)
		done := make(chan struct{})
		go func() {
			defer close(done)
			RunCheckpointing(epochCtx, target, pipeline, 1, instanceID, time.Hour, saveNow,
				func(storedTerm int64) { go s.onFenced(ctx, epochCtx, 1, ha.Term(storedTerm)) })
		}()
		t.Cleanup(func() {
			cancel()
			<-done
			require.NoError(t, target.Disconnect(t.Context()))
		})

		// When: the epoch's writer is fenced by the stored term.
		saveNow <- struct{}{}
		synctest.Wait()

		// Then: the writer stopped, the step-down is recorded at the stale term
		// and the epoch is over.
		select {
		case <-done:
		default:
			require.FailNow(t, "a fenced write did not stop the checkpoint writer")
		}
		role, term := membership.CurrentRole()
		require.Equal(t, ha.RoleStandby, role)
		require.Equal(t, ha.Term(1), term)
		s.mu.Lock()
		require.Nil(t, s.epochCancel, "a fenced write must end the epoch")
		s.mu.Unlock()

		// And: the lease this instance still owns was advanced past the term
		// that fenced it, by an owner-only write.
		commandsMu.Lock()
		advances := append([]string(nil), leaseWrites...)
		commandsMu.Unlock()
		require.Len(t, advances, 1, "exactly one lease write repairs the term")
		require.Contains(t, advances[0], `"$max"`)
		require.Contains(t, advances[0], `"instanceId": "`+instanceID+`"`)

		// And: the next lease tick is a real promotion at the advanced term,
		// not a same-term resume that would be fenced again.
		membership.FirstLeaseTick(ctx)
		select {
		case rc := <-membership.RoleChanges():
			require.Equal(t, ha.RoleChange{Role: ha.RoleActive, Term: 6}, rc)
		default:
			require.FailNow(t, "the renewal after a repaired term did not promote")
		}
	})
}

// TestRunCheckpointingSavesOnSignal pins that a state change reaches the
// target through the epoch's single writer as soon as it is signaled, not at
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

// TestPromotionJoinsPreviousCheckpointWriter pins that a promotion waits for
// the previous epoch's checkpoint writer to exit before this tenure saves
// anything, so a snapshot that writer captured cannot land over newer ones.
func TestPromotionJoinsPreviousCheckpointWriter(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		// No checkpoint is stored: the idle pipeline has nothing to restore.
		deployment := drivertest.NewMockDeployment(bson.D{{"ok", 1}, {"cursor", bson.D{
			{"id", int64(0)},
			{"ns", config.PCSMDatabase + "." + config.RecoveryCollection},
			{"firstBatch", bson.A{}},
		}}})
		opts := options.Client()
		require.NoError(t, xoptions.SetInternalClientOptions(opts, "deployment", deployment))
		target, err := mongo.Connect(opts)
		require.NoError(t, err)
		t.Cleanup(func() {
			cancel()
			synctest.Wait()
			require.NoError(t, target.Disconnect(t.Context()))
		})

		membership := &ha.Membership{}
		membership.SetRole(ha.RoleActive, 2)
		previous := make(chan struct{})
		s := &server{
			cfg:            &config.Config{RecoveryCheckpointInterval: time.Hour},
			targetCluster:  target,
			pcsm:           pcsm.New(ctx, nil, target, mdb.ServerVersion{}, false, false),
			membership:     membership,
			activeTerm:     1,
			checkpointDone: previous,
		}

		// Given: the previous epoch's checkpoint writer has not exited yet.
		promoted := make(chan struct{})
		go func() {
			defer close(promoted)
			s.onPromote(ctx, 2)
		}()
		synctest.Wait()

		// Then: the promotion waits for it.
		select {
		case <-promoted:
			require.FailNow(t, "promotion did not wait for the previous checkpoint writer")
		default:
		}

		// And: completes once it has exited.
		close(previous)
		synctest.Wait()

		select {
		case <-promoted:
		default:
			require.FailNow(t, "promotion did not complete after the writer exited")
		}

		s.mu.Lock()
		activeTerm := s.activeTerm
		s.mu.Unlock()
		require.Equal(t, ha.Term(2), activeTerm)
	})
}
