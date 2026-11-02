package ha //nolint:testpackage

import (
	"context"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/drivertest"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/xoptions"

	"github.com/percona/percona-clustersync-mongodb/config"
	"github.com/percona/percona-clustersync-mongodb/errors"
	"github.com/percona/percona-clustersync-mongodb/util"
)

func TestMembershipPeriodicHeartbeatWithoutSetRole(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		commands := make(chan bson.Raw, 1)
		completed := make(chan struct{}, 1)
		opts := options.Client().SetMonitor(&event.CommandMonitor{
			Started: func(_ context.Context, e *event.CommandStartedEvent) {
				if e.CommandName == "update" {
					select {
					case commands <- append(bson.Raw(nil), e.Command...):
					default:
					}
				}
			},
			Succeeded: func(_ context.Context, e *event.CommandSucceededEvent) {
				if e.CommandName == "update" {
					select {
					case completed <- struct{}{}:
					default:
					}
				}
			},
		})
		require.NoError(t, xoptions.SetInternalClientOptions(opts, "deployment",
			drivertest.NewMockDeployment(bson.D{{"ok", 1}, {"n", 1}})))
		client, err := mongo.Connect(opts)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, util.CtxWithTimeout(
				context.WithoutCancel(t.Context()), config.HAOperationTimeout, client.Disconnect))
		}()

		m := newTestMembership()
		m.target = client
		m.instanceID = "pcsm-periodic"
		m.group = "periodic-group"
		m.host = "periodic-host"
		m.port = 2242
		m.version = "periodic-version"
		m.startedAt = time.Now().UTC()
		m.role = RoleActive
		m.term = 7

		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan struct{})
		defer func() {
			cancel()
			assert.NoError(t, util.CtxWithTimeout(
				context.WithoutCancel(ctx), config.HAOperationTimeout, func(cleanupCtx context.Context) error {
					select {
					case <-done:
						return nil
					case <-cleanupCtx.Done():
						return errors.Wrap(cleanupCtx.Err(), "membership heartbeat loop did not stop during cleanup")
					}
				}))
		}()
		go func() {
			defer close(done)
			m.run(ctx)
		}()

		// Wait until run has installed its ticker; no role nudge is sent.
		synctest.Wait()
		select {
		case <-commands:
			require.FailNow(t, "heartbeat emitted before the first periodic tick")
		default:
		}

		// Time advances only inside the synctest bubble.
		<-time.After(config.MemberHeartbeatInterval)
		synctest.Wait()
		select {
		case <-completed:
		default:
			require.FailNow(t, "periodic tick did not complete a heartbeat without SetRole")
		}
		var command bson.Raw
		select {
		case command = <-commands:
		default:
			require.FailNow(t, "completed heartbeat had no observed update command")
		}
		assert.Equal(t, config.PCSMDatabase, command.Lookup("$db").StringValue())
		assert.Equal(t, config.MembersCollection, command.Lookup("update").StringValue())
		update := command.Lookup("updates").Array().Index(0).Document()
		assert.Equal(t, m.instanceID, update.Lookup("q", "_id").StringValue())
		assert.True(t, update.Lookup("upsert").Boolean())
		set := update.Lookup("u").Array().Index(0).Document().Lookup("$set").Document()
		assert.Equal(t, m.group, set.Lookup("group").StringValue())
		assert.Equal(t, m.host, set.Lookup("host").StringValue())
		assert.Equal(t, int32(2242), set.Lookup("port").Int32())
		assert.Equal(t, string(RoleActive), set.Lookup("role").StringValue())
		assert.Equal(t, int64(7), set.Lookup("term").Int64())
		assert.Equal(t, m.version, set.Lookup("pcsmVersion").StringValue())
		assert.Equal(t, bson.NewDateTimeFromTime(m.startedAt), bson.DateTime(set.Lookup("startedAt").DateTime()))
		assert.Equal(t, "$$NOW", set.Lookup("lastHeartbeat").StringValue())

		cancel()
		synctest.Wait()
		select {
		case <-done:
		default:
			require.FailNow(t, "membership heartbeat loop did not exit on cancellation")
		}
	})
}

// newTestMembership builds a Membership with only the in-memory fields needed
// for role/signal logic. It does not touch MongoDB and never starts the loop.
func newTestMembership() *Membership {
	return &Membership{
		role:    RoleStandby,
		term:    0,
		beatNow: make(chan struct{}, 1),
	}
}

func TestNewInstanceID(t *testing.T) {
	t.Parallel()

	a := NewInstanceID()
	b := NewInstanceID()

	assert.True(t, strings.HasPrefix(a, "pcsm-"), "instance id should be prefixed with pcsm-")
	assert.NotEqual(t, a, b, "two generated ids should be unique")
	assert.Greater(t, len(a), len("pcsm-"), "instance id should contain a uuid suffix")
}

func TestMembershipDefaultRole(t *testing.T) {
	t.Parallel()

	m := newTestMembership()

	role, term := m.currentRole()
	assert.Equal(t, RoleStandby, role, "default role should be STANDBY")
	assert.Equal(t, Term(0), term, "default term should be 0")
}

func TestSetRoleRoundTrip(t *testing.T) {
	t.Parallel()

	m := newTestMembership()

	m.SetRole(RoleActive, 7)
	role, term := m.currentRole()
	assert.Equal(t, RoleActive, role)
	assert.Equal(t, Term(7), term)

	// A subsequent transition overwrites the previous one.
	m.SetRole(RoleStandby, 8)
	role, term = m.currentRole()
	assert.Equal(t, RoleStandby, role)
	assert.Equal(t, Term(8), term)
}

func TestSetRoleReportsTransition(t *testing.T) {
	t.Parallel()

	m := newTestMembership()

	assert.True(t, m.SetRole(RoleActive, 1), "STANDBY->ACTIVE is a transition")
	assert.False(t, m.SetRole(RoleActive, 1), "a renewal is not")
	assert.False(t, m.SetRole(RoleActive, 2), "nor is a new term while ACTIVE; the role is what transitions")
	assert.True(t, m.SetRole(RoleStandby, 2), "ACTIVE->STANDBY is a transition")
}

func TestSetRoleSignalsBeatNow(t *testing.T) {
	t.Parallel()

	m := newTestMembership()

	m.SetRole(RoleActive, 1)

	select {
	case <-m.beatNow:
		// expected: a nudge was delivered
	default:
		t.Fatal("expected SetRole to signal beatNow")
	}
}

func TestSetRoleNudgeCoalesces(t *testing.T) {
	t.Parallel()

	m := newTestMembership()

	// First call fills the cap-1 buffer.
	m.SetRole(RoleActive, 1)
	// Second call must not block even though the buffer is full; the token is
	// dropped but the latest state is still stored.
	m.SetRole(RoleActive, 2)

	role, term := m.currentRole()
	assert.Equal(t, RoleActive, role)
	assert.Equal(t, Term(2), term, "latest SetRole state must win even when the nudge is coalesced")

	// Exactly one token should be buffered (coalesced).
	<-m.beatNow
	select {
	case <-m.beatNow:
		t.Fatal("expected a single coalesced nudge, found a second token")
	default:
	}
}

func TestSetRoleConcurrent(t *testing.T) {
	t.Parallel()

	m := newTestMembership()

	// Continuously drain nudges so writers never observe a full buffer issue;
	// this also exercises concurrent send/receive on beatNow under -race.
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-m.beatNow:
			case <-done:
				return
			}
		}
	}()

	var wg sync.WaitGroup

	const n = 50

	for i := range n {
		wg.Go(func() {
			m.SetRole(RoleActive, Term(i))
		})
	}

	for range n {
		wg.Go(func() {
			_, _ = m.currentRole()
		})
	}

	wg.Wait()
	close(done)

	// Final state must be a valid ACTIVE role (term is one of the writers' values).
	role, _ := m.currentRole()
	assert.Equal(t, RoleActive, role)
}
