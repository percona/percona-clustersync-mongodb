package ha //nolint:testpackage

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/percona/percona-clustersync-mongodb/config"
	"github.com/percona/percona-clustersync-mongodb/errors"
	"github.com/percona/percona-clustersync-mongodb/log"
)

func testLogger() log.Logger {
	return log.New("ha:lease:test")
}

// newTestLeaseMember builds an in-memory Membership for lease role logic. It
// does not touch MongoDB and never starts the loop.
func newTestLeaseMember() *Membership {
	m := newTestMembership()
	m.instanceID = "pcsm-test"
	m.group = "group-a"
	m.roleChangeCh = make(chan RoleChange, 1)
	m.acquire = func(context.Context) (leaseAttempt, error) {
		return leaseAttempt{}, nil
	}

	return m
}

func TestLeaseTickErrorGrace(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		role       Role
		expired    bool
		expected   Role
		wantChange bool
	}{
		{name: "active within deadline", role: RoleActive, expected: RoleActive},
		{name: "active past deadline", role: RoleActive, expired: true, expected: RoleStandby, wantChange: true},
		{name: "standby", role: RoleStandby, expected: RoleStandby},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m := newTestLeaseMember()
			m.SetRole(tt.role, 7)
			m.leaseDeadline = time.Now().Add(time.Hour)
			if tt.expired {
				m.leaseDeadline = time.Unix(1, 0)
			}
			deadline := m.leaseDeadline
			m.acquire = func(context.Context) (leaseAttempt, error) {
				return leaseAttempt{}, errors.New("target unavailable")
			}

			m.leaseTick(t.Context(), testLogger())

			role, term := m.CurrentRole()
			assert.Equal(t, tt.expected, role)
			assert.Equal(t, Term(7), term)
			assert.Equal(t, deadline, m.leaseDeadline, "errors must not extend the deadline")
			select {
			case rc := <-m.RoleChanges():
				require.True(t, tt.wantChange, "unexpected role change")
				assert.Equal(t, RoleChange{Role: RoleStandby, Term: 7}, rc)
			default:
				require.False(t, tt.wantChange, "expected demotion")
			}
		})
	}
}

func TestLeaseTickBoundsActiveAttempt(t *testing.T) {
	t.Parallel()

	m := newTestLeaseMember()
	m.SetRole(RoleActive, 7)
	m.leaseDeadline = time.Now().Add(time.Hour)
	called := false
	m.acquire = func(ctx context.Context) (leaseAttempt, error) {
		called = true
		deadline, ok := ctx.Deadline()
		require.True(t, ok, "ACTIVE attempt must carry a deadline")
		assert.False(t, deadline.After(m.leaseDeadline))

		return leaseAttempt{}, errors.New("target unavailable")
	}

	m.leaseTick(t.Context(), testLogger())
	assert.True(t, called)
}

func TestLeaseTickSuccessfulAttemptAdvancesDeadline(t *testing.T) {
	t.Parallel()

	for _, initialRole := range []Role{RoleStandby, RoleActive} {
		t.Run(string(initialRole), func(t *testing.T) {
			t.Parallel()

			m := newTestLeaseMember()
			m.SetRole(initialRole, 7)
			m.leaseDeadline = time.Now().Add(config.LeaseTTL / 2)
			previous := m.leaseDeadline
			var receivedAt time.Time
			m.acquire = func(context.Context) (leaseAttempt, error) {
				receivedAt = time.Now()

				return leaseAttempt{Acquired: true, Term: 7}, nil
			}

			before := time.Now()
			m.FirstLeaseTick(t.Context())

			role, term := m.CurrentRole()
			assert.Equal(t, RoleActive, role)
			assert.Equal(t, Term(7), term)
			assert.True(t, m.leaseDeadline.After(previous))
			assert.False(t, m.leaseDeadline.Before(before.Add(config.LeaseTTL)))
			assert.False(t, m.leaseDeadline.After(receivedAt.Add(config.LeaseTTL)))
		})
	}
}

func TestLeaseTickLostLeaseDemotesWithinGrace(t *testing.T) {
	t.Parallel()

	m := newTestLeaseMember()
	m.SetRole(RoleActive, 7)
	m.leaseDeadline = time.Now().Add(time.Hour)

	m.leaseTick(t.Context(), testLogger())

	role, _ := m.CurrentRole()
	assert.Equal(t, RoleStandby, role)
	select {
	case rc := <-m.RoleChanges():
		assert.Equal(t, RoleStandby, rc.Role)
	default:
		t.Fatal("expected immediate demotion after losing the lease")
	}
}

func TestLeaseTickExpiredAttemptCannotPromote(t *testing.T) {
	t.Parallel()

	m := newTestLeaseMember()
	m.SetRole(RoleActive, 7)
	m.leaseDeadline = time.Unix(1, 0)
	m.acquire = func(ctx context.Context) (leaseAttempt, error) {
		require.ErrorIs(t, ctx.Err(), context.DeadlineExceeded)

		return leaseAttempt{Acquired: true, Term: 7}, nil
	}

	m.leaseTick(t.Context(), testLogger())

	role, term := m.CurrentRole()
	assert.Equal(t, RoleStandby, role)
	assert.Equal(t, Term(7), term)
}

func TestRunLeaseExpiresWithoutRenewal(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		m := newTestLeaseMember()
		m.SetRole(RoleActive, 7)
		m.leaseDeadline = time.Unix(1, 0)
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan struct{})
		changes := m.RoleChanges()
		started := time.Now()
		go func() {
			defer close(done)
			m.RunLease(ctx)
		}()
		t.Cleanup(func() {
			cancel()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Error("lease loop did not stop after cancellation")
			}
		})

		select {
		case rc := <-changes:
			assert.Equal(t, RoleChange{Role: RoleStandby, Term: 7}, rc)
			assert.Equal(t, started, time.Now(), "expiry must not wait for the renewal ticker")
		case <-time.After(5 * time.Second):
			t.Fatal("lease deadline did not emit a demotion")
		}
	})
}

func TestMemberDefaultRole(t *testing.T) {
	t.Parallel()

	m := newTestLeaseMember()

	role, term := m.CurrentRole()
	assert.Equal(t, RoleStandby, role, "default role should be STANDBY")
	assert.Equal(t, Term(0), term, "default term should be 0")
}

func TestReconcileEmitsOnRoleChange(t *testing.T) {
	t.Parallel()

	m := newTestLeaseMember()

	m.reconcileRole(RoleActive, 1, testLogger())

	role, term := m.CurrentRole()
	assert.Equal(t, RoleActive, role)
	assert.Equal(t, Term(1), term)

	select {
	case rc := <-m.roleChangeCh:
		assert.Equal(t, RoleChange{Role: RoleActive, Term: 1}, rc)
	default:
		t.Fatal("expected a RoleChange to be emitted on STANDBY->ACTIVE")
	}
}

func TestReconcileNoEmitOnRenew(t *testing.T) {
	t.Parallel()

	m := newTestLeaseMember()

	// First transition to ACTIVE emits.
	m.reconcileRole(RoleActive, 1, testLogger())
	<-m.roleChangeCh

	// Staying ACTIVE across renewals (term unchanged) must not emit again.
	m.reconcileRole(RoleActive, 1, testLogger())

	select {
	case rc := <-m.roleChangeCh:
		t.Fatalf("expected no emit on renew, got %+v", rc)
	default:
	}
}

func TestReconcileNoEmitOnTermOnlyChangeWhileActive(t *testing.T) {
	t.Parallel()

	m := newTestLeaseMember()

	m.reconcileRole(RoleActive, 1, testLogger())
	<-m.roleChangeCh

	// A term advance with no role change is still not a transition.
	m.reconcileRole(RoleActive, 2, testLogger())

	_, term := m.CurrentRole()
	assert.Equal(t, Term(2), term, "term should still be updated")

	select {
	case rc := <-m.roleChangeCh:
		t.Fatalf("expected no emit on term-only change while ACTIVE, got %+v", rc)
	default:
	}
}

func TestEmitRoleChangeCoalesces(t *testing.T) {
	t.Parallel()

	m := newTestMembership()
	m.roleChangeCh = make(chan RoleChange, 1)

	// Fill the cap-1 buffer, then emit a newer change without draining.
	m.emitRoleChange(RoleChange{Role: RoleActive, Term: 1})
	m.emitRoleChange(RoleChange{Role: RoleStandby, Term: 2})

	// Exactly one value should be buffered, and it must be the latest.
	rc := <-m.roleChangeCh
	assert.Equal(t, RoleChange{Role: RoleStandby, Term: 2}, rc,
		"latest emit must win when coalesced")

	select {
	case extra := <-m.roleChangeCh:
		t.Fatalf("expected a single coalesced change, found a second: %+v", extra)
	default:
	}
}

func TestReconcileConcurrent(t *testing.T) {
	t.Parallel()

	m := newTestLeaseMember()

	// Continuously drain emitted changes so writers never block on the buffer;
	// this also exercises concurrent send/receive on roleCh under -race.
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-m.roleChangeCh:
			case <-done:
				return
			}
		}
	}()

	var wg sync.WaitGroup

	const n = 50

	for i := range n {
		wg.Go(func() {
			role := RoleActive
			if i%2 == 0 {
				role = RoleStandby
			}
			m.reconcileRole(role, Term(i), testLogger())
		})
	}

	for range n {
		wg.Go(func() {
			_, _ = m.CurrentRole()
		})
	}

	wg.Wait()
	close(done)

	// Role must be one of the valid roles; no assertion on the exact final term
	// since writers interleave.
	role, _ := m.CurrentRole()
	assert.Contains(t, []Role{RoleActive, RoleStandby}, role)
}
