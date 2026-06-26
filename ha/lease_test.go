package ha //nolint:testpackage

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/percona/percona-clustersync-mongodb/log"
)

func testLogger() log.Logger {
	return log.New("ha:lease:test")
}

// newTestLeaseMember builds an in-memory Membership for lease role logic (the
// single source of truth for role/term). It does not touch MongoDB and never
// starts the loop.
func newTestLeaseMember() *Membership {
	m := newTestMembership()
	m.instanceID = "pcsm-test"
	m.group = "group-a"
	m.roleChangeCh = make(chan RoleChange, 1)

	return m
}

func TestMemberDefaultRole(t *testing.T) {
	t.Parallel()

	m := newTestLeaseMember()

	role, term := m.CurrentRole()
	assert.Equal(t, RoleStandby, role, "default role should be STANDBY")
	assert.Equal(t, int64(0), term, "default term should be 0")
}

func TestReconcileEmitsOnRoleChange(t *testing.T) {
	t.Parallel()

	m := newTestLeaseMember()

	m.reconcileRole(RoleActive, 1, testLogger())

	role, term := m.CurrentRole()
	assert.Equal(t, RoleActive, role)
	assert.Equal(t, int64(1), term)

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
	assert.Equal(t, int64(2), term, "term should still be updated")

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
			m.reconcileRole(role, int64(i), testLogger())
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
