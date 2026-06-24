package ha //nolint:testpackage

import (
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

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
	assert.Equal(t, int64(0), term, "default term should be 0")
}

func TestSetRoleRoundTrip(t *testing.T) {
	t.Parallel()

	m := newTestMembership()

	m.SetRole(RoleActive, 7)
	role, term := m.currentRole()
	assert.Equal(t, RoleActive, role)
	assert.Equal(t, int64(7), term)

	// A subsequent transition overwrites the previous one.
	m.SetRole(RoleStandby, 8)
	role, term = m.currentRole()
	assert.Equal(t, RoleStandby, role)
	assert.Equal(t, int64(8), term)
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
	assert.Equal(t, int64(2), term, "latest SetRole state must win even when the nudge is coalesced")

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
			m.SetRole(RoleActive, int64(i))
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
