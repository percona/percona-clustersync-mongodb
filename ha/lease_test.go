package ha //nolint:testpackage

import (
	"context"
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

// newTestLeaseMember builds an in-memory Membership for lease logic. It does
// not touch MongoDB and never starts the loop.
func newTestLeaseMember() *Membership {
	m := newTestMembership()
	m.instanceID = "pcsm-test"
	m.group = "group-a"
	m.acquire = func(context.Context) (leaseAttempt, error) {
		return leaseAttempt{}, nil
	}

	return m
}

// recorder collects the ownership a lease attempt reported.
type recorder struct {
	reports []Ownership
}

func (r *recorder) observe(o Ownership) { r.reports = append(r.reports, o) }

func TestLeaseTickErrorGrace(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		held    bool
		expired bool
		want    []Ownership
	}{
		{name: "held within deadline", held: true, want: nil},
		{name: "held past deadline", held: true, expired: true, want: []Ownership{{}}},
		{name: "not held", want: []Ownership{{}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m := newTestLeaseMember()
			m.held = tt.held
			m.leaseDeadline = time.Now().Add(time.Hour)
			if tt.expired {
				m.leaseDeadline = time.Unix(1, 0)
			}
			deadline := m.leaseDeadline
			m.acquire = func(context.Context) (leaseAttempt, error) {
				return leaseAttempt{}, errors.New("target unavailable")
			}

			var rec recorder
			m.leaseTick(t.Context(), testLogger(), rec.observe)

			assert.Equal(t, tt.want, rec.reports)
			assert.Equal(t, deadline, m.leaseDeadline, "errors must not extend the deadline")
			assert.Equal(t, len(tt.want) == 0 && tt.held, m.held)
		})
	}
}

func TestLeaseTickBoundsHeldAttempt(t *testing.T) {
	t.Parallel()

	m := newTestLeaseMember()
	m.held = true
	m.leaseDeadline = time.Now().Add(time.Hour)
	called := false
	m.acquire = func(ctx context.Context) (leaseAttempt, error) {
		called = true
		deadline, ok := ctx.Deadline()
		require.True(t, ok, "an attempt while holding the lease must carry a deadline")
		assert.False(t, deadline.After(m.leaseDeadline))

		return leaseAttempt{}, errors.New("target unavailable")
	}

	var rec recorder
	m.leaseTick(t.Context(), testLogger(), rec.observe)
	assert.True(t, called)
	assert.Empty(t, rec.reports)
}

func TestLeaseTickSuccessfulAttemptAdvancesDeadline(t *testing.T) {
	t.Parallel()

	for _, held := range []bool{false, true} {
		t.Run(map[bool]string{false: "not held", true: "held"}[held], func(t *testing.T) {
			t.Parallel()

			m := newTestLeaseMember()
			m.held = held
			m.leaseDeadline = time.Now().Add(config.LeaseTTL / 2)
			previous := m.leaseDeadline
			var receivedAt time.Time
			m.acquire = func(context.Context) (leaseAttempt, error) {
				receivedAt = time.Now()

				return leaseAttempt{Acquired: true, Term: 7}, nil
			}

			var rec recorder
			before := time.Now()
			m.FirstLeaseTick(t.Context(), rec.observe)

			assert.Equal(t, []Ownership{{Held: true, Term: 7}}, rec.reports)
			assert.True(t, m.held)
			assert.True(t, m.leaseDeadline.After(previous))
			assert.False(t, m.leaseDeadline.Before(before.Add(config.LeaseTTL)))
			assert.False(t, m.leaseDeadline.After(receivedAt.Add(config.LeaseTTL)))
		})
	}
}

func TestLeaseTickLostLeaseReportsAtOnce(t *testing.T) {
	t.Parallel()

	m := newTestLeaseMember()
	m.held = true
	m.leaseDeadline = time.Now().Add(time.Hour)

	var rec recorder
	m.leaseTick(t.Context(), testLogger(), rec.observe)

	assert.Equal(t, []Ownership{{}}, rec.reports, "a lost lease is reported before the deadline")
	assert.False(t, m.held)
}

func TestLeaseTickExpiredAttemptCannotAcquire(t *testing.T) {
	t.Parallel()

	m := newTestLeaseMember()
	m.held = true
	m.leaseDeadline = time.Unix(1, 0)
	m.acquire = func(ctx context.Context) (leaseAttempt, error) {
		require.ErrorIs(t, ctx.Err(), context.DeadlineExceeded)

		return leaseAttempt{Acquired: true, Term: 7}, nil
	}

	var rec recorder
	m.leaseTick(t.Context(), testLogger(), rec.observe)

	assert.Equal(t, []Ownership{{}}, rec.reports, "an attempt past the deadline cannot establish ownership")
	assert.False(t, m.held)
}

func TestLeaseTickCanceledReportsNothing(t *testing.T) {
	t.Parallel()

	m := newTestLeaseMember()
	m.held = true
	m.leaseDeadline = time.Now().Add(time.Hour)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	var rec recorder
	m.leaseTick(ctx, testLogger(), rec.observe)

	assert.Empty(t, rec.reports, "cancellation is shutdown, not a loss")
	assert.True(t, m.held)
}

func TestLeaseTickReportsEveryAttempt(t *testing.T) {
	t.Parallel()

	m := newTestLeaseMember()
	m.acquire = func(context.Context) (leaseAttempt, error) {
		return leaseAttempt{Acquired: true, Term: 1}, nil
	}

	var rec recorder
	for range 3 {
		m.leaseTick(t.Context(), testLogger(), rec.observe)
	}

	assert.Equal(t, []Ownership{{Held: true, Term: 1}, {Held: true, Term: 1}, {Held: true, Term: 1}}, rec.reports,
		"ownership is a level: every attempt reports it, unchanged or not")
}

func TestRunLeaseExpiresWithoutRenewal(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		m := newTestLeaseMember()
		m.held = true
		m.leaseDeadline = time.Unix(1, 0)
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan struct{})
		reports := make(chan Ownership, 1)
		started := time.Now()
		go func() {
			defer close(done)
			m.RunLease(ctx, func(o Ownership) { reports <- o })
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
		case o := <-reports:
			assert.Equal(t, Ownership{}, o)
			assert.Equal(t, started, time.Now(), "expiry must not wait for the renewal ticker")
		case <-time.After(5 * time.Second):
			t.Fatal("lease deadline did not report the lease lost")
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
