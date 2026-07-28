//go:build integration

package ha //nolint:testpackage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/percona/percona-clustersync-mongodb/config"
)

func leaseCollection(client *mongo.Client) *mongo.Collection {
	return client.Database(config.PCSMDatabase).Collection(config.LeaseCollection)
}

// cleanLease drops the lease collection so each test starts from a known baseline.
func cleanLease(t *testing.T, ctx context.Context, client *mongo.Client) {
	t.Helper()

	require.NoError(t, leaseCollection(client).Drop(ctx))
}

// newLeaseMemberForTest joins membership for instanceID, mirroring production
// wiring. The refresh loop is stopped on test cleanup.
func newLeaseMemberForTest(t *testing.T, ctx context.Context, client *mongo.Client, instanceID string) *Membership {
	t.Helper()

	m, err := JoinMembership(ctx, client, MembershipOptions{InstanceID: instanceID, Group: "group-a"})
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Stop(ctx) })

	return m
}

// forceExpireLease moves expiresAt into the past so the lease is immediately
// acquirable, keeping failover tests deterministic without sleeping a full TTL.
func forceExpireLease(t *testing.T, ctx context.Context, client *mongo.Client) {
	t.Helper()

	res, err := leaseCollection(client).UpdateOne(
		ctx,
		bson.D{{"_id", LeaseID}},
		mongo.Pipeline{
			{{"$set", bson.D{{"expiresAt", bson.D{{"$subtract", bson.A{"$$NOW", config.LeaseTTL.Milliseconds()}}}}}}},
		},
	)
	require.NoError(t, err)
	require.Equal(t, int64(1), res.MatchedCount, "lease document should exist to expire")
}

func readLease(t *testing.T, ctx context.Context, client *mongo.Client) Lease {
	t.Helper()

	var lease Lease
	err := leaseCollection(client).FindOne(ctx, bson.D{{"_id", LeaseID}}).Decode(&lease)
	require.NoError(t, err)

	return lease
}

func TestLeaseSingleAcquirer(t *testing.T) {
	ctx := t.Context()
	client := connectToMongoDB(t)
	defer func() { _ = client.Disconnect(ctx) }()

	cleanLease(t, ctx, client)

	m := newLeaseMemberForTest(t, ctx, client, "pcsm-solo")

	att, err := m.tryAcquireOrRenew(ctx)
	require.NoError(t, err)
	assert.True(t, att.Acquired, "the only instance should win the lease")
	assert.Equal(t, int64(1), att.Term, "first acquisition should be term 1")

	lease := readLease(t, ctx, client)
	assert.Equal(t, "pcsm-solo", lease.InstanceID)
	assert.Equal(t, "group-a", lease.Group)
	assert.Equal(t, int64(1), lease.Term)
	assert.False(t, lease.ExpiresAt.IsZero(), "expiresAt should be stamped by the server")
}

func TestLeaseRenewKeepsTerm(t *testing.T) {
	ctx := t.Context()
	client := connectToMongoDB(t)
	defer func() { _ = client.Disconnect(ctx) }()

	cleanLease(t, ctx, client)

	m := newLeaseMemberForTest(t, ctx, client, "pcsm-renew")

	att, err := m.tryAcquireOrRenew(ctx)
	require.NoError(t, err)
	require.True(t, att.Acquired)
	require.Equal(t, int64(1), att.Term)

	first := readLease(t, ctx, client)

	for range 3 {
		att, err = m.tryAcquireOrRenew(ctx)
		require.NoError(t, err)
		assert.True(t, att.Acquired, "owner should keep renewing")
		assert.Equal(t, int64(1), att.Term, "renewal must not bump the term")
	}

	latest := readLease(t, ctx, client)
	assert.Equal(t, first.ElectionDate, latest.ElectionDate, "electionDate is preserved across renewals")
	assert.False(t, latest.ExpiresAt.Before(first.ExpiresAt), "expiresAt should advance on renewal")
}

func TestLeaseContentionExactlyOneActive(t *testing.T) {
	ctx := t.Context()
	client := connectToMongoDB(t)
	defer func() { _ = client.Disconnect(ctx) }()

	cleanLease(t, ctx, client)

	const n = 5

	winners := 0
	for i := range n {
		m := newLeaseMemberForTest(t, ctx, client, "pcsm-contender-"+string(rune('a'+i)))

		att, err := m.tryAcquireOrRenew(ctx)
		require.NoError(t, err)
		if att.Acquired {
			winners++
		}
	}

	assert.Equal(t, 1, winners, "exactly one contender should hold the lease")

	lease := readLease(t, ctx, client)
	assert.Equal(t, int64(1), lease.Term, "a single contested acquisition is term 1")
}

func TestLeaseFailoverBumpsTerm(t *testing.T) {
	ctx := t.Context()
	client := connectToMongoDB(t)
	defer func() { _ = client.Disconnect(ctx) }()

	cleanLease(t, ctx, client)

	active := newLeaseMemberForTest(t, ctx, client, "pcsm-active")
	standby := newLeaseMemberForTest(t, ctx, client, "pcsm-standby")

	att, err := active.tryAcquireOrRenew(ctx)
	require.NoError(t, err)
	require.True(t, att.Acquired)
	require.Equal(t, int64(1), att.Term)

	// While the active's lease is valid, the standby cannot take over.
	att, err = standby.tryAcquireOrRenew(ctx)
	require.NoError(t, err)
	require.False(t, att.Acquired, "standby must not win while the lease is valid")

	// Simulate the active dying: force the lease to expire, then the standby
	// wins with an incremented term.
	forceExpireLease(t, ctx, client)

	att, err = standby.tryAcquireOrRenew(ctx)
	require.NoError(t, err)
	assert.True(t, att.Acquired, "standby should win after the lease expires")
	assert.Equal(t, int64(2), att.Term, "failover must bump the term")

	lease := readLease(t, ctx, client)
	assert.Equal(t, "pcsm-standby", lease.InstanceID)
}

func TestLeaseReleaseFreesPromptly(t *testing.T) {
	ctx := t.Context()
	client := connectToMongoDB(t)
	defer func() { _ = client.Disconnect(ctx) }()

	cleanLease(t, ctx, client)

	active := newLeaseMemberForTest(t, ctx, client, "pcsm-active")
	standby := newLeaseMemberForTest(t, ctx, client, "pcsm-standby")

	att, err := active.tryAcquireOrRenew(ctx)
	require.NoError(t, err)
	require.True(t, att.Acquired)

	require.NoError(t, active.Release(ctx))

	// After release the lease is immediately acquirable without waiting TTL.
	att, err = standby.tryAcquireOrRenew(ctx)
	require.NoError(t, err)
	assert.True(t, att.Acquired, "standby should win immediately after the active releases")
	assert.Equal(t, int64(2), att.Term, "the post-release acquisition is a fresh win")
}

func TestLeaseLosingContenderDoesNotAcquire(t *testing.T) {
	ctx := t.Context()
	client := connectToMongoDB(t)
	defer func() { _ = client.Disconnect(ctx) }()

	cleanLease(t, ctx, client)

	// Another instance owns an unexpired lease.
	owner := newLeaseMemberForTest(t, ctx, client, "pcsm-owner")
	att, err := owner.tryAcquireOrRenew(ctx)
	require.NoError(t, err)
	require.True(t, att.Acquired)

	// A contender that reached the re-renew step sees no match: the lease is
	// held by another instance and unexpired.
	contender := newLeaseMemberForTest(t, ctx, client, "pcsm-contender")
	att, matched, err := contender.tryTakeOrRenewExisting(ctx)
	require.NoError(t, err)
	assert.False(t, matched, "foreign unexpired lease must not match the take/renew filter")
	assert.False(t, att.Acquired, "not-matched must report not-acquired")

	// End-to-end: the contender loses and reports not-acquired with no error.
	att, err = contender.tryAcquireOrRenew(ctx)
	require.NoError(t, err)
	assert.False(t, att.Acquired, "a losing contender must report not-acquired")

	// The owner still holds the lease.
	lease := readLease(t, ctx, client)
	assert.Equal(t, "pcsm-owner", lease.InstanceID)
}

func TestLeaseRunEmitsActive(t *testing.T) {
	ctx := t.Context()
	client := connectToMongoDB(t)
	defer func() { _ = client.Disconnect(ctx) }()

	cleanLease(t, ctx, client)

	m := newLeaseMemberForTest(t, ctx, client, "pcsm-run")

	// FirstLeaseTick settles the role synchronously, as at startup.
	m.FirstLeaseTick(ctx)

	select {
	case rc := <-m.RoleChanges():
		assert.Equal(t, RoleActive, rc.Role, "the sole instance should become ACTIVE")
		assert.Equal(t, int64(1), rc.Term)
	case <-time.After(5 * time.Second):
		t.Fatal("expected a RoleChange to ACTIVE within timeout")
	}

	role, _ := m.CurrentRole()
	assert.Equal(t, RoleActive, role)
}
