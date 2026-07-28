package ha

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/percona/percona-clustersync-mongodb/config"
	"github.com/percona/percona-clustersync-mongodb/errors"
	"github.com/percona/percona-clustersync-mongodb/log"
	"github.com/percona/percona-clustersync-mongodb/mdb"
)

// RoleChange is a single active-standby role transition. Term is the lease term
// at the transition; it grows monotonically and doubles as the fencing token.
type RoleChange struct {
	Role Role
	Term int64
}

// runLease tries to acquire or renew the lease on every tick and reconciles the
// member's role, until ctx is canceled. Holding the lease grants the right to be
// ACTIVE. Time comparisons use the target server clock ($$NOW), not the client
// clock. A failed renewal while ACTIVE demotes the member so it fails safe.
func (m *Membership) runLease(ctx context.Context) {
	lg := log.New("ha:lease").With(log.String("instanceId", m.instanceID))

	ticker := time.NewTicker(config.LeaseRenewInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			lg.Info("Lease loop canceled")

			return

		case <-ticker.C:
			m.leaseTick(ctx, lg)
		}
	}
}

// FirstLeaseTick performs one synchronous acquire/renew attempt. Called once at
// startup, before the HTTP server serves requests, so the role is settled by
// then: without it a fresh instance would briefly report the STANDBY default
// and spuriously reject writes. Subsequent renewals run in RunLease.
func (m *Membership) FirstLeaseTick(ctx context.Context) {
	lg := log.New("ha:lease").With(log.String("instanceId", m.instanceID))
	m.leaseTick(ctx, lg)
}

// leaseAttempt is the outcome of a single acquire/renew attempt. Term is
// meaningful only when Acquired is true.
type leaseAttempt struct {
	Acquired bool
	Term     int64
}

// leaseTick performs one acquire/renew attempt and reconciles the resulting role.
func (m *Membership) leaseTick(ctx context.Context, lg log.Logger) {
	att, err := m.tryAcquireOrRenew(ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return
		}

		// The error says nothing about ownership; we can no longer prove we
		// hold the lease, so fail safe and demote.
		lg.Error(err, "acquire or renew lease")
		_, lastTerm := m.CurrentRole()
		m.reconcileRole(RoleStandby, lastTerm, lg)

		return
	}

	if att.Acquired {
		m.reconcileRole(RoleActive, att.Term, lg)

		return
	}

	m.reconcileRole(RoleStandby, att.Term, lg)
}

// reconcileRole records the role/term on the member and emits a RoleChange when
// the role actually transitions. Same-role renewals do not emit.
func (m *Membership) reconcileRole(role Role, term int64, lg log.Logger) {
	if !m.SetRole(role, term) {
		return
	}

	lg.With(log.String("role", string(role)), log.Int64("term", term)).
		Info("Role transition")

	m.emitRoleChange(RoleChange{Role: role, Term: term})
}

// tryAcquireOrRenew atomically acquires or renews the lease. Losing to another
// instance's unexpired lease is reported as held=false with a nil error.
//
// Two steps, because MongoDB forbids $expr in an upsert predicate:
//  1. Take/renew an existing lease with a non-upsert update whose server-clock
//     $expr filter is "I own it OR it has expired".
//  2. If nothing matched, bootstrap with an insert. Insert (not upsert) is
//     deliberate: a duplicate-key error is how a losing contender is detected,
//     without overwriting the current owner's lease.
//
// $$NOW is invalid in an insert, so the bootstrap stamps client-clock times; a
// successful bootstrap immediately re-renews to replace them with server-clock
// stamps.
func (m *Membership) tryAcquireOrRenew(ctx context.Context) (leaseAttempt, error) {
	ctx, cancel := context.WithTimeout(ctx, config.HAOperationTimeout)
	defer cancel()

	// Step 1 (common path): take or renew an existing lease.
	att, matched, err := m.tryTakeOrRenewExisting(ctx)
	if err != nil {
		return leaseAttempt{}, err
	}
	if matched {
		return att, nil
	}

	// Step 2: the lease is absent (bootstrap it) or held by another instance
	// (duplicate key -> we lose).
	err = m.tryInsertLease(ctx)
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return leaseAttempt{}, nil
		}

		return leaseAttempt{}, errors.Wrap(err, "bootstrap lease")
	}

	att, matched, err = m.tryTakeOrRenewExisting(ctx)
	if err != nil {
		return leaseAttempt{}, err
	}
	if !matched {
		return leaseAttempt{}, nil
	}

	return att, nil
}

// tryInsertLease creates the lease document at term 1. A duplicate-key error
// (returned unwrapped) means another instance owns the lease: a loss. The
// client-clock timestamps are provisional; the caller re-renews immediately so
// they are re-stamped with the server clock, which is also where the
// authoritative ownership/term is read.
func (m *Membership) tryInsertLease(ctx context.Context) error {
	now := time.Now().UTC()

	// A duplicate-key error is not transient: returned immediately, unwrapped.
	err := mdb.RunWithRetry(ctx, func(ctx context.Context) error {
		_, err := m.leaseColl().InsertOne(ctx, bson.D{
			{"_id", LeaseID},
			{fieldGroup, m.group},
			{fieldInstanceID, m.instanceID},
			{fieldTerm, int64(1)},
			{"electionDate", now},
			{fieldExpiresAt, now.Add(config.LeaseTTL)},
		})

		return err //nolint:wrapcheck
	}, mdb.DefaultRetryInterval, mdb.DefaultMaxRetries)

	return err //nolint:wrapcheck // caller inspects for duplicate-key
}

// tryTakeOrRenewExisting updates an existing lease when this instance owns it
// or it has expired (per the server clock). The matched return reports whether
// the filter matched a document: false means the lease is absent or held by
// another instance and the caller must disambiguate. On a match the post-image
// is always owned by this instance (the pipeline sets instanceId
// unconditionally), so a match is always an acquisition.
func (m *Membership) tryTakeOrRenewExisting(ctx context.Context) (leaseAttempt, bool, error) {
	ttlMS := config.LeaseTTL.Milliseconds()

	// isRenew is true when this instance already owns the lease in the pre-image.
	isRenew := bson.D{{"$eq", bson.A{"$instanceId", m.instanceID}}}

	pipeline := mongo.Pipeline{
		{{"$set", bson.D{
			{fieldGroup, m.group},
			{fieldInstanceID, m.instanceID},
			{fieldExpiresAt, bson.D{{aggAdd, bson.A{aggNow, ttlMS}}}},
			{"electionDate", bson.D{{"$cond", bson.D{
				{"if", isRenew},
				{"then", bson.D{{aggIfNull, bson.A{"$electionDate", aggNow}}}},
				{"else", aggNow},
			}}}},
			{fieldTerm, bson.D{{"$cond", bson.D{
				{"if", isRenew},
				{"then", bson.D{{aggIfNull, bson.A{"$term", int64(0)}}}},
				{"else", bson.D{{aggAdd, bson.A{
					bson.D{{aggIfNull, bson.A{"$term", int64(0)}}},
					int64(1),
				}}}},
			}}}},
		}}},
	}

	// Filter without upsert may use $expr/$$NOW: take when we own it or it expired.
	filter := bson.D{{"_id", LeaseID}, {"$expr", bson.D{{"$or", bson.A{
		bson.D{{"$eq", bson.A{"$instanceId", m.instanceID}}},
		bson.D{{"$lte", bson.A{"$expiresAt", aggNow}}},
	}}}}}

	var (
		updatedLease Lease
		matched      bool
	)

	// ErrNoDocuments is the normal "filter did not match" outcome, not an error
	// to retry, so it is absorbed inside the closure.
	err := mdb.RunWithRetry(ctx, func(ctx context.Context) error {
		decodeErr := m.leaseColl().FindOneAndUpdate(
			ctx, filter, pipeline,
			options.FindOneAndUpdate().SetReturnDocument(options.After),
		).Decode(&updatedLease)
		if errors.Is(decodeErr, mongo.ErrNoDocuments) {
			matched = false

			return nil
		}
		if decodeErr != nil {
			return decodeErr //nolint:wrapcheck
		}

		matched = true

		return nil
	}, mdb.DefaultRetryInterval, mdb.DefaultMaxRetries)
	if err != nil {
		return leaseAttempt{}, false, errors.Wrap(err, "take or renew lease")
	}

	if !matched {
		return leaseAttempt{}, false, nil
	}

	return leaseAttempt{Acquired: true, Term: updatedLease.Term}, true, nil
}

// releaseLease expires the lease (only if this instance still owns it) so a
// standby can take over without waiting for the TTL, and demotes the member.
func (m *Membership) releaseLease(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, config.HAOperationTimeout)
	defer cancel()

	err := mdb.RunWithRetry(ctx, func(ctx context.Context) error {
		_, err := m.leaseColl().UpdateOne(
			ctx,
			bson.D{{"_id", LeaseID}, {fieldInstanceID, m.instanceID}},
			mongo.Pipeline{
				{{"$set", bson.D{{fieldExpiresAt, aggNow}}}},
			},
		)

		return err //nolint:wrapcheck
	}, mdb.DefaultRetryInterval, mdb.DefaultMaxRetries)

	_, term := m.CurrentRole()
	m.SetRole(RoleStandby, term)

	return errors.Wrap(err, "release lease")
}

func (m *Membership) leaseColl() *mongo.Collection {
	return m.target.Database(config.PCSMDatabase).Collection(config.LeaseCollection)
}

// DeleteLease clears the lease collection. Used by reset.
func DeleteLease(ctx context.Context, target *mongo.Client) error {
	err := mdb.RunWithRetry(ctx, func(ctx context.Context) error {
		_, err := target.Database(config.PCSMDatabase).
			Collection(config.LeaseCollection).
			DeleteMany(ctx, bson.D{})

		return err //nolint:wrapcheck
	}, mdb.DefaultRetryInterval, mdb.DefaultMaxRetries)

	return errors.Wrap(err, "delete lease")
}
