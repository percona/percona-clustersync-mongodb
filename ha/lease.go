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
// in effect at the transition; it increases monotonically across acquisitions
// and doubles as the fencing token.
type RoleChange struct {
	Role Role
	Term int64
}

// runLease drives the lease loop until ctx is canceled. There is no election:
// the member competes for a single lease document via an atomic conditional
// write, and holding the lease grants the right to be ACTIVE. Time comparisons
// use the target server clock ($$NOW), so the loop does not depend on client
// wall-clock time. On each tick it attempts to acquire or renew the lease and
// reconciles the member's role; a failed renewal while ACTIVE (lost lease,
// expired, or target unreachable) demotes the member so it fails safe.
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

// FirstLeaseTick performs a single synchronous acquire/renew attempt and
// reconciles the resulting role. It is meant to be called once at startup,
// before the HTTP server begins serving, so the instance's role is already
// settled (ACTIVE for an uncontested single instance, STANDBY if another
// instance holds the lease) by the time requests arrive. Without it there is a
// brief window where a freshly started instance still reports the STANDBY
// default and would spuriously reject writes. Subsequent renewals run in
// RunLease.
func (m *Membership) FirstLeaseTick(ctx context.Context) {
	lg := log.New("ha:lease").With(log.String("instanceId", m.instanceID))
	m.leaseTick(ctx, lg)
}

// leaseTick performs one acquire/renew attempt and reconciles the resulting role.
func (m *Membership) leaseTick(ctx context.Context, lg log.Logger) {
	held, term, err := m.tryAcquireOrRenew(ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return
		}

		// An acquire/renew error is not authoritative about lease ownership.
		// Fail safe: demote, since we can no longer prove we hold the lease.
		lg.Error(err, "acquire or renew lease")
		_, lastTerm := m.CurrentRole()
		m.reconcileRole(RoleStandby, lastTerm, lg)

		return
	}

	if held {
		m.reconcileRole(RoleActive, term, lg)

		return
	}

	m.reconcileRole(RoleStandby, term, lg)
}

// reconcileRole records the role/term (the member is the single source of truth)
// and emits a RoleChange when the role actually transitions. Term-only changes
// while staying in the same role (ordinary renewals) do not emit.
func (m *Membership) reconcileRole(role Role, term int64, lg log.Logger) {
	if !m.SetRole(role, term) {
		return
	}

	lg.With(log.String("role", string(role)), log.Int64("term", term)).
		Info("Role transition")

	m.emitRoleChange(RoleChange{Role: role, Term: term})
}

// tryAcquireOrRenew atomically acquires or renews the lease. It first tries to
// take or renew an existing lease; if no document matches the take/renew filter
// it bootstraps the lease with an insert, then immediately re-renews it. A loser
// (another instance holds an unexpired lease) is reported as held=false with a
// nil error.
//
// Two-step design, and why bootstrap is not a single upsert:
//   - Take/renew is a non-upsert update whose filter uses a server-clock $expr
//     predicate ("I own it OR it has expired"). MongoDB forbids $expr in the
//     query predicate of an upsert, so this cannot be folded into an upsert.
//   - Bootstrap is an insert-only write (not an upsert) on purpose: the
//     duplicate-key error is exactly how a losing contender is detected when the
//     lease already exists and is held by another instance. An upsert would
//     instead overwrite the current owner's lease and break the single-active
//     guarantee.
//
// Because the insert cannot use the server clock ($$NOW is only valid in update
// pipelines), the bootstrap stamps timestamps with the client clock. Every other
// part of the protocol compares against the server clock, so a fresh leader
// immediately re-renews to convert those provisional client timestamps into
// server-stamped ones (see the re-renew step below).
func (m *Membership) tryAcquireOrRenew(ctx context.Context) (bool, int64, error) {
	ctx, cancel := context.WithTimeout(ctx, config.HeartbeatTimeout)
	defer cancel()

	// Step 1 (common path): conditionally take or renew an existing lease.
	held, term, matched, err := m.tryTakeOrRenewExisting(ctx)
	if err != nil {
		return false, 0, err
	}
	if matched {
		return held, term, nil
	}

	// Step 2: no document matched. Either the lease does not exist yet
	// (bootstrap) or it is held by another instance. Attempt the bootstrap
	// insert; a duplicate-key collision means it already exists and is owned by
	// someone else with an unexpired lease -> we lose.
	won, term, err := m.tryInsertLease(ctx)
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return false, 0, nil
		}

		return false, 0, errors.Wrap(err, "bootstrap lease")
	}

	held, renewedTerm, matched, err := m.tryTakeOrRenewExisting(ctx)
	if err != nil {
		return false, 0, err
	}
	if matched {
		return held, renewedTerm, nil
	}

	return won, term, nil
}

// tryInsertLease creates the lease document for the first time at term 1. It
// inserts only when no lease exists; a duplicate-key error (returned unwrapped)
// means the document already exists and the caller should treat it as a loss.
// Insert-only (not upsert) is deliberate: it is how a losing contender is
// detected when the lease is already held by another instance.
//
// The client-stamped timestamps here are provisional bootstrap values. The
// caller re-renews immediately after a successful bootstrap so expiresAt and
// electionDate are re-stamped against the server clock ($$NOW); this avoids
// client/server clock skew making the bootstrap expiresAt inconsistent with
// subsequent server-stamped renewals.
func (m *Membership) tryInsertLease(ctx context.Context) (bool, int64, error) {
	now := time.Now().UTC()

	// Retried on transient errors. A duplicate-key error is not transient, so it
	// is returned immediately and unwrapped for the caller's IsDuplicateKeyError
	// check.
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
	if err != nil {
		return false, 0, err //nolint:wrapcheck // caller inspects for duplicate-key
	}

	return true, 1, nil
}

// tryTakeOrRenewExisting conditionally updates an existing lease document. The
// filter (server-clock $expr) matches only when this instance already owns the
// lease or the lease has expired. The matched return reports whether any
// document matched the filter; when false, the lease either does not exist or is
// held by another instance (the caller disambiguates).
func (m *Membership) tryTakeOrRenewExisting(ctx context.Context) (bool, int64, bool, error) {
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
		updated Lease
		matched bool
	)

	// ErrNoDocuments is a normal "filter did not match" signal (lease absent or
	// held by another instance), not a failure to retry, so it is absorbed inside
	// the closure. Other errors are retried on transient classification.
	err := mdb.RunWithRetry(ctx, func(ctx context.Context) error {
		decodeErr := m.leaseColl().FindOneAndUpdate(
			ctx, filter, pipeline,
			options.FindOneAndUpdate().SetReturnDocument(options.After),
		).Decode(&updated)
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
		return false, 0, false, errors.Wrap(err, "take or renew lease")
	}

	if !matched {
		return false, 0, false, nil
	}

	return updated.InstanceID == m.instanceID, updated.Term, true, nil
}

// releaseLease best-effort clears the lease so a standby can take over without
// waiting for it to expire. It only clears the lease when this instance still
// owns it, then reflects the demotion in the member.
func (m *Membership) releaseLease(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, config.HeartbeatTimeout)
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

	// Reflect the demotion in the single source of truth, preserving the term.
	_, term := m.CurrentRole()
	m.SetRole(RoleStandby, term)

	return errors.Wrap(err, "release lease")
}

func (m *Membership) leaseColl() *mongo.Collection {
	return m.target.Database(config.PCSMDatabase).Collection(config.LeaseCollection)
}

// DeleteLease clears the lease collection. Used by reset. It removes all
// documents (not just the current LeaseID) so a lease written under a previous
// _id scheme is also cleared.
func DeleteLease(ctx context.Context, target *mongo.Client) error {
	err := mdb.RunWithRetry(ctx, func(ctx context.Context) error {
		_, err := target.Database(config.PCSMDatabase).
			Collection(config.LeaseCollection).
			DeleteMany(ctx, bson.D{})

		return err //nolint:wrapcheck
	}, mdb.DefaultRetryInterval, mdb.DefaultMaxRetries)

	return errors.Wrap(err, "delete lease")
}
