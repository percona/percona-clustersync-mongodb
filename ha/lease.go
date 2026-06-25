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

	// Attempt immediately so the first role is established without waiting a tick.
	m.leaseTick(ctx, lg)

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
// it attempts a bootstrap insert. A loser (another instance holds an unexpired
// lease) is reported as held=false with a nil error.
//
// MongoDB forbids $expr in the query predicate of an upsert, so take/renew (a
// non-upsert update with a server-clock predicate) and bootstrap (a plain
// insert) are kept as separate steps rather than a single upsert.
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
	if err == nil {
		return won, term, nil
	}
	if mongo.IsDuplicateKeyError(err) {
		return false, 0, nil
	}

	return false, 0, errors.Wrap(err, "bootstrap lease")
}

// tryInsertLease creates the lease document for the first time at term 1. It
// inserts only when no lease exists; a duplicate-key error (returned unwrapped)
// means the document already exists and the caller should treat it as a loss.
func (m *Membership) tryInsertLease(ctx context.Context) (bool, int64, error) {
	_, err := m.leaseColl().InsertOne(ctx, bson.D{
		{"_id", LeaseID},
		{"group", m.group},
		{"activeId", m.instanceID},
		{"term", int64(1)},
		// electionDate/expiresAt are stamped relative to insertion time. The next
		// renew tick re-stamps expiresAt against the server clock; this initial
		// value only needs to be in the future, which LeaseTTL guarantees.
		{"electionDate", time.Now().UTC()},
		{"expiresAt", time.Now().UTC().Add(config.LeaseTTL)},
	})
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
	isRenew := bson.D{{"$eq", bson.A{"$activeId", m.instanceID}}}

	pipeline := mongo.Pipeline{
		{{"$set", bson.D{
			{"group", m.group},
			{"activeId", m.instanceID},
			{"expiresAt", bson.D{{"$add", bson.A{"$$NOW", ttlMS}}}},
			{"electionDate", bson.D{{"$cond", bson.D{
				{"if", isRenew},
				{"then", bson.D{{"$ifNull", bson.A{"$electionDate", "$$NOW"}}}},
				{"else", "$$NOW"},
			}}}},
			{"term", bson.D{{"$cond", bson.D{
				{"if", isRenew},
				{"then", bson.D{{"$ifNull", bson.A{"$term", int64(0)}}}},
				{"else", bson.D{{"$add", bson.A{
					bson.D{{"$ifNull", bson.A{"$term", int64(0)}}},
					int64(1),
				}}}},
			}}}},
		}}},
	}

	// Filter without upsert may use $expr/$$NOW: take when we own it or it expired.
	filter := bson.D{{"_id", LeaseID}, {"$expr", bson.D{{"$or", bson.A{
		bson.D{{"$eq", bson.A{"$activeId", m.instanceID}}},
		bson.D{{"$lte", bson.A{"$expiresAt", "$$NOW"}}},
	}}}}}

	var updated Lease

	decodeErr := m.leaseColl().FindOneAndUpdate(ctx, filter, pipeline,
		options.FindOneAndUpdate().SetReturnDocument(options.After),
	).Decode(&updated)
	if decodeErr != nil {
		if errors.Is(decodeErr, mongo.ErrNoDocuments) {
			// Filter did not match: lease is absent or held by another instance.
			return false, 0, false, nil
		}

		return false, 0, false, errors.Wrap(decodeErr, "take or renew lease")
	}

	return updated.ActiveID == m.instanceID, updated.Term, true, nil
}

// releaseLease best-effort clears the lease so a standby can take over without
// waiting for it to expire. It only clears the lease when this instance still
// owns it, then reflects the demotion in the member.
func (m *Membership) releaseLease(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, config.HeartbeatTimeout)
	defer cancel()

	_, err := m.leaseColl().UpdateOne(ctx,
		bson.D{{"_id", LeaseID}, {"activeId", m.instanceID}},
		mongo.Pipeline{
			{{"$set", bson.D{{"expiresAt", "$$NOW"}}}},
		},
	)

	// Reflect the demotion in the single source of truth, preserving the term.
	_, term := m.CurrentRole()
	m.SetRole(RoleStandby, term)

	return errors.Wrap(err, "release lease")
}

func (m *Membership) leaseColl() *mongo.Collection {
	return m.target.Database(config.PCSMDatabase).Collection(config.LeaseCollection)
}

// DeleteLease removes the lease document. Used by reset.
func DeleteLease(ctx context.Context, target *mongo.Client) error {
	_, err := target.Database(config.PCSMDatabase).
		Collection(config.LeaseCollection).
		DeleteOne(ctx, bson.D{{"_id", LeaseID}})

	return err //nolint:wrapcheck
}
