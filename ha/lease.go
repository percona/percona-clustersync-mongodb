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

// Ownership is what one lease attempt established: whether this instance
// holds the lease and, when it does, at which term. The lease loop reports it
// after every attempt that settles it, as a level, not a transition: an
// observer reconciles against the latest ownership and never depends on
// having seen every change. A canceled attempt, or a failed one while the
// lease is held within its deadline, settles nothing and reports nothing.
type Ownership struct {
	Held bool
	Term Term
}

// Observer receives the ownership each lease attempt established. The lease
// loop calls it synchronously, so the next attempt waits for it to return and
// an observer that ends work on a loss has done so before the next renewal.
type Observer func(Ownership)

// runLease tries to acquire or renew the lease on every tick and reports the
// resulting ownership, until ctx is canceled. Holding the lease grants the
// right to be ACTIVE. MongoDB uses its server clock ($$NOW) for lease expiry.
// A local monotonic deadline bounds how long a holder can tolerate renewal
// errors before it reports the lease lost.
func (m *Membership) runLease(ctx context.Context, observe Observer) {
	lg := log.New("ha:lease").With(log.String("instanceId", m.instanceID))

	ticker := time.NewTicker(config.LeaseRenewInterval)
	defer ticker.Stop()

	expiry := time.NewTimer(0)
	defer expiry.Stop()

	for {
		var expiryCh <-chan time.Time
		if m.held {
			expiry.Reset(time.Until(m.leaseDeadline))
			expiryCh = expiry.C
		}

		select {
		case <-ctx.Done():
			lg.Info("Lease loop canceled")

			return

		case <-ticker.C:
			m.leaseTick(ctx, lg, observe)

		case <-expiryCh:
			lg.Warn("Lease deadline passed without a successful renewal")
			m.report(Ownership{}, observe)
		}
	}
}

// FirstLeaseTick performs one synchronous acquire/renew attempt and reports
// its outcome. Called once at startup, before the HTTP server serves requests,
// so ownership is settled by then: without it a fresh instance would briefly
// report the STANDBY default and spuriously reject writes. Subsequent attempts
// run in RunLease.
func (m *Membership) FirstLeaseTick(ctx context.Context, observe Observer) {
	lg := log.New("ha:lease").With(log.String("instanceId", m.instanceID))
	m.leaseTick(ctx, lg, observe)
}

// leaseAttempt is the outcome of a single acquire/renew attempt. Term is
// meaningful only when Acquired is true.
type leaseAttempt struct {
	Acquired bool
	Term     Term
}

// leaseTick performs one acquire/renew attempt and reports the ownership it
// established. A failed attempt while the lease is held and the local deadline
// has not passed reports nothing: ownership is unchanged until the deadline.
func (m *Membership) leaseTick(ctx context.Context, lg log.Logger, observe Observer) {
	if m.held {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, m.leaseDeadline)
		defer cancel()
	}

	// Starting before the RPC makes this bound no later than server expiry,
	// without comparing the local wall clock to the server clock.
	sentAt := time.Now()
	att, err := m.acquire(ctx)
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return
		}

		if m.held && time.Now().Before(m.leaseDeadline) {
			lg.With(log.String("error", err.Error()),
				log.String("leaseDeadline", m.leaseDeadline.Format(time.RFC3339Nano))).
				Warn("acquire or renew lease; retaining the lease within its deadline")

			return
		}

		lg.Error(err, "acquire or renew lease")
		m.report(Ownership{}, observe)

		return
	}

	if att.Acquired {
		m.leaseDeadline = sentAt.Add(config.LeaseTTL)
		m.report(Ownership{Held: true, Term: att.Term}, observe)

		return
	}

	m.report(Ownership{}, observe)
}

// report records the ownership the lease loop established and hands it to
// the observer. Only the lease loop (and FirstLeaseTick before it starts)
// calls it.
func (m *Membership) report(o Ownership, observe Observer) {
	m.held = o.Held
	observe(o)
}

// tryAcquireOrRenew acquires or renews the lease. Losing to another instance's
// unexpired lease is a non-acquired attempt with a nil error.
//
// Two steps, because MongoDB forbids $expr in an upsert predicate:
//  1. Take/renew an existing lease with a non-upsert update whose server-clock
//     $expr filter is "I own it OR it has expired".
//  2. If nothing matched, bootstrap with an insert. Insert (not upsert) is
//     deliberate: a duplicate-key error is how a losing contender is detected,
//     without overwriting the current owner's lease.
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

	// Re-renew the freshly bootstrapped lease to finalize it with server-clock
	// timestamps; not acquiring means another instance took it in the meantime.
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
// client-clock timestamps are provisional; the caller re-renews immediately to
// re-stamp them with the server clock.
func (m *Membership) tryInsertLease(ctx context.Context) error {
	now := time.Now().UTC()

	// A duplicate-key error is not transient: returned immediately, unwrapped.
	err := mdb.RunWithRetry(ctx, func(ctx context.Context) error {
		_, err := m.leaseColl().InsertOne(ctx, bson.D{
			{"_id", LeaseID},
			{fieldGroup, m.group},
			{fieldInstanceID, m.instanceID},
			{fieldTerm, int64(1)},
			{fieldElectionDate, now},
			{fieldExpiresAt, now.Add(config.LeaseTTL)},
		})

		return err //nolint:wrapcheck
	}, mdb.DefaultRetryInterval, mdb.DefaultMaxRetries)

	return err //nolint:wrapcheck // caller inspects for duplicate-key
}

// tryTakeOrRenewExisting updates an existing lease when this instance owns it
// or it has expired (per the server clock). matched=false means the lease is
// absent or held by another instance; the caller disambiguates. A match is
// always an acquisition: the pipeline sets instanceId unconditionally.
func (m *Membership) tryTakeOrRenewExisting(ctx context.Context) (leaseAttempt, bool, error) {
	ttlMS := config.LeaseTTL.Milliseconds()

	// isRenew is true when this instance already owns the lease in the pre-image.
	isRenew := bson.D{{"$eq", bson.A{"$" + fieldInstanceID, m.instanceID}}}

	pipeline := mongo.Pipeline{
		{{"$set", bson.D{
			{fieldGroup, m.group},
			{fieldInstanceID, m.instanceID},
			{fieldExpiresAt, bson.D{{aggAdd, bson.A{aggNow, ttlMS}}}},
			{fieldElectionDate, bson.D{{"$cond", bson.D{
				{"if", isRenew},
				{"then", bson.D{{aggIfNull, bson.A{"$" + fieldElectionDate, aggNow}}}},
				{"else", aggNow},
			}}}},
			{fieldTerm, bson.D{{"$cond", bson.D{
				{"if", isRenew},
				{"then", bson.D{{aggIfNull, bson.A{"$" + fieldTerm, int64(0)}}}},
				{"else", bson.D{{aggAdd, bson.A{
					bson.D{{aggIfNull, bson.A{"$" + fieldTerm, int64(0)}}},
					int64(1),
				}}}},
			}}}},
		}}},
	}

	// Filter without upsert may use $expr/$$NOW: take when we own it or it expired.
	filter := bson.D{{"_id", LeaseID}, {"$expr", bson.D{{"$or", bson.A{
		bson.D{{"$eq", bson.A{"$" + fieldInstanceID, m.instanceID}}},
		bson.D{{"$lte", bson.A{"$" + fieldExpiresAt, aggNow}}},
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
// standby can take over without waiting for the TTL.
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

	return errors.Wrap(err, "release lease")
}

// AdvanceTerm raises the lease term to at least `to` while this instance owns
// the lease, and reports whether the lease is now owned at such a term. The
// next renewal returns the new term. A lease owned by another instance is
// left untouched.
//
// It gives a new ACTIVE tenure a term no earlier tenure used: a renewal after
// a loss keeps the term when nobody else took the lease in between, and a
// lease document reset while recovery data existed can sit below the
// checkpoint's fencing term. Both would otherwise let two tenures write under
// one term.
func (m *Membership) AdvanceTerm(ctx context.Context, to Term) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, config.HAOperationTimeout)
	defer cancel()

	var lease Lease

	err := mdb.RunWithRetry(ctx, func(ctx context.Context) error {
		err := m.leaseColl().FindOneAndUpdate(
			ctx,
			bson.D{{"_id", LeaseID}, {fieldInstanceID, m.instanceID}},
			mongo.Pipeline{
				{{"$set", bson.D{{fieldTerm, bson.D{{aggMax, bson.A{
					bson.D{{aggIfNull, bson.A{"$" + fieldTerm, int64(0)}}},
					int64(to),
				}}}}}}},
			},
			options.FindOneAndUpdate().SetReturnDocument(options.After),
		).Decode(&lease)
		if errors.Is(err, mongo.ErrNoDocuments) {
			// Not the owner: nothing to advance.
			lease = Lease{}

			return nil
		}

		return err //nolint:wrapcheck // RunWithRetry classifies the raw driver error
	}, mdb.DefaultRetryInterval, mdb.DefaultMaxRetries)
	if err != nil {
		return false, errors.Wrap(err, "advance lease term")
	}

	return lease.InstanceID == m.instanceID && lease.Term >= to, nil
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
