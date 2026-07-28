package ha

import (
	"context"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/percona/percona-clustersync-mongodb/config"
	"github.com/percona/percona-clustersync-mongodb/errors"
	"github.com/percona/percona-clustersync-mongodb/log"
	"github.com/percona/percona-clustersync-mongodb/mdb"
)

// MembershipOptions configures this instance's participation in the set.
type MembershipOptions struct {
	// InstanceID uniquely identifies this PCSM process. If empty, a random
	// UUID is generated.
	InstanceID string
	// Host is the host the instance is reachable on (advisory; used for the
	// cluster member list).
	Host string
	// Port is the HTTP server port (advisory; used for the cluster member list).
	Port int
	// PCSMVersion is the build version reported in the member document.
	PCSMVersion string
	// Group is the logical name of the active-standby group this instance joins.
	Group string
}

// Membership is this instance's participation in the HA set. It owns the
// instance identity and refreshes the instance's member document on a timer so
// other instances can discover it and detect liveness. Election state lives in
// the lease document.
type Membership struct {
	target     *mongo.Client
	instanceID string
	host       string
	port       int
	version    string
	group      string
	startedAt  time.Time
	cancel     context.CancelFunc

	// mu guards role, term, and leaseCancel.
	mu sync.Mutex
	// leaseCancel cancels the lease loop started by RunLease.
	leaseCancel context.CancelFunc
	// role and term change together on a role transition.
	role Role
	term Term

	// beatNow signals the refresh loop to write an immediate heartbeat so a
	// role change lands in the member document without waiting for the next tick.
	beatNow chan struct{}

	// roleChangeCh delivers role transitions. Cap-1 and coalescing: a pending
	// change is replaced by a newer one, so a slow consumer sees the latest.
	roleChangeCh chan RoleChange
}

// NewInstanceID returns a fresh random instance identifier.
func NewInstanceID() string {
	return "pcsm-" + uuid.NewString()
}

// JoinMembership writes the initial member document and starts the periodic
// refresh loop, joining this instance to the set. The returned Membership's Stop
// method cancels the loop and removes the member document.
func JoinMembership(ctx context.Context, target *mongo.Client, opts MembershipOptions) (*Membership, error) {
	instanceID := opts.InstanceID
	if instanceID == "" {
		instanceID = NewInstanceID()
	}

	host := opts.Host
	if host == "" {
		host, _ = os.Hostname()
	}

	m := &Membership{
		target:       target,
		instanceID:   instanceID,
		host:         host,
		port:         opts.Port,
		version:      opts.PCSMVersion,
		group:        opts.Group,
		startedAt:    time.Now().UTC(),
		role:         RoleStandby,
		term:         0,
		beatNow:      make(chan struct{}, 1),
		roleChangeCh: make(chan RoleChange, 1),
	}

	err := m.beat(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "initial heartbeat")
	}

	loopCtx, cancel := context.WithCancel(ctx)
	m.cancel = cancel

	go m.run(loopCtx)

	log.New("ha:membership").With(log.String("instanceId", instanceID)).Info("Joined membership")

	return m, nil
}

// InstanceID returns this instance's identifier.
func (m *Membership) InstanceID() string { return m.instanceID }

// Group returns the HA group name this instance joined.
func (m *Membership) Group() string { return m.group }

// SetRole updates the advertised role and term and triggers an immediate
// heartbeat so the member document reflects the change without waiting for the
// next tick. It returns true when the role actually transitioned. Membership is
// the single source of truth for (role, term); the election layer drives all
// transitions through here.
func (m *Membership) SetRole(role Role, term Term) bool {
	m.mu.Lock()
	transitioned := m.role != role
	m.role = role
	m.term = term
	m.mu.Unlock()

	// Non-blocking nudge; if a beat is already pending, this is a no-op.
	select {
	case m.beatNow <- struct{}{}:
	default:
	}

	return transitioned
}

// CurrentRole returns the role and term this instance currently advertises.
func (m *Membership) CurrentRole() (Role, Term) {
	return m.currentRole()
}

// RunLease runs the lease loop, through which this member competes to be
// ACTIVE, until ctx is canceled or Release is called. Role transitions are
// published to the member document and mirrored on RoleChanges.
func (m *Membership) RunLease(ctx context.Context) {
	loopCtx, cancel := context.WithCancel(ctx)

	m.mu.Lock()
	m.leaseCancel = cancel
	m.mu.Unlock()

	m.runLease(loopCtx)
}

// Release stops the lease loop, best-effort releases the lease (if held) so a
// standby can take over without waiting for the TTL, and demotes this member.
// Use it on shutdown; the instance no longer competes afterwards.
func (m *Membership) Release(ctx context.Context) error {
	m.mu.Lock()
	cancel := m.leaseCancel
	m.mu.Unlock()

	if cancel != nil {
		cancel()
	}

	return m.releaseLease(ctx)
}

// RelinquishLease gives up the lease (if held) and demotes to STANDBY without
// stopping the lease loop, so the instance keeps competing. Use it when an
// instance cannot currently act as ACTIVE (e.g. failed to recover on promotion)
// but should remain a viable standby.
func (m *Membership) RelinquishLease(ctx context.Context) error {
	return m.releaseLease(ctx)
}

// RoleChanges returns the channel on which role transitions are delivered.
// Coalescing (cap 1): the consumer always sees the latest transition.
func (m *Membership) RoleChanges() <-chan RoleChange { return m.roleChangeCh }

// emitRoleChange delivers a role change, replacing any pending one so the
// consumer observes the latest transition.
func (m *Membership) emitRoleChange(rc RoleChange) {
	for {
		select {
		case m.roleChangeCh <- rc:
			return
		default:
			// Drop the stale pending change, then retry. The drain may race
			// with the consumer; the loop converges because at most one value
			// is ever buffered.
			select {
			case <-m.roleChangeCh:
			default:
			}
		}
	}
}

func (m *Membership) currentRole() (Role, Term) {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.role, m.term
}

func (m *Membership) run(ctx context.Context) {
	lg := log.New("ha:membership")

	ticker := time.NewTicker(config.MemberHeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			lg.Info("Membership heartbeat canceled")

			return

		case <-ticker.C:
		case <-m.beatNow:
		}

		err := m.beat(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}

			lg.Error(err, "beat")
		}
	}
}

// beat upserts this instance's member document, stamping lastHeartbeat with the
// server clock ($$NOW) so staleness checks do not depend on client clocks.
func (m *Membership) beat(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, config.HAOperationTimeout)
	defer cancel()

	role, term := m.currentRole()

	// Aggregation-pipeline update so $$NOW resolves on the server.
	update := mongo.Pipeline{
		{{"$set", bson.D{
			{fieldGroup, m.group},
			{fieldHost, m.host},
			{fieldPort, m.port},
			{fieldRole, role},
			{fieldTerm, term},
			{fieldPCSMVersion, m.version},
			{fieldStartedAt, m.startedAt},
			{fieldLastHeartbeat, aggNow},
		}}},
	}

	err := mdb.RunWithRetry(ctx, func(ctx context.Context) error {
		_, err := membersColl(m.target).UpdateOne(ctx,
			bson.D{{"_id", m.instanceID}},
			update,
			options.UpdateOne().SetUpsert(true),
		)

		return err //nolint:wrapcheck
	}, mdb.DefaultRetryInterval, mdb.DefaultMaxRetries)

	return errors.Wrap(err, "heartbeat")
}

// Stop cancels the refresh loop and removes this instance's member document,
// leaving the set.
func (m *Membership) Stop(ctx context.Context) error {
	if m.cancel != nil {
		m.cancel()
	}

	err := mdb.RunWithRetry(ctx, func(ctx context.Context) error {
		_, err := membersColl(m.target).DeleteOne(ctx, bson.D{{"_id", m.instanceID}})

		return err //nolint:wrapcheck
	}, mdb.DefaultRetryInterval, mdb.DefaultMaxRetries)

	return errors.Wrap(err, "delete member")
}

// Members returns the live members: documents whose lastHeartbeat is within the
// stale threshold, evaluated against the server clock ($$NOW).
func Members(ctx context.Context, target *mongo.Client) ([]Member, error) {
	pipeline := mongo.Pipeline{
		{{"$match", bson.D{
			{"$expr", bson.D{
				{"$gte", bson.A{
					"$" + fieldLastHeartbeat,
					bson.D{{"$subtract", bson.A{aggNow, config.StaleMemberDuration.Milliseconds()}}},
				}},
			}},
		}}},
	}

	members, err := mdb.RunWithRetryVal(ctx, func(ctx context.Context) ([]Member, error) {
		cur, err := membersColl(target).Aggregate(ctx, pipeline)
		if err != nil {
			return nil, errors.Wrap(err, "aggregate members")
		}

		var out []Member
		err = cur.All(ctx, &out)
		if err != nil {
			return nil, errors.Wrap(err, "decode members")
		}

		return out, nil
	}, mdb.DefaultRetryInterval, mdb.DefaultMaxRetries)

	return members, errors.Wrap(err, "list members")
}

func membersColl(target *mongo.Client) *mongo.Collection {
	return target.Database(config.PCSMDatabase).Collection(config.MembersCollection)
}

// DeleteMembers removes all member documents. Used by reset.
func DeleteMembers(ctx context.Context, target *mongo.Client) error {
	err := mdb.RunWithRetry(ctx, func(ctx context.Context) error {
		_, err := membersColl(target).DeleteMany(ctx, bson.D{})

		return err //nolint:wrapcheck
	}, mdb.DefaultRetryInterval, mdb.DefaultMaxRetries)

	return errors.Wrap(err, "delete members")
}
