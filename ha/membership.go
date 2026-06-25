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

// Membership represents this instance's participation in the HA set. It owns the
// instance identity and maintains the instance's document in the members
// collection, refreshing it on a timer so other instances can discover it and
// detect liveness. It is identity/liveness only; election lives in the lease
// document.
type Membership struct {
	target     *mongo.Client
	instanceID string
	host       string
	port       int
	version    string
	group      string
	startedAt  time.Time
	cancel     context.CancelFunc

	// leaseCancel cancels the lease loop started by RunLease.
	leaseCancel context.CancelFunc

	// mu guards role and term, which change together on a role transition.
	mu   sync.Mutex
	role Role
	term int64

	// beatNow signals the refresh loop to write an immediate heartbeat,
	// used so a role change is reflected in the member document promptly
	// instead of waiting up to MemberHeartbeatInterval.
	beatNow chan struct{}

	// roleChangeCh delivers role transitions to a consumer. It is buffered at cap 1
	// and coalescing: a pending change is overwritten by a newer one so a slow
	// consumer always observes the latest transition rather than a stale queue.
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
		startedAt:    time.Now(),
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

// SetRole updates the role and term this instance advertises in its member
// document. It is the one-way feed from the election layer (the lease document
// is authoritative): the elected role is pushed here, and the heartbeat loop
// publishes it. It triggers an immediate heartbeat so the change is reflected in
// the member list without waiting for the next tick. It returns true when the
// role actually transitioned (a different role than before), so the caller can
// react to transitions while ignoring ordinary same-role renewals.
//
// Membership is the single source of truth for (role, term): the election layer
// drives transitions through here rather than tracking role/term separately.
func (m *Membership) SetRole(role Role, term int64) bool {
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
func (m *Membership) CurrentRole() (Role, int64) {
	return m.currentRole()
}

// RunLease starts the lease loop, through which this member competes for the
// lease and gains (or loses) the right to be ACTIVE. It runs until ctx is
// canceled or Release is called. Role transitions are published to the member
// document and mirrored on RoleChanges.
func (m *Membership) RunLease(ctx context.Context) {
	loopCtx, cancel := context.WithCancel(ctx)
	m.leaseCancel = cancel

	m.runLease(loopCtx)
}

// Release best-effort relinquishes the lease (if held) so a standby can take
// over without waiting for it to expire, demotes this member to STANDBY, and
// stops the lease loop.
func (m *Membership) Release(ctx context.Context) error {
	if m.leaseCancel != nil {
		m.leaseCancel()
	}

	return m.releaseLease(ctx)
}

// RoleChanges returns the channel on which role transitions are delivered. The
// channel is coalescing (cap 1): the consumer always sees the latest transition.
func (m *Membership) RoleChanges() <-chan RoleChange { return m.roleChangeCh }

// emitRoleChange delivers a role change on the coalescing cap-1 channel. If a
// change is already pending it is replaced so the consumer observes the latest
// transition.
func (m *Membership) emitRoleChange(rc RoleChange) {
	for {
		select {
		case m.roleChangeCh <- rc:
			return
		default:
			// Drop the stale pending change, then retry the send. The drain may
			// race with the consumer; the loop converges because at most one
			// value is ever buffered.
			select {
			case <-m.roleChangeCh:
			default:
			}
		}
	}
}

func (m *Membership) currentRole() (Role, int64) {
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

// beat upserts this instance's member document with the currently advertised
// role and term, stamping lastHeartbeat with the server-side clock ($$NOW) to
// avoid relying on client wall-clock time.
func (m *Membership) beat(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, config.HeartbeatTimeout)
	defer cancel()

	role, term := m.currentRole()

	// Aggregation-pipeline update so $$NOW resolves on the server.
	update := mongo.Pipeline{
		{{"$set", bson.D{
			{"host", m.host},
			{"port", m.port},
			{"role", role},
			{"term", term},
			{"pcsmVersion", m.version},
			{"startedAt", m.startedAt},
			{"lastHeartbeat", "$$NOW"},
		}}},
	}

	_, err := membersColl(m.target).UpdateOne(ctx,
		bson.D{{"_id", m.instanceID}},
		update,
		options.UpdateOne().SetUpsert(true),
	)

	return err //nolint:wrapcheck
}

// Stop cancels the refresh loop and removes this instance's member document,
// leaving the set.
func (m *Membership) Stop(ctx context.Context) error {
	if m.cancel != nil {
		m.cancel()
	}

	_, err := membersColl(m.target).DeleteOne(ctx, bson.D{{"_id", m.instanceID}})

	return err //nolint:wrapcheck
}

// Members returns the current set of live members, filtering out documents whose
// lastHeartbeat is older than the stale threshold. Staleness is evaluated using
// the server clock via an aggregation match against $$NOW.
func Members(ctx context.Context, target *mongo.Client) ([]Member, error) {
	pipeline := mongo.Pipeline{
		{{"$match", bson.D{
			{"$expr", bson.D{
				{"$gte", bson.A{
					"$lastHeartbeat",
					bson.D{{"$subtract", bson.A{"$$NOW", config.StaleMemberDuration.Milliseconds()}}},
				}},
			}},
		}}},
	}

	cur, err := membersColl(target).Aggregate(ctx, pipeline)
	if err != nil {
		return nil, errors.Wrap(err, "aggregate members")
	}

	var members []Member
	err = cur.All(ctx, &members)
	if err != nil {
		return nil, errors.Wrap(err, "decode members")
	}

	return members, nil
}

func membersColl(target *mongo.Client) *mongo.Collection {
	return target.Database(config.PCSMDatabase).Collection(config.MembersCollection)
}

// DeleteMembers removes all member documents. Used by reset.
func DeleteMembers(ctx context.Context, target *mongo.Client) error {
	_, err := membersColl(target).DeleteMany(ctx, bson.D{})

	return err //nolint:wrapcheck
}
