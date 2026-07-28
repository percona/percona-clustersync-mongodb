// Package ha provides active-standby high availability for PCSM during the
// replication phase. Multiple PCSM instances connect to the same target; one
// holds a MongoDB-backed lease and is ACTIVE while the rest stay STANDBY and
// take over on failover. The lease term doubles as a fencing token so a deposed
// active cannot corrupt the target after losing the lease.
package ha

import (
	"time"
)

// Role is the HA role of a PCSM instance within a set.
type Role string

// Term is the monotonic lease term. It grows across acquisitions and doubles as
// the fencing token stamped into checkpoint writes.
type Term int64

const (
	// RoleActive indicates the instance currently holds the lease and performs replication.
	RoleActive Role = "ACTIVE"
	// RoleStandby indicates the instance is passive and ready to take over on failover.
	RoleStandby Role = "STANDBY"
)

// LeaseID is the fixed _id of the single lease document in the lease collection.
const LeaseID = "lease"

// BSON field names of the lease and member documents.
const (
	fieldGroup         = "group"
	fieldInstanceID    = "instanceId"
	fieldTerm          = "term"
	fieldExpiresAt     = "expiresAt"
	fieldElectionDate  = "electionDate"
	fieldHost          = "host"
	fieldPort          = "port"
	fieldRole          = "role"
	fieldPCSMVersion   = "pcsmVersion"
	fieldStartedAt     = "startedAt"
	fieldLastHeartbeat = "lastHeartbeat"
)

// MongoDB aggregation variables/operators used in update pipelines.
const (
	// aggNow is the server-side current time, which keeps lease and heartbeat
	// comparisons independent of client clocks.
	aggNow    = "$$NOW"
	aggIfNull = "$ifNull"
	aggAdd    = "$add"
)

// Member is the per-instance liveness/identity document in the members
// collection. Role and Term are informational; the lease document is the
// source of truth for election state.
type Member struct {
	InstanceID    string    `bson:"_id"`
	Group         string    `bson:"group"`
	Host          string    `bson:"host"`
	Port          int       `bson:"port"`
	Role          Role      `bson:"role"`
	Term          Term      `bson:"term"`
	PCSMVersion   string    `bson:"pcsmVersion"`
	StartedAt     time.Time `bson:"startedAt"`
	LastHeartbeat time.Time `bson:"lastHeartbeat"`
}

// Lease is the single lease document used for election and term-based fencing.
// The active instance renews ExpiresAt on a timer; a standby may take over once
// ExpiresAt has passed (per the server clock).
type Lease struct {
	ID           string    `bson:"_id"`
	Group        string    `bson:"group"`
	Term         Term      `bson:"term"`
	InstanceID   string    `bson:"instanceId"`
	ElectionDate time.Time `bson:"electionDate"`
	ExpiresAt    time.Time `bson:"expiresAt"`
}
