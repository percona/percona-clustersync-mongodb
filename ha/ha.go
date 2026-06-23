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

const (
	// RoleActive indicates the instance currently holds the lease and performs replication.
	RoleActive Role = "ACTIVE"
	// RoleStandby indicates the instance is passive and ready to take over on failover.
	RoleStandby Role = "STANDBY"
)

// LeaseID is the fixed _id of the single lease document in the lease collection.
const LeaseID = "active"

// Member is a per-instance liveness/identity document stored in the members
// collection (one document per instanceId). It carries no authoritative election
// state; Term here is informational and the lease document is the source of truth.
type Member struct {
	InstanceID    string    `bson:"_id"`
	Host          string    `bson:"host"`
	Port          int       `bson:"port"`
	Role          Role      `bson:"role"`
	Term          int64     `bson:"term"`
	PCSMVersion   string    `bson:"pcsmVersion"`
	StartedAt     time.Time `bson:"startedAt"`
	LastHeartbeat time.Time `bson:"lastHeartbeat"`
}

// Lease is the single active-standby lease document used for election and
// term-based fencing. The active instance renews ExpiresAt on a timer; a standby
// may win election once ExpiresAt has passed (compared against server-side $$NOW).
type Lease struct {
	ID           string    `bson:"_id"`
	SetName      string    `bson:"setName"`
	Term         int64     `bson:"term"`
	ActiveID     string    `bson:"activeId"`
	ElectionDate time.Time `bson:"electionDate"`
	ExpiresAt    time.Time `bson:"expiresAt"`
}
