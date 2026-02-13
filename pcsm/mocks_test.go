package pcsm //nolint:testpackage

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/percona/percona-clustersync-mongodb/pcsm/clone"
	"github.com/percona/percona-clustersync-mongodb/pcsm/repl"
)

// mockCloner is a test double for the Cloner interface.
type mockCloner struct {
	doneCh           chan struct{}
	status           clone.Status
	checkpoint       *clone.Checkpoint
	recoverErr       error
	resetErrorCalled bool
}

func (m *mockCloner) Start(context.Context) error   { return nil }
func (m *mockCloner) Done() <-chan struct{}         { return m.doneCh }
func (m *mockCloner) Status() clone.Status          { return m.status }
func (m *mockCloner) Checkpoint() *clone.Checkpoint { return m.checkpoint }
func (m *mockCloner) Recover(*clone.Checkpoint) error {
	return m.recoverErr
}
func (m *mockCloner) ResetError() { m.resetErrorCalled = true }

// mockReplicator is a test double for the Replicator interface.
type mockReplicator struct {
	doneCh           chan struct{}
	startTime        time.Time
	pauseTime        time.Time
	lastOpTime       bson.Timestamp
	err              error
	checkpoint       *repl.Checkpoint
	pauseErr         error
	recoverErr       error
	resetErrorCalled bool
}

func (m *mockReplicator) Start(context.Context, bson.Timestamp) error { return nil }
func (m *mockReplicator) Pause(context.Context) error                 { return m.pauseErr }
func (m *mockReplicator) Resume(context.Context) error                { return nil }
func (m *mockReplicator) Done() <-chan struct{}                       { return m.doneCh }
func (m *mockReplicator) Status() repl.Status {
	return repl.Status{
		StartTime:            m.startTime,
		PauseTime:            m.pauseTime,
		LastReplicatedOpTime: m.lastOpTime,
		Err:                  m.err,
	}
}
func (m *mockReplicator) Checkpoint() *repl.Checkpoint { return m.checkpoint }
func (m *mockReplicator) Recover(*repl.Checkpoint) error {
	return m.recoverErr
}
func (m *mockReplicator) ResetError() { m.resetErrorCalled = true }
