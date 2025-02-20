package mongolink

import (
	"context"
	"sync"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/percona-lab/percona-mongolink/errors"
	"github.com/percona-lab/percona-mongolink/log"
	"github.com/percona-lab/percona-mongolink/sel"
	"github.com/percona-lab/percona-mongolink/topo"
)

// State represents the state of the mongolink.
type State string

const (
	FailedState     = "failed"
	IdleState       = "idle"
	RunningState    = "running"
	FinalizingState = "finalizing"
	FinalizedState  = "finalized"
)

// Status represents the status of the mongolink.
type Status struct {
	State             State          // Current state of the mongolink
	LastAppliedOpTime bson.Timestamp // Last applied operation time
	Finalizable       bool           // Indicates if the process can be finalized
	Info              string         // Additional information
	EventsProcessed   int64          // Number of events processed
	Clone             CloneStatus    // Status of the cloning process
}

// MongoLink manages the replication process.
type MongoLink struct {
	source *mongo.Client // Source MongoDB client
	target *mongo.Client // Target MongoDB client

	drop     bool         // Drop collections before creating them
	nsFilter sel.NSFilter // Namespace filter

	state   State    // Current state of the mongolink
	catalog *Catalog // Catalog for managing collections and indexes
	clone   *Clone   // Clone process
	repl    *Repl    // Replication process

	startedAt bson.Timestamp // Timestamp when the process started
	clonedAt  bson.Timestamp // Timestamp when the cloning finished

	mu sync.Mutex
}

// New creates a new MongoLink.
func New(source, target *mongo.Client) *MongoLink {
	r := &MongoLink{
		source: source,
		target: target,
		state:  IdleState,
	}
	return r
}

// StartOptions represents the options for starting the mongolink.
type StartOptions struct {
	DropBeforeCreate  bool     // Drop collections before creating them
	IncludeNamespaces []string // Namespaces to include
	ExcludeNamespaces []string // Namespaces to exclude
}

// Start starts the replication process with the given options.
func (c *MongoLink) Start(ctx context.Context, options *StartOptions) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	ctx = log.WithAttrs(ctx, log.Scope("coord:repl"))

	if c.state != IdleState && c.state != FinalizedState && c.state != FailedState {
		return errors.New(string(c.state))
	}

	if options == nil {
		options = &StartOptions{}
	}

	c.drop = options.DropBeforeCreate
	c.nsFilter = sel.MakeFilter(options.IncludeNamespaces, options.ExcludeNamespaces)

	c.repl = nil
	c.startedAt = bson.Timestamp{}
	c.clonedAt = bson.Timestamp{}
	c.state = RunningState

	c.catalog = NewCatalog()
	c.clone = &Clone{
		Source:   c.source,
		Target:   c.target,
		Drop:     c.drop,
		NSFilter: c.nsFilter,
		Catalog:  c.catalog,
	}

	c.repl = &Repl{
		Source:   c.source,
		Target:   c.target,
		Drop:     c.drop,
		NSFilter: c.nsFilter,
		Catalog:  c.catalog,
	}

	go func() {
		ctx := log.CopyContext(ctx, context.Background())
		err := c.run(ctx)
		if err != nil {
			c.mu.Lock()
			c.state = FailedState
			c.mu.Unlock()

			log.Error(ctx, err, "failed")
			return
		}

		c.mu.Lock()
		c.state = FinalizedState
		c.mu.Unlock()

		log.Info(ctx, "finalized")
	}()

	log.Info(ctx, "started")
	return nil
}

// run executes the replication process.
func (c *MongoLink) run(ctx context.Context) error {
	ctx = log.WithAttrs(ctx, log.Scope("coord:run"))
	log.Info(ctx, "starting data cloning")

	startedAt, err := topo.ClusterTime(ctx, c.source)
	if err != nil {
		return errors.Wrap(err, "get cluster time")
	}

	c.mu.Lock()
	c.startedAt = startedAt
	c.mu.Unlock()

	err = c.clone.Clone(ctx)
	if err != nil {
		return errors.Wrap(err, "close")
	}

	clonedAt, err := topo.ClusterTime(ctx, c.source)
	if err != nil {
		return errors.Wrap(err, "get cluster time")
	}

	c.mu.Lock()
	c.clonedAt = clonedAt
	c.mu.Unlock()

	log.Infof(ctx, "starting change replication at %d.%d", startedAt.T, startedAt.I)
	err = c.repl.Start(ctx, startedAt)
	if err != nil {
		return errors.Wrap(err, "start change replication")
	}

	err = c.repl.Wait()
	if err != nil {
		return errors.Wrap(err, "change replication")
	}

	err = c.catalog.FinalizeIndexes(ctx, c.target)
	if err != nil {
		return errors.Wrap(err, "finalize indexes")
	}

	return nil
}

// Finalize finalizes the replication process.
func (c *MongoLink) Finalize(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.state != RunningState {
		return errors.New(string(c.state))
	}

	cloneStatus := c.clone.Status()
	if !cloneStatus.Finished {
		return errors.New("clone has not finished")
	}

	replStatus := c.repl.Status()
	if replStatus.LastAppliedOpTime.IsZero() {
		return errors.New("repl has not been started")
	}
	if replStatus.LastAppliedOpTime.Before(c.clonedAt) {
		return errors.New("not finalizable")
	}

	c.repl.Pause()

	c.state = FinalizingState
	log.Info(ctx, "finalizing")
	return nil
}

// Status returns the current status of the mongolink.
func (c *MongoLink) Status(ctx context.Context) (*Status, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	s := &Status{State: c.state}

	if c.state == IdleState {
		return s, nil
	}

	cloneStatus := c.clone.Status()
	s.Clone = cloneStatus

	replStatus := c.repl.Status()
	s.LastAppliedOpTime = replStatus.LastAppliedOpTime
	s.EventsProcessed = replStatus.EventsProcessed
	if cloneStatus.Finished && !replStatus.LastAppliedOpTime.IsZero() {
		s.Finalizable = !replStatus.LastAppliedOpTime.Before(c.clonedAt)
	}

	switch {
	case c.state == IdleState:
		s.Info = "waiting for start"
	case c.state == RunningState && !cloneStatus.Finished:
		s.Info = "cloning data"
	case c.state == RunningState && !replStatus.LastAppliedOpTime.IsZero():
		s.Info = "replicating changes"
	}

	return s, nil
}
