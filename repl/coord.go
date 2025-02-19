package repl

import (
	"context"
	"sync"

	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/percona-lab/percona-mongolink/errors"
	"github.com/percona-lab/percona-mongolink/log"
	"github.com/percona-lab/percona-mongolink/topo"
)

type State string

const (
	FailedState     = "failed"
	IdleState       = "idle"
	RunningState    = "running"
	FinalizingState = "finalizing"
	FinalizedState  = "finalized"
)

type Status struct {
	State             State
	LastAppliedOpTime primitive.Timestamp
	Finalizable       bool
	Info              string
	EventsProcessed   int64

	Clone CloneStatus
}

type Coordinator struct {
	source *mongo.Client
	target *mongo.Client

	drop     bool
	nsFilter NSFilter

	state   State
	catalog *Catalog
	cloner  *DataCloner
	repl    *ChangeReplicator

	startedAt primitive.Timestamp
	clonedAt  primitive.Timestamp

	mu sync.Mutex
}

func New(source, target *mongo.Client) *Coordinator {
	r := &Coordinator{
		source: source,
		target: target,
		state:  IdleState,
	}
	return r
}

type StartOptions struct {
	DropBeforeCreate bool

	IncludeNamespaces []string
	ExcludeNamespaces []string
}

func (c *Coordinator) Start(ctx context.Context, options *StartOptions) error {
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
	c.nsFilter = makeFilter(options.IncludeNamespaces, options.ExcludeNamespaces)

	c.repl = nil
	c.startedAt = primitive.Timestamp{}
	c.clonedAt = primitive.Timestamp{}
	c.state = RunningState

	c.catalog = NewCatalog()
	c.cloner = &DataCloner{
		Source:   c.source,
		Target:   c.target,
		Drop:     c.drop,
		NSFilter: c.nsFilter,
		Catalog:  c.catalog,
	}

	c.repl = &ChangeReplicator{
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

func (c *Coordinator) run(ctx context.Context) error {
	ctx = log.WithAttrs(ctx, log.Scope("coord:run"))
	log.Info(ctx, "starting data cloning")

	startedAt, err := topo.ClusterTime(ctx, c.source)
	if err != nil {
		return errors.Wrap(err, "get cluster time")
	}

	c.mu.Lock()
	c.startedAt = startedAt
	c.mu.Unlock()

	err = c.cloner.Clone(ctx)
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

func (c *Coordinator) Finalize(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.state != RunningState {
		return errors.New(string(c.state))
	}

	cloneStatus := c.cloner.Status()
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

func (c *Coordinator) Status(ctx context.Context) (*Status, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	s := &Status{State: c.state}

	if c.state == IdleState {
		return s, nil
	}

	cloneStatus := c.cloner.Status()
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
