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
	IdleState       = "idle"
	RunningState    = "running"
	FinalizingState = "finalizing"
	FinalizedState  = "finalized"
)

type CloneStatus struct {
	Finished             bool
	EstimatedTotalBytes  int64
	EstimatedClonedBytes int64
}

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

	repl      *ChangeReplicator
	startedAt primitive.Timestamp
	clonedAt  primitive.Timestamp

	state State

	cloner *DataCloner

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

	if c.state != IdleState && c.state != FinalizedState {
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

	go func() {
		ctx := log.CopyContext(ctx, context.Background())
		err := c.run(ctx)
		if err != nil {
			log.Error(ctx, err, "run")
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

	catalog := NewCatalog()
	cloner := &DataCloner{
		Source:   c.source,
		Target:   c.target,
		Drop:     c.drop,
		NSFilter: c.nsFilter,
		Catalog:  catalog,
	}
	c.cloner = cloner

	err = cloner.Clone(ctx)
	if err != nil {
		return errors.Wrap(err, "close")
	}

	clonedAt, err := topo.ClusterTime(ctx, c.source)
	if err != nil {
		return errors.Wrap(err, "get cluster time")
	}

	repl := &ChangeReplicator{
		Source:   c.source,
		Target:   c.target,
		Drop:     c.drop,
		NSFilter: c.nsFilter,
		Catalog:  catalog,
	}

	c.mu.Lock()
	c.clonedAt = clonedAt
	c.repl = repl
	c.mu.Unlock()

	log.Infof(ctx, "starting change replication at %d.%d", startedAt.T, startedAt.I)
	err = repl.Start(ctx, startedAt)
	if err != nil {
		return errors.Wrap(err, "start change replication")
	}

	err = repl.Wait()
	if err != nil {
		return errors.Wrap(err, "change replication")
	}

	err = catalog.FinalizeIndexes(ctx, c.target)
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

	if c.repl == nil || c.repl.GetLastAppliedOpTime().Before(c.clonedAt) {
		return errors.New("not ready")
	}

	c.repl.Pause()

	c.state = FinalizingState
	log.Info(ctx, "finalizing")
	return nil
}

func (c *Coordinator) Status(ctx context.Context) (*Status, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	s := &Status{
		State: c.state,
		Info:  "waiting for start",
	}

	if c.repl != nil {
		optime := c.repl.GetLastAppliedOpTime()
		s.Finalizable = !optime.Before(c.clonedAt)
		s.LastAppliedOpTime = optime
		s.EventsProcessed = c.repl.EventsProcessed
		s.Info = "replicating changes"
	}

	if c.cloner != nil {
		s.Clone = CloneStatus{
			Finished:             c.cloner.Finished,
			EstimatedTotalBytes:  c.cloner.EstimatedTotalBytes.Load(),
			EstimatedClonedBytes: c.cloner.EstimatedClonedBytes.Load(),
		}

		if c.cloner.Finished {
			s.Info = "cloning data"
		}
	}

	return s, nil
}
