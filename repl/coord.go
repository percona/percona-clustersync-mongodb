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

type Status struct {
	State             State
	Finalizable       bool
	LastAppliedOpTime primitive.Timestamp
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
	mu    sync.Mutex
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

func (r *Coordinator) Start(ctx context.Context, options *StartOptions) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	ctx = log.WithAttrs(ctx, log.Scope("coord:repl"))

	if r.state != IdleState && r.state != FinalizedState {
		return errors.New(string(r.state))
	}

	if options == nil {
		options = &StartOptions{}
	}

	r.drop = options.DropBeforeCreate
	r.nsFilter = makeFilter(options.IncludeNamespaces, options.ExcludeNamespaces)

	r.repl = nil
	r.startedAt = primitive.Timestamp{}
	r.clonedAt = primitive.Timestamp{}
	r.state = RunningState

	go func() {
		ctx := log.CopyContext(ctx, context.Background())
		err := r.run(ctx)
		if err != nil {
			log.Error(ctx, err, "run")
			return
		}

		r.mu.Lock()
		r.state = FinalizedState
		r.mu.Unlock()

		log.Info(ctx, "finalized")
	}()

	log.Info(ctx, "started")
	return nil
}

func (r *Coordinator) run(ctx context.Context) error {
	ctx = log.WithAttrs(ctx, log.Scope("coord:run"))
	log.Info(ctx, "starting data cloning")

	startedAt, err := topo.ClusterTime(ctx, r.source)
	if err != nil {
		return errors.Wrap(err, "get cluster time")
	}

	r.mu.Lock()
	r.startedAt = startedAt
	r.mu.Unlock()

	catalog := NewCatalog()
	cloner := DataCloner{
		Source:       r.source,
		Target:       r.target,
		Drop:         r.drop,
		NSFilter:     r.nsFilter,
		IndexCatalog: catalog,
	}

	err = cloner.Clone(ctx)
	if err != nil {
		return errors.Wrap(err, "close")
	}

	clonedAt, err := topo.ClusterTime(ctx, r.source)
	if err != nil {
		return errors.Wrap(err, "get cluster time")
	}

	repl := &ChangeReplicator{
		Source:   r.source,
		Target:   r.target,
		Drop:     r.drop,
		NSFilter: r.nsFilter,
		Catalog:  catalog,
	}

	r.mu.Lock()
	r.clonedAt = clonedAt
	r.repl = repl
	r.mu.Unlock()

	log.Infof(ctx, "starting change replication at %d.%d", startedAt.T, startedAt.I)
	err = repl.Start(ctx, startedAt)
	if err != nil {
		return errors.Wrap(err, "start change replication")
	}

	err = repl.Wait()
	if err != nil {
		return errors.Wrap(err, "change replication")
	}

	err = catalog.FinalizeIndexes(ctx, r.target)
	if err != nil {
		return errors.Wrap(err, "finalize indexes")
	}

	return nil
}

func (r *Coordinator) Finalize(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state != RunningState {
		return errors.New(string(r.state))
	}

	if r.repl == nil || r.repl.GetLastAppliedOpTime().Before(r.clonedAt) {
		return errors.New("not ready")
	}

	r.repl.Pause()

	r.state = FinalizingState
	log.Info(ctx, "finalizing")
	return nil
}

func (r *Coordinator) Status(ctx context.Context) (*Status, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	s := &Status{
		State: r.state,
	}

	if r.repl != nil {
		optime := r.repl.GetLastAppliedOpTime()
		s.Finalizable = !optime.Before(r.clonedAt)
		s.LastAppliedOpTime = optime
	}
	return s, nil
}
