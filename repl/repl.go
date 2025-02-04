package repl

import (
	"context"
	"sync"

	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

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
	LastAppliedOpTime primitive.Timestamp
}

type Replicator struct {
	source *mongo.Client
	target *mongo.Client

	drop       bool
	isSelected FilterFunc

	stopC chan struct{}
	state State

	indexCatalog      *IndexCatalog
	lastAppliedOpTime primitive.Timestamp

	mu sync.Mutex
}

func New(source, target *mongo.Client) *Replicator {
	r := &Replicator{
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

func (r *Replicator) Start(ctx context.Context, options *StartOptions) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	ctx = log.WithAttrs(ctx, log.Scope("repl"))

	if options == nil {
		options = &StartOptions{}
	}

	switch r.state {
	case IdleState:
	case FinalizedState:
	default:
		return errors.Errorf("wrong status %q. already running", r.state)
	}

	r.drop = options.DropBeforeCreate
	r.isSelected = makeFilter(options.IncludeNamespaces, options.ExcludeNamespaces)

	r.stopC = make(chan struct{})
	r.state = RunningState
	r.lastAppliedOpTime = primitive.Timestamp{}

	go func() {
		ctx := log.CopyContext(ctx, context.Background())
		err := r.run(ctx)
		if err != nil {
			log.Error(ctx, err, "run")
		}

		log.Info(ctx, "finalized")
		r.mu.Lock()
		r.state = FinalizedState
		r.mu.Unlock()
	}()

	return nil
}

func (r *Replicator) init() {
	r.indexCatalog = NewIndexCatalog()
}

func (r *Replicator) deinit() {
	r.indexCatalog = nil
}

func (r *Replicator) run(ctx context.Context) error {
	ctx = log.WithAttrs(ctx, log.Scope("repl.run"))
	log.Info(ctx, "starting data cloning")

	r.init()
	defer r.deinit()

	startedAt, err := topo.ClusterTime(ctx, r.source)
	if err != nil {
		return errors.Wrap(err, "get cluster time")
	}

	cloner := dataCloner{
		Source:       r.source,
		Target:       r.target,
		Drop:         r.drop,
		IsSelected:   r.isSelected,
		IndexCatalog: r.indexCatalog,
	}

	err = cloner.Clone(ctx)
	if err != nil {
		return errors.Wrap(err, "close")
	}

	err = cloner.BuildIndexes(ctx)
	if err != nil {
		return errors.Wrap(err, "build indexes")
	}

	log.Infof(ctx, "starting change relication since %d.%d", startedAt.T, startedAt.I)
	err = r.runChangeRelication(ctx, startedAt)
	if err != nil {
		return errors.Wrap(err, "run change replication")
	}

	err = cloner.FinalizeIndexes(ctx)
	if err != nil {
		return errors.Wrap(err, "finalize indexes")
	}

	return nil
}

func (r *Replicator) Finalize(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state != RunningState {
		return errors.Errorf("wrong status %q. expected %q", r.state, RunningState)
	}

	log.Info(ctx, "finalizing")

	r.state = FinalizingState
	close(r.stopC)
	return nil
}

func (r *Replicator) Status(ctx context.Context) (*Status, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	s := &Status{
		State:             r.state,
		LastAppliedOpTime: r.lastAppliedOpTime,
	}
	return s, nil
}

func (r *Replicator) runChangeRelication(ctx context.Context, startAt primitive.Timestamp) error {
	ctx = log.WithAttrs(ctx, log.Scope("repl.apply"))

	applier := &eventApplier{
		Client:       r.target,
		Drop:         r.drop,
		IsSelected:   r.isSelected,
		IndexCatalog: r.indexCatalog,
	}
	opts := options.ChangeStream().
		SetStartAtOperationTime(&startAt).
		SetShowExpandedEvents(true)
	cur, err := r.source.Watch(ctx, mongo.Pipeline{}, opts)
	if err != nil {
		return errors.Wrap(err, "start change stream")
	}
	defer cur.Close(ctx)

	var optime primitive.Timestamp
	for {
		select {
		case <-r.stopC:
			return nil
		default:
		}

		for cur.TryNext(ctx) {
			optime, err = applier.Apply(ctx, cur.Current)

			if IsUnsupportedEventError(err) {
				r.updateLastAppliedOpTime(optime)
				continue
			}
			if IsInvalidatedError(err) {
				r.updateLastAppliedOpTime(optime)

				opts := options.ChangeStream().
					SetResumeAfter(cur.ResumeToken()).
					SetShowExpandedEvents(true)
				// TODO: use include/exclude namespaces in pipeline
				cur, err = r.source.Watch(ctx, mongo.Pipeline{}, opts)
			}
			if mongo.IsDuplicateKeyError(err) {
				r.updateLastAppliedOpTime(optime)
				log.Error(ctx, err, "DuplicateKeyError")
				continue
			}

			if err != nil {
				return errors.Wrap(err, "resume change stream")
			}
		}

		err = cur.Err()
		if err != nil || cur.ID() == 0 {
			return errors.Wrap(err, "cursor")
		}

		log.Trace(ctx, "no events")
	}
}

func (r *Replicator) updateLastAppliedOpTime(optime primitive.Timestamp) {
	r.mu.Lock()
	r.lastAppliedOpTime = optime
	r.mu.Unlock()
}
