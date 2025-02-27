package mongolink

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/percona-lab/percona-mongolink/config"
	"github.com/percona-lab/percona-mongolink/errors"
	"github.com/percona-lab/percona-mongolink/log"
	"github.com/percona-lab/percona-mongolink/sel"
	"github.com/percona-lab/percona-mongolink/topo"
)

// State represents the state of the MongoLink.
type State string

const (
	// StateFailed indicates that the mongolink has failed.
	StateFailed = "failed"
	// StateIdle indicates that the mongolink is idle.
	StateIdle = "idle"
	// StateRunning indicates that the mongolink is running.
	StateRunning = "running"
	// StateFinalizing indicates that the mongolink is finalizing.
	StateFinalizing = "finalizing"
	// StateFinalized indicates that the mongolink has been finalized.
	StateFinalized = "finalized"
)

// Status represents the status of the MongoLink.
type Status struct {
	// State is the current state of the MongoLink.
	State State
	// Error is the error message if the operation failed.
	Error error

	// TotalLagTime is the current lag time in logical seconds between source and target clusters.
	TotalLagTime *int64
	// InitialSyncLagTime is the lag time during the initial sync.
	InitialSyncLagTime *int64
	// InitialSyncCompleted indicates if the initial sync is completed.
	InitialSyncCompleted bool
	// PauseOnInitialSync indicates if the replication is paused on initial sync.
	PauseOnInitialSync bool

	// Repl is the status of the replication process.
	Repl ReplStatus
	// Clone is the status of the cloning process.
	Clone CloneStatus
}

// MongoLink manages the replication process.
type MongoLink struct {
	source *mongo.Client // Source MongoDB client
	target *mongo.Client // Target MongoDB client

	nsFilter           sel.NSFilter // Namespace filter
	pauseOnInitialSync bool

	state State // Current state of the MongoLink

	catalog *Catalog // Catalog for managing collections and indexes
	clone   *Clone   // Clone process
	repl    *Repl    // Replication process

	cloneStartedAtTS  bson.Timestamp // Timestamp when the process started
	cloneFinishedAtTS bson.Timestamp // Timestamp when the cloning finished

	mu sync.Mutex
}

// New creates a new MongoLink.
func New(source, target *mongo.Client) *MongoLink {
	return &MongoLink{
		source: source,
		target: target,
		state:  StateIdle,
	}
}

// StartOptions represents the options for starting the MongoLink.
type StartOptions struct {
	// PauseOnInitialSync indicates whether to finalize after the initial sync.
	PauseOnInitialSync bool
	// IncludeNamespaces are the namespaces to include.
	IncludeNamespaces []string
	// ExcludeNamespaces are the namespaces to exclude.
	ExcludeNamespaces []string
}

// Start starts the replication process with the given options.
func (ml *MongoLink) Start(_ context.Context, options *StartOptions) error {
	ml.mu.Lock()
	defer ml.mu.Unlock()

	lg := log.New("mongolink")

	if ml.state != StateIdle && ml.state != StateFinalized && ml.state != StateFailed {
		return errors.New(string(ml.state))
	}

	if options == nil {
		options = &StartOptions{}
	}

	ml.nsFilter = sel.MakeFilter(options.IncludeNamespaces, options.ExcludeNamespaces)
	ml.pauseOnInitialSync = options.PauseOnInitialSync

	ml.repl = nil
	ml.cloneStartedAtTS = bson.Timestamp{}
	ml.cloneFinishedAtTS = bson.Timestamp{}
	ml.state = StateRunning

	ml.catalog = NewCatalog()
	ml.clone = &Clone{
		Source:   ml.source,
		Target:   ml.target,
		NSFilter: ml.nsFilter,
		Catalog:  ml.catalog,
	}

	ml.repl = &Repl{
		Source:   ml.source,
		Target:   ml.target,
		NSFilter: ml.nsFilter,
		Catalog:  ml.catalog,
	}

	go func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		err := ml.run(lg.WithContext(ctx))
		if err != nil {
			ml.mu.Lock()
			ml.state = StateFailed
			ml.mu.Unlock()

			lg.Error(err, "Cluster replication has failed")

			return
		}

		ml.mu.Lock()
		ml.state = StateFinalized
		ml.mu.Unlock()
	}()

	lg.Info("Cluster replication has started")

	return nil
}

// run executes the replication process.
func (ml *MongoLink) run(ctx context.Context) error {
	lg := log.Ctx(ctx)

	lg.Info("Starting data clone")

	clusterReplStartedAt := time.Now()

	cloneStartedAtSourceTS, err := topo.ClusterTime(ctx, ml.source)
	if err != nil {
		return errors.Wrap(err, "get cluster time")
	}

	ml.mu.Lock()
	ml.cloneStartedAtTS = cloneStartedAtSourceTS
	ml.mu.Unlock()

	cloneStartAt := time.Now()

	err = ml.clone.Clone(ctx)
	if err != nil {
		return errors.Wrap(err, "clone")
	}

	lg.InfoWith("Data clone is completed",
		log.Elapsed(time.Since(cloneStartAt)))

	cloneFinishedAtSourceTS, err := topo.ClusterTime(ctx, ml.source)
	if err != nil {
		return errors.Wrap(err, "get cluster time")
	}

	ml.mu.Lock()
	ml.cloneFinishedAtTS = cloneFinishedAtSourceTS
	ml.mu.Unlock()

	lg.Infof("Remaining logical seconds until Initial Sync completed: %d",
		cloneFinishedAtSourceTS.T-cloneStartedAtSourceTS.T)
	lg.Infof("Starting Change Replication since %d.%d source cluster time",
		cloneStartedAtSourceTS.T, cloneStartedAtSourceTS.I)

	replStartedAt := time.Now()

	err = ml.repl.Start(ctx, cloneStartedAtSourceTS)
	if err != nil {
		return errors.Wrap(err, "start change replication")
	}

	replDoneSig := ml.repl.Done()

	go func() {
		// make sure the repl processes its first operation
		<-time.After(config.ReplTickInteral)

		t := time.NewTicker(config.ReplInitialSyncCheckInterval)
		defer t.Stop()

		for {
			select {
			case <-t.C:
			case <-replDoneSig:
				return
			}

			replStatus := ml.repl.Status()
			if replStatus.Error != nil {
				return
			}

			if !replStatus.LastReplicatedOpTime.Before(cloneFinishedAtSourceTS) {
				lg.InfoWith("Initial sync has been completed",
					log.Elapsed(time.Since(replStartedAt)),
					log.TotalElapsed(time.Since(clusterReplStartedAt)))

				if ml.pauseOnInitialSync {
					lg.Info("Pausing [PauseOnInitialSync]")
					ml.repl.Pause()
				}

				return
			}

			lg.Debugf("Remaining logical seconds until Initial Sync completed: %d",
				cloneFinishedAtSourceTS.T-replStatus.LastReplicatedOpTime.T)
		}
	}()

	<-replDoneSig

	replFinishedAt := time.Now()

	replStatus := ml.repl.Status()
	if replStatus.Error != nil {
		return errors.Wrap(err, "change replication")
	}

	lg.InfoWith(
		fmt.Sprintf("Change replication stopped at %d.%d source cluster time",
			replStatus.LastReplicatedOpTime.T, replStatus.LastReplicatedOpTime.T),
		log.Elapsed(replFinishedAt.Sub(replStartedAt)))

	err = ml.catalog.FinalizeIndexes(ctx, ml.target)
	if err != nil {
		return errors.Wrap(err, "finalize indexes")
	}

	lg.InfoWith("Cluster replication is finalized",
		log.Elapsed(time.Since(replFinishedAt)),
		log.TotalElapsed(time.Since(clusterReplStartedAt)))

	return nil
}

// Finalize finalizes the replication process.
func (ml *MongoLink) Finalize(_ context.Context) error {
	ml.mu.Lock()
	defer ml.mu.Unlock()

	if ml.state != StateRunning {
		return errors.New(string(ml.state))
	}

	cloneStatus := ml.clone.Status()
	if cloneStatus.Error != nil {
		return errors.Wrap(cloneStatus.Error, "clone failed")
	}

	if !cloneStatus.Completed {
		return errors.New("clone has not been completed")
	}

	replStatus := ml.repl.Status()
	if replStatus.Error != nil {
		return errors.Wrap(replStatus.Error, "cannot finalize due to existing error")
	}

	if replStatus.LastReplicatedOpTime.IsZero() {
		return errors.New("replication has not been started")
	}

	if replStatus.LastReplicatedOpTime.Before(ml.cloneFinishedAtTS) {
		return errors.New("not finalizable")
	}

	ml.repl.Pause()
	ml.state = StateFinalizing

	log.New("mongolink").Info("Finalizing")

	return nil
}

// Status returns the current status of the MongoLink.
func (ml *MongoLink) Status(ctx context.Context) *Status {
	ml.mu.Lock()
	defer ml.mu.Unlock()

	if ml.state == StateIdle {
		return &Status{State: StateIdle}
	}

	s := &Status{
		State: ml.state,

		Clone: ml.clone.Status(),
		Repl:  ml.repl.Status(),

		PauseOnInitialSync: ml.pauseOnInitialSync,
	}

	switch {
	case s.Repl.Error != nil:
		s.Error = errors.Wrap(s.Repl.Error, "Change Replication")
	case s.Clone.Error != nil:
		s.Error = errors.Wrap(s.Clone.Error, "Clone")
	}

	switch {
	case ml.state == StateFinalizing || ml.state == StateFinalized:
		zero := int64(0)
		s.TotalLagTime = &zero
		s.InitialSyncLagTime = &zero
		s.InitialSyncCompleted = true

		return s

	case ml.state == StateFailed:
		return s
	}

	sourceTime, err := topo.ClusterTime(ctx, ml.source)
	if err != nil {
		// Do not block status if source cluster is lost
		log.New("mongolink").Error(err, "Status: get source cluster time")
	} else {
		switch {
		case !s.Repl.LastReplicatedOpTime.IsZero():
			// max() in case, sourceTime and more have already been processed
			totalLag := max(int64(sourceTime.T)-int64(s.Repl.LastReplicatedOpTime.T), 0)
			s.TotalLagTime = &totalLag
		case !ml.cloneStartedAtTS.IsZero():
			totalLag := int64(sourceTime.T) - int64(ml.cloneStartedAtTS.T)
			s.TotalLagTime = &totalLag
		}
	}

	s.InitialSyncLagTime = s.TotalLagTime

	if !ml.cloneFinishedAtTS.IsZero() && !s.Repl.LastReplicatedOpTime.IsZero() {
		intialSyncLag := max(int64(ml.cloneFinishedAtTS.T)-int64(s.Repl.LastReplicatedOpTime.T), 0)
		s.InitialSyncLagTime = &intialSyncLag

		if intialSyncLag == 0 {
			s.InitialSyncCompleted = true
		}
	}

	return s
}
