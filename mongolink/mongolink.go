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
	State State // Current state of the MongoLink
	Error error

	PauseOnInitialSync  bool
	InitialSyncComplete bool // Indicates if the process can be finalized
	InitialSyncLagTime  int64

	LagTime *int64

	Repl  ReplStatus
	Clone CloneStatus // Status of the cloning process
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
	ml.cloneStartedAtSourceTS = bson.Timestamp{}
	ml.cloneFinishedAtSourceTS = bson.Timestamp{}
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

	cloneStartedAt := time.Now()

	err = ml.clone.Clone(ctx)
	if err != nil {
		return errors.Wrap(err, "clone")
	}

	cloneFinishedAt := time.Now()

	cloneFinishedAtSourceTS, err := topo.ClusterTime(ctx, ml.source)
	if err != nil {
		return errors.Wrap(err, "get cluster time")
	}

	ml.mu.Lock()
	ml.cloneFinishedAtTS = cloneFinishedAtSourceTS
	ml.mu.Unlock()

	lg.InfoWith("Data clone is completed",
		log.Elapsed(cloneFinishedAt.Sub(cloneStartedAt)))
	lg.Infof("Remaining logical seconds until Initial Sync complete: %d",
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

			lg.Debugf("Remaining logical seconds until Initial Sync complete: %d",
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

	if !cloneStatus.Complete {
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
func (ml *MongoLink) Status(ctx context.Context) (*Status, error) {
	ml.mu.Lock()
	defer ml.mu.Unlock()

	s := &Status{State: ml.state}

	if ml.state == StateIdle {
		return s, nil
	}

	sourceTime, err := topo.ClusterTime(ctx, ml.source)
	if err != nil {
		// Do not block status if source cluster is lost
		log.New("mongolink").Error(err, "Status: get source cluster time")
	}

	s.Clone = ml.clone.Status()
	s.Repl = ml.repl.Status()

	s.PauseOnInitialSync = ml.pauseOnInitialSync

	if s.Clone.Complete && !s.Repl.LastReplicatedOpTime.IsZero() {
		s.InitialSyncComplete = !s.Repl.LastReplicatedOpTime.Before(ml.cloneFinishedAtTS)
		if !s.InitialSyncComplete {
			var lag int64
			if !s.Repl.LastReplicatedOpTime.IsZero() {
				lag = int64(ml.cloneFinishedAtTS.T) - int64(s.Repl.LastReplicatedOpTime.T)
			} else { // replication has not processed its first operation
				lag = int64(ml.cloneFinishedAtTS.T) - int64(ml.cloneStartedAtTS.T)
			}

			s.InitialSyncLagTime = lag
		}
	}

	if !s.Repl.LastReplicatedOpTime.IsZero() && !sourceTime.IsZero() {
		lag := max(int64(sourceTime.T)-int64(s.Repl.LastReplicatedOpTime.T), 0)
		s.LagTime = &lag
	}

	switch {
	case s.Repl.Error != nil:
		s.Error = errors.Wrap(s.Repl.Error, "Change Replication")
	case s.Clone.Error != nil:
		s.Error = errors.Wrap(s.Clone.Error, "Clone")
	}

	return s, nil
}
