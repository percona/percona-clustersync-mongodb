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

// State represents the state of the mongolink.
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

// Status represents the status of the mongolink.
type Status struct {
	State State  // Current state of the mongolink
	Info  string // Additional information
	Error error

	Finalizable       bool           // Indicates if the process can be finalized
	LastAppliedOpTime bson.Timestamp // Last applied operation time
	EventsProcessed   int64          // Number of events processed
	Clone             CloneStatus    // Status of the cloning process
}

// MongoLink manages the replication process.
type MongoLink struct {
	source *mongo.Client // Source MongoDB client
	target *mongo.Client // Target MongoDB client

	nsFilter sel.NSFilter // Namespace filter

	state   State    // Current state of the mongolink
	catalog *Catalog // Catalog for managing collections and indexes
	clone   *Clone   // Clone process
	repl    *Repl    // Replication process

	cloneStartedAtSourceTS  bson.Timestamp // Timestamp when the process started
	cloneFinishedAtSourceTS bson.Timestamp // Timestamp when the cloning finished

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

// StartOptions represents the options for starting the mongolink.
type StartOptions struct {
	IncludeNamespaces []string // Namespaces to include
	ExcludeNamespaces []string // Namespaces to exclude
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

			lg.Error(err, "cluster replication has failed")

			return
		}

		ml.mu.Lock()
		ml.state = StateFinalized
		ml.mu.Unlock()
	}()

	lg.Info("cluster replication has started")

	return nil
}

// run executes the replication process.
func (ml *MongoLink) run(ctx context.Context) error {
	lg := log.Ctx(ctx)

	lg.Info("starting data clone")

	clusterReplStartedAt := time.Now()

	cloneStartedAtSourceTS, err := topo.ClusterTime(ctx, ml.source)
	if err != nil {
		return errors.Wrap(err, "get cluster time")
	}

	ml.mu.Lock()
	ml.cloneStartedAtSourceTS = cloneStartedAtSourceTS
	ml.mu.Unlock()

	cloneStartedAt := time.Now()

	err = ml.clone.Clone(ctx)
	if err != nil {
		return errors.Wrap(err, "close")
	}

	cloneFinishedAt := time.Now()

	cloneFinishedAtSourceTS, err := topo.ClusterTime(ctx, ml.source)
	if err != nil {
		return errors.Wrap(err, "get cluster time")
	}

	ml.mu.Lock()
	ml.cloneFinishedAtSourceTS = cloneFinishedAtSourceTS
	ml.mu.Unlock()

	lg.InfoWith("data clone is completed",
		log.Elapsed(cloneFinishedAt.Sub(cloneStartedAt)))
	lg.Infof("remaining logical seconds until initial sync done: %d",
		cloneFinishedAtSourceTS.T-cloneStartedAtSourceTS.T)
	lg.Infof("starting change replication since %d.%d source cluster time",
		cloneStartedAtSourceTS.T, cloneStartedAtSourceTS.I)

	replStartedAt := time.Now()

	err = ml.repl.Start(ctx, cloneStartedAtSourceTS)
	if err != nil {
		return errors.Wrap(err, "start change replication")
	}

	go func() {
		// wait until repl makes applies first op
		<-time.After(config.ReplTickInteral)

		t := time.NewTicker(config.ReplInitialSyncCheckInterval)
		defer t.Stop()

		for {
			select {
			case <-t.C:
			case <-ml.repl.Done():
				return
			}

			replStatus := ml.repl.Status()
			if replStatus.Error != nil {
				return
			}

			if !replStatus.LastAppliedOpTime.Before(cloneFinishedAtSourceTS) {
				lg.InfoWith("initial sync done",
					log.Elapsed(time.Since(replStartedAt)),
					log.TotalElapsed(time.Since(clusterReplStartedAt)))

				return
			}

			lg.Infof("remaining logical seconds until initial sync done: %d",
				cloneFinishedAtSourceTS.T-replStatus.LastAppliedOpTime.T)
		}
	}()

	<-ml.repl.Done()

	replFinishedAt := time.Now()

	replStatus := ml.repl.Status()
	if replStatus.Error != nil {
		return errors.Wrap(err, "change replication")
	}

	lg.InfoWith(
		fmt.Sprintf("change replication stopped at %d.%d source cluster time",
			replStatus.LastAppliedOpTime.T, replStatus.LastAppliedOpTime.T),
		log.Elapsed(replFinishedAt.Sub(replStartedAt)))

	err = ml.catalog.FinalizeIndexes(ctx, ml.target)
	if err != nil {
		return errors.Wrap(err, "finalize indexes")
	}

	lg.InfoWith("cluster replication is finalized",
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

	if !cloneStatus.Finished {
		return errors.New("clone has not finished")
	}

	replStatus := ml.repl.Status()
	if replStatus.Error != nil {
		return errors.Wrap(replStatus.Error, "cannot finalize due to existing error")
	}

	if replStatus.LastAppliedOpTime.IsZero() {
		return errors.New("repl has not been started")
	}

	if replStatus.LastAppliedOpTime.Before(ml.cloneFinishedAtSourceTS) {
		return errors.New("not finalizable")
	}

	ml.repl.Pause()
	ml.state = StateFinalizing

	log.New("mongolink").Info("finalizing")

	return nil
}

// Status returns the current status of the mongolink.
func (ml *MongoLink) Status(_ context.Context) (*Status, error) {
	ml.mu.Lock()
	defer ml.mu.Unlock()

	s := &Status{State: ml.state}

	if ml.state == StateIdle {
		return s, nil
	}

	cloneStatus := ml.clone.Status()
	s.Clone = cloneStatus

	replStatus := ml.repl.Status()
	s.Error = replStatus.Error
	s.LastAppliedOpTime = replStatus.LastAppliedOpTime
	s.EventsProcessed = replStatus.EventsProcessed

	if cloneStatus.Finished && !replStatus.LastAppliedOpTime.IsZero() {
		s.Finalizable = !replStatus.LastAppliedOpTime.Before(ml.cloneFinishedAtSourceTS)
	}

	switch {
	case ml.state == StateIdle:
		s.Info = "waiting for start"
	case ml.state == StateRunning && !cloneStatus.Finished:
		s.Info = "cloning data"
	case ml.state == StateRunning && !replStatus.LastAppliedOpTime.IsZero():
		s.Info = "replicating changes"
	}

	return s, nil
}
