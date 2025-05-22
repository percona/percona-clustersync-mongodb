/*
Package mongolink provides functionality for cloning and replicating data between MongoDB clusters.

This package includes the following main components:

  - MongoLink: Manages the overall replication process, including cloning and change replication.

  - Clone: Handles the cloning of data from a source MongoDB cluster to a target MongoDB cluster.

  - Repl: Handles the replication of changes from a source MongoDB cluster to a target MongoDB cluster.

  - Catalog: Manages collections and indexes in the target MongoDB cluster.
*/
package mongolink

import (
	"context"
	"math"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/percona/percona-mongolink/config"
	"github.com/percona/percona-mongolink/errors"
	"github.com/percona/percona-mongolink/log"
	"github.com/percona/percona-mongolink/metrics"
	"github.com/percona/percona-mongolink/sel"
	"github.com/percona/percona-mongolink/topo"
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
	// StatePaused indicates that the mongolink is paused.
	StatePaused = "paused"
	// StateFinalizing indicates that the mongolink is finalizing.
	StateFinalizing = "finalizing"
	// StateFinalized indicates that the mongolink has been finalized.
	StateFinalized = "finalized"
)

type OnStateChangedFunc func(newState State)

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

	// Repl is the status of the replication process.
	Repl ReplStatus
	// Clone is the status of the cloning process.
	Clone CloneStatus
}

// MongoLink manages the replication process.
type MongoLink struct {
	source *mongo.Client // Source MongoDB client
	target *mongo.Client // Target MongoDB client

	nsInclude []string
	nsExclude []string
	nsFilter  sel.NSFilter // Namespace filter

	onStateChanged OnStateChangedFunc // onStateChanged is invoked on each state change

	pauseOnInitialSync bool

	state State // Current state of the MongoLink

	catalog *Catalog // Catalog for managing collections and indexes
	clone   *Clone   // Clone process
	repl    *Repl    // Replication process

	err error

	lock sync.Mutex
}

// New creates a new MongoLink.
func New(source, target *mongo.Client) *MongoLink {
	return &MongoLink{
		source:         source,
		target:         target,
		state:          StateIdle,
		onStateChanged: func(State) {},
	}
}

type checkpoint struct {
	NSInclude []string `bson:"nsInclude,omitempty"`
	NSExclude []string `bson:"nsExclude,omitempty"`

	Catalog *catalogCheckpoint `bson:"catalog,omitempty"`
	Clone   *cloneCheckpoint   `bson:"clone,omitempty"`
	Repl    *replCheckpoint    `bson:"repl,omitempty"`

	State State  `bson:"state"`
	Error string `bson:"error,omitempty"`
}

func (ml *MongoLink) Checkpoint(context.Context) ([]byte, error) {
	ml.lock.Lock()
	defer ml.lock.Unlock()

	if ml.state == StateIdle {
		return nil, nil
	}

	// prevent catalog changes during checkpoint
	ml.catalog.LockWrite()
	defer ml.catalog.UnlockWrite()

	cp := &checkpoint{
		NSInclude: ml.nsInclude,
		NSExclude: ml.nsExclude,

		Catalog: ml.catalog.Checkpoint(),
		Clone:   ml.clone.Checkpoint(),
		Repl:    ml.repl.Checkpoint(),

		State: ml.state,
	}

	if ml.err != nil {
		cp.Error = ml.err.Error()
	}

	return bson.Marshal(cp) //nolint:wrapcheck
}

func (ml *MongoLink) Recover(ctx context.Context, data []byte) error {
	ml.lock.Lock()
	defer ml.lock.Unlock()

	if ml.state != StateIdle {
		return errors.New("cannot recover: invalid MongoLink state")
	}

	var cp checkpoint

	err := bson.Unmarshal(data, &cp)
	if err != nil {
		return errors.Wrap(err, "unmarshal")
	}

	if cp.State == StateIdle {
		return nil
	}

	nsFilter := sel.MakeFilter(cp.NSInclude, cp.NSExclude)
	catalog := NewCatalog(ml.target)
	clone := NewClone(ml.source, ml.target, catalog, nsFilter)
	repl := NewRepl(ml.source, ml.target, catalog, nsFilter)

	if cp.Catalog != nil {
		err = catalog.Recover(cp.Catalog)
		if err != nil {
			return errors.Wrap(err, "recover catalog")
		}
	}

	if cp.Clone != nil {
		err = clone.Recover(cp.Clone)
		if err != nil {
			return errors.Wrap(err, "recover clone")
		}
	}

	if cp.Repl != nil {
		err = repl.Recover(cp.Repl)
		if err != nil {
			return errors.Wrap(err, "recover repl")
		}
	}

	ml.nsInclude = cp.NSInclude
	ml.nsExclude = cp.NSExclude
	ml.nsFilter = nsFilter
	ml.catalog = catalog
	ml.clone = clone
	ml.repl = repl
	ml.state = cp.State

	if cp.Error != "" {
		ml.err = errors.New(cp.Error)
	}

	if cp.State == StateRunning {
		return ml.doResume(ctx, false)
	}

	return nil
}

// SetOnStateChanged set the f function to be called on each state change.
func (ml *MongoLink) SetOnStateChanged(f OnStateChangedFunc) {
	if f == nil {
		f = func(State) {}
	}

	ml.lock.Lock()
	ml.onStateChanged = f
	ml.lock.Unlock()
}

// Status returns the current status of the MongoLink.
func (ml *MongoLink) Status(ctx context.Context) *Status {
	ml.lock.Lock()
	defer ml.lock.Unlock()

	if ml.state == StateIdle {
		return &Status{State: StateIdle}
	}

	s := &Status{
		State: ml.state,
		Clone: ml.clone.Status(),
		Repl:  ml.repl.Status(),
	}

	switch {
	case ml.err != nil:
		s.Error = ml.err
	case s.Repl.Err != nil:
		s.Error = errors.Wrap(s.Repl.Err, "Change Replication")
	case s.Clone.Err != nil:
		s.Error = errors.Wrap(s.Clone.Err, "Clone")
	}

	if ml.state == StateFailed {
		return s
	}

	sourceTime, err := topo.ClusterTime(ctx, ml.source)
	if err != nil {
		// Do not block status if source cluster is lost
		log.New("mongolink").Error(err, "Status: get source cluster time")
	} else {
		switch {
		case !s.Repl.LastReplicatedOpTime.IsZero():
			totalLag := int64(sourceTime.T) - int64(s.Repl.LastReplicatedOpTime.T)
			s.TotalLagTime = &totalLag
		case !s.Clone.StartTS.IsZero():
			totalLag := int64(sourceTime.T) - int64(s.Clone.StartTS.T)
			s.TotalLagTime = &totalLag
		}
	}

	s.InitialSyncLagTime = s.TotalLagTime

	if s.Repl.IsStarted() {
		intialSyncLag := max(int64(s.Clone.FinishTS.T)-int64(s.Repl.LastReplicatedOpTime.T), 0)
		s.InitialSyncLagTime = &intialSyncLag
		s.InitialSyncCompleted = s.Repl.LastReplicatedOpTime.After(s.Clone.FinishTS)
	}

	return s
}

func (ml *MongoLink) resetError() {
	ml.err = nil
	ml.clone.resetError()
	ml.repl.resetError()
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
	ml.lock.Lock()
	defer ml.lock.Unlock()

	switch ml.state {
	case StateRunning, StateFinalizing, StateFailed:
		err := errors.New("already running")
		log.New("mongolink:start").Error(err, "")

		return err

	case StatePaused:
		err := errors.New("paused")
		log.New("mongolink:start").Error(err, "")

		return err
	}

	if options == nil {
		options = &StartOptions{}
	}

	ml.nsInclude = options.IncludeNamespaces
	ml.nsExclude = options.ExcludeNamespaces
	ml.nsFilter = sel.MakeFilter(ml.nsInclude, ml.nsExclude)
	ml.pauseOnInitialSync = options.PauseOnInitialSync
	ml.catalog = NewCatalog(ml.target)
	ml.clone = NewClone(ml.source, ml.target, ml.catalog, ml.nsFilter)
	ml.repl = NewRepl(ml.source, ml.target, ml.catalog, ml.nsFilter)
	ml.state = StateRunning

	go ml.run()

	return nil
}

func (ml *MongoLink) setFailed(err error) {
	ml.lock.Lock()
	ml.state = StateFailed
	ml.err = err
	ml.lock.Unlock()

	log.New("mongolink").Error(err, "Cluster Replication has failed")

	go ml.onStateChanged(StateFailed)
}

// run executes the cluster replication.
func (ml *MongoLink) run() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lg := log.New("mongolink")

	lg.Info("Starting Cluster Replication")

	cloneStatus := ml.clone.Status()
	if !cloneStatus.IsFinished() {
		err := ml.clone.Start(ctx)
		if err != nil {
			ml.setFailed(errors.Wrap(cloneStatus.Err, "start clone"))

			return
		}

		<-ml.clone.Done()

		cloneStatus = ml.clone.Status()
		if cloneStatus.Err != nil {
			ml.setFailed(errors.Wrap(cloneStatus.Err, "clone"))

			return
		}
	}

	replStatus := ml.repl.Status()
	if !replStatus.IsStarted() {
		err := ml.repl.Start(ctx, cloneStatus.StartTS)
		if err != nil {
			ml.setFailed(errors.Wrap(err, "start change replication"))

			return
		}
	} else {
		err := ml.repl.Resume(ctx)
		if err != nil {
			ml.setFailed(errors.Wrap(err, "resume change replication"))

			return
		}
	}

	if replStatus.LastReplicatedOpTime.Before(cloneStatus.FinishTS) {
		go ml.monitorInitialSync(ctx)
	}
	go ml.monitorLagTime(ctx)

	<-ml.repl.Done()

	replStatus = ml.repl.Status()
	if replStatus.Err != nil {
		ml.setFailed(errors.Wrap(replStatus.Err, "change replication"))
	}
}

func (ml *MongoLink) monitorInitialSync(ctx context.Context) {
	lg := log.New("monitor:initial-sync-lag-time")

	t := time.NewTicker(time.Second)
	defer t.Stop()

	cloneStatus := ml.clone.Status()
	if cloneStatus.Err != nil {
		return
	}

	replStatus := ml.repl.Status()
	if replStatus.Err != nil {
		return
	}

	if replStatus.LastReplicatedOpTime.After(cloneStatus.FinishTS) {
		return
	}

	lastPrintAt := time.Time{}

	for {
		select {
		case <-ctx.Done():
			return

		case <-t.C:
		}

		replStatus = ml.repl.Status()
		if replStatus.LastReplicatedOpTime.After(cloneStatus.FinishTS) {
			elapsed := time.Since(replStatus.StartTime)
			lg.With(log.Elapsed(elapsed)).
				Infof("Clone event backlog processed in %s", elapsed.Round(time.Second))
			elapsed = time.Since(cloneStatus.StartTime)
			lg.With(log.Elapsed(elapsed)).
				Infof("Initial Sync completed in %s", elapsed.Round(time.Second))

			ml.lock.Lock()
			pauseOnInitialSync := ml.pauseOnInitialSync
			ml.lock.Unlock()

			if pauseOnInitialSync {
				lg.Info("Pausing [PauseOnInitialSync]")

				err := ml.Pause(ctx)
				if err != nil {
					lg.Error(err, "PauseOnInitialSync")
				}
			}

			return
		}

		lagTime := max(int64(cloneStatus.FinishTS.T)-int64(replStatus.LastReplicatedOpTime.T), 0)
		metrics.SetInitialSyncLagTimeSeconds(uint32(min(lagTime, math.MaxUint32))) //nolint:gosec

		now := time.Now()
		if now.Sub(lastPrintAt) >= config.InitialSyncCheckInterval {
			lg.Debugf("Remaining logical seconds until Initial Sync completed: %d", lagTime)
			lastPrintAt = now
		}
	}
}

func (ml *MongoLink) monitorLagTime(ctx context.Context) {
	lg := log.New("monitor:lag-time")

	t := time.NewTicker(time.Second)
	defer t.Stop()

	lastPrintAt := time.Time{}

	for {
		select {
		case <-ctx.Done():
			return

		case <-t.C:
		}

		sourceTS, err := topo.ClusterTime(ctx, ml.source)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}

			lg.Error(err, "source cluster time")

			continue
		}

		replStatus := ml.repl.Status()
		timeDiff := max(int64(sourceTS.T)-int64(replStatus.LastReplicatedOpTime.T), 0)
		if timeDiff == 1 && replStatus.LastReplicatedOpTime.I == 1 {
			timeDiff = 0 // likely the oplog note from [Repl]. can approximate the 1 increment.
		}

		lagTime := uint32(min(timeDiff, math.MaxUint32)) //nolint:gosec
		metrics.SetLagTimeSeconds(lagTime)

		now := time.Now()
		if now.Sub(lastPrintAt) >= config.PrintLagTimeInterval {
			lg.Infof("Lag Time: %d", lagTime)
			lastPrintAt = now
		}
	}
}

// Pause pauses the replication process.
func (ml *MongoLink) Pause(ctx context.Context) error {
	ml.lock.Lock()
	defer ml.lock.Unlock()

	err := ml.doPause(ctx)
	if err != nil {
		log.New("mongolink").Error(err, "Pause Cluster Replication")

		return err
	}

	log.New("mongolink").Info("Cluster Replication paused")

	return nil
}

func (ml *MongoLink) doPause(ctx context.Context) error {
	if ml.state != StateRunning {
		return errors.New("cannot pause: not running")
	}

	replStatus := ml.repl.Status()

	if !replStatus.IsRunning() {
		return errors.New("cannot pause: Change Replication is not runnning")
	}

	err := ml.repl.Pause(ctx)
	if err != nil {
		return err
	}

	ml.state = StatePaused
	go ml.onStateChanged(StatePaused)

	return nil
}

type ResumeOptions struct {
	ResumeFromFailure bool
}

// Resume resumes the replication process.
func (ml *MongoLink) Resume(ctx context.Context, options ResumeOptions) error {
	ml.lock.Lock()
	defer ml.lock.Unlock()

	if ml.state != StatePaused && !(ml.state == StateFailed && options.ResumeFromFailure) {
		return errors.New("cannot resume: not paused or not resuming from failure")
	}

	err := ml.doResume(ctx, options.ResumeFromFailure)
	if err != nil {
		log.New("mongolink").Error(err, "Resume Cluster Replication")

		return err
	}

	log.New("mongolink").Info("Cluster Replication resumed")

	return nil
}

func (ml *MongoLink) doResume(_ context.Context, fromFailure bool) error {
	replStatus := ml.repl.Status()

	if !replStatus.IsStarted() && !fromFailure {
		return errors.New("cannot resume: replication is not started or not resuming from failure")
	}

	if !replStatus.IsPaused() && fromFailure {
		return errors.New("cannot resume: replication is not paused or not resuming from failure")
	}

	ml.state = StateRunning
	ml.resetError()

	go ml.run()
	go ml.onStateChanged(StateRunning)

	return nil
}

type FinalizeOptions struct {
	IgnoreHistoryLost bool
}

// Finalize finalizes the replication process.
func (ml *MongoLink) Finalize(ctx context.Context, options FinalizeOptions) error {
	status := ml.Status(ctx)

	ml.lock.Lock()
	defer ml.lock.Unlock()

	if status.State == StateFailed {
		if !options.IgnoreHistoryLost || !errors.Is(status.Repl.Err, ErrOplogHistoryLost) {
			return errors.Wrap(status.Error, "failed state")
		}
	}

	if !status.Clone.IsFinished() {
		return errors.New("clone is not completed")
	}

	if !status.Repl.IsStarted() {
		return errors.New("change replication is not started")
	}

	if !status.InitialSyncCompleted {
		return errors.New("initial sync is not completed")
	}

	lg := log.Ctx(ctx)
	lg.Info("Starting Finalization")

	if status.Repl.IsRunning() {
		lg.Info("Pausing Change Replication")

		err := ml.repl.Pause(ctx)
		if err != nil {
			return errors.Wrap(err, "pause change replication")
		}

		<-ml.repl.Done()
		lg.Info("Change Replication is paused")

		err = ml.repl.Status().Err
		if err != nil {
			// no need to set the MongoLink failed status here.
			// [MongoLink.setFailed] is called in [MongoLink.run].
			return errors.Wrap(err, "post-pause change replication")
		}
	}

	startedTime := time.Now()
	ml.state = StateFinalizing

	go func() {
		err := ml.catalog.Finalize(context.Background())
		if err != nil {
			ml.setFailed(errors.Wrap(err, "finalization"))

			return
		}

		ml.lock.Lock()
		ml.state = StateFinalized
		ml.lock.Unlock()

		lg.With(log.Elapsed(time.Since(startedTime))).
			Info("Finalization is completed")

		go ml.onStateChanged(StateFinalized)
	}()

	log.New("mongolink").Info("Finalizing")

	go ml.onStateChanged(StateFinalizing)

	return nil
}
