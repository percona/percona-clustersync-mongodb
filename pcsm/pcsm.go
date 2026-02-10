/*
Package pcsm provides functionality for cloning and replicating data between MongoDB clusters.

This package includes the following main components:

  - PCSM: Manages the overall replication process, including cloning and change replication.

  - Clone: Handles the cloning of data from a source MongoDB cluster to a target MongoDB cluster.

  - Repl: Handles the replication of changes from a source MongoDB cluster to a target MongoDB cluster.

  - Catalog: Manages collections and indexes in the target MongoDB cluster.
*/
package pcsm

import (
	"context"
	"math"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/percona/percona-clustersync-mongodb/config"
	"github.com/percona/percona-clustersync-mongodb/errors"
	"github.com/percona/percona-clustersync-mongodb/log"
	"github.com/percona/percona-clustersync-mongodb/metrics"
	"github.com/percona/percona-clustersync-mongodb/pcsm/catalog"
	"github.com/percona/percona-clustersync-mongodb/pcsm/clone"
	"github.com/percona/percona-clustersync-mongodb/pcsm/repl"
	"github.com/percona/percona-clustersync-mongodb/sel"
	"github.com/percona/percona-clustersync-mongodb/topo"
)

// State represents the state of the PCSM.
type State string

const (
	// StateFailed indicates that the pcsm has failed.
	StateFailed = "failed"
	// StateIdle indicates that the pcsm is idle.
	StateIdle = "idle"
	// StateRunning indicates that the pcsm is running.
	StateRunning = "running"
	// StatePaused indicates that the pcsm is paused.
	StatePaused = "paused"
	// StateFinalizing indicates that the pcsm is finalizing.
	StateFinalizing = "finalizing"
	// StateFinalized indicates that the pcsm has been finalized.
	StateFinalized = "finalized"
)

type OnStateChangedFunc func(newState State)

// Cloner defines the interface for the clone component.
type Cloner interface {
	Start(ctx context.Context) error
	Done() <-chan struct{}
	Status() clone.Status
	Checkpoint() *clone.Checkpoint
	Recover(cp *clone.Checkpoint) error
	ResetError()
}

// Replicator defines the interface for the replication component.
type Replicator interface {
	Start(ctx context.Context, startAt bson.Timestamp) error
	Pause(ctx context.Context) error
	Resume(ctx context.Context) error
	Done() <-chan struct{}
	Status() repl.Status
	Checkpoint() *repl.Checkpoint
	Recover(cp *repl.Checkpoint) error
	ResetError()
}

// Status represents the status of the PCSM.
type Status struct {
	// State is the current state of the PCSM.
	State State
	// Error is the error message if the operation failed.
	Error error

	// TotalLagTimeSeconds is the current lag time in logical seconds between source and target clusters.
	TotalLagTimeSeconds int64
	// InitialSyncLagTimeSeconds is the lag time during the initial sync.
	InitialSyncLagTimeSeconds int64
	// InitialSyncCompleted indicates if the initial sync is completed.
	InitialSyncCompleted bool

	// Repl is the status of the replication process.
	Repl repl.Status
	// Clone is the status of the cloning process.
	Clone clone.Status
}

// PCSM manages the replication process.
type PCSM struct {
	source *mongo.Client // Source MongoDB client
	target *mongo.Client // Target MongoDB client

	nsInclude []string
	nsExclude []string
	nsFilter  sel.NSFilter // Namespace filter

	onStateChanged OnStateChangedFunc // onStateChanged is invoked on each state change

	pauseOnInitialSync bool

	state State // Current state of the PCSM

	catalog *catalog.Catalog // Catalog for managing collections and indexes
	clone   Cloner           // Clone process
	repl    Replicator       // Replication process

	err error

	lock sync.Mutex
}

// New creates a new PCSM.
func New(source, target *mongo.Client) *PCSM {
	return &PCSM{
		source:         source,
		target:         target,
		state:          StateIdle,
		onStateChanged: func(State) {},
	}
}

type checkpoint struct {
	NSInclude []string `bson:"nsInclude,omitempty"`
	NSExclude []string `bson:"nsExclude,omitempty"`

	Catalog *catalog.Checkpoint `bson:"catalog,omitempty"`
	Clone   *clone.Checkpoint   `bson:"clone,omitempty"`
	Repl    *repl.Checkpoint    `bson:"repl,omitempty"`

	State State  `bson:"state"`
	Error string `bson:"error,omitempty"`
}

func (ml *PCSM) Checkpoint(context.Context) ([]byte, error) {
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

func (ml *PCSM) Recover(ctx context.Context, data []byte) error {
	ml.lock.Lock()
	defer ml.lock.Unlock()

	if ml.state != StateIdle {
		return errors.New("cannot recover: invalid PCSM state")
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
	cat := catalog.NewCatalog(ml.target)
	// Use empty options for recovery (clone tuning is less relevant when resuming from checkpoint)
	cloner := clone.NewClone(ml.source, ml.target, cat, nsFilter, &clone.Options{})
	replInst := repl.NewRepl(ml.source, ml.target, cat, nsFilter, &repl.Options{})

	if cp.Catalog != nil {
		err = cat.Recover(cp.Catalog)
		if err != nil {
			return errors.Wrap(err, "recover catalog")
		}
	}

	if cp.Clone != nil {
		err = cloner.Recover(cp.Clone)
		if err != nil {
			return errors.Wrap(err, "recover clone")
		}
	}

	if cp.Repl != nil {
		err = replInst.Recover(cp.Repl)
		if err != nil {
			return errors.Wrap(err, "recover repl")
		}
	}

	ml.nsInclude = cp.NSInclude
	ml.nsExclude = cp.NSExclude
	ml.nsFilter = nsFilter
	ml.catalog = cat
	ml.clone = cloner
	ml.repl = replInst
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
func (ml *PCSM) SetOnStateChanged(f OnStateChangedFunc) {
	if f == nil {
		f = func(State) {}
	}

	ml.lock.Lock()
	ml.onStateChanged = f
	ml.lock.Unlock()
}

// Status returns the current status of the PCSM.
func (ml *PCSM) Status(ctx context.Context) *Status {
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

	if s.Repl.IsStarted() {
		s.InitialSyncCompleted = s.Repl.LastReplicatedOpTime.After(s.Clone.FinishTS)
	}

	if ml.state == StateFailed {
		return s
	}

	sourceTime, err := topo.ClusterTime(ctx, ml.source)
	if err != nil {
		// Do not block status if source cluster is lost
		log.New("pcsm").Error(err, "Status: get source cluster time")
	} else {
		switch {
		case !s.Repl.LastReplicatedOpTime.IsZero():
			totalLag := int64(sourceTime.T) - int64(s.Repl.LastReplicatedOpTime.T)
			s.TotalLagTimeSeconds = totalLag
		case !s.Clone.StartTS.IsZero():
			totalLag := int64(sourceTime.T) - int64(s.Clone.StartTS.T)
			s.TotalLagTimeSeconds = totalLag
		}
	}

	if !s.InitialSyncCompleted {
		s.InitialSyncLagTimeSeconds = s.TotalLagTimeSeconds
	}

	return s
}

func (ml *PCSM) resetError() {
	ml.err = nil
	ml.clone.ResetError()
	ml.repl.ResetError()
}

// StartOptions represents the options for starting the PCSM.
type StartOptions struct {
	// PauseOnInitialSync indicates whether to pause after the initial sync completes.
	PauseOnInitialSync bool
	// IncludeNamespaces are the namespaces to include.
	IncludeNamespaces []string
	// ExcludeNamespaces are the namespaces to exclude.
	ExcludeNamespaces []string

	// Clone contains clone tuning options.
	Clone clone.Options
	// Repl contains replication behavior options.
	Repl repl.Options
}

// Start starts the replication process with the given options.
func (ml *PCSM) Start(_ context.Context, options *StartOptions) error {
	ml.lock.Lock()
	defer ml.lock.Unlock()

	switch ml.state {
	case StateRunning, StateFinalizing, StateFailed:
		err := errors.New("already running")
		log.New("pcsm:start").Error(err, "")

		return err

	case StatePaused:
		err := errors.New("paused")
		log.New("pcsm:start").Error(err, "")

		return err
	}

	if options == nil {
		options = &StartOptions{}
	}

	ml.nsInclude = options.IncludeNamespaces
	ml.nsExclude = options.ExcludeNamespaces
	ml.nsFilter = sel.MakeFilter(ml.nsInclude, ml.nsExclude)
	ml.pauseOnInitialSync = options.PauseOnInitialSync
	ml.catalog = catalog.NewCatalog(ml.target)
	ml.clone = clone.NewClone(ml.source, ml.target, ml.catalog, ml.nsFilter, &options.Clone)
	ml.repl = repl.NewRepl(ml.source, ml.target, ml.catalog, ml.nsFilter, &options.Repl)
	ml.state = StateRunning

	go ml.run()

	return nil
}

func (ml *PCSM) setFailed(err error) {
	ml.lock.Lock()
	ml.state = StateFailed
	ml.err = err
	ml.lock.Unlock()

	log.New("pcsm").Error(err, "Cluster Replication has failed")

	go ml.onStateChanged(StateFailed)
}

// run executes the cluster replication.
func (ml *PCSM) run() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lg := log.New("pcsm")

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

func (ml *PCSM) monitorInitialSync(ctx context.Context) {
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

func (ml *PCSM) monitorLagTime(ctx context.Context) {
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
func (ml *PCSM) Pause(ctx context.Context) error {
	ml.lock.Lock()
	defer ml.lock.Unlock()

	err := ml.doPause(ctx)
	if err != nil {
		log.New("pcsm").Error(err, "Pause Cluster Replication")

		return err
	}

	log.New("pcsm").Info("Cluster Replication paused")

	return nil
}

func (ml *PCSM) doPause(ctx context.Context) error {
	if ml.state != StateRunning {
		return errors.New("cannot pause: not running")
	}

	replStatus := ml.repl.Status()

	if !replStatus.IsRunning() {
		return errors.New("cannot pause: Change Replication is not running")
	}

	err := ml.repl.Pause(ctx)
	if err != nil {
		return errors.Wrap(err, "pause replication")
	}

	ml.state = StatePaused
	go ml.onStateChanged(StatePaused)

	return nil
}

type ResumeOptions struct {
	ResumeFromFailure bool
}

// Resume resumes the replication process.
func (ml *PCSM) Resume(ctx context.Context, options ResumeOptions) error {
	ml.lock.Lock()
	defer ml.lock.Unlock()

	if ml.state != StatePaused && (ml.state != StateFailed || !options.ResumeFromFailure) {
		return errors.New("cannot resume: not paused or not resuming from failure")
	}

	err := ml.doResume(ctx, options.ResumeFromFailure)
	if err != nil {
		log.New("pcsm").Error(err, "Resume Cluster Replication")

		return err
	}

	log.New("pcsm").Info("Cluster Replication resumed")

	return nil
}

func (ml *PCSM) doResume(_ context.Context, fromFailure bool) error {
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
func (ml *PCSM) Finalize(ctx context.Context, options FinalizeOptions) error {
	status := ml.Status(ctx)

	ml.lock.Lock()
	defer ml.lock.Unlock()

	if status.State == StateFailed {
		if !options.IgnoreHistoryLost || !errors.Is(status.Repl.Err, repl.ErrOplogHistoryLost) {
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

	lg := log.New("finalize")
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
			// no need to set the PCSM failed status here.
			// [PCSM.setFailed] is called in [PCSM.run].
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

	lg.Info("Finalizing")

	go ml.onStateChanged(StateFinalizing)

	return nil
}
