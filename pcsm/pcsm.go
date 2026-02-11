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

func (p *PCSM) Checkpoint(context.Context) ([]byte, error) {
	p.lock.Lock()
	defer p.lock.Unlock()

	if p.state == StateIdle {
		return nil, nil
	}

	// prevent catalog changes during checkpoint
	p.catalog.LockWrite()
	defer p.catalog.UnlockWrite()

	cp := &checkpoint{
		NSInclude: p.nsInclude,
		NSExclude: p.nsExclude,

		Catalog: p.catalog.Checkpoint(),
		Clone:   p.clone.Checkpoint(),
		Repl:    p.repl.Checkpoint(),

		State: p.state,
	}

	if p.err != nil {
		cp.Error = p.err.Error()
	}

	return bson.Marshal(cp) //nolint:wrapcheck
}

func (p *PCSM) Recover(ctx context.Context, data []byte) error {
	p.lock.Lock()
	defer p.lock.Unlock()

	if p.state != StateIdle {
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
	cat := catalog.NewCatalog(p.target)
	// Use empty options for recovery (clone tuning is less relevant when resuming from checkpoint)
	cln := clone.NewClone(p.source, p.target, cat, nsFilter, &clone.Options{})
	rpl := repl.NewRepl(p.source, p.target, cat, nsFilter, &repl.Options{})

	if cp.Catalog != nil {
		err = cat.Recover(cp.Catalog)
		if err != nil {
			return errors.Wrap(err, "recover catalog")
		}
	}

	if cp.Clone != nil {
		err = cln.Recover(cp.Clone)
		if err != nil {
			return errors.Wrap(err, "recover clone")
		}
	}

	if cp.Repl != nil {
		err = rpl.Recover(cp.Repl)
		if err != nil {
			return errors.Wrap(err, "recover repl")
		}
	}

	p.nsInclude = cp.NSInclude
	p.nsExclude = cp.NSExclude
	p.nsFilter = nsFilter
	p.catalog = cat
	p.clone = cln
	p.repl = rpl
	p.state = cp.State

	if cp.Error != "" {
		p.err = errors.New(cp.Error)
	}

	if cp.State == StateRunning {
		return p.doResume(ctx, false)
	}

	return nil
}

// SetOnStateChanged set the f function to be called on each state change.
func (p *PCSM) SetOnStateChanged(f OnStateChangedFunc) {
	if f == nil {
		f = func(State) {}
	}

	p.lock.Lock()
	p.onStateChanged = f
	p.lock.Unlock()
}

// Status returns the current status of the PCSM.
func (p *PCSM) Status(ctx context.Context) *Status {
	p.lock.Lock()
	defer p.lock.Unlock()

	if p.state == StateIdle {
		return &Status{State: StateIdle}
	}

	s := &Status{
		State: p.state,
		Clone: p.clone.Status(),
		Repl:  p.repl.Status(),
	}

	switch {
	case p.err != nil:
		s.Error = p.err
	case s.Repl.Err != nil:
		s.Error = errors.Wrap(s.Repl.Err, "Change Replication")
	case s.Clone.Err != nil:
		s.Error = errors.Wrap(s.Clone.Err, "Clone")
	}

	if s.Repl.IsStarted() {
		s.InitialSyncCompleted = s.Repl.LastReplicatedOpTime.After(s.Clone.FinishTS)
	}

	if p.state == StateFailed {
		return s
	}

	sourceTime, err := topo.ClusterTime(ctx, p.source)
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

func (p *PCSM) resetError() {
	p.err = nil
	p.clone.ResetError()
	p.repl.ResetError()
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
func (p *PCSM) Start(_ context.Context, options *StartOptions) error {
	p.lock.Lock()
	defer p.lock.Unlock()

	switch p.state {
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

	p.nsInclude = options.IncludeNamespaces
	p.nsExclude = options.ExcludeNamespaces
	p.nsFilter = sel.MakeFilter(p.nsInclude, p.nsExclude)
	p.pauseOnInitialSync = options.PauseOnInitialSync
	p.catalog = catalog.NewCatalog(p.target)
	p.clone = clone.NewClone(p.source, p.target, p.catalog, p.nsFilter, &options.Clone)
	p.repl = repl.NewRepl(p.source, p.target, p.catalog, p.nsFilter, &options.Repl)
	p.state = StateRunning

	go p.run()

	return nil
}

func (p *PCSM) setFailed(err error) {
	p.lock.Lock()
	p.state = StateFailed
	p.err = err
	p.lock.Unlock()

	log.New("pcsm").Error(err, "Cluster Replication has failed")

	go p.onStateChanged(StateFailed)
}

// run executes the cluster replication.
func (p *PCSM) run() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lg := log.New("pcsm")

	lg.Info("Starting Cluster Replication")

	cloneStatus := p.clone.Status()
	if !cloneStatus.IsFinished() {
		err := p.clone.Start(ctx)
		if err != nil {
			p.setFailed(errors.Wrap(cloneStatus.Err, "start clone"))

			return
		}

		<-p.clone.Done()

		cloneStatus = p.clone.Status()
		if cloneStatus.Err != nil {
			p.setFailed(errors.Wrap(cloneStatus.Err, "clone"))

			return
		}
	}

	replStatus := p.repl.Status()
	if !replStatus.IsStarted() {
		err := p.repl.Start(ctx, cloneStatus.StartTS)
		if err != nil {
			p.setFailed(errors.Wrap(err, "start change replication"))

			return
		}
	} else {
		err := p.repl.Resume(ctx)
		if err != nil {
			p.setFailed(errors.Wrap(err, "resume change replication"))

			return
		}
	}

	if replStatus.LastReplicatedOpTime.Before(cloneStatus.FinishTS) {
		go p.monitorInitialSync(ctx)
	}
	go p.monitorLagTime(ctx)

	<-p.repl.Done()

	replStatus = p.repl.Status()
	if replStatus.Err != nil {
		p.setFailed(errors.Wrap(replStatus.Err, "change replication"))
	}
}

func (p *PCSM) monitorInitialSync(ctx context.Context) {
	lg := log.New("monitor:initial-sync-lag-time")

	t := time.NewTicker(time.Second)
	defer t.Stop()

	cloneStatus := p.clone.Status()
	if cloneStatus.Err != nil {
		return
	}

	replStatus := p.repl.Status()
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

		replStatus = p.repl.Status()
		if replStatus.LastReplicatedOpTime.After(cloneStatus.FinishTS) {
			elapsed := time.Since(replStatus.StartTime)
			lg.With(log.Elapsed(elapsed)).
				Infof("Clone event backlog processed in %s", elapsed.Round(time.Second))
			elapsed = time.Since(cloneStatus.StartTime)
			lg.With(log.Elapsed(elapsed)).
				Infof("Initial Sync completed in %s", elapsed.Round(time.Second))

			p.lock.Lock()
			pauseOnInitialSync := p.pauseOnInitialSync
			p.lock.Unlock()

			if pauseOnInitialSync {
				lg.Info("Pausing [PauseOnInitialSync]")

				err := p.Pause(ctx)
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

func (p *PCSM) monitorLagTime(ctx context.Context) {
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

		sourceTS, err := topo.ClusterTime(ctx, p.source)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}

			lg.Error(err, "source cluster time")

			continue
		}

		replStatus := p.repl.Status()
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
func (p *PCSM) Pause(ctx context.Context) error {
	p.lock.Lock()
	defer p.lock.Unlock()

	err := p.doPause(ctx)
	if err != nil {
		log.New("pcsm").Error(err, "Pause Cluster Replication")

		return err
	}

	log.New("pcsm").Info("Cluster Replication paused")

	return nil
}

func (p *PCSM) doPause(ctx context.Context) error {
	if p.state != StateRunning {
		return errors.New("cannot pause: not running")
	}

	replStatus := p.repl.Status()

	if !replStatus.IsRunning() {
		return errors.New("cannot pause: Change Replication is not running")
	}

	err := p.repl.Pause(ctx)
	if err != nil {
		return errors.Wrap(err, "pause replication")
	}

	p.state = StatePaused
	go p.onStateChanged(StatePaused)

	return nil
}

type ResumeOptions struct {
	ResumeFromFailure bool
}

// Resume resumes the replication process.
func (p *PCSM) Resume(ctx context.Context, options ResumeOptions) error {
	p.lock.Lock()
	defer p.lock.Unlock()

	if p.state != StatePaused && (p.state != StateFailed || !options.ResumeFromFailure) {
		return errors.New("cannot resume: not paused or not resuming from failure")
	}

	err := p.doResume(ctx, options.ResumeFromFailure)
	if err != nil {
		log.New("pcsm").Error(err, "Resume Cluster Replication")

		return err
	}

	log.New("pcsm").Info("Cluster Replication resumed")

	return nil
}

func (p *PCSM) doResume(_ context.Context, fromFailure bool) error {
	replStatus := p.repl.Status()

	if !replStatus.IsStarted() && !fromFailure {
		return errors.New("cannot resume: replication is not started or not resuming from failure")
	}

	if !replStatus.IsPaused() && fromFailure {
		return errors.New("cannot resume: replication is not paused or not resuming from failure")
	}

	p.state = StateRunning
	p.resetError()

	go p.run()
	go p.onStateChanged(StateRunning)

	return nil
}

// Finalize finalizes the replication process.
func (p *PCSM) Finalize(ctx context.Context) error {
	status := p.Status(ctx)

	p.lock.Lock()
	defer p.lock.Unlock()

	if status.State == StateFailed {
		return errors.Wrap(status.Error, "failed state")
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

		err := p.repl.Pause(ctx)
		if err != nil {
			return errors.Wrap(err, "pause change replication")
		}

		<-p.repl.Done()
		lg.Info("Change Replication is paused")

		err = p.repl.Status().Err
		if err != nil {
			// no need to set the PCSM failed status here.
			// [PCSM.setFailed] is called in [PCSM.run].
			return errors.Wrap(err, "post-pause change replication")
		}
	}

	startedTime := time.Now()
	p.state = StateFinalizing

	go func() {
		err := p.catalog.Finalize(context.Background())
		if err != nil {
			p.setFailed(errors.Wrap(err, "finalization"))

			return
		}

		p.lock.Lock()
		p.state = StateFinalized
		p.lock.Unlock()

		lg.With(log.Elapsed(time.Since(startedTime))).
			Info("Finalization is completed")

		go p.onStateChanged(StateFinalized)
	}()

	lg.Info("Finalizing")

	go p.onStateChanged(StateFinalizing)

	return nil
}
