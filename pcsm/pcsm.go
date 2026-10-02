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
	"github.com/percona/percona-clustersync-mongodb/mdb"
	"github.com/percona/percona-clustersync-mongodb/metrics"
	"github.com/percona/percona-clustersync-mongodb/pcsm/catalog"
	"github.com/percona/percona-clustersync-mongodb/pcsm/clone"
	"github.com/percona/percona-clustersync-mongodb/pcsm/repl"
	"github.com/percona/percona-clustersync-mongodb/sel"
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
	Resume(ctx context.Context) error
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
	Recover(ctx context.Context, cp *repl.Checkpoint) error
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
	// FinalizeStatus is the status of the finalize stage.
	// It is non-nil once /finalize has been triggered (states: finalizing, finalized,
	// or failed after a finalize attempt).
	FinalizeStatus *FinalizeStatus
}

// FinalizeStatus describes the progress of a finalize run.
//
// While finalize is in flight (state == finalizing) Completed is false and
// CompletedAt is zero. When finalize completes successfully (state == finalized)
// Completed is true, CompletedAt is set, and UnsuccessfulIndexes is populated
// from the catalog.
type FinalizeStatus struct {
	// Completed indicates whether the finalize stage has finished successfully.
	Completed bool
	// StartedAt is when the finalize stage was triggered.
	StartedAt time.Time
	// CompletedAt is when the finalize stage finished. Zero unless Completed.
	CompletedAt time.Time
	// UnsuccessfulIndexes lists indexes that did not complete cleanly. Empty
	// until the finalize stage completes.
	UnsuccessfulIndexes []catalog.UnsuccessfulIndex
}

// catalogFinalizer is the part of the catalog that startFinalize drives.
// Tests substitute it to hold a finalizer open; production leaves
// PCSM.finalizer nil and finalizes through the catalog itself.
type catalogFinalizer interface {
	Finalize(ctx context.Context) []catalog.UnsuccessfulIndex
}

// ErrNotActive is returned by Start, Resume, Recover and Finalize when the
// ACTIVE epoch the request was admitted under has ended: the instance is no
// longer ACTIVE for that tenure and must not launch work.
var ErrNotActive = errors.New("instance is no longer active for this request's epoch")

type epochKey struct{}

// WithEpoch returns a ctx carrying epoch, the context of the ACTIVE tenure the
// caller was admitted under. Work launched from that ctx derives from epoch,
// so canceling the epoch ends it, and a launch is refused once the epoch is
// canceled. The ctx itself keeps bounding the call and its waits.
func WithEpoch(ctx, epoch context.Context) context.Context {
	return context.WithValue(ctx, epochKey{}, epoch)
}

// execHandle is one launched execution: its cancel and its completion.
type execHandle struct {
	cancel context.CancelFunc
	done   chan struct{}
}

// PCSM manages the replication process.
type PCSM struct {
	lifecycleCtx context.Context //nolint:containedctx // Lifecycle context for background operations

	source *mongo.Client // Source MongoDB client
	target *mongo.Client // Target MongoDB client

	sourceVer       mdb.ServerVersion
	sourceIsSharded bool
	targetIsSharded bool

	nsInclude []string
	nsExclude []string
	nsFilter  sel.NSFilter // Namespace filter

	onStateChanged OnStateChangedFunc // onStateChanged is invoked on each state change

	pauseOnInitialSync bool

	state State // Current state of the PCSM

	catalog   *catalog.Catalog // Catalog for managing collections and indexes
	clone     Cloner           // Clone process
	repl      Replicator       // Replication process
	finalizer catalogFinalizer // Test seam for startFinalize; nil means catalog

	// finalizeStatus tracks finalize-stage state. Nil until /finalize is triggered.
	finalizeStatus *FinalizeStatus
	// finalizeActive is true only while this process owns a finalizer goroutine.
	finalizeActive bool

	err error

	// runDone covers run and its monitors, not just the replication workers.
	// Component replacement and reuse must wait for this ownership to end.
	runDone chan struct{}
	lock    sync.Mutex

	// execMu guards the execution handles. It is never held across I/O or a
	// join: Suspend reads the handles, releases it, then cancels and joins.
	// Lock order is lock -> execMu, never the reverse.
	execMu       sync.Mutex
	runExec      *execHandle
	finalizeExec *execHandle

	// suspended is set by Suspend when it ended running work and cleared by
	// Start, doResume and Recover. Local, never persisted: it is what lets
	// doResume continue an unfinished clone, which a checkpoint-restored
	// interrupted clone must never do.
	suspended bool
}

// New creates a new PCSM.
func New(
	lifecycleCtx context.Context,
	source, target *mongo.Client,
	sourceVer mdb.ServerVersion,
	sourceIsSharded bool,
	targetIsSharded bool,
) *PCSM {
	return &PCSM{
		lifecycleCtx:    lifecycleCtx,
		source:          source,
		target:          target,
		sourceVer:       sourceVer,
		sourceIsSharded: sourceIsSharded,
		targetIsSharded: targetIsSharded,
		state:           StateIdle,
		onStateChanged:  func(State) {},
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

func (p *PCSM) Checkpoint(_ context.Context) ([]byte, error) {
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
	err := p.lockAfterRun(ctx)
	if err != nil {
		return err
	}
	defer p.lock.Unlock()

	// A finalizing pipeline whose finalizer was suspended holds no live work
	// and can be replaced like any other settled state.
	if p.state == StateRunning || (p.state == StateFinalizing && p.finalizeActive) ||
		(p.state == StatePaused && p.repl != nil && p.repl.Status().Pausing) {
		return errors.Errorf("cannot recover: invalid PCSM state %s", p.state)
	}

	parent, err := p.executionParent(ctx)
	if err != nil {
		return err
	}

	var cp checkpoint

	err = bson.Unmarshal(data, &cp)
	if err != nil {
		return errors.Wrap(err, "unmarshal")
	}

	if cp.State == StateIdle {
		return nil
	}

	nsFilter := sel.MakeFilter(cp.NSInclude, cp.NSExclude)
	cat := catalog.NewCatalog(p.source, p.target, p.sourceVer)
	// Use empty options for recovery (clone tuning is less relevant when resuming from checkpoint)
	cln := clone.NewClone(p.source, p.target, cat, nsFilter, &clone.Options{}, p.targetIsSharded)
	rpl := repl.NewRepl(
		p.source, p.target, cat, nsFilter, &repl.Options{},
		p.sourceVer, p.sourceIsSharded, p.targetIsSharded,
	)

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
		err = rpl.Recover(ctx, cp.Repl)
		if err != nil {
			return errors.Wrap(err, "recover repl")
		}
	}

	// Restore a minimal finalization status when the checkpoint represents a
	// completed finalize, so operators that restart the server between finalize
	// and reading /status still see at least Completed=true. StartedAt,
	// CompletedAt and UnsuccessfulIndexes are not persisted across restarts:
	// the per-index reasons are observed at finalize time and are not recorded
	// in the catalog.
	var finalizeStatus *FinalizeStatus
	if cp.State == StateFinalized {
		finalizeStatus = &FinalizeStatus{Completed: true}
	}

	p.nsInclude = cp.NSInclude
	p.nsExclude = cp.NSExclude
	p.nsFilter = nsFilter
	p.catalog = cat
	p.clone = cln
	p.repl = rpl
	p.finalizeStatus = finalizeStatus
	p.state = cp.State
	p.err = nil
	p.suspended = false

	if cp.Error != "" {
		p.err = errors.New(cp.Error)
	}

	if cp.State == StateRunning {
		// The initial clone is not resumable. If it was interrupted mid-flight
		// (started but not finished), fail with a clear reason. Recovery is a
		// fresh /start, which re-clones from scratch.
		cloneStatus := cln.Status()
		if cloneStatus.IsRunning() {
			err := errors.New(
				"initial clone interrupted by failover and is not resumable; " +
					"start a new run to re-clone from scratch",
			)
			p.state = StateFailed
			p.err = err

			log.New("pcsm").Error(err, "Cluster Replication has failed")

			go p.onStateChanged(StateFailed)

			return nil
		}

		// run(), not doResume: it handles both a checkpoint persisted at /start
		// time (repl not yet started) and one persisted after. doResume would
		// reject the not-yet-started case.
		p.startRun(parent)
		go p.onStateChanged(StateRunning)
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
		State:          p.state,
		Clone:          p.clone.Status(),
		Repl:           p.repl.Status(),
		FinalizeStatus: copyFinalizeStatus(p.finalizeStatus),
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

	sourceTime, err := mdb.ClusterTime(ctx, p.source)
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

// copyFinalizeStatus returns a copy of fs with a fresh UnsuccessfulIndexes
// slice so callers can append or reorder without mutating the source. The
// bson.Raw Keys field inside each entry still aliases the source entry's
// bytes; do not mutate Keys in place.
func copyFinalizeStatus(fs *FinalizeStatus) *FinalizeStatus {
	if fs == nil {
		return nil
	}

	out := *fs

	if len(fs.UnsuccessfulIndexes) > 0 {
		out.UnsuccessfulIndexes = make([]catalog.UnsuccessfulIndex, len(fs.UnsuccessfulIndexes))
		copy(out.UnsuccessfulIndexes, fs.UnsuccessfulIndexes)
	}

	return &out
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
func (p *PCSM) Start(ctx context.Context, options *StartOptions) error {
	err := p.lockAfterRun(ctx)
	if err != nil {
		return err
	}
	defer p.lock.Unlock()

	switch p.state {
	case StateRunning, StateFinalizing:
		err := errors.New("already running")
		log.New("pcsm:start").Error(err, "")

		return err

	case StateFailed:
		// Allow a fresh /start (re-clone from scratch) only if the clone never
		// finished. A failure after the clone completed is a repl-phase failure,
		// recovered with resume --from-failure, so it is still rejected here.
		if p.clone != nil {
			cloneStatus := p.clone.Status()
			if cloneStatus.IsFinished() {
				err := errors.New("already running")
				log.New("pcsm:start").Error(err, "")

				return err
			}
		}

	case StatePaused:
		err := errors.New("paused")
		log.New("pcsm:start").Error(err, "")

		return err
	}

	if options == nil {
		options = &StartOptions{}
	}

	parent, err := p.executionParent(ctx)
	if err != nil {
		return err
	}

	p.err = nil
	p.suspended = false

	p.nsInclude = options.IncludeNamespaces
	p.nsExclude = options.ExcludeNamespaces
	p.nsFilter = sel.MakeFilter(p.nsInclude, p.nsExclude)
	p.pauseOnInitialSync = options.PauseOnInitialSync
	p.catalog = catalog.NewCatalog(p.source, p.target, p.sourceVer)
	p.clone = clone.NewClone(p.source, p.target, p.catalog, p.nsFilter, &options.Clone, p.targetIsSharded)
	p.repl = repl.NewRepl(
		p.source, p.target, p.catalog, p.nsFilter, &options.Repl,
		p.sourceVer, p.sourceIsSharded, p.targetIsSharded,
	)
	p.finalizeStatus = nil
	p.state = StateRunning

	p.startRun(parent)

	// Persist idle->running immediately: a crash before the first periodic
	// checkpoint would otherwise leave no recovery data to resume from.
	go p.onStateChanged(StateRunning)

	return nil
}

// lockAfterRun returns with p.lock held on success. A stopped run may still be
// reporting failure or exiting its monitors; let it finish before validating
// the next transition. Active states are left for the caller to reject.
func (p *PCSM) lockAfterRun(ctx context.Context) error {
	p.lock.Lock()

	for p.runDone != nil && p.state != StateRunning && p.state != StateFinalizing {
		done := p.runDone
		p.lock.Unlock()

		select {
		case <-ctx.Done():
			return errors.Wrap(ctx.Err(), "wait for previous run")
		case <-done:
		}

		p.lock.Lock()
		// Another caller may have started a run while the lock was released.
	}

	return nil
}

// executionParent returns the context new work derives from: the epoch
// carried by ctx when there is one, otherwise the lifecycle context. A
// canceled epoch is refused with ErrNotActive, before any state changes.
func (p *PCSM) executionParent(ctx context.Context) (context.Context, error) {
	epoch, ok := ctx.Value(epochKey{}).(context.Context)
	if !ok || epoch == nil {
		return p.lifecycleCtx, nil
	}

	if epoch.Err() != nil {
		return nil, ErrNotActive
	}

	return epoch, nil
}

// startRun registers ownership before launching the goroutine. The caller holds
// p.lock and has joined any previous run through lockAfterRun. The run derives
// from parent, so canceling the epoch ends it; its handle is published before
// launch and cleared only by its own generation.
func (p *PCSM) startRun(parent context.Context) {
	done := make(chan struct{})
	p.runDone = done

	execCtx, cancel := context.WithCancel(parent)

	p.execMu.Lock()
	p.runExec = &execHandle{cancel: cancel, done: done}
	p.execMu.Unlock()

	go func() {
		defer cancel()

		p.run(execCtx)

		p.execMu.Lock()
		if p.runExec != nil && p.runExec.done == done {
			p.runExec = nil
		}
		p.execMu.Unlock()

		p.lock.Lock()
		p.runDone = nil
		close(done)
		p.lock.Unlock()
	}()
}

// notifyStateChanged invokes the state-change callback the way every
// transition does, for a caller that does not hold p.lock.
func (p *PCSM) notifyStateChanged(state State) {
	p.lock.Lock()
	f := p.onStateChanged
	p.lock.Unlock()

	go f(state)
}

// Suspend ends the work of an instance that is no longer ACTIVE: it cancels
// the current run and finalizer, joins them, then settles the state. A run
// becomes paused and is marked suspended so a same-term re-promotion can
// resume it in memory; a finalizer leaves the state finalizing for an
// explicit /finalize. It reports whether it suspended running work, so an
// operator pause or a failed pipeline is never mistaken for a suspension.
// Nothing is persisted: no state-change notification is sent.
//
// Cancellation never waits for a lock: the handles are read under execMu and
// released before the join, so a Finalize blocked on the drain under p.lock
// is unblocked by it. On ctx expiry before the join completes it returns an
// error and changes nothing.
func (p *PCSM) Suspend(ctx context.Context) (bool, error) {
	for {
		p.execMu.Lock()
		run, finalize := p.runExec, p.finalizeExec
		p.execMu.Unlock()

		if run == nil && finalize == nil {
			break
		}

		// A joined execution clears its handle before closing done, so the
		// next read sees only work launched meanwhile.
		for _, h := range []*execHandle{run, finalize} {
			if h == nil {
				continue
			}

			h.cancel()

			select {
			case <-h.done:
			case <-ctx.Done():
				return false, errors.Wrap(ctx.Err(), "suspend: wait for the execution to stop")
			}
		}
	}

	p.lock.Lock()
	defer p.lock.Unlock()

	lg := log.New("pcsm")

	switch p.state {
	case StateRunning:
		// A canceled run leaves the state to its suspender: the clone is
		// resumable and replication is paused at its inclusive floor.
		p.state = StatePaused
		p.suspended = true

		lg.Info("Cluster Replication suspended")

		return true, nil

	case StateFinalizing:
		if !p.finalizeActive {
			p.suspended = true

			lg.Info("Finalization suspended")

			return true, nil
		}

		return false, nil

	default:
		return false, nil
	}
}

func (p *PCSM) setFailed(err error) {
	p.lock.Lock()
	p.state = StateFailed
	p.err = err
	go p.onStateChanged(StateFailed)
	p.lock.Unlock()

	log.New("pcsm").Error(err, "Cluster Replication has failed")
}

// run executes the cluster replication.
func (p *PCSM) run(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	var monitors sync.WaitGroup
	defer func() {
		cancel()
		monitors.Wait()
	}()

	lg := log.New("pcsm")

	lg.Info("Starting Cluster Replication")

	cloneStatus, ok := p.runClone(ctx)
	if !ok {
		return
	}

	replStatus := p.repl.Status()
	if !p.startRepl(ctx, &replStatus, &cloneStatus) {
		return
	}

	if replStatus.LastReplicatedOpTime.Before(cloneStatus.FinishTS) {
		monitors.Go(func() { p.monitorInitialSync(ctx) })
	}
	monitors.Go(func() { p.monitorLagTime(ctx) })

	<-p.repl.Done()

	replStatus = p.repl.Status()
	if replStatus.Err != nil {
		p.setFailed(errors.Wrap(replStatus.Err, "change replication"))
	}
}

// runClone runs the initial clone when it has not finished, resuming a
// suspended one, and reports whether replication may start. A recorded clone
// error is a failure even when a suspension raced it; a clone that ended on
// cancellation alone records none, and the suspender settles the state.
func (p *PCSM) runClone(ctx context.Context) (clone.Status, bool) {
	cloneStatus := p.clone.Status()
	if cloneStatus.IsFinished() {
		return cloneStatus, true
	}

	var err error
	if cloneStatus.IsStarted() {
		err = errors.Wrap(p.clone.Resume(ctx), "resume clone")
	} else {
		err = errors.Wrap(p.clone.Start(ctx), "start clone")
	}

	if err != nil {
		p.setFailed(err)

		return cloneStatus, false
	}

	<-p.clone.Done()

	cloneStatus = p.clone.Status()
	if cloneStatus.Err != nil {
		p.setFailed(errors.Wrap(cloneStatus.Err, "clone"))

		return cloneStatus, false
	}

	if ctx.Err() != nil {
		return cloneStatus, false
	}

	// Persist the completed clone before replication starts, so a restore in
	// the next tenure never takes it for an interrupted one.
	p.notifyStateChanged(StateRunning)

	return cloneStatus, true
}

// startRepl starts or resumes change replication and reports whether it is
// running. A failure while the execution is being canceled is the suspension,
// not a pipeline failure.
func (p *PCSM) startRepl(ctx context.Context, replStatus *repl.Status, cloneStatus *clone.Status) bool {
	var err error
	if replStatus.IsStarted() {
		err = errors.Wrap(p.repl.Resume(ctx), "resume change replication")
	} else {
		err = errors.Wrap(p.repl.Start(ctx, cloneStatus.StartTS), "start change replication")
	}

	if err == nil {
		return true
	}

	if ctx.Err() == nil {
		p.setFailed(err)
	}

	return false
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

		sourceTS, err := mdb.ClusterTime(ctx, p.source)
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
	err := p.lockAfterRun(ctx)
	if err != nil {
		return err
	}
	defer p.lock.Unlock()

	if p.state != StatePaused && (p.state != StateFailed || !options.ResumeFromFailure) {
		return errors.New("cannot resume: not paused or not resuming from failure")
	}

	parent, err := p.executionParent(ctx)
	if err != nil {
		return err
	}

	err = p.doResume(parent, options.ResumeFromFailure)
	if err != nil {
		log.New("pcsm").Error(err, "Resume Cluster Replication")

		return err
	}

	log.New("pcsm").Info("Cluster Replication resumed")

	return nil
}

func (p *PCSM) doResume(parent context.Context, fromFailure bool) error {
	replStatus := p.repl.Status()
	cloneStatus := p.clone.Status()

	// A run suspended before replication started (clone in flight, or clone
	// finished but not handed over) resumes through run, which continues the
	// clone and starts replication. A checkpoint-restored interrupted clone
	// never qualifies: Recover clears suspended.
	suspendedBeforeRepl := p.suspended && !replStatus.IsStarted() &&
		cloneStatus.IsStarted() && cloneStatus.Err == nil

	if !replStatus.IsStarted() && !suspendedBeforeRepl && !fromFailure {
		return errors.New("cannot resume: replication is not started or not resuming from failure")
	}

	if !replStatus.IsPaused() && fromFailure {
		return errors.New("cannot resume: replication is not paused or not resuming from failure")
	}

	p.state = StateRunning
	p.suspended = false
	p.resetError()

	p.startRun(parent)
	go p.onStateChanged(StateRunning)

	return nil
}

// Finalize finalizes the replication process.
func (p *PCSM) Finalize(ctx context.Context) error {
	status := p.Status(ctx)

	parent, err := p.executionParent(ctx)
	if err != nil {
		return err
	}

	p.lock.Lock()
	defer p.lock.Unlock()

	if p.finalizeActive {
		return errors.New("finalization is already in progress")
	}

	lg := log.New("finalize")

	// A recovered or suspended "finalizing" pipeline passed these checks
	// before; only catalog finalization is left to resume.
	if p.state == StateFinalizing {
		lg.Info("Resuming Finalization")
	} else {
		err = checkFinalizePreconditions(status)
		if err != nil {
			return err
		}

		lg.Info("Starting Finalization")
	}

	// Decide from the live repl status under the lock, not the pre-lock
	// snapshot above. With PauseOnInitialSync, monitorInitialSync can pause
	// repl concurrently; a stale "running" snapshot would make us call Pause
	// on an already-pausing/paused repl and fail. A non-running repl has either
	// never run in this process or has already closed Done before recording its
	// pause time, so only a running repl needs to be awaited.
	replStatus := p.repl.Status()
	if replStatus.IsRunning() {
		if !replStatus.Pausing {
			lg.Info("Pausing Change Replication")

			err = p.repl.Pause(ctx)
			if err != nil {
				return errors.Wrap(err, "pause change replication")
			}
		}

		<-p.repl.Done()
	}

	// The drain may have ended because the epoch was canceled: an instance
	// that lost the lease meanwhile must not start catalog work. Suspend
	// settles the state once this returns.
	if parent.Err() != nil {
		return ErrNotActive
	}

	lg.Info("Change Replication is paused")

	err = p.repl.Status().Err
	if err != nil {
		// no need to set the PCSM failed status here.
		// [PCSM.setFailed] is called in [PCSM.run].
		return errors.Wrap(err, "post-pause change replication")
	}

	p.startFinalize(parent, lg)

	return nil
}

// checkFinalizePreconditions reports why the pipeline cannot start finalization.
func checkFinalizePreconditions(status *Status) error {
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

	return nil
}

// startFinalize launches catalog finalization on an execution derived from
// parent. The caller must hold p.lock. A finalizer ended by cancellation did
// not finish: Completed stays false and the state stays finalizing, so an
// explicit /finalize after a promotion runs it again.
func (p *PCSM) startFinalize(parent context.Context, lg log.Logger) {
	p.finalizeStatus = &FinalizeStatus{StartedAt: time.Now()}
	p.finalizeActive = true
	p.state = StateFinalizing

	finalizer := p.finalizer
	if finalizer == nil {
		finalizer = p.catalog
	}

	execCtx, cancel := context.WithCancel(parent)
	done := make(chan struct{})

	p.execMu.Lock()
	p.finalizeExec = &execHandle{cancel: cancel, done: done}
	p.execMu.Unlock()

	go func() {
		defer func() {
			cancel()

			p.execMu.Lock()
			if p.finalizeExec != nil && p.finalizeExec.done == done {
				p.finalizeExec = nil
			}
			p.execMu.Unlock()

			close(done)
		}()

		unsuccessful := finalizer.Finalize(execCtx)

		p.lock.Lock()
		if execCtx.Err() != nil {
			p.finalizeActive = false
			p.lock.Unlock()

			lg.Warn("Finalization interrupted; reissue /finalize once ACTIVE")

			return
		}

		p.finalizeStatus.UnsuccessfulIndexes = unsuccessful
		p.finalizeStatus.CompletedAt = time.Now()
		p.finalizeStatus.Completed = true
		p.finalizeActive = false
		p.state = StateFinalized
		startedAt := p.finalizeStatus.StartedAt
		p.lock.Unlock()

		lg.With(log.Elapsed(time.Since(startedAt))).
			Info("Finalization is completed")

		go p.onStateChanged(StateFinalized)
	}()

	lg.Info("Finalizing")

	go p.onStateChanged(StateFinalizing)
}
