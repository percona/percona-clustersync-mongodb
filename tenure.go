package main

import (
	"context"

	"github.com/percona/percona-clustersync-mongodb/errors"
	"github.com/percona/percona-clustersync-mongodb/ha"
	"github.com/percona/percona-clustersync-mongodb/log"
	"github.com/percona/percona-clustersync-mongodb/pcsm"
)

// tenure is one ACTIVE epoch of this process: the lease term it opened at, the
// context every execution of it derives from, and the pipeline built for it.
// A tenure ends as a whole, on any loss of ownership or when its checkpoint
// writer ends, and nothing of it is reused: the next tenure builds a new
// pipeline and restores it from the checkpoint, the one recovery path, the
// same one a restarted process takes.
// Reacquiring the lease in the same process is not permission to resume its
// old in-memory pipeline: it must recover the persisted state just as another
// instance would. An interrupted clone therefore needs a new /start.
type tenure struct {
	term     ha.Term
	ctx      context.Context //nolint:containedctx // the epoch is the tenure's identity; see server.tenure
	cancel   context.CancelFunc
	pipeline *pcsm.PCSM
	// ready is closed once the pipeline is restored and checkpointing runs;
	// requests are admitted from then on.
	ready chan struct{}
	// done is closed when the goroutine that opened the tenure has returned.
	done chan struct{}
}

func (t *tenure) isReady() bool {
	select {
	case <-t.ready:
		return true
	default:
		return false
	}
}

// ownershipObserver adapts the lifecycle to the lease loop's Observer.
func (s *server) ownershipObserver(ctx context.Context) ha.Observer {
	return func(o ha.Ownership) { s.observeOwnership(ctx, o) }
}

// observeOwnership reconciles the lifecycle with the ownership the lease loop
// reports. The report is a level, not a transition: a tenure open at the
// reported term is a renewal and nothing happens; any other open tenure ends;
// ownership at an acceptable term with no tenure open opens one. A report
// that ended a tenure opens nothing itself: ending one joins its work, and
// the report may be older than the lease by then, so the next report, a
// fresh level, decides. Ownership below minTerm is advanced through the lease
// instead, so the tenure that follows gets a term no earlier one used.
func (s *server) observeOwnership(ctx context.Context, o ha.Ownership) {
	lg := log.New("ha:role").With(log.Int64("term", int64(o.Term)))

	s.mu.Lock()
	defer s.mu.Unlock()

	if o.Held {
		s.term = o.Term
	}

	if s.tenure != nil {
		if o.Held && o.Term == s.tenure.term {
			return
		}

		if o.Held {
			lg.With(log.Int64("tenureTerm", int64(s.tenure.term))).
				Warn("Instance role: STANDBY (lease held at another term than the tenure's)")
		} else {
			lg.Info("Instance role: STANDBY (lease lost)")
		}

		s.endTenure(ctx, lg)

		return
	}

	if !o.Held {
		s.publishRole()

		return
	}

	if o.Term < s.minTerm {
		advanced, err := s.membership.AdvanceTerm(ctx, s.minTerm)
		switch {
		case err != nil:
			lg.Error(err, "advance the lease term for a new tenure")
		case advanced:
			lg.With(log.Int64("minTerm", int64(s.minTerm))).
				Info("Lease term advanced for a new tenure; the next renewal opens it")
		default:
			lg.Debug("Lease no longer owned; nothing to advance")
		}

		s.publishRole()

		return
	}

	// A report that lands between Close ending the tenure and Release
	// stopping the loop must not open a tenure on a server that is shutting down.
	if ctx.Err() != nil {
		s.publishRole()

		return
	}

	s.openTenure(ctx, o.Term, lg)
}

// publishRole advertises the role the lifecycle derived, ACTIVE while a
// tenure is open, with the last term held, when either differs from what is
// advertised. The caller holds mu.
func (s *server) publishRole() {
	role := ha.RoleStandby
	if s.tenure != nil {
		role = ha.RoleActive
	}

	if current, term := s.membership.CurrentRole(); current == role && term == s.term {
		return
	}

	s.membership.SetRole(role, s.term)
}

// openTenure opens a tenure at term with a new pipeline and launches its
// work. The caller holds mu.
func (s *server) openTenure(ctx context.Context, term ha.Term, lg log.Logger) {
	tctx, cancel := context.WithCancel(ctx)
	t := &tenure{
		term:   term,
		ctx:    tctx,
		cancel: cancel,
		pipeline: pcsm.New(tctx, s.sourceCluster, s.targetCluster,
			s.sourceVersion, s.sourceIsMongos, s.targetIsMongos),
		ready: make(chan struct{}),
		done:  make(chan struct{}),
	}

	s.tenure = t
	s.minTerm = term + 1
	s.publishRole()

	lg.Info("Instance role: ACTIVE (promoted)")

	go s.runTenure(ctx, t)
}

// runTenure does the work of opening tenure t: it restores the pipeline from
// the checkpoint, starts the tenure's checkpoint writer, applies the deferred
// --start and then admits requests. It never takes mu, since endTenure joins
// it under mu; an outcome it must act on is handed to a goroutine of its own.
func (s *server) runTenure(ctx context.Context, t *tenure) {
	defer close(t.done)

	lg := log.New("ha:role").With(log.Int64("term", int64(t.term)))
	ectx := pcsm.WithEpoch(t.ctx, t.ctx)

	// A state change signals this tenure's checkpoint writer, coalescing
	// pending saves. Installed before Restore, which emits transitions; a
	// signal sent before the writer starts waits for it.
	saveNow := make(chan struct{}, 1)
	t.pipeline.SetOnStateChanged(func(_ pcsm.State) {
		if t.ctx.Err() != nil {
			return
		}

		select {
		case saveNow <- struct{}{}:
		default: // a save is already pending and captures this state too
		}
	})

	err := Restore(ectx, s.targetCluster, t.pipeline, int64(t.term), s.membership.InstanceID())
	switch {
	case err == nil, errors.Is(err, errNoCheckpoint):

	case errors.Is(err, errCheckpointFenced):
		go s.onCheckpointLost(ctx, t, err)

		return

	case t.ctx.Err() != nil:
		return

	default:
		lg.Error(err, "restore on promotion; relinquishing the lease")
		go s.relinquish(ctx, t)

		return
	}

	// The tenure's checkpoint writer. It ends on the first write it cannot
	// account for, and the tenure ends with it.
	go func() {
		err := RunCheckpointing(t.ctx, s.targetCluster, t.pipeline, int64(t.term),
			s.membership.InstanceID(), s.cfg.RecoveryCheckpointInterval, saveNow)
		if err != nil {
			s.onCheckpointLost(ctx, t, err)
		}
	}()

	// Apply the deferred --start now that this instance is ACTIVE, before any
	// request is admitted, so a /start cannot pre-empt the configured options.
	// Only valid from idle: a restored non-idle pipeline must not be restarted.
	if s.autoStartOpts != nil {
		s.autoStart(ectx, t, lg)
	}

	close(t.ready)
}

// autoStart applies the deferred --start to tenure t's pipeline, when it is
// idle.
func (s *server) autoStart(ectx context.Context, t *tenure, lg log.Logger) {
	state := t.pipeline.Status(ectx).State
	if state != pcsm.StateIdle {
		lg.With(log.String("state", string(state))).
			Info("Skipping --start on promotion: replication already in progress")

		return
	}

	err := t.pipeline.Start(ectx, s.autoStartOpts)
	if err != nil {
		lg.Error(err, "auto-start on promotion")
	}
}

// endTenure ends the current tenure, if any: it cancels the epoch, advertises
// STANDBY, then joins the goroutine that opened the tenure and the pipeline's
// executions, and drops the pipeline. Cancellation contains a deposed
// instance's writes; a command already on the wire may still apply. The
// caller holds mu, also while joining: nothing of the tenure may be reused,
// so no replacement opens before the join. Cancellation makes the join short
// except where a component finishes its own cleanup on a bounded context of
// its own; a join cut short by ctx is logged, and the tenure is over either
// way, since every execution derives from the canceled epoch.
func (s *server) endTenure(ctx context.Context, lg log.Logger) {
	t := s.tenure
	if t == nil {
		return
	}

	s.tenure = nil
	t.cancel()
	s.publishRole()

	select {
	case <-t.done:
	case <-ctx.Done():
		lg.Warn("Ending the tenure without waiting for its opener to return")
	}

	err := t.pipeline.Stop(ctx)
	if err != nil {
		lg.Error(err, "stop the pipeline of the ended tenure")
	}
}

// onCheckpointLost handles the end of tenure t's claim or checkpoint writer:
// a write of t's term was fenced by a newer term, or returned an error that
// the writer treats as unsettled. Either way no further write of t's term may
// be issued, so t is over. A report for a tenure that already ended ends
// nothing: the current tenure opened above t's term and claimed the
// checkpoint for itself.
func (s *server) onCheckpointLost(ctx context.Context, t *tenure, err error) {
	lg := log.New("ha:role").With(log.Int64("term", int64(t.term)))

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.tenure != t {
		lg.Debug("Ignoring the lost checkpoint of an ended tenure")

		return
	}

	if errors.Is(err, errCheckpointFenced) {
		lg.Info("Instance role: STANDBY (demoted: checkpoint fenced by a newer term)")
	} else {
		lg.Error(err, "Instance role: STANDBY (checkpoint write outcome unknown)")
	}

	s.endTenure(ctx, lg)
}

// relinquish ends tenure t, while it is still current, and gives the lease up
// so another instance, or this one at a later term, can take over.
func (s *server) relinquish(ctx context.Context, t *tenure) {
	lg := log.New("ha:role").With(log.Int64("term", int64(t.term)))

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.tenure != t {
		return
	}

	s.endTenure(ctx, lg)

	err := s.membership.RelinquishLease(ctx)
	if err != nil {
		lg.Error(err, "relinquish lease")
	}
}
