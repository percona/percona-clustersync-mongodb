package pcsm

import (
	"context"
	"fmt"
	"slices"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"

	"github.com/percona/percona-clustersync-mongodb/config"
	"github.com/percona/percona-clustersync-mongodb/errors"
	"github.com/percona/percona-clustersync-mongodb/log"
	"github.com/percona/percona-clustersync-mongodb/pcsm/catalog"
	"github.com/percona/percona-clustersync-mongodb/pcsm/clone"
	"github.com/percona/percona-clustersync-mongodb/pcsm/repl"
	"github.com/percona/percona-clustersync-mongodb/sel"
)

var (
	errStartPending = errors.New("a start is waiting for its checkpoint")
	errStartSaved   = errors.New("start checkpoint saved but not launched here; check /status on ACTIVE before retrying")
)

// Start starts replication once its initial checkpoint is acknowledged.
func (p *PCSM) Start(ctx context.Context, options *StartOptions) error {
	err := p.lockAfterRun(ctx, false)
	if err != nil {
		return err
	}

	err = p.checkStartStateLocked()
	if err != nil {
		p.lock.Unlock()
		log.New("pcsm:start").Error(err, "")

		return err
	}

	parent, err := p.executionParent(ctx)
	if err != nil {
		p.lock.Unlock()

		return err
	}

	var opts StartOptions
	if options != nil {
		opts = *options
		// The writer may install these after the caller has stopped waiting.
		opts.IncludeNamespaces = slices.Clone(options.IncludeNamespaces)
		opts.ExcludeNamespaces = slices.Clone(options.ExcludeNamespaces)
	}

	wc, err := config.ParseTargetWriteConcern(opts.TargetWriteConcern)
	if err != nil {
		p.lock.Unlock()

		return errors.Wrap(err, "start target write concern")
	}

	save := p.saveTransition
	if save == nil {
		defer p.lock.Unlock()

		return p.launchStartLocked(parent, opts, wc)
	}

	// A fresh run has no catalog entries, clone progress or repl progress.
	cp := &checkpoint{
		NSInclude: opts.IncludeNamespaces,
		NSExclude: opts.ExcludeNamespaces,
		Options:   newRunOptions(opts.PauseOnInitialSync, &opts.Clone, &opts.Repl),
		State:     StateRunning,
	}
	if wc.W != "majority" {
		cp.TargetWriteConcern = fmt.Sprint(wc.W)
	}
	data, err := bson.Marshal(cp)
	if err != nil {
		p.lock.Unlock()

		return errors.Wrap(err, "build start checkpoint")
	}

	p.startPending = true
	p.lock.Unlock()

	return save(ctx, data, func(saved bool) error {
		p.lock.Lock()
		defer p.lock.Unlock()

		p.startPending = false
		if !saved {
			return nil
		}
		launchErr := p.launchStartLocked(parent, opts, wc)
		if launchErr != nil {
			// The next ACTIVE can restore this run even though we cannot
			// launch it on the tenure that saved it.
			return errStartSaved
		}

		return nil
	})
}

func (p *PCSM) checkStartStateLocked() error {
	if p.startPending {
		return errStartPending
	}
	switch p.state {
	case StateRunning, StatePausing, StateFinalizing:
		return errors.New("already running")
	case StateFailed:
		// Re-clone only if the clone never finished. A repl-phase failure
		// must be recovered with resume --from-failure.
		if p.clone != nil {
			status := p.clone.Status()
			if status.IsFinished() {
				return errors.New("already running")
			}
		}
	case StatePaused:
		return errors.New("paused")
	}

	return nil
}

func (p *PCSM) launchStartLocked(parent context.Context, options StartOptions, wc *writeconcern.WriteConcern) error {
	execCtx, execution, err := p.admitExecution(parent, &p.runExec)
	if err != nil {
		return err
	}

	p.err = nil
	p.targetWriteConcern = fmt.Sprint(wc.W)
	p.nsInclude = options.IncludeNamespaces
	p.nsExclude = options.ExcludeNamespaces
	p.nsFilter = sel.MakeFilter(p.nsInclude, p.nsExclude)
	p.pauseOnInitialSync = options.PauseOnInitialSync
	p.cloneOptions = options.Clone
	p.replOptions = options.Repl

	cloneOptions := options.Clone
	cloneOptions.WriteConcern = wc
	replOptions := options.Repl
	replOptions.WriteConcern = wc
	p.catalog = catalog.NewCatalog(p.source, p.target, p.sourceVer)
	p.clone = clone.NewClone(p.source, p.target, p.catalog, p.nsFilter, &cloneOptions, p.targetIsSharded)
	p.repl = repl.NewRepl(
		p.source, p.target, p.catalog, p.nsFilter, &replOptions,
		p.sourceVer, p.sourceIsSharded, p.targetIsSharded,
	)
	p.finalizeStatus = nil
	p.state = StateRunning
	p.startRun(execCtx, execution)
	go p.onStateChanged(StateRunning)

	return nil
}
