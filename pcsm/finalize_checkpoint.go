package pcsm

import (
	"context"
	"time"

	"github.com/percona/percona-clustersync-mongodb/log"
	"github.com/percona/percona-clustersync-mongodb/pcsm/catalog"
)

func (p *PCSM) completeFinalization(ctx context.Context, unsuccessful []catalog.UnsuccessfulIndex, lg log.Logger) {
	p.lock.Lock()
	if ctx.Err() != nil {
		p.finalizeActive = false
		p.lock.Unlock()
		lg.Warn("Finalization interrupted; reissue /finalize once ACTIVE")

		return
	}

	save := p.saveTransition
	if save == nil {
		p.publishFinalizedLocked(unsuccessful, lg)
		p.lock.Unlock()

		return
	}

	data, err := p.checkpointLocked(StateFinalized)
	if err != nil {
		// There was no write. Leave finalization incomplete and allow a retry.
		p.finalizeActive = false
		p.lock.Unlock()
		lg.Error(err, "Build finalized checkpoint")

		return
	}
	p.lock.Unlock()

	err = save(ctx, data, func(saved bool) error {
		p.lock.Lock()
		defer p.lock.Unlock()

		if !saved {
			return nil
		}
		if ctx.Err() != nil {
			p.finalizeActive = false

			return ErrNotActive
		}

		p.publishFinalizedLocked(unsuccessful, lg)

		return nil
	})
	if err != nil && ctx.Err() != nil {
		p.lock.Lock()
		p.finalizeActive = false
		p.lock.Unlock()
		lg.Warn("Finalization interrupted; reissue /finalize once ACTIVE")
	}
}

func (p *PCSM) publishFinalizedLocked(unsuccessful []catalog.UnsuccessfulIndex, lg log.Logger) {
	p.finalizeStatus.UnsuccessfulIndexes = unsuccessful
	p.finalizeStatus.CompletedAt = time.Now()
	p.finalizeStatus.Completed = true
	p.finalizeActive = false
	p.state = StateFinalized

	lg.With(log.Elapsed(time.Since(p.finalizeStatus.StartedAt))).Info("Finalization is completed")
	go p.onStateChanged(StateFinalized)
}
