package pcsm

import (
	"context"

	"github.com/percona/percona-clustersync-mongodb/errors"
)

// ErrCheckpointUnconfirmed means a caller stopped waiting or the writer did
// not learn the outcome. It does not say the snapshot was rejected.
var ErrCheckpointUnconfirmed = errors.New(
	"checkpoint outcome could not be confirmed; check /status on ACTIVE before retrying",
)

// TransitionSaver submits an unpublished snapshot to the tenure's writer.
// settle(true) runs on acknowledgement, before the writer may capture
// another snapshot. settle(false) runs if the request is skipped before the
// write. A failed write discards the tenure rather than settling in memory.
// The caller's context bounds submission and waiting, not an issued write.
type TransitionSaver func(ctx context.Context, data []byte, settle func(saved bool) error) error

// SetTransitionSaver connects the pipeline to its tenure's checkpoint writer
// before admitting requests.
func (p *PCSM) SetTransitionSaver(save TransitionSaver) {
	p.lock.Lock()
	defer p.lock.Unlock()

	p.saveTransition = save
}
