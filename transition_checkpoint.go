package main

import (
	"context"

	"github.com/percona/percona-clustersync-mongodb/errors"
	"github.com/percona/percona-clustersync-mongodb/pcsm"
)

type transitionCheckpoint struct {
	data     []byte
	canceled <-chan struct{}
	settle   func(saved bool) error
	result   chan error
}

// transitionCheckpoints uses an unbuffered handoff: if the caller exits
// before handing off, the writer cannot have accepted its snapshot.
type transitionCheckpoints struct {
	requests chan transitionCheckpoint
	stopped  chan struct{}
}

func newTransitionCheckpoints() transitionCheckpoints {
	return transitionCheckpoints{
		requests: make(chan transitionCheckpoint),
		stopped:  make(chan struct{}),
	}
}

func (c transitionCheckpoints) submit(
	ctx context.Context, data []byte, settle func(saved bool) error,
) error {
	result := make(chan error, 1)
	req := transitionCheckpoint{
		data: data, canceled: ctx.Done(), settle: settle, result: result,
	}

	select {
	case <-ctx.Done():
		return errors.Join(errors.Wrap(ctx.Err(), "checkpoint not submitted"), settle(false))
	case <-c.stopped:
		return errors.Join(pcsm.ErrNotActive, settle(false))
	case c.requests <- req:
	}

	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		// If the writer has a verdict already, report it instead of guessing.
		select {
		case err := <-result:
			return err
		default:
			return pcsm.ErrCheckpointUnconfirmed
		}
	}
}

// save runs on the checkpoint writer. After issuing the write, only ctx (the
// tenure context) bounds it, not the requester's deadline. The waiter can
// leave independently. Send the verdict before the writer reports its exit
// to the tenure controller, which can join a finalizer awaiting this save.
func (r transitionCheckpoint) save(ctx context.Context, persist func(context.Context, []byte) error) error {
	select {
	case <-ctx.Done():
		r.result <- errors.Join(pcsm.ErrNotActive, r.settle(false))

		return nil
	case <-r.canceled:
		r.result <- errors.Join(errTransitionCanceled, r.settle(false))

		return nil
	default:
	}

	err := persist(ctx, r.data)
	switch {
	case err == nil:
		// Publish before the writer can take its next periodic snapshot.
		// If publication fails, stop rather than overwrite a committed transition.
		err = r.settle(true)
		r.result <- err
	case errors.Is(err, errCheckpointFenced):
		r.result <- pcsm.ErrNotActive
	default:
		r.result <- pcsm.ErrCheckpointUnconfirmed
	}

	return err
}

var errTransitionCanceled = errors.New("checkpoint request canceled before the write")
