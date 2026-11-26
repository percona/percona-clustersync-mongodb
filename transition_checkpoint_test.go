package main //nolint:testpackage // Exercise cancellation after the writer receives a request.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCanceledTransitionDoesNotIssueCheckpoint(t *testing.T) {
	t.Parallel()

	queue := newTransitionCheckpoints()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	settled := make(chan bool, 1)
	done := make(chan error, 1)
	go func() {
		done <- queue.submit(ctx, nil, func(saved bool) error {
			settled <- saved

			return nil
		})
	}()

	req := <-queue.requests
	cancel()
	wrote := false
	err := req.save(t.Context(), func(context.Context, []byte) error {
		wrote = true

		return nil
	})
	require.NoError(t, err)
	require.False(t, wrote)
	require.Len(t, settled, 1)
	require.False(t, <-settled)
	require.Error(t, <-done)
}
