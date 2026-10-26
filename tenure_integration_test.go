//go:build integration

package main //nolint:testpackage // Shared helpers for integration tests that drive the lifecycle controller.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/percona/percona-clustersync-mongodb/log"
)

// closeTenure ends the current tenure, if any, on test cleanup.
func closeTenure(ctx context.Context, s *server) {
	s.mu.Lock()
	s.endTenure(ctx, log.New("test"))
	s.mu.Unlock()
}

// awaitTenure returns the current tenure once it is open for requests. A
// tenure that ends before that, or a missing one, fails the test.
func awaitTenure(t *testing.T, ctx context.Context, s *server) *tenure {
	t.Helper()

	tn := currentTenure(s)
	require.NotNil(t, tn, "no tenure is open")

	select {
	case <-tn.ready:
	case <-tn.done:
		require.FailNow(t, "the tenure ended before it opened for requests")
	case <-ctx.Done():
		require.FailNow(t, "the tenure did not open for requests", "%v", ctx.Err())
	}

	return tn
}
