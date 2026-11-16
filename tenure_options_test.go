package main //nolint:testpackage // Exercise the lifecycle controller's restore with the server's options.

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/percona/percona-clustersync-mongodb/config"
	"github.com/percona/percona-clustersync-mongodb/ha"
	"github.com/percona/percona-clustersync-mongodb/pcsm"
)

// TestTenureRestoresOldCheckpointWithServerOptions pins the fallback for a
// checkpoint written before run options were persisted: the tenure restores
// it with the server's configured options, not built-in defaults.
func TestTenureRestoresOldCheckpointWithServerOptions(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		// The stored checkpoint has a state and no run options.
		record := failedRecord(t, 2)
		target := mockedTarget(t, nil, claimed(record), claimed(record))
		t.Cleanup(func() { require.NoError(t, target.Disconnect(t.Context())) })

		s := &server{
			cfg:              &config.Config{RecoveryCheckpointInterval: time.Hour},
			targetCluster:    target,
			membership:       &ha.Membership{},
			recoveryDefaults: &pcsm.StartOptions{PauseOnInitialSync: true},
		}

		s.observeOwnership(ctx, ha.Ownership{Held: true, Term: 3})
		synctest.Wait()

		tn := currentTenure(s)
		require.NotNil(t, tn)
		require.True(t, tn.isReady(), "the tenure did not open for requests")
		status := tn.pipeline.Status(ctx)
		require.Equal(t, pcsm.State(pcsm.StateFailed), status.State, "the checkpoint was not restored")
		require.True(t, status.PauseOnInitialSync, "the restore must fall back to the server's options")

		s.observeOwnership(ctx, ha.Ownership{})
	})
}
