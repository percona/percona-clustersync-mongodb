package clone_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/percona/percona-clustersync-mongodb/pcsm/clone"
)

// TestCheckpointRoundTripFinishTS is a regression test for PCSM-338: the clone
// checkpoint must persist the in-memory finish timestamp so initial-sync
// completion is decided correctly after recovery. Before the fix, Checkpoint()
// wrote a zero FinishTS, which recovered as zero and made the completion
// comparison pass prematurely.
func TestCheckpointRoundTripFinishTS(t *testing.T) {
	t.Parallel()

	finishTS := bson.Timestamp{T: 123, I: 1}

	// Seed the finish timestamp through the public Recover API. StartTime is set
	// so Checkpoint() returns a non-nil checkpoint.
	seed := &clone.Checkpoint{StartTime: time.Now(), FinishTS: finishTS}
	c := &clone.Clone{}
	require.NoError(t, c.Recover(seed))

	cp := c.Checkpoint()
	require.NotNil(t, cp, "Checkpoint() must be non-nil once the clone has started")
	require.Equal(t, finishTS, cp.FinishTS, "Checkpoint() must persist finishTS, not zero")

	raw, err := bson.Marshal(cp)
	require.NoError(t, err)

	var restored clone.Checkpoint
	require.NoError(t, bson.Unmarshal(raw, &restored))
	// omitempty must not drop a populated timestamp.
	require.Equal(t, finishTS, restored.FinishTS, "FinishTS must survive a BSON round-trip")

	c2 := &clone.Clone{}
	require.NoError(t, c2.Recover(&restored))
	require.Equal(t, finishTS, c2.Status().FinishTS, "Recover() must restore finishTS")
}
