package pcsm //nolint:testpackage // Verifies checkpoint and reconstructed component state.

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/percona/percona-clustersync-mongodb/errors"
	"github.com/percona/percona-clustersync-mongodb/mdb"
)

func TestRecoverTargetWriteConcern(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name       string
		stored     string
		normalized string
		w          any
	}{
		{"legacy missing", "", "majority", "majority"},
		{"majority", "majority", "majority", "majority"},
		{"override", "1", "1", 1},
		{"normalized", "0002", "2", 2},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			data, err := bson.Marshal(checkpoint{State: StatePaused, TargetWriteConcern: tt.stored})
			require.NoError(t, err)
			p := New(t.Context(), nil, nil, mdb.ServerVersion{}, false, false)
			require.NoError(t, p.Recover(t.Context(), data))
			assert.Equal(t, tt.normalized, p.targetWriteConcern)

			// Inspect the options held by the real reconstructed components,
			// without adding production getters solely for tests.
			for _, component := range []any{p.clone, p.repl} {
				w := reflect.ValueOf(component).Elem().FieldByName("options").Elem().
					FieldByName("WriteConcern").Elem().FieldByName("W").Elem()
				if w.Kind() == reflect.String {
					assert.Equal(t, tt.w, w.String())
				} else {
					assert.Equal(t, tt.w, int(w.Int()))
				}
			}

			data, err = p.Checkpoint(t.Context())
			require.NoError(t, err)
			var restored checkpoint
			require.NoError(t, bson.Unmarshal(data, &restored))
			if tt.normalized == "majority" {
				assert.Empty(t, restored.TargetWriteConcern)
			} else {
				assert.Equal(t, tt.normalized, restored.TargetWriteConcern)
			}
		})
	}
}

func TestInvalidTargetWriteConcernPreservesPipeline(t *testing.T) {
	t.Parallel()

	for _, recoverCheckpoint := range []bool{false, true} {
		t.Run(map[bool]string{false: "start", true: "recover"}[recoverCheckpoint], func(t *testing.T) {
			t.Parallel()
			cln := &mockCloner{}
			rpl := &mockReplicator{}
			oldErr := errors.New("previous run error")
			p := &PCSM{
				state: StateFailed, clone: cln, repl: rpl, err: oldErr,
				targetWriteConcern: "2", nsInclude: []string{"old.*"},
			}

			var err error
			if recoverCheckpoint {
				data, marshalErr := bson.Marshal(checkpoint{State: StatePaused, TargetWriteConcern: "0"})
				require.NoError(t, marshalErr)
				err = p.Recover(context.Background(), data)
			} else {
				err = p.Start(context.Background(), &StartOptions{TargetWriteConcern: "0"})
			}
			require.Error(t, err)
			assert.Equal(t, State(StateFailed), p.state)
			assert.Same(t, cln, p.clone)
			assert.Same(t, rpl, p.repl)
			assert.Same(t, oldErr, p.err)
			assert.Equal(t, "2", p.targetWriteConcern)
			assert.Equal(t, []string{"old.*"}, p.nsInclude)
		})
	}
}
