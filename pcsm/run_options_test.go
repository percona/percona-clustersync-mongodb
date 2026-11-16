package pcsm //nolint:testpackage // Verifies checkpoint and reconstructed component state.

import (
	"reflect"
	"runtime"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"

	"github.com/percona/percona-clustersync-mongodb/mdb"
	"github.com/percona/percona-clustersync-mongodb/pcsm/catalog"
	"github.com/percona/percona-clustersync-mongodb/pcsm/clone"
	"github.com/percona/percona-clustersync-mongodb/pcsm/repl"
)

// componentOptions returns the options held by a reconstructed clone or repl,
// without adding production getters solely for tests.
func componentOptions(component any) reflect.Value {
	return reflect.ValueOf(component).Elem().FieldByName("options").Elem()
}

// A restore keeps the tuning and pauseOnInitialSync the run was started with.
func TestRecoverRunOptions(t *testing.T) {
	t.Parallel()

	data, err := bson.Marshal(bson.D{
		{"state", StatePaused},
		{"options", bson.D{
			{"pauseOnInitialSync", true},
			{"clone", bson.D{{"readWorkers", 3}, {"skipPresplit", true}}},
			{"repl", bson.D{
				{"useCollectionBulkWrite", true},
				{"numWorkers", 2},
				{"bulkOpsSize", 100},
				{"workerFlushInterval", int64(250 * time.Millisecond)},
			}},
		}},
	})
	require.NoError(t, err)

	p := New(t.Context(), nil, nil, mdb.ServerVersion{}, false, false)
	require.NoError(t, p.Recover(t.Context(), data))

	assert.True(t, p.pauseOnInitialSync)

	cln := componentOptions(p.clone)
	assert.Equal(t, int64(3), cln.FieldByName("ReadWorkers").Int())
	assert.True(t, cln.FieldByName("SkipPresplit").Bool())

	rpl := componentOptions(p.repl)
	assert.True(t, rpl.FieldByName("UseCollectionBulkWrite").Bool())
	assert.Equal(t, int64(2), rpl.FieldByName("NumWorkers").Int())
	assert.Equal(t, int64(100), rpl.FieldByName("BulkOpsSize").Int())
	assert.Equal(t, int64(250*time.Millisecond), rpl.FieldByName("WorkerFlushInterval").Int())

	// The next checkpoint carries the same request on.
	data, err = p.Checkpoint(t.Context())
	require.NoError(t, err)
	opts, err := bson.Raw(data).LookupErr("options")
	require.NoError(t, err, "a checkpoint must record the run's options")
	assert.True(t, opts.Document().Lookup("pauseOnInitialSync").Boolean())
	assert.Equal(t, int32(3), opts.Document().Lookup("clone", "readWorkers").Int32())
	assert.Equal(t, int32(2), opts.Document().Lookup("repl", "numWorkers").Int32())
	assert.Equal(t, int32(100), opts.Document().Lookup("repl", "bulkOpsSize").Int32())
}

// A checkpoint written before run options were persisted falls back to the
// server's configured options, then to built-in defaults.
func TestRecoverOldCheckpointUsesRecoveryDefaults(t *testing.T) {
	t.Parallel()

	data, err := bson.Marshal(bson.D{{"state", StatePaused}})
	require.NoError(t, err)

	t.Run("server config", func(t *testing.T) {
		t.Parallel()

		p := New(t.Context(), nil, nil, mdb.ServerVersion{}, false, false)
		p.SetRecoveryDefaults(&StartOptions{
			PauseOnInitialSync: true,
			Clone:              clone.Options{ReadWorkers: 3},
			Repl:               repl.Options{NumWorkers: 2},
		})
		require.NoError(t, p.Recover(t.Context(), data))

		assert.True(t, p.pauseOnInitialSync)
		assert.Equal(t, int64(3), componentOptions(p.clone).FieldByName("ReadWorkers").Int())
		assert.Equal(t, int64(2), componentOptions(p.repl).FieldByName("NumWorkers").Int())
	})

	t.Run("built-in defaults", func(t *testing.T) {
		t.Parallel()

		p := New(t.Context(), nil, nil, mdb.ServerVersion{}, false, false)
		require.NoError(t, p.Recover(t.Context(), data))

		assert.False(t, p.pauseOnInitialSync)
		assert.Equal(t, int64(runtime.NumCPU()), componentOptions(p.repl).FieldByName("NumWorkers").Int())
	})
}

// Status reports whether the run pauses once the initial sync completes.
func TestStatusReportsPauseOnInitialSync(t *testing.T) {
	t.Parallel()

	// A failed run's status is complete before the source is queried.
	data, err := bson.Marshal(bson.D{
		{"state", StateFailed},
		{"error", "boom"},
		{"options", bson.D{{"pauseOnInitialSync", true}, {"clone", bson.D{}}, {"repl", bson.D{}}}},
	})
	require.NoError(t, err)

	p := New(t.Context(), nil, nil, mdb.ServerVersion{}, false, false)
	require.NoError(t, p.Recover(t.Context(), data))
	assert.True(t, p.Status(t.Context()).PauseOnInitialSync)
}

// The pause on initial sync happens once. The checkpoint written for the pause
// records it as done: a restored run can find the initial sync incomplete again,
// since ticks don't move the persisted resume checkpoint, and must not pause
// a second time.
func TestPauseOnInitialSyncPausesOnce(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		r := &drainingReplicator{}
		p := &PCSM{
			state: StateRunning, pauseOnInitialSync: true,
			catalog: catalog.NewCatalog(nil, nil, mdb.ServerVersion{}),
			clone:   finishedLifecycleCloner(), repl: r,
			runDone: make(chan struct{}),
		}
		saved := make(chan []byte, 1)
		p.onStateChanged = func(State) {
			data, err := p.Checkpoint(t.Context())
			if err == nil {
				select {
				case saved <- data:
				default:
				}
			}
		}

		monitorDone := make(chan struct{})
		go func() {
			p.monitorInitialSync(t.Context())
			close(monitorDone)
		}()
		synctest.Wait()
		r.lastOp.Store(2) // the frontier passes the clone's finish
		<-monitorDone
		synctest.Wait()

		cp := bson.Raw(<-saved)
		assert.Equal(t, string(StatePaused), cp.Lookup("state").StringValue())
		_, err := cp.LookupErr("options", "pauseOnInitialSync")
		require.ErrorIs(t, err, bsoncore.ErrElementNotFound, "the checkpoint of the pause must record it as done")
	})
}

// Every tuning option survives the checkpoint. An option added to
// clone.Options or repl.Options must be persisted too, or this fails.
func TestRunOptionsKeepEveryOption(t *testing.T) {
	t.Parallel()

	var cloneIn clone.Options
	var replIn repl.Options
	fillNonZero(t, reflect.ValueOf(&cloneIn).Elem())
	fillNonZero(t, reflect.ValueOf(&replIn).Elem())

	data, err := bson.Marshal(newRunOptions(true, &cloneIn, &replIn))
	require.NoError(t, err)
	var out runOptions
	require.NoError(t, bson.Unmarshal(data, &out))

	assert.True(t, out.PauseOnInitialSync)
	assert.Equal(t, cloneIn, out.cloneOptions())
	assert.Equal(t, replIn, out.replOptions())
}

// fillNonZero sets every field but WriteConcern, which the checkpoint stores
// on its own, to a distinct non-zero value.
func fillNonZero(t *testing.T, v reflect.Value) {
	t.Helper()

	for i := range v.NumField() {
		f, name := v.Field(i), v.Type().Field(i).Name
		switch {
		case name == "WriteConcern":
		case f.Kind() == reflect.Bool:
			f.SetBool(true)
		case f.CanInt():
			f.SetInt(int64(i + 1))
		default:
			t.Fatalf("no test value for %s.%s (%s)", v.Type(), name, f.Kind())
		}
	}
}

// Auto tuning stays auto in the checkpoint: the instance that restores the run
// resolves it for its own host instead of inheriting another host's NumCPU.
func TestCheckpointKeepsAutoTuning(t *testing.T) {
	t.Parallel()

	data, err := bson.Marshal(bson.D{
		{"state", StatePaused},
		{"options", bson.D{{"clone", bson.D{}}, {"repl", bson.D{}}}},
	})
	require.NoError(t, err)

	p := New(t.Context(), nil, nil, mdb.ServerVersion{}, false, false)
	require.NoError(t, p.Recover(t.Context(), data))
	assert.False(t, p.pauseOnInitialSync)
	assert.Equal(t, int64(runtime.NumCPU()), componentOptions(p.repl).FieldByName("NumWorkers").Int())

	data, err = p.Checkpoint(t.Context())
	require.NoError(t, err)
	raw := bson.Raw(data)
	_, err = raw.LookupErr("options", "repl")
	require.NoError(t, err, "a checkpoint must record the run's options, even when all are auto")
	_, err = raw.LookupErr("options", "repl", "numWorkers")
	require.ErrorIs(t, err, bsoncore.ErrElementNotFound, "auto workers must not be pinned to this host's NumCPU")
}
