package repl //nolint

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/percona/percona-clustersync-mongodb/mdb"
	"github.com/percona/percona-clustersync-mongodb/pcsm/catalog"
)

type mockCatalog struct {
	collectionExists bool

	collectionUUIDResult *bson.Binary
	collectionUUIDExists bool

	dropCollectionCalled bool
	dropCollectionDB     string
	dropCollectionColl   string
	dropCollectionErr    error

	createCollectionCalled bool
	createCollectionErr    error

	setCollectionUUIDCalled bool
	setCollectionUUIDDB     string
	setCollectionUUIDColl   string

	setCollectionShardingMetadataCalled bool
	setCollectionShardingMetadataDB     string
	setCollectionShardingMetadataColl   string
	setCollectionShardingMetadataKey    bson.D
	setCollectionShardingMetadataErr    error
}

type mockPool struct {
	barrierErr    error
	barrierCalled bool
	releaseCalled bool
	callOrder     []string
	onRelease     func()
}

func (m *mockPool) Route(_ *ChangeEvent) {}

func (m *mockPool) Barrier() error {
	m.barrierCalled = true
	m.callOrder = append(m.callOrder, "Barrier")

	return m.barrierErr
}

func (m *mockPool) ReleaseBarrier() {
	m.releaseCalled = true
	m.callOrder = append(m.callOrder, "ReleaseBarrier")
	if m.onRelease != nil {
		m.onRelease()
	}
}

func (m *mockPool) Checkpoint() bson.Timestamp { return bson.Timestamp{} }

func (m *mockPool) Idle() bool { return true }

func (m *mockPool) TotalEventsApplied() int64 { return 0 }

func (m *mockPool) Stop() {}

func (m *mockPool) Err() <-chan error { return make(chan error) }

func (m *mockCatalog) CollectionExists(_, _ string) bool {
	return m.collectionExists
}

func (m *mockCatalog) DropCollection(_ context.Context, db, coll string) error {
	m.dropCollectionCalled = true
	m.dropCollectionDB = db
	m.dropCollectionColl = coll

	return m.dropCollectionErr
}

func (m *mockCatalog) CreateCollection(_ context.Context, _, _ string, _ *catalog.CreateCollectionOptions) error {
	m.createCollectionCalled = true

	return m.createCollectionErr
}

func (m *mockCatalog) SetCollectionUUID(_ context.Context, db, coll string, _ *bson.Binary) {
	m.setCollectionUUIDCalled = true
	m.setCollectionUUIDDB = db
	m.setCollectionUUIDColl = coll
}

func (m *mockCatalog) CreateIndexes(_ context.Context, _, _ string, _ []*mdb.IndexSpecification) error {
	return nil
}

func (m *mockCatalog) ShardCollection(_ context.Context, _, _ string, _ bson.D, _ bool) error {
	return nil
}

func (m *mockCatalog) SetCollectionShardingMetadata(_ context.Context, db, coll string, shardKey bson.D) error {
	m.setCollectionShardingMetadataCalled = true
	m.setCollectionShardingMetadataDB = db
	m.setCollectionShardingMetadataColl = coll
	m.setCollectionShardingMetadataKey = shardKey

	return m.setCollectionShardingMetadataErr
}

func (m *mockCatalog) UUIDMap() catalog.UUIDMap {
	return catalog.UUIDMap{}
}

func (m *mockCatalog) CollectionUUID(_, _ string) (*bson.Binary, bool) {
	return m.collectionUUIDResult, m.collectionUUIDExists
}

func (m *mockCatalog) DropDatabase(_ context.Context, _ string) error {
	return nil
}

func (m *mockCatalog) DropIndex(_ context.Context, _, _, _ string) error {
	return nil
}

func (m *mockCatalog) Rename(_ context.Context, _, _, _, _ string) error {
	return nil
}

func (m *mockCatalog) ModifyIndex(_ context.Context, _, _ string, _ *catalog.ModifyIndexOption) error {
	return nil
}

func (m *mockCatalog) ModifyCappedCollection(_ context.Context, _, _ string, _, _ *int64) error {
	return nil
}

func (m *mockCatalog) ModifyView(_ context.Context, _, _, _ string, _ any) error {
	return nil
}

func (m *mockCatalog) ModifyChangeStreamPreAndPostImages(_ context.Context, _, _ string, _ bool) error {
	return nil
}

func (m *mockCatalog) ModifyValidation(
	_ context.Context, _, _ string,
	_ *bson.Raw, _, _ *string,
) error {
	return nil
}

func newInvalidateTestRepl(pool *mockPool, sourceIsMongos bool, sourceVer mdb.ServerVersion) *Repl {
	doneCh := make(chan struct{})
	close(doneCh)

	return &Repl{
		pool:              pool,
		catalog:           &mockCatalog{},
		sourceIsMongos:    sourceIsMongos,
		sourceVer:         sourceVer,
		movePrimaryMarker: movePrimaryMarker{ns: make(map[string]struct{})},
		pauseCh:           make(chan struct{}, 1),
		doneCh:            doneCh,
	}
}

func TestDispatch_Invalidate(t *testing.T) {
	t.Parallel()

	change := &ChangeEvent{
		EventHeader: EventHeader{
			OperationType: Invalidate,
			ClusterTime:   bson.Timestamp{T: 123, I: 1},
		},
	}

	tests := []struct {
		name              string
		sourceIsMongos    bool
		sourceVer         mdb.ServerVersion
		markerArmed       bool
		expectFailed      bool
		expectCheckpoint  bool
		expectEventsApply int64
	}{
		{
			name:              "skip_on_armed_marker_pre8_mongos",
			sourceIsMongos:    true,
			sourceVer:         mdb.ServerVersion{7, 0, 0, 0},
			markerArmed:       true,
			expectCheckpoint:  true,
			expectEventsApply: 1,
		},
		{
			name:           "recoverable_no_marker_pre8_mongos",
			sourceIsMongos: true,
			sourceVer:      mdb.ServerVersion{7, 0, 0, 0},
		},
		{
			name:           "recoverable_8x_with_marker_mongos",
			sourceIsMongos: true,
			sourceVer:      mdb.ServerVersion{8, 0, 0, 0},
			markerArmed:    true,
		},
		{
			name:         "fail_closed_rs_source",
			sourceVer:    mdb.ServerVersion{7, 0, 0, 0},
			markerArmed:  true,
			expectFailed: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			pool := &mockPool{}
			r := newInvalidateTestRepl(pool, tt.sourceIsMongos, tt.sourceVer)
			if tt.markerArmed {
				r.movePrimaryMarker.Arm(movePrimarySentinelNS)
			}

			r.handleInvalidate(change)

			assert.True(t, pool.barrierCalled)
			assert.True(t, pool.releaseCalled)
			if tt.expectFailed {
				require.Error(t, r.err)

				return
			}

			require.NoError(t, r.err)
			if tt.expectCheckpoint {
				assert.Equal(t, change.ClusterTime, r.checkpointOpTime)
			} else {
				assert.Equal(t, bson.Timestamp{}, r.checkpointOpTime)
			}

			assert.Equal(t, tt.expectEventsApply, r.eventsApplied)
		})
	}
}

func TestDispatch_Invalidate_CallOrdering(t *testing.T) {
	t.Parallel()

	pool := &mockPool{}
	r := newInvalidateTestRepl(pool, true, mdb.ServerVersion{7, 0, 0, 0})
	r.movePrimaryMarker.Arm(movePrimarySentinelNS)
	pool.onRelease = func() {
		_, markerExists := r.movePrimaryMarker.ns["::movePrimary::.sentinel"]
		assert.False(t, markerExists)
	}

	change := &ChangeEvent{
		EventHeader: EventHeader{
			OperationType: Invalidate,
			ClusterTime:   bson.Timestamp{T: 123, I: 1},
		},
	}

	r.handleInvalidate(change)

	assert.Equal(t, []string{"Barrier", "ReleaseBarrier"}, pool.callOrder)
	require.NoError(t, r.err)
}

func TestFindNamespaceByUUID(t *testing.T) {
	t.Parallel()

	eventUUID := &bson.Binary{Subtype: 4, Data: []byte("0123456789abcdef")}
	unknownUUID := &bson.Binary{Subtype: 4, Data: []byte("fedcba9876543210")}
	bareNS := catalog.Namespace{Database: "testdb", Collection: "testcoll"}
	enrichedNS := catalog.Namespace{
		Database:   "testdb",
		Collection: "testcoll",
		Sharded:    true,
		ShardKey:   bson.D{{Key: "category", Value: 1}, {Key: "item_id", Value: 1}},
	}
	otherNS := catalog.Namespace{
		Database:   "otherdb",
		Collection: "othercoll",
		Sharded:    true,
		ShardKey:   bson.D{{Key: "other", Value: 1}},
	}

	tests := []struct {
		name      string
		uuidMap   catalog.UUIDMap
		change    *ChangeEvent
		expected  catalog.Namespace
		bareMatch bool
	}{
		{
			name: "uuid match returns enriched namespace",
			uuidMap: catalog.UUIDMap{
				"30313233343536373839616263646566": enrichedNS,
			},
			change:   &ChangeEvent{EventHeader: EventHeader{Namespace: bareNS, CollectionUUID: eventUUID}},
			expected: enrichedNS,
		},
		{
			name: "nil uuid falls back to namespace match",
			uuidMap: catalog.UUIDMap{
				"30313233343536373839616263646566": enrichedNS,
			},
			change:   &ChangeEvent{EventHeader: EventHeader{Namespace: bareNS}},
			expected: enrichedNS,
		},
		{
			name: "unknown uuid falls back to namespace match",
			uuidMap: catalog.UUIDMap{
				"30313233343536373839616263646566": enrichedNS,
			},
			change:   &ChangeEvent{EventHeader: EventHeader{Namespace: bareNS, CollectionUUID: unknownUUID}},
			expected: enrichedNS,
		},
		{
			name: "no match returns bare namespace",
			uuidMap: catalog.UUIDMap{
				"30313233343536373839616263646566": otherNS,
			},
			change:    &ChangeEvent{EventHeader: EventHeader{Namespace: bareNS, CollectionUUID: unknownUUID}},
			expected:  bareNS,
			bareMatch: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			actual := findNamespaceByUUID(tt.uuidMap, tt.change)

			assert.Equal(t, tt.expected, actual)
			if tt.bareMatch {
				assert.False(t, actual.Sharded)
				assert.Nil(t, actual.ShardKey)
			} else {
				assert.True(t, actual.Sharded)
				assert.NotNil(t, actual.ShardKey)
			}
		})
	}
}

func TestApplyCreateDDLChange(t *testing.T) {
	t.Parallel()

	eventUUID := &bson.Binary{Subtype: 4, Data: []byte("0123456789abcdef")}
	differentUUID := &bson.Binary{Subtype: 4, Data: []byte("fedcba9876543210")}
	ns := catalog.Namespace{Database: "testdb", Collection: "testcoll"}

	tests := []struct {
		name                 string
		catalogUUID          *bson.Binary
		catalogUUIDExists    bool
		eventUUID            *bson.Binary
		sourceIsMongos       bool
		sourceVer            mdb.ServerVersion
		createEvent          CreateEvent
		expectDrop           bool
		expectCreate         bool
		expectSetUUID        bool
		sourceShardingInfo   *mdb.ShardingInfo
		sourceShardingErr    error
		expectSetSharding    bool
		expectMarker         bool
		expectSentinelMarker bool
	}{
		{
			name:                 "same UUID replay on pre8 mongos arms markers and noops",
			catalogUUID:          eventUUID,
			catalogUUIDExists:    true,
			eventUUID:            eventUUID,
			sourceIsMongos:       true,
			sourceVer:            mdb.ServerVersion{7, 0, 0, 0},
			sourceShardingErr:    mdb.ErrNotFound,
			expectMarker:         true,
			expectSentinelMarker: true,
		},
		{
			name:              "same UUID replay on mongos repairs sharding metadata",
			catalogUUID:       eventUUID,
			catalogUUIDExists: true,
			eventUUID:         eventUUID,
			sourceIsMongos:    true,
			sourceVer:         mdb.ServerVersion{8, 0, 0, 0},
			sourceShardingInfo: &mdb.ShardingInfo{
				ShardKey: bson.D{{Key: "sku", Value: 1}},
			},
			expectSetSharding: true,
		},
		{
			name:              "same UUID replay on replica set does not arm markers",
			catalogUUID:       eventUUID,
			catalogUUIDExists: true,
			eventUUID:         eventUUID,
			sourceIsMongos:    false,
			sourceVer:         mdb.ServerVersion{7, 0, 0, 0},
		},
		{
			name:                 "different UUID phantom create updates catalog and arms markers",
			catalogUUID:          differentUUID,
			catalogUUIDExists:    true,
			eventUUID:            eventUUID,
			sourceIsMongos:       true,
			sourceVer:            mdb.ServerVersion{7, 0, 0, 0},
			expectSetUUID:        true,
			expectMarker:         true,
			expectSentinelMarker: true,
		},
		{
			name:              "nil_event_uuid_real_create",
			catalogUUID:       differentUUID,
			catalogUUIDExists: true,
			eventUUID:         nil,
			sourceIsMongos:    true,
			sourceVer:         mdb.ServerVersion{7, 0, 0, 0},
			expectDrop:        true,
			expectCreate:      true,
			expectSetUUID:     true,
		},
		{
			name:              "missing catalog entry applies real create",
			catalogUUIDExists: false,
			eventUUID:         eventUUID,
			sourceIsMongos:    true,
			sourceVer:         mdb.ServerVersion{7, 0, 0, 0},
			expectDrop:        true,
			expectCreate:      true,
			expectSetUUID:     true,
		},
		{
			name:              "timeseries create returns without catalog changes",
			catalogUUIDExists: false,
			eventUUID:         eventUUID,
			sourceIsMongos:    true,
			sourceVer:         mdb.ServerVersion{7, 0, 0, 0},
			createEvent: CreateEvent{
				OperationDescription: catalog.CreateCollectionOptions{ViewOn: catalog.TimeseriesPrefix + "testcoll"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cat := &mockCatalog{
				collectionUUIDResult: tt.catalogUUID,
				collectionUUIDExists: tt.catalogUUIDExists,
			}
			r := &Repl{
				catalog:           cat,
				movePrimaryMarker: movePrimaryMarker{ns: make(map[string]struct{})},
				sourceIsMongos:    tt.sourceIsMongos,
				sourceVer:         tt.sourceVer,
			}
			r.getCollectionShardingInfo = func(
				_ context.Context,
				_ *mongo.Client,
				db string,
				coll string,
			) (*mdb.ShardingInfo, error) {
				require.Equal(t, ns.Database, db)
				require.Equal(t, ns.Collection, coll)

				return tt.sourceShardingInfo, tt.sourceShardingErr
			}

			change := &ChangeEvent{
				EventHeader: EventHeader{
					OperationType:  Create,
					Namespace:      ns,
					CollectionUUID: tt.eventUUID,
				},
				Event: tt.createEvent,
			}

			err := r.applyDDLChange(context.Background(), change)
			require.NoError(t, err)

			assert.Equal(t, tt.expectDrop, cat.dropCollectionCalled)
			assert.Equal(t, tt.expectCreate, cat.createCollectionCalled)
			assert.Equal(t, tt.expectSetUUID, cat.setCollectionUUIDCalled)
			assert.Equal(t, tt.expectSetSharding, cat.setCollectionShardingMetadataCalled)

			if tt.expectDrop {
				assert.Equal(t, ns.Database, cat.dropCollectionDB)
				assert.Equal(t, ns.Collection, cat.dropCollectionColl)
			}
			if tt.expectSetUUID {
				assert.Equal(t, ns.Database, cat.setCollectionUUIDDB)
				assert.Equal(t, ns.Collection, cat.setCollectionUUIDColl)
			}
			if tt.expectSetSharding {
				assert.Equal(t, ns.Database, cat.setCollectionShardingMetadataDB)
				assert.Equal(t, ns.Collection, cat.setCollectionShardingMetadataColl)
				assert.Equal(t, tt.sourceShardingInfo.ShardKey, cat.setCollectionShardingMetadataKey)
			}

			_, markerArmed := r.movePrimaryMarker.ns["testdb.testcoll"]
			assert.Equal(t, tt.expectMarker, markerArmed)

			_, sentinelArmed := r.movePrimaryMarker.ns["::movePrimary::.sentinel"]
			assert.Equal(t, tt.expectSentinelMarker, sentinelArmed)
		})
	}
}

func TestApplyDropDDLChange(t *testing.T) {
	t.Parallel()

	eventUUID := &bson.Binary{Subtype: 4, Data: []byte("0123456789abcdef")}
	differentUUID := &bson.Binary{Subtype: 4, Data: []byte("fedcba9876543210")}
	ns := catalog.Namespace{Database: "testdb", Collection: "testcoll"}

	tests := []struct {
		name              string
		catalogUUID       *bson.Binary
		catalogUUIDExists bool
		eventUUID         *bson.Binary
		sourceIsMongos    bool
		armMarker         bool
		expectDrop        bool
		expectMarkerGone  bool
	}{
		{
			name:              "missing catalog entry applies real drop and consumes marker",
			catalogUUIDExists: false,
			eventUUID:         eventUUID,
			sourceIsMongos:    true,
			armMarker:         true,
			expectDrop:        true,
			expectMarkerGone:  true,
		},
		{
			name:              "UUID mismatch on pre8 mongos suppresses phantom drop without consuming marker",
			catalogUUID:       differentUUID,
			catalogUUIDExists: true,
			eventUUID:         eventUUID,
			sourceIsMongos:    true,
			armMarker:         true,
		},
		{
			name:              "UUID mismatch on replica set suppresses phantom drop",
			catalogUUID:       differentUUID,
			catalogUUIDExists: true,
			eventUUID:         eventUUID,
		},
		{
			name:              "UUID match on pre8 mongos applies drop and consumes marker",
			catalogUUID:       eventUUID,
			catalogUUIDExists: true,
			eventUUID:         eventUUID,
			sourceIsMongos:    true,
			armMarker:         true,
			expectDrop:        true,
			expectMarkerGone:  true,
		},
		{
			name:              "UUID match on replica set applies drop without marker consumption",
			catalogUUID:       eventUUID,
			catalogUUIDExists: true,
			eventUUID:         eventUUID,
			armMarker:         true,
			expectDrop:        true,
		},
		{
			name:              "nil UUIDs apply drop through default branch",
			catalogUUIDExists: true,
			expectDrop:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cat := &mockCatalog{
				collectionUUIDResult: tt.catalogUUID,
				collectionUUIDExists: tt.catalogUUIDExists,
			}
			r := &Repl{
				catalog:           cat,
				movePrimaryMarker: movePrimaryMarker{ns: make(map[string]struct{})},
				sourceIsMongos:    tt.sourceIsMongos,
				sourceVer:         mdb.ServerVersion{7, 0, 0, 0},
			}
			if tt.armMarker {
				r.movePrimaryMarker.Arm(ns)
			}

			change := &ChangeEvent{
				EventHeader: EventHeader{
					OperationType:  Drop,
					Namespace:      ns,
					CollectionUUID: tt.eventUUID,
				},
				Event: DropEvent{},
			}

			err := r.applyDDLChange(context.Background(), change)
			require.NoError(t, err)

			assert.Equal(t, tt.expectDrop, cat.dropCollectionCalled)
			if tt.expectDrop {
				assert.Equal(t, ns.Database, cat.dropCollectionDB)
				assert.Equal(t, ns.Collection, cat.dropCollectionColl)
			}

			if tt.armMarker {
				_, markerExists := r.movePrimaryMarker.ns["testdb.testcoll"]
				assert.Equal(t, !tt.expectMarkerGone, markerExists)
			}
		})
	}
}

func TestIsChangeStreamUnrecoverable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{"ChangeStreamHistoryLost", mongo.CommandError{Code: 0, Name: "ChangeStreamHistoryLost"}, true},
		{"CappedPositionLost", mongo.CommandError{Code: 0, Name: "CappedPositionLost"}, true},
		{"ShutdownInProgress", mongo.CommandError{Code: 91, Name: "ShutdownInProgress"}, false},
		{"generic error", errors.New("something broke"), false}, //nolint:err113
		{"context canceled", context.Canceled, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, isChangeStreamUnrecoverable(tt.err))
		})
	}
}

func TestChangeStreamInvalidateError(t *testing.T) {
	t.Parallel()

	token := bson.Raw{0x05, 0x00, 0x00, 0x00, 0x00}
	err := changeStreamInvalidateError{
		token:       token,
		clusterTime: bson.Timestamp{T: 123, I: 1},
	}

	var target changeStreamInvalidateError
	require.ErrorAs(t, err, &target)
	assert.Equal(t, token, target.token)
	assert.Equal(t, bson.Timestamp{T: 123, I: 1}, target.clusterTime)
}

func TestChangeStreamCursorErrorPrefersInvalidateError(t *testing.T) {
	t.Parallel()

	token := bson.Raw{0x05, 0x00, 0x00, 0x00, 0x00}
	invalidateErr := &changeStreamInvalidateError{
		token:       token,
		clusterTime: bson.Timestamp{T: 123, I: 1},
	}
	cursorErr := errors.New("cursor closed by mongos") //nolint:err113 // test-only cursor error for precedence check.

	err := changeStreamCursorError(invalidateErr, cursorErr, 0)

	var target changeStreamInvalidateError
	require.ErrorAs(t, err, &target)
	assert.Equal(t, token, target.token)
	assert.Equal(t, bson.Timestamp{T: 123, I: 1}, target.clusterTime)
	assert.NotContains(t, err.Error(), "cursor")
}

func advanceCases() []struct {
	name     string
	current  bson.Timestamp
	input    bson.Timestamp
	expected bson.Timestamp
} {
	return []struct {
		name     string
		current  bson.Timestamp
		input    bson.Timestamp
		expected bson.Timestamp
	}{
		{
			name:     "lower timestamp",
			current:  bson.Timestamp{T: 100, I: 10},
			input:    bson.Timestamp{T: 99, I: 5},
			expected: bson.Timestamp{T: 100, I: 10},
		},
		{
			name:     "equal timestamp",
			current:  bson.Timestamp{T: 100, I: 10},
			input:    bson.Timestamp{T: 100, I: 10},
			expected: bson.Timestamp{T: 100, I: 10},
		},
		{
			name:     "higher timestamp",
			current:  bson.Timestamp{T: 100, I: 10},
			input:    bson.Timestamp{T: 101, I: 1},
			expected: bson.Timestamp{T: 101, I: 1},
		},
		{
			name:     "same T lower I",
			current:  bson.Timestamp{T: 100, I: 10},
			input:    bson.Timestamp{T: 100, I: 8},
			expected: bson.Timestamp{T: 100, I: 10},
		},
		{
			name:     "same T higher I",
			current:  bson.Timestamp{T: 100, I: 10},
			input:    bson.Timestamp{T: 100, I: 15},
			expected: bson.Timestamp{T: 100, I: 15},
		},
		{
			name:     "zero current",
			current:  bson.Timestamp{T: 0, I: 0},
			input:    bson.Timestamp{T: 50, I: 1},
			expected: bson.Timestamp{T: 50, I: 1},
		},
	}
}

func TestAdvanceCheckpoint(t *testing.T) {
	t.Parallel()

	for _, tt := range advanceCases() {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r := &Repl{lastReplicatedOpTime: tt.current, checkpointOpTime: tt.current}
			r.lock.Lock()
			r.advanceCheckpoint(tt.input)
			r.lock.Unlock()
			assert.Equal(t, tt.expected, r.lastReplicatedOpTime)
			assert.Equal(t, tt.expected, r.checkpointOpTime)
		})
	}
}

func TestIsReplay(t *testing.T) {
	t.Parallel()

	checkpoint := bson.Timestamp{T: 100, I: 10}

	tests := []struct {
		name       string
		checkpoint bson.Timestamp
		changeTime bson.Timestamp
		expected   bool
	}{
		{
			name:       "zero checkpoint",
			changeTime: bson.Timestamp{T: 99, I: 9},
		},
		{
			name:       "event before checkpoint",
			checkpoint: checkpoint,
			changeTime: bson.Timestamp{T: 100, I: 9},
			expected:   true,
		},
		{
			name:       "event equal checkpoint",
			checkpoint: checkpoint,
			changeTime: checkpoint,
		},
		{
			name:       "event after checkpoint",
			checkpoint: checkpoint,
			changeTime: bson.Timestamp{T: 100, I: 11},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := &Repl{checkpointOpTime: tt.checkpoint}
			change := &ChangeEvent{EventHeader: EventHeader{ClusterTime: tt.changeTime}}

			assert.Equal(t, tt.expected, r.isReplay(change))
		})
	}
}

func TestAdvanceReportedOpTime(t *testing.T) {
	t.Parallel()

	fixedCheckpoint := bson.Timestamp{T: 42, I: 42}

	for _, tt := range advanceCases() {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r := &Repl{lastReplicatedOpTime: tt.current, checkpointOpTime: fixedCheckpoint}
			r.lock.Lock()
			r.advanceReportedOpTime(tt.input)
			r.lock.Unlock()
			assert.Equal(t, tt.expected, r.lastReplicatedOpTime)
			assert.Equal(t, fixedCheckpoint, r.checkpointOpTime,
				"advanceReportedOpTime must not advance checkpointOpTime")
		})
	}
}

func TestAlignCappedSize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    int64
		expected int64
	}{
		{"non-aligned rounds up", 3333, 3584},
		{"already aligned is unchanged", 3584, 3584},
		{"zero stays zero", 0, 0},
		{"256 stays 256", 256, 256},
		{"257 rounds to 512", 257, 512},
		{"1 rounds to 256", 1, 256},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, alignCappedSize(tt.input))
		})
	}
}
