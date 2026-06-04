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

const (
	replTestDBName     = "testdb"
	replTestCollection = "testcoll"
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
}

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

func TestApplyCreateDDLChange(t *testing.T) {
	t.Parallel()

	eventUUID := &bson.Binary{Subtype: 4, Data: []byte("0123456789abcdef")}
	differentUUID := &bson.Binary{Subtype: 4, Data: []byte("fedcba9876543210")}
	ns := catalog.Namespace{Database: replTestDBName, Collection: replTestCollection}

	tests := []struct {
		name              string
		catalogUUID       *bson.Binary
		catalogUUIDExists bool
		eventUUID         *bson.Binary
		createEvent       CreateEvent
		expectDrop        bool
		expectCreate      bool
		expectSetUUID     bool
	}{
		{
			name:              "same UUID replay noops",
			catalogUUID:       eventUUID,
			catalogUUIDExists: true,
			eventUUID:         eventUUID,
		},
		{
			name:              "different UUID phantom create updates catalog only",
			catalogUUID:       differentUUID,
			catalogUUIDExists: true,
			eventUUID:         eventUUID,
			expectSetUUID:     true,
		},
		{
			name:              "nil event UUID applies real create",
			catalogUUID:       differentUUID,
			catalogUUIDExists: true,
			expectDrop:        true,
			expectCreate:      true,
			expectSetUUID:     true,
		},
		{
			name:              "missing catalog entry applies real create",
			catalogUUIDExists: false,
			eventUUID:         eventUUID,
			expectDrop:        true,
			expectCreate:      true,
			expectSetUUID:     true,
		},
		{
			name:              "timeseries create returns without catalog changes",
			catalogUUIDExists: false,
			eventUUID:         eventUUID,
			createEvent: CreateEvent{
				OperationDescription: catalog.CreateCollectionOptions{ViewOn: catalog.TimeseriesPrefix + replTestCollection},
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
			r := &Repl{catalog: cat}

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
			if tt.expectDrop {
				assert.Equal(t, ns.Database, cat.dropCollectionDB)
				assert.Equal(t, ns.Collection, cat.dropCollectionColl)
			}
			if tt.expectSetUUID {
				assert.Equal(t, ns.Database, cat.setCollectionUUIDDB)
				assert.Equal(t, ns.Collection, cat.setCollectionUUIDColl)
			}
		})
	}
}

func TestApplyDropDDLChange(t *testing.T) {
	t.Parallel()

	eventUUID := &bson.Binary{Subtype: 4, Data: []byte("0123456789abcdef")}
	differentUUID := &bson.Binary{Subtype: 4, Data: []byte("fedcba9876543210")}
	ns := catalog.Namespace{Database: replTestDBName, Collection: replTestCollection}

	tests := []struct {
		name              string
		catalogUUID       *bson.Binary
		catalogUUIDExists bool
		eventUUID         *bson.Binary
		expectDrop        bool
	}{
		{
			name:              "missing catalog entry applies real drop",
			catalogUUIDExists: false,
			eventUUID:         eventUUID,
			expectDrop:        true,
		},
		{
			name:              "UUID mismatch suppresses phantom drop",
			catalogUUID:       differentUUID,
			catalogUUIDExists: true,
			eventUUID:         eventUUID,
		},
		{
			name:              "UUID match applies drop",
			catalogUUID:       eventUUID,
			catalogUUIDExists: true,
			eventUUID:         eventUUID,
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
			r := &Repl{catalog: cat}

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

func TestSourceIsPre8AndMongos(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mongos bool
		ver    mdb.ServerVersion
		want   bool
	}{
		{"mongos 6.0 is pre-8", true, mdb.ServerVersion{6, 0, 0, 0}, true},
		{"mongos 7.0 is pre-8", true, mdb.ServerVersion{7, 0, 0, 0}, true},
		{"mongos 8.0 is not pre-8", true, mdb.ServerVersion{8, 0, 0, 0}, false},
		{"replica set 6.0 is not mongos", false, mdb.ServerVersion{6, 0, 0, 0}, false},
		{"replica set 8.0 is not mongos", false, mdb.ServerVersion{8, 0, 0, 0}, false},
		{"mongos zero version treated as pre-8", true, mdb.ServerVersion{}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r := &Repl{sourceIsMongos: tt.mongos, sourceVer: tt.ver}
			assert.Equal(t, tt.want, r.sourceIsPre8AndMongos())
		})
	}
}

func TestParseChangeEvent_Invalidate(t *testing.T) {
	t.Parallel()

	data, err := bson.Marshal(bson.D{{Key: "operationType", Value: "invalidate"}})
	require.NoError(t, err)

	var change ChangeEvent
	err = parseChangeEvent(data, &change)
	require.NoError(t, err)

	assert.Equal(t, Invalidate, change.OperationType)
	_, ok := change.Event.(InvalidateEvent)
	assert.True(t, ok)
}
