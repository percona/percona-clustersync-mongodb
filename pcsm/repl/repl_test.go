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

func TestApplyDDLChange_Create(t *testing.T) {
	t.Parallel()

	cat := &mockCatalog{}
	r := &Repl{catalog: cat}

	change := &ChangeEvent{
		EventHeader: EventHeader{
			OperationType: Create,
			Namespace: catalog.Namespace{
				Database:   "testdb",
				Collection: "testcoll",
			},
			CollectionUUID: &bson.Binary{Subtype: 4, Data: []byte("0123456789abcdef")},
		},
		Event: CreateEvent{},
	}

	err := r.applyDDLChange(context.Background(), change)
	require.NoError(t, err)

	assert.True(t, cat.dropCollectionCalled)
	assert.True(t, cat.createCollectionCalled)
	assert.True(t, cat.setCollectionUUIDCalled)
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

func TestAdvanceOpTime(t *testing.T) {
	t.Parallel()

	tests := []struct {
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

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r := &Repl{lastReplicatedOpTime: tt.current, safeCheckpointOpTime: tt.current}
			r.lock.Lock()
			r.advanceSafeCheckpoint(tt.input)
			r.lock.Unlock()
			assert.Equal(t, tt.expected, r.lastReplicatedOpTime)
			assert.Equal(t, tt.expected, r.safeCheckpointOpTime)
		})
	}
}
