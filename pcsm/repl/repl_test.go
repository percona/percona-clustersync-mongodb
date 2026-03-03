package repl //nolint

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/percona/percona-clustersync-mongodb/pcsm/catalog"
	"github.com/percona/percona-clustersync-mongodb/topo"
)

// mockCatalog implements the Catalog interface with minimal tracking for tests.
type mockCatalog struct {
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

func (m *mockCatalog) CreateIndexes(_ context.Context, _, _ string, _ []*topo.IndexSpecification) error {
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

func TestApplyDDLChange_MovePrimary(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		operationType OperationType
		sourceExists  bool
		expectDrop    bool
		expectCreate  bool
		expectSetUUID bool
	}{
		{
			name:          "drop_skipped_when_source_has_collection",
			operationType: Drop,
			sourceExists:  true,
			expectDrop:    false,
			expectCreate:  false,
			expectSetUUID: false,
		},
		{
			name:          "drop_proceeds_when_source_missing_collection",
			operationType: Drop,
			sourceExists:  false,
			expectDrop:    true,
			expectCreate:  false,
			expectSetUUID: false,
		},
		{
			name:          "create_skipped_when_source_has_collection",
			operationType: Create,
			sourceExists:  true,
			expectDrop:    false,
			expectCreate:  false,
			expectSetUUID: true,
		},
		{
			name:          "create_proceeds_when_source_missing_collection",
			operationType: Create,
			sourceExists:  false,
			expectDrop:    true,
			expectCreate:  true,
			expectSetUUID: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cat := &mockCatalog{}

			r := &Repl{
				catalog: cat,
				sourceCollExists: func(_ context.Context, _, _ string) bool {
					return tt.sourceExists
				},
			}

			change := &ChangeEvent{
				EventHeader: EventHeader{
					OperationType: tt.operationType,
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

			assert.Equal(t, tt.expectDrop, cat.dropCollectionCalled, "DropCollection called mismatch")
			assert.Equal(t, tt.expectCreate, cat.createCollectionCalled, "CreateCollection called mismatch")
			assert.Equal(t, tt.expectSetUUID, cat.setCollectionUUIDCalled, "SetCollectionUUID called mismatch")
		})
	}
}
