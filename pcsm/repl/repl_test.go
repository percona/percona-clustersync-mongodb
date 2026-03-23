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
		name                 string
		catalogHasCollection bool
		expectDrop           bool
		expectCreate         bool
		expectSetUUID        bool
	}{
		{
			name:                 "create_skipped_when_catalog_has_collection",
			catalogHasCollection: true,
			expectDrop:           false,
			expectCreate:         false,
			expectSetUUID:        true,
		},
		{
			name:                 "create_proceeds_when_catalog_missing_collection",
			catalogHasCollection: false,
			expectDrop:           true,
			expectCreate:         true,
			expectSetUUID:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cat := &mockCatalog{
				collectionExists: tt.catalogHasCollection,
			}

			r := &Repl{
				catalog: cat,
			}

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

			assert.Equal(t, tt.expectDrop, cat.dropCollectionCalled, "DropCollection called mismatch")
			assert.Equal(t, tt.expectCreate, cat.createCollectionCalled, "CreateCollection called mismatch")
			assert.Equal(t, tt.expectSetUUID, cat.setCollectionUUIDCalled, "SetCollectionUUID called mismatch")
		})
	}
}

func TestApplyDDLChange_MovePrimaryDropSuppression(t *testing.T) {
	t.Parallel()

	ns := catalog.Namespace{Database: "testdb", Collection: "testcoll"}

	createEvent := &ChangeEvent{
		EventHeader: EventHeader{
			OperationType: Create,
			Namespace:     ns,
			CollectionUUID: &bson.Binary{
				Subtype: 4,
				Data:    []byte("0123456789abcdef"),
			},
		},
		Event: CreateEvent{},
	}

	dropEvent := &ChangeEvent{
		EventHeader: EventHeader{
			OperationType: Drop,
			Namespace:     ns,
		},
		Event: DropEvent{},
	}

	t.Run("pre8_phantom_create_then_drop_suppressed", func(t *testing.T) {
		t.Parallel()

		cat := &mockCatalog{collectionExists: true}
		r := &Repl{
			catalog:         cat,
			movePrimaryDrop: make(map[string]struct{}),
		}

		err := r.applyDDLChange(context.Background(), createEvent)
		require.NoError(t, err)
		assert.Contains(t, r.movePrimaryDrop, ns.String())

		cat.collectionExists = false

		err = r.applyDDLChange(context.Background(), dropEvent)
		require.NoError(t, err)
		assert.False(t, cat.dropCollectionCalled, "drop should be suppressed")
		assert.NotContains(t, r.movePrimaryDrop, ns.String())
	})

	t.Run("v8_phantom_create_no_tracking", func(t *testing.T) {
		t.Parallel()

		cat := &mockCatalog{collectionExists: true}
		r := &Repl{
			catalog:         cat,
			movePrimaryDrop: nil,
		}

		err := r.applyDDLChange(context.Background(), createEvent)
		require.NoError(t, err)
		assert.Nil(t, r.movePrimaryDrop)
	})

	t.Run("pre8_real_drop_not_suppressed", func(t *testing.T) {
		t.Parallel()

		cat := &mockCatalog{collectionExists: false}
		r := &Repl{
			catalog:         cat,
			movePrimaryDrop: make(map[string]struct{}),
		}

		err := r.applyDDLChange(context.Background(), dropEvent)
		require.NoError(t, err)
		assert.True(t, cat.dropCollectionCalled, "real drop should proceed")
	})
}
