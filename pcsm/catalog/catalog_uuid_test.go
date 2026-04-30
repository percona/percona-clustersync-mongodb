package catalog //nolint:testpackage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/percona/percona-clustersync-mongodb/mdb"
)

func TestCatalog_CollectionUUID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		databases map[string]databaseCatalog
		db        string
		coll      string
		expected  *bson.Binary
		exists    bool
	}{
		{
			name:      "missing db",
			databases: map[string]databaseCatalog{},
			db:        "missing",
			coll:      "coll",
			expected:  nil,
			exists:    false,
		},
		{
			name: "missing coll",
			databases: map[string]databaseCatalog{
				"db": {Collections: map[string]collectionCatalog{}},
			},
			db:       "db",
			coll:     "missing",
			expected: nil,
			exists:   false,
		},
		{
			name: "existing entry with nil UUID",
			databases: map[string]databaseCatalog{
				"db": {
					Collections: map[string]collectionCatalog{
						"coll": {UUID: nil},
					},
				},
			},
			db:       "db",
			coll:     "coll",
			expected: nil,
			exists:   true,
		},
		{
			name: "existing entry with non-nil UUID",
			databases: map[string]databaseCatalog{
				"db": {
					Collections: map[string]collectionCatalog{
						"coll": {UUID: &bson.Binary{Subtype: 0x04, Data: []byte{0x01, 0x02}}},
					},
				},
			},
			db:       "db",
			coll:     "coll",
			expected: &bson.Binary{Subtype: 0x04, Data: []byte{0x01, 0x02}},
			exists:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cat := NewCatalog(nil, mdb.ServerVersion{})
			cat.Databases = tt.databases

			uuid, ok := cat.CollectionUUID(tt.db, tt.coll)
			require.Equal(t, tt.exists, ok)
			require.Equal(t, tt.expected, uuid)
		})
	}
}

func TestCatalog_SetCollectionShardingMetadata(t *testing.T) {
	t.Parallel()

	shardKey := bson.D{{Key: "sku", Value: 1}}

	tests := []struct {
		name      string
		databases map[string]databaseCatalog
		db        string
		coll      string
		expectErr bool
	}{
		{
			name:      "missing db",
			databases: map[string]databaseCatalog{},
			db:        "missing",
			coll:      "coll",
			expectErr: true,
		},
		{
			name: "missing coll",
			databases: map[string]databaseCatalog{
				"db": {Collections: map[string]collectionCatalog{}},
			},
			db:        "db",
			coll:      "missing",
			expectErr: true,
		},
		{
			name: "existing entry",
			databases: map[string]databaseCatalog{
				"db": {
					Collections: map[string]collectionCatalog{
						"coll": {},
					},
				},
			},
			db:   "db",
			coll: "coll",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cat := NewCatalog(nil, mdb.ServerVersion{})
			cat.Databases = tt.databases

			err := cat.SetCollectionShardingMetadata(t.Context(), tt.db, tt.coll, shardKey)
			if tt.expectErr {
				require.ErrorIs(t, err, mdb.ErrNotFound)

				return
			}

			require.NoError(t, err)
			entry := cat.Databases[tt.db].Collections[tt.coll]
			require.True(t, entry.Sharded)
			require.Equal(t, shardKey, entry.ShardKey)
		})
	}
}
