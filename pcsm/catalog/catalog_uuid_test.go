package catalog //nolint:testpackage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/percona/percona-clustersync-mongodb/mdb"
)

const (
	missingDatabaseName     = "missing"
	missingCollectionName   = "missing_coll"
	testCollectionName      = "coll"
	testCatalogDatabaseName = "db"
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
			db:        missingDatabaseName,
			coll:      testCollectionName,
			expected:  nil,
			exists:    false,
		},
		{
			name: "missing coll",
			databases: map[string]databaseCatalog{
				testCatalogDatabaseName: {Collections: map[string]collectionCatalog{}},
			},
			db:       testCatalogDatabaseName,
			coll:     missingCollectionName,
			expected: nil,
			exists:   false,
		},
		{
			name: "existing entry with nil UUID",
			databases: map[string]databaseCatalog{
				testCatalogDatabaseName: {
					Collections: map[string]collectionCatalog{
						testCollectionName: {UUID: nil},
					},
				},
			},
			db:       testCatalogDatabaseName,
			coll:     testCollectionName,
			expected: nil,
			exists:   true,
		},
		{
			name: "existing entry with non-nil UUID",
			databases: map[string]databaseCatalog{
				testCatalogDatabaseName: {
					Collections: map[string]collectionCatalog{
						testCollectionName: {UUID: &bson.Binary{Subtype: 0x04, Data: []byte{0x01, 0x02}}},
					},
				},
			},
			db:       testCatalogDatabaseName,
			coll:     testCollectionName,
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
