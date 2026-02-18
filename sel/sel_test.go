package sel_test

import (
	"testing"

	"github.com/percona/percona-clustersync-mongodb/sel"
)

func TestFilter(t *testing.T) { //nolint:maintidx
	t.Parallel()

	tests := []struct {
		name           string
		includeFilter  []string
		excludeFilter  []string
		testNamespaces map[string]map[string]bool
	}{
		// Core filter modes
		{
			name:          "both filters empty - allow all",
			includeFilter: []string{},
			excludeFilter: []string{},
			testNamespaces: map[string]map[string]bool{
				"any_db": {
					"any_coll": true,
				},
				"another_db": {
					"some_coll": true,
				},
			},
		},
		{
			name: "include only",
			includeFilter: []string{
				"db_0.*",
				"db_1.coll_0",
				"db_1.coll_1",
			},
			excludeFilter: nil,
			testNamespaces: map[string]map[string]bool{
				"db_0": {
					"coll_0": true,
					"coll_1": true,
					"coll_2": true,
				},
				"db_1": {
					"coll_0": true,
					"coll_1": true,
					"coll_2": false,
				},
				"db_2": {
					"coll_0": false,
					"coll_1": false,
					"coll_2": false,
				},
			},
		},
		{
			name:          "exclude only",
			includeFilter: nil,
			excludeFilter: []string{
				"db_0.*",
				"db_1.coll_0",
				"db_1.coll_1",
			},
			testNamespaces: map[string]map[string]bool{
				"db_0": {
					"coll_0": false,
					"coll_1": false,
					"coll_2": false,
				},
				"db_1": {
					"coll_0": false,
					"coll_1": false,
					"coll_2": true,
				},
				"db_2": {
					"coll_0": true,
					"coll_1": true,
					"coll_2": true,
				},
			},
		},
		{
			name: "include with exclude",
			includeFilter: []string{
				"db_0.*",
				"db_1.coll_0",
				"db_1.coll_1",
				"db_2.coll_0",
				"db_2.coll_1",
			},
			excludeFilter: []string{
				"db_0.*",
				"db_1.coll_0",
				"db_3.coll_1",
			},
			testNamespaces: map[string]map[string]bool{
				"db_0": {
					"coll_0": false,
					"coll_1": false,
					"coll_2": false,
				},
				"db_1": {
					"coll_0": false,
					"coll_1": true,
					"coll_2": false,
				},
				"db_2": {
					"coll_0": true,
					"coll_1": true,
					"coll_2": false,
				},
				"db_3": {
					"coll_0": false,
					"coll_1": false,
					"coll_2": false,
				},
			},
		},

		// More advanced inclusion/exclusion scenarios
		{
			name:          "include one DB, no exclusions",
			includeFilter: []string{"test_db1.*"},
			excludeFilter: nil,
			testNamespaces: map[string]map[string]bool{
				"test_db1": {
					"test_collection1": true,
					"test_collection2": true,
				},
				"test_db2": {
					"test_collection1": false,
					"test_collection2": false,
				},
				"test_db3": {
					"test_collection1": false,
					"test_collection2": false,
				},
			},
		},
		{
			name:          "include one collection, no exclusions",
			includeFilter: []string{"test_db1.test_collection1"},
			excludeFilter: nil,
			testNamespaces: map[string]map[string]bool{
				"test_db1": {
					"test_collection1": true,
					"test_collection2": false,
				},
				"test_db2": {
					"test_collection1": false,
					"test_collection2": false,
				},
				"test_db3": {
					"test_collection1": false,
					"test_collection2": false,
				},
			},
		},
		{
			name:          "include and exclude same DB",
			includeFilter: []string{"test_db1.*"},
			excludeFilter: []string{"test_db1.*"},
			testNamespaces: map[string]map[string]bool{
				"test_db1": {
					"test_collection1": false,
					"test_collection2": false,
				},
				"test_db2": {
					"test_collection1": false,
					"test_collection2": false,
				},
				"test_db3": {
					"test_collection1": false,
					"test_collection2": false,
				},
			},
		},
		{
			name:          "include and exclude same collection",
			includeFilter: []string{"test_db1.test_collection1"},
			excludeFilter: []string{"test_db1.test_collection1"},
			testNamespaces: map[string]map[string]bool{
				"test_db1": {
					"test_collection1": false,
					"test_collection2": false,
				},
				"test_db2": {
					"test_collection1": false,
					"test_collection2": false,
				},
				"test_db3": {
					"test_collection1": false,
					"test_collection2": false,
				},
			},
		},
		{
			name:          "include collection with mistake in name",
			includeFilter: []string{"test_db1.mistake_in_collection_name"},
			excludeFilter: nil,
			testNamespaces: map[string]map[string]bool{
				"test_db1": {
					"test_collection1": false,
					"test_collection2": false,
				},
				"test_db2": {
					"test_collection1": false,
					"test_collection2": false,
				},
				"test_db3": {
					"test_collection1": false,
					"test_collection2": false,
				},
			},
		},
		{
			name:          "include DB and exclude one collection from it",
			includeFilter: []string{"test_db1.*"},
			excludeFilter: []string{"test_db1.test_collection1"},
			testNamespaces: map[string]map[string]bool{
				"test_db1": {
					"test_collection1": false,
					"test_collection2": true,
				},
				"test_db2": {
					"test_collection1": false,
					"test_collection2": false,
				},
				"test_db3": {
					"test_collection1": false,
					"test_collection2": false,
				},
			},
		},

		// Edge cases
		{
			name:          "both filters nil - allow all",
			includeFilter: nil,
			excludeFilter: nil,
			testNamespaces: map[string]map[string]bool{
				"any_db": {
					"any_coll": true,
				},
				"another_db": {
					"some_coll": true,
				},
			},
		},
		{
			name:          "collection name with dots",
			includeFilter: []string{"mydb.coll.with.dots"},
			excludeFilter: nil,
			testNamespaces: map[string]map[string]bool{
				"mydb": {
					"coll.with.dots": true,
					"coll":           false,
					"regular_coll":   false,
				},
				"coll": {
					"with": false,
				},
			},
		},
		{
			name:          "database wildcard with collection containing dots",
			includeFilter: []string{"testdb.*"},
			excludeFilter: []string{"testdb.exclude.this"},
			testNamespaces: map[string]map[string]bool{
				"testdb": {
					"normal_coll":    true,
					"coll.with.dots": true,
					"exclude.this":   false,
				},
			},
		},
		{
			name:          "case sensitive - uppercase database",
			includeFilter: []string{"DB.*"},
			excludeFilter: nil,
			testNamespaces: map[string]map[string]bool{
				"DB": {
					"coll": true,
				},
				"db": {
					"coll": false,
				},
				"Db": {
					"coll": false,
				},
			},
		},
		{
			name:          "case sensitive - uppercase collection",
			includeFilter: []string{"db.COLLECTION"},
			excludeFilter: nil,
			testNamespaces: map[string]map[string]bool{
				"db": {
					"COLLECTION": true,
					"collection": false,
					"Collection": false,
				},
			},
		},
		{
			name:          "case sensitive - mixed case filters",
			includeFilter: []string{"TestDB.*", "mydb.MyCollection"},
			excludeFilter: []string{"TestDB.ExcludeThis"},
			testNamespaces: map[string]map[string]bool{
				"TestDB": {
					"SomeCollection": true,
					"ExcludeThis":    false,
				},
				"testdb": {
					"SomeCollection": false,
				},
				"mydb": {
					"MyCollection": true,
					"mycollection": false,
				},
			},
		},
		{
			name:          "empty collection name",
			includeFilter: []string{"db."},
			excludeFilter: nil,
			testNamespaces: map[string]map[string]bool{
				"db": {
					"":     true,
					"coll": false,
				},
			},
		},
		{
			name:          "multiple wildcards for same database",
			includeFilter: []string{"db.*", "db.*", "db.specific"},
			excludeFilter: nil,
			testNamespaces: map[string]map[string]bool{
				"db": {
					"coll1":    true,
					"coll2":    true,
					"specific": true,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			filter := sel.MakeFilter(tt.includeFilter, tt.excludeFilter)

			for db, colls := range tt.testNamespaces {
				for coll, expected := range colls {
					if got := filter(db, coll); got != expected {
						t.Errorf("%s.%s: expected %v, got %v", db, coll, expected, got)
					}
				}
			}
		})
	}
}
