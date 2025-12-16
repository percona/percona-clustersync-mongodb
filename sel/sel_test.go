package sel_test

import (
	"testing"

	"github.com/percona/percona-clustersync-mongodb/sel"
)

func TestFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		includeFilter  []string
		excludeFilter  []string
		testNamespaces map[string]map[string]bool
	}{
		// Core filter modes
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
