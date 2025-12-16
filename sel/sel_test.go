package sel_test

import (
	"testing"

	"github.com/percona/percona-clustersync-mongodb/sel"
)

func TestFilterNew(t *testing.T) {
	t.Parallel()

	// Base namespaces present in source - 3 DBs with 2 collections each
	baseNamespaces := map[string]map[string]bool{
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
	}

	// prepareNamespaces sets expected inclusion results for all base namespaces,
	// based on the provided expected map.
	prepareNamespaces := func(expected map[string]map[string]bool) map[string]map[string]bool {
		result := make(map[string]map[string]bool)

		for db, colls := range baseNamespaces {
			result[db] = make(map[string]bool)

			for coll := range colls {
				if expColls, ok := expected[db]; ok {
					result[db][coll] = expColls[coll]
				} else {
					result[db][coll] = false
				}
			}
		}

		return result
	}

	t.Run("case 1: include one DB, no exclusions", func(t *testing.T) {
		t.Parallel()

		includeFilter := []string{"test_db1.*"}

		namespaces := prepareNamespaces(map[string]map[string]bool{
			"test_db1": {
				"test_collection1": true,
				"test_collection2": true,
			},
		})

		isIncluded := sel.MakeFilter(includeFilter, nil)

		for db, colls := range namespaces {
			for coll, expected := range colls {
				if got := isIncluded(db, coll); got != expected {
					t.Errorf("%s.%s: expected %v, got %v", db, coll, expected, got)
				}
			}
		}
	})

	t.Run("case 2: include one collection, no exclusions", func(t *testing.T) {
		t.Parallel()

		includeFilter := []string{"test_db1.test_collection1"}

		namespaces := prepareNamespaces(map[string]map[string]bool{
			"test_db1": {
				"test_collection1": true,
				"test_collection2": false,
			},
		})

		isIncluded := sel.MakeFilter(includeFilter, nil)

		for db, colls := range namespaces {
			for coll, expected := range colls {
				if got := isIncluded(db, coll); got != expected {
					t.Errorf("%s.%s: expected %v, got %v", db, coll, expected, got)
				}
			}
		}
	})

	t.Run("case 3a: include and exclude same DB", func(t *testing.T) {
		t.Parallel()

		includeFilter := []string{"test_db1.*"}
		excludeFilter := []string{"test_db1.*"}

		namespaces := prepareNamespaces(map[string]map[string]bool{})

		isIncluded := sel.MakeFilter(includeFilter, excludeFilter)

		for db, colls := range namespaces {
			for coll, expected := range colls {
				if got := isIncluded(db, coll); got != expected {
					t.Errorf("%s.%s: expected %v, got %v", db, coll, expected, got)
				}
			}
		}
	})

	t.Run("case 3b: include and exclude same collection", func(t *testing.T) {
		t.Parallel()

		includeFilter := []string{"test_db1.test_collection1"}
		excludeFilter := []string{"test_db1.test_collection1"}

		namespaces := prepareNamespaces(map[string]map[string]bool{})

		isIncluded := sel.MakeFilter(includeFilter, excludeFilter)

		for db, colls := range namespaces {
			for coll, expected := range colls {
				if got := isIncluded(db, coll); got != expected {
					t.Errorf("%s.%s: expected %v, got %v", db, coll, expected, got)
				}
			}
		}
	})

	t.Run("case 4: include collection with mistake in name", func(t *testing.T) {
		t.Parallel()

		includeFilter := []string{"test_db1.mistake_in_collection_name"}

		namespaces := prepareNamespaces(map[string]map[string]bool{})

		isIncluded := sel.MakeFilter(includeFilter, nil)

		for db, colls := range namespaces {
			for coll, expected := range colls {
				if got := isIncluded(db, coll); got != expected {
					t.Errorf("%s.%s: expected %v, got %v", db, coll, expected, got)
				}
			}
		}
	})

	t.Run("case 5: include DB and exclude one collection from it", func(t *testing.T) {
		t.Parallel()

		includeFilter := []string{"test_db1.*"}
		excludeFilter := []string{"test_db1.test_collection1"}

		namespaces := prepareNamespaces(map[string]map[string]bool{
			"test_db1": {
				"test_collection1": false,
				"test_collection2": true,
			},
		})

		isIncluded := sel.MakeFilter(includeFilter, excludeFilter)

		for db, colls := range namespaces {
			for coll, expected := range colls {
				if got := isIncluded(db, coll); got != expected {
					t.Errorf("%s.%s: expected %v, got %v", db, coll, expected, got)
				}
			}
		}
	})
}

func TestFilter(t *testing.T) {
	t.Parallel()

	t.Run("include", func(t *testing.T) {
		t.Parallel()

		includeFilter := []string{
			"db_0.*",
			"db_1.coll_0",
			"db_1.coll_1",
		}

		namespaces := map[string]map[string]bool{
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
		}

		isIncluded := sel.MakeFilter(includeFilter, nil)

		for db, colls := range namespaces {
			for coll, expected := range colls {
				if got := isIncluded(db, coll); got != expected {
					t.Errorf("%s.%s: expected %v, got %v", db, coll, expected, got)
				}
			}
		}
	})

	t.Run("exclude", func(t *testing.T) {
		t.Parallel()

		excludedFilter := []string{
			"db_0.*",
			"db_1.coll_0",
			"db_1.coll_1",
		}

		namespaces := map[string]map[string]bool{
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
		}

		isIncluded := sel.MakeFilter(nil, excludedFilter)

		for db, colls := range namespaces {
			for coll, expected := range colls {
				if got := isIncluded(db, coll); got != expected {
					t.Errorf("%s.%s: expected %v, got %v", db, coll, expected, got)
				}
			}
		}
	})

	t.Run("include with exclude", func(t *testing.T) {
		t.Parallel()

		includedFilter := []string{
			"db_0.*",
			"db_1.coll_0",
			"db_1.coll_1",
			"db_2.coll_0",
			"db_2.coll_1",
		}

		excludedFilter := []string{
			"db_0.*",
			"db_1.coll_0",
			"db_3.coll_1",
		}

		namespaces := map[string]map[string]bool{
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
		}

		isIncluded := sel.MakeFilter(includedFilter, excludedFilter)

		for db, colls := range namespaces {
			for coll, expected := range colls {
				if got := isIncluded(db, coll); got != expected {
					t.Errorf("%s.%s: expected %v, got %v", db, coll, expected, got)
				}
			}
		}
	})
}
