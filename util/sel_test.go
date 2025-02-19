package util

import "testing"

func TestFilter(t *testing.T) {
	t.Run("include", func(t *testing.T) {
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

		isIncluded := MakeFilter(includeFilter, nil)
		for db, colls := range namespaces {
			for coll, expected := range colls {
				if got := isIncluded(db, coll); got != expected {
					t.Errorf("%s.%s: expected %v, got %v", db, coll, expected, got)
				}
			}
		}
	})

	t.Run("exclude", func(t *testing.T) {
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

		isIncluded := MakeFilter(nil, excludedFilter)
		for db, colls := range namespaces {
			for coll, expected := range colls {
				if db == "db_1" {
					_ = coll
				}
				if got := isIncluded(db, coll); got != expected {
					t.Errorf("%s.%s: expected %v, got %v", db, coll, expected, got)
				}
			}
		}
	})

	t.Run("include with exclude", func(t *testing.T) {
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
		}

		isIncluded := MakeFilter(includedFilter, excludedFilter)
		for db, colls := range namespaces {
			for coll, expected := range colls {
				if got := isIncluded(db, coll); got != expected {
					t.Errorf("%s.%s: expected %v, got %v", db, coll, expected, got)
				}
			}
		}
	})
}
