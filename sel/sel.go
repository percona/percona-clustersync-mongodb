package sel

import (
	"slices"
	"strings"
)

// NSFilter returns true if a namespace is allowed.
type NSFilter func(db, coll string) bool

func AllowAllFilter(string, string) bool {
	return true
}

func MakeFilter(include, exclude []string) NSFilter {
	if len(include) == 0 && len(exclude) == 0 {
		return AllowAllFilter
	}

	includeFilter := doMakeFitler(include)
	excludeFilter := doMakeFitler(exclude)

	return func(db, coll string) bool {
		_, dbFiltered := includeFilter[db]
		included := len(includeFilter) > 0 && includeFilter.Has(db, coll)

		if included && dbFiltered {
			// if the namespace is included, it is allowed
			return true
		}

		if dbFiltered {
			// if the database is filtered, but the namespace is not included,
			// it is not allowed
			return false
		}

		_, dbFiltered = excludeFilter[db]
		excluded := len(excludeFilter) > 0 && excludeFilter.Has(db, coll)

		if excluded && dbFiltered {
			// if the namespace is excluded, it is not allowed
			return false
		}

		if dbFiltered {
			// if the database is filtered, but the namespace is not excluded, it is allowed
			return true
		}

		// if the namespace is not present in either filter, it is allowed by default
		return true
	}
}

type filterMap map[string][]string

func (f filterMap) Has(db, coll string) bool {
	list, ok := f[db]
	if !ok {
		return false // the db is not included
	}

	if len(list) == 0 {
		return true // all namespaces of the database are included
	}

	return slices.Contains(list, coll) // only if explcitly listed
}

func doMakeFitler(filter []string) filterMap {
	// keys are database names. values are list collections that belong to the db.
	// if a key contains empty/nil, whole db is included (all its collections).
	filterMap := make(map[string][]string)

	for _, filter := range filter {
		db, coll, _ := strings.Cut(filter, ".")

		l, ok := filterMap[db]
		if ok && len(l) == 0 {
			// all namespaces of the database is allowed
			continue
		}

		if coll == "*" {
			// set key as allow all
			filterMap[db] = nil

			continue
		}

		filterMap[db] = append(filterMap[db], coll)
	}

	return filterMap
}
