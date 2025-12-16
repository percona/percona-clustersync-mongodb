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
		_, dbIncluded := includeFilter[db]

		nsIncluded := len(includeFilter) > 0 && includeFilter.Has(db, coll)
		nsExcluded := len(excludeFilter) > 0 && excludeFilter.Has(db, coll)

		// Exclusion takes precedence - if explicitly excluded, deny immediately.
		if nsExcluded {
			return false
		}

		// If include filter exists, use whitelist logic (deny by default).
		if len(includeFilter) > 0 {
			// Allow if namespace is explicitly included.
			if nsIncluded {
				return true
			}
			// DB is in include filter but collection is not - deny.
			if dbIncluded {
				return false
			}
			// DB not in include filter at all - deny.
			return false
		}

		// No include filter (exclude-only or no filters).
		// Allow by default since exclusions are already handled above.

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
