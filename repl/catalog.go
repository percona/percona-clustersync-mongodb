package repl

import (
	"iter"
	"slices"
	"sync"
)

type IndexCatalog struct {
	mu sync.Mutex

	cat map[string]map[string][]IndexSpecification
}

func NewIndexCatalog() *IndexCatalog {
	return &IndexCatalog{cat: make(map[string]map[string][]IndexSpecification)}
}

func (ic *IndexCatalog) CollectionIndexes(db, coll string) iter.Seq[IndexSpecification] {
	return func(yield func(IndexSpecification) bool) {
		ic.mu.Lock()
		defer ic.mu.Unlock()

		if _, ok := ic.cat[db]; !ok {
			return
		}

		for _, index := range ic.cat[db][coll] {
			if !yield(index) {
				return
			}
		}
	}
}

func (ic *IndexCatalog) CreateIndexes(db, coll string, indexes []IndexSpecification) {
	ic.mu.Lock()
	defer ic.mu.Unlock()

	if _, ok := ic.cat[db]; !ok {
		ic.cat[db] = make(map[string][]IndexSpecification)
	}

	ic.cat[db][coll] = append(ic.cat[db][coll], indexes...)
}

func (ic *IndexCatalog) CreateIndex(db, coll string, index IndexSpecification) {
	ic.mu.Lock()
	defer ic.mu.Unlock()

	if _, ok := ic.cat[db]; !ok {
		ic.cat[db] = make(map[string][]IndexSpecification)
	}

	ic.cat[db][coll] = append(ic.cat[db][coll], index)
}

func (ic *IndexCatalog) DropIndex(db, coll, name string) {
	ic.mu.Lock()
	defer ic.mu.Unlock()

	if _, ok := ic.cat[db]; !ok {
		return
	}

	ic.cat[db][coll] = slices.DeleteFunc(ic.cat[db][coll], func(index IndexSpecification) bool {
		return index.Name == name
	})
}

func (ic *IndexCatalog) DropCollection(db, coll string) {
	ic.mu.Lock()
	defer ic.mu.Unlock()

	delete(ic.cat[db], coll)
}

func (ic *IndexCatalog) DropDatabase(db string) {
	ic.mu.Lock()
	defer ic.mu.Unlock()

	delete(ic.cat, db)
}
