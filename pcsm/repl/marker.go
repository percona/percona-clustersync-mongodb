package repl

import (
	"sync"

	"github.com/percona/percona-clustersync-mongodb/pcsm/catalog"
)

type movePrimaryMarker struct {
	ns map[string]struct{}
	mu sync.Mutex
}

func (m *movePrimaryMarker) Arm(ns catalog.Namespace) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.ns[ns.Database+"."+ns.Collection] = struct{}{}
}

func (m *movePrimaryMarker) Take(ns catalog.Namespace) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := ns.Database + "." + ns.Collection
	_, ok := m.ns[key]
	if ok {
		delete(m.ns, key)
	}

	return ok
}

func (m *movePrimaryMarker) Clear(ns catalog.Namespace) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.ns, ns.Database+"."+ns.Collection)
}
