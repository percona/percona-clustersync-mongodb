package repl //nolint:testpackage

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/percona/percona-clustersync-mongodb/pcsm/catalog"
)

func TestMovePrimaryMarker(t *testing.T) {
	t.Parallel()

	ns := catalog.Namespace{Database: "db", Collection: "coll"}

	tests := []struct {
		name string
		run  func(*movePrimaryMarker) bool
		want bool
	}{
		{
			name: "arm then take",
			run: func(m *movePrimaryMarker) bool {
				m.Arm(ns)

				return m.Take(ns)
			},
			want: true,
		},
		{
			name: "arm then clear then take",
			run: func(m *movePrimaryMarker) bool {
				m.Arm(ns)
				m.Clear(ns)

				return m.Take(ns)
			},
			want: false,
		},
		{
			name: "double arm single take true once",
			run: func(m *movePrimaryMarker) bool {
				m.Arm(ns)
				m.Arm(ns)

				first := m.Take(ns)
				second := m.Take(ns)

				return first && !second
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m := &movePrimaryMarker{ns: make(map[string]struct{})}
			assert.Equal(t, tt.want, tt.run(m))
		})
	}
}

func TestMovePrimaryMarker_Concurrent(t *testing.T) {
	t.Parallel()

	ns := catalog.Namespace{Database: "db", Collection: "coll"}
	m := &movePrimaryMarker{ns: make(map[string]struct{})}
	m.Arm(ns)

	const workers = 64

	var won atomic.Int64
	var wg sync.WaitGroup

	for range workers {
		wg.Go(func() {
			if m.Take(ns) {
				won.Add(1)
			}
		})
	}

	wg.Wait()

	assert.Equal(t, int64(1), won.Load())
}

func TestMovePrimaryMarker_PerNamespace(t *testing.T) {
	t.Parallel()

	nsA := catalog.Namespace{Database: "db", Collection: "collA"}
	nsB := catalog.Namespace{Database: "db", Collection: "collB"}
	m := &movePrimaryMarker{ns: make(map[string]struct{})}

	m.Arm(nsA)

	assert.False(t, m.Take(nsB))
	assert.True(t, m.Take(nsA))
}
