package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func ptrUint64(v uint64) *uint64 {
	return &v
}

// TestPoolBelowWorkers covers the effective-pool-size comparison, including the
// PCSM-312 regression where an unset maxPoolSize (nil) must be treated as the
// Go driver default of 100 rather than skipped, so large pods whose clone
// worker count exceeds 100 still get warned.
func TestPoolBelowWorkers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		maxPool       *uint64
		workers       int
		wantEffective uint64
		wantWarn      bool
	}{
		{"unset pool below workers warns at driver default 100", nil, 200, 100, true},
		{"unset pool above workers does not warn", nil, 50, 100, false},
		{"unlimited pool never warns", ptrUint64(0), 200, 0, false},
		{"explicit pool below workers warns", ptrUint64(50), 100, 50, true},
		{"explicit pool above workers does not warn", ptrUint64(200), 100, 200, false},
		{"explicit pool equal to workers does not warn", ptrUint64(100), 100, 100, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			effective, warn := poolBelowWorkers(tt.maxPool, tt.workers)
			assert.Equal(t, tt.wantEffective, effective)
			assert.Equal(t, tt.wantWarn, warn)
		})
	}
}
