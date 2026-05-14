package mdb //nolint:testpackage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestCollStats_DecodeFromFloat64(t *testing.T) {
	t.Parallel()

	input := bson.M{
		"count":      0.1,
		"size":       73179136.2,
		"avgObjSize": 0.3,
	}

	data, err := bson.Marshal(input)
	require.NoError(t, err)

	var stats CollStats
	err = bson.Unmarshal(data, &stats)
	require.NoError(t, err)

	assert.Equal(t, int64(0), stats.Count)
	assert.Equal(t, int64(73179136), stats.Size)
	assert.Equal(t, int64(0), stats.AvgObjSize)
}

func TestShardingInfo_IsSharded(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		info     ShardingInfo
		expected bool
	}{
		{
			name:     "missing shard key",
			info:     ShardingInfo{},
			expected: false,
		},
		{
			name: "present shard key",
			info: ShardingInfo{
				ShardKey: bson.D{{Key: "sku", Value: 1}},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.expected, tt.info.IsSharded())
		})
	}
}
