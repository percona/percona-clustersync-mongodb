package config //nolint:testpackage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFilterCompressors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    []string
		expected []string
	}{
		{
			name:     "nil input",
			input:    nil,
			expected: nil,
		},
		{
			name:     "empty input",
			input:    []string{},
			expected: nil,
		},
		{
			name:     "single valid compressor",
			input:    []string{"zstd"},
			expected: []string{"zstd"},
		},
		{
			name:     "all valid compressors",
			input:    []string{"zstd", "zlib", "snappy"},
			expected: []string{"zstd", "zlib", "snappy"},
		},
		{
			name:     "invalid compressor filtered out",
			input:    []string{"zstd", "lz4", "snappy"},
			expected: []string{"zstd", "snappy"},
		},
		{
			name:     "all invalid compressors",
			input:    []string{"lz4", "brotli", "gzip"},
			expected: []string{},
		},
		{
			name:     "duplicates removed",
			input:    []string{"zstd", "zstd", "zlib"},
			expected: []string{"zstd", "zlib"},
		},
		{
			name:     "whitespace trimmed",
			input:    []string{" zstd ", " zlib"},
			expected: []string{"zstd", "zlib"},
		},
		{
			name:     "whitespace only entries filtered",
			input:    []string{" ", "zstd"},
			expected: []string{"zstd"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := filterCompressors(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}
