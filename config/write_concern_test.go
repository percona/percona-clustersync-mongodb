package config_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/percona/percona-clustersync-mongodb/config"
)

func TestParseTargetWriteConcern(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		input string
		want  any
	}{
		{"", "majority"},
		{"majority", "majority"},
		{"1", 1},
		{"2", 2},
		{"0002", 2},
		{"2147483647", 2147483647},
	} {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			wc, err := config.ParseTargetWriteConcern(tt.input)
			require.NoError(t, err)
			assert.Equal(t, tt.want, wc.W)
			assert.Nil(t, wc.Journal)
			assert.True(t, wc.Acknowledged())
		})
	}

	for _, input := range []string{
		"0", "00", "-1", "+1", "2147483648", "99999999999999999999",
		"tag", "Majority", " majority", "1 ", "1.0", "1e2", "0x1", " ",
	} {
		t.Run(input, func(t *testing.T) {
			t.Parallel()
			wc, err := config.ParseTargetWriteConcern(input)
			require.Error(t, err)
			assert.Nil(t, wc)
		})
	}
}
