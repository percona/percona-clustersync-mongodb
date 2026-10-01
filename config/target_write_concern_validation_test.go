package config_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/percona/percona-clustersync-mongodb/config"
)

func TestValidateTargetWriteConcern(t *testing.T) {
	t.Parallel()

	tests := []struct {
		value   string
		wantErr bool
	}{
		{value: ""},
		{value: "majority"},
		{value: "1"},
		{value: "0", wantErr: true},
		{value: "-1", wantErr: true},
		{value: "invalid", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.value, func(t *testing.T) {
			t.Parallel()

			err := config.Validate(&config.Config{
				Source:             "mongodb://source",
				Target:             "mongodb://target",
				TargetWriteConcern: tt.value,
			})
			if tt.wantErr {
				require.Error(t, err)

				return
			}
			require.NoError(t, err)
		})
	}
}
