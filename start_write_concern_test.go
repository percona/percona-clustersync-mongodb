package main //nolint:testpackage // Tests request resolution before pipeline startup.

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/percona/percona-clustersync-mongodb/config"
)

func TestResolveStartOptionsTargetWriteConcern(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		config  string
		body    string
		want    string
		wantErr bool
	}{
		{name: "default", body: `{}`, want: ""},
		{name: "server setting ignored", config: "1", body: `{}`, want: ""},
		{name: "invalid server setting ignored", config: "invalid", body: `{}`, want: ""},
		{name: "request sets run", body: `{"targetWriteConcern":"1"}`, want: "1"},
		{name: "explicit majority", config: "1", body: `{"targetWriteConcern":"majority"}`, want: "majority"},
		{name: "unacknowledged request", body: `{"targetWriteConcern":"0"}`, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var req startRequest
			require.NoError(t, json.Unmarshal([]byte(tt.body), &req))
			opts, err := resolveStartOptions(&config.Config{TargetWriteConcern: tt.config}, req)
			if tt.wantErr {
				require.Error(t, err)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, opts.TargetWriteConcern)
		})
	}
}
