package main_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStartTargetWriteConcernPrecedence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		args []string
		env  map[string]string
		want *string
	}{
		{name: "omitted", env: map[string]string{"PCSM_TARGET_WRITE_CONCERN": ""}},
		{name: "flag", args: []string{"--target-write-concern=1"}, want: new("1")},
		{name: "environment", env: map[string]string{"PCSM_TARGET_WRITE_CONCERN": "2"}, want: new("2")},
		{
			name: "flag overrides environment", args: []string{"--target-write-concern=majority"},
			env: map[string]string{"PCSM_TARGET_WRITE_CONCERN": "1"}, want: new("majority"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			server := newMockServer(t, mockResponse{Ok: true})
			defer server.Close()
			args := append([]string{"--port", extractPort(server.URL), "start"}, tt.args...)
			_, stderr, err := runPCSM(t, args, tt.env)
			require.NoError(t, err, "stderr: %s", stderr)

			var body struct {
				TargetWriteConcern *string `json:"targetWriteConcern"`
			}
			require.NoError(t, json.Unmarshal(server.request.Body, &body))
			assert.Equal(t, tt.want, body.TargetWriteConcern)
		})
	}
}

func TestStartRejectsInvalidTargetWriteConcern(t *testing.T) {
	t.Parallel()

	for _, value := range []string{"0", "-1", "invalid", "2147483648"} {
		t.Run(value, func(t *testing.T) {
			t.Parallel()

			server := newMockServer(t, mockResponse{Ok: true})
			defer server.Close()
			_, _, err := runPCSM(t, []string{
				"--port", extractPort(server.URL), "start", "--target-write-concern=" + value,
			}, nil)
			require.Error(t, err)
			assert.Empty(t, server.request.Path)
		})
	}
}
