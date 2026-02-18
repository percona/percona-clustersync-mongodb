package main_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/percona/percona-clustersync-mongodb/errors"
)

var errCommandTimeout = errors.New("command timed out")

var binaryPath string //nolint:gochecknoglobals

func TestMain(m *testing.M) {
	os.Exit(runTestMain(m))
}

func runTestMain(m *testing.M) int {
	tmpDir, err := os.MkdirTemp("", "pcsm-integration-test")
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create temp dir: %v\n", err)

		return 1
	}
	defer os.RemoveAll(tmpDir)

	binaryPath = filepath.Join(tmpDir, "pcsm")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	cmd := exec.CommandContext(ctx, "go", "build", "-race", "-o", binaryPath, ".")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err = cmd.Run()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to build binary: %v\n", err)

		return 1
	}

	return m.Run()
}

type capturedRequest struct {
	Method string
	Path   string
	Body   []byte
}

type mockResponse struct {
	Ok             bool   `json:"ok"`
	Error          string `json:"error,omitempty"`
	State          string `json:"state,omitempty"`
	LagTimeSeconds int64  `json:"lagTimeSeconds,omitempty"`
}

type mockServer struct {
	*httptest.Server

	request capturedRequest
}

func newMockServer(t *testing.T, response any) *mockServer {
	t.Helper()

	mock := &mockServer{}

	mock.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mock.request.Method = r.Method
		mock.request.Path = r.URL.Path

		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("failed to read request body: %v", err)
			http.Error(w, "internal error", http.StatusInternalServerError)

			return
		}
		mock.request.Body = body

		w.Header().Set("Content-Type", "application/json")

		encErr := json.NewEncoder(w).Encode(response)
		if encErr != nil {
			t.Errorf("failed to encode response: %v", encErr)
		}
	}))

	return mock
}

func extractPort(serverURL string) string {
	parts := strings.Split(serverURL, ":")
	if len(parts) < 3 {
		return ""
	}

	return parts[len(parts)-1]
}

func runPCSM(t *testing.T, args []string, env map[string]string) (string, string, error) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, binaryPath, args...)
	cmd.Env = os.Environ()

	for k, v := range env {
		cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", k, v))
	}

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()

	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return stdout.String(), stderr.String(), errCommandTimeout
	}

	return stdout.String(), stderr.String(), err
}

type commandTestCase struct {
	name         string
	args         []string
	expectedBody map[string]any
}

// testCommandHTTPRequest tests that a command creates the expected HTTP request.
func testCommandHTTPRequest(t *testing.T, tc commandTestCase, expectedPath string) {
	t.Helper()

	server := newMockServer(t, mockResponse{Ok: true})
	defer server.Close()

	port := extractPort(server.URL)
	args := append([]string{"--port", port}, tc.args...)

	_, stderr, err := runPCSM(t, args, nil)
	require.NoError(t, err, "stderr: %s", stderr)

	assert.Equal(t, http.MethodPost, server.request.Method)
	assert.Equal(t, expectedPath, server.request.Path)

	var capturedBody map[string]any
	if len(server.request.Body) > 0 {
		require.NoError(t, json.Unmarshal(server.request.Body, &capturedBody))
	} else {
		capturedBody = map[string]any{}
	}

	want, err := json.Marshal(tc.expectedBody)
	require.NoError(t, err)

	got, err := json.Marshal(capturedBody)
	require.NoError(t, err)

	assert.JSONEq(t, string(want), string(got))
}

func TestStatusCommandStates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		response         mockResponse
		expectedInOutput []string
	}{
		{
			name:             "idle state",
			response:         mockResponse{Ok: true, State: "idle"},
			expectedInOutput: []string{`"ok": true`, `"state": "idle"`},
		},
		{
			name:             "running state with lag",
			response:         mockResponse{Ok: true, State: "running", LagTimeSeconds: 10},
			expectedInOutput: []string{`"ok": true`, `"state": "running"`, `"lagTimeSeconds": 10`},
		},
		{
			name:             "paused state",
			response:         mockResponse{Ok: true, State: "paused"},
			expectedInOutput: []string{`"ok": true`, `"state": "paused"`},
		},
		{
			name:             "failed state with error",
			response:         mockResponse{Ok: true, State: "failed", Error: "replication error occurred"},
			expectedInOutput: []string{`"ok": true`, `"state": "failed"`, `"error": "replication error occurred"`},
		},
		{
			name:             "finalizing state",
			response:         mockResponse{Ok: true, State: "finalizing"},
			expectedInOutput: []string{`"ok": true`, `"state": "finalizing"`},
		},
		{
			name:             "finalized state",
			response:         mockResponse{Ok: true, State: "finalized"},
			expectedInOutput: []string{`"ok": true`, `"state": "finalized"`},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			server := newMockServer(t, tt.response)
			defer server.Close()

			port := extractPort(server.URL)
			stdout, stderr, err := runPCSM(t, []string{"--port", port, "status"}, nil)
			require.NoError(t, err, "stderr: %s", stderr)

			captured := server.request

			assert.Equal(t, http.MethodGet, captured.Method)
			assert.Equal(t, "/status", captured.Path)

			for _, expected := range tt.expectedInOutput {
				assert.Contains(t, stdout, expected)
			}
		})
	}
}

func TestStartCommand(t *testing.T) {
	t.Parallel()

	tests := []commandTestCase{
		{
			name:         "no flags",
			args:         []string{"start"},
			expectedBody: map[string]any{},
		},
		{
			name:         "pause-on-initial-sync",
			args:         []string{"start", "--pause-on-initial-sync"},
			expectedBody: map[string]any{"pauseOnInitialSync": true},
		},
		{
			name:         "include-namespaces single",
			args:         []string{"start", "--include-namespaces=db.coll"},
			expectedBody: map[string]any{"includeNamespaces": []any{"db.coll"}},
		},
		{
			name:         "include-namespaces multiple",
			args:         []string{"start", "--include-namespaces=db1.coll1,db2.coll2"},
			expectedBody: map[string]any{"includeNamespaces": []any{"db1.coll1", "db2.coll2"}},
		},
		{
			name:         "exclude-namespaces single",
			args:         []string{"start", "--exclude-namespaces=db.coll"},
			expectedBody: map[string]any{"excludeNamespaces": []any{"db.coll"}},
		},
		{
			name:         "exclude-namespaces multiple",
			args:         []string{"start", "--exclude-namespaces=db1.*,db2.coll"},
			expectedBody: map[string]any{"excludeNamespaces": []any{"db1.*", "db2.coll"}},
		},
		{
			name: "all flags combined",
			args: []string{
				"start",
				"--pause-on-initial-sync",
				"--include-namespaces=db1.coll1",
				"--exclude-namespaces=db2.*",
			},
			expectedBody: map[string]any{
				"pauseOnInitialSync": true,
				"includeNamespaces":  []any{"db1.coll1"},
				"excludeNamespaces":  []any{"db2.*"},
			},
		},
		{
			name:         "clone-num-parallel-collections",
			args:         []string{"start", "--clone-num-parallel-collections=8"},
			expectedBody: map[string]any{"cloneNumParallelCollections": float64(8)},
		},
		{
			name:         "clone-num-read-workers",
			args:         []string{"start", "--clone-num-read-workers=16"},
			expectedBody: map[string]any{"cloneNumReadWorkers": float64(16)},
		},
		{
			name:         "clone-num-insert-workers",
			args:         []string{"start", "--clone-num-insert-workers=4"},
			expectedBody: map[string]any{"cloneNumInsertWorkers": float64(4)},
		},
		{
			name:         "clone-segment-size",
			args:         []string{"start", "--clone-segment-size=500MB"},
			expectedBody: map[string]any{"cloneSegmentSize": "500MB"},
		},
		{
			name:         "clone-read-batch-size",
			args:         []string{"start", "--clone-read-batch-size=32MiB"},
			expectedBody: map[string]any{"cloneReadBatchSize": "32MiB"},
		},
		{
			name: "all clone flags combined",
			args: []string{
				"start",
				"--clone-num-parallel-collections=8",
				"--clone-num-read-workers=16",
				"--clone-num-insert-workers=4",
				"--clone-segment-size=1GiB",
				"--clone-read-batch-size=48MB",
			},
			expectedBody: map[string]any{
				"cloneNumParallelCollections": float64(8),
				"cloneNumReadWorkers":         float64(16),
				"cloneNumInsertWorkers":       float64(4),
				"cloneSegmentSize":            "1GiB",
				"cloneReadBatchSize":          "48MB",
			},
		},
		{
			name: "all flags including clone options",
			args: []string{
				"start",
				"--pause-on-initial-sync",
				"--include-namespaces=db1.coll1",
				"--exclude-namespaces=db2.*",
				"--clone-num-parallel-collections=4",
				"--clone-segment-size=2GiB",
			},
			expectedBody: map[string]any{
				"pauseOnInitialSync":          true,
				"includeNamespaces":           []any{"db1.coll1"},
				"excludeNamespaces":           []any{"db2.*"},
				"cloneNumParallelCollections": float64(4),
				"cloneSegmentSize":            "2GiB",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			testCommandHTTPRequest(t, tt, "/start")
		})
	}
}

func TestFinalizeCommand(t *testing.T) {
	t.Parallel()

	tests := []commandTestCase{
		{
			name:         "no flags",
			args:         []string{"finalize"},
			expectedBody: map[string]any{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			testCommandHTTPRequest(t, tt, "/finalize")
		})
	}
}

func TestResumeCommand(t *testing.T) {
	t.Parallel()

	tests := []commandTestCase{
		{
			name:         "no flags",
			args:         []string{"resume"},
			expectedBody: map[string]any{},
		},
		{
			name:         "from-failure",
			args:         []string{"resume", "--from-failure"},
			expectedBody: map[string]any{"fromFailure": true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			testCommandHTTPRequest(t, tt, "/resume")
		})
	}
}

func TestPauseCommand(t *testing.T) {
	t.Parallel()

	tests := []commandTestCase{
		{
			name:         "pause sends empty body",
			args:         []string{"pause"},
			expectedBody: map[string]any{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			testCommandHTTPRequest(t, tt, "/pause")
		})
	}
}

func TestResumeFromFailure(t *testing.T) {
	t.Parallel()

	t.Run("resume succeeds from failed state with --from-failure", func(t *testing.T) {
		t.Parallel()

		server := newMockServer(t, mockResponse{Ok: true})
		defer server.Close()

		port := extractPort(server.URL)
		stdout, stderr, err := runPCSM(t, []string{"--port", port, "resume", "--from-failure"}, nil)
		require.NoError(t, err, "stderr: %s", stderr)

		captured := server.request

		assert.Equal(t, http.MethodPost, captured.Method)
		assert.Equal(t, "/resume", captured.Path)
		assert.Contains(t, stdout, `"ok": true`)

		var body map[string]any
		require.NoError(t, json.Unmarshal(captured.Body, &body))
		assert.Equal(t, true, body["fromFailure"])
	})

	t.Run("resume fails from failed state without --from-failure", func(t *testing.T) {
		t.Parallel()

		server := newMockServer(t, mockResponse{Ok: false, Error: "cannot resume: not paused or not resuming from failure"})
		defer server.Close()

		port := extractPort(server.URL)
		_, stderr, err := runPCSM(t, []string{"--port", port, "resume"}, nil)
		require.Error(t, err)

		captured := server.request

		assert.Equal(t, http.MethodPost, captured.Method)
		assert.Equal(t, "/resume", captured.Path)
		assert.Contains(t, stderr, "cannot resume")
	})
}

func TestPortConfiguration(t *testing.T) {
	t.Parallel()

	t.Run("port via flag", func(t *testing.T) {
		t.Parallel()

		server := newMockServer(t, mockResponse{Ok: true, State: "idle"})
		defer server.Close()

		port := extractPort(server.URL)
		_, stderr, err := runPCSM(t, []string{"--port", port, "status"}, nil)
		require.NoError(t, err, "stderr: %s", stderr)

		captured := server.request

		assert.Equal(t, "/status", captured.Path)
	})

	t.Run("port via PCSM_PORT env var", func(t *testing.T) {
		t.Parallel()

		server := newMockServer(t, mockResponse{Ok: true, State: "idle"})
		defer server.Close()

		port := extractPort(server.URL)
		_, stderr, err := runPCSM(t, []string{"status"}, map[string]string{"PCSM_PORT": port})
		require.NoError(t, err, "stderr: %s", stderr)

		captured := server.request

		assert.Equal(t, "/status", captured.Path)
	})

	t.Run("flag takes precedence over env var", func(t *testing.T) {
		t.Parallel()

		wrongServer := newMockServer(t, mockResponse{Ok: false, State: "wrong"})
		defer wrongServer.Close()

		wrongPort := extractPort(wrongServer.URL)

		correctServer := newMockServer(t, mockResponse{Ok: true, State: "correct"})
		defer correctServer.Close()

		correctPort := extractPort(correctServer.URL)

		stdout, stderr, err := runPCSM(
			t,
			[]string{"--port", correctPort, "status"},
			map[string]string{"PCSM_PORT": wrongPort},
		)
		require.NoError(t, err, "stderr: %s", stderr)

		captured := correctServer.request

		assert.Equal(t, "/status", captured.Path)
		assert.Contains(t, stdout, `"state": "correct"`)
	})
}

func TestStartConfigPrecedence(t *testing.T) {
	t.Parallel()

	t.Run("flag takes precedence over env var for clone-num-parallel-collections", func(t *testing.T) {
		t.Parallel()

		server := newMockServer(t, mockResponse{Ok: true})
		defer server.Close()

		port := extractPort(server.URL)
		_, stderr, err := runPCSM(
			t,
			[]string{"--port", port, "start", "--clone-num-parallel-collections=8"},
			map[string]string{"PCSM_CLONE_NUM_PARALLEL_COLLECTIONS": "2"},
		)
		require.NoError(t, err, "stderr: %s", stderr)

		captured := server.request

		assert.Equal(t, http.MethodPost, captured.Method)
		assert.Equal(t, "/start", captured.Path)

		var actualBody map[string]any
		require.NoError(t, json.Unmarshal(captured.Body, &actualBody))
		assert.EqualValues(t, 8, actualBody["cloneNumParallelCollections"])
	})

	t.Run("env var is used when flag not provided", func(t *testing.T) {
		t.Parallel()

		server := newMockServer(t, mockResponse{Ok: true})
		defer server.Close()

		port := extractPort(server.URL)
		_, stderr, err := runPCSM(
			t,
			[]string{"--port", port, "start"},
			map[string]string{"PCSM_CLONE_NUM_PARALLEL_COLLECTIONS": "4"},
		)
		require.NoError(t, err, "stderr: %s", stderr)

		captured := server.request

		assert.Equal(t, http.MethodPost, captured.Method)
		assert.Equal(t, "/start", captured.Path)

		var actualBody map[string]any
		require.NoError(t, json.Unmarshal(captured.Body, &actualBody))
		assert.EqualValues(t, 4, actualBody["cloneNumParallelCollections"])
	})
}

func TestConnectionRefused(t *testing.T) {
	t.Parallel()

	_, stderr, err := runPCSM(t, []string{"--port", "59999", "status"}, nil)

	require.Error(t, err)
	assert.Contains(t, stderr, "connection refused", "expected connection error, got: %s", stderr)
}
