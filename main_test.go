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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/percona/percona-clustersync-mongodb/errors"
)

// errCommandTimeout is returned when the command execution times out.
var errCommandTimeout = errors.New("command timed out")

// binaryPath holds the path to the compiled pcsm binary.
//
//nolint:gochecknoglobals
var binaryPath string

// TestMain builds the binary once before running all tests.
func TestMain(m *testing.M) {
	code := runTestMain(m)
	os.Exit(code)
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

	// Run tests
	return m.Run()
}

// capturedRequest holds the details of an HTTP request captured by the mock server.
type capturedRequest struct {
	Method string
	Path   string
	Body   []byte
}

// mockPCSMServer creates a mock PCSM HTTP server that captures requests.
// It returns the server and a channel that receives captured requests.
func mockPCSMServer(t *testing.T, response any) (*httptest.Server, *capturedRequest, *sync.Mutex) {
	t.Helper()

	var captured capturedRequest
	var mu sync.Mutex

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()

		captured.Method = r.Method
		captured.Path = r.URL.Path

		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("failed to read request body: %v", err)
			http.Error(w, "internal error", http.StatusInternalServerError)

			return
		}
		captured.Body = body

		w.Header().Set("Content-Type", "application/json")

		encErr := json.NewEncoder(w).Encode(response)
		if encErr != nil {
			t.Errorf("failed to encode response: %v", encErr)
		}
	}))

	return server, &captured, &mu
}

func extractPort(serverURL string) string {
	parts := strings.Split(serverURL, ":")
	if len(parts) < 3 {
		return ""
	}

	return parts[len(parts)-1]
}

// runPCSM runs the pcsm binary with the given arguments and environment variables.
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

type mockResponse struct {
	Ok             bool   `json:"ok"`
	Error          string `json:"error,omitempty"`
	State          string `json:"state,omitempty"`
	LagTimeSeconds int64  `json:"lagTimeSeconds,omitempty"`
}

func TestStatusCommand(t *testing.T) {
	t.Parallel()

	response := mockResponse{
		Ok:             true,
		State:          "running",
		LagTimeSeconds: 5,
	}

	server, captured, mu := mockPCSMServer(t, response)
	defer server.Close()

	port := extractPort(server.URL)

	stdout, stderr, err := runPCSM(t, []string{"--port", port, "status"}, nil)

	require.NoError(t, err, "stderr: %s", stderr)

	mu.Lock()
	defer mu.Unlock()

	assert.Equal(t, http.MethodGet, captured.Method)
	assert.Equal(t, "/status", captured.Path)
	assert.Empty(t, captured.Body)

	assert.Contains(t, stdout, `"ok": true`)
	assert.Contains(t, stdout, `"state": "running"`)
}

func TestStatusCommandStates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		response         mockResponse
		expectedInOutput []string
	}{
		{
			name: "idle state",
			response: mockResponse{
				Ok:    true,
				State: "idle",
			},
			expectedInOutput: []string{`"ok": true`, `"state": "idle"`},
		},
		{
			name: "running state with lag",
			response: mockResponse{
				Ok:             true,
				State:          "running",
				LagTimeSeconds: 10,
			},
			expectedInOutput: []string{`"ok": true`, `"state": "running"`, `"lagTimeSeconds": 10`},
		},
		{
			name: "paused state",
			response: mockResponse{
				Ok:    true,
				State: "paused",
			},
			expectedInOutput: []string{`"ok": true`, `"state": "paused"`},
		},
		{
			name: "failed state with error",
			response: mockResponse{
				Ok:    true,
				State: "failed",
				Error: "replication error occurred",
			},
			expectedInOutput: []string{`"ok": true`, `"state": "failed"`, `"error": "replication error occurred"`},
		},
		{
			name: "finalizing state",
			response: mockResponse{
				Ok:    true,
				State: "finalizing",
			},
			expectedInOutput: []string{`"ok": true`, `"state": "finalizing"`},
		},
		{
			name: "finalized state",
			response: mockResponse{
				Ok:    true,
				State: "finalized",
			},
			expectedInOutput: []string{`"ok": true`, `"state": "finalized"`},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			server, captured, mu := mockPCSMServer(t, tt.response)
			defer server.Close()

			port := extractPort(server.URL)

			stdout, stderr, err := runPCSM(t, []string{"--port", port, "status"}, nil)

			require.NoError(t, err, "stderr: %s", stderr)

			mu.Lock()
			defer mu.Unlock()

			assert.Equal(t, http.MethodGet, captured.Method)
			assert.Equal(t, "/status", captured.Path)

			for _, expected := range tt.expectedInOutput {
				assert.Contains(t, stdout, expected)
			}
		})
	}
}

func TestStartCommandErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		response       mockResponse
		expectedOutput string
	}{
		{
			name:           "error when already running",
			response:       mockResponse{Ok: false, Error: "already running"},
			expectedOutput: "already running",
		},
		{
			name:           "error when in failed state",
			response:       mockResponse{Ok: false, Error: "already running"},
			expectedOutput: "already running",
		},
		{
			name:           "error when paused",
			response:       mockResponse{Ok: false, Error: "paused"},
			expectedOutput: "paused",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			server, captured, mu := mockPCSMServer(t, tt.response)
			defer server.Close()

			port := extractPort(server.URL)

			stdout, stderr, err := runPCSM(t, []string{"--port", port, "start"}, nil)

			require.NoError(t, err, "stderr: %s", stderr)

			mu.Lock()
			defer mu.Unlock()

			assert.Equal(t, http.MethodPost, captured.Method)
			assert.Equal(t, "/start", captured.Path)
			assert.Contains(t, stdout, `"ok": false`)
			assert.Contains(t, stdout, tt.expectedOutput)
		})
	}
}

func TestStartCommand(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		args         []string
		expectedBody map[string]any
	}{
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
		// Clone tuning flags
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

			response := mockResponse{Ok: true}
			server, captured, mu := mockPCSMServer(t, response)
			defer server.Close()

			port := extractPort(server.URL)

			// Prepend port flag
			args := append([]string{"--port", port}, tt.args...)

			_, stderr, err := runPCSM(t, args, nil)
			require.NoError(t, err, "stderr: %s", stderr)

			mu.Lock()
			defer mu.Unlock()

			assert.Equal(t, http.MethodPost, captured.Method)
			assert.Equal(t, "/start", captured.Path)

			var actualBody map[string]any
			if len(captured.Body) > 0 {
				err = json.Unmarshal(captured.Body, &actualBody)
				require.NoError(t, err)
			} else {
				actualBody = map[string]any{}
			}

			expectedJSON, _ := json.Marshal(tt.expectedBody)
			actualJSON, _ := json.Marshal(actualBody)
			assert.JSONEq(t, string(expectedJSON), string(actualJSON))
		})
	}
}

func TestFinalizeCommand(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		args         []string
		expectedBody map[string]any
	}{
		{
			name:         "no flags",
			args:         []string{"finalize"},
			expectedBody: map[string]any{},
		},
		{
			name:         "ignore-history-lost",
			args:         []string{"finalize", "--ignore-history-lost"},
			expectedBody: map[string]any{"ignoreHistoryLost": true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			response := mockResponse{Ok: true}
			server, captured, mu := mockPCSMServer(t, response)
			defer server.Close()

			port := extractPort(server.URL)
			args := append([]string{"--port", port}, tt.args...)

			_, stderr, err := runPCSM(t, args, nil)
			require.NoError(t, err, "stderr: %s", stderr)

			mu.Lock()
			defer mu.Unlock()

			assert.Equal(t, http.MethodPost, captured.Method)
			assert.Equal(t, "/finalize", captured.Path)

			var actualBody map[string]any
			if len(captured.Body) > 0 {
				err = json.Unmarshal(captured.Body, &actualBody)
				require.NoError(t, err)
			} else {
				actualBody = map[string]any{}
			}

			expectedJSON, _ := json.Marshal(tt.expectedBody)
			actualJSON, _ := json.Marshal(actualBody)
			assert.JSONEq(t, string(expectedJSON), string(actualJSON))
		})
	}
}

func TestPauseCommand(t *testing.T) {
	t.Parallel()

	response := mockResponse{Ok: true}
	server, captured, mu := mockPCSMServer(t, response)
	defer server.Close()

	port := extractPort(server.URL)

	_, stderr, err := runPCSM(t, []string{"--port", port, "pause"}, nil)
	require.NoError(t, err, "stderr: %s", stderr)

	mu.Lock()
	defer mu.Unlock()

	assert.Equal(t, http.MethodPost, captured.Method)
	assert.Equal(t, "/pause", captured.Path)
	// Pause sends nil body, which becomes empty string
	assert.Empty(t, captured.Body)
}

func TestPauseCommandErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		response       mockResponse
		expectedOutput string
	}{
		{
			name:           "pause fails when not running",
			response:       mockResponse{Ok: false, Error: "cannot pause: Change Replication is not runnning"},
			expectedOutput: "cannot pause",
		},
		{
			name:           "pause fails when already paused",
			response:       mockResponse{Ok: false, Error: "cannot pause: Change Replication is not runnning"},
			expectedOutput: "cannot pause",
		},
		{
			name:           "pause fails in idle state",
			response:       mockResponse{Ok: false, Error: "cannot pause: Change Replication is not runnning"},
			expectedOutput: "cannot pause",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			server, captured, mu := mockPCSMServer(t, tt.response)
			defer server.Close()

			port := extractPort(server.URL)

			stdout, stderr, err := runPCSM(t, []string{"--port", port, "pause"}, nil)

			require.NoError(t, err, "stderr: %s", stderr)

			mu.Lock()
			defer mu.Unlock()

			assert.Equal(t, http.MethodPost, captured.Method)
			assert.Equal(t, "/pause", captured.Path)
			assert.Contains(t, stdout, `"ok": false`)
			assert.Contains(t, stdout, tt.expectedOutput)
		})
	}
}

func TestResumeCommand(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		args         []string
		expectedBody map[string]any
	}{
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

			response := mockResponse{Ok: true}
			server, captured, mu := mockPCSMServer(t, response)
			defer server.Close()

			port := extractPort(server.URL)
			args := append([]string{"--port", port}, tt.args...)

			_, stderr, err := runPCSM(t, args, nil)
			require.NoError(t, err, "stderr: %s", stderr)

			mu.Lock()
			defer mu.Unlock()

			assert.Equal(t, http.MethodPost, captured.Method)
			assert.Equal(t, "/resume", captured.Path)

			var actualBody map[string]any
			if len(captured.Body) > 0 {
				err = json.Unmarshal(captured.Body, &actualBody)
				require.NoError(t, err)
			} else {
				actualBody = map[string]any{}
			}

			expectedJSON, _ := json.Marshal(tt.expectedBody)
			actualJSON, _ := json.Marshal(actualBody)
			assert.JSONEq(t, string(expectedJSON), string(actualJSON))
		})
	}
}

func TestResumeCommandErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		args           []string
		response       mockResponse
		expectedOutput string
	}{
		{
			name:           "resume fails in failed state without --from-failure",
			args:           []string{"resume"},
			response:       mockResponse{Ok: false, Error: "cannot resume: not paused"},
			expectedOutput: "cannot resume",
		},
		{
			name:           "resume fails when replication not started",
			args:           []string{"resume"},
			response:       mockResponse{Ok: false, Error: "cannot resume: replication is not started"},
			expectedOutput: "cannot resume",
		},
		{
			name:           "resume fails when not paused and using --from-failure",
			args:           []string{"resume", "--from-failure"},
			response:       mockResponse{Ok: false, Error: "cannot resume: replication is not paused"},
			expectedOutput: "cannot resume",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			server, captured, mu := mockPCSMServer(t, tt.response)
			defer server.Close()

			port := extractPort(server.URL)
			args := append([]string{"--port", port}, tt.args...)

			stdout, stderr, err := runPCSM(t, args, nil)

			require.NoError(t, err, "stderr: %s", stderr)

			mu.Lock()
			defer mu.Unlock()

			assert.Equal(t, http.MethodPost, captured.Method)
			assert.Equal(t, "/resume", captured.Path)
			assert.Contains(t, stdout, `"ok": false`)
			assert.Contains(t, stdout, tt.expectedOutput)
		})
	}
}

func TestPortConfiguration(t *testing.T) {
	t.Parallel()

	t.Run("port via flag", func(t *testing.T) {
		t.Parallel()

		response := mockResponse{Ok: true, State: "idle"}
		server, captured, mu := mockPCSMServer(t, response)
		defer server.Close()

		port := extractPort(server.URL)

		_, stderr, err := runPCSM(t, []string{"--port", port, "status"}, nil)
		require.NoError(t, err, "stderr: %s", stderr)

		mu.Lock()
		defer mu.Unlock()

		assert.Equal(t, "/status", captured.Path)
	})

	t.Run("port via PCSM_PORT env var", func(t *testing.T) {
		t.Parallel()

		response := mockResponse{Ok: true, State: "idle"}
		server, captured, mu := mockPCSMServer(t, response)
		defer server.Close()

		port := extractPort(server.URL)

		_, stderr, err := runPCSM(t, []string{"status"}, map[string]string{
			"PCSM_PORT": port,
		})
		require.NoError(t, err, "stderr: %s", stderr)

		mu.Lock()
		defer mu.Unlock()

		assert.Equal(t, "/status", captured.Path)
	})

	t.Run("flag takes precedence over env var", func(t *testing.T) {
		t.Parallel()

		// Create two servers - one for env var (wrong), one for flag (correct)
		wrongResponse := mockResponse{Ok: false, State: "wrong"}
		wrongServer, _, _ := mockPCSMServer(t, wrongResponse)
		defer wrongServer.Close()
		wrongPort := extractPort(wrongServer.URL)

		correctResponse := mockResponse{Ok: true, State: "correct"}
		correctServer, captured, mu := mockPCSMServer(t, correctResponse)
		defer correctServer.Close()
		correctPort := extractPort(correctServer.URL)

		stdout, stderr, err := runPCSM(t, []string{"--port", correctPort, "status"}, map[string]string{
			"PCSM_PORT": wrongPort,
		})
		require.NoError(t, err, "stderr: %s", stderr)

		mu.Lock()
		defer mu.Unlock()

		// Should have hit the correct server (flag)
		assert.Equal(t, "/status", captured.Path)
		assert.Contains(t, stdout, `"state": "correct"`)
	})
}

func TestStartConfigPrecedence(t *testing.T) {
	t.Parallel()

	t.Run("flag takes precedence over env var for clone-num-parallel-collections", func(t *testing.T) {
		t.Parallel()

		response := mockResponse{Ok: true}
		server, captured, mu := mockPCSMServer(t, response)
		defer server.Close()

		port := extractPort(server.URL)

		_, stderr, err := runPCSM(t,
			[]string{"--port", port, "start", "--clone-num-parallel-collections=8"},
			map[string]string{
				"PCSM_CLONE_NUM_PARALLEL_COLLECTIONS": "2",
			})
		require.NoError(t, err, "stderr: %s", stderr)

		mu.Lock()
		defer mu.Unlock()

		assert.Equal(t, http.MethodPost, captured.Method)
		assert.Equal(t, "/start", captured.Path)

		var actualBody map[string]any
		err = json.Unmarshal(captured.Body, &actualBody)
		require.NoError(t, err)

		assert.EqualValues(t, 8, actualBody["cloneNumParallelCollections"])
	})

	t.Run("env var is used when flag not provided", func(t *testing.T) {
		t.Parallel()

		response := mockResponse{Ok: true}
		server, captured, mu := mockPCSMServer(t, response)
		defer server.Close()

		port := extractPort(server.URL)

		_, stderr, err := runPCSM(t,
			[]string{"--port", port, "start"},
			map[string]string{
				"PCSM_CLONE_NUM_PARALLEL_COLLECTIONS": "4",
			})
		require.NoError(t, err, "stderr: %s", stderr)

		mu.Lock()
		defer mu.Unlock()

		assert.Equal(t, http.MethodPost, captured.Method)
		assert.Equal(t, "/start", captured.Path)

		var actualBody map[string]any
		err = json.Unmarshal(captured.Body, &actualBody)
		require.NoError(t, err)

		assert.EqualValues(t, 4, actualBody["cloneNumParallelCollections"])
	})
}

func TestConnectionRefused(t *testing.T) {
	t.Parallel()

	_, stderr, err := runPCSM(t, []string{"--port", "59999", "status"}, nil)

	// Should fail with connection refused
	require.Error(t, err)
	// Error should mention connection issue
	combinedOutput := stderr
	assert.True(t,
		strings.Contains(combinedOutput, "connection refused") ||
			strings.Contains(combinedOutput, "connect:") ||
			strings.Contains(combinedOutput, "dial"),
		"expected connection error, got: %s", combinedOutput)
}
