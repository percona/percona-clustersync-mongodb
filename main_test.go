package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

const startPath = "/start"

type testHelper struct {
	t      *testing.T
	server *httptest.Server
}

func extractPort(serverURL string) int {
	u, err := url.Parse(serverURL)
	if err != nil {
		return 0
	}

	port, _ := strconv.Atoi(u.Port())

	return port
}

// newTestHelper creates a new test helper.
func newTestHelper(t *testing.T) *testHelper {
	t.Helper()

	return &testHelper{
		t:      t,
		server: nil,
	}
}

// cleanup closes the test server.
func (h *testHelper) cleanup() {
	if h.server != nil {
		h.server.Close()
	}
}

// setupCommand prepares a command for testing with the test server port.
func (h *testHelper) setupCommand(cmd *cobra.Command) func() {
	originalRunE := cmd.RunE
	cmd.RunE = func(c *cobra.Command, args []string) error {
		port := extractPort(h.server.URL)
		if err := c.Flags().Set("port", strconv.Itoa(port)); err != nil {
			return fmt.Errorf("failed to set port flag: %w", err)
		}

		return originalRunE(c, args)
	}

	return func() { cmd.RunE = originalRunE }
}

func TestStartCmd(t *testing.T) {
	t.Parallel()
	testCmd := &cobra.Command{
		Use:  "start",
		RunE: startCmd.RunE,
	}

	testCmd.Flags().Int("port", 3000, "Port number")
	testCmd.Flags().StringSlice("include-namespaces", nil,
		"Namespaces to include in the replication (e.g. db1.collection1,db2.collection2)")
	testCmd.Flags().StringSlice("exclude-namespaces", nil,
		"Namespaces to exclude from the replication (e.g. db3.collection3,db4.*)")

	h := newTestHelper(t)
	t.Cleanup(h.cleanup)

	h.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != startPath {
			return
		}

		var startReq startRequest

		if r.ContentLength > 0 {
			data, err := io.ReadAll(r.Body)
			if err != nil {
				t.Errorf("Failed to read request body: %v", err)

				return
			}
			if err := json.Unmarshal(data, &startReq); err != nil {
				t.Errorf("Failed to unmarshal request: %v", err)

				return
			}
		}

		expectedInclude := []string{"db1.collection1"}
		expectedExclude := []string{"db3.collection3", "db4.*"}

		if !reflect.DeepEqual(startReq.IncludeNamespaces, expectedInclude) {
			t.Errorf("Expected include namespaces %v, got %v", expectedInclude, startReq.IncludeNamespaces)
		}

		if !reflect.DeepEqual(startReq.ExcludeNamespaces, expectedExclude) {
			t.Errorf("Expected exclude namespaces %v, got %v", expectedExclude, startReq.ExcludeNamespaces)
		}

		response := startResponse{Ok: true}
		if err := json.NewEncoder(w).Encode(response); err != nil {
			t.Errorf("Failed to encode response: %v", err)
		}
	}))

	cleanup := h.setupCommand(testCmd)
	defer cleanup()

	testCmd.SetArgs([]string{"--exclude-namespaces=db3.collection3,db4.*", "--include-namespaces=db1.collection1"})
	err := testCmd.Execute()
	require.NoError(t, err)
}

func TestStartCmd_NamespaceValidation(t *testing.T) {
	t.Parallel()
	testCmd := &cobra.Command{
		Use:  "start",
		RunE: startCmd.RunE,
	}

	testCmd.Flags().Int("port", 3000, "Port number")
	testCmd.Flags().StringSlice("include-namespaces", nil,
		"Namespaces to include in the replication (e.g. db1.collection1,db2.collection2)")
	testCmd.Flags().StringSlice("exclude-namespaces", nil,
		"Namespaces to exclude from the replication (e.g. db3.collection3,db4.*)")

	h := newTestHelper(t)
	t.Cleanup(h.cleanup)

	h.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != startPath {
			return
		}

		var startReq startRequest

		if r.ContentLength > 0 {
			data, err := io.ReadAll(r.Body)
			if err != nil {
				t.Errorf("Failed to read request body: %v", err)

				return
			}
			if err := json.Unmarshal(data, &startReq); err != nil {
				t.Errorf("Failed to unmarshal request: %v", err)

				return
			}
		}

		t.Logf("Received exclude namespaces: %v", startReq.ExcludeNamespaces)
		t.Logf("Received include namespaces: %v", startReq.IncludeNamespaces)

		response := startResponse{Ok: true}
		if err := json.NewEncoder(w).Encode(response); err != nil {
			t.Errorf("Failed to encode response: %v", err)
		}
	}))

	cleanup := h.setupCommand(testCmd)
	defer cleanup()

	testCases := []struct {
		name        string
		excludeArgs string
		expectError bool
	}{
		{"valid_db.collection", "db.collection", false},
		{"valid_db.*", "db.*", false},
		{"valid_multiple", "db1.collection1,db2.*", false},
		{"malformed_extra_chars", "$#@!!#invalid", false},   // validation should be added?
		{"malformed_double_dot", "dfa..collection", false},  // validation should be added?
		{"malformed_trailing_dot", "db.collection.", false}, // validation should be added?
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// Create a fresh command instance for each sub-test
			subTestCmd := &cobra.Command{
				Use:  "start",
				RunE: startCmd.RunE,
			}
			subTestCmd.Flags().Int("port", 3000, "Port number")
			subTestCmd.Flags().StringSlice("include-namespaces", nil,
				"Namespaces to include in the replication (e.g. db1.collection1,db2.collection2)")
			subTestCmd.Flags().StringSlice("exclude-namespaces", nil,
				"Namespaces to exclude from the replication (e.g. db3.collection3,db4.*)")

			// Create a fresh test helper for each sub-test
			subH := newTestHelper(t)
			t.Cleanup(subH.cleanup)

			subH.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != startPath {
					return
				}

				var startReq startRequest

				if r.ContentLength > 0 {
					data, err := io.ReadAll(r.Body)
					if err != nil {
						t.Errorf("Failed to read request body: %v", err)

						return
					}
					if err := json.Unmarshal(data, &startReq); err != nil {
						t.Errorf("Failed to unmarshal request: %v", err)

						return
					}
				}

				t.Logf("Received exclude namespaces: %v", startReq.ExcludeNamespaces)
				t.Logf("Received include namespaces: %v", startReq.IncludeNamespaces)

				response := startResponse{Ok: true}
				if err := json.NewEncoder(w).Encode(response); err != nil {
					t.Errorf("Failed to encode response: %v", err)
				}
			}))

			cleanup := subH.setupCommand(subTestCmd)
			defer cleanup()

			subTestCmd.SetArgs([]string{"--exclude-namespaces=" + tc.excludeArgs})
			err := subTestCmd.Execute()

			if tc.expectError {
				require.Error(t, err, "Expected error for: %s", tc.excludeArgs)
			} else {
				require.NoError(t, err, "Expected success for: %s", tc.excludeArgs)
			}
		})
	}
}

func TestStatusCmd(t *testing.T) {
	t.Parallel()
	testCmd := &cobra.Command{
		Use:  "status",
		RunE: statusCmd.RunE,
	}
	testCmd.Flags().Int("port", 3000, "Port number")

	h := newTestHelper(t)
	t.Cleanup(h.cleanup)

	h.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/status" {
			response := statusResponse{Ok: true, State: "idle"}
			if err := json.NewEncoder(w).Encode(response); err != nil {
				t.Errorf("Failed to encode response: %v", err)
			}
		}
	}))

	cleanup := h.setupCommand(testCmd)
	defer cleanup()

	testCmd.SetArgs([]string{})
	err := testCmd.Execute()
	require.NoError(t, err)
}

func TestFinalizeCmd(t *testing.T) {
	t.Parallel()
	testCmd := &cobra.Command{
		Use:  "finalize",
		RunE: finalizeCmd.RunE,
	}
	testCmd.Flags().Int("port", 3000, "Port number")
	testCmd.Flags().Bool("ignore-history-lost", false, "Ignore history lost error")

	h := newTestHelper(t)
	t.Cleanup(h.cleanup)

	h.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/finalize" {
			return
		}

		var finalizeReq finalizeRequest

		if r.ContentLength > 0 {
			data, err := io.ReadAll(r.Body)
			if err != nil {
				t.Errorf("Failed to read request body: %v", err)

				return
			}
			if err := json.Unmarshal(data, &finalizeReq); err != nil {
				t.Errorf("Failed to unmarshal request: %v", err)

				return
			}
		}

		if !finalizeReq.IgnoreHistoryLost {
			t.Errorf("Expected IgnoreHistoryLost to be true, got false")
		}

		response := finalizeResponse{Ok: true}
		if err := json.NewEncoder(w).Encode(response); err != nil {
			t.Errorf("Failed to encode response: %v", err)
		}
	}))

	cleanup := h.setupCommand(testCmd)
	defer cleanup()

	testCmd.SetArgs([]string{"--ignore-history-lost"})
	err := testCmd.Execute()
	require.NoError(t, err)
}

func TestPauseCmd(t *testing.T) {
	t.Parallel()
	testCmd := &cobra.Command{
		Use:  "pause",
		RunE: pauseCmd.RunE,
	}
	testCmd.Flags().Int("port", 3000, "Port number")

	h := newTestHelper(t)
	t.Cleanup(h.cleanup)

	h.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/pause" {
			response := pauseResponse{Ok: true}
			if err := json.NewEncoder(w).Encode(response); err != nil {
				t.Errorf("Failed to encode response: %v", err)
			}
		}
	}))

	cleanup := h.setupCommand(testCmd)
	defer cleanup()

	testCmd.SetArgs([]string{})
	err := testCmd.Execute()
	require.NoError(t, err)
}

func TestResumeCmd(t *testing.T) {
	t.Parallel()
	testCmd := &cobra.Command{
		Use:  "resume",
		RunE: resumeCmd.RunE,
	}
	testCmd.Flags().Int("port", 3000, "Port number")
	testCmd.Flags().Bool("from-failure", false, "Resume from failure")

	h := newTestHelper(t)
	t.Cleanup(h.cleanup)

	h.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/resume" {
			return
		}

		var resumeReq resumeRequest

		if r.ContentLength > 0 {
			data, err := io.ReadAll(r.Body)
			if err != nil {
				t.Errorf("Failed to read request body: %v", err)

				return
			}
			if err := json.Unmarshal(data, &resumeReq); err != nil {
				t.Errorf("Failed to unmarshal request: %v", err)

				return
			}
		}

		if !resumeReq.FromFailure {
			t.Errorf("Expected FromFailure to be true, got false")
		}

		response := resumeResponse{Ok: true}
		if err := json.NewEncoder(w).Encode(response); err != nil {
			t.Errorf("Failed to encode response: %v", err)
		}
	}))

	cleanup := h.setupCommand(testCmd)
	defer cleanup()

	testCmd.SetArgs([]string{"--from-failure"})
	err := testCmd.Execute()
	require.NoError(t, err)
}
