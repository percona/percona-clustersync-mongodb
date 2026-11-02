package main

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/percona/percona-clustersync-mongodb/config"
)

func TestStartCommandRepeatedNamespaceFlags(t *testing.T) {
	t.Parallel()

	type receivedRequest struct {
		method string
		path   string
		body   startRequest
		err    error
	}

	received := make(chan receivedRequest, 1)
	var requests atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		got := receivedRequest{method: r.Method, path: r.URL.Path}
		got.err = json.NewDecoder(r.Body).Decode(&got.body)
		select {
		case received <- got:
		default:
		}
		w.Header().Set("Content-Type", "application/json")
		err := json.NewEncoder(w).Encode(startResponse{Ok: true})
		if err != nil {
			t.Errorf("encode start response: %v", err)
		}
	}))
	defer srv.Close()

	addr, ok := srv.Listener.Addr().(*net.TCPAddr)
	require.True(t, ok)
	testPort := addr.Port
	cmd := newStartCmd(&config.Config{Port: testPort})
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{
		"--include-namespaces=db1.first,db1.second",
		"--exclude-namespaces=db3.first,db3.second",
		"--include-namespaces=db2.third",
		"--exclude-namespaces=db4.third",
	})
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	require.NoError(t, cmd.ExecuteContext(ctx))

	select {
	case got := <-received:
		require.NoError(t, got.err)
		require.Equal(t, http.MethodPost, got.method)
		require.Equal(t, "/start", got.path)
		require.Equal(t, []string{"db1.first", "db1.second", "db2.third"}, got.body.IncludeNamespaces)
		require.Equal(t, []string{"db3.first", "db3.second", "db4.third"}, got.body.ExcludeNamespaces)
	case <-ctx.Done():
		t.Fatal("start request was not received")
	}
	require.Equal(t, int64(1), requests.Load())
}

func TestStartCommandRejectsNamespaceFlagSyntaxBeforeHTTP(t *testing.T) {
	t.Parallel()

	for _, flag := range []string{"include-namespaces", "exclude-namespaces"} {
		for _, tt := range []struct {
			name string
			arg  string
		}{
			{name: "missing argument", arg: "--" + flag},
			{name: "malformed CSV", arg: "--" + flag + `="db.coll`},
		} {
			t.Run(flag+"/"+tt.name, func(t *testing.T) {
				t.Parallel()

				var requests atomic.Int64
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					requests.Add(1)
					w.Header().Set("Content-Type", "application/json")
					err := json.NewEncoder(w).Encode(startResponse{Ok: true})
					if err != nil {
						t.Errorf("encode start response: %v", err)
					}
				}))
				defer srv.Close()

				addr, ok := srv.Listener.Addr().(*net.TCPAddr)
				require.True(t, ok)
				testPort := addr.Port
				cmd := newStartCmd(&config.Config{Port: testPort})
				cmd.SetOut(io.Discard)
				cmd.SetErr(io.Discard)
				cmd.SetArgs([]string{tt.arg})
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()

				err := cmd.ExecuteContext(ctx)

				require.Error(t, err)
				require.ErrorContains(t, err, "--"+flag)
				require.Zero(t, requests.Load())
			})
		}
	}
}
