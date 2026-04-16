package webhook //nolint:testpackage

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSend_DeliversCorrectPayload(t *testing.T) {
	t.Parallel()

	var gotReq *http.Request
	var gotBody []byte

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotReq = r
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	n := New(Config{URL: srv.URL})
	n.send(EventPCSMStarted, "test message")

	require.NotNil(t, gotReq)
	assert.Equal(t, http.MethodPost, gotReq.Method)
	assert.Equal(t, "application/json", gotReq.Header.Get("Content-Type"))

	var payload Payload
	err := json.Unmarshal(gotBody, &payload)
	require.NoError(t, err)

	assert.Equal(t, EventPCSMStarted, payload.Event)
	assert.Equal(t, "test message", payload.Message)

	_, err = time.Parse(time.RFC3339, payload.Timestamp)
	assert.NoError(t, err, "timestamp should be valid RFC3339")
}

func TestSend_SetsAuthHeader(t *testing.T) {
	t.Parallel()

	var gotAuth string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	n := New(Config{URL: srv.URL, AuthToken: "my-token"})
	n.send(EventPCSMStarted, "")

	assert.Equal(t, "Bearer my-token", gotAuth)
}

func TestSend_NoAuthHeaderWhenEmpty(t *testing.T) {
	t.Parallel()

	var gotAuth string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	n := New(Config{URL: srv.URL})
	n.send(EventPCSMStarted, "")

	assert.Empty(t, gotAuth)
}

func TestSend_FiltersEvents(t *testing.T) {
	t.Parallel()

	var callCount atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	n := New(Config{URL: srv.URL, Events: FailureEvents()})

	// Failure event should be sent
	n.send(EventCloneFailed, "clone failed")
	assert.Equal(t, int32(1), callCount.Load())

	// Non-failure event should be filtered out (Send checks filter before calling send)
	n.Send(EventPCSMStarted, "started")
	time.Sleep(50 * time.Millisecond)

	assert.Equal(t, int32(1), callCount.Load(), "non-failure event should be filtered out")
}

func TestSend_NoFilterSendsAll(t *testing.T) {
	t.Parallel()

	var callCount atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	n := New(Config{URL: srv.URL})

	n.Send(EventPCSMStarted, "started")
	n.Send(EventCloneFailed, "failed")
	time.Sleep(100 * time.Millisecond)

	assert.Equal(t, int32(2), callCount.Load(), "all events should be sent when no filter is set")
}

func TestSend_NilNotifier(t *testing.T) {
	t.Parallel()

	var n *Notifier

	assert.NotPanics(t, func() {
		n.Send(EventPCSMStarted, "should not panic")
	})
}

func TestSend_EmptyURL(t *testing.T) {
	t.Parallel()

	var called atomic.Bool

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called.Store(true)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	// URL is empty even though server exists
	n := New(Config{URL: ""})

	n.Send(EventPCSMStarted, "should not send")
	time.Sleep(50 * time.Millisecond)

	assert.False(t, called.Load(), "no request should be made when URL is empty")
}

func TestSend_SlackPayload(t *testing.T) {
	t.Parallel()

	var gotBody []byte

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	n := New(Config{URL: srv.URL, Target: "slack"})
	n.send(EventReplicationFailed, "change stream error")

	var payload map[string]any
	err := json.Unmarshal(gotBody, &payload)
	require.NoError(t, err)

	text, ok := payload["text"].(string)
	require.True(t, ok)

	assert.Contains(t, text, "*PCSM*")
	assert.Contains(t, text, "*Event:* `replication:failed`")
	assert.Contains(t, text, "*Message:* change stream error")
	assert.Contains(t, text, "*Time:*")
	assert.NotContains(t, payload, "event", "slack payload should not have 'event' field")
}

func TestSend_SlackNoAuthHeader(t *testing.T) {
	t.Parallel()

	var gotAuth string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	n := New(Config{URL: srv.URL, Target: "slack", AuthToken: "should-be-ignored"})
	n.send(EventPCSMStarted, "test")

	assert.Empty(t, gotAuth, "Slack webhooks should not send Authorization header")
}

func TestFailureEvents(t *testing.T) {
	t.Parallel()

	events := FailureEvents()

	assert.Equal(t, []Event{EventCloneFailed, EventReplicationFailed}, events)
}
