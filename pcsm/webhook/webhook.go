// Package webhook provides a simple HTTP webhook notifier for PCSM lifecycle events.
package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"slices"
	"time"

	"github.com/percona/percona-clustersync-mongodb/log"
)

// Event represents a webhook event type.
type Event string

const (
	// EventCloneCompleted is emitted when the data clone phase completes successfully.
	EventCloneCompleted Event = "initial-sync:clone-completed"
	// EventCloneFailed is emitted when the data clone phase fails.
	EventCloneFailed Event = "initial-sync:clone-failed"
	// EventInitialSyncCompleted is emitted when the initial sync (clone + change stream catch-up) completes.
	EventInitialSyncCompleted Event = "initial-sync:completed"
	// EventFinalizationStarted is emitted when finalization begins.
	EventFinalizationStarted Event = "finalization:started"
	// EventFinalizationFinished is emitted when finalization completes successfully.
	EventFinalizationFinished Event = "finalization:finished"
	// EventPCSMStarted is emitted when replication starts (pcsm start).
	EventPCSMStarted Event = "pcsm:started"
	// EventReplicationFailed is emitted when change replication fails.
	EventReplicationFailed Event = "replication:failed"
	// EventReplicationPaused is emitted when replication is paused.
	EventReplicationPaused Event = "replication:paused"
)

// AllEvents returns all available webhook events.
func AllEvents() []Event {
	return []Event{
		EventCloneCompleted,
		EventCloneFailed,
		EventInitialSyncCompleted,
		EventFinalizationStarted,
		EventFinalizationFinished,
		EventPCSMStarted,
		EventReplicationFailed,
		EventReplicationPaused,
	}
}

// FailureEvents returns only failure-related webhook events.
func FailureEvents() []Event {
	return []Event{
		EventCloneFailed,
		EventReplicationFailed,
	}
}

// Payload is the JSON body sent to the webhook URL.
type Payload struct {
	Event     Event  `json:"event"`
	Timestamp string `json:"timestamp"`
	Message   string `json:"message,omitempty"`
}

// Config holds the webhook configuration.
type Config struct {
	URL       string
	AuthToken string
	Events    []Event
}

// Notifier sends HTTP POST requests to a configured webhook URL.
type Notifier struct {
	cfg    Config
	client *http.Client
}

// New creates a new Notifier. If cfg.URL is empty, Send is a no-op.
func New(cfg Config) *Notifier {
	return &Notifier{
		cfg: cfg,
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// Send sends a webhook notification for the given event.
// It runs the HTTP request in a separate goroutine so it never blocks the caller.
// It is safe to call on a nil Notifier.
func (n *Notifier) Send(event Event, message string) {
	if n == nil || n.cfg.URL == "" {
		return
	}

	if len(n.cfg.Events) > 0 && !slices.Contains(n.cfg.Events, event) {
		return
	}

	go n.send(event, message)
}

func (n *Notifier) send(event Event, message string) {
	lg := log.New("webhook")

	payload := Payload{
		Event:     event,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Message:   message,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		lg.Error(err, "Marshal webhook payload")

		return
	}

	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, n.cfg.URL, bytes.NewReader(body))
	if err != nil {
		lg.Error(err, "Create webhook request")

		return
	}

	req.Header.Set("Content-Type", "application/json")

	if n.cfg.AuthToken != "" {
		req.Header.Set("Authorization", "Bearer "+n.cfg.AuthToken)
	}

	resp, err := n.client.Do(req)
	if err != nil {
		lg.Errorf(err, "Send webhook for event %q", event)

		return
	}
	defer resp.Body.Close()

	if resp.StatusCode >= http.StatusBadRequest {
		lg.Warnf("Webhook for event %q returned status %d", event, resp.StatusCode)

		return
	}

	lg.Debugf("Webhook sent for event %q (status %d)", event, resp.StatusCode)
}
