// Package alert posts page-worthy notifications (disk full, share down,
// peer offline, deletion guard, …) to an ntfy topic or a generic webhook.
//
// One JSON shape works for both: ntfy accepts {title, message, tags} posted
// to https://ntfy.sh/<topic>, and a generic webhook can read the same
// fields. The daemon fires only a curated, throttled set of events so a
// noisy condition cannot flood the endpoint.
package alert

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Notifier sends alerts to a configured URL. A nil receiver is a safe
// no-op, so callers can pass it through unconditionally.
type Notifier struct {
	URL    string
	Client *http.Client
	// Tags are attached to every alert (e.g. ["crosssync"]). ntfy renders
	// them as emoji; webhooks can ignore them.
	Tags []string
}

// New creates a notifier for url. Empty url returns a notifier that is a
// no-op (Fire returns nil).
func New(url string) *Notifier {
	return &Notifier{
		URL:    url,
		Client: &http.Client{Timeout: 10 * time.Second},
		Tags:   []string{"crosssync"},
	}
}

type payload struct {
	Title   string   `json:"title"`
	Message string   `json:"message"`
	Tags    []string `json:"tags,omitempty"`
}

// Fire posts one alert. It never blocks long (10s client timeout) and
// swallows nothing: the caller decides whether an error is worth logging.
func (n *Notifier) Fire(title, message string) error {
	if n == nil || n.URL == "" {
		return nil
	}
	body, err := json.Marshal(payload{Title: title, Message: message, Tags: n.Tags})
	if err != nil {
		return err
	}
	resp, err := n.Client.Post(n.URL, "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("alert endpoint returned %d", resp.StatusCode)
	}
	return nil
}
