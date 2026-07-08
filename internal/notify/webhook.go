// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package notify

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"sluicesync.dev/sluice/internal/diagnose"
)

// defaultHTTPTimeout bounds a single notification POST. A notification is
// advisory; it must never block the alerter goroutine for long, so the
// per-request timeout is short. Applied when a sink's HTTPClient is nil.
const defaultHTTPTimeout = 10 * time.Second

// WebhookNotifier POSTs a [Notification] as JSON to a generic endpoint, so
// an operator can fan an alert out to anything that accepts a JSON webhook
// (their own relay, a PagerDuty/Opsgenie ingress, a Lambda, etc.). The
// body is the stable, documented shape below — distinct from the
// Slack-specific [SlackNotifier].
type WebhookNotifier struct {
	// URL is the POST target. Treated as a credential (it embeds the
	// caller's secret path) — set via env, never logged.
	URL string

	// HTTPClient is optional; nil ⇒ a client with [defaultHTTPTimeout].
	HTTPClient *http.Client
}

// webhookPayload is the documented JSON body of a generic-webhook POST. The
// field names are snake_case + stable so downstream consumers can parse it.
type webhookPayload struct {
	Level     string    `json:"level"`
	StreamID  string    `json:"stream_id"`
	Metric    string    `json:"metric"`
	Title     string    `json:"title"`
	Body      string    `json:"body"`
	Value     float64   `json:"value"`
	Threshold float64   `json:"threshold"`
	At        time.Time `json:"at"`
}

// Notify POSTs n as JSON. A non-2xx response is an error. ctx bounds the
// request (in addition to the client timeout).
func (w *WebhookNotifier) Notify(ctx context.Context, n Notification) error {
	body := webhookPayload{
		Level:     string(n.Level),
		StreamID:  n.StreamID,
		Metric:    n.Metric,
		Title:     n.Title,
		Body:      n.Body,
		Value:     n.Value,
		Threshold: n.Threshold,
		At:        n.At,
	}
	return postJSON(ctx, w.client(), w.Name(), w.URL, body)
}

// Name implements [Notifier].
func (w *WebhookNotifier) Name() string { return "webhook" }

func (w *WebhookNotifier) client() *http.Client {
	if w.HTTPClient != nil {
		return w.HTTPClient
	}
	return &http.Client{Timeout: defaultHTTPTimeout}
}

// postJSON marshals v and POSTs it to url with a JSON content type,
// returning an error for any transport failure or non-2xx status. Shared by
// the webhook and Slack sinks; sink names the caller in error messages. The
// response body is drained+closed so the connection can be reused; on a
// non-2xx it is read (bounded) into the error for diagnosis.
//
// URL-leak wart (audit N-12): the URL here IS a credential — its path is
// the secret for webhook/Slack sinks (see diagnose/redact.go) — and both
// url.Parse (inside NewRequest) and client.Do wrap failures in *url.Error,
// whose Error() embeds the full URL. The alerter WARN-logs err.Error(), so
// every error below passes through [diagnose.SafeParseError] to strip the
// *url.Error wrapper, keeping the sink name + underlying cause only.
func postJSON(ctx context.Context, client *http.Client, sink, url string, v any) error {
	buf, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(buf))
	if err != nil {
		return fmt.Errorf("build %s request: %w", sink, diagnose.SafeParseError(err))
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("post to %s sink: %w", sink, diagnose.SafeParseError(err))
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		// Read a bounded slice of the body for the error message; a dead
		// sink shouldn't let us slurp an unbounded response.
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("non-2xx status %d: %s", resp.StatusCode, bytes.TrimSpace(snippet))
	}
	// Drain so the connection can be reused.
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
	return nil
}
