// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package notify

import (
	"context"
	"fmt"
	"net/http"
)

// SlackNotifier POSTs a Slack incoming-webhook message ({"text": "..."})
// composed from a [Notification]. It is a thin convenience over the generic
// webhook for the most-common operator sink; a user who wants richer Slack
// formatting (blocks, channels) can point a [WebhookNotifier] at their own
// relay instead.
type SlackNotifier struct {
	// WebhookURL is the Slack incoming-webhook URL. A credential (anyone
	// with it can post to the channel) — set via env, never logged.
	WebhookURL string

	// HTTPClient is optional; nil ⇒ a client with [defaultHTTPTimeout].
	HTTPClient *http.Client
}

// slackPayload is the minimal Slack incoming-webhook body.
type slackPayload struct {
	Text string `json:"text"`
}

// Notify composes a readable one-line Slack message and POSTs it. A non-2xx
// response is an error.
func (s *SlackNotifier) Notify(ctx context.Context, n Notification) error {
	return postJSON(ctx, s.client(), s.Name(), s.WebhookURL, slackPayload{Text: slackText(n)})
}

// Name implements [Notifier].
func (s *SlackNotifier) Name() string { return "slack" }

func (s *SlackNotifier) client() *http.Client {
	if s.HTTPClient != nil {
		return s.HTTPClient
	}
	return &http.Client{Timeout: defaultHTTPTimeout}
}

// slackText renders a Notification as a single readable line. The emoji
// encodes the level (rotating light for critical, warning sign otherwise).
//
// The body shape is category-dependent: a threshold alert renders the
// numeric "metric V ≥ T" reading, e.g. ":rotating_light: [sluice s1]
// storage_util 0.91 ≥ 0.90 — target storage approaching capacity"; an
// EVENT alert (schema-drift, ADR-0157; slot-health, ADR-0059) has no
// numeric reading, so it renders the Title as the headline and appends
// the Body detail, e.g. ":rotating_light: [sluice s1] Schema change
// stalled sync \"s1\" — <drift + recovery steps>". A threshold
// notification (empty or CategoryThreshold) renders exactly as before.
func slackText(n Notification) string {
	emoji := ":warning:"
	if n.Level == LevelCritical {
		emoji = ":rotating_light:"
	}
	if n.Category.IsEvent() {
		line := fmt.Sprintf("%s [sluice %s] %s", emoji, n.StreamID, n.Title)
		if n.Body != "" {
			line += " — " + n.Body
		}
		return line
	}
	line := fmt.Sprintf("%s [sluice %s] %s %.2f ≥ %.2f", emoji, n.StreamID, n.Metric, n.Value, n.Threshold)
	if n.Title != "" {
		line += " — " + n.Title
	}
	return line
}
