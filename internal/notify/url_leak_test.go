// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package notify

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// closedServerURL returns a URL on the loopback whose listener has
// already been closed, so any POST fails with an immediate transport
// (connection refused) error — the shape that pre-fix leaked the full
// URL through *url.Error into the alerter's WARN log.
func closedServerURL(t *testing.T) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	u := srv.URL
	srv.Close()
	return u
}

// TestNotify_TransportErrorNeverLeaksWebhookURL pins the audit N-12 fix:
// a webhook/Slack URL's PATH is the credential (diagnose/redact.go
// documents exactly that), and client.Do wraps every transport failure
// in a *url.Error whose Error() embeds the full URL. The alerter
// WARN-logs err.Error() verbatim, so the secret bytes must never appear
// in the returned error string — only the sink name + underlying cause.
func TestNotify_TransportErrorNeverLeaksWebhookURL(t *testing.T) {
	const secretPath = "/services/T000/B000/secret-hook-token-XYZ"
	base := closedServerURL(t)
	client := &http.Client{Timeout: 2 * time.Second}

	cases := []struct {
		name     string
		notify   func() error
		wantSink string
	}{
		{
			name: "webhook",
			notify: func() error {
				return (&WebhookNotifier{URL: base + secretPath, HTTPClient: client}).
					Notify(context.Background(), sampleNotification())
			},
			wantSink: "webhook",
		},
		{
			name: "slack",
			notify: func() error {
				return (&SlackNotifier{WebhookURL: base + secretPath, HTTPClient: client}).
					Notify(context.Background(), sampleNotification())
			},
			wantSink: "slack",
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			err := c.notify()
			if err == nil {
				t.Fatal("expected a transport error against a closed listener; got nil")
			}
			if strings.Contains(err.Error(), "secret-hook-token-XYZ") {
				t.Fatalf("error leaks the webhook secret path (the N-12 leak): %q", err.Error())
			}
			if !strings.Contains(err.Error(), "post to "+c.wantSink+" sink") {
				t.Errorf("error %q should name the %s sink for diagnosability", err.Error(), c.wantSink)
			}
		})
	}
}

// TestNotify_BuildRequestErrorNeverLeaksWebhookURL covers the sibling
// leak on the request-construction leg: an invalid URL makes
// http.NewRequestWithContext fail with a *url.Error that also embeds
// the raw URL (secret path included).
func TestNotify_BuildRequestErrorNeverLeaksWebhookURL(t *testing.T) {
	// The named port makes url.Parse fail while the secret rides the path.
	badURL := "http://localhost:namedport/secret-hook-token-XYZ"
	err := (&WebhookNotifier{URL: badURL}).Notify(context.Background(), sampleNotification())
	if err == nil {
		t.Fatal("expected a build-request error for an unparseable URL; got nil")
	}
	if strings.Contains(err.Error(), "secret-hook-token-XYZ") {
		t.Fatalf("build-request error leaks the webhook secret path: %q", err.Error())
	}
}
