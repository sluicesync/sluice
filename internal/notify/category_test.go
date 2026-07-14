// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package notify

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// schemaDriftNotification is the ADR-0157 event-shaped alert the pipeline
// builds from a schema-forward refusal: critical, CategorySchemaDrift, a
// title + a body carrying the drift detail and the recovery hint, no
// numeric reading.
func schemaDriftNotification() Notification {
	return Notification{
		Level:    LevelCritical,
		Category: CategorySchemaDrift,
		StreamID: "s1",
		Title:    `Schema change stalled sync "s1" — manual recovery needed`,
		Body: "RENAME COLUMN \"a\"→\"b\" on \"orders\" cannot be auto-forwarded ... " +
			"recovery: drained model — run 'sluice sync stop --wait', then run schema migrate.",
		At: time.Date(2026, 6, 22, 12, 0, 0, 0, time.UTC),
	}
}

// TestCategory_ZeroValueIsThreshold pins the "empty ⇒ threshold" contract:
// the zero Category (what every metrics notification carries) is NOT
// schema-drift, so it takes the numeric rendering in every sink.
func TestCategory_ZeroValueIsThreshold(t *testing.T) {
	if Category("").IsSchemaDrift() {
		t.Error("empty Category must be treated as threshold, not schema-drift")
	}
	if CategoryThreshold.IsSchemaDrift() {
		t.Error("CategoryThreshold must not be schema-drift")
	}
	if !CategorySchemaDrift.IsSchemaDrift() {
		t.Error("CategorySchemaDrift.IsSchemaDrift() must be true")
	}
}

// --- Slack ---

// TestSlackText_ThresholdByteIdentical pins that a threshold notification
// renders EXACTLY as before the ADR-0157 Category split (byte-for-byte).
func TestSlackText_ThresholdByteIdentical(t *testing.T) {
	const want = ":rotating_light: [sluice s1] storage_util 0.91 ≥ 0.90 — target storage approaching capacity"
	if got := slackText(sampleNotification()); got != want {
		t.Errorf("threshold slack text changed:\n got %q\nwant %q", got, want)
	}
	// Empty Category (the metrics path never sets it) must render identically.
	n := sampleNotification()
	n.Category = ""
	if got := slackText(n); got != want {
		t.Errorf("empty-category slack text = %q; want %q", got, want)
	}
}

// TestSlackText_SchemaDrift pins the event rendering: Title as the headline,
// the Body detail appended, and NO "V ≥ T" numeric line.
func TestSlackText_SchemaDrift(t *testing.T) {
	n := schemaDriftNotification()
	got := slackText(n)
	if !strings.HasPrefix(got, ":rotating_light: [sluice s1] ") {
		t.Errorf("schema-drift slack should keep the emoji + stream prefix: %q", got)
	}
	for _, want := range []string{n.Title, "recovery:", "sluice sync stop --wait"} {
		if !strings.Contains(got, want) {
			t.Errorf("schema-drift slack %q missing %q", got, want)
		}
	}
	if strings.Contains(got, "≥") {
		t.Errorf("schema-drift slack must NOT render the numeric ≥ line: %q", got)
	}
}

// --- Webhook ---

// TestWebhookPayload_ThresholdByteIdentical pins that a threshold
// notification's JSON body is byte-identical to before: the new `category`
// field is omitempty, so it never appears for a threshold alert.
func TestWebhookPayload_ThresholdByteIdentical(t *testing.T) {
	var raw map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
			t.Errorf("decode: %v", err)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	if err := (&WebhookNotifier{URL: srv.URL}).Notify(context.Background(), sampleNotification()); err != nil {
		t.Fatalf("Notify: %v", err)
	}
	if _, present := raw["category"]; present {
		t.Errorf("threshold webhook payload must NOT carry a category key (omitempty): %v", raw)
	}
	if raw["value"] != 0.91 || raw["threshold"] != 0.90 {
		t.Errorf("threshold webhook value/threshold changed: %v", raw)
	}
}

// TestWebhookPayload_SchemaDrift pins that a schema-drift notification
// carries category="schema-drift" and the title/body, so a consumer keys
// off the discriminator rather than the (zero) value/threshold fields.
func TestWebhookPayload_SchemaDrift(t *testing.T) {
	var got webhookPayload
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Errorf("decode: %v", err)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	n := schemaDriftNotification()
	if err := (&WebhookNotifier{URL: srv.URL}).Notify(context.Background(), n); err != nil {
		t.Fatalf("Notify: %v", err)
	}
	if got.Category != "schema-drift" {
		t.Errorf("schema-drift webhook category = %q; want schema-drift", got.Category)
	}
	if got.Title != n.Title || got.Body != n.Body {
		t.Errorf("schema-drift webhook title/body mismatch: %+v", got)
	}
	if got.Value != 0 || got.Threshold != 0 {
		t.Errorf("schema-drift webhook value/threshold should be 0, got %v/%v", got.Value, got.Threshold)
	}
}

// --- SMTP / email ---

// TestEmailSubject_SchemaDrift pins the distinct schema-drift subject (no
// metric, names the stall) while the threshold subject stays unchanged.
func TestEmailSubject_SchemaDrift(t *testing.T) {
	if got, want := emailSubject(schemaDriftNotification()), "[sluice] schema change stalled sync: s1"; got != want {
		t.Errorf("schema-drift subject = %q; want %q", got, want)
	}
	// Threshold subject byte-identical (guards the branch didn't leak).
	if got, want := emailSubject(sampleNotification()), "[sluice] storage_util threshold alert: s1"; got != want {
		t.Errorf("threshold subject changed = %q; want %q", got, want)
	}
	n := schemaDriftNotification()
	n.StreamID = ""
	if got, want := emailSubject(n), "[sluice] schema change stalled sync"; got != want {
		t.Errorf("schema-drift subject (no stream) = %q; want %q", got, want)
	}
}

// TestRenderEmail_SchemaDrift pins the event-shaped email: the Title + Body
// (with the recovery hint) appear in both parts, WITHOUT the "V ≥ T
// (threshold)" line and without the metric facts.
func TestRenderEmail_SchemaDrift(t *testing.T) {
	n := schemaDriftNotification()
	subject, html, text, err := renderEmail(n)
	if err != nil {
		t.Fatalf("renderEmail: %v", err)
	}
	if !strings.Contains(subject, "schema change stalled sync") {
		t.Errorf("subject %q not schema-drift-shaped", subject)
	}
	for _, part := range []struct{ name, body string }{{"html", html}, {"text", text}} {
		for _, want := range []string{"stalled sync", "recovery:", "sluice sync stop --wait", "CRITICAL", "s1"} {
			if !strings.Contains(part.body, want) {
				t.Errorf("%s schema-drift body missing %q", part.name, want)
			}
		}
		// No numeric-threshold rendering leaks into the event body.
		if strings.Contains(part.body, "(threshold)") || strings.Contains(part.body, "threshold was breached") {
			t.Errorf("%s schema-drift body must not carry the threshold rendering", part.name)
		}
	}
	if !strings.Contains(html, "<!DOCTYPE html>") {
		t.Error("schema-drift html body should be an HTML document")
	}
	if strings.Contains(text, "<html") {
		t.Error("schema-drift text body should not contain HTML tags")
	}
}

// TestRenderEmail_ThresholdUnchanged is the byte-identical pin for the SMTP
// threshold branch: the untouched templates must still render the numeric
// line + metric facts exactly as before.
func TestRenderEmail_ThresholdUnchanged(t *testing.T) {
	_, html, text, err := renderEmail(sampleNotification())
	if err != nil {
		t.Fatalf("renderEmail: %v", err)
	}
	for _, part := range []struct{ name, body string }{{"html", html}, {"text", text}} {
		for _, want := range []string{"storage_util", "0.91", "threshold", "approaching capacity"} {
			if !strings.Contains(part.body, want) {
				t.Errorf("%s threshold body missing %q (byte-identical regression?)", part.name, want)
			}
		}
	}
	// The text template's numeric line survives.
	if !strings.Contains(text, "0.91 >= 0.9 (threshold)") {
		t.Errorf("threshold text numeric line changed: %q", text)
	}
}
