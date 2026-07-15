// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package notify

// Rendering pins for [CategorySlotHealth] (ADR-0059 implementation note,
// roadmap item 64a) across EVERY sink — slack, webhook, email — the
// category-dispatch counterpart of category_test.go's schema-drift pins
// (the Bug 74 rule: the sinks dispatch on the category family, so each
// new category is pinned per sink, not per representative).

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// slotHealthNotification is the roadmap-64a event-shaped alert the pipeline
// builds from a slot-health threshold crossing: critical (the 85% class),
// CategorySlotHealth, a title + a body carrying the slot facts and the
// remediation, no numeric reading.
func slotHealthNotification() Notification {
	return Notification{
		Level:    LevelCritical,
		Category: CategorySlotHealth,
		StreamID: "s1",
		Title:    `Replication slot nearing eviction on sync "s1" — WAL retention at 92.5%`,
		Body: `slot "sluice_slot" is holding back 925 bytes of WAL (92.5% of max_slot_wal_keep_size); ` +
			`intervene now: confirm the consumer is alive ... [max_slot_wal_keep_size=1000 bytes, wal_status="extended", lag=925 bytes]`,
		At: time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC),
	}
}

// TestCategory_SlotHealthContract pins the helper contract: only the
// explicit slot-health value takes the slot-health rendering, the zero
// value stays threshold-shaped, and IsEvent covers exactly the two
// event categories.
func TestCategory_SlotHealthContract(t *testing.T) {
	if Category("").IsSlotHealth() || Category("").IsEvent() {
		t.Error("empty Category must be treated as threshold, not slot-health/event")
	}
	if CategoryThreshold.IsSlotHealth() || CategoryThreshold.IsEvent() {
		t.Error("CategoryThreshold must not be slot-health/event")
	}
	if !CategorySlotHealth.IsSlotHealth() || !CategorySlotHealth.IsEvent() {
		t.Error("CategorySlotHealth must be slot-health AND event-shaped")
	}
	if !CategorySchemaDrift.IsEvent() {
		t.Error("CategorySchemaDrift must remain event-shaped")
	}
	if CategorySlotHealth.IsSchemaDrift() {
		t.Error("CategorySlotHealth must not render as schema-drift")
	}
}

// TestSlackText_SlotHealth pins the slack rendering: Title as the headline,
// the Body facts appended, NO "V ≥ T" numeric line, and the level emoji
// per severity (critical vs the warning classes).
func TestSlackText_SlotHealth(t *testing.T) {
	n := slotHealthNotification()
	got := slackText(n)
	if !strings.HasPrefix(got, ":rotating_light: [sluice s1] ") {
		t.Errorf("critical slot-health slack should keep the emoji + stream prefix: %q", got)
	}
	for _, want := range []string{n.Title, `slot "sluice_slot"`, "max_slot_wal_keep_size"} {
		if !strings.Contains(got, want) {
			t.Errorf("slot-health slack %q missing %q", got, want)
		}
	}
	if strings.Contains(got, "≥") {
		t.Errorf("slot-health slack must NOT render the numeric ≥ line: %q", got)
	}

	// The 70%/inactivity classes are warnings → warning emoji.
	n.Level = LevelWarning
	if got := slackText(n); !strings.HasPrefix(got, ":warning: ") {
		t.Errorf("warning-level slot-health slack should use the warning emoji: %q", got)
	}
}

// TestWebhookPayload_SlotHealth pins that a slot-health notification
// carries category="slot-health" and the title/body, so a consumer keys
// off the discriminator rather than the (zero) value/threshold fields.
func TestWebhookPayload_SlotHealth(t *testing.T) {
	var got webhookPayload
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Errorf("decode: %v", err)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	n := slotHealthNotification()
	if err := (&WebhookNotifier{URL: srv.URL}).Notify(context.Background(), n); err != nil {
		t.Fatalf("Notify: %v", err)
	}
	if got.Category != "slot-health" {
		t.Errorf("slot-health webhook category = %q; want slot-health", got.Category)
	}
	if got.Title != n.Title || got.Body != n.Body {
		t.Errorf("slot-health webhook title/body mismatch: %+v", got)
	}
	if got.Value != 0 || got.Threshold != 0 {
		t.Errorf("slot-health webhook value/threshold should be 0, got %v/%v", got.Value, got.Threshold)
	}
}

// TestEmailSubject_SlotHealth pins the distinct slot-health subject (no
// metric, names the slot-health class) while the threshold and
// schema-drift subjects stay untouched by the new branch.
func TestEmailSubject_SlotHealth(t *testing.T) {
	if got, want := emailSubject(slotHealthNotification()), "[sluice] replication slot health alert: s1"; got != want {
		t.Errorf("slot-health subject = %q; want %q", got, want)
	}
	n := slotHealthNotification()
	n.StreamID = ""
	if got, want := emailSubject(n), "[sluice] replication slot health alert"; got != want {
		t.Errorf("slot-health subject (no stream) = %q; want %q", got, want)
	}
	// Threshold subject byte-identical (guards the branch didn't leak).
	if got, want := emailSubject(sampleNotification()), "[sluice] storage_util threshold alert: s1"; got != want {
		t.Errorf("threshold subject changed = %q; want %q", got, want)
	}
}

// TestRenderEmail_SlotHealth pins the event-shaped email: the Title + Body
// (with the remediation) appear in both parts, WITHOUT the "V ≥ T
// (threshold)" line and without the metric facts.
func TestRenderEmail_SlotHealth(t *testing.T) {
	n := slotHealthNotification()
	subject, html, text, err := renderEmail(n)
	if err != nil {
		t.Fatalf("renderEmail: %v", err)
	}
	if !strings.Contains(subject, "replication slot health") {
		t.Errorf("subject %q not slot-health-shaped", subject)
	}
	for _, part := range []struct{ name, body string }{{"html", html}, {"text", text}} {
		for _, want := range []string{"replication slot", "sluice_slot", "max_slot_wal_keep_size", "CRITICAL", "s1"} {
			if !strings.Contains(part.body, want) {
				t.Errorf("%s slot-health body missing %q", part.name, want)
			}
		}
		// No numeric-threshold and no schema-drift rendering leaks in.
		if strings.Contains(part.body, "(threshold)") || strings.Contains(part.body, "threshold was breached") {
			t.Errorf("%s slot-health body must not carry the threshold rendering", part.name)
		}
		if strings.Contains(part.body, "schema change") {
			t.Errorf("%s slot-health body must not carry the schema-drift rendering", part.name)
		}
	}
	if !strings.Contains(html, "<!DOCTYPE html>") {
		t.Error("slot-health html body should be an HTML document")
	}
	if strings.Contains(text, "<html") {
		t.Error("slot-health text body should not contain HTML tags")
	}
}
