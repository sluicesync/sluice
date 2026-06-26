// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package notify

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestEmailSubject(t *testing.T) {
	n := sampleNotification()
	if got, want := emailSubject(n), "[sluice] storage_util threshold alert: s1"; got != want {
		t.Errorf("subject = %q, want %q", got, want)
	}
	// No stream id ⇒ no dangling colon.
	n.StreamID = ""
	if got, want := emailSubject(n), "[sluice] storage_util threshold alert"; got != want {
		t.Errorf("subject (no stream) = %q, want %q", got, want)
	}
}

func TestRenderEmail_BothBodiesCarryFacts(t *testing.T) {
	n := sampleNotification()
	subject, html, text, err := renderEmail(n)
	if err != nil {
		t.Fatalf("renderEmail: %v", err)
	}
	if !strings.Contains(subject, "storage_util threshold alert: s1") {
		t.Errorf("subject %q missing the metric/stream", subject)
	}
	// Both bodies must carry the same facts (a text-only client loses nothing).
	for _, part := range []struct{ name, body string }{{"html", html}, {"text", text}} {
		for _, want := range []string{"storage_util", "0.91", "0.9", "s1", "approaching capacity", "CRITICAL", "2026-06-22T12:00:00Z"} {
			if !strings.Contains(part.body, want) {
				t.Errorf("%s body missing %q", part.name, want)
			}
		}
	}
	// HTML must be HTML; text must not be.
	if !strings.Contains(html, "<!DOCTYPE html>") {
		t.Error("html body should be an HTML document")
	}
	if strings.Contains(text, "<html") {
		t.Error("text body should not contain HTML tags")
	}
}

func TestRenderEmail_LevelAccent(t *testing.T) {
	crit := sampleNotification()
	_, html, _, err := renderEmail(crit)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(html, emailAccentCritical) {
		t.Errorf("critical alert should use the critical accent %s", emailAccentCritical)
	}
	warn := sampleNotification()
	warn.Level = LevelWarning
	_, whtml, _, err := renderEmail(warn)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(whtml, emailAccentWarning) {
		t.Errorf("warning alert should use the warning accent %s", emailAccentWarning)
	}
	if strings.Contains(whtml, emailAccentCritical) {
		t.Error("warning alert should not use the critical accent")
	}
}

// TestRenderEmail_HTMLEscapes confirms html/template auto-escapes hostile
// content (a title with markup) — defence in depth even though alert content
// is sluice-internal.
func TestRenderEmail_HTMLEscapes(t *testing.T) {
	n := sampleNotification()
	n.Title = "<script>alert(1)</script>"
	_, html, _, err := renderEmail(n)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(html, "<script>alert(1)</script>") {
		t.Error("html template must escape markup in the title")
	}
	if !strings.Contains(html, "&lt;script&gt;") {
		t.Error("expected the escaped form of the title")
	}
}

func TestFormatMetricNumber(t *testing.T) {
	cases := map[float64]string{
		0.91:    "0.91",
		0.9:     "0.9",
		120:     "120",
		3600.5:  "3600", // 4 significant figures
		0.00012: "0.00012",
	}
	for in, want := range cases {
		if got := formatMetricNumber(in); got != want {
			t.Errorf("formatMetricNumber(%v) = %q, want %q", in, got, want)
		}
	}
}

// sampleAlerts are the representative alerts the preview generator renders.
// Exported via the generator below so the committed HTML/TXT samples cover the
// distinct shapes an operator will actually receive.
func sampleAlerts() []struct {
	slug string
	n    Notification
} {
	at := time.Date(2026, 6, 22, 12, 0, 0, 0, time.UTC)
	return []struct {
		slug string
		n    Notification
	}{
		{"storage-util", Notification{
			Level: LevelCritical, StreamID: "prod-orders", Metric: "storage_util",
			Title: "target storage approaching capacity",
			Body:  "storage_util 0.91 ≥ 0.9 (target storage approaching capacity)",
			Value: 0.91, Threshold: 0.9, At: at,
		}},
		{"sync-lag", Notification{
			Level: LevelCritical, StreamID: "prod-orders", Metric: "sync_lag_seconds",
			Title: "sync lag high (target falling behind source)",
			Body:  "sync_lag_seconds 312 ≥ 120 (sync lag high (target falling behind source))",
			Value: 312, Threshold: 120, At: at,
		}},
		{"cpu-util", Notification{
			Level: LevelWarning, StreamID: "analytics-replica", Metric: "cpu_util",
			Title: "target CPU saturating",
			Body:  "cpu_util 0.88 ≥ 0.85 (target CPU saturating)",
			Value: 0.88, Threshold: 0.85, At: at,
		}},
	}
}

// TestRenderEmailSamples is the committed-sample preview generator. It always
// renders (so it contributes coverage under plain `go test ./...`), but only
// WRITES the .html/.txt files when SLUICE_WRITE_EMAIL_SAMPLES is set — so a
// normal test run never dirties the tree. To regenerate the committed
// previews:
//
//	SLUICE_WRITE_EMAIL_SAMPLES=docs/operator/email-samples \
//	  go test -run TestRenderEmailSamples ./internal/notify/...
//
// (the path is relative to the repo root; the test resolves it from this
// package directory two levels up).
func TestRenderEmailSamples(t *testing.T) {
	outDir := os.Getenv("SLUICE_WRITE_EMAIL_SAMPLES")
	for _, s := range sampleAlerts() {
		subject, html, text, err := renderEmail(s.n)
		if err != nil {
			t.Fatalf("render %s: %v", s.slug, err)
		}
		if subject == "" || html == "" || text == "" {
			t.Fatalf("render %s: empty output", s.slug)
		}
		if outDir == "" {
			continue
		}
		dir := outDir
		if !filepath.IsAbs(dir) {
			dir = filepath.Join("..", "..", dir)
		}
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", dir, err)
		}
		writeSample(t, filepath.Join(dir, s.slug+".html"), html)
		writeSample(t, filepath.Join(dir, s.slug+".txt"), "Subject: "+subject+"\n\n"+text)
	}
}

func writeSample(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil { //nolint:gosec // operator-readable preview files, not secrets
		t.Fatalf("write %s: %v", path, err)
	}
}
