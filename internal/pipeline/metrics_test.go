// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/orware/sluice/internal/ir"
)

// TestEmitMetrics_Empty pins the no-streams shape: each metric block's
// HELP/TYPE comments still emit so a Prometheus scraper sees the
// known shape, just with zero stream-keyed lines.
func TestEmitMetrics_Empty(t *testing.T) {
	var buf bytes.Buffer
	now := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)
	emitMetrics(&buf, nil, now)
	out := buf.String()
	for _, want := range []string{
		"# HELP sluice_seconds_since_last_apply",
		"# TYPE sluice_seconds_since_last_apply gauge",
		"# HELP sluice_stream_known",
		"# TYPE sluice_stream_known gauge",
		"sluice_metrics_scrape_unix_seconds",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("expected %q in output; got:\n%s", want, out)
		}
	}
}

// TestEmitMetrics_TwoStreams pins the multi-stream shape — one line
// per metric per stream, distinguished by stream_id label.
func TestEmitMetrics_TwoStreams(t *testing.T) {
	now := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)
	streams := []ir.StreamStatus{
		{StreamID: "myapp-prod", UpdatedAt: now.Add(-30 * time.Second)},
		{StreamID: "analytics-stream", UpdatedAt: now.Add(-3600 * time.Second)},
	}
	var buf bytes.Buffer
	emitMetrics(&buf, streams, now)
	out := buf.String()
	for _, want := range []string{
		`sluice_seconds_since_last_apply{stream_id="myapp-prod"} 30`,
		`sluice_seconds_since_last_apply{stream_id="analytics-stream"} 3600`,
		`sluice_stream_known{stream_id="myapp-prod"} 1`,
		`sluice_stream_known{stream_id="analytics-stream"} 1`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("expected %q in output; got:\n%s", want, out)
		}
	}
}

// TestEmitMetrics_ScrapeTimestamp pins the scrape-time gauge — useful
// for scraper-side staleness detection (Prometheus alerting can
// detect "scrape happened but the target's clock disagrees" via this
// vs. the scrape's own timestamp).
func TestEmitMetrics_ScrapeTimestamp(t *testing.T) {
	now := time.Date(2026, 5, 7, 12, 30, 45, 0, time.UTC)
	var buf bytes.Buffer
	emitMetrics(&buf, nil, now)
	want := "sluice_metrics_scrape_unix_seconds " // followed by a number
	if !strings.Contains(buf.String(), want) {
		t.Errorf("expected scrape-time gauge in output; got:\n%s", buf.String())
	}
}

// TestEmitMetrics_StreamIDQuoting pins the label-value quoting shape
// — stream IDs that include underscores, hyphens, or numbers should
// round-trip without escape mangling.
func TestEmitMetrics_StreamIDQuoting(t *testing.T) {
	streams := []ir.StreamStatus{
		{StreamID: "my-app_prod-2026", UpdatedAt: time.Now()},
	}
	var buf bytes.Buffer
	emitMetrics(&buf, streams, time.Now())
	if !strings.Contains(buf.String(), `stream_id="my-app_prod-2026"`) {
		t.Errorf("stream_id label should round-trip verbatim; got:\n%s", buf.String())
	}
}
