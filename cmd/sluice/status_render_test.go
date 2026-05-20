// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/orware/sluice/internal/ir"
)

// fixedNow is the test reference time. Using a literal lets the
// human-age assertions stay deterministic (the rendered "5m ago" / "2h
// ago" strings depend on now-updated_at, so a static now is the only
// way to pin them without parsing back the output).
var fixedNow = time.Date(2026, 5, 20, 22, 0, 0, 0, time.UTC)

func makeStream(id string, updatedAgo time.Duration, posEngine, posToken string) ir.StreamStatus {
	return ir.StreamStatus{
		StreamID: id,
		Position: ir.Position{
			Engine: posEngine,
			Token:  posToken,
		},
		UpdatedAt: fixedNow.Add(-updatedAgo),
	}
}

// TestRenderStatusText_DefaultShape covers the pre-refactor text
// output: STREAM/UPDATED/AGE/POSITION header + one row per stream,
// no summary header by default. Existing scripts that parse this
// output must keep working.
func TestRenderStatusText_DefaultShape(t *testing.T) {
	var buf bytes.Buffer
	streams := []ir.StreamStatus{
		makeStream("alpha", 10*time.Second, "mysql", "binlog:mysql-bin.000003:1024"),
		makeStream("beta", 5*time.Minute, "postgres", "lsn:0/1A2B3C4D"),
	}
	err := renderStatus(&buf, streams, statusRenderOpts{Format: "text"}, fixedNow)
	if err != nil {
		t.Fatalf("renderStatus: %v", err)
	}
	got := buf.String()
	for _, want := range []string{
		"STREAM",
		"UPDATED",
		"AGE",
		"POSITION",
		"alpha",
		"beta",
		"10s ago",
		"5m ago",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("text output missing %q\n--- got ---\n%s", want, got)
		}
	}
	// Default shape must NOT have a SUMMARY: prefix.
	if strings.Contains(got, "SUMMARY:") {
		t.Errorf("default text output unexpectedly contains SUMMARY header:\n%s", got)
	}
}

// TestRenderStatusText_MostRecentFirst pins the row ordering: streams
// are sorted by UpdatedAt descending (most recent first) regardless
// of input order. Operators rely on this for "what's been moving?"
// at a glance.
func TestRenderStatusText_MostRecentFirst(t *testing.T) {
	var buf bytes.Buffer
	streams := []ir.StreamStatus{
		makeStream("old", 1*time.Hour, "mysql", "x"),
		makeStream("new", 1*time.Second, "mysql", "y"),
		makeStream("mid", 5*time.Minute, "mysql", "z"),
	}
	if err := renderStatus(&buf, streams, statusRenderOpts{Format: "text"}, fixedNow); err != nil {
		t.Fatalf("renderStatus: %v", err)
	}
	got := buf.String()
	newIdx := strings.Index(got, "new")
	midIdx := strings.Index(got, "mid")
	oldIdx := strings.Index(got, "old")
	if newIdx >= midIdx || midIdx >= oldIdx {
		t.Errorf("expected order new<mid<old; got idx new=%d mid=%d old=%d\n%s",
			newIdx, midIdx, oldIdx, got)
	}
}

// TestRenderStatusText_Summary pins the --summary header shape and
// that the count + oldest/most-recent ages reflect the data.
func TestRenderStatusText_Summary(t *testing.T) {
	var buf bytes.Buffer
	streams := []ir.StreamStatus{
		makeStream("a", 1*time.Second, "mysql", "x"),
		makeStream("b", 30*time.Minute, "mysql", "y"),
	}
	if err := renderStatus(&buf, streams, statusRenderOpts{Format: "text", Summary: true}, fixedNow); err != nil {
		t.Fatalf("renderStatus: %v", err)
	}
	got := buf.String()
	for _, want := range []string{
		"SUMMARY: 2 streams",
		"oldest=30m ago",
		"most-recent=1s ago",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("summary missing %q\n--- got ---\n%s", want, got)
		}
	}
}

// TestRenderStatusText_SummarySingularPlural pins that "1 stream"
// (no plural s) is rendered correctly. A pedantic copy nit but
// operators do notice "1 streams" in a dashboard.
func TestRenderStatusText_SummarySingularPlural(t *testing.T) {
	var buf bytes.Buffer
	streams := []ir.StreamStatus{makeStream("solo", 5*time.Second, "mysql", "x")}
	if err := renderStatus(&buf, streams, statusRenderOpts{Format: "text", Summary: true}, fixedNow); err != nil {
		t.Fatalf("renderStatus: %v", err)
	}
	got := buf.String()
	if !strings.Contains(got, "SUMMARY: 1 stream,") {
		t.Errorf("expected singular 'SUMMARY: 1 stream,'; got:\n%s", got)
	}
	if strings.Contains(got, "1 streams") {
		t.Errorf("unexpected plural 'streams' on count=1:\n%s", got)
	}
}

// TestRenderStatusText_Empty pins the empty-result messaging both
// with and without a --stream-id filter. Existing scripts may grep
// these strings; keep them stable.
func TestRenderStatusText_Empty(t *testing.T) {
	t.Run("no filter", func(t *testing.T) {
		var buf bytes.Buffer
		if err := renderStatus(&buf, nil, statusRenderOpts{Format: "text"}, fixedNow); err != nil {
			t.Fatalf("renderStatus: %v", err)
		}
		if got := buf.String(); !strings.Contains(got, "no streams recorded on target") {
			t.Errorf("expected 'no streams recorded on target'; got %q", got)
		}
	})
	t.Run("with stream-id filter", func(t *testing.T) {
		var buf bytes.Buffer
		opts := statusRenderOpts{Format: "text", StreamID: "missing-stream"}
		if err := renderStatus(&buf, nil, opts, fixedNow); err != nil {
			t.Fatalf("renderStatus: %v", err)
		}
		if got := buf.String(); !strings.Contains(got, `no stream "missing-stream" on target`) {
			t.Errorf("expected 'no stream \"missing-stream\" on target'; got %q", got)
		}
	})
}

// TestRenderStatusJSON_Shape pins the JSON document structure: top-
// level keys (generated_at, summary, streams) plus the per-stream
// field set including age_seconds. Parses with the stdlib decoder
// (no string contains hacks) so the assertion fails clearly on
// shape regressions.
func TestRenderStatusJSON_Shape(t *testing.T) {
	var buf bytes.Buffer
	streams := []ir.StreamStatus{
		makeStream("alpha", 10*time.Second, "mysql", "binlog:mysql-bin.000003:1024"),
		makeStream("beta", 5*time.Minute, "postgres", "lsn:0/1A2B3C4D"),
	}
	if err := renderStatus(&buf, streams, statusRenderOpts{Format: "json"}, fixedNow); err != nil {
		t.Fatalf("renderStatus: %v", err)
	}

	var doc struct {
		GeneratedAt time.Time `json:"generated_at"`
		Summary     struct {
			Count         int   `json:"count"`
			OldestSeconds int64 `json:"oldest_seconds"`
			NewestSeconds int64 `json:"newest_seconds"`
		} `json:"summary"`
		Streams []struct {
			StreamID string `json:"stream_id"`
			Position struct {
				Engine string `json:"engine"`
				Token  string `json:"token"`
			} `json:"position"`
			UpdatedAt  time.Time `json:"updated_at"`
			AgeSeconds int64     `json:"age_seconds"`
		} `json:"streams"`
	}
	if err := json.Unmarshal(buf.Bytes(), &doc); err != nil {
		t.Fatalf("decode JSON: %v\n--- raw ---\n%s", err, buf.String())
	}

	if !doc.GeneratedAt.Equal(fixedNow) {
		t.Errorf("generated_at = %v; want %v", doc.GeneratedAt, fixedNow)
	}
	if doc.Summary.Count != 2 {
		t.Errorf("summary.count = %d; want 2", doc.Summary.Count)
	}
	if doc.Summary.OldestSeconds != 300 {
		t.Errorf("summary.oldest_seconds = %d; want 300 (5min)", doc.Summary.OldestSeconds)
	}
	if doc.Summary.NewestSeconds != 10 {
		t.Errorf("summary.newest_seconds = %d; want 10", doc.Summary.NewestSeconds)
	}
	if len(doc.Streams) != 2 {
		t.Fatalf("len(streams) = %d; want 2", len(doc.Streams))
	}
	// Sort order: most-recently-updated first; alpha (10s) before beta (5m).
	if doc.Streams[0].StreamID != "alpha" {
		t.Errorf("streams[0].stream_id = %q; want alpha (most recent)", doc.Streams[0].StreamID)
	}
	if doc.Streams[0].AgeSeconds != 10 {
		t.Errorf("streams[0].age_seconds = %d; want 10", doc.Streams[0].AgeSeconds)
	}
	if doc.Streams[0].Position.Engine != "mysql" {
		t.Errorf("streams[0].position.engine = %q; want mysql", doc.Streams[0].Position.Engine)
	}
	if doc.Streams[1].StreamID != "beta" {
		t.Errorf("streams[1].stream_id = %q; want beta", doc.Streams[1].StreamID)
	}
}

// TestRenderStatusJSON_Empty pins the empty-streams JSON shape: the
// document still has generated_at + summary{count:0} + streams:[].
// A consumer parsing this must not need a special "no rows" path.
func TestRenderStatusJSON_Empty(t *testing.T) {
	var buf bytes.Buffer
	if err := renderStatus(&buf, nil, statusRenderOpts{Format: "json"}, fixedNow); err != nil {
		t.Fatalf("renderStatus: %v", err)
	}
	var doc struct {
		Summary struct {
			Count int `json:"count"`
		} `json:"summary"`
		Streams []any `json:"streams"`
	}
	if err := json.Unmarshal(buf.Bytes(), &doc); err != nil {
		t.Fatalf("decode JSON: %v\n--- raw ---\n%s", err, buf.String())
	}
	if doc.Summary.Count != 0 {
		t.Errorf("summary.count = %d; want 0", doc.Summary.Count)
	}
	if doc.Streams == nil {
		// json.Unmarshal of `"streams": []` into []any yields a non-nil
		// empty slice; nil would indicate the key was omitted, which
		// would break consumers expecting an array.
		t.Error("streams field decoded as nil; expected empty slice")
	}
}

// TestRenderStatus_UnknownFormat pins the explicit error for an
// unrecognised --format value (defence-in-depth alongside kong's
// enum: validation, which should reject it earlier).
func TestRenderStatus_UnknownFormat(t *testing.T) {
	var buf bytes.Buffer
	err := renderStatus(&buf, nil, statusRenderOpts{Format: "yaml"}, fixedNow)
	if err == nil {
		t.Fatal("expected error for unknown format; got nil")
	}
	if !strings.Contains(err.Error(), "unknown --format") {
		t.Errorf("expected 'unknown --format' in error; got %v", err)
	}
}

// TestFilterStreams_Identity pins that an empty filter returns the
// input unchanged (no copy).
func TestFilterStreams_Identity(t *testing.T) {
	in := []ir.StreamStatus{makeStream("a", 0, "mysql", "x"), makeStream("b", 0, "mysql", "y")}
	got := filterStreams(in, "")
	if len(got) != 2 {
		t.Errorf("len(got) = %d; want 2", len(got))
	}
}

// TestFilterStreams_Match pins exact-match filtering.
func TestFilterStreams_Match(t *testing.T) {
	in := []ir.StreamStatus{
		makeStream("alpha", 0, "mysql", "x"),
		makeStream("beta", 0, "mysql", "y"),
		makeStream("gamma", 0, "mysql", "z"),
	}
	got := filterStreams(in, "beta")
	if len(got) != 1 || got[0].StreamID != "beta" {
		t.Errorf("got %d streams, first = %q; want 1 stream 'beta'",
			len(got), func() string {
				if len(got) > 0 {
					return got[0].StreamID
				}
				return ""
			}())
	}
}

// TestAgesSpan_OldestVsNewest pins that the helper finds the
// extremes regardless of input order.
func TestAgesSpan_OldestVsNewest(t *testing.T) {
	streams := []ir.StreamStatus{
		makeStream("a", 5*time.Minute, "mysql", "x"),
		makeStream("b", 1*time.Second, "mysql", "y"),
		makeStream("c", 1*time.Hour, "mysql", "z"),
	}
	oldest, newest := agesSpan(streams, fixedNow)
	if oldest != 1*time.Hour {
		t.Errorf("oldest = %v; want 1h", oldest)
	}
	if newest != 1*time.Second {
		t.Errorf("newest = %v; want 1s", newest)
	}
}
