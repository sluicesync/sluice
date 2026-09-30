// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// tokyoSkew is the age measured by the v0.156.5 regression cycle on a
// stream row a pre-v0.156.5 binary wrote under an Asia/Tokyo database zone.
const tokyoSkew = -32395 * time.Second

// TestEvaluateHealth_FutureDatedRowIsNotHealthy pins GC-40 (c) on `sync
// health`: a stream row dated past the skew tolerance must not read as
// healthy — with or without --max-stale-seconds, since a negative age is
// under every threshold — and must exit 1 naming the marker and the remedy.
func TestEvaluateHealth_FutureDatedRowIsNotHealthy(t *testing.T) {
	now := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)
	for _, threshold := range []int{0, 60} {
		streams := []ir.StreamStatus{{StreamID: "s", Position: ir.Position{Token: "x"}, UpdatedAt: now.Add(-tokyoSkew)}}
		r := evaluateHealth(streams, "s", threshold, now)
		if !r.TimestampInFuture || !r.Stale {
			t.Fatalf("threshold %d: TimestampInFuture=%v Stale=%v; want both true (stale is set so a .stale-only JSON consumer fails closed)", threshold, r.TimestampInFuture, r.Stale)
		}
		if r.SecondsSinceLastApply != -32395 {
			t.Errorf("threshold %d: seconds = %d; want the raw -32395", threshold, r.SecondsSinceLastApply)
		}
		var buf bytes.Buffer
		if err := renderHealth(&buf, r, "text"); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(buf.String(), "healthy") || !strings.Contains(buf.String(), "CONTROL-TIMESTAMP-IN-FUTURE") {
			t.Errorf("threshold %d: text must name the marker and not say healthy:\n%s", threshold, buf.String())
		}
		buf.Reset()
		if err := renderHealth(&buf, r, "json"); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(buf.String(), `"control_timestamp_in_future": true`) {
			t.Errorf("threshold %d: JSON lacks the flag:\n%s", threshold, buf.String())
		}
	}
	err := controlTimestampInFutureError{streamID: "s", secondsAgo: -32395}
	if err.ExitCode() != 1 || !strings.Contains(err.Error(), "CONTROL-TIMESTAMP-IN-FUTURE") || !strings.Contains(err.Error(), "sync start") {
		t.Errorf("error = %q (exit %d); want exit 1 naming the marker and the remedy", err.Error(), err.ExitCode())
	}

	// Skew inside the tolerance stays an ordinary, healthy reading.
	r := evaluateHealth([]ir.StreamStatus{{StreamID: "s", UpdatedAt: now.Add(30 * time.Second)}}, "s", 60, now)
	if r.TimestampInFuture || r.Stale {
		t.Errorf("30s of skew: %+v; want an ordinary healthy reading", r)
	}
}

// TestRenderStatus_FutureDatedRowIsFlagged pins GC-40 (c) on `sync status`
// (and the fleet view, which renders through the same function): the AGE
// cell carries the marker instead of "in the future", the remedy prints once
// under the table, and the JSON row carries control_timestamp_in_future while
// an ordinary row does not.
func TestRenderStatus_FutureDatedRowIsFlagged(t *testing.T) {
	streams := []ir.StreamStatus{
		makeStream("tokyo", tokyoSkew, "postgres", "lsn:0/1"),
		makeStream("fine", 10*time.Second, "postgres", "lsn:0/2"),
	}
	var buf bytes.Buffer
	if err := renderStatus(&buf, streams, nil, nil, nil, statusRenderOpts{Format: "text"}, fixedNow); err != nil {
		t.Fatal(err)
	}
	text := buf.String()
	for _, want := range []string{"CONTROL-TIMESTAMP-IN-FUTURE (+8h59m55s)", "1 stream marked CONTROL-TIMESTAMP-IN-FUTURE", "10s ago"} {
		if !strings.Contains(text, want) {
			t.Errorf("text lacks %q:\n%s", want, text)
		}
	}

	buf.Reset()
	if err := renderStatus(&buf, streams, nil, nil, nil, statusRenderOpts{Format: "json"}, fixedNow); err != nil {
		t.Fatal(err)
	}
	if got := strings.Count(buf.String(), `"control_timestamp_in_future": true`); got != 1 {
		t.Errorf("JSON flags %d rows; want exactly the future-dated one:\n%s", got, buf.String())
	}

	// GC-40 L3: the summary fails closed. The future-dated row is left out
	// of most-recent and makes oldest unknowable (the maximum, not a
	// negative), with the count beside it.
	var doc struct {
		Summary struct {
			Oldest   int64 `json:"oldest_seconds"`
			Newest   int64 `json:"newest_seconds"`
			InFuture int   `json:"control_timestamp_in_future_count"`
		} `json:"summary"`
	}
	if err := json.Unmarshal(buf.Bytes(), &doc); err != nil {
		t.Fatal(err)
	}
	if doc.Summary.InFuture != 1 || doc.Summary.Newest != 10 || doc.Summary.Oldest < 1<<32 {
		t.Errorf("summary = %+v; want count 1, newest 10 (the readable row), oldest the maximum", doc.Summary)
	}
	buf.Reset()
	if err := renderStatus(&buf, streams, nil, nil, nil, statusRenderOpts{Format: "text", Summary: true}, fixedNow); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "oldest=unknown (1 CONTROL-TIMESTAMP-IN-FUTURE), most-recent=10s ago") {
		t.Errorf("SUMMARY line does not fail closed:\n%s", buf.String())
	}
}

// TestEvaluateHealth_LegacyRowWindow documents GC-40's accepted RESIDUAL-1
// exactly. A row an older binary wrote on a database zone O east of UTC is
// stored O ahead, so d seconds after the write it ages d−O. `sync health`
// therefore reads it:
//
//   - UNKNOWN (exit 1) while d < O−60s — the row is more than the skew
//     tolerance in the future;
//   - HEALTHY (exit 0) for d in [O−60s, O+threshold] — the residual: a
//     stream stalled since before the upgrade reads fresh for this window,
//     because nothing in the row distinguishes it from a current write
//     (v0.156.5 added no per-row version stamp, and the table's legacy
//     DEFAULT is no evidence — every current write names updated_at);
//   - STALE (exit 1) once d > O+threshold.
//
// The first position write by a current binary heals the row; the window
// only matters for a stream that stalled before the upgrade.
func TestEvaluateHealth_LegacyRowWindow(t *testing.T) {
	const (
		offset    = 9 * time.Hour // Asia/Tokyo
		threshold = 60
	)
	written := time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC)
	stored := written.Add(offset)
	for _, c := range []struct {
		d        time.Duration
		exitCode int
	}{
		{offset - 61*time.Second, 1},
		{offset - 30*time.Second, 0}, // the residual
		{offset + (threshold+1)*time.Second, 1},
	} {
		r := evaluateHealth([]ir.StreamStatus{{StreamID: "s", UpdatedAt: stored}}, "s", threshold, written.Add(c.d))
		got := 0
		if r.Stale || r.TimestampInFuture {
			got = 1
		}
		if got != c.exitCode {
			t.Errorf("d = O%+v: exit %d (%+v); want %d", c.d-offset, got, r, c.exitCode)
		}
	}
}
