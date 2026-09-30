// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
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
		if !r.TimestampInFuture || r.Stale {
			t.Fatalf("threshold %d: TimestampInFuture=%v Stale=%v; want true/false", threshold, r.TimestampInFuture, r.Stale)
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
}
