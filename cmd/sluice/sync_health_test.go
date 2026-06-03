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

func TestEvaluateHealth_StreamFound_NotStale(t *testing.T) {
	now := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)
	streams := []ir.StreamStatus{
		{StreamID: "myapp-prod", Position: ir.Position{Token: "lsn=0/1A2B3C4D"}, UpdatedAt: now.Add(-30 * time.Second)},
	}
	r := evaluateHealth(streams, "myapp-prod", 60, now)
	if !r.Found {
		t.Fatal("expected Found=true")
	}
	if r.Stale {
		t.Errorf("expected not stale (30s ago, threshold 60s); got %+v", r)
	}
	if r.SecondsSinceLastApply != 30 {
		t.Errorf("expected 30 seconds; got %d", r.SecondsSinceLastApply)
	}
}

func TestEvaluateHealth_Stale(t *testing.T) {
	now := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)
	streams := []ir.StreamStatus{
		{StreamID: "myapp-prod", Position: ir.Position{Token: "lsn=0/1A2B3C4D"}, UpdatedAt: now.Add(-120 * time.Second)},
	}
	r := evaluateHealth(streams, "myapp-prod", 60, now)
	if !r.Stale {
		t.Errorf("expected stale (120s ago, threshold 60s); got %+v", r)
	}
}

func TestEvaluateHealth_ThresholdZeroDisablesCheck(t *testing.T) {
	now := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)
	streams := []ir.StreamStatus{
		{StreamID: "myapp-prod", Position: ir.Position{Token: "x"}, UpdatedAt: now.Add(-10 * time.Hour)},
	}
	r := evaluateHealth(streams, "myapp-prod", 0, now)
	if r.Stale {
		t.Errorf("threshold 0 should disable check even on a 10-hour-old stream; got %+v", r)
	}
	if r.SecondsSinceLastApply == 0 {
		t.Error("seconds_since_last_apply should still be populated even when threshold is 0")
	}
}

func TestEvaluateHealth_StreamNotFound(t *testing.T) {
	now := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)
	streams := []ir.StreamStatus{
		{StreamID: "other-stream", UpdatedAt: now},
	}
	r := evaluateHealth(streams, "myapp-prod", 60, now)
	if r.Found {
		t.Errorf("expected Found=false; got %+v", r)
	}
	if r.Stale {
		t.Errorf("not-found shouldn't be marked stale (it's an op error, not a threshold breach)")
	}
}

func TestRenderHealth_TextHealthy(t *testing.T) {
	r := HealthResult{
		StreamID: "myapp-prod", Found: true, Position: "lsn=0/1A",
		UpdatedAt: "2026-05-07T12:00:00Z", SecondsSinceLastApply: 30, Threshold: 60,
	}
	var buf bytes.Buffer
	if err := renderHealth(&buf, r, "text"); err != nil {
		t.Fatalf("renderHealth: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "state: healthy") {
		t.Errorf("text output should say healthy; got:\n%s", out)
	}
	if !strings.Contains(out, "seconds_since_last_apply: 30") {
		t.Errorf("text output missing seconds line; got:\n%s", out)
	}
}

func TestRenderHealth_TextStale(t *testing.T) {
	r := HealthResult{
		StreamID: "myapp-prod", Found: true, Position: "x",
		SecondsSinceLastApply: 200, Threshold: 60, Stale: true,
	}
	var buf bytes.Buffer
	if err := renderHealth(&buf, r, "text"); err != nil {
		t.Fatalf("renderHealth: %v", err)
	}
	if !strings.Contains(buf.String(), "STALE") {
		t.Errorf("text output should announce STALE; got:\n%s", buf.String())
	}
}

func TestRenderHealth_JSON(t *testing.T) {
	r := HealthResult{
		StreamID: "myapp-prod", Found: true, Position: "lsn=0/1A",
		UpdatedAt: "2026-05-07T12:00:00Z", SecondsSinceLastApply: 30, Threshold: 60,
	}
	var buf bytes.Buffer
	if err := renderHealth(&buf, r, "json"); err != nil {
		t.Fatalf("renderHealth: %v", err)
	}
	var got HealthResult
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("decode JSON: %v", err)
	}
	if got.SecondsSinceLastApply != 30 || got.StreamID != "myapp-prod" {
		t.Errorf("JSON round-trip mismatch; got %+v", got)
	}
}

// TestRenderHealth_SourceProbeFields covers the v0.15.0 source-side
// addition: when the source probe ran, the rendered text/json
// includes source_position; when it didn't, source_probe shows the
// reason.
func TestRenderHealth_SourceProbeFields(t *testing.T) {
	t.Run("source probe available with lag bytes", func(t *testing.T) {
		r := HealthResult{
			StreamID: "myapp", Found: true,
			Position: "0/1A2B3C4D", UpdatedAt: "2026-05-08T12:00:00Z",
			SecondsSinceLastApply: 5,
			SourcePosition:        "0/1A2B3F12",
			SourceProbeAvailable:  true,
			LagBytes:              973, LagBytesIsAvail: true,
		}
		var buf bytes.Buffer
		if err := renderHealth(&buf, r, "text"); err != nil {
			t.Fatalf("renderHealth: %v", err)
		}
		out := buf.String()
		for _, want := range []string{
			"source_position: 0/1A2B3F12",
			"lag_bytes: 973",
		} {
			if !strings.Contains(out, want) {
				t.Errorf("expected %q; got:\n%s", want, out)
			}
		}
	})
	t.Run("source probe skipped with reason", func(t *testing.T) {
		r := HealthResult{
			StreamID: "myapp", Found: true,
			Position:              "x",
			SecondsSinceLastApply: 5,
			SourceProbeReason:     "open source schema reader: connection refused",
		}
		var buf bytes.Buffer
		if err := renderHealth(&buf, r, "text"); err != nil {
			t.Fatalf("renderHealth: %v", err)
		}
		if !strings.Contains(buf.String(), "source_probe: skipped (open source schema reader: connection refused)") {
			t.Errorf("expected skipped reason; got:\n%s", buf.String())
		}
	})
	t.Run("source probe available but lag-bytes unavailable", func(t *testing.T) {
		r := HealthResult{
			StreamID: "myapp", Found: true,
			Position: "abc", SecondsSinceLastApply: 1,
			SourcePosition: "def", SourceProbeAvailable: true,
			LagBytesIsAvail: false,
		}
		var buf bytes.Buffer
		if err := renderHealth(&buf, r, "text"); err != nil {
			t.Fatalf("renderHealth: %v", err)
		}
		if !strings.Contains(buf.String(), "lag_bytes: unavailable") {
			t.Errorf("expected lag_bytes-unavailable line; got:\n%s", buf.String())
		}
	})
}

// TestRenderHealth_LagBytesStaleState pins the new STALE state when
// lag-bytes exceeds the threshold (vs the existing time-based STALE).
func TestRenderHealth_LagBytesStaleState(t *testing.T) {
	r := HealthResult{
		StreamID: "myapp", Found: true,
		Position: "x", SecondsSinceLastApply: 0,
		LagBytes: 5000000, LagBytesIsAvail: true, LagBytesStale: true, LagThreshold: 1000000,
	}
	var buf bytes.Buffer
	if err := renderHealth(&buf, r, "text"); err != nil {
		t.Fatalf("renderHealth: %v", err)
	}
	if !strings.Contains(buf.String(), "STALE (lag 5000000 bytes, threshold 1000000)") {
		t.Errorf("expected lag-bytes STALE state; got:\n%s", buf.String())
	}
}

func TestRenderHealth_NotFound(t *testing.T) {
	r := HealthResult{StreamID: "myapp-prod"}
	var buf bytes.Buffer
	if err := renderHealth(&buf, r, "text"); err != nil {
		t.Fatalf("renderHealth: %v", err)
	}
	if !strings.Contains(buf.String(), "found:  false") {
		t.Errorf("not-found shape should report false; got:\n%s", buf.String())
	}
}

// TestRenderHealth_SpillFields pins the severity-B finding F2 surface
// shape on the sync-health command: spill_txns + spill_bytes render in
// the text output (and round-trip through JSON) when set, and stay
// absent from both when nil. The pointer-omitempty pattern is load-
// bearing — a nil pointer means "stats unavailable" (PG < 14, no decode
// yet, or non-PG source); rendering a literal 0 in that case would
// mislead operators into thinking spill is "definitely zero" when the
// real signal is "we can't tell."
func TestRenderHealth_SpillFields(t *testing.T) {
	t.Run("spill present renders both fields", func(t *testing.T) {
		txns := int64(7)
		bytes64 := int64(123456)
		r := HealthResult{
			StreamID: "myapp", Found: true,
			Position: "x", SecondsSinceLastApply: 5,
			SourcePosition: "y", SourceProbeAvailable: true,
			LagBytesIsAvail: false,
			SpillTxns:       &txns,
			SpillBytes:      &bytes64,
		}
		var buf bytes.Buffer
		if err := renderHealth(&buf, r, "text"); err != nil {
			t.Fatalf("renderHealth: %v", err)
		}
		out := buf.String()
		for _, want := range []string{"spill_txns: 7", "spill_bytes: 123456"} {
			if !strings.Contains(out, want) {
				t.Errorf("expected %q in output; got:\n%s", want, out)
			}
		}
	})

	t.Run("spill absent omits both fields in text", func(t *testing.T) {
		r := HealthResult{
			StreamID: "myapp", Found: true,
			Position: "x", SecondsSinceLastApply: 5,
			SourcePosition: "y", SourceProbeAvailable: true,
			LagBytesIsAvail: false,
		}
		var buf bytes.Buffer
		if err := renderHealth(&buf, r, "text"); err != nil {
			t.Fatalf("renderHealth: %v", err)
		}
		out := buf.String()
		for _, banned := range []string{"spill_txns", "spill_bytes"} {
			if strings.Contains(out, banned) {
				t.Errorf("expected %q absent (stats unavailable); got:\n%s", banned, out)
			}
		}
	})

	t.Run("JSON omitempty when nil", func(t *testing.T) {
		r := HealthResult{
			StreamID: "myapp", Found: true,
			Position: "x", SecondsSinceLastApply: 5,
		}
		b, err := json.Marshal(r)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		out := string(b)
		for _, banned := range []string{`"spill_txns"`, `"spill_bytes"`} {
			if strings.Contains(out, banned) {
				t.Errorf("expected %q absent from JSON when stats unavailable; got:\n%s", banned, out)
			}
		}
	})

	t.Run("JSON round-trip when populated", func(t *testing.T) {
		txns := int64(3)
		bytes64 := int64(64 * 1024 * 1024)
		r := HealthResult{
			StreamID: "myapp", Found: true,
			Position: "x", SecondsSinceLastApply: 5,
			SpillTxns:  &txns,
			SpillBytes: &bytes64,
		}
		b, err := json.Marshal(r)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		var got HealthResult
		if err := json.Unmarshal(b, &got); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if got.SpillTxns == nil || *got.SpillTxns != 3 {
			t.Errorf("SpillTxns round-trip; got %v", got.SpillTxns)
		}
		if got.SpillBytes == nil || *got.SpillBytes != 64*1024*1024 {
			t.Errorf("SpillBytes round-trip; got %v", got.SpillBytes)
		}
	})
}
