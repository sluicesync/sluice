// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"log/slog"
	"os"
	"testing"

	"github.com/mattn/go-isatty"

	"sluicesync.dev/sluice/internal/ir"
)

// fakeEventSink records the events the slog gate forwards, so the gate can be
// asserted without a live bubbletea program.
type fakeEventSink struct {
	events []string // "LEVEL: text"
}

func (f *fakeEventSink) Event(level, text string) {
	f.events = append(f.events, level+": "+text)
}

// TestLiveGate_ForwardsWarnErrorDropsInfo pins ADR-0156's log-handling contract
// for the continuous panel: INFO/DEBUG are dropped on the TTY, WARN/ERROR are
// FORWARDED (not buffered) into the recent-events ring with their attrs
// flattened into the line, and ERROR is tagged distinctly from WARN.
func TestLiveGate_ForwardsWarnErrorDropsInfo(t *testing.T) {
	fake := &fakeEventSink{}
	gate := &liveGateHandler{sink: fake}
	logger := slog.New(gate)

	if gate.Enabled(context.Background(), slog.LevelInfo) {
		t.Error("INFO must be dropped on the TTY (Enabled should be false)")
	}
	if !gate.Enabled(context.Background(), slog.LevelWarn) {
		t.Error("WARN must be forwarded (Enabled should be true)")
	}

	logger.Info("phase progress that must be suppressed")
	logger.Warn("cdc: source reconnecting", slog.String("stream_id", "demo"), slog.Int("attempt", 2))
	logger.Error("apply failed")

	if len(fake.events) != 2 {
		t.Fatalf("want exactly 2 forwarded events (WARN+ERROR), got %d: %v", len(fake.events), fake.events)
	}
	if fake.events[0] != "WARN: cdc: source reconnecting (stream_id=demo attempt=2)" {
		t.Errorf("WARN event = %q; want attrs flattened into the line", fake.events[0])
	}
	if fake.events[1] != "ERROR: apply failed" {
		t.Errorf("ERROR event = %q; want ERROR-tagged", fake.events[1])
	}
}

// TestLiveGate_WithAttrs pins that logger-scoped attrs (WithAttrs) survive into
// the forwarded event line.
func TestLiveGate_WithAttrs(t *testing.T) {
	fake := &fakeEventSink{}
	logger := slog.New(&liveGateHandler{sink: fake}).With(slog.String("stream_id", "s1"))
	logger.Warn("throttled")
	if len(fake.events) != 1 || fake.events[0] != "WARN: throttled (stream_id=s1)" {
		t.Fatalf("WithAttrs not carried into event: %v", fake.events)
	}
}

// TestPickLiveStream pins the stream-selection rule the poller uses: match
// --stream-id when set; adopt the sole stream when the id was auto-generated;
// otherwise report "not yet" so the panel stays in initial-copy mode rather
// than fabricating a CDC reading.
func TestPickLiveStream(t *testing.T) {
	a := ir.StreamStatus{StreamID: "a"}
	b := ir.StreamStatus{StreamID: "b"}

	if got, ok := pickLiveStream([]ir.StreamStatus{a, b}, "b"); !ok || got.StreamID != "b" {
		t.Errorf("explicit id: got %+v ok=%v, want b", got, ok)
	}
	if _, ok := pickLiveStream([]ir.StreamStatus{a, b}, "c"); ok {
		t.Error("explicit id with no match must report not-found")
	}
	if got, ok := pickLiveStream([]ir.StreamStatus{a}, ""); !ok || got.StreamID != "a" {
		t.Errorf("empty id + single stream: got %+v ok=%v, want a", got, ok)
	}
	if _, ok := pickLiveStream([]ir.StreamStatus{a, b}, ""); ok {
		t.Error("empty id + multiple streams must report not-found (ambiguous)")
	}
	if _, ok := pickLiveStream(nil, ""); ok {
		t.Error("no streams must report not-found")
	}
}

// TestSyncLivePanelGating_NonTerminal pins that `sync start` gates its live
// panel on the SAME wantPrettyProgress rule as ADR-0155: a non-terminal stdout
// keeps the byte-identical structured slog stream even under --log-format=text.
// Under `go test`, os.Stdout is a pipe (not a TTY).
func TestSyncLivePanelGating_NonTerminal(t *testing.T) {
	if isatty.IsTerminal(os.Stdout.Fd()) {
		t.Skip("test stdout is a terminal; the non-TTY assertion needs a piped stdout")
	}
	g := &Globals{LogFormat: "text"}
	if wantPrettyProgress(g, false, false, false) {
		t.Error("non-terminal stdout must keep the structured stream for sync start")
	}
}
