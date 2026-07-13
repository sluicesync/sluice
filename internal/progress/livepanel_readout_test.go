// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package progress

import (
	"strings"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/x/exp/teatest"
)

// readoutHeader is a representative readout-panel header (the broker shape).
func readoutHeader() LiveHeader {
	return LiveHeader{Mode: "broker", Source: "file:///chain", Target: "postgresql", StreamID: "broker-1"}
}

// TestReadoutPanelStartingThenBody pins the ADR-0156 phases-2/3 generic
// readout body: the brand line carries the mode word, the header renders the
// route, the pre-first-readout body is a dimmed "starting…" placeholder (NOT
// the `sync start` initial-copy checklist), and the first [readoutMsg] renders
// the ordered label/value list.
func TestReadoutPanelStartingThenBody(t *testing.T) {
	m := newReadoutPanel(readoutHeader(), nil, epoch)

	start := m.View()
	for _, want := range []string{"sluice broker", "file:///chain -> postgresql", "stream broker-1", "starting…"} {
		if !strings.Contains(start, want) {
			t.Errorf("pre-readout view missing %q\n---\n%s\n---", want, start)
		}
	}
	// A readout panel must NOT render the sync-start initial-copy checklist.
	if strings.Contains(start, "mode: initial copy") {
		t.Errorf("readout panel must not show the initial-copy checklist:\n%s", start)
	}

	lp := applyLive(m, readoutMsg{fields: []Field{
		{Label: "position", Value: "incr-42"},
		{Label: "incrementals", Value: "7"},
		{Label: "chunks", Value: "13"},
		{Label: "last poll", Value: "2026-07-13T00:00:00Z"},
	}})
	body := lp.View()
	for _, want := range []string{"position", "incr-42", "incrementals", "7", "chunks", "13", "last poll"} {
		if !strings.Contains(body, want) {
			t.Errorf("readout body missing %q\n---\n%s\n---", want, body)
		}
	}
	if strings.Contains(body, "starting…") {
		t.Errorf("readout body should replace the starting placeholder once fields arrive:\n%s", body)
	}
	// The events + footer chrome is shared with the sync-start panel.
	for _, want := range []string{"recent events", "drain and stop"} {
		if !strings.Contains(body, want) {
			t.Errorf("readout panel missing shared chrome %q\n---\n%s\n---", want, body)
		}
	}
}

// TestReadoutPanelDetailHeader pins the metrics-watch-shape header: a Detail
// string REPLACES the source→target route (that panel has no target DSN), and
// the brand carries the "metrics-watch" mode.
func TestReadoutPanelDetailHeader(t *testing.T) {
	h := LiveHeader{Mode: "metrics-watch", Detail: "watching app_db  ·  acme"}
	m := newReadoutPanel(h, nil, epoch)
	view := m.View()
	for _, want := range []string{"sluice metrics-watch", "watching app_db  ·  acme"} {
		if !strings.Contains(view, want) {
			t.Errorf("detail header missing %q\n---\n%s\n---", want, view)
		}
	}
	if strings.Contains(view, "->") {
		t.Errorf("a Detail header must not render the source->target route:\n%s", view)
	}
}

// TestReadoutPanelEventsAndDrain pins that the readout panel reuses the phase-1
// events ring (slog-gate-forwarded WARN/ERROR surface live) and the
// drain-and-stop keybinding — the load-bearing continuous-panel chrome must be
// identical across all four commands.
func TestReadoutPanelEventsAndDrain(t *testing.T) {
	called := false
	stopCmd := func() tea.Msg {
		called = true
		return stopRequestedMsg{}
	}
	m := newReadoutPanel(readoutHeader(), stopCmd, epoch)

	lp := applyLive(m, eventMsg{level: "ERROR", text: "broker: replay failed; retrying"})
	if lp.eventTotal != 1 {
		t.Fatalf("event total = %d, want 1", lp.eventTotal)
	}
	if got := lp.View(); !strings.Contains(got, "recent events (1 total)") || !strings.Contains(got, "replay failed") {
		t.Errorf("readout panel events region missing the forwarded ERROR:\n%s", got)
	}

	next, cmd := lp.Update(tea.KeyMsg{Type: tea.KeyCtrlC})
	if !next.(livePanel).stopping {
		t.Fatal("first ctrl+c must set stopping on a readout panel")
	}
	if cmd == nil {
		t.Fatal("first ctrl+c must return the drain stopCmd on a readout panel")
	}
	_ = cmd()
	if !called {
		t.Fatal("drain side effect (cancel) was not invoked")
	}
}

// TestReadoutPanelTeatest drives a readout panel through a real bubbletea
// program (no terminal): the starting placeholder, then a readout refresh, then
// a two-press force-quit.
func TestReadoutPanelTeatest(t *testing.T) {
	m := newReadoutPanel(LiveHeader{Mode: "metrics-watch", Detail: "watching app_db  ·  acme"}, nil, epoch)
	tm := teatest.NewTestModel(t, m, teatest.WithInitialTermSize(120, 30))

	tm.Send(readoutMsg{fields: []Field{
		{Label: "cpu", Value: "0.120"},
		{Label: "storage", Value: "0.400 (used 40.0G/100.0G)"},
	}})
	teatest.WaitFor(t, tm.Output(), func(b []byte) bool {
		return strings.Contains(string(b), "sluice metrics-watch") && strings.Contains(string(b), "used 40.0G/100.0G")
	}, teatest.WithDuration(3*time.Second))

	tm.Send(tea.KeyMsg{Type: tea.KeyCtrlC})
	tm.Send(tea.KeyMsg{Type: tea.KeyCtrlC})
	tm.WaitFinished(t, teatest.WithFinalTimeout(3*time.Second))
}

// TestSyncStartHeaderUnchanged pins that the phase-1 `sync start` panel's brand
// + header are byte-identical after the LiveHeader/brand generalisation: an
// empty Mode still renders "sluice sync", and an empty Detail keeps the
// source→target route. (Guards the ADR-0156 "do not regress phase 1" contract.)
func TestSyncStartHeaderUnchanged(t *testing.T) {
	m := newLivePanel(testMigrateSpec, LiveHeader{Source: "mysql", Target: "postgresql", StreamID: "s1"}, nil, epoch)
	if got := m.brandLine(); got != "sluice sync" {
		t.Errorf("sync-start brand drifted: got %q, want %q", got, "sluice sync")
	}
	if got := m.headerLine(); got != "mysql -> postgresql  ·  stream s1" {
		t.Errorf("sync-start header drifted: got %q", got)
	}
}
