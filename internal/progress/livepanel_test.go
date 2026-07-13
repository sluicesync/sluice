// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package progress

import (
	"fmt"
	"strings"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/x/exp/teatest"
)

// epoch is a fixed clock for deterministic livePanel construction.
func epoch() time.Time { return time.Unix(0, 0) }

// applyLive folds a sequence of messages through livePanel.Update — the pure
// msg->model path the teatest program also drives.
func applyLive(m livePanel, msgs ...tea.Msg) livePanel {
	for _, msg := range msgs {
		next, _ := m.Update(msg)
		m = next.(livePanel)
	}
	return m
}

func liveHeader() LiveHeader {
	return LiveHeader{Source: "mysql", Target: "postgresql", StreamID: "homepage-demo"}
}

// TestLivePanelInitialCopyToCDC pins the core continuous transition: the panel
// starts in initial-copy mode rendering the reused checklist + bar, then flips
// to the CDC live body on the first known status reading (position + freshness).
func TestLivePanelInitialCopyToCDC(t *testing.T) {
	m := newLivePanel(testMigrateSpec, liveHeader(), nil, epoch)

	lp := applyLive(
		m,
		phaseStartedMsg{key: "tables"},
		phaseCompletedMsg{key: "tables"},
		tableProgressMsg{table: "orders", done: 1234, total: 3000},
	)
	if lp.cdc {
		t.Fatal("panel should still be in initial-copy mode before any CDC status")
	}
	copyView := lp.View()
	for _, want := range []string{"sluice sync", "mysql -> postgresql", "stream homepage-demo", "mode: initial copy", "orders", "1,234 rows"} {
		if !strings.Contains(copyView, want) {
			t.Errorf("initial-copy view missing %q\n---\n%s\n---", want, copyView)
		}
	}

	lp = applyLive(lp, statusMsg{status: LiveStatus{Position: "0/16B7400", Freshness: 3 * time.Second, Known: true}})
	if !lp.cdc {
		t.Fatal("a known status reading must flip the panel to CDC mode")
	}
	cdcView := lp.View()
	for _, want := range []string{"mode: CDC", "0/16B7400", "3s since last apply", "health"} {
		if !strings.Contains(cdcView, want) {
			t.Errorf("CDC body missing %q\n---\n%s\n---", want, cdcView)
		}
	}
}

// TestLivePanelWarmResumeStraightToCDC pins that a warm resume (no cold-copy
// phases fire) still flips to CDC on the first known status, so the panel is
// never stuck showing an empty initial-copy checklist.
func TestLivePanelWarmResumeStraightToCDC(t *testing.T) {
	m := newLivePanel(testMigrateSpec, liveHeader(), nil, epoch)
	lp := applyLive(m, statusMsg{status: LiveStatus{Position: "mysql-bin.000042:9001", Freshness: time.Minute + 10*time.Second, Known: true}})
	if !lp.cdc {
		t.Fatal("warm resume: first known status must flip to CDC")
	}
	if got := lp.View(); !strings.Contains(got, "1m10s since last apply") {
		t.Errorf("freshness formatting drift:\n%s", got)
	}
}

// TestLivePanelStatusUnknownStaysHonest pins the *Known honesty contract: an
// unknown reading renders "—", never a fabricated 0-second freshness, and does
// not flip mode on its own.
func TestLivePanelStatusUnknownStaysHonest(t *testing.T) {
	m := newLivePanel(testMigrateSpec, liveHeader(), nil, epoch)
	lp := applyLive(m, enterCDCMsg{}, statusMsg{status: LiveStatus{Known: false}})
	body := lp.cdcBody()
	if strings.Contains(body, "0s since last apply") {
		t.Errorf("unknown freshness must not render as 0s:\n%s", body)
	}
	if !strings.Contains(body, "freshness    —") {
		t.Errorf("unknown freshness should render em-dash:\n%s", body)
	}
	// Rows are an honest named gap, never a fabricated number; no lag_bytes.
	if !strings.Contains(body, "n/a (phase 1)") {
		t.Errorf("rows should be surfaced as an honest n/a gap:\n%s", body)
	}
	if strings.Contains(body, "lag_bytes") {
		t.Errorf("cross-engine panel must not imply lag_bytes:\n%s", body)
	}
}

// TestLivePanelWarnIntoRing pins that WARN/ERROR events surface live in the
// bounded recent-events ring with a running total, and that a cold-start
// Sink.Warn is BOTH a checklist warning and a live event.
func TestLivePanelWarnIntoRing(t *testing.T) {
	m := newLivePanel(testMigrateSpec, liveHeader(), nil, epoch)
	lp := applyLive(
		m,
		warnMsg{text: "postgres: dropping cross-engine collation (table=orders)"},
		eventMsg{level: "ERROR", text: "cdc: source connection reset; reconnecting"},
	)
	if lp.eventTotal != 2 {
		t.Fatalf("event total = %d, want 2", lp.eventTotal)
	}
	view := lp.View()
	for _, want := range []string{"recent events (2 total)", "dropping cross-engine collation", "source connection reset"} {
		if !strings.Contains(view, want) {
			t.Errorf("events region missing %q\n---\n%s\n---", want, view)
		}
	}
}

// TestLivePanelEventRingBounded pins the flat-memory guarantee: the ring never
// exceeds maxLiveEvents even after a WARN storm, while the running total keeps
// climbing and the most-recent events are the ones retained.
func TestLivePanelEventRingBounded(t *testing.T) {
	m := newLivePanel(testMigrateSpec, liveHeader(), nil, epoch)
	const storm = maxLiveEvents + 50
	msgs := make([]tea.Msg, storm)
	for i := range msgs {
		msgs[i] = eventMsg{level: "WARN", text: fmt.Sprintf("throttle warning %d", i)}
	}
	lp := applyLive(m, msgs...)

	if len(lp.events) != maxLiveEvents {
		t.Fatalf("ring not bounded: len=%d, want %d", len(lp.events), maxLiveEvents)
	}
	if lp.eventTotal != storm {
		t.Fatalf("running total = %d, want %d", lp.eventTotal, storm)
	}
	newest := lp.events[len(lp.events)-1].text
	if want := fmt.Sprintf("throttle warning %d", storm-1); newest != want {
		t.Fatalf("newest retained event = %q, want %q", newest, want)
	}
}

// TestLivePanelHealth pins the health line: connected/reconnecting styling and
// the reconnect counter.
func TestLivePanelHealth(t *testing.T) {
	m := newLivePanel(testMigrateSpec, liveHeader(), nil, epoch)
	lp := applyLive(m, enterCDCMsg{}, healthMsg{state: healthReconnecting, restarts: 3})
	if got := lp.cdcBody(); !strings.Contains(got, "reconnecting (3 reconnect(s))") {
		t.Errorf("health line drift:\n%s", got)
	}
	lp = applyLive(lp, healthMsg{state: healthConnected, restarts: 3})
	if got := lp.cdcBody(); !strings.Contains(got, "connected") {
		t.Errorf("health should read connected:\n%s", got)
	}
}

// TestLivePanelDrainAndStop pins the drain-and-stop keybinding contract: the
// first q/ctrl+c sets stopping and RETURNS the injected drain side effect (the
// graceful RequestStop), and a second press forces an immediate quit. This is
// the load-bearing correctness point of the panel (no dropped in-flight
// changes) covered without a terminal.
func TestLivePanelDrainAndStop(t *testing.T) {
	for _, key := range []tea.KeyMsg{{Type: tea.KeyCtrlC}, {Type: tea.KeyRunes, Runes: []rune("q")}} {
		called := false
		stopCmd := func() tea.Msg {
			called = true
			return stopRequestedMsg{}
		}
		m := newLivePanel(testMigrateSpec, liveHeader(), stopCmd, epoch)

		next, cmd := m.Update(key)
		lp := next.(livePanel)
		if !lp.stopping {
			t.Fatalf("%v: first press must set stopping", key)
		}
		if cmd == nil {
			t.Fatalf("%v: first press must return the drain stopCmd", key)
		}
		if msg := cmd(); func() bool { _, ok := msg.(stopRequestedMsg); return !ok }() {
			t.Fatalf("%v: drain cmd must yield stopRequestedMsg, got %T", key, msg)
		}
		if !called {
			t.Fatalf("%v: drain side effect (RequestStop) was not invoked", key)
		}
		if !strings.Contains(lp.footer(), "draining in-flight changes") {
			t.Errorf("%v: footer should show draining state:\n%s", key, lp.footer())
		}

		next2, cmd2 := lp.Update(key)
		if !next2.(livePanel).forced {
			t.Fatalf("%v: second press must force quit", key)
		}
		if cmd2 == nil {
			t.Fatalf("%v: second press must return a quit command", key)
		}
	}
}

// TestLivePanelTeatest drives the continuous panel through a real bubbletea
// program (no terminal) to confirm the initial-copy->CDC transition renders and
// that a second q/ctrl+c quits cleanly.
func TestLivePanelTeatest(t *testing.T) {
	m := newLivePanel(testMigrateSpec, liveHeader(), nil, epoch)
	tm := teatest.NewTestModel(t, m, teatest.WithInitialTermSize(120, 30))

	tm.Send(phaseCompletedMsg{key: "tables"})
	tm.Send(tableProgressMsg{table: "orders", done: 500, total: 1000})
	tm.Send(statusMsg{status: LiveStatus{Position: "0/16B7400", Freshness: 5 * time.Second, Known: true}})

	teatest.WaitFor(t, tm.Output(), func(b []byte) bool {
		return strings.Contains(string(b), "mode: CDC") && strings.Contains(string(b), "0/16B7400")
	}, teatest.WithDuration(3*time.Second))

	// First press → draining; second press → force quit.
	tm.Send(tea.KeyMsg{Type: tea.KeyCtrlC})
	tm.Send(tea.KeyMsg{Type: tea.KeyCtrlC})
	tm.WaitFinished(t, teatest.WithFinalTimeout(3*time.Second))
}
