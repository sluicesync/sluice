// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package fleettui

import (
	"context"
	"errors"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
)

// stubFetch returns a FetchFunc that always yields rep, nil.
func stubFetch(rep fleetReport) FetchFunc {
	return func(context.Context) (fleetReport, error) {
		return rep, nil
	}
}

func sampleReport() fleetReport {
	return fleetReport{
		GeneratedAt: "2026-06-26T15:04:05Z",
		Syncs: []fleetSync{
			{ID: "orders", State: "running", Restarts: 1, SecondsInState: 192},
			{ID: "users", State: "backoff", ConsecutiveFailures: 2, Restarts: 3, LastError: "dial tcp: connection refused", SecondsInState: 12},
			{ID: "audit", State: "failed", ConsecutiveFailures: 9, Restarts: 9, LastError: "slot in use", SecondsInState: 5},
		},
	}
}

// keyMsg builds a tea.KeyMsg for a named key (e.g. "up", "enter").
func keyMsg(s string) tea.KeyMsg {
	switch s {
	case "up":
		return tea.KeyMsg{Type: tea.KeyUp}
	case "down":
		return tea.KeyMsg{Type: tea.KeyDown}
	case "enter":
		return tea.KeyMsg{Type: tea.KeyEnter}
	case "esc":
		return tea.KeyMsg{Type: tea.KeyEsc}
	case "ctrl+c":
		return tea.KeyMsg{Type: tea.KeyCtrlC}
	default:
		return tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune(s)}
	}
}

func update(t *testing.T, m Model, msg tea.Msg) (Model, tea.Cmd) {
	t.Helper()
	next, cmd := m.Update(msg)
	got, ok := next.(Model)
	if !ok {
		t.Fatalf("Update returned %T, want fleettui.Model", next)
	}
	return got, cmd
}

func TestFleetMsgPopulatesRowsAndClampsSelection(t *testing.T) {
	m := NewWithFetch(":9300", 2*time.Second, stubFetch(sampleReport()))
	// Pretend a prior, larger fleet left the cursor on row 5.
	m.selected = 5

	m, cmd := update(t, m, fleetMsg(sampleReport()))

	if len(m.syncs) != 3 {
		t.Fatalf("rows = %d, want 3", len(m.syncs))
	}
	if m.generatedAt != "2026-06-26T15:04:05Z" {
		t.Fatalf("generatedAt = %q", m.generatedAt)
	}
	if m.selected != 2 {
		t.Fatalf("selection not clamped: selected = %d, want 2", m.selected)
	}
	if m.connErr != nil {
		t.Fatalf("connErr should be cleared on success, got %v", m.connErr)
	}
	if cmd == nil {
		t.Fatalf("fleetMsg should re-arm the tick (non-nil cmd)")
	}
	if _, ok := cmd().(tickMsg); !ok {
		t.Fatalf("fleetMsg cmd should yield a tickMsg")
	}
}

func TestErrMsgSetsBannerAndRetainsRows(t *testing.T) {
	m := NewWithFetch(":9300", 2*time.Second, stubFetch(sampleReport()))
	m, _ = update(t, m, fleetMsg(sampleReport()))

	wantErr := errors.New("connection refused")
	m, cmd := update(t, m, errMsg{err: wantErr})

	if m.connErr == nil {
		t.Fatalf("connErr should be set after errMsg")
	}
	if len(m.syncs) != 3 {
		t.Fatalf("errMsg must retain prior rows: got %d, want 3", len(m.syncs))
	}
	if cmd == nil {
		t.Fatalf("errMsg should re-arm the tick (non-nil cmd)")
	}
	if _, ok := cmd().(tickMsg); !ok {
		t.Fatalf("errMsg cmd should yield a tickMsg")
	}

	// A subsequent successful fetch clears the banner.
	m, _ = update(t, m, fleetMsg(sampleReport()))
	if m.connErr != nil {
		t.Fatalf("connErr should clear on next success, got %v", m.connErr)
	}
}

func TestTickMsgIssuesFetch(t *testing.T) {
	rep := sampleReport()
	m := NewWithFetch(":9300", 2*time.Second, stubFetch(rep))

	_, cmd := update(t, m, tickMsg(time.Now()))
	if cmd == nil {
		t.Fatalf("tickMsg should issue a fetch cmd")
	}
	msg := cmd()
	fm, ok := msg.(fleetMsg)
	if !ok {
		t.Fatalf("tick fetch cmd yielded %T, want fleetMsg", msg)
	}
	if len(fm.Syncs) != 3 {
		t.Fatalf("fetched %d rows, want 3", len(fm.Syncs))
	}
}

func TestTickMsgFetchSurfacesError(t *testing.T) {
	wantErr := errors.New("boom")
	m := NewWithFetch(":9300", 2*time.Second, func(context.Context) (fleetReport, error) {
		return fleetReport{}, wantErr
	})
	_, cmd := update(t, m, tickMsg(time.Now()))
	em, ok := cmd().(errMsg)
	if !ok {
		t.Fatalf("failed fetch should yield errMsg")
	}
	if !errors.Is(em.err, wantErr) {
		t.Fatalf("errMsg.err = %v, want %v", em.err, wantErr)
	}
}

func TestQuitKeys(t *testing.T) {
	m := NewWithFetch(":9300", time.Second, stubFetch(sampleReport()))
	for _, k := range []string{"q", "ctrl+c"} {
		_, cmd := update(t, m, keyMsg(k))
		if cmd == nil {
			t.Fatalf("%q should return a quit cmd", k)
		}
		if _, ok := cmd().(tea.QuitMsg); !ok {
			t.Fatalf("%q cmd should be tea.Quit", k)
		}
	}
}

func TestSelectionMovesWithinBounds(t *testing.T) {
	m := NewWithFetch(":9300", time.Second, stubFetch(sampleReport()))
	m, _ = update(t, m, fleetMsg(sampleReport()))
	if m.selected != 0 {
		t.Fatalf("initial selection = %d, want 0", m.selected)
	}

	// Up at the top is a no-op.
	m, _ = update(t, m, keyMsg("up"))
	if m.selected != 0 {
		t.Fatalf("up at top moved to %d, want 0", m.selected)
	}

	// Down walks to the last row and stops.
	for range 5 {
		m, _ = update(t, m, keyMsg("down"))
	}
	if m.selected != 2 {
		t.Fatalf("down past end = %d, want 2 (clamped)", m.selected)
	}

	// k/j are vim aliases for up/down.
	m, _ = update(t, m, keyMsg("k"))
	if m.selected != 1 {
		t.Fatalf("k = %d, want 1", m.selected)
	}
	m, _ = update(t, m, keyMsg("j"))
	if m.selected != 2 {
		t.Fatalf("j = %d, want 2", m.selected)
	}
}

func TestDetailToggleAndClose(t *testing.T) {
	m := NewWithFetch(":9300", time.Second, stubFetch(sampleReport()))
	m, _ = update(t, m, fleetMsg(sampleReport()))

	if m.detailOpen {
		t.Fatalf("detail should start closed")
	}
	m, _ = update(t, m, keyMsg("enter"))
	if !m.detailOpen {
		t.Fatalf("enter should open detail")
	}
	m, _ = update(t, m, keyMsg("enter"))
	if m.detailOpen {
		t.Fatalf("enter should toggle detail closed")
	}
	// esc always closes.
	m, _ = update(t, m, keyMsg("enter"))
	m, _ = update(t, m, keyMsg("esc"))
	if m.detailOpen {
		t.Fatalf("esc should close detail")
	}
}

func TestEnterIsNoOpOnEmptyFleet(t *testing.T) {
	m := NewWithFetch(":9300", time.Second, stubFetch(fleetReport{}))
	m, _ = update(t, m, keyMsg("enter"))
	if m.detailOpen {
		t.Fatalf("enter on an empty fleet should not open detail")
	}
}

func TestWindowSizeUpdatesWidth(t *testing.T) {
	m := NewWithFetch(":9300", time.Second, stubFetch(sampleReport()))
	m, _ = update(t, m, tea.WindowSizeMsg{Width: 120, Height: 40})
	if m.width != 120 || m.height != 40 {
		t.Fatalf("window size = %dx%d, want 120x40", m.width, m.height)
	}
}

func TestNewWithFetchDefaultsRefresh(t *testing.T) {
	m := NewWithFetch(":9300", 0, stubFetch(sampleReport()))
	if m.refresh != 2*time.Second {
		t.Fatalf("zero refresh should default to 2s, got %s", m.refresh)
	}
}

func TestInitKicksOffFetch(t *testing.T) {
	m := NewWithFetch(":9300", time.Second, stubFetch(sampleReport()))
	cmd := m.Init()
	if cmd == nil {
		t.Fatalf("Init should return a fetch cmd")
	}
	if _, ok := cmd().(fleetMsg); !ok {
		t.Fatalf("Init cmd should yield a fleetMsg")
	}
}
