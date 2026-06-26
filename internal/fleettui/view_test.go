// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package fleettui

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/charmbracelet/lipgloss"
	"github.com/muesli/termenv"
)

// TestMain pins lipgloss to the ASCII (no-color) profile so View output
// is deterministic regardless of whether the test runner has a TTY — the
// assertions match on plain text, not ANSI-wrapped text.
func TestMain(m *testing.M) {
	lipgloss.SetColorProfile(termenv.Ascii)
	os.Exit(m.Run())
}

func viewOf(t *testing.T, mutate func(*Model)) string {
	t.Helper()
	m := NewWithFetch(":9300", 2*time.Second, stubFetch(sampleReport()))
	mm, _ := m.Update(fleetMsg(sampleReport()))
	model := mm.(Model)
	if mutate != nil {
		mutate(&model)
	}
	return model.View()
}

func TestViewShowsIDsStatesAndCounts(t *testing.T) {
	out := viewOf(t, nil)

	for _, want := range []string{"orders", "users", "audit", "running", "backoff", "failed"} {
		if !strings.Contains(out, want) {
			t.Errorf("View missing %q\n%s", want, out)
		}
	}
	// 3 total, 1 running, 1 failed (orders running; audit failed).
	if !strings.Contains(out, "3 total · 1 running · 1 failed") {
		t.Errorf("counts line wrong\n%s", out)
	}
	// Header brand + footer hint.
	if !strings.Contains(out, "sluice · fleet dashboard") {
		t.Errorf("missing header title\n%s", out)
	}
	if !strings.Contains(out, "refresh 2s") {
		t.Errorf("footer should show refresh interval\n%s", out)
	}
	// Humanized in-state for orders (192s → 3m12s).
	if !strings.Contains(out, "3m12s") {
		t.Errorf("missing humanized in-state\n%s", out)
	}
}

func TestViewCountsAllRunning(t *testing.T) {
	rep := fleetReport{
		GeneratedAt: "2026-06-26T15:04:05Z",
		Syncs: []fleetSync{
			{ID: "a", State: "running"},
			{ID: "b", State: "running"},
		},
	}
	m := NewWithFetch(":9300", time.Second, stubFetch(rep))
	mm, _ := m.Update(fleetMsg(rep))
	out := mm.(Model).View()
	if !strings.Contains(out, "2 running") {
		t.Errorf("expected '2 running'\n%s", out)
	}
}

func TestViewDetailPaneShowsFullError(t *testing.T) {
	// A multiline, special-char error that the table row truncates/
	// collapses but the detail pane must round-trip verbatim.
	fullErr := "apply failed: relation \"héllo\" \n\tnested cause: pq: duplicate key value violates unique constraint \"orders_pkey\" (SQLSTATE 23505)"
	rep := fleetReport{
		GeneratedAt: "2026-06-26T15:04:05Z",
		Syncs: []fleetSync{
			{ID: "orders", State: "failed", ConsecutiveFailures: 4, Restarts: 7, LastError: fullErr, LastStart: "2026-06-26T15:00:00Z", Since: "2026-06-26T15:03:00Z", SecondsInState: 65},
		},
	}
	m := NewWithFetch(":9300", time.Second, stubFetch(rep))
	mm, _ := m.Update(fleetMsg(rep))
	model := mm.(Model)

	// Detail closed: the full multiline error is NOT present verbatim
	// (the table collapses newlines and truncates).
	closed := model.View()
	if strings.Contains(closed, fullErr) {
		t.Fatalf("table row should not contain the full multiline error verbatim")
	}

	// Open the detail pane.
	mm, _ = model.Update(keyMsg("enter"))
	open := mm.(Model).View()
	if !strings.Contains(open, fullErr) {
		t.Fatalf("detail pane must contain the full last_error verbatim\nerror: %q\nview:\n%s", fullErr, open)
	}
	if !strings.Contains(open, "2026-06-26T15:00:00Z") {
		t.Errorf("detail pane should show last_start\n%s", open)
	}
	if !strings.Contains(open, "2026-06-26T15:03:00Z") {
		t.Errorf("detail pane should show since\n%s", open)
	}
	// seconds_in_state shown both raw and humanized.
	if !strings.Contains(open, "65") || !strings.Contains(open, "1m05s") {
		t.Errorf("detail pane should show seconds_in_state raw + humanized\n%s", open)
	}
}

func TestViewBannerOnConnErrKeepsTable(t *testing.T) {
	out := viewOf(t, func(m *Model) {
		m.connErr = os.ErrDeadlineExceeded
	})
	if !strings.Contains(out, "⚠ :9300 unreachable — showing last known state") {
		t.Errorf("missing unreachable banner\n%s", out)
	}
	// The table is still rendered alongside the banner.
	if !strings.Contains(out, "orders") {
		t.Errorf("banner must not blank the table\n%s", out)
	}
}

func TestViewEmptyFleet(t *testing.T) {
	m := NewWithFetch(":9300", time.Second, stubFetch(fleetReport{}))
	mm, _ := m.Update(fleetMsg(fleetReport{}))
	out := mm.(Model).View()
	if !strings.Contains(out, "(no syncs reported)") {
		t.Errorf("empty fleet should show placeholder\n%s", out)
	}
	if !strings.Contains(out, "0 total · 0 running · 0 failed") {
		t.Errorf("empty counts wrong\n%s", out)
	}
}

func TestHumanizeSeconds(t *testing.T) {
	cases := []struct {
		in   float64
		want string
	}{
		{0, "0s"},
		{45, "45s"},
		{59, "59s"},
		{60, "1m00s"},
		{192, "3m12s"},
		{3599, "59m59s"},
		{3600, "1h00m"},
		{3660, "1h01m"},
		{-5, "0s"},
	}
	for _, c := range cases {
		if got := humanizeSeconds(c.in); got != c.want {
			t.Errorf("humanizeSeconds(%v) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestClipTruncatesWithEllipsis(t *testing.T) {
	if got := clip("hello world", 8); got != "hello w…" {
		t.Errorf("clip = %q, want %q", got, "hello w…")
	}
	if got := clip("short", 10); got != "short" {
		t.Errorf("clip should not pad/truncate when it fits: %q", got)
	}
}

func TestCellPadsToWidth(t *testing.T) {
	got := cell("hi", 5)
	if lipgloss.Width(got) != 5 {
		t.Errorf("cell width = %d, want 5 (%q)", lipgloss.Width(got), got)
	}
}
