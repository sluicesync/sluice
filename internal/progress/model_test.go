// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package progress

import (
	"io"
	"os"
	"strings"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/charmbracelet/x/exp/teatest"
	"github.com/muesli/termenv"

	"sluicesync.dev/sluice/internal/ir"
)

// TestMain forces lipgloss to the ASCII (no-ANSI) profile so the rendered
// frames in these tests are plain text — deterministic to pin, and a clean
// snapshot to paste into the review. Production styling (brand colours,
// bold) is unaffected; only the test process's renderer is pinned.
func TestMain(m *testing.M) {
	lipgloss.SetColorProfile(termenv.Ascii)
	os.Exit(m.Run())
}

// apply folds a sequence of messages through Update, returning the final
// model — the pure-Update path the teatest program also drives.
func apply(m model, msgs ...tea.Msg) model {
	for _, msg := range msgs {
		next, _ := m.Update(msg)
		m = next.(model)
	}
	return m
}

// fixedClock returns a model whose duration renders deterministically as
// elapsed after start.
func fixedModel(elapsed time.Duration) model {
	start := time.Unix(0, 0)
	return newModel(start, func() time.Time { return start.Add(elapsed) })
}

// TestModelPhaseTransitions drives the model through a realistic migrate
// via teatest — phases fill in, a table bar advances, the summary is
// terminal (tea.Quit) — and asserts the final rendered frame is the static
// summary panel. This is the ADR-0155 phase->summary transition pin.
func TestModelPhaseTransitions(t *testing.T) {
	m := fixedModel(4200 * time.Millisecond)
	tm := teatest.NewTestModel(t, m, teatest.WithInitialTermSize(120, 24))

	tm.Send(phaseStartedMsg{phase: ir.MigrationPhaseTables})
	tm.Send(phaseCompletedMsg{phase: ir.MigrationPhaseTables})
	tm.Send(tableProgressMsg{table: "orders", done: 1234, total: 3000})
	tm.Send(tableProgressMsg{table: "orders", done: 3000, total: 3000})
	tm.Send(phaseCompletedMsg{phase: ir.MigrationPhaseBulkCopy})
	tm.Send(phaseCompletedMsg{phase: ir.MigrationPhaseIndexes})
	tm.Send(phaseCompletedMsg{phase: ir.MigrationPhaseIdentitySync})
	tm.Send(warnMsg{text: "constraint attached degraded (NOT VALID) (table=orders)"})
	tm.Send(phaseCompletedMsg{phase: ir.MigrationPhaseConstraints})
	tm.Send(phaseCompletedMsg{phase: ir.MigrationPhaseViews})
	tm.Send(summaryMsg{result: Result{Tables: 3, Rows: 12345}})

	tm.WaitFinished(t, teatest.WithFinalTimeout(3*time.Second))

	// The live view clears on quit (View returns "" when done) — the summary
	// is printed by TTYSink AFTER the program exits, so it is not in the
	// program's own output. Assert on the final model's summaryView, which is
	// exactly what TTYSink writes.
	fm, ok := tm.FinalModel(t).(model)
	if !ok {
		t.Fatalf("final model is %T, want progress.model", tm.FinalModel(t))
	}
	if !fm.done {
		t.Fatal("final model not marked done after summaryMsg")
	}
	// And the live frames must have cleared cleanly (no leftover checklist in
	// the final program output).
	out, err := io.ReadAll(tm.FinalOutput(t))
	if err != nil {
		t.Fatalf("read final output: %v", err)
	}
	if strings.Contains(string(out), "sluice migrate - complete") {
		t.Error("summary must be printed post-program by TTYSink, not inside the bubbletea frame")
	}

	summary := fm.summaryView()
	for _, want := range []string{
		"sluice migrate - complete",
		"Tables",
		"Rows",
		"12,345",
		"Duration",
		"4.2s",
		"Warnings",
		"constraint attached degraded",
	} {
		if !strings.Contains(summary, want) {
			t.Errorf("summary missing %q\n---\n%s\n---", want, summary)
		}
	}
}

// TestLiveViewSnapshot pins the mid-run checklist frame (Tables done,
// Bulk copy active with an in-flight table bar). The golden string doubles
// as the visual review artifact.
func TestLiveViewSnapshot(t *testing.T) {
	m := apply(
		fixedModel(0),
		phaseStartedMsg{phase: ir.MigrationPhaseTables},
		phaseCompletedMsg{phase: ir.MigrationPhaseTables},
		tableProgressMsg{table: "orders", done: 1234, total: 3000},
	)
	want := strings.Join([]string{
		"sluice migrate",
		"",
		"  [ok] Tables",
		"  [..] Bulk copy   orders                   [########------------]  41%  (1,234 rows)",
		"  [  ] Indexes",
		"  [  ] Identity",
		"  [  ] Constraints",
		"  [  ] Views",
		"",
	}, "\n")
	if got := m.liveView(); got != want {
		t.Errorf("live view drift:\n got:\n%s\nwant:\n%s", got, want)
	}
}

// TestActiveRowEstExceeded pins Fix B: when the copy passes the row-count
// estimate (done>total — routine for an undershooting InnoDB estimate), the
// bar stays full but the row shows "100%+" and "est. exceeded" so it reads
// as still-progressing rather than stuck.
func TestActiveRowEstExceeded(t *testing.T) {
	m := apply(
		fixedModel(0),
		phaseStartedMsg{phase: ir.MigrationPhaseBulkCopy},
		tableProgressMsg{table: "events", done: 1500000, total: 1000000},
	)
	row := m.renderActiveTable()
	for _, want := range []string{"100%+", "1,500,000 rows, est. exceeded", "[####################]"} {
		if !strings.Contains(row, want) {
			t.Errorf("est-exceeded active row missing %q\n got: %s", want, row)
		}
	}
}

// TestSummaryViewSnapshot pins the terminal summary panel.
func TestSummaryViewSnapshot(t *testing.T) {
	m := apply(
		fixedModel(4200*time.Millisecond),
		warnMsg{text: "constraint attached degraded (NOT VALID) (table=orders constraint=orders_fk)"},
		summaryMsg{result: Result{Tables: 3, Rows: 12345}},
	)
	got := m.summaryView()
	for _, want := range []string{
		"sluice migrate - complete",
		"Tables      3",
		"Rows        12,345",
		"Duration    4.2s",
		"Warnings    1",
		"- constraint attached degraded (NOT VALID) (table=orders constraint=orders_fk)",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("summary view missing %q\n---\n%s\n---", want, got)
		}
	}
}

// TestCtrlCInterrupts pins that ctrl+c records the interrupt (the TTYSink's
// post-run hook reads this to cancel the migration) and returns tea.Quit.
func TestCtrlCInterrupts(t *testing.T) {
	m := fixedModel(0)
	next, cmd := m.Update(tea.KeyMsg{Type: tea.KeyCtrlC})
	if !next.(model).interrupted {
		t.Error("ctrl+c did not set interrupted")
	}
	if cmd == nil {
		t.Error("ctrl+c did not return a quit command")
	}
}

// TestActiveTableClampsPercentOver100 pins that a done count exceeding the
// (estimated) total — routine for a MySQL source, whose InnoDB row estimate
// undershoots — renders a bar clamped to 100%, never a nonsensical >100%.
func TestActiveTableClampsPercentOver100(t *testing.T) {
	m := apply(
		fixedModel(time.Second),
		tableProgressMsg{table: "customers", done: 324887, total: 25000},
	)
	view := m.View()
	if !strings.Contains(view, "100%") {
		t.Errorf("clamped bar should read 100%%; view:\n%s", view)
	}
	if strings.Contains(view, "1299%") || strings.Contains(view, "299%") {
		t.Errorf("percent must clamp at 100%%, got a >100%% value; view:\n%s", view)
	}
}

// TestSummaryWarningTruncatedToWidth pins that a long warning is truncated to
// the terminal width (with an ASCII "..." marker) so it never overflows the
// summary box's right edge.
func TestSummaryWarningTruncatedToWidth(t *testing.T) {
	long := "postgres: dropping cross-engine column collations (no PG equivalent; " +
		"the target columns use the database default collation, which may change " +
		"sort/comparison semantics) table=customers"
	m := apply(
		fixedModel(time.Second),
		tea.WindowSizeMsg{Width: 70},
		warnMsg{text: long},
		summaryMsg{result: Result{Tables: 1, Rows: 100}},
	)
	// Fix A: once done, View() returns "" (the summary is printed by TTYSink
	// after the program exits, so the inline renderer can't clip the box).
	// Assert on summaryView() — exactly the string TTYSink writes.
	view := m.summaryView()
	if !strings.Contains(view, "...") {
		t.Errorf("long warning should be truncated with '...'; view:\n%s", view)
	}
	if strings.Contains(view, "comparison semantics") {
		t.Errorf("warning tail should be truncated away at width 70; view:\n%s", view)
	}
}
