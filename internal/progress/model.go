// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package progress

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"sluicesync.dev/sluice/internal/ir"
)

// The messages the [TTYSink] translates each [Sink] call into and sends
// to the running bubbletea program. They are the model's whole input
// surface; the teatest suite drives the model with these directly, no
// terminal required (mirroring internal/fleettui's pure-Update design).
type (
	phaseStartedMsg   struct{ phase ir.MigrationPhase }
	phaseCompletedMsg struct{ phase ir.MigrationPhase }
	tableProgressMsg  struct {
		table       string
		done, total int64
	}
	warnMsg    struct{ text string }
	summaryMsg struct{ result Result }
)

// phaseStatus is a checklist row's state.
type phaseStatus int

const (
	statusPending phaseStatus = iota
	statusActive
	statusDone
)

// phaseRow is one line of the phase checklist.
type phaseRow struct {
	label  string
	phase  ir.MigrationPhase
	status phaseStatus
}

// tableStat is a bulk-copy table's latest reported progress.
type tableStat struct {
	done, total int64
}

// model is the bubbletea model behind the pretty `migrate` view. Update
// is a pure msg->model function so every transition is teatest-covered
// without a terminal; the bubbletea/lipgloss dependency is confined to
// this package.
type model struct {
	phases []phaseRow
	// index maps an ir phase to its checklist row. Phases outside the
	// checklist (pending/complete/failed) are absent and ignored.
	index map[ir.MigrationPhase]int

	tables map[string]tableStat
	active string // most-recently-updated table, drives the inline bar

	warnings []string

	result     Result
	haveResult bool
	done       bool
	// interrupted records a ctrl+c so the TTYSink can cancel the
	// underlying migration after the program returns.
	interrupted bool

	width int

	// startedAt / now drive the summary's duration; now is injected so
	// the teatest golden output is deterministic.
	startedAt time.Time
	now       func() time.Time
}

// checklist is the phase order shown in the view (ADR-0155): the
// operator-legible sequence, which differs slightly from the internal
// completion order (identity-sync completes before indexes on the MySQL
// fallback path). A row is marked done whenever its PhaseCompleted
// arrives, so an out-of-display-order completion still fills in
// correctly.
var checklist = []phaseRow{
	{label: "Tables", phase: ir.MigrationPhaseTables},
	{label: "Bulk copy", phase: ir.MigrationPhaseBulkCopy},
	{label: "Indexes", phase: ir.MigrationPhaseIndexes},
	{label: "Identity", phase: ir.MigrationPhaseIdentitySync},
	{label: "Constraints", phase: ir.MigrationPhaseConstraints},
	{label: "Views", phase: ir.MigrationPhaseViews},
}

// newModel builds the initial model. startedAt anchors the summary
// duration; now supplies the end time (injectable for deterministic
// tests).
func newModel(startedAt time.Time, now func() time.Time) model {
	rows := make([]phaseRow, len(checklist))
	copy(rows, checklist)
	idx := make(map[ir.MigrationPhase]int, len(rows))
	for i, r := range rows {
		idx[r.phase] = i
	}
	return model{
		phases:    rows,
		index:     idx,
		tables:    map[string]tableStat{},
		startedAt: startedAt,
		now:       now,
	}
}

func (m model) Init() tea.Cmd { return nil }

// Update is the pure state transition.
func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case phaseStartedMsg:
		if i, ok := m.index[msg.phase]; ok && m.phases[i].status != statusDone {
			m.phases[i].status = statusActive
		}
		return m, nil
	case phaseCompletedMsg:
		if i, ok := m.index[msg.phase]; ok {
			m.phases[i].status = statusDone
		}
		return m, nil
	case tableProgressMsg:
		m.tables[msg.table] = tableStat{done: msg.done, total: msg.total}
		m.active = msg.table
		// A table reporting progress means the bulk-copy phase is live.
		if i, ok := m.index[ir.MigrationPhaseBulkCopy]; ok && m.phases[i].status == statusPending {
			m.phases[i].status = statusActive
		}
		return m, nil
	case warnMsg:
		m.warnings = append(m.warnings, msg.text)
		return m, nil
	case summaryMsg:
		m.result = msg.result
		m.haveResult = true
		m.done = true
		// The summary is terminal: quit so bubbletea prints the final
		// static View (the summary panel) and the program returns.
		return m, tea.Quit
	case tea.WindowSizeMsg:
		m.width = msg.Width
		return m, nil
	case tea.KeyMsg:
		// ctrl+c aborts the view AND (via the TTYSink's post-run
		// callback) the migration. In raw mode the terminal delivers
		// ctrl+c as this KeyMsg rather than a process SIGINT, so it must
		// be handled here or it would be swallowed.
		if msg.Type == tea.KeyCtrlC {
			m.interrupted = true
			return m, tea.Quit
		}
		return m, nil
	}
	return m, nil
}

// View renders the live checklist while running. Once done it returns the
// empty string so bubbletea clears the live view on quit; the static
// summary is printed by [TTYSink] AFTER the program exits, as a plain write
// to the terminal. This sidesteps the inline (non-altscreen) renderer
// clipping the last line of its final frame on Quit — which was dropping
// the summary box's bottom border.
func (m model) View() string {
	if m.done {
		return ""
	}
	return m.liveView()
}

// liveView renders the phase checklist with an inline bar on the active
// bulk-copy row.
func (m model) liveView() string {
	var b strings.Builder
	b.WriteString(brandStyle.Render("sluice migrate"))
	b.WriteString("\n\n")
	for _, r := range m.phases {
		b.WriteString("  ")
		b.WriteString(m.renderPhaseRow(r))
		b.WriteString("\n")
	}
	if len(m.warnings) > 0 {
		b.WriteString("\n")
		b.WriteString(warnStyle.Render(fmt.Sprintf("  %d warning(s)", len(m.warnings))))
		b.WriteString("\n")
	}
	return b.String()
}

// renderPhaseRow renders one checklist line, appending the active table's
// bar to the bulk-copy row while it is in progress.
func (m model) renderPhaseRow(r phaseRow) string {
	var mark, label string
	switch r.status {
	case statusDone:
		mark = okStyle.Render(markDone)
		label = r.label
	case statusActive:
		mark = activeStyle.Render(markActive)
		label = activeStyle.Render(r.label)
	default:
		mark = dimStyle.Render(markPending)
		label = dimStyle.Render(r.label)
	}
	line := mark + " " + label
	if r.phase == ir.MigrationPhaseBulkCopy && r.status == statusActive {
		if m.active != "" {
			line += "   " + m.renderActiveTable()
		} else {
			// Copy is under way but the first per-table tick (fired by the
			// 2s ticker) hasn't landed yet — show a hint so a fast copy
			// still reads as alive rather than a frozen "[..] Bulk copy".
			line += "   " + dimStyle.Render("(copying...)")
		}
	}
	return line
}

// renderActiveTable renders the current table's name, bar, and count.
//
// The done>total case is routine, not an error: the row-count total is an
// ESTIMATE (for a MySQL source it is InnoDB's information_schema.table_rows,
// which routinely undershoots — it can even read 0 for a freshly-loaded
// table), so a live copy sails past 100%. Clamping the bar to full and
// leaving it there reads as "stuck". So when the estimate is exceeded we
// keep the bar full but mark the percentage "100%+" and annotate the
// (always-climbing) row count "est. exceeded" — the row count is the source
// of truth and it keeps moving, which resolves the "is it still working?"
// confusion.
func (m model) renderActiveTable() string {
	st := m.tables[m.active]
	var frac float64
	pct := ""
	estExceeded := st.total > 0 && st.done > st.total
	switch {
	case estExceeded:
		frac = 1
		pct = " 100%+"
	case st.total > 0:
		frac = float64(st.done) / float64(st.total)
		pct = fmt.Sprintf(" %3.0f%%", frac*100)
	}
	count := fmt.Sprintf("(%s rows)", humanCount(st.done))
	if estExceeded {
		count = fmt.Sprintf("(%s rows, est. exceeded)", humanCount(st.done))
	}
	return fmt.Sprintf("%-24s %s%s  %s",
		clipName(m.active, 24), renderBar(frac, 20), pct, dimStyle.Render(count))
}

// summaryView renders the compact static panel that replaces the live
// view on completion (so scrollback stays clean).
func (m model) summaryView() string {
	var b strings.Builder
	b.WriteString(headerStyle.Render("sluice migrate - complete"))
	b.WriteString("\n\n")
	fmt.Fprintf(&b, "  %-11s %s\n", "Tables", humanCount(int64(m.result.Tables)))
	if m.result.Rows > 0 {
		fmt.Fprintf(&b, "  %-11s %s\n", "Rows", humanCount(m.result.Rows))
	}
	fmt.Fprintf(&b, "  %-11s %s\n", "Duration", m.elapsed())
	if len(m.warnings) > 0 {
		fmt.Fprintf(&b, "  %-11s %d\n", "Warnings", len(m.warnings))
		for _, w := range m.warnings {
			b.WriteString(warnStyle.Render("    - " + clipLine(oneLine(w), m.warnWidth())))
			b.WriteString("\n")
		}
	}
	return boxStyle.Render(strings.TrimRight(b.String(), "\n"))
}

// elapsed formats the run duration for the summary panel.
func (m model) elapsed() string {
	now := time.Now
	if m.now != nil {
		now = m.now
	}
	d := now().Sub(m.startedAt)
	if d < 0 {
		d = 0
	}
	return d.Round(100 * time.Millisecond).String()
}

// humanCount renders n with thousands separators ("1,234,567").
func humanCount(n int64) string {
	s := strconv.FormatInt(n, 10)
	neg := strings.HasPrefix(s, "-")
	if neg {
		s = s[1:]
	}
	var out strings.Builder
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			out.WriteByte(',')
		}
		out.WriteRune(c)
	}
	if neg {
		return "-" + out.String()
	}
	return out.String()
}

// warnWidth is the max width a summary warning line may occupy before it is
// truncated, derived from the terminal width with room for the "    - "
// prefix and the box border/padding. Falls back to a sane default when the
// width is unknown (no WindowSizeMsg yet — e.g. under teatest), so a long
// warning never overflows the box's right edge.
func (m model) warnWidth() int {
	w := m.width
	if w <= 0 {
		w = 100
	}
	maxw := w - 12
	if maxw < 20 {
		maxw = 20
	}
	return maxw
}

// clipLine truncates s to at most maxw runes, appending an ASCII "..."
// marker when it overflows (ASCII, not the "…" glyph, per the same
// render-everywhere discipline as the checklist marks).
func clipLine(s string, maxw int) string {
	r := []rune(s)
	if len(r) <= maxw {
		return s
	}
	if maxw <= 3 {
		return string(r[:maxw])
	}
	return string(r[:maxw-3]) + "..."
}

// clipName right-pads/truncates a table name to a fixed cell so the bar
// column aligns across tables.
func clipName(s string, w int) string {
	if len(s) <= w {
		return s
	}
	if w <= 1 {
		return s[:w]
	}
	return s[:w-1] + "~"
}

// oneLine collapses newlines/tabs so a multiline warning stays on one
// summary line.
func oneLine(s string) string {
	return strings.NewReplacer("\n", " ", "\r", " ", "\t", " ").Replace(s)
}
