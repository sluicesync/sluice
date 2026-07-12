// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package fleettui

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"
)

// Column widths for the fleet table. The last-error column takes the
// remaining content width (computed per-render); these are the fixed
// leading columns.
const (
	colID       = 22
	colState    = 9
	colRestarts = 8
	colConsec   = 8
	colInState  = 9
	// colGaps is the number of single-space separators between the six
	// columns.
	colGaps = 5
	// defaultWidth is the fallback total terminal width before the first
	// WindowSizeMsg arrives.
	defaultWidth = 100
	// minLastError floors the last-error column so it stays useful on a
	// narrow terminal.
	minLastError = 12
)

// Brand palette (ADR-0125 §2 / sluicesync.com). Primary #F35815, deep
// #C0410A; states are color-coded per the ADR.
var (
	brandColor = lipgloss.Color("#F35815")
	deepColor  = lipgloss.Color("#C0410A")
	greyColor  = lipgloss.Color("#6b7280")

	brandStyle    = lipgloss.NewStyle().Foreground(brandColor).Bold(true)
	headerStyle   = lipgloss.NewStyle().Foreground(deepColor).Bold(true)
	dimStyle      = lipgloss.NewStyle().Foreground(greyColor)
	bannerStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("#c0392b")).Bold(true)
	selectedStyle = lipgloss.NewStyle().Background(brandColor).Foreground(lipgloss.Color("#ffffff")).Bold(true)
	boxStyle      = lipgloss.NewStyle().Border(lipgloss.RoundedBorder()).BorderForeground(brandColor).Padding(0, 1)
)

// stateColors maps a sync state to its accent color.
var stateColors = map[string]lipgloss.Color{
	"running":  lipgloss.Color("#1a7f37"),
	"backoff":  lipgloss.Color("#b7791f"),
	"starting": lipgloss.Color("#F35815"),
	"failed":   lipgloss.Color("#c0392b"),
	"stopped":  greyColor,
}

// stateStyle returns the foreground style for a state, defaulting to
// grey for an unknown state.
func stateStyle(state string) lipgloss.Style {
	c, ok := stateColors[state]
	if !ok {
		c = greyColor
	}
	return lipgloss.NewStyle().Foreground(c)
}

// View renders the fleet as a bordered, lipgloss-styled box: a brand
// header with live counts, a color-coded table, an optional detail pane
// and unreachable banner, and a key-hint footer.
func (m Model) View() string {
	lastErrW := m.lastErrorWidth()

	var b strings.Builder
	b.WriteString(brandStyle.Render("sluice · fleet dashboard"))
	b.WriteString("\n")
	b.WriteString(m.headerCounts())
	b.WriteString("\n\n")

	b.WriteString(m.tableHeader(lastErrW))
	b.WriteString("\n")
	if len(m.syncs) == 0 {
		b.WriteString(dimStyle.Render("(no syncs reported)"))
		b.WriteString("\n")
	}
	for i, s := range m.syncs {
		b.WriteString(m.renderRow(s, i == m.selected, lastErrW))
		b.WriteString("\n")
	}

	if m.connErr != nil {
		b.WriteString("\n")
		b.WriteString(bannerStyle.Render(fmt.Sprintf("⚠ %s unreachable — showing last known state", m.addr)))
		b.WriteString("\n")
	}

	b.WriteString("\n")
	b.WriteString(dimStyle.Render(fmt.Sprintf("q quit · ↑↓ select · enter detail · refresh %s", m.refresh)))

	out := boxStyle.Render(b.String())

	// The detail pane renders BELOW the box as a raw block rather than
	// inside it: a bordered box pads every line to a common width and
	// expands tabs, which would mangle a multiline last_error. Keeping it
	// outside preserves the error verbatim — the whole point of the pane
	// is to show the full, un-truncated record.
	if m.detailOpen && len(m.syncs) > 0 {
		out += "\n\n" + m.detailPane()
	}
	return out
}

// headerCounts renders the live total/running/failed line plus the
// generated-at stamp.
func (m Model) headerCounts() string {
	var running, failed int
	for _, s := range m.syncs {
		switch s.State {
		case "running":
			running++
		case "failed":
			failed++
		}
	}
	counts := fmt.Sprintf("%d total · %d running · %d failed", len(m.syncs), running, failed)

	gen := m.generatedAt
	if t, err := time.Parse(time.RFC3339, m.generatedAt); err == nil {
		gen = t.Format("15:04:05")
	}
	if gen == "" {
		gen = "—"
	}
	return counts + "    " + dimStyle.Render("generated "+gen)
}

// tableHeader renders the column-title row.
func (m Model) tableHeader(lastErrW int) string {
	hdr := strings.Join([]string{
		cell("STREAM ID", colID),
		cell("STATE", colState),
		cell("RESTARTS", colRestarts),
		cell("CONSEC", colConsec),
		cell("IN STATE", colInState),
		cell("LAST ERROR", lastErrW),
	}, " ")
	return headerStyle.Render(hdr)
}

// renderRow renders one sync. The selected row is highlighted with the
// brand background (plain cells, to avoid nesting ANSI inside the
// highlight); unselected rows color just the state cell.
func (m Model) renderRow(s fleetSync, selected bool, lastErrW int) string {
	idC := cell(s.ID, colID)
	stateC := cell(s.State, colState)
	restartsC := cell(strconv.Itoa(s.Restarts), colRestarts)
	consecC := cell(strconv.Itoa(s.ConsecutiveFailures), colConsec)
	inStateC := cell(humanizeSeconds(s.SecondsInState), colInState)
	errC := cell(oneLine(s.LastError), lastErrW)

	if selected {
		line := strings.Join([]string{idC, stateC, restartsC, consecC, inStateC, errC}, " ")
		return selectedStyle.Render(line)
	}
	stateC = stateStyle(s.State).Render(stateC)
	return strings.Join([]string{idC, stateC, restartsC, consecC, inStateC, errC}, " ")
}

// detailPane renders the full record for the selected sync — the fields
// the table truncates (the complete last_error) plus the timestamps.
func (m Model) detailPane() string {
	s := m.syncs[m.selected]
	lastErr := s.LastError
	if lastErr == "" {
		lastErr = "(none)"
	}
	lastStart := s.LastStart
	if lastStart == "" {
		lastStart = "(never)"
	}
	since := s.Since
	if since == "" {
		since = "—"
	}
	var b strings.Builder
	b.WriteString(headerStyle.Render("── detail: " + s.ID + " ──"))
	b.WriteString("\n")
	fmt.Fprintf(&b, "state            : %s\n", s.State)
	fmt.Fprintf(&b, "last start       : %s\n", lastStart)
	fmt.Fprintf(&b, "since            : %s\n", since)
	fmt.Fprintf(&b, "seconds in state : %.0f (%s)\n", s.SecondsInState, humanizeSeconds(s.SecondsInState))
	fmt.Fprintf(&b, "last error       : %s", lastErr)
	return b.String()
}

// lastErrorWidth computes the remaining width for the last-error column
// from the terminal width (or the default before the first resize).
func (m Model) lastErrorWidth() int {
	total := m.width
	if total <= 0 {
		total = defaultWidth
	}
	// Subtract the border (2) and horizontal padding (2).
	content := total - 4
	fixed := colID + colState + colRestarts + colConsec + colInState + colGaps
	w := content - fixed
	if w < minLastError {
		w = minLastError
	}
	return w
}

// humanizeSeconds renders a seconds-in-state float as a compact
// duration: "45s", "3m12s", or "1h05m".
func humanizeSeconds(s float64) string {
	if s < 0 {
		s = 0
	}
	d := time.Duration(s) * time.Second
	h := int(d.Hours())
	mins := int(d.Minutes()) % 60
	secs := int(d.Seconds()) % 60
	switch {
	case h > 0:
		return fmt.Sprintf("%dh%02dm", h, mins)
	case d >= time.Minute:
		return fmt.Sprintf("%dm%02ds", mins, secs)
	default:
		return fmt.Sprintf("%ds", secs)
	}
}

// oneLine collapses newlines/tabs to spaces so a multiline error
// doesn't break the table row (the full text shows in the detail pane).
func oneLine(s string) string {
	return strings.NewReplacer("\n", " ", "\r", " ", "\t", " ").Replace(s)
}

// cell clips s to display-width w (with an ellipsis when truncated) and
// right-pads it to exactly w.
func cell(s string, w int) string {
	s = clip(s, w)
	if pad := w - lipgloss.Width(s); pad > 0 {
		s += strings.Repeat(" ", pad)
	}
	return s
}

// clip truncates s to display-width w, appending an ellipsis when it
// had to cut.
func clip(s string, w int) string {
	if w <= 0 {
		return ""
	}
	if lipgloss.Width(s) <= w {
		return s
	}
	if w == 1 {
		return "…"
	}
	r := []rune(s)
	for len(r) > 0 && lipgloss.Width(string(r))+1 > w {
		r = r[:len(r)-1]
	}
	return string(r) + "…"
}
