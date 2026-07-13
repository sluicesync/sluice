// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package progress

import (
	"strings"

	"github.com/charmbracelet/lipgloss"
)

// Brand palette — the sluicesync.com / fleet-dashboard colours (ADR-0125
// §2), mirrored here (rather than importing internal/fleettui's
// unexported styles) so the progress view stays on-brand without widening
// fleettui's API or coupling the two TUIs' internals. Keep the hex values
// in sync with internal/fleettui/view.go if the brand palette ever moves.
var (
	brandColor = lipgloss.Color("#F35815") // primary
	deepColor  = lipgloss.Color("#C0410A") // deep accent
	greyColor  = lipgloss.Color("#6b7280") // dim
	okColor    = lipgloss.Color("#1a7f37") // done / success green
	warnColor  = lipgloss.Color("#b7791f") // warnings amber
)

var (
	brandStyle  = lipgloss.NewStyle().Foreground(brandColor).Bold(true)
	headerStyle = lipgloss.NewStyle().Foreground(deepColor).Bold(true)
	dimStyle    = lipgloss.NewStyle().Foreground(greyColor)
	okStyle     = lipgloss.NewStyle().Foreground(okColor).Bold(true)
	warnStyle   = lipgloss.NewStyle().Foreground(warnColor)
	activeStyle = lipgloss.NewStyle().Foreground(brandColor)
	boxStyle    = lipgloss.NewStyle().Border(lipgloss.RoundedBorder()).BorderForeground(brandColor).Padding(0, 1)
)

// ASCII-only status marks (the ADR-0155 sibling of the v0.99.232 tofu
// fix): NEVER a glyph the default Windows console font lacks. "[ok]" for
// done, "[..]" for in-progress, "[  ]" for pending. Note the fleet
// dashboard's rounded-border box IS used — its box-drawing runes are in
// the default console font and were never part of the tofu class; only
// status/spinner glyphs (checkmarks, the reload arrow) were.
const (
	markDone    = "[ok]"
	markActive  = "[..]"
	markPending = "[  ]"
	// barFill / barEmpty build the progress bar out of pure ASCII so it
	// renders on every terminal ("#####-----").
	barFill  = "#"
	barEmpty = "-"
)

// renderBar draws an ASCII progress bar of the given inner width for a
// fraction in [0,1]. width is the number of cells between the brackets;
// a width <= 0 yields just the brackets.
func renderBar(frac float64, width int) string {
	if frac < 0 {
		frac = 0
	}
	if frac > 1 {
		frac = 1
	}
	if width < 0 {
		width = 0
	}
	filled := int(frac * float64(width))
	if filled > width {
		filled = width
	}
	return "[" + strings.Repeat(barFill, filled) + strings.Repeat(barEmpty, width-filled) + "]"
}
