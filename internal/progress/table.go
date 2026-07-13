// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package progress

import (
	"strings"

	"github.com/charmbracelet/lipgloss"
	"github.com/charmbracelet/lipgloss/table"
)

// Table renders an on-brand, rounded-border grid for the TTY pretty view:
// a brand-coloured title above a bordered table with a bold header row.
//
// activeCol, when >= 0, names a column whose "yes"/"true" cells render in
// the success colour and everything else in the dim colour — used by
// `slot list`'s ACTIVE column so an operator can see at a glance which
// slots have a live consumer. Pass -1 to style every body cell plainly.
//
// This is a TTY-only presentation helper: callers invoke it only on the
// pretty path (gated by wantPrettyProgress). The non-TTY path keeps its
// own plain renderer so piped / CI / --log-format=json output stays
// byte-identical to prior releases.
func Table(title string, headers []string, rows [][]string, activeCol int) string {
	t := table.New().
		Border(lipgloss.RoundedBorder()).
		BorderStyle(lipgloss.NewStyle().Foreground(brandColor)).
		Headers(headers...).
		Rows(rows...).
		StyleFunc(func(row, col int) lipgloss.Style {
			switch {
			case row == table.HeaderRow:
				return headerStyle.Padding(0, 1)
			case activeCol >= 0 && col == activeCol && row >= 0 && row < len(rows):
				if v := strings.ToLower(rows[row][col]); v == "yes" || v == "true" {
					return okStyle.Padding(0, 1)
				}
				return dimStyle.Padding(0, 1)
			default:
				return lipgloss.NewStyle().Padding(0, 1)
			}
		})
	return brandStyle.Render(title) + "\n" + t.Render()
}
