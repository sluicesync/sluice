// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"sluicesync.dev/sluice/internal/fleettui"
)

// SyncTuiCmd implements `sluice sync tui` (ADR-0125): a full-screen,
// read-only terminal dashboard that polls a running `sync run
// --dashboard-listen` server's /api/fleet JSON endpoint (ADR-0124) and
// renders the fleet live. It is a pure HTTP client — no mutation, no
// supervisor coupling — so it composes with the already-shipped
// dashboard and works against a remote fleet over an SSH tunnel.
type SyncTuiCmd struct {
	Connect string        `help:"host:port or URL of a running 'sync run --dashboard-listen' server (e.g. ':9300', 'localhost:9300', 'http://host:9300', or a full '.../api/fleet' URL). The TUI polls its /api/fleet endpoint." required:"" placeholder:"ADDR"`
	Refresh time.Duration `help:"How often to poll /api/fleet for a fresh fleet view." default:"2s" placeholder:"DUR"`
}

// Run builds the bubbletea model over the production HTTP fetcher and
// runs the full-screen program. The TUI requires an interactive
// terminal; when stdout is not a TTY, bubbletea returns an error which
// is wrapped with a pointer to the non-interactive alternatives.
func (c *SyncTuiCmd) Run(_ *Globals) error {
	model, err := fleettui.New(c.Connect, c.Refresh)
	if err != nil {
		return fmt.Errorf("--connect: %w", err)
	}
	p := tea.NewProgram(model, tea.WithAltScreen(), tea.WithContext(kongContext()))
	if _, err := p.Run(); err != nil {
		return fmt.Errorf("sync tui: %w (the TUI needs an interactive terminal; "+
			"without one, use the web dashboard via 'sync run --dashboard-listen' "+
			"or the one-shot 'sync status --all')", err)
	}
	return nil
}
