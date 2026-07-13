// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package progress

import (
	"fmt"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
)

// ADR-0156: the CONTINUOUS live status panel for `sluice sync start`.
//
// This is the sibling of the one-shot [model] (ADR-0155). A continuous sync
// never completes on its own, so the checklist-then-summary shape is wrong:
// there is no terminal summary, and a run can last days. The [livePanel]
// therefore has a distinct contract — a persistent, in-place-updating view
// that never renders a terminal summary — while REUSING the one-shot model's
// initial-copy checklist + per-table bar (the [model.checklistView] delegate)
// and this package's brand styling.
//
// It is a pure msg->model [tea.Model] like the one-shot model, so every
// phase/CDC/event transition is teatest-covered without a terminal.
//
// Feed:
//   - The initial-copy checklist/bar is fed by the SAME [Sink] events the
//     shared bulk-copy phase already emits via [FromContext] (phaseStarted /
//     phaseCompleted / tableProgress / warn) — the [LiveTTYSink] forwards them
//     into the embedded [model].
//   - The CDC body (position, freshness, health) is fed by the CLI-side status
//     poller through [LiveTTYSink.EnterCDC] / [LiveTTYSink.Status] /
//     [LiveTTYSink.Health].
//   - WARN/ERROR records are forwarded (not buffered) into the bounded
//     recent-events ring via [LiveTTYSink.Event].

// maxLiveEvents bounds the recent-events ring so a days-long run's memory
// stays flat (ADR-0156 §"Log handling — surface, don't buffer"). Older events
// are dropped from the head; the running total is kept separately so the count
// a WARN storm produced stays visible even after the individual lines age out.
const maxLiveEvents = 100

// liveEventsShown is how many of the most-recent events the panel renders.
const liveEventsShown = 5

// liveHealth is the stream's connection health as the panel understands it.
type liveHealth int

const (
	// healthConnecting is the pre-CDC / pre-first-reading state.
	healthConnecting liveHealth = iota
	healthConnected
	healthReconnecting
)

func (h liveHealth) String() string {
	switch h {
	case healthConnected:
		return "connected"
	case healthReconnecting:
		return "reconnecting"
	default:
		return "connecting"
	}
}

// LiveHeader is the static identity the panel renders in its header row.
type LiveHeader struct {
	Source   string
	Target   string
	StreamID string
}

// LiveStatus is one poll of the CDC steady-state signal. It is deliberately
// the honest subset the target's control table can surface cross-engine:
//
//   - Position is the last-applied source-relative position token (opaque).
//   - Freshness is seconds-since-last-apply (now − control-row UpdatedAt), the
//     LOAD-BEARING cross-engine lag signal the panel leads with.
//   - Known is false until the poller has a real reading (the *Known honesty
//     contract — absence is "unknown", never a fabricated 0).
//
// It carries NO lag_bytes: that is a PG-pair-only quantity this cross-engine
// panel cannot compute, and ADR-0156 is explicit that the panel must not imply
// a byte-lag it cannot compute. Cumulative rows-applied / throughput are a
// named phase-1 gap (see the CDC body view) — the control table does not carry
// an applied-row counter, so the panel refuses to fabricate one.
type LiveStatus struct {
	Position  string
	Freshness time.Duration
	Known     bool
}

// liveEvent is one WARN/ERROR record surfaced live in the recent-events ring.
type liveEvent struct {
	level string // "WARN" or "ERROR"
	text  string
}

// The continuous-mode messages the [LiveTTYSink] translates the CLI-side
// feed into. The initial-copy checklist reuses the one-shot messages
// (phaseStartedMsg / phaseCompletedMsg / tableProgressMsg / warnMsg) which the
// embedded [model] already understands.
type (
	enterCDCMsg struct{}
	statusMsg   struct{ status LiveStatus }
	healthMsg   struct {
		state    liveHealth
		restarts int
	}
	eventMsg struct {
		level string
		text  string
	}
	// stopRequestedMsg carries the result of the drain-and-stop side effect
	// (the [tea.Cmd] the panel returns on q/ctrl+c). A non-nil err means the
	// RequestStop write failed; the panel surfaces it in the footer but keeps
	// rendering (the CLI still cancels as a backstop).
	stopRequestedMsg struct{ err error }
)

// livePanel is the continuous bubbletea model behind `sync start`'s live view.
// Update is pure (msg->model); all I/O (the drain-and-stop RequestStop, the
// status poll) lives outside it — injected as stopCmd or delivered as msgs —
// so the renderer stays isolated from the sync goroutine (ADR-0156: a renderer
// failure must never abort the stream).
type livePanel struct {
	header LiveHeader

	// copy is the reused one-shot model rendering the initial-copy checklist +
	// per-table bar (with ADR-0155's est-exceeded clamp). Only its
	// checklistView is used; its summary/quit paths are never driven here.
	copy model

	cdc      bool // flipped once the snapshot hands off to CDC
	status   LiveStatus
	health   liveHealth
	restarts int

	// events is the bounded recent-events ring (last maxLiveEvents). eventTotal
	// is the monotonic running counter kept visible after lines age out.
	events     []liveEvent
	eventTotal int

	// stopping is set on the first q/ctrl+c; the panel then renders a draining
	// footer and returns stopCmd (the graceful RequestStop). A second q/ctrl+c
	// forces an immediate quit (the CLI cancels the run context as the backstop).
	stopping bool
	forced   bool
	stopErr  error

	// stopCmd is the injected drain-and-stop side effect returned on the first
	// q/ctrl+c. nil in pure-model tests that only assert the transition.
	stopCmd tea.Cmd

	width int
}

// newLivePanel builds the initial continuous model. spec parameterizes the
// initial-copy checklist (reuse [MigrateProgressSpec]'s phases); header is the
// static identity; stopCmd is the drain-and-stop side effect (may be nil).
func newLivePanel(spec Spec, header LiveHeader, stopCmd tea.Cmd, now func() time.Time) livePanel {
	return livePanel{
		header:  header,
		copy:    newModel(spec, now(), now),
		health:  healthConnecting,
		stopCmd: stopCmd,
	}
}

func (m livePanel) Init() tea.Cmd { return nil }

// Update is the pure state transition.
func (m livePanel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case phaseStartedMsg, phaseCompletedMsg, tableProgressMsg:
		// Delegate the initial-copy checklist/bar to the embedded one-shot
		// model. Its Update never returns a quit for these messages.
		next, _ := m.copy.Update(msg)
		m.copy = next.(model)
		return m, nil
	case warnMsg:
		// A cold-start warning (degraded FK, dropped collation) is both a
		// checklist warning AND a live event.
		next, _ := m.copy.Update(msg)
		m.copy = next.(model)
		m.pushEvent("WARN", msg.text)
		return m, nil
	case enterCDCMsg:
		m.cdc = true
		return m, nil
	case statusMsg:
		m.status = msg.status
		if msg.status.Known {
			// A known reading means CDC is applying (or a warm-resume started
			// straight in CDC with no cold-copy phases): flip the mode.
			m.cdc = true
		}
		return m, nil
	case healthMsg:
		m.health = msg.state
		m.restarts = msg.restarts
		return m, nil
	case eventMsg:
		m.pushEvent(msg.level, msg.text)
		return m, nil
	case stopRequestedMsg:
		m.stopErr = msg.err
		return m, nil
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.copy.width = msg.Width
		return m, nil
	case tea.KeyMsg:
		return m.handleKey(msg)
	}
	return m, nil
}

// handleKey implements the drain-and-stop keybinding contract. In raw mode the
// terminal delivers ctrl+c as a KeyMsg (not a process SIGINT), so it is handled
// here alongside `q`:
//
//   - first press: request a GRACEFUL drain (return stopCmd — the RequestStop
//     that trips the streamer's stop-signal poll so in-flight changes drain)
//     and keep rendering a draining footer;
//   - second press: force an immediate quit (the CLI cancels the run context
//     as the backstop for a wedged drain).
func (m livePanel) handleKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	if msg.Type != tea.KeyCtrlC && msg.String() != "q" {
		return m, nil
	}
	if m.stopping {
		m.forced = true
		return m, tea.Quit
	}
	m.stopping = true
	return m, m.stopCmd
}

// pushEvent appends to the bounded recent-events ring and bumps the running
// total. The ring is trimmed to maxLiveEvents so memory is flat over a
// days-long run (ADR-0156).
func (m *livePanel) pushEvent(level, text string) {
	m.eventTotal++
	m.events = append(m.events, liveEvent{level: level, text: oneLine(text)})
	if len(m.events) > maxLiveEvents {
		m.events = m.events[len(m.events)-maxLiveEvents:]
	}
}

// View renders the persistent panel. Unlike the one-shot model it NEVER returns
// "" / a terminal summary: a continuous run's view is live for the process
// lifetime. On quit the [LiveTTYSink] prints a final static line after the
// program releases the terminal.
func (m livePanel) View() string {
	var b strings.Builder
	b.WriteString(brandStyle.Render("sluice sync"))
	b.WriteString("  ")
	b.WriteString(dimStyle.Render(m.headerLine()))
	b.WriteString("\n\n")

	if m.cdc {
		b.WriteString(m.cdcBody())
	} else {
		b.WriteString(activeStyle.Render("  mode: initial copy"))
		b.WriteString("\n\n")
		b.WriteString(m.copy.checklistView())
	}

	b.WriteString(m.eventsView())
	b.WriteString("\n")
	b.WriteString(m.footer())
	return b.String()
}

// headerLine renders "source -> target · stream <id>".
func (m livePanel) headerLine() string {
	route := fmt.Sprintf("%s -> %s", m.header.Source, m.header.Target)
	if m.header.StreamID != "" {
		return route + "  ·  stream " + m.header.StreamID
	}
	return route
}

// cdcBody renders the steady-state CDC signals: mode, last-applied position,
// freshness (the load-bearing lag signal, led with), and health.
//
// Rows-applied / throughput are a NAMED phase-1 gap: the target control table
// carries no applied-row counter, and per the loud-failure discipline the panel
// refuses to fabricate one — it shows "n/a" with the reason rather than an
// invented number (the same posture as the omitted PG-only lag_bytes). Wiring a
// truthful applied-row counter is deferred to a follow-up (ADR-0156 phase 2/3).
func (m livePanel) cdcBody() string {
	var b strings.Builder
	b.WriteString(okStyle.Render("  mode: CDC"))
	b.WriteString("\n\n")

	pos := m.status.Position
	if pos == "" {
		pos = "—"
	} else {
		pos = clipLine(pos, 60)
	}
	fmt.Fprintf(&b, "  %-12s %s\n", "position", pos)

	fresh := "—"
	if m.status.Known {
		fresh = humanFreshness(m.status.Freshness) + " since last apply"
	}
	fmt.Fprintf(&b, "  %-12s %s\n", "freshness", fresh)

	// Honest gap: no applied-row counter is surfaced by the control table.
	fmt.Fprintf(&b, "  %-12s %s\n", "rows", dimStyle.Render("n/a (phase 1)"))

	health := m.health.String()
	if m.restarts > 0 {
		health = fmt.Sprintf("%s (%d reconnect(s))", health, m.restarts)
	}
	fmt.Fprintf(&b, "  %-12s %s\n", "health", m.healthStyled(health))
	return b.String()
}

func (m livePanel) healthStyled(s string) string {
	switch m.health {
	case healthConnected:
		return okStyle.Render(s)
	case healthReconnecting:
		return warnStyle.Render(s)
	default:
		return dimStyle.Render(s)
	}
}

// eventsView renders the bounded recent-events region plus the running total.
func (m livePanel) eventsView() string {
	var b strings.Builder
	b.WriteString("\n")
	b.WriteString(headerStyle.Render(fmt.Sprintf("  recent events (%d total)", m.eventTotal)))
	b.WriteString("\n")
	if len(m.events) == 0 {
		b.WriteString(dimStyle.Render("    (none)"))
		b.WriteString("\n")
		return b.String()
	}
	start := 0
	if len(m.events) > liveEventsShown {
		start = len(m.events) - liveEventsShown
	}
	for _, e := range m.events[start:] {
		line := clipLine(e.text, m.eventWidth())
		fmt.Fprintf(&b, "    %s %s\n", warnStyle.Render(e.level), line)
	}
	return b.String()
}

// footer renders the keybindings, or the draining state after a stop request.
func (m livePanel) footer() string {
	if m.stopping {
		msg := "draining in-flight changes — press ctrl+c again to force-quit"
		if m.stopErr != nil {
			msg = "stop request failed: " + oneLine(m.stopErr.Error()) + " — press ctrl+c again to force-quit"
		}
		return warnStyle.Render("  " + msg)
	}
	return dimStyle.Render("  q / ctrl+c  drain and stop")
}

// eventWidth is the max width an event line may occupy before truncation,
// derived from the terminal width. Falls back to a sane default before the
// first WindowSizeMsg (e.g. under teatest).
func (m livePanel) eventWidth() int {
	w := m.width
	if w <= 0 {
		w = 100
	}
	maxw := w - 14
	if maxw < 20 {
		maxw = 20
	}
	return maxw
}

// humanFreshness renders a seconds-since-last-apply duration compactly:
// "3s", "2m10s", "1h04m".
func humanFreshness(d time.Duration) string {
	if d < 0 {
		d = 0
	}
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
