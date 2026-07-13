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
	// Mode is the panel's brand/mode word rendered after "sluice " in the
	// header (e.g. "sync", "broker", "backup stream", "metrics-watch").
	// Empty defaults to "sync" so the phase-1 `sync start` panel's header is
	// byte-identical.
	Mode string

	Source   string
	Target   string
	StreamID string

	// Detail, when non-empty, REPLACES the "source -> target · stream <id>"
	// route in the header line — for the readout-mode panels whose identity
	// isn't a source→target pair (e.g. metrics-watch's "watching <db> ·
	// <org>"). Empty keeps the route, so `sync start`'s header is unchanged.
	Detail string
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
// a byte-lag it cannot compute.
//
// RowsApplied is the lifetime cumulative row-level-DML-applied counter the
// target control table now persists ([ir.StreamStatus.RowsApplied]), valid
// only when Known (the same control-row reading). Throughput is NOT carried
// here — the panel computes it itself from the delta between successive
// RowsApplied readings and their PolledAt instants, so a single reading never
// implies a rate.
type LiveStatus struct {
	Position  string
	Freshness time.Duration
	Known     bool

	// RowsApplied is the cumulative rows-applied count from this reading
	// (valid only when Known). 0 is honest for a legacy pre-counter row or a
	// stream that has applied no rows yet.
	RowsApplied int64

	// PolledAt is the wall-clock instant this reading was taken. It is the
	// denominator clock for the panel's throughput (rows/s = Δrows /
	// Δ(PolledAt)); carried in the message rather than read from a clock so
	// the pure-model panel stays teatest-deterministic. Zero disables the
	// throughput computation for this reading (no fabricated rate).
	PolledAt time.Time
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
	// readoutMsg carries one refresh of the GENERIC mode-appropriate readout
	// body (ADR-0156 phases 2/3): an ordered label/value list the panel
	// renders in place of the CDC-specific [LiveStatus] body. Used by the
	// broker / backup-stream / metrics-watch panels, whose steady-state
	// signals are a different shape than `sync start`'s position/freshness.
	readoutMsg struct{ fields []Field }
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

	// readoutMode marks a GENERIC-readout panel (broker / backup-stream /
	// metrics-watch, ADR-0156 phases 2/3): it never renders the
	// `sync start` initial-copy checklist or the CDC-specific body — its
	// body is the ordered label/value [readout] list. False for the phase-1
	// `sync start` panel, keeping its View byte-identical.
	readoutMode bool
	readout     []Field
	haveReadout bool

	cdc      bool // flipped once the snapshot hands off to CDC
	status   LiveStatus
	health   liveHealth
	restarts int

	// Throughput is computed panel-side from successive rows-applied readings
	// (Δrows / Δ(PolledAt)), so a single reading never implies a rate. lastRows
	// / lastRowsAt hold the previous reading's baseline; haveLastRows guards
	// the first reading. rate / haveRate hold the last GOOD rate: a
	// non-monotonic reading (a stream reset / ClearStream drops the counter)
	// re-baselines without fabricating a negative spike, keeping the last good
	// rate (or a dash if none yet).
	lastRows     int64
	lastRowsAt   time.Time
	haveLastRows bool
	rate         float64
	haveRate     bool

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

// newReadoutPanel builds a GENERIC-readout continuous panel (ADR-0156 phases
// 2/3) for the broker / backup-stream / metrics-watch commands. It shares the
// header / events / footer chrome and the drain-and-stop keybinding with the
// `sync start` panel, but renders an ordered label/value [readout] body
// instead of the initial-copy checklist / CDC-specific body. No [Spec] is
// needed (there is no initial-copy phase list).
func newReadoutPanel(header LiveHeader, stopCmd tea.Cmd, now func() time.Time) livePanel {
	m := newLivePanel(Spec{}, header, stopCmd, now)
	m.readoutMode = true
	return m
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
		m.updateThroughput(msg.status)
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
	case readoutMsg:
		m.readout = msg.fields
		m.haveReadout = true
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

// updateThroughput folds one status reading into the panel-side rows/s
// computation. It computes Δrows / Δ(PolledAt) against the previous reading,
// guarding: the first reading (no baseline yet → no rate), a missing poll
// timestamp (either side zero → skip; never divide by a bogus interval), a
// non-positive time delta (skip), and a NON-MONOTONIC rows delta (a stream
// reset / ClearStream drops the counter → do NOT show a negative or a bogus
// spike; keep the last good rate and re-baseline). It always advances the
// baseline to the current reading so the next delta is measured from here.
func (m *livePanel) updateThroughput(s LiveStatus) {
	if !s.Known {
		return
	}
	if m.haveLastRows && !m.lastRowsAt.IsZero() && !s.PolledAt.IsZero() && s.PolledAt.After(m.lastRowsAt) {
		dRows := s.RowsApplied - m.lastRows
		dt := s.PolledAt.Sub(m.lastRowsAt).Seconds()
		if dRows >= 0 && dt > 0 {
			m.rate = float64(dRows) / dt
			m.haveRate = true
		}
		// dRows < 0: counter regressed (stream reset) — keep the last good
		// rate (or none) and fall through to re-baseline.
	}
	m.lastRows = s.RowsApplied
	m.lastRowsAt = s.PolledAt
	m.haveLastRows = true
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
	b.WriteString(brandStyle.Render(m.brandLine()))
	b.WriteString("  ")
	b.WriteString(dimStyle.Render(m.headerLine()))
	b.WriteString("\n\n")

	switch {
	case m.readoutMode:
		b.WriteString(m.readoutBody())
	case m.cdc:
		b.WriteString(m.cdcBody())
	default:
		b.WriteString(activeStyle.Render("  mode: initial copy"))
		b.WriteString("\n\n")
		b.WriteString(m.copy.checklistView())
	}

	b.WriteString(m.eventsView())
	b.WriteString("\n")
	b.WriteString(m.footer())
	return b.String()
}

// brandLine renders the "sluice <mode>" brand. Mode defaults to "sync" so the
// phase-1 `sync start` panel's brand ("sluice sync") is byte-identical.
func (m livePanel) brandLine() string {
	mode := m.header.Mode
	if mode == "" {
		mode = "sync"
	}
	return "sluice " + mode
}

// headerLine renders the panel's identity: the operator-supplied Detail when
// set (the readout panels' free-form line), else "source -> target · stream
// <id>".
func (m livePanel) headerLine() string {
	if m.header.Detail != "" {
		return m.header.Detail
	}
	route := fmt.Sprintf("%s -> %s", m.header.Source, m.header.Target)
	if m.header.StreamID != "" {
		return route + "  ·  stream " + m.header.StreamID
	}
	return route
}

// readoutBody renders the GENERIC mode-appropriate body (ADR-0156 phases
// 2/3): an ordered label/value list. Before the first [readoutMsg] arrives it
// shows a dimmed "starting…" placeholder (these panels have no initial-copy
// checklist to fill the gap).
func (m livePanel) readoutBody() string {
	var b strings.Builder
	if !m.haveReadout {
		b.WriteString(dimStyle.Render("  starting…"))
		b.WriteString("\n")
		return b.String()
	}
	for _, f := range m.readout {
		fmt.Fprintf(&b, "  %-14s %s\n", f.Label, clipLine(f.Value, 72))
	}
	return b.String()
}

// cdcBody renders the steady-state CDC signals: mode, last-applied position,
// freshness (the load-bearing lag signal, led with), rows-applied +
// throughput, and health.
//
// Rows: the cumulative row-level-DML-applied counter the control table now
// persists ([ir.StreamStatus.RowsApplied]) plus a panel-computed throughput
// (rows/s, from successive readings). The *Known honesty contract holds — with
// no reading yet the panel shows "—", never a fabricated 0; the rate is shown
// only once two readings bound a positive interval (see updateThroughput).
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

	fmt.Fprintf(&b, "  %-12s %s\n", "rows", m.rowsLine())

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

// rowsLine renders the rows-applied field: the cumulative count plus, once a
// rate is known, a "(N rows/s)" throughput. Honors the *Known contract — with
// no reading yet it shows "—" (dimmed), never a fabricated 0.
func (m livePanel) rowsLine() string {
	if !m.status.Known {
		return dimStyle.Render("—")
	}
	line := formatCount(m.status.RowsApplied)
	if m.haveRate {
		line += "  " + dimStyle.Render(fmt.Sprintf("(%s)", formatRate(m.rate)))
	}
	return line
}

// formatRate renders a rows/second throughput: one decimal below 10 rows/s
// (so a slow trickle isn't rounded to "0 rows/s"), whole numbers above.
func formatRate(r float64) string {
	if r < 10 {
		return fmt.Sprintf("%.1f rows/s", r)
	}
	return fmt.Sprintf("%.0f rows/s", r)
}

// formatCount renders an int64 with thousands separators ("1,234,567") for
// readability in the panel. Handles the (not-expected) negative case for
// completeness.
func formatCount(n int64) string {
	neg := n < 0
	if neg {
		n = -n
	}
	digits := fmt.Sprintf("%d", n)
	var out strings.Builder
	if neg {
		out.WriteByte('-')
	}
	pre := len(digits) % 3
	if pre == 0 {
		pre = 3
	}
	out.WriteString(digits[:pre])
	for i := pre; i < len(digits); i += 3 {
		out.WriteByte(',')
		out.WriteString(digits[i : i+3])
	}
	return out.String()
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
