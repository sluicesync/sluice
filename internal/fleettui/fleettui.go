// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Package fleettui implements the bubbletea model behind
// `sluice sync tui` (ADR-0125): a read-only, full-screen terminal view
// of a running fleet, polling a `sync run --dashboard-listen` server's
// `/api/fleet` JSON endpoint (ADR-0124) on a ticker and rendering it
// with lipgloss.
//
// The model is a pure HTTP client of the dashboard contract — it never
// touches the supervisor or the apply path. It defines its own structs
// to unmarshal `/api/fleet` (decoupled from internal/pipeline's
// unexported report types), so the JSON shape is the only contract
// between them. Update is a pure msg→(model, cmd) function so every
// state transition (data refresh, selection, quit, error banner) is
// unit-tested without a terminal; the bubbletea/lipgloss dependency is
// confined to this package and the wiring command.
package fleettui

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"sluicesync.dev/sluice/internal/diagnose"
)

// fleetReport is the `/api/fleet` JSON envelope. It mirrors the shape
// internal/pipeline serves (ADR-0124 §2) but is defined here so the TUI
// stays decoupled from the pipeline package — the JSON is the contract.
type fleetReport struct {
	GeneratedAt string      `json:"generated_at"`
	Syncs       []fleetSync `json:"syncs"`
}

// fleetSync is one supervised sync's row in the `/api/fleet` report.
type fleetSync struct {
	ID                  string  `json:"id"`
	State               string  `json:"state"`
	ConsecutiveFailures int     `json:"consecutive_failures"`
	Restarts            int     `json:"restarts"`
	LastError           string  `json:"last_error"`
	LastStart           string  `json:"last_start"`
	Since               string  `json:"since"`
	SecondsInState      float64 `json:"seconds_in_state"`
}

// FetchFunc retrieves the current fleet report from the dashboard API.
// Injected into the model so tests stub it; production uses the
// HTTP-backed fetcher from HTTPFetcher.
type FetchFunc func(ctx context.Context) (fleetReport, error)

// fleetMsg is delivered on a successful fetch — the fresh report.
type fleetMsg fleetReport

// errMsg is delivered when a fetch fails. The model keeps the prior
// rows on screen and raises the "unreachable" banner.
type errMsg struct{ err error }

// tickMsg is the refresh-ticker pulse; each one issues another fetch.
type tickMsg time.Time

// Model is the bubbletea model for the fleet TUI. It is the testable
// core: Update is pure and View renders deterministically from it.
type Model struct {
	// addr is the operator-facing connect address (for the banner).
	addr string
	// fetch retrieves the report; injectable for tests.
	fetch FetchFunc
	// refresh is the poll interval, also shown in the footer.
	refresh time.Duration

	syncs       []fleetSync
	generatedAt string
	selected    int
	detailOpen  bool
	connErr     error

	// width/height track the terminal size (WindowSizeMsg); 0 until the
	// first resize, in which case View falls back to a sensible default.
	width  int
	height int
}

// New builds a production Model: it normalizes connect into the
// `/api/fleet` URL, wires the HTTP fetcher, and records connect as the
// banner address. refresh is the poll interval (and the footer hint).
func New(connect string, refresh time.Duration) (Model, error) {
	endpoint, err := NormalizeURL(connect)
	if err != nil {
		return Model{}, err
	}
	return NewWithFetch(connect, refresh, HTTPFetcher(endpoint, fetchTimeout(refresh))), nil
}

// NewWithFetch builds a Model with an injected fetcher. The production
// path goes through New; tests construct the model directly with a stub
// fetch so Update/View are exercised without a real server.
func NewWithFetch(addr string, refresh time.Duration, fetch FetchFunc) Model {
	if refresh <= 0 {
		refresh = 2 * time.Second
	}
	return Model{
		addr:    addr,
		fetch:   fetch,
		refresh: refresh,
	}
}

// Init kicks off the first fetch. The periodic tick is armed by each
// fetch result (see Update), so refreshes are single-flight: a slow or
// failed fetch never stacks overlapping requests.
func (m Model) Init() tea.Cmd {
	return m.fetchCmd()
}

// Update is the pure state transition. It returns the next model and an
// optional command; bubbletea drives it, but every branch is unit-
// testable by calling it directly and asserting on the returned model
// and command.
func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case fleetMsg:
		m.syncs = msg.Syncs
		m.generatedAt = msg.GeneratedAt
		m.connErr = nil
		m.clampSelection()
		// A successful fetch re-arms the refresh ticker.
		return m, m.tickCmd()
	case errMsg:
		// Keep the last-known fleet on screen; raise the banner.
		m.connErr = msg.err
		return m, m.tickCmd()
	case tickMsg:
		// The ticker fired — fetch again.
		return m, m.fetchCmd()
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		return m, nil
	case tea.KeyMsg:
		return m.handleKey(msg)
	}
	return m, nil
}

// handleKey applies a keypress to the model. Split out of Update so the
// key map reads as one block.
func (m Model) handleKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "q", "ctrl+c":
		return m, tea.Quit
	case "up", "k":
		if m.selected > 0 {
			m.selected--
		}
	case "down", "j":
		if m.selected < len(m.syncs)-1 {
			m.selected++
		}
	case "enter":
		// Toggle the detail pane for the selected sync. No-op when the
		// fleet is empty (nothing to detail).
		if len(m.syncs) > 0 {
			m.detailOpen = !m.detailOpen
		}
	case "esc":
		m.detailOpen = false
	}
	return m, nil
}

// clampSelection keeps the selected index within the current row set so
// a shrinking fleet can't leave the cursor pointing past the end.
func (m *Model) clampSelection() {
	if m.selected < 0 {
		m.selected = 0
	}
	if m.selected >= len(m.syncs) {
		m.selected = len(m.syncs) - 1
	}
	if m.selected < 0 {
		m.selected = 0
	}
}

// fetchCmd runs the injected fetch and wraps the result as a fleetMsg
// (success) or errMsg (failure). The fetch carries its own timeout, so
// a background context is the right scope here.
func (m Model) fetchCmd() tea.Cmd {
	return func() tea.Msg {
		rep, err := m.fetch(context.Background())
		if err != nil {
			return errMsg{err: err}
		}
		return fleetMsg(rep)
	}
}

// tickCmd schedules the next refresh pulse.
func (m Model) tickCmd() tea.Cmd {
	return tea.Tick(m.refresh, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}

// fetchTimeout derives a per-request timeout from the refresh interval:
// long enough not to abort a healthy-but-slow dashboard, short enough
// that an unreachable one raises the banner promptly. Bounded to a few
// seconds either way.
func fetchTimeout(refresh time.Duration) time.Duration {
	const (
		floor = 2 * time.Second
		ceil  = 10 * time.Second
	)
	t := refresh
	if t < floor {
		t = floor
	}
	if t > ceil {
		t = ceil
	}
	return t
}

// HTTPFetcher returns a FetchFunc that GETs the `/api/fleet` endpoint
// and decodes the report. Every failure (build, transport, non-200,
// decode) is wrapped so the banner names what went wrong. timeout
// bounds each request.
func HTTPFetcher(endpoint string, timeout time.Duration) FetchFunc {
	client := &http.Client{Timeout: timeout}
	return func(ctx context.Context) (fleetReport, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, http.NoBody)
		if err != nil {
			return fleetReport{}, fmt.Errorf("build request: %w", err)
		}
		resp, err := client.Do(req)
		if err != nil {
			return fleetReport{}, fmt.Errorf("fetch %s: %w", endpoint, err)
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusOK {
			return fleetReport{}, fmt.Errorf("fetch %s: unexpected status %s", endpoint, resp.Status)
		}
		var rep fleetReport
		if err := json.NewDecoder(resp.Body).Decode(&rep); err != nil {
			return fleetReport{}, fmt.Errorf("decode /api/fleet response: %w", err)
		}
		return rep, nil
	}
}

// NormalizeURL turns a --connect value into the `/api/fleet` URL the
// model polls. It accepts three forms:
//   - "host:port"               → "http://host:port/api/fleet"
//   - "http(s)://host:port"     → "<scheme>://host:port/api/fleet"
//   - a full URL ending in /api/fleet → used as-is
//
// A missing scheme defaults to http (the dashboard is plain HTTP).
func NormalizeURL(connect string) (string, error) {
	s := strings.TrimSpace(connect)
	if s == "" {
		return "", errors.New("connect address is empty")
	}
	if !strings.Contains(s, "://") {
		s = "http://" + s
	}
	u, err := url.Parse(s)
	if err != nil {
		// Drop the raw connect address: url.Parse embeds it verbatim in
		// its error, and a connect address can carry basic-auth
		// userinfo. SafeParseError keeps just the reason.
		return "", fmt.Errorf("parse connect address: %w", diagnose.SafeParseError(err))
	}
	if u.Host == "" {
		return "", fmt.Errorf("connect address %q has no host", connect)
	}
	trimmed := strings.TrimRight(u.Path, "/")
	if !strings.HasSuffix(trimmed, "/api/fleet") {
		u.Path = trimmed + "/api/fleet"
	} else {
		u.Path = trimmed
	}
	return u.String(), nil
}
