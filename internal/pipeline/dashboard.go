// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Read-only fleet web dashboard for `sluice sync run --dashboard-listen
// ADDR` (ADR-0124). A single self-contained HTML page plus a JSON API
// (`/api/fleet`) rendering the live fleet's health, both backed PURELY by
// the supervisor's existing [Supervisor.Snapshot]. No data path, no
// mutation, no controls — strictly GET, strictly observability.
//
// **Failure-isolated, like the metrics server.** Constructed and Started
// alongside the supervisor in the `sync run` wiring; mirrors
// [MetricsServer]'s lifecycle (synchronous bind in Start, background
// Serve, 5s-grace Shutdown in Close). A bind failure is surfaced to the
// caller before the fleet starts (the CLI treats it as loud-fatal, since
// an operator who asked for the dashboard wants to know it didn't bind);
// once serving, the dashboard can NEVER affect a running sync.
//
// **No new dependency.** Hand-rendered JSON (encoding/json) + a single
// embedded HTML/CSS/JS file (`go:embed`). Mirrors the metrics server's
// stdlib-only precedent.

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"
)

// dashboardHTML is the embedded single-page dashboard served at `/`. It
// is self-contained (inline CSS + vanilla JS, no external/CDN assets, no
// build step) and polls /api/fleet client-side to refresh.
//
//go:embed dashboard.html
var dashboardHTML []byte

// fleetSnapshotter is the tiny read-only seam the dashboard server reads
// the fleet through, so it can be unit-tested without booting a real
// supervisor. *Supervisor already satisfies it via [Supervisor.Snapshot].
type fleetSnapshotter interface {
	Snapshot() []SyncStatusSnapshot
}

// DashboardServer serves the read-only fleet dashboard (`/`), its backing
// JSON API (`/api/fleet`), and a liveness probe (`/healthz`) over a tiny
// stdlib HTTP server. The CLI creates one when `sync run` is given a
// non-empty `--dashboard-listen` and Closes it on fleet teardown.
type DashboardServer struct {
	addr   string
	snap   fleetSnapshotter
	server *http.Server

	// clock sources "now" for the seconds_in_state computation. Defaults
	// to time.Now; injectable so tests can pin a deterministic elapsed
	// time against a fixture snapshot's Since timestamp.
	clock func() time.Time
}

// NewDashboardServer wires the HTTP server over the given supervisor
// snapshot source. Does NOT start listening — call Start to begin
// serving. Returns an error when the address is empty or the snapshotter
// is nil.
func NewDashboardServer(addr string, snap fleetSnapshotter) (*DashboardServer, error) {
	if addr == "" {
		return nil, errors.New("DashboardServer: addr is empty")
	}
	if snap == nil {
		return nil, errors.New("DashboardServer: snapshotter is nil")
	}
	mux := http.NewServeMux()
	ds := &DashboardServer{
		addr:  addr,
		snap:  snap,
		clock: time.Now,
	}
	mux.HandleFunc("/", ds.handleIndex)
	mux.HandleFunc("/api/fleet", ds.handleFleet)
	mux.HandleFunc("/healthz", ds.handleHealthz)
	ds.server = &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       30 * time.Second,
	}
	return ds, nil
}

// Start binds the listener and serves in a background goroutine. Returns
// when the listener is bound (so the caller knows the address is
// reserved) or fails to bind. Use Close to stop the server cleanly.
func (d *DashboardServer) Start() error {
	// Background context: the listener outlives any single request, and
	// Close() drives the shutdown. Lint guidance is to use
	// ListenConfig.Listen rather than net.Listen for context-aware dial;
	// the listener itself isn't ctx-cancellable but giving it a
	// background context satisfies the linter cleanly. (Mirrors
	// MetricsServer.Start.)
	lc := &net.ListenConfig{}
	ln, err := lc.Listen(context.Background(), "tcp", d.addr)
	if err != nil {
		return fmt.Errorf("dashboard: listen %s: %w", d.addr, err)
	}
	go func() {
		// Serve blocks until Close is called or an unrecoverable error
		// occurs. We don't surface ErrServerClosed (the expected
		// teardown signal); other errors are swallowed since a
		// dashboard-server failure shouldn't kill the supervisor — it is
		// strictly an observability surface.
		_ = d.server.Serve(ln)
	}()
	return nil
}

// Close shuts down the HTTP server with a 5s grace period.
func (d *DashboardServer) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return d.server.Shutdown(ctx)
}

// handleIndex serves the embedded dashboard page at `/`. Non-root paths
// (anything the mux falls through to "/" for) get a 404 so a stray
// request doesn't get the page under a misleading URL.
func (d *DashboardServer) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(dashboardHTML)
}

// fleetReport is the `/api/fleet` JSON envelope: a generation timestamp
// plus the per-sync rows. A stable, documented shape (ADR-0124 §2) so the
// embedded page — or any external poller — can consume it.
type fleetReport struct {
	GeneratedAt string          `json:"generated_at"`
	Syncs       []fleetSyncView `json:"syncs"`
}

// fleetSyncView is one supervised sync's row in the JSON report, derived
// purely from a [SyncStatusSnapshot]. Timestamps render as RFC3339, or
// the empty string for the zero time (a not-yet-started sync) rather than
// the misleading "0001-01-01T..." zero-time string. seconds_in_state is
// the wall-clock seconds the sync has been in its current State.
type fleetSyncView struct {
	ID                  string  `json:"id"`
	State               string  `json:"state"`
	ConsecutiveFailures int     `json:"consecutive_failures"`
	Restarts            int     `json:"restarts"`
	LastError           string  `json:"last_error"`
	LastStart           string  `json:"last_start"`
	Since               string  `json:"since"`
	SecondsInState      float64 `json:"seconds_in_state"`
}

// handleFleet is the GET /api/fleet handler: it snapshots the supervisor
// and renders the fleet as JSON. Pure read — never touches the apply
// path.
func (d *DashboardServer) handleFleet(w http.ResponseWriter, _ *http.Request) {
	now := d.clock()
	snaps := d.snap.Snapshot()
	report := fleetReport{
		GeneratedAt: now.Format(time.RFC3339),
		Syncs:       make([]fleetSyncView, 0, len(snaps)),
	}
	for _, s := range snaps {
		report.Syncs = append(report.Syncs, fleetSyncView{
			ID:                  s.ID,
			State:               string(s.State),
			ConsecutiveFailures: s.ConsecutiveFailures,
			Restarts:            s.Restarts,
			LastError:           s.LastError,
			LastStart:           rfc3339OrEmpty(s.LastStart),
			Since:               rfc3339OrEmpty(s.Since),
			SecondsInState:      now.Sub(s.Since).Seconds(),
		})
	}
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	_ = enc.Encode(report)
}

// rfc3339OrEmpty renders t as RFC3339, or "" for the zero time so a
// not-yet-started sync's empty timestamps don't serialize as the
// misleading zero-time string.
func rfc3339OrEmpty(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format(time.RFC3339)
}

// handleHealthz is the liveness probe — "is the dashboard process
// responsive?". Returns 200 "ok" unconditionally; doesn't touch the
// supervisor. Mirrors the metrics server's healthz so the dashboard port
// is probe-able.
func (d *DashboardServer) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = io.WriteString(w, "ok\n")
}
