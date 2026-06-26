// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// fakeSnapshotter is a static [fleetSnapshotter] returning a fixed fleet,
// so the dashboard handlers can be pinned without booting a real
// supervisor.
type fakeSnapshotter struct {
	snaps []SyncStatusSnapshot
}

func (f fakeSnapshotter) Snapshot() []SyncStatusSnapshot { return f.snaps }

// dashboardFixture builds a 3-sync fleet in varied states, including one
// failed sync whose last error carries HTML-special / quote / unicode
// characters (the XSS + JSON round-trip surface), and one not-yet-started
// sync with zero-time LastStart/Since.
func dashboardFixture(base time.Time) []SyncStatusSnapshot {
	return []SyncStatusSnapshot{
		{
			ID:        "orders-prod",
			State:     SyncRunning,
			Restarts:  2,
			LastStart: base.Add(-10 * time.Minute),
			Since:     base.Add(-10 * time.Minute),
		},
		{
			ID:                  "users-eu",
			State:               SyncBackoff,
			ConsecutiveFailures: 3,
			Restarts:            5,
			LastError:           "dial tcp: connection refused",
			LastStart:           base.Add(-2 * time.Minute),
			Since:               base.Add(-15 * time.Second),
		},
		{
			ID:                  "audit-<svc>",
			State:               SyncFailed,
			ConsecutiveFailures: 8,
			Restarts:            8,
			LastError:           `boom: <script>alert("x")</script> — naïve "quotes" & ümlauts`,
			// LastStart left zero: a sync that never entered running.
			Since: base.Add(-90 * time.Second),
		},
	}
}

// fixedClock returns a clock func pinned to t.
func fixedClock(t time.Time) func() time.Time {
	return func() time.Time { return t }
}

func TestNewDashboardServer_Errors(t *testing.T) {
	snap := fakeSnapshotter{}
	if _, err := NewDashboardServer("", snap); err == nil {
		t.Error("expected error for empty addr; got nil")
	}
	if _, err := NewDashboardServer(":9300", nil); err == nil {
		t.Error("expected error for nil snapshotter; got nil")
	}
}

func TestDashboard_FleetJSON(t *testing.T) {
	now := time.Date(2026, 6, 26, 12, 0, 0, 0, time.UTC)
	ds, err := NewDashboardServer(":9300", fakeSnapshotter{snaps: dashboardFixture(now)})
	if err != nil {
		t.Fatalf("NewDashboardServer: %v", err)
	}
	ds.clock = fixedClock(now)

	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/api/fleet", http.NoBody)
	ds.handleFleet(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}

	var got fleetReport
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode JSON: %v\nbody: %s", err, rec.Body.String())
	}

	if got.GeneratedAt != now.Format(time.RFC3339) {
		t.Errorf("generated_at = %q, want %q", got.GeneratedAt, now.Format(time.RFC3339))
	}
	if len(got.Syncs) != 3 {
		t.Fatalf("len(syncs) = %d, want 3", len(got.Syncs))
	}

	// Sync 0: running, both timestamps present, seconds_in_state = 600.
	s0 := got.Syncs[0]
	if s0.ID != "orders-prod" || s0.State != "running" {
		t.Errorf("sync0 id/state = %q/%q", s0.ID, s0.State)
	}
	if s0.Restarts != 2 {
		t.Errorf("sync0 restarts = %d, want 2", s0.Restarts)
	}
	if s0.SecondsInState != 600 {
		t.Errorf("sync0 seconds_in_state = %v, want 600", s0.SecondsInState)
	}
	if s0.LastStart == "" || s0.Since == "" {
		t.Errorf("sync0 timestamps should be non-empty: last_start=%q since=%q", s0.LastStart, s0.Since)
	}

	// Sync 1: backoff, consecutive failures + restarts, seconds_in_state = 15.
	s1 := got.Syncs[1]
	if s1.State != "backoff" || s1.ConsecutiveFailures != 3 || s1.Restarts != 5 {
		t.Errorf("sync1 = %+v", s1)
	}
	if s1.SecondsInState != 15 {
		t.Errorf("sync1 seconds_in_state = %v, want 15", s1.SecondsInState)
	}

	// Sync 2: failed; zero-time LastStart must serialize as "" not the
	// zero-time string; the error string round-trips byte-for-byte
	// through JSON (the special-char surface).
	s2 := got.Syncs[2]
	if s2.State != "failed" || s2.ConsecutiveFailures != 8 {
		t.Errorf("sync2 state/consec = %q/%d", s2.State, s2.ConsecutiveFailures)
	}
	if s2.LastStart != "" {
		t.Errorf("sync2 last_start = %q, want empty (zero time)", s2.LastStart)
	}
	if s2.Since == "" {
		t.Errorf("sync2 since should be non-empty")
	}
	wantErr := `boom: <script>alert("x")</script> — naïve "quotes" & ümlauts`
	if s2.LastError != wantErr {
		t.Errorf("sync2 last_error round-trip mismatch:\n got: %q\nwant: %q", s2.LastError, wantErr)
	}

	// The raw JSON body must carry the error string JSON-encoded with the
	// angle brackets HTML-escaped (encoding/json escapes < > & to <
	// > & by default — an extra XSS-safety layer beyond the
	// page's textContent rendering), proving the special chars survive the
	// wire intact and never reach the client as literal markup.
	body := rec.Body.String()
	escaped := "\\u003cscript\\u003e"
	if !strings.Contains(body, escaped) {
		t.Errorf("expected HTML-escaped %s in body; got:\n%s", escaped, body)
	}
}

// TestDashboard_FleetJSON_Empty pins the no-syncs shape: a valid envelope
// with an empty (non-null) syncs array.
func TestDashboard_FleetJSON_Empty(t *testing.T) {
	now := time.Date(2026, 6, 26, 12, 0, 0, 0, time.UTC)
	ds, err := NewDashboardServer(":9300", fakeSnapshotter{})
	if err != nil {
		t.Fatalf("NewDashboardServer: %v", err)
	}
	ds.clock = fixedClock(now)

	rec := httptest.NewRecorder()
	ds.handleFleet(rec, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/api/fleet", http.NoBody))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), `"syncs":[]`) {
		t.Errorf("expected empty syncs array in body; got:\n%s", rec.Body.String())
	}
}

func TestDashboard_IndexPage(t *testing.T) {
	ds, err := NewDashboardServer(":9300", fakeSnapshotter{})
	if err != nil {
		t.Fatalf("NewDashboardServer: %v", err)
	}
	rec := httptest.NewRecorder()
	ds.handleIndex(rec, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", http.NoBody))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "text/html; charset=utf-8" {
		t.Errorf("Content-Type = %q, want text/html; charset=utf-8", ct)
	}
	if !strings.Contains(rec.Body.String(), "fleet dashboard") {
		t.Errorf("expected 'fleet dashboard' marker in HTML body")
	}
}

func TestDashboard_IndexNotFound(t *testing.T) {
	ds, err := NewDashboardServer(":9300", fakeSnapshotter{})
	if err != nil {
		t.Fatalf("NewDashboardServer: %v", err)
	}
	rec := httptest.NewRecorder()
	ds.handleIndex(rec, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/nope", http.NoBody))
	if rec.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404 for non-root path", rec.Code)
	}
}

func TestDashboard_Healthz(t *testing.T) {
	ds, err := NewDashboardServer(":9300", fakeSnapshotter{})
	if err != nil {
		t.Fatalf("NewDashboardServer: %v", err)
	}
	rec := httptest.NewRecorder()
	ds.handleHealthz(rec, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/healthz", http.NoBody))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if got := strings.TrimSpace(rec.Body.String()); got != "ok" {
		t.Errorf("body = %q, want ok", got)
	}
}
