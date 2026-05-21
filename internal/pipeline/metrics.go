// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Prometheus-format `/metrics` endpoint for `sluice sync start
// --metrics-listen ADDR`. Phase 2 of the sync-health monitoring proto-
// ADR (`docs/dev/design-sync-health-monitoring.md`).
//
// MVP scope: scrape-time read of the target's sluice_cdc_state for
// each stream the target has been a destination for; emit a small
// gauge set that captures liveness without touching the apply path.
// Same data that `sluice sync health` exposes per-call as a one-shot
// probe; this surface lets monitoring systems scrape continuously
// without polling the CLI.
//
// **No new dependency.** Hand-written Prometheus text-format encoder
// (~30 lines). Switching to prometheus/client_golang is a future
// option if histograms / labels / multi-process aggregation become
// load-bearing; today the gauge surface is small enough that the
// dependency cost (binary size + transitive deps) outweighs the
// ergonomic benefit. Listed in the proto-ADR's "Open question #4".
//
// **No instrumentation of the apply path.** The MVP endpoint reads
// `ListStreams` and computes seconds-since-last-apply from
// UpdatedAt. This deliberately avoids touching the streamer's hot
// loop: a metrics surface that risks introducing apply-path bugs
// would defeat its own purpose. Future revisions can add per-event
// counters by instrumenting the applier directly when the cost is
// proven safe.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/orware/sluice/internal/appliercontrol"
	"github.com/orware/sluice/internal/ir"
)

// MetricsServer serves `/metrics` for an active sync stream. The
// streamer creates one of these when [Streamer.MetricsListen] is
// non-empty, hands it the applier handle, and Closes it on stream
// teardown.
//
// As of ADR-0052 the server can optionally snapshot a per-stream
// [appliercontrol.Controller] alongside the applier-derived stream
// status. AIMD gauges are emitted only when AttachAIMDController has
// been called.
type MetricsServer struct {
	addr    string
	applier ir.ChangeApplier
	server  *http.Server

	mu             sync.RWMutex
	aimdController *appliercontrol.Controller
}

// NewMetricsServer wires the HTTP server. Does NOT start listening
// — call Start to begin serving. Returns an error when the address
// is unusable.
func NewMetricsServer(addr string, applier ir.ChangeApplier) (*MetricsServer, error) {
	if addr == "" {
		return nil, errors.New("MetricsServer: addr is empty")
	}
	if applier == nil {
		return nil, errors.New("MetricsServer: applier is nil")
	}
	mux := http.NewServeMux()
	ms := &MetricsServer{
		addr:    addr,
		applier: applier,
	}
	mux.HandleFunc("/metrics", ms.handleMetrics)
	mux.HandleFunc("/healthz", ms.handleHealthz)
	ms.server = &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       30 * time.Second,
	}
	return ms, nil
}

// Start binds the listener and serves in a background goroutine.
// Returns when the listener is bound (so caller knows the address is
// reserved) or fails to bind. Use Close to stop the server cleanly.
func (m *MetricsServer) Start() error {
	// Background context: the listener outlives any single request,
	// and Close() drives the shutdown. Lint guidance is to use
	// ListenConfig.Listen rather than net.Listen for context-aware
	// dial; the listener itself isn't ctx-cancellable but giving it a
	// background context satisfies the linter cleanly.
	lc := &net.ListenConfig{}
	ln, err := lc.Listen(context.Background(), "tcp", m.addr)
	if err != nil {
		return fmt.Errorf("metrics: listen %s: %w", m.addr, err)
	}
	go func() {
		// Serve blocks until Close is called or an unrecoverable error
		// occurs. We don't surface ErrServerClosed (the expected
		// teardown signal); other errors are silently swallowed since
		// a metrics-server failure shouldn't kill the streamer — the
		// operator's monitoring stack will alert on the missing scrape
		// target faster than any internal error reporting could.
		_ = m.server.Serve(ln)
	}()
	return nil
}

// Close shuts down the HTTP server with a 5s grace period.
func (m *MetricsServer) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return m.server.Shutdown(ctx)
}

// AttachAIMDController plugs an [appliercontrol.Controller] into the
// metrics server so the AIMD gauges fire alongside the existing
// stream-status gauges. ADR-0052 DP-3. The controller is snapshotted
// at scrape time via [appliercontrol.Controller.Snapshot] — no
// instrumentation of the apply hot path. Idempotent; a nil argument
// detaches.
func (m *MetricsServer) AttachAIMDController(c *appliercontrol.Controller) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.aimdController = c
}

// aimdSnapshot returns a snapshot of the attached AIMD controller, or
// (snapshot, false) when no controller is attached.
func (m *MetricsServer) aimdSnapshot() (appliercontrol.MetricsSnapshot, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.aimdController == nil {
		return appliercontrol.MetricsSnapshot{}, false
	}
	return m.aimdController.Snapshot(), true
}

// handleMetrics is the GET /metrics handler. Reads ListStreams,
// emits Prometheus exposition format. Errors fall through as
// 500 so the operator's scraper visibly fails rather than silently
// returning empty metrics (which a careless reader could mistake
// for "all streams idle").
func (m *MetricsServer) handleMetrics(w http.ResponseWriter, r *http.Request) {
	streams, err := m.applier.ListStreams(r.Context())
	if err != nil {
		http.Error(w, fmt.Sprintf("# error: list streams: %v\n", err), http.StatusInternalServerError)
		return
	}
	now := time.Now()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	emitMetrics(w, streams, now)
	if snap, ok := m.aimdSnapshot(); ok {
		emitAIMDMetrics(w, snap)
	}
}

// handleHealthz is a tiny "is the server alive" endpoint that
// monitoring stacks use to distinguish "scrape target is gone" from
// "scrape target is up but reports zero streams". Returns 200 with
// the word "ok"; doesn't touch the applier.
func (m *MetricsServer) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = io.WriteString(w, "ok\n")
}

// emitMetrics renders the current stream snapshot as Prometheus
// exposition format. Pure function — pulled out for unit tests that
// don't need a live HTTP listener.
//
// Metric set (all gauges; labels: stream_id):
//   - sluice_seconds_since_last_apply — wall-clock seconds between
//     now and the stream's most recent applier commit.
//   - sluice_stream_known — 1 for every stream surfaced by
//     ListStreams (so Prometheus operators can `count(...)` them).
//   - sluice_metrics_scrape_unix_seconds — Unix timestamp of this
//     scrape, useful for staleness detection on the scraper side.
func emitMetrics(w io.Writer, streams []ir.StreamStatus, now time.Time) {
	fmt.Fprintln(w, "# HELP sluice_seconds_since_last_apply Wall-clock seconds since the stream's most recent applier commit.")
	fmt.Fprintln(w, "# TYPE sluice_seconds_since_last_apply gauge")
	for _, s := range streams {
		fmt.Fprintf(w, `sluice_seconds_since_last_apply{stream_id=%q} %d`+"\n",
			s.StreamID, int64(now.Sub(s.UpdatedAt).Seconds()))
	}
	fmt.Fprintln(w)

	fmt.Fprintln(w, "# HELP sluice_stream_known Constant 1 for every stream the target has tracked. Lets operators count(sluice_stream_known) for stream-count alerts.")
	fmt.Fprintln(w, "# TYPE sluice_stream_known gauge")
	for _, s := range streams {
		fmt.Fprintf(w, `sluice_stream_known{stream_id=%q} 1`+"\n", s.StreamID)
	}
	fmt.Fprintln(w)

	fmt.Fprintln(w, "# HELP sluice_metrics_scrape_unix_seconds Unix timestamp of this scrape, for scraper-side staleness detection.")
	fmt.Fprintln(w, "# TYPE sluice_metrics_scrape_unix_seconds gauge")
	fmt.Fprintf(w, "sluice_metrics_scrape_unix_seconds %d\n", now.Unix())
}

// emitAIMDMetrics renders the AIMD apply-batch-size controller's
// scrape-time snapshot in Prometheus exposition format. ADR-0052 DP-3
// gauge set:
//
//   - sluice_apply_batch_size_current{stream_id}     — controller's
//     current target batch size after its latest decision.
//   - sluice_apply_batch_size_p95_seconds{stream_id} — rolling p95
//     latency over the controller's sliding window.
//   - sluice_apply_batch_size_decreases_total{stream_id} — counter of
//     multiplicative-decrease events (lets operators alert on
//     persistent oscillation).
//   - sluice_apply_batch_size_cooloff{stream_id}     — 0 or 1 for
//     "currently in cool-off period."
//
// Reads the controller's state via Snapshot — atomic and lock-light;
// no per-batch instrumentation cost.
func emitAIMDMetrics(w io.Writer, s appliercontrol.MetricsSnapshot) {
	fmt.Fprintln(w)
	fmt.Fprintln(w, "# HELP sluice_apply_batch_size_current AIMD controller's current target apply-batch-size after its latest decision.")
	fmt.Fprintln(w, "# TYPE sluice_apply_batch_size_current gauge")
	fmt.Fprintf(w, `sluice_apply_batch_size_current{stream_id=%q} %d`+"\n", s.StreamID, s.CurrentSize)
	fmt.Fprintln(w)

	fmt.Fprintln(w, "# HELP sluice_apply_batch_size_p95_seconds Rolling p95 batch-apply latency, in seconds, over the AIMD controller's sliding window.")
	fmt.Fprintln(w, "# TYPE sluice_apply_batch_size_p95_seconds gauge")
	fmt.Fprintf(w, `sluice_apply_batch_size_p95_seconds{stream_id=%q} %s`+"\n", s.StreamID, formatPrometheusSeconds(s.P95))
	fmt.Fprintln(w)

	fmt.Fprintln(w, "# HELP sluice_apply_batch_size_decreases_total Number of multiplicative-decrease events the AIMD controller has fired on this stream.")
	fmt.Fprintln(w, "# TYPE sluice_apply_batch_size_decreases_total counter")
	fmt.Fprintf(w, `sluice_apply_batch_size_decreases_total{stream_id=%q} %d`+"\n", s.StreamID, s.DecreasesTotal)
	fmt.Fprintln(w)

	fmt.Fprintln(w, "# HELP sluice_apply_batch_size_cooloff 1 when the AIMD controller is currently in cool-off (suppressing AI), 0 otherwise.")
	fmt.Fprintln(w, "# TYPE sluice_apply_batch_size_cooloff gauge")
	coolOff := 0
	if s.InCoolOff {
		coolOff = 1
	}
	fmt.Fprintf(w, `sluice_apply_batch_size_cooloff{stream_id=%q} %d`+"\n", s.StreamID, coolOff)
}

// formatPrometheusSeconds renders a duration as a Prometheus gauge
// value — fixed-point seconds with millisecond resolution (the
// controller's p95 lives in the milliseconds-to-tens-of-seconds
// range; sub-millisecond resolution is noise). The output is
// dot-separated so operators can drop it straight into a Grafana
// panel.
func formatPrometheusSeconds(d time.Duration) string {
	ms := d.Milliseconds()
	secs := ms / 1000
	frac := ms % 1000
	return fmt.Sprintf("%d.%03d", secs, frac)
}

// quoteForPrometheusLabelValue escapes a string for use as a
// Prometheus label value. Currently unused (the StreamID flows
// through %q which is sufficient for ASCII stream IDs); kept here
// for future use when label values may carry non-ASCII content
// requiring strict quoting per the exposition format spec.
//
//nolint:unused // forward-compat utility
func quoteForPrometheusLabelValue(s string) string {
	var sb strings.Builder
	sb.WriteByte('"')
	for _, r := range s {
		switch r {
		case '\\':
			sb.WriteString(`\\`)
		case '"':
			sb.WriteString(`\"`)
		case '\n':
			sb.WriteString(`\n`)
		default:
			sb.WriteRune(r)
		}
	}
	sb.WriteByte('"')
	return sb.String()
}
