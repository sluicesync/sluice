// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Prometheus-format `/metrics` endpoint for `sluice sync start
// --metrics-listen ADDR`. Phase 2 of the sync-health monitoring proto-
// ADR (`docs/dev/design/sync-health-monitoring.md`).
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
	"sync/atomic"
	"time"

	"sluicesync.dev/sluice/internal/appliercontrol"
	"sluicesync.dev/sluice/internal/ir"
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
	// laneAIMDControllers holds the W per-lane controllers when the stream
	// runs the ADR-0104/0105 concurrent key-hash apply path. Mutually
	// exclusive with aimdController in practice (the streamer attaches one
	// or the other), but both are snapshotted at scrape time if set.
	laneAIMDControllers []*appliercontrol.Controller
	spillReporter       SpillReporterFunc

	// ready flips to true once the streamer has finished its cold-start
	// or warm-resume preamble and entered the apply loop. /readyz reads
	// it via atomic.Bool so handler lookups stay lock-free.
	ready atomic.Bool
}

// SpillReporterFunc is the scrape-time hook the streamer plugs in to
// surface PG-14+ `pg_stat_replication_slots.spill_*` counters via the
// metrics endpoint (severity-B finding F2 of the 2026-05-22 PG-internals
// research run). The closure owns its own source-database connection
// (the streamer's CDC reader / replication conn is not safe to share);
// it's called on every scrape and returns the current cumulative
// counters for the slot the streamer is using.
//
// ok=false signals "no signal available" — same semantics as
// [ir.SlotSpillReporter]: the consumer omits the metric lines rather
// than emit a misleading 0. Errors fall through to a single `#` comment
// in the exposition output so the scraper visibly sees the failure
// without losing the rest of the metric set.
type SpillReporterFunc func(ctx context.Context) (snap SpillSnapshot, ok bool, err error)

// SpillSnapshot is the per-scrape, per-stream payload of the spill
// reporter (severity-B finding F2). Both counters are cumulative since
// the slot's creation; restarting sluice does NOT reset them — only
// dropping and recreating the slot does.
type SpillSnapshot struct {
	StreamID   string
	SlotName   string
	SpillTxns  int64
	SpillBytes int64
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
	mux.HandleFunc("/readyz", ms.handleReadyz)
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

// AttachLaneAIMDControllers plugs the W per-lane [appliercontrol.Controller]s
// of the ADR-0104/0105 concurrent key-hash apply path into the metrics
// server so the AIMD gauges fire PER LANE — the same four metric families as
// the serial path, each carrying an additional `lane="N"` label. Snapshotted
// at scrape time via [appliercontrol.Controller.Snapshot]; no instrumentation
// of the apply hot path. Idempotent; a nil/empty slice detaches.
//
// Item 31 made the concurrent path the default (`--apply-concurrency` resolves
// to auto:N), so without this the default stream lost AIMD observability
// entirely — the serial AttachAIMDController surface above never engages on
// the per-lane path.
func (m *MetricsServer) AttachLaneAIMDControllers(cs []*appliercontrol.Controller) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.laneAIMDControllers = cs
}

// AttachSpillReporter plugs a PG-slot-spill snapshotter into the metrics
// server (severity-B finding F2). The closure owns its own source DB
// connection and is invoked at scrape time; see [SpillReporterFunc] for
// the contract. Idempotent; a nil argument detaches.
//
// Off by default — engines that don't surface spill stats (MySQL) and
// streams that don't supply a source DSN to the metrics server simply
// never attach a reporter, and the corresponding metric lines never
// appear in the output.
func (m *MetricsServer) AttachSpillReporter(fn SpillReporterFunc) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.spillReporter = fn
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

// laneAIMDSnapshots returns one snapshot per attached lane controller, in
// lane-index order (index i is lane i), or nil when none are attached. Each
// controller is snapshotted atomically via Snapshot; the returned slice is
// freshly allocated so the caller can emit it outside the lock.
func (m *MetricsServer) laneAIMDSnapshots() []appliercontrol.MetricsSnapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.laneAIMDControllers) == 0 {
		return nil
	}
	snaps := make([]appliercontrol.MetricsSnapshot, len(m.laneAIMDControllers))
	for i, c := range m.laneAIMDControllers {
		snaps[i] = c.Snapshot()
	}
	return snaps
}

// spillReporterFn returns the attached spill-reporter closure (or nil
// when none is attached). Pulled out for read-locking discipline; the
// closure itself is invoked outside the mutex so a long-running source
// query doesn't block AttachSpillReporter or aimdSnapshot.
func (m *MetricsServer) spillReporterFn() SpillReporterFunc {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.spillReporter
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
	if snaps := m.laneAIMDSnapshots(); len(snaps) > 0 {
		emitLaneAIMDMetrics(w, snaps)
	}
	if fn := m.spillReporterFn(); fn != nil {
		snap, ok, err := fn(r.Context())
		switch {
		case err != nil:
			// Surface the failure as an exposition-format comment so
			// the scraper visibly sees it without losing the rest of
			// the metric set — same posture as the AIMD-not-attached
			// case (silently emit nothing) vs the list-streams-failed
			// case (return 500). Spill is a secondary signal; a
			// transient source-side hiccup shouldn't blank /metrics.
			fmt.Fprintf(w, "\n# error: slot-spill-stats: %v\n", err)
		case ok:
			emitSpillMetrics(w, snap)
		}
	}
}

// handleHealthz is the liveness probe — "is the process responsive?".
// Returns 200 "ok" unconditionally; doesn't touch the applier. k8s
// pulls a pod and restarts it when this stops responding.
//
// Paired with handleReadyz, which gates traffic on the streamer being
// past the cold-start preamble.
func (m *MetricsServer) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = io.WriteString(w, "ok\n")
}

// handleReadyz is the readiness probe — "is the streamer past its
// cold-start / warm-resume preamble and into the apply loop?". Returns
// 200 "ready" once [MetricsServer.MarkReady] has been called; otherwise
// 503 "not ready". k8s, Heroku, and systemd-based orchestrators use
// this to delay routing traffic or marking the unit as Started until
// the stream is actually mirroring.
//
// The signal is "streaming phase entered" only — no lag-threshold
// check, no per-scrape DB roundtrip. A streamer that has begun applying
// but fallen badly behind still reports ready; operators alert on lag
// via the `sluice_seconds_since_last_apply` metric, not /readyz. (This
// design choice was the operator-confirmed default in the #110 design
// review; see ADR-0069.)
func (m *MetricsServer) handleReadyz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	if m.ready.Load() {
		_, _ = io.WriteString(w, "ready\n")
		return
	}
	w.WriteHeader(http.StatusServiceUnavailable)
	_, _ = io.WriteString(w, "not ready\n")
}

// MarkReady flips the /readyz signal to 200 OK. The streamer calls
// this once after cold-start / warm-resume returns success and before
// the apply loop begins consuming events. Idempotent — repeated calls
// have no effect; the signal is monotonic (no "un-ready" path, since
// a streamer that loses the stream exits and the process is restarted
// by the orchestrator).
func (m *MetricsServer) MarkReady() {
	m.ready.Store(true)
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
	fmt.Fprintf(w, `sluice_apply_batch_size_cooloff{stream_id=%q} %d`+"\n", s.StreamID, boolGauge(s.InCoolOff))
}

// emitLaneAIMDMetrics renders the per-lane AIMD controllers of the
// ADR-0104/0105 concurrent key-hash apply path (item 31's now-default
// surface). It emits the SAME four metric families as [emitAIMDMetrics] but
// each series carries an additional `lane="N"` label (N = the lane index), so
// the serial path's existing series shape is untouched and a concurrent
// stream surfaces one series per lane:
//
//	sluice_apply_batch_size_current{stream_id="…",lane="0"} 1000
//	sluice_apply_batch_size_current{stream_id="…",lane="1"} 750
//	…
//
// HELP/TYPE headers are emitted once per family (the lane series share the
// metric name); the per-lane samples follow. snaps is in lane-index order.
func emitLaneAIMDMetrics(w io.Writer, snaps []appliercontrol.MetricsSnapshot) {
	fmt.Fprintln(w)
	fmt.Fprintln(w, "# HELP sluice_apply_batch_size_current AIMD controller's current target apply-batch-size after its latest decision.")
	fmt.Fprintln(w, "# TYPE sluice_apply_batch_size_current gauge")
	for i, s := range snaps {
		fmt.Fprintf(w, `sluice_apply_batch_size_current{stream_id=%q,lane="%d"} %d`+"\n", s.StreamID, i, s.CurrentSize)
	}
	fmt.Fprintln(w)

	fmt.Fprintln(w, "# HELP sluice_apply_batch_size_p95_seconds Rolling p95 batch-apply latency, in seconds, over the AIMD controller's sliding window.")
	fmt.Fprintln(w, "# TYPE sluice_apply_batch_size_p95_seconds gauge")
	for i, s := range snaps {
		fmt.Fprintf(w, `sluice_apply_batch_size_p95_seconds{stream_id=%q,lane="%d"} %s`+"\n", s.StreamID, i, formatPrometheusSeconds(s.P95))
	}
	fmt.Fprintln(w)

	fmt.Fprintln(w, "# HELP sluice_apply_batch_size_decreases_total Number of multiplicative-decrease events the AIMD controller has fired on this stream.")
	fmt.Fprintln(w, "# TYPE sluice_apply_batch_size_decreases_total counter")
	for i, s := range snaps {
		fmt.Fprintf(w, `sluice_apply_batch_size_decreases_total{stream_id=%q,lane="%d"} %d`+"\n", s.StreamID, i, s.DecreasesTotal)
	}
	fmt.Fprintln(w)

	fmt.Fprintln(w, "# HELP sluice_apply_batch_size_cooloff 1 when the AIMD controller is currently in cool-off (suppressing AI), 0 otherwise.")
	fmt.Fprintln(w, "# TYPE sluice_apply_batch_size_cooloff gauge")
	for i, s := range snaps {
		fmt.Fprintf(w, `sluice_apply_batch_size_cooloff{stream_id=%q,lane="%d"} %d`+"\n", s.StreamID, i, boolGauge(s.InCoolOff))
	}
}

// boolGauge maps a Go bool to the Prometheus 1/0 gauge convention.
func boolGauge(b bool) int {
	if b {
		return 1
	}
	return 0
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

// emitSpillMetrics renders the PG-14+ logical-decoding spill counters
// (severity-B finding F2 of the 2026-05-22 PG-internals research run).
// Both metrics are exposed as counters (cumulative since slot creation;
// PG resets them only on drop+recreate, which counter semantics handle
// correctly via the implicit "_total" rate-of-change view in
// Prometheus).
//
// Label set mirrors the existing per-stream gauges (stream_id) plus a
// `slot` label so operators querying a target with multiple streams
// (each on its own slot) can disambiguate. Cardinality is bounded by
// the number of slots a single sluice instance writes to — small.
//
//   - sluice_pg_slot_spill_txns_total{stream_id, slot} — cumulative
//     transactions that spilled to disk during decoding.
//   - sluice_pg_slot_spill_bytes_total{stream_id, slot} — cumulative
//     bytes of decoded transaction data that spilled to disk.
//
// Operator action when these grow: tune `logical_decoding_work_mem` on
// the source (default 64 MB) up, or split application transactions to
// stay under the threshold. See `docs/postgres-source-prep.md` for the
// operator-facing playbook.
func emitSpillMetrics(w io.Writer, s SpillSnapshot) {
	fmt.Fprintln(w)
	fmt.Fprintln(w, "# HELP sluice_pg_slot_spill_txns_total Cumulative count of transactions that spilled out of memory during logical decoding for this PG slot (pg_stat_replication_slots.spill_txns, PG 14+).")
	fmt.Fprintln(w, "# TYPE sluice_pg_slot_spill_txns_total counter")
	fmt.Fprintf(w, `sluice_pg_slot_spill_txns_total{stream_id=%q,slot=%q} %d`+"\n", s.StreamID, s.SlotName, s.SpillTxns)
	fmt.Fprintln(w)

	fmt.Fprintln(w, "# HELP sluice_pg_slot_spill_bytes_total Cumulative bytes of decoded transaction data that spilled to disk for this PG slot (pg_stat_replication_slots.spill_bytes, PG 14+).")
	fmt.Fprintln(w, "# TYPE sluice_pg_slot_spill_bytes_total counter")
	fmt.Fprintf(w, `sluice_pg_slot_spill_bytes_total{stream_id=%q,slot=%q} %d`+"\n", s.StreamID, s.SlotName, s.SpillBytes)
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
