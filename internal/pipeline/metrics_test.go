// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/appliercontrol"
	"sluicesync.dev/sluice/internal/ir"
)

// appliercontrolSnapshot builds an appliercontrol.MetricsSnapshot
// without exercising the controller — used so emitAIMDMetrics tests
// can pin the renderer in isolation.
func appliercontrolSnapshot(streamID string, size int, p95 time.Duration, decreases uint64, cool bool) appliercontrol.MetricsSnapshot {
	return appliercontrol.MetricsSnapshot{
		StreamID:       streamID,
		CurrentSize:    size,
		P95:            p95,
		DecreasesTotal: decreases,
		InCoolOff:      cool,
	}
}

// TestEmitMetrics_Empty pins the no-streams shape: each metric block's
// HELP/TYPE comments still emit so a Prometheus scraper sees the
// known shape, just with zero stream-keyed lines.
func TestEmitMetrics_Empty(t *testing.T) {
	var buf bytes.Buffer
	now := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)
	emitMetrics(&buf, nil, now)
	out := buf.String()
	for _, want := range []string{
		"# HELP sluice_seconds_since_last_apply",
		"# TYPE sluice_seconds_since_last_apply gauge",
		"# HELP sluice_stream_known",
		"# TYPE sluice_stream_known gauge",
		"sluice_metrics_scrape_unix_seconds",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("expected %q in output; got:\n%s", want, out)
		}
	}
}

// TestEmitMetrics_TwoStreams pins the multi-stream shape — one line
// per metric per stream, distinguished by stream_id label.
func TestEmitMetrics_TwoStreams(t *testing.T) {
	now := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)
	streams := []ir.StreamStatus{
		{StreamID: "myapp-prod", UpdatedAt: now.Add(-30 * time.Second)},
		{StreamID: "analytics-stream", UpdatedAt: now.Add(-3600 * time.Second)},
	}
	var buf bytes.Buffer
	emitMetrics(&buf, streams, now)
	out := buf.String()
	for _, want := range []string{
		`sluice_seconds_since_last_apply{stream_id="myapp-prod"} 30`,
		`sluice_seconds_since_last_apply{stream_id="analytics-stream"} 3600`,
		`sluice_stream_known{stream_id="myapp-prod"} 1`,
		`sluice_stream_known{stream_id="analytics-stream"} 1`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("expected %q in output; got:\n%s", want, out)
		}
	}
}

// TestEmitMetrics_ScrapeTimestamp pins the scrape-time gauge — useful
// for scraper-side staleness detection (Prometheus alerting can
// detect "scrape happened but the target's clock disagrees" via this
// vs. the scrape's own timestamp).
func TestEmitMetrics_ScrapeTimestamp(t *testing.T) {
	now := time.Date(2026, 5, 7, 12, 30, 45, 0, time.UTC)
	var buf bytes.Buffer
	emitMetrics(&buf, nil, now)
	want := "sluice_metrics_scrape_unix_seconds " // followed by a number
	if !strings.Contains(buf.String(), want) {
		t.Errorf("expected scrape-time gauge in output; got:\n%s", buf.String())
	}
}

// TestEmitMetrics_StreamIDQuoting pins the label-value quoting shape
// — stream IDs that include underscores, hyphens, or numbers should
// round-trip without escape mangling.
func TestEmitMetrics_StreamIDQuoting(t *testing.T) {
	streams := []ir.StreamStatus{
		{StreamID: "my-app_prod-2026", UpdatedAt: time.Now()},
	}
	var buf bytes.Buffer
	emitMetrics(&buf, streams, time.Now())
	if !strings.Contains(buf.String(), `stream_id="my-app_prod-2026"`) {
		t.Errorf("stream_id label should round-trip verbatim; got:\n%s", buf.String())
	}
}

// TestEmitAIMDMetrics pins the ADR-0052 DP-3 gauge set. The HELP/TYPE
// comments shape and the four metric lines must be present so
// Prometheus operators get a stable scrape format.
func TestEmitAIMDMetrics(t *testing.T) {
	snap := appliercontrolSnapshot("mystream", 75, 2_500*time.Millisecond, 3, true)
	var buf bytes.Buffer
	emitAIMDMetrics(&buf, snap)
	out := buf.String()
	for _, want := range []string{
		"# HELP sluice_apply_batch_size_current",
		"# TYPE sluice_apply_batch_size_current gauge",
		`sluice_apply_batch_size_current{stream_id="mystream"} 75`,
		"# HELP sluice_apply_batch_size_p95_seconds",
		"# TYPE sluice_apply_batch_size_p95_seconds gauge",
		`sluice_apply_batch_size_p95_seconds{stream_id="mystream"} 2.500`,
		"# HELP sluice_apply_batch_size_decreases_total",
		"# TYPE sluice_apply_batch_size_decreases_total counter",
		`sluice_apply_batch_size_decreases_total{stream_id="mystream"} 3`,
		"# HELP sluice_apply_batch_size_cooloff",
		"# TYPE sluice_apply_batch_size_cooloff gauge",
		`sluice_apply_batch_size_cooloff{stream_id="mystream"} 1`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("expected %q in AIMD metrics output; got:\n%s", want, out)
		}
	}
}

func TestEmitAIMDMetrics_NotInCoolOff(t *testing.T) {
	snap := appliercontrolSnapshot("s", 50, 0, 0, false)
	var buf bytes.Buffer
	emitAIMDMetrics(&buf, snap)
	if !strings.Contains(buf.String(), `sluice_apply_batch_size_cooloff{stream_id="s"} 0`) {
		t.Errorf("cool-off gauge should emit 0 when InCoolOff is false; got:\n%s", buf.String())
	}
}

// newAIMDControllerForTest builds a real appliercontrol.Controller wound to
// a known CurrentSize, so the handler-level lane tests exercise the actual
// Snapshot path (not a hand-built snapshot). InitialSize == ceiling so the
// controller's CurrentSize starts at `size`.
func newAIMDControllerForTest(t *testing.T, streamID string, size int) *appliercontrol.Controller {
	t.Helper()
	ctrl, err := appliercontrol.New(appliercontrol.Config{
		StreamID:      streamID,
		Floor:         1,
		Ceiling:       size,
		InitialSize:   size,
		TargetLatency: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("appliercontrol.New: %v", err)
	}
	return ctrl
}

// TestEmitLaneAIMDMetrics pins the ADR-0104/0105 per-lane gauge set (item
// 31's now-default concurrent path). Every lane must emit ALL FOUR metric
// families with a `lane="N"` label and the correct per-lane value — the
// renderer dispatches the same four lines per slice element, so the pin
// exercises >1 lane with DISTINCT values across all four families (current,
// p95, decreases, cool-off true AND false) rather than a single
// representative lane. HELP/TYPE headers emit once per family.
func TestEmitLaneAIMDMetrics(t *testing.T) {
	snaps := []appliercontrol.MetricsSnapshot{
		appliercontrolSnapshot("mystream", 1000, 1_200*time.Millisecond, 0, false),
		appliercontrolSnapshot("mystream", 500, 4_750*time.Millisecond, 2, true),
		appliercontrolSnapshot("mystream", 250, 9_001*time.Millisecond, 5, false),
	}
	var buf bytes.Buffer
	emitLaneAIMDMetrics(&buf, snaps)
	out := buf.String()
	for _, want := range []string{
		// HELP/TYPE headers — once per family (shared metric name).
		"# HELP sluice_apply_batch_size_current",
		"# TYPE sluice_apply_batch_size_current gauge",
		"# HELP sluice_apply_batch_size_p95_seconds",
		"# TYPE sluice_apply_batch_size_p95_seconds gauge",
		"# HELP sluice_apply_batch_size_decreases_total",
		"# TYPE sluice_apply_batch_size_decreases_total counter",
		"# HELP sluice_apply_batch_size_cooloff",
		"# TYPE sluice_apply_batch_size_cooloff gauge",
		// Lane 0.
		`sluice_apply_batch_size_current{stream_id="mystream",lane="0"} 1000`,
		`sluice_apply_batch_size_p95_seconds{stream_id="mystream",lane="0"} 1.200`,
		`sluice_apply_batch_size_decreases_total{stream_id="mystream",lane="0"} 0`,
		`sluice_apply_batch_size_cooloff{stream_id="mystream",lane="0"} 0`,
		// Lane 1 — distinct values, cool-off ON.
		`sluice_apply_batch_size_current{stream_id="mystream",lane="1"} 500`,
		`sluice_apply_batch_size_p95_seconds{stream_id="mystream",lane="1"} 4.750`,
		`sluice_apply_batch_size_decreases_total{stream_id="mystream",lane="1"} 2`,
		`sluice_apply_batch_size_cooloff{stream_id="mystream",lane="1"} 1`,
		// Lane 2.
		`sluice_apply_batch_size_current{stream_id="mystream",lane="2"} 250`,
		`sluice_apply_batch_size_p95_seconds{stream_id="mystream",lane="2"} 9.001`,
		`sluice_apply_batch_size_decreases_total{stream_id="mystream",lane="2"} 5`,
		`sluice_apply_batch_size_cooloff{stream_id="mystream",lane="2"} 0`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("expected %q in per-lane AIMD output; got:\n%s", want, out)
		}
	}
	// The serial (no-lane) series shape MUST NOT appear on the per-lane path:
	// the substring `_current{stream_id="mystream"} ` (label set closing right
	// after stream_id, no lane) would mean we accidentally emitted the serial
	// form too.
	if strings.Contains(out, `sluice_apply_batch_size_current{stream_id="mystream"} `) {
		t.Errorf("per-lane path must not emit the serial (lane-less) series; got:\n%s", out)
	}
}

// TestMetricsHandler_LaneAIMD_AttachedEmitsPerLane pins the end-to-end scrape
// wiring of the now-default concurrent path: AttachLaneAIMDControllers([W])
// makes /metrics emit W lane-labeled series across all four AIMD families.
// This is the regression guard for the v0.99.91 gate-blocker — without the
// fix the default --apply-concurrency stream emitted no AIMD gauges at all.
func TestMetricsHandler_LaneAIMD_AttachedEmitsPerLane(t *testing.T) {
	ms := newTestMetricsServer(t)
	ms.AttachLaneAIMDControllers([]*appliercontrol.Controller{
		newAIMDControllerForTest(t, "concstream", 1000),
		newAIMDControllerForTest(t, "concstream", 750),
	})
	body := scrapeMetrics(t, ms)
	for _, want := range []string{
		`sluice_apply_batch_size_current{stream_id="concstream",lane="0"} 1000`,
		`sluice_apply_batch_size_current{stream_id="concstream",lane="1"} 750`,
		`sluice_apply_batch_size_p95_seconds{stream_id="concstream",lane="0"}`,
		`sluice_apply_batch_size_decreases_total{stream_id="concstream",lane="1"}`,
		`sluice_apply_batch_size_cooloff{stream_id="concstream",lane="0"}`,
		// The substring the failing integration test asserts is satisfied by
		// the lane series — pin it here so the contract is explicit.
		`sluice_apply_batch_size_current{stream_id=`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("expected %q in /metrics body; got:\n%s", want, body)
		}
	}
}

// TestMetricsHandler_LaneAIMD_NotAttached pins the off-by-default surface:
// with no lane controllers attached, the per-lane (`lane=`) series never
// appear. Guards the serial path's byte-identical contract.
func TestMetricsHandler_LaneAIMD_NotAttached(t *testing.T) {
	ms := newTestMetricsServer(t)
	body := scrapeMetrics(t, ms)
	if strings.Contains(body, `lane="`) {
		t.Errorf("no lane-labeled series should appear when no lane controllers attached; got:\n%s", body)
	}
}

// TestMetricsHandler_LaneAIMD_DetachOnNil pins idempotent detach: passing a
// nil/empty slice to AttachLaneAIMDControllers removes previously-attached
// controllers, mirroring AttachAIMDController's contract.
func TestMetricsHandler_LaneAIMD_DetachOnNil(t *testing.T) {
	ms := newTestMetricsServer(t)
	ms.AttachLaneAIMDControllers([]*appliercontrol.Controller{
		newAIMDControllerForTest(t, "s", 100),
	})
	if body := scrapeMetrics(t, ms); !strings.Contains(body, `lane="0"`) {
		t.Fatalf("precondition: lane series should be present before detach; got:\n%s", body)
	}
	ms.AttachLaneAIMDControllers(nil)
	if body := scrapeMetrics(t, ms); strings.Contains(body, `lane="`) {
		t.Errorf("lane series should NOT appear after detach; got:\n%s", body)
	}
}

// TestEmitSpillMetrics pins the severity-B finding F2 gauge set. The
// HELP/TYPE shape and the two counter lines must be present so
// Prometheus operators get a stable scrape format for the PG-14+
// pg_stat_replication_slots.spill_* counters.
func TestEmitSpillMetrics(t *testing.T) {
	snap := SpillSnapshot{
		StreamID:   "myapp-prod",
		SlotName:   "sluice_slot",
		SpillTxns:  42,
		SpillBytes: 7_340_032,
	}
	var buf bytes.Buffer
	emitSpillMetrics(&buf, snap)
	out := buf.String()
	for _, want := range []string{
		"# HELP sluice_pg_slot_spill_txns_total",
		"# TYPE sluice_pg_slot_spill_txns_total counter",
		`sluice_pg_slot_spill_txns_total{stream_id="myapp-prod",slot="sluice_slot"} 42`,
		"# HELP sluice_pg_slot_spill_bytes_total",
		"# TYPE sluice_pg_slot_spill_bytes_total counter",
		`sluice_pg_slot_spill_bytes_total{stream_id="myapp-prod",slot="sluice_slot"} 7340032`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("expected %q in spill metrics output; got:\n%s", want, out)
		}
	}
}

// TestMetricsHandler_SpillReporter_AttachedAndEmits pins the end-to-end
// scrape wiring: when AttachSpillReporter is called with a closure that
// returns ok=true, /metrics emits the F2 spill lines alongside the
// existing stream gauges. The test exercises the handler through a real
// httptest server to catch any HTTP-layer regressions (header set,
// status code, etc.).
func TestMetricsHandler_SpillReporter_AttachedAndEmits(t *testing.T) {
	ms := newTestMetricsServer(t)
	ms.AttachSpillReporter(func(_ context.Context) (SpillSnapshot, bool, error) {
		return SpillSnapshot{
			StreamID:   "myapp-prod",
			SlotName:   "sluice_slot",
			SpillTxns:  9,
			SpillBytes: 1_048_576,
		}, true, nil
	})
	body := scrapeMetrics(t, ms)
	for _, want := range []string{
		`sluice_pg_slot_spill_txns_total{stream_id="myapp-prod",slot="sluice_slot"} 9`,
		`sluice_pg_slot_spill_bytes_total{stream_id="myapp-prod",slot="sluice_slot"} 1048576`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("expected %q in /metrics body; got:\n%s", want, body)
		}
	}
}

// TestMetricsHandler_SpillReporter_NotAttached pins the off-by-default
// surface: with no reporter attached, /metrics serves the existing
// stream/AIMD lines and the spill metric names never appear. This
// guards the "engines without spill stats (MySQL) and streams without
// source DSNs leave the metric off" contract documented on
// AttachSpillReporter.
func TestMetricsHandler_SpillReporter_NotAttached(t *testing.T) {
	ms := newTestMetricsServer(t)
	body := scrapeMetrics(t, ms)
	for _, banned := range []string{
		"sluice_pg_slot_spill_txns_total",
		"sluice_pg_slot_spill_bytes_total",
	} {
		if strings.Contains(body, banned) {
			t.Errorf("metric %q should not appear when no reporter attached; got:\n%s", banned, body)
		}
	}
}

// TestMetricsHandler_SpillReporter_NoSignalSkipsEmit pins the ok=false
// contract: when the reporter returns ok=false (PG < 14, no decode
// yet), the metric lines are suppressed. A literal 0 here would
// mislead operators into thinking spill is "definitely zero" when the
// real signal is "we can't tell yet."
func TestMetricsHandler_SpillReporter_NoSignalSkipsEmit(t *testing.T) {
	ms := newTestMetricsServer(t)
	ms.AttachSpillReporter(func(_ context.Context) (SpillSnapshot, bool, error) {
		return SpillSnapshot{}, false, nil
	})
	body := scrapeMetrics(t, ms)
	for _, banned := range []string{
		"sluice_pg_slot_spill_txns_total",
		"sluice_pg_slot_spill_bytes_total",
	} {
		if strings.Contains(body, banned) {
			t.Errorf("metric %q should not appear when reporter returned ok=false; got:\n%s", banned, body)
		}
	}
}

// TestMetricsHandler_SpillReporter_ErrorRendersComment pins the error
// posture: a transient source-side failure surfaces as a `# error:`
// comment in /metrics rather than a 500 (which would blank the rest
// of the metric set, surfacing as "scrape target down" — misleading
// because the streamer is fine).
func TestMetricsHandler_SpillReporter_ErrorRendersComment(t *testing.T) {
	ms := newTestMetricsServer(t)
	ms.AttachSpillReporter(func(_ context.Context) (SpillSnapshot, bool, error) {
		return SpillSnapshot{}, false, errors.New("source connection refused")
	})
	body := scrapeMetrics(t, ms)
	if !strings.Contains(body, "# error: slot-spill-stats: source connection refused") {
		t.Errorf("expected error comment in /metrics body; got:\n%s", body)
	}
	// The rest of the metric set should still render.
	if !strings.Contains(body, "sluice_metrics_scrape_unix_seconds") {
		t.Errorf("base metric set should still render alongside spill error; got:\n%s", body)
	}
}

// TestMetricsHandler_SpillReporter_DetachOnNil pins idempotent detach:
// passing nil to AttachSpillReporter removes a previously-attached
// closure, mirroring AttachAIMDController's contract.
func TestMetricsHandler_SpillReporter_DetachOnNil(t *testing.T) {
	ms := newTestMetricsServer(t)
	ms.AttachSpillReporter(func(_ context.Context) (SpillSnapshot, bool, error) {
		return SpillSnapshot{StreamID: "s", SlotName: "x", SpillBytes: 100}, true, nil
	})
	body := scrapeMetrics(t, ms)
	if !strings.Contains(body, "sluice_pg_slot_spill_bytes_total") {
		t.Fatalf("precondition: spill metric should be present before detach; got:\n%s", body)
	}
	ms.AttachSpillReporter(nil)
	body = scrapeMetrics(t, ms)
	if strings.Contains(body, "sluice_pg_slot_spill_bytes_total") {
		t.Errorf("spill metric should NOT appear after detach; got:\n%s", body)
	}
}

// TestReadyz_InitialIs503 pins the default: a freshly constructed
// MetricsServer reports /readyz = 503 until [MetricsServer.MarkReady]
// is called. The streamer flips the flag after cold-start / warm-resume
// returns success; until then the orchestrator should NOT route traffic
// or mark the unit started.
func TestReadyz_InitialIs503(t *testing.T) {
	ms := newTestMetricsServer(t)
	status, body := probeReadyz(t, ms)
	if status != http.StatusServiceUnavailable {
		t.Errorf("initial /readyz status = %d; want 503", status)
	}
	if !strings.Contains(body, "not ready") {
		t.Errorf("initial /readyz body = %q; want to contain 'not ready'", body)
	}
}

// TestReadyz_AfterMarkReadyIs200 pins the monotonic transition: once
// MarkReady is called, /readyz reports 200 with body "ready". There is
// no un-ready path — a streamer that loses the stream exits, and the
// orchestrator restarts the process, which starts with a fresh 503.
func TestReadyz_AfterMarkReadyIs200(t *testing.T) {
	ms := newTestMetricsServer(t)
	ms.MarkReady()
	status, body := probeReadyz(t, ms)
	if status != http.StatusOK {
		t.Errorf("post-MarkReady /readyz status = %d; want 200", status)
	}
	if !strings.Contains(body, "ready") || strings.Contains(body, "not ready") {
		t.Errorf("post-MarkReady /readyz body = %q; want 'ready'", body)
	}
}

// TestReadyz_MarkReadyIsIdempotent pins the "repeated calls have no
// effect" contract documented on MarkReady — a defensive check so a
// future call site that fires MarkReady more than once (e.g., a retry
// loop) doesn't accidentally regress the signal.
func TestReadyz_MarkReadyIsIdempotent(t *testing.T) {
	ms := newTestMetricsServer(t)
	ms.MarkReady()
	ms.MarkReady()
	ms.MarkReady()
	status, _ := probeReadyz(t, ms)
	if status != http.StatusOK {
		t.Errorf("after idempotent MarkReady calls, /readyz status = %d; want 200", status)
	}
}

// TestHealthz_AlwaysOK pins the liveness probe's "process responsive"
// semantics — it returns 200 regardless of readiness state. k8s uses
// /healthz to decide whether to restart the pod; conflating it with
// readiness would cause a cold-starting streamer to be killed mid-bulk.
func TestHealthz_AlwaysOK(t *testing.T) {
	ms := newTestMetricsServer(t)
	// Before MarkReady — /healthz should still be 200.
	status, body := probeHealthz(t, ms)
	if status != http.StatusOK || !strings.Contains(body, "ok") {
		t.Errorf("pre-ready /healthz: status=%d body=%q; want 200 'ok'", status, body)
	}
	ms.MarkReady()
	// Post MarkReady — /healthz is the same.
	status, body = probeHealthz(t, ms)
	if status != http.StatusOK || !strings.Contains(body, "ok") {
		t.Errorf("post-ready /healthz: status=%d body=%q; want 200 'ok'", status, body)
	}
}

// probeReadyz invokes the /readyz handler via httptest.ResponseRecorder
// and returns the status code and body, pulled out so each readyz test
// reads as a declarative call.
func probeReadyz(t *testing.T, ms *MetricsServer) (status int, body string) {
	t.Helper()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/readyz", http.NoBody)
	rec := httptest.NewRecorder()
	ms.handleReadyz(rec, req)
	res := rec.Result()
	defer func() { _ = res.Body.Close() }()
	raw, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return res.StatusCode, string(raw)
}

// probeHealthz mirrors probeReadyz for /healthz.
func probeHealthz(t *testing.T, ms *MetricsServer) (status int, body string) {
	t.Helper()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/healthz", http.NoBody)
	rec := httptest.NewRecorder()
	ms.handleHealthz(rec, req)
	res := rec.Result()
	defer func() { _ = res.Body.Close() }()
	raw, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return res.StatusCode, string(raw)
}

// newTestMetricsServer constructs a MetricsServer wired to an empty
// applier (ListStreams returns no streams) for use in handler-level
// tests. The applier's other methods are panics; handlers under test
// must not call them.
func newTestMetricsServer(t *testing.T) *MetricsServer {
	t.Helper()
	ms, err := NewMetricsServer(":0", &emptyApplier{})
	if err != nil {
		t.Fatalf("NewMetricsServer: %v", err)
	}
	return ms
}

// scrapeMetrics invokes the handler via httptest.ResponseRecorder and
// returns the body. Pulled out so each handler-level test reads as a
// declarative "set up, scrape, assert."
func scrapeMetrics(t *testing.T, ms *MetricsServer) string {
	t.Helper()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/metrics", http.NoBody)
	rec := httptest.NewRecorder()
	ms.handleMetrics(rec, req)
	res := rec.Result()
	defer func() { _ = res.Body.Close() }()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d; want 200. body:\n%s", res.StatusCode, body)
	}
	return string(body)
}

// emptyApplier is the minimum ir.ChangeApplier needed to drive
// MetricsServer.handleMetrics — only ListStreams is invoked. The rest
// of the surface panics so an accidental code path that calls them is
// caught at test-write time rather than running silently.
type emptyApplier struct{}

func (*emptyApplier) EnsureControlTable(context.Context) error {
	panic("emptyApplier.EnsureControlTable should not be called from metrics handler")
}

func (*emptyApplier) ReadPosition(context.Context, string) (ir.Position, bool, error) {
	panic("emptyApplier.ReadPosition should not be called from metrics handler")
}

func (*emptyApplier) ListStreams(context.Context) ([]ir.StreamStatus, error) {
	return nil, nil
}

func (*emptyApplier) Apply(context.Context, string, <-chan ir.Change) error {
	panic("emptyApplier.Apply should not be called from metrics handler")
}

func (*emptyApplier) RequestStop(context.Context, string) error {
	panic("emptyApplier.RequestStop should not be called from metrics handler")
}

func (*emptyApplier) ReadStopRequested(context.Context, string) (bool, error) {
	panic("emptyApplier.ReadStopRequested should not be called from metrics handler")
}

func (*emptyApplier) ClearStopRequested(context.Context, string) error {
	panic("emptyApplier.ClearStopRequested should not be called from metrics handler")
}

// TestEmitSpillMetrics_Zero pins the "decode has happened but no spill"
// case: the counters render as 0 (not absent) because ok=true upstream
// means "we have real data, and the real data is 0." The "no signal"
// case (ok=false) is handled at the call site by skipping the emitter
// entirely; this test pins what the emitter does when invoked.
func TestEmitSpillMetrics_Zero(t *testing.T) {
	snap := SpillSnapshot{
		StreamID:   "s",
		SlotName:   "sluice_slot",
		SpillTxns:  0,
		SpillBytes: 0,
	}
	var buf bytes.Buffer
	emitSpillMetrics(&buf, snap)
	out := buf.String()
	if !strings.Contains(out, `sluice_pg_slot_spill_txns_total{stream_id="s",slot="sluice_slot"} 0`) {
		t.Errorf("zero spill_txns should render as 0; got:\n%s", out)
	}
	if !strings.Contains(out, `sluice_pg_slot_spill_bytes_total{stream_id="s",slot="sluice_slot"} 0`) {
		t.Errorf("zero spill_bytes should render as 0; got:\n%s", out)
	}
}
