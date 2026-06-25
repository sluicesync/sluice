// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

func TestFormatWatchLine_KnownAndUnknownHonesty(t *testing.T) {
	now := time.Date(2026, 6, 23, 16, 0, 0, 0, time.UTC)

	// All-unknown snapshot: every metric prints n/a, never a misleading 0.
	got := formatWatchLine(now, ir.TargetHealthSnapshot{SampledAt: now}, true)
	for _, want := range []string{"cpu=n/a", "mem=n/a", "storage=n/a", "lag=n/a", "conns=n/a"} {
		if !strings.Contains(got, want) {
			t.Errorf("unknown-honesty: %q missing %q", got, want)
		}
	}
	if strings.Contains(got, "(used") {
		t.Errorf("unknown storage should not print a used/cap detail: %q", got)
	}

	// Known snapshot: values render, storage shows the GB detail.
	snap := ir.TargetHealthSnapshot{
		SampledAt: now,
		CPUUtil:   0.42, CPUKnown: true,
		MemUtil: 0.70, MemKnown: true,
		StorageUtil:           0.55,
		StorageAvailableBytes: 5_000_000_000,
		StorageCapacityBytes:  10_000_000_000,
		StorageKnown:          true,
		ReplicaLagSeconds:     1.5, LagKnown: true,
		ActiveConnections: 12, MaxConnections: 100, ConnKnown: true,
	}
	got = formatWatchLine(now, snap, true)
	for _, want := range []string{"cpu=0.420", "mem=0.700", "storage=0.550", "lag=1.5s", "conns=12/100", "(used 5.0G/10.0G)", "fresh=true"} {
		if !strings.Contains(got, want) {
			t.Errorf("known-render: %q missing %q", got, want)
		}
	}

	// ok=false: the warming-up line, regardless of any snapshot fields.
	if got := formatWatchLine(now, snap, false); !strings.Contains(got, "no fresh sample") {
		t.Errorf("ok=false should print the warming-up line, got %q", got)
	}
}

func TestRunMetricsWatch_NilProvider(t *testing.T) {
	if err := RunMetricsWatch(context.Background(), nil, MetricsWatchConfig{Once: true}); err == nil {
		t.Fatal("nil provider must be a hard error")
	}
}

func TestRunMetricsWatch_OnceWatchOnlyPrints(t *testing.T) {
	prov := &fakeTelemetry{ok: true, snap: ir.TargetHealthSnapshot{
		SampledAt: time.Now(),
		CPUUtil:   0.10, CPUKnown: true,
		StorageUtil: 0.20, StorageKnown: true,
		StorageCapacityBytes: 10_000_000_000, StorageAvailableBytes: 8_000_000_000,
	}}
	var buf bytes.Buffer
	// No thresholds set ⇒ watch-only; must still print and return cleanly.
	err := RunMetricsWatch(context.Background(), prov, MetricsWatchConfig{
		Once: true, Print: true, Out: &buf,
	})
	if err != nil {
		t.Fatalf("watch-only Once: %v", err)
	}
	if !strings.Contains(buf.String(), "cpu=0.100") {
		t.Errorf("expected a printed sample line, got %q", buf.String())
	}
}

// TestRunMetricsWatch_OnceFiresAlert exercises the full standalone path:
// build rules from the config, build the webhook sink from the URL, evaluate
// the fresh breaching snapshot, and DELIVER — proving the daemon's wiring
// reuses the alerter end to end (not just the pure evaluator).
func TestRunMetricsWatch_OnceFiresAlert(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	prov := &fakeTelemetry{ok: true, snap: ir.TargetHealthSnapshot{
		SampledAt:   time.Now(),
		StorageUtil: 0.95, StorageKnown: true,
		StorageCapacityBytes: 10_000_000_000, StorageAvailableBytes: 500_000_000,
	}}
	err := RunMetricsWatch(context.Background(), prov, MetricsWatchConfig{
		StorageUtil: 0.90, // breached by 0.95
		WebhookURL:  srv.URL,
		Once:        true,
	})
	if err != nil {
		t.Fatalf("alerting Once: %v", err)
	}
	if got := hits.Load(); got != 1 {
		t.Errorf("expected exactly one webhook delivery, got %d", got)
	}
}

// TestRunMetricsWatch_OnceNoFireWhenUnobserved pins the *Known honesty
// contract end-to-end: a breaching-looking value whose metric is UNOBSERVED
// must not fire.
func TestRunMetricsWatch_OnceNoFireWhenUnobserved(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	prov := &fakeTelemetry{ok: true, snap: ir.TargetHealthSnapshot{
		SampledAt:   time.Now(),
		StorageUtil: 0.99, StorageKnown: false, // value present but UNOBSERVED
	}}
	err := RunMetricsWatch(context.Background(), prov, MetricsWatchConfig{
		StorageUtil: 0.90,
		WebhookURL:  srv.URL,
		Once:        true,
	})
	if err != nil {
		t.Fatalf("unobserved Once: %v", err)
	}
	if got := hits.Load(); got != 0 {
		t.Errorf("unobserved metric must not fire; got %d deliveries", got)
	}
}

// TestStartWatchExporter_ServesTargetGauges pins the standalone exporter: it
// serves /metrics re-exporting the watched DB's health as the sluice_target_*
// family alongside build_info + the Go-runtime block.
func TestStartWatchExporter_ServesTargetGauges(t *testing.T) {
	prov := &fakeTelemetry{ok: true, snap: ir.TargetHealthSnapshot{
		SampledAt: time.Now(),
		CPUUtil:   0.30, CPUKnown: true,
		StorageUtil: 0.77, StorageKnown: true,
		StorageCapacityBytes: 10_000_000_000, StorageAvailableBytes: 2_300_000_000,
	}}

	// Pick a free port, then hand it to the exporter (small TOCTOU window,
	// acceptable for a unit test).
	l, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("pick port: %v", err)
	}
	addr := l.Addr().String()
	_ = l.Close()

	stop, err := startWatchExporter(context.Background(), addr, prov, "watch-lbl", "v0.99.115", "deadbee", slog.Default())
	if err != nil {
		t.Fatalf("startWatchExporter: %v", err)
	}
	defer stop()

	resp, err := http.Get("http://" + addr + "/metrics") //nolint:noctx // test
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	out := string(body)
	for _, want := range []string{
		`sluice_build_info{version="v0.99.115"`,
		"sluice_go_goroutines ",
		`sluice_target_cpu_util{stream_id="watch-lbl"} 0.3000`,
		`sluice_target_storage_util{stream_id="watch-lbl"} 0.7700`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("/metrics missing %q in:\n%s", want, out)
		}
	}
}

// TestStartWatchExporter_NoSignalComment pins that an unwarmed provider yields
// the "no signal" comment instead of misleading target gauges (but build_info
// + runtime still emit).
func TestStartWatchExporter_NoSignalComment(t *testing.T) {
	prov := &fakeTelemetry{ok: false}
	l, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("pick port: %v", err)
	}
	addr := l.Addr().String()
	_ = l.Close()

	stop, err := startWatchExporter(context.Background(), addr, prov, "lbl", "v1", "c1", slog.Default())
	if err != nil {
		t.Fatalf("startWatchExporter: %v", err)
	}
	defer stop()

	resp, err := http.Get("http://" + addr + "/metrics") //nolint:noctx // test
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	out := string(body)
	if !strings.Contains(out, "# target-telemetry: no signal") {
		t.Errorf("expected no-signal comment, got:\n%s", out)
	}
	if strings.Contains(out, "sluice_target_cpu_util") {
		t.Errorf("no-signal scrape must not emit target gauges:\n%s", out)
	}
	if !strings.Contains(out, "sluice_build_info") {
		t.Errorf("build_info should still emit on a no-signal scrape:\n%s", out)
	}
}
