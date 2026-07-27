// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/telemetrysink"
)

// fakeFleet is the ir.FleetTelemetry stub: a fixed set of per-target samples.
type fakeFleet struct {
	mu      sync.Mutex
	samples []ir.FleetHealthSample
}

func (f *fakeFleet) SampleFleet(context.Context) []ir.FleetHealthSample {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]ir.FleetHealthSample, len(f.samples))
	copy(out, f.samples)
	return out
}

func (f *fakeFleet) set(samples []ir.FleetHealthSample) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.samples = samples
}

// fleetSample builds one OK sample for a target.
func fleetSample(db string, cpu, storage float64) ir.FleetHealthSample {
	return ir.FleetHealthSample{
		Target: ir.FleetTarget{Database: db, Branch: "main"},
		Snapshot: ir.TargetHealthSnapshot{
			SampledAt:             time.Now(),
			CPUUtil:               cpu,
			CPUKnown:              true,
			StorageUtil:           storage,
			StorageKnown:          true,
			StorageCapacityBytes:  10_000_000_000,
			StorageAvailableBytes: 2_000_000_000,
		},
		OK: true,
	}
}

// recordingSink captures the batches the watch persisted.
type recordingSink struct {
	mu      sync.Mutex
	batches [][]telemetrysink.Record
	fail    bool
}

func (r *recordingSink) Name() string { return "recording" }
func (r *recordingSink) Close() error { return nil }

func (r *recordingSink) Write(_ context.Context, recs []telemetrysink.Record) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.batches = append(r.batches, recs)
	if r.fail {
		return errRecordingSink
	}
	return nil
}

func (r *recordingSink) all() []telemetrysink.Record {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []telemetrysink.Record
	for _, b := range r.batches {
		out = append(out, b...)
	}
	return out
}

var errRecordingSink = errors.New("sink is dead")

// --- (b) the org-wide summary line ---

// TestFormatFleetWatchLine_SummaryAndHonesty pins the org-wide live line: ONE
// summary row (not one per database), the worst reading per metric with the
// database that produced it, and "n/a" for a metric no target observed.
func TestFormatFleetWatchLine_SummaryAndHonesty(t *testing.T) {
	now := time.Date(2026, 7, 24, 16, 0, 0, 0, time.UTC)
	samples := []ir.FleetHealthSample{
		fleetSample("alpha", 0.10, 0.90),
		fleetSample("beta", 0.80, 0.20),
		{Target: ir.FleetTarget{Database: "gamma", Branch: "main"}}, // discovered, unobserved
	}
	got := formatFleetWatchLine(now, samples)
	for _, want := range []string{
		"targets=3", "observed=2",
		"cpu.max=0.800(beta)",
		"storage.max=0.900(alpha)",
		"mem.max=n/a", "lag.max=n/a",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("fleet line %q missing %q", got, want)
		}
	}

	if got := formatFleetWatchLine(now, nil); !strings.Contains(got, "no targets discovered") {
		t.Errorf("empty fleet must say so, got %q", got)
	}
}

// TestFormatFleetWatchLine_StaleTargetContributesNothing pins that an
// unobserved (OK=false) target never contributes a reading to the summary —
// a stale snapshot must not masquerade as the fleet's worst case.
func TestFormatFleetWatchLine_StaleTargetContributesNothing(t *testing.T) {
	now := time.Date(2026, 7, 24, 16, 0, 0, 0, time.UTC)
	stale := fleetSample("stale", 0.99, 0.99)
	stale.OK = false
	got := formatFleetWatchLine(now, []ir.FleetHealthSample{fleetSample("live", 0.10, 0.10), stale})
	if strings.Contains(got, "0.990") {
		t.Errorf("a stale target must not contribute a reading: %q", got)
	}
	if !strings.Contains(got, "observed=1") {
		t.Errorf("want observed=1: %q", got)
	}
}

// TestFleetWatchReadoutFields_MirrorsTheLine pins the panel readout carries
// the same summary the non-TTY line does.
func TestFleetWatchReadoutFields_MirrorsTheLine(t *testing.T) {
	fields := fleetWatchReadoutFields(time.Now(), []ir.FleetHealthSample{
		fleetSample("alpha", 0.10, 0.90),
		fleetSample("beta", 0.80, 0.20),
	})
	want := map[string]string{
		"targets": "2", "observed": "2",
		"cpu.max": "0.800(beta)", "storage.max": "0.900(alpha)",
		"mem.max": "n/a", "lag.max": "n/a",
	}
	for _, f := range fields {
		if w, ok := want[f.Label]; ok && f.Value != w {
			t.Errorf("readout %s = %q, want %q", f.Label, f.Value, w)
		}
		delete(want, f.Label)
	}
	if len(want) != 0 {
		t.Errorf("readout missing labels: %v", want)
	}
}

// --- (b) per-target alerting ---

// TestRunFleetMetricsWatch_AlertsPerTargetIndependently pins that each
// database+branch latches on its OWN state: two breaching databases fire two
// alerts naming their own targets, and a non-breaching sibling fires none.
func TestRunFleetMetricsWatch_AlertsPerTargetIndependently(t *testing.T) {
	var (
		mu     sync.Mutex
		bodies []string
	)
	srv := newRecordingWebhook(t, &mu, &bodies)
	defer srv.Close()

	fleet := &fakeFleet{}
	fleet.set([]ir.FleetHealthSample{
		fleetSample("hot-a", 0.10, 0.95),
		fleetSample("hot-b", 0.10, 0.97),
		fleetSample("cool", 0.10, 0.10),
	})
	err := RunMetricsWatch(t.Context(), nil, MetricsWatchConfig{
		Fleet:       fleet,
		StorageUtil: 0.90,
		WebhookURL:  srv.URL,
		Label:       "metrics-watch:acme",
		Once:        true,
	})
	if err != nil {
		t.Fatalf("fleet Once: %v", err)
	}
	mu.Lock()
	got := strings.Join(bodies, "\n")
	n := len(bodies)
	mu.Unlock()
	if n != 2 {
		t.Fatalf("want exactly 2 alerts (the two breaching targets), got %d:\n%s", n, got)
	}
	for _, want := range []string{"metrics-watch:acme/hot-a/main", "metrics-watch:acme/hot-b/main"} {
		if !strings.Contains(got, want) {
			t.Errorf("alert stream ids must name the target; missing %q in:\n%s", want, got)
		}
	}
	if strings.Contains(got, "cool") {
		t.Errorf("a non-breaching target must not fire:\n%s", got)
	}
}

// TestRunFleetMetricsWatch_UnobservedTargetNeverFires pins the *Known honesty
// contract per target: a breaching-looking value the provider did not observe
// must not alert.
func TestRunFleetMetricsWatch_UnobservedTargetNeverFires(t *testing.T) {
	var (
		mu     sync.Mutex
		bodies []string
	)
	srv := newRecordingWebhook(t, &mu, &bodies)
	defer srv.Close()

	unobserved := fleetSample("ghost", 0.10, 0.99)
	unobserved.Snapshot.StorageKnown = false
	stale := fleetSample("stale", 0.10, 0.99)
	stale.OK = false

	err := RunMetricsWatch(t.Context(), nil, MetricsWatchConfig{
		Fleet:       &fakeFleet{samples: []ir.FleetHealthSample{unobserved, stale}},
		StorageUtil: 0.90,
		WebhookURL:  srv.URL,
		Once:        true,
	})
	if err != nil {
		t.Fatalf("fleet Once: %v", err)
	}
	mu.Lock()
	n := len(bodies)
	mu.Unlock()
	if n != 0 {
		t.Fatalf("unobserved/stale targets must not fire, got %d alerts", n)
	}
}

// --- (b) the org-wide exporter ---

// TestStartFleetWatchExporter_GroupsFamiliesAndLabelsTargets pins the
// exposition's load-bearing shape: ONE HELP/TYPE block per metric name (a
// duplicate makes the text invalid for a strict scraper) with one
// database/branch-labelled series per observed target, plus the fleet-size
// gauges.
func TestStartFleetWatchExporter_GroupsFamiliesAndLabelsTargets(t *testing.T) {
	fleet := &fakeFleet{}
	fleet.set([]ir.FleetHealthSample{
		fleetSample("alpha", 0.30, 0.77),
		fleetSample("beta", 0.40, 0.20),
		{Target: ir.FleetTarget{Database: "gamma", Branch: "dev"}}, // unobserved
	})

	out := scrapeFleetExporter(t, fleet)
	if n := strings.Count(out, "# HELP sluice_target_cpu_util "); n != 1 {
		t.Fatalf("want exactly ONE HELP line for sluice_target_cpu_util, got %d:\n%s", n, out)
	}
	if n := strings.Count(out, "# TYPE sluice_target_cpu_util "); n != 1 {
		t.Fatalf("want exactly ONE TYPE line for sluice_target_cpu_util, got %d:\n%s", n, out)
	}
	for _, want := range []string{
		`sluice_target_cpu_util{database="alpha",branch="main"} 0.3000`,
		`sluice_target_cpu_util{database="beta",branch="main"} 0.4000`,
		`sluice_target_storage_util{database="alpha",branch="main"} 0.7700`,
		"sluice_fleet_targets 3",
		"sluice_fleet_targets_observed 2",
		`sluice_build_info{version="v0.100.0"`,
		"sluice_go_goroutines ",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("/metrics missing %q in:\n%s", want, out)
		}
	}
	if strings.Contains(out, `database="gamma"`) {
		t.Errorf("an unobserved target must contribute NO series:\n%s", out)
	}
	// A metric no target observed must not emit a header with no series.
	if strings.Contains(out, "sluice_target_replica_lag_seconds") {
		t.Errorf("an all-unobserved family must be omitted entirely:\n%s", out)
	}
}

// TestStartFleetWatchExporter_EmptyFleetIsHonest pins that an empty/warming
// fleet reports zero targets rather than omitting the gauges (which a
// dashboard would read as "no data" rather than "no databases").
func TestStartFleetWatchExporter_EmptyFleetIsHonest(t *testing.T) {
	out := scrapeFleetExporter(t, &fakeFleet{})
	if !strings.Contains(out, "sluice_fleet_targets 0") {
		t.Errorf("empty fleet must still report its size:\n%s", out)
	}
	if !strings.Contains(out, "sluice_fleet_targets_observed 0") {
		t.Errorf("empty fleet must still report the observed count:\n%s", out)
	}
}

// scrapeFleetExporter starts the fleet exporter on a free port and returns
// the /metrics body.
func scrapeFleetExporter(t *testing.T, fleet ir.FleetTelemetry) string {
	t.Helper()
	l, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("pick port: %v", err)
	}
	addr := l.Addr().String()
	_ = l.Close()

	stop, err := startFleetWatchExporter(t.Context(), addr, fleet, "v0.100.0", "deadbee", slog.Default())
	if err != nil {
		t.Fatalf("startFleetWatchExporter: %v", err)
	}
	defer stop()

	resp, err := http.Get("http://" + addr + "/metrics") //nolint:noctx // test
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	return string(body)
}

// TestMetricsDocSync_FleetExporter is the org-wide twin of
// [TestMetricsDocSync_RunningAsAService]: every series the FLEET exporter can
// emit must appear in the operator doc, so a new fleet metric cannot ship
// undocumented. (Direction 2 — "the doc's table can't claim a series that
// doesn't exist" — is already covered by the sync gate, whose table parser
// owns the `| `sluice_…` |` rows; the fleet-only gauges are documented in
// prose beneath that table precisely so the two gates don't fight.)
func TestMetricsDocSync_FleetExporter(t *testing.T) {
	// A fully-observed fleet, so the name inventory is the complete emit
	// surface rather than the subset a partially-known snapshot produces.
	full := ir.TargetHealthSnapshot{
		SampledAt: time.Now(),
		CPUUtil:   0.5, CPUKnown: true,
		MemUtil: 0.5, MemKnown: true,
		StorageUtil: 0.5, StorageAvailableBytes: 1 << 30, StorageCapacityBytes: 1 << 31, StorageKnown: true,
		// The worst-POD family must be ENABLED here or this gate cannot see
		// it: the exporter emits a family only when some target reports it
		// known, so a fixture leaving the flag false lets new series ship both
		// undocumented and unpinned — the exact drift this test exists to
		// catch, and the same fixture blind spot the single-database doc-sync
		// gate had. Deliberately fuller than the primary above, mirroring the
		// live PS-PG shape that motivated the signal.
		StorageUtilWorst: 0.75, StorageAvailableWorstBytes: 1 << 29, StorageCapacityWorstBytes: 1 << 31, StorageWorstKnown: true,
		ReplicaLagSeconds: 1.5, LagKnown: true,
		ActiveConnections: 3, MaxConnections: 100, ActiveConnKnown: true,
		MaxConnKnown: true,
	}
	var buf bytes.Buffer
	emitFleetTelemetryMetrics(&buf, []ir.FleetHealthSample{
		{Target: ir.FleetTarget{Database: "app", Branch: "main"}, Snapshot: full, OK: true},
	})

	typeRe := regexp.MustCompile(`(?m)^# TYPE (sluice_[a-z0-9_]+) `)
	matches := typeRe.FindAllStringSubmatch(buf.String(), -1)
	names := make([]string, 0, len(matches))
	for _, m := range matches {
		names = append(names, m[1])
	}
	// Vacuity guard: 8 target gauges + 2 fleet gauges. Adding a fleet metric
	// moves this number, which forces the doc update below.
	const wantSeries = 13
	if len(names) != wantSeries {
		t.Fatalf("fleet exporter emitted %d distinct series, want %d — if a metric was added/removed, update docs/operator/running-as-a-service.md and this count together: %v", len(names), wantSeries, names)
	}

	raw, err := os.ReadFile(filepath.Join("..", "..", "docs", "operator", "running-as-a-service.md"))
	if err != nil {
		t.Fatalf("read operator doc: %v", err)
	}
	doc := string(raw)
	for _, name := range names {
		if !regexp.MustCompile(regexp.QuoteMeta(name) + `\b`).MatchString(doc) {
			t.Errorf("series %q is emitted by the fleet exporter but never mentioned in docs/operator/running-as-a-service.md", name)
		}
	}
}

// --- (c) the persistent sink, through the watch loops ---

// TestRunFleetMetricsWatch_PersistsOneRecordPerTarget pins the org-wide sink
// path: every DISCOVERED target gets a row each tick, including the
// unobserved one (fresh=false with null metrics) so a portal sees a gap
// rather than a missing row.
func TestRunFleetMetricsWatch_PersistsOneRecordPerTarget(t *testing.T) {
	sink := &recordingSink{}
	unobserved := ir.FleetHealthSample{Target: ir.FleetTarget{Database: "gamma", Branch: "dev"}}
	err := RunMetricsWatch(t.Context(), nil, MetricsWatchConfig{
		Fleet: &fakeFleet{samples: []ir.FleetHealthSample{fleetSample("alpha", 0.3, 0.7), unobserved}},
		Label: "metrics-watch:acme",
		Sink:  sink,
		Once:  true,
	})
	if err != nil {
		t.Fatalf("fleet Once: %v", err)
	}
	recs := sink.all()
	if len(recs) != 2 {
		t.Fatalf("want one record per discovered target, got %d", len(recs))
	}
	alpha, gamma := recs[0], recs[1]
	if alpha.Database != "alpha" || alpha.Branch != "main" || !alpha.Fresh {
		t.Errorf("observed target row wrong: %+v", alpha)
	}
	if alpha.CPUUtil == nil || *alpha.CPUUtil != 0.3 {
		t.Errorf("observed cpu should be recorded, got %v", alpha.CPUUtil)
	}
	if gamma.Database != "gamma" || gamma.Branch != "dev" || gamma.Fresh {
		t.Errorf("unobserved target row wrong: %+v", gamma)
	}
	if gamma.CPUUtil != nil || gamma.StorageUtil != nil || gamma.SampledAt != nil {
		t.Errorf("an unobserved row must carry NULL metrics, not zeros: %+v", gamma)
	}
	if alpha.Watch != "metrics-watch:acme" {
		t.Errorf("record must carry the watch label, got %q", alpha.Watch)
	}
}

// TestRunMetricsWatch_SingleModePersistsTheSample pins the sink on the
// single-database path too, with the database/branch labels the CLI supplies.
func TestRunMetricsWatch_SingleModePersistsTheSample(t *testing.T) {
	sink := &recordingSink{}
	prov := &fakeTelemetry{ok: true, snap: ir.TargetHealthSnapshot{
		SampledAt: time.Now(),
		CPUUtil:   0.25, CPUKnown: true,
		ActiveConnections: 12, MaxConnections: 100, ActiveConnKnown: true,
		MaxConnKnown: true,
	}}
	err := RunMetricsWatch(t.Context(), prov, MetricsWatchConfig{
		Label: "metrics-watch:app", Database: "app", Branch: "main",
		Sink: sink, Once: true,
	})
	if err != nil {
		t.Fatalf("single Once: %v", err)
	}
	recs := sink.all()
	if len(recs) != 1 {
		t.Fatalf("want 1 record, got %d", len(recs))
	}
	r := recs[0]
	if r.Database != "app" || r.Branch != "main" || !r.Fresh {
		t.Errorf("record identity wrong: %+v", r)
	}
	if r.CPUUtil == nil || *r.CPUUtil != 0.25 {
		t.Errorf("cpu not recorded: %v", r.CPUUtil)
	}
	if r.ActiveConnections == nil || *r.ActiveConnections != 12 {
		t.Errorf("connections not recorded: %v", r.ActiveConnections)
	}
	// Unobserved families stay null.
	if r.MemUtil != nil || r.StorageUtil != nil || r.ReplicaLagSeconds != nil {
		t.Errorf("unobserved metrics must be null, got %+v", r)
	}
}

// TestRunMetricsWatch_SinkFailureIsSwallowed pins the advisory contract: a
// dead sink is logged and swallowed, never failing the poll.
func TestRunMetricsWatch_SinkFailureIsSwallowed(t *testing.T) {
	sink := &recordingSink{fail: true}
	prov := &fakeTelemetry{ok: true, snap: ir.TargetHealthSnapshot{SampledAt: time.Now()}}
	if err := RunMetricsWatch(t.Context(), prov, MetricsWatchConfig{Sink: sink, Once: true}); err != nil {
		t.Fatalf("a failing sink must never fail the watch: %v", err)
	}
	if len(sink.all()) != 1 {
		t.Error("the sink should still have been called")
	}

	// Same on the fleet path.
	fleetSink := &recordingSink{fail: true}
	err := RunMetricsWatch(t.Context(), nil, MetricsWatchConfig{
		Fleet: &fakeFleet{samples: []ir.FleetHealthSample{fleetSample("a", 0.1, 0.1)}},
		Sink:  fleetSink, Once: true,
	})
	if err != nil {
		t.Fatalf("a failing sink must never fail the fleet watch: %v", err)
	}
}

// TestSampleRecord_UnobservedIsNullNotZero pins the ir → record mapping at
// the source of the honesty contract: every *Known=false metric becomes nil.
func TestSampleRecord_UnobservedIsNullNotZero(t *testing.T) {
	now := time.Date(2026, 7, 24, 16, 0, 0, 0, time.UTC)
	// All metrics carry a non-zero VALUE but are flagged unobserved — the
	// shape that would silently persist a plausible-looking lie.
	snap := ir.TargetHealthSnapshot{
		SampledAt: now,
		CPUUtil:   0.9, MemUtil: 0.9, StorageUtil: 0.9,
		StorageAvailableBytes: 5, StorageCapacityBytes: 10,
		ReplicaLagSeconds: 7, ActiveConnections: 3, MaxConnections: 4,
	}
	rec := sampleRecord(now, "w", "db", "main", snap, true)
	if rec.CPUUtil != nil || rec.MemUtil != nil || rec.StorageUtil != nil ||
		rec.StorageAvailableBytes != nil || rec.StorageCapacityBytes != nil ||
		rec.ReplicaLagSeconds != nil || rec.ActiveConnections != nil || rec.MaxConnections != nil {
		t.Fatalf("unobserved metrics must map to nil, got %+v", rec)
	}
	if !rec.Fresh || rec.SampledAt == nil {
		t.Errorf("a fresh-but-unobserved tick still records its stamps: %+v", rec)
	}

	// ok=false ⇒ no SampledAt either.
	stale := sampleRecord(now, "w", "db", "main", snap, false)
	if stale.Fresh || stale.SampledAt != nil {
		t.Errorf("a no-sample tick must record fresh=false and a null sampled_at: %+v", stale)
	}
}

// TestFleetSampleRecords_EncodeThroughTheRealCodec closes the loop end to
// end: the records the watch produces survive the real encoder and decode
// back to the same values (no field the pipeline fills can be one the codec
// refuses).
func TestFleetSampleRecords_EncodeThroughTheRealCodec(t *testing.T) {
	now := time.Date(2026, 7, 24, 16, 0, 0, 0, time.UTC)
	recs := fleetSampleRecords(now, "metrics-watch:acme", []ir.FleetHealthSample{
		fleetSample("alpha", 0.3, 0.7),
		{Target: ir.FleetTarget{Database: "gamma", Branch: "dev"}},
	})
	var buf bytes.Buffer
	for _, r := range recs {
		line, err := telemetrysink.EncodeRecord(r)
		if err != nil {
			t.Fatalf("encode %+v: %v", r, err)
		}
		buf.Write(line)
	}
	lines := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("want 2 encoded lines, got %d", len(lines))
	}
	var got telemetrysink.Record
	if err := json.Unmarshal([]byte(lines[0]), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Database != "alpha" || got.CPUUtil == nil || *got.CPUUtil != 0.3 {
		t.Errorf("round-trip lost fidelity: %+v", got)
	}
	if !strings.Contains(lines[1], `"cpu_util":null`) {
		t.Errorf("unobserved target must persist a null, got %s", lines[1])
	}
}

// TestRunMetricsWatch_NilProviderAndNilFleetIsAHardError pins that the watch
// still refuses to start with no telemetry source at all.
func TestRunMetricsWatch_NilProviderAndNilFleetIsAHardError(t *testing.T) {
	if err := RunMetricsWatch(t.Context(), nil, MetricsWatchConfig{Once: true}); err == nil {
		t.Fatal("no provider and no fleet must be a hard error")
	}
}

// newRecordingWebhook serves a webhook that appends each POST body to bodies.
func newRecordingWebhook(t *testing.T, mu *sync.Mutex, bodies *[]string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		*bodies = append(*bodies, string(body))
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
}
