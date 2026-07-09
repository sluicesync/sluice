// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package diagnose

import (
	"archive/zip"
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"runtime"
	"sort"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// Request is the operator's diagnose-bundle request, populated by the
// CLI subcommand and passed into [Write]. Each field's privacy
// contract is documented at the field; the assembler honours the
// PrivacyLevel by silently omitting sections rather than failing when
// the operator opts down.
type Request struct {
	// StreamID is the stream the bundle is scoped to. Required.
	StreamID string

	// PrivacyLevel controls which sections contribute (see the
	// PrivacyLevel doc + ADR-0056 for the per-level inclusion
	// contract).
	PrivacyLevel PrivacyLevel

	// SourceEngine + SourceDSN identify the source side (optional —
	// the operator may diagnose a target-only stream if the source is
	// unreachable). Engine + DSN pair only ever appear together.
	SourceEngine ir.Engine
	SourceDSN    string

	// TargetEngine + TargetDSN identify the target side. Required.
	TargetEngine ir.Engine
	TargetDSN    string

	// SlotName is the optional slot name (PG-only); empty falls back
	// to the engine's default slot name. Mirrors `sluice sync health`'s
	// --slot-name flag.
	SlotName string

	// SluiceVersion + SluiceCommit + SluiceBuildDate identify the
	// running sluice binary. Populated by the CLI; absent at
	// PrivacyBasic.
	SluiceVersion   string
	SluiceCommit    string
	SluiceBuildDate string

	// CLIArgs is the raw command-line argv the operator invoked sluice
	// with. The assembler runs [RedactCLIArgs] before embedding. Empty
	// at PrivacyBasic.
	CLIArgs []string

	// LogFile is the path to sluice's slog output file. PrivacyVerbose
	// includes the last VerboseLogTailLines lines. Empty disables.
	LogFile string

	// CrashContext is set by the auto-on-crash hook to carry the
	// original error the hook caught. Empty for operator-initiated
	// bundles.
	CrashContext string

	// TargetTelemetry is an optional control-plane telemetry provider
	// (ADR-0107 — today: PlanetScale metrics) for the target. When set,
	// PrivacyStandard+ bundles include a "Target health" section with the
	// most recent CPU/mem/storage/lag/connection snapshot, so a recipient
	// sees WHY apply was slow (a hot or storage-constrained target) without
	// leaving the bundle. nil ⇒ the section records "telemetry not
	// configured" — the honest absence, never a fabricated reading.
	TargetTelemetry ir.TargetTelemetry

	// Now overrides the assembler's wall-clock for tests. Production
	// callers leave this zero and the assembler uses time.Now().
	Now time.Time
}

// Write assembles the diagnose bundle described by req and writes it
// to w as a ZIP archive. Returns an error only on a structural
// failure (the request is malformed, the manifest itself cannot be
// written); per-section probe AND write failures are recorded in the
// bundle — reason files for probe failures, sections.json rows for
// everything — and DO NOT fail the overall write.
//
// Best-effort collection is the design tenet — see [DiagnoseProber]'s
// doc. The bundle exists to help the operator file a useful GH issue;
// refusing to write a bundle because one probe couldn't reach the
// source would defeat the purpose. What keeps best-effort honest is
// sections.json: every section attempted lands there as written /
// skipped(reason) / failed(reason), so a bundle can never silently
// omit a section (audit 2026-07-08 §4.4). The one structural error
// class is "the request itself is invalid" (no stream-id, no privacy
// level) — those refuse loudly.
func Write(ctx context.Context, w io.Writer, req Request) error {
	if req.StreamID == "" {
		return errors.New("diagnose: request StreamID is empty")
	}
	if req.PrivacyLevel == PrivacyUnset {
		return errors.New("diagnose: request PrivacyLevel is unset; expected basic|standard|verbose")
	}

	zw := zip.NewWriter(w)
	defer func() { _ = zw.Close() }()
	b := &bundleWriter{zw: zw}

	now := req.Now
	if now.IsZero() {
		now = time.Now()
	}

	mf := Manifest{
		ManifestVersion: ManifestVersion,
		GeneratedAt:     now.UTC().Format(time.RFC3339),
		PrivacyLevel:    req.PrivacyLevel.String(),
		StreamID:        req.StreamID,
		CrashContext:    req.CrashContext,
	}
	if req.PrivacyLevel >= PrivacyStandard {
		mf.SluiceVersion = req.SluiceVersion
		mf.SluiceCommit = req.SluiceCommit
		mf.SluiceBuildDate = req.SluiceBuildDate
		mf.GoVersion = runtime.Version()
		mf.GOOS = runtime.GOOS
		mf.GOARCH = runtime.GOARCH
		if req.SourceDSN != "" {
			mf.SourceDSNRedacted = RedactDSN(req.SourceDSN)
		}
		if req.TargetDSN != "" {
			mf.TargetDSNRedacted = RedactDSN(req.TargetDSN)
		}
		if req.SourceEngine != nil {
			mf.SourceEngine = req.SourceEngine.Name()
		}
		if req.TargetEngine != nil {
			mf.TargetEngine = req.TargetEngine.Name()
		}
	}

	if err := writeJSON(zw, "bundle.json", mf); err != nil {
		return fmt.Errorf("diagnose: write manifest: %w", err)
	}
	b.noteWritten("bundle.json")

	// PrivacyBasic and up: state-table dumps.
	collectStateTables(ctx, b, req)

	if req.PrivacyLevel >= PrivacyStandard {
		collectStandardSections(ctx, b, req)
	}

	if req.PrivacyLevel >= PrivacyVerbose {
		collectVerboseSections(ctx, b, req)
	}

	// sections.json is the bundle's own account of every section it
	// attempted. A failure to write IT is structural — without the
	// account, a silently-absent section becomes possible again.
	if err := writeJSON(zw, "sections.json", b.sections); err != nil {
		return fmt.Errorf("diagnose: write sections manifest: %w", err)
	}

	return zw.Close()
}

// sectionOutcome is one row of sections.json — the bundle's account of
// a section it attempted. Status values: "written" (the section file
// landed), "skipped" (an honest-absence reason file was written
// instead — probe failed, interface not implemented, input not
// configured), "failed" (the section file itself could not be written
// — encode or zip error). Failed sections are exactly the class that
// used to vanish behind `_ = writeJSON(...)`.
type sectionOutcome struct {
	Name   string `json:"name"`
	Status string `json:"status"`
	Reason string `json:"reason,omitempty"`
}

// bundleWriter pairs the zip writer with the per-section outcome log
// behind sections.json. Section writes stay best-effort by design
// (see [Write]); the recorder is what keeps best-effort honest — a
// write error no longer disappears, it lands in the manifest.
type bundleWriter struct {
	zw       *zip.Writer
	sections []sectionOutcome
}

// writeJSON writes one JSON section and records its outcome. Never
// fails the bundle.
func (b *bundleWriter) writeJSON(name string, payload any) {
	if err := writeJSON(b.zw, name, payload); err != nil {
		b.sections = append(b.sections, sectionOutcome{Name: name, Status: "failed", Reason: err.Error()})
		return
	}
	b.noteWritten(name)
}

// writeReason writes an honest-absence reason file for a section and
// records it as skipped. Never fails the bundle: if even the reason
// file cannot be written, the section is recorded as failed with both
// reasons.
func (b *bundleWriter) writeReason(prefix, reason string) {
	if err := writeReason(b.zw, prefix, reason); err != nil {
		b.sections = append(b.sections, sectionOutcome{
			Name:   prefix,
			Status: "failed",
			Reason: reason + " (and the reason file could not be written: " + err.Error() + ")",
		})
		return
	}
	b.sections = append(b.sections, sectionOutcome{Name: prefix, Status: "skipped", Reason: reason})
}

// noteWritten records a section a non-bundleWriter path wrote
// successfully (bundle.json, the log tail).
func (b *bundleWriter) noteWritten(name string) {
	b.sections = append(b.sections, sectionOutcome{Name: name, Status: "written"})
}

// collectStateTables writes the PrivacyBasic state-table dumps:
// sluice_cdc_state row, sluice_cdc_schema_history rows (capped),
// sluice_shard_consolidation_lease rows. Each section probes a
// type-asserted interface on the target's ChangeApplier; engines that
// don't implement a section get a "reason" file rather than a missing
// section so the recipient can tell "absent because not supported" from
// "absent because the probe failed".
func collectStateTables(ctx context.Context, b *bundleWriter, req Request) {
	if req.TargetEngine == nil || req.TargetDSN == "" {
		b.writeReason("state/", "target engine or DSN missing; state-table dump skipped")
		return
	}
	applier, err := req.TargetEngine.OpenChangeApplier(ctx, req.TargetDSN)
	if err != nil {
		b.writeReason("state/", fmt.Sprintf("open target applier: %v", err))
		return
	}
	defer closeIfCloser(applier)

	// sluice_cdc_state — scoped to the request's stream-id.
	streams, lerr := applier.ListStreams(ctx)
	if lerr != nil {
		b.writeReason("state/cdc_state", fmt.Sprintf("list streams: %v", lerr))
	} else {
		var matched []ir.StreamStatus
		for _, s := range streams {
			if s.StreamID == req.StreamID {
				matched = append(matched, s)
			}
		}
		b.writeJSON("state/cdc_state.json", renderStreamStatuses(matched))
	}

	// sluice_cdc_schema_history — capped to SchemaHistoryRowCap.
	if reader, ok := applier.(ir.SchemaHistoryReader); ok {
		rows, herr := reader.ListSchemaHistory(ctx, req.StreamID, SchemaHistoryRowCap)
		if herr != nil {
			b.writeReason("state/schema_history", fmt.Sprintf("list schema history: %v", herr))
		} else {
			b.writeJSON("state/schema_history.json", rows)
		}
	} else {
		b.writeReason("state/schema_history",
			fmt.Sprintf("engine %q does not implement ir.SchemaHistoryReader", req.TargetEngine.Name()))
	}

	// sluice_shard_consolidation_lease — engine-wide listing per ADR-0054.
	if lister, ok := applier.(ir.ShardConsolidationLeaseLister); ok {
		leases, lerr := lister.ListLeases(ctx)
		if lerr != nil {
			b.writeReason("state/shard_consolidation_lease", fmt.Sprintf("list leases: %v", lerr))
		} else {
			b.writeJSON("state/shard_consolidation_lease.json", leases)
		}
	} else {
		b.writeReason("state/shard_consolidation_lease",
			fmt.Sprintf("engine %q does not implement ir.ShardConsolidationLeaseLister", req.TargetEngine.Name()))
	}
}

// collectStandardSections writes the PrivacyStandard additions: CLI
// args (redacted), engine snapshots via ir.DiagnoseProber, health
// probes mirroring `sluice sync health`.
func collectStandardSections(ctx context.Context, b *bundleWriter, req Request) {
	// CLI args (redacted) + an effective-config snapshot the bundle
	// recipient can re-run the sluice invocation against (with
	// secret material stripped).
	if len(req.CLIArgs) > 0 {
		b.writeJSON("config/cli_args.json", RedactCLIArgs(req.CLIArgs))
	}

	// Capabilities — declared shape per engine. Mirrors
	// `sluice engines` but per side. Always populated when the engine
	// is set.
	caps := struct {
		Source *ir.Capabilities `json:"source,omitempty"`
		Target *ir.Capabilities `json:"target,omitempty"`
	}{}
	if req.SourceEngine != nil {
		c := req.SourceEngine.Capabilities()
		caps.Source = &c
	}
	if req.TargetEngine != nil {
		c := req.TargetEngine.Capabilities()
		caps.Target = &c
	}
	b.writeJSON("engine/capabilities.json", caps)

	// Engine snapshots — ir.DiagnoseProber on each side.
	if req.SourceEngine != nil && req.SourceDSN != "" {
		probeAndWriteDiagnose(ctx, b, "engine/source_diagnose", req.SourceEngine, req.SourceDSN, req.StreamID)
	}
	if req.TargetEngine != nil && req.TargetDSN != "" {
		probeAndWriteDiagnose(ctx, b, "engine/target_diagnose", req.TargetEngine, req.TargetDSN, req.StreamID)
	}

	// Health probe mirrors `sluice sync health` JSON. Only fires
	// when source + target are both set (the cross-engine lag-bytes
	// needs both sides; the standalone target-side freshness lives
	// in the cdc_state dump already).
	if req.SourceEngine != nil && req.SourceDSN != "" && req.TargetEngine != nil && req.TargetDSN != "" {
		probeAndWriteHealth(ctx, b, "health/sync_health.json", req)
	}

	// Target health — the ADR-0107 control-plane telemetry snapshot
	// (CPU/mem/storage/lag/conns). Always emitted at PrivacyStandard+ so
	// the recipient can tell "telemetry not configured" from "configured but
	// no fresh sample" from a real reading.
	probeAndWriteTargetHealth(ctx, b, "health/target_health.json", req)

	// Target metrics ROLLING HISTORY — the ADR-0107 item 35 persisted trend
	// (recent rows + current/1m/5m/10m avg+max aggregates for cpu/mem/
	// storage). Read from the sluice_target_metrics_history table on the
	// target via the optional ir.TargetMetricsHistoryStore surface.
	probeAndWriteTargetMetricsHistory(ctx, b, "health/target_metrics_history.json", req)
}

// targetMetricsHistoryRowCap bounds the rolling-history rows embedded in the
// bundle: 120 rows ≈ 2h at the ~60s scrape cadence — enough trend for a
// recipient diagnosing a slow apply without ballooning the bundle.
const targetMetricsHistoryRowCap = 120

// probeAndWriteTargetMetricsHistory writes the ADR-0107 item 35
// rolling-history section: the recent persisted samples plus current /
// 1m / 5m / 10m avg+max aggregates for cpu/mem/storage. Honest states,
// mirroring the other section helpers (reason file, never fail the bundle):
//   - target engine/DSN missing ⇒ reason file,
//   - engine's applier doesn't implement ir.TargetMetricsHistoryStore ⇒
//     reason file (so "not supported" is distinguishable from "no rows"),
//   - no rows ⇒ {recent: [], aggregates: {...empty windows...}}.
func probeAndWriteTargetMetricsHistory(ctx context.Context, b *bundleWriter, name string, req Request) {
	if req.TargetEngine == nil || req.TargetDSN == "" {
		b.writeReason(name, "target engine or DSN missing; target-metrics history skipped")
		return
	}
	applier, err := req.TargetEngine.OpenChangeApplier(ctx, req.TargetDSN)
	if err != nil {
		b.writeReason(name, fmt.Sprintf("open target applier: %v", err))
		return
	}
	defer closeIfCloser(applier)

	store, ok := applier.(ir.TargetMetricsHistoryStore)
	if !ok {
		b.writeReason(name,
			fmt.Sprintf("engine %q does not implement ir.TargetMetricsHistoryStore", req.TargetEngine.Name()))
		return
	}
	rows, err := store.ListTargetMetricsHistory(ctx, req.StreamID, targetMetricsHistoryRowCap)
	if err != nil {
		b.writeReason(name, fmt.Sprintf("list target-metrics history: %v", err))
		return
	}
	b.writeJSON(name, map[string]any{
		"recent":     renderTargetMetricsRows(rows),
		"aggregates": computeTargetMetricsAggregates(rows),
	})
}

// renderTargetMetricsRows formats the persisted history rows for the
// bundle, gating each metric by its *Known flag — an unobserved value is
// OMITTED (never rendered as 0/idle), the same honesty contract the live
// target_health section keeps.
func renderTargetMetricsRows(rows []ir.TargetMetricsHistoryRow) []map[string]any {
	out := make([]map[string]any, 0, len(rows))
	for _, r := range rows {
		m := map[string]any{
			"sampled_at": r.SampledAt.UTC().Format(time.RFC3339),
		}
		if r.Database != "" {
			m["database"] = r.Database
		}
		if r.Branch != "" {
			m["branch"] = r.Branch
		}
		if r.CPUKnown {
			m["cpu_util"] = r.CPUUtil
		}
		if r.MemKnown {
			m["mem_util"] = r.MemUtil
		}
		if r.StorageKnown {
			m["storage_util"] = r.StorageUtil
			m["storage_available_bytes"] = r.StorageAvailableBytes
			m["storage_capacity_bytes"] = r.StorageCapacityBytes
		}
		if r.LagKnown {
			m["replica_lag_seconds"] = r.ReplicaLagSeconds
		}
		if r.ConnKnown {
			m["active_connections"] = r.ActiveConnections
			m["max_connections"] = r.MaxConnections
		}
		out = append(out, m)
	}
	return out
}

// computeTargetMetricsAggregates rolls the recent history into the
// current value + 1m/5m/10m avg+max windows for cpu/mem/storage, computed
// RELATIVE TO THE NEWEST ROW (rows arrive sampled_at DESC). Only *Known
// values contribute to a window; a window with no observed value omits its
// avg/max (honest absence, never a fabricated 0). The "current" value is
// the newest *Known reading for that metric.
func computeTargetMetricsAggregates(rows []ir.TargetMetricsHistoryRow) map[string]any {
	cpu := metricSeries(rows, func(r ir.TargetMetricsHistoryRow) (float64, bool) {
		return r.CPUUtil, r.CPUKnown
	})
	mem := metricSeries(rows, func(r ir.TargetMetricsHistoryRow) (float64, bool) {
		return r.MemUtil, r.MemKnown
	})
	storage := metricSeries(rows, func(r ir.TargetMetricsHistoryRow) (float64, bool) {
		return r.StorageUtil, r.StorageKnown
	})
	return map[string]any{
		"cpu":     cpu,
		"mem":     mem,
		"storage": storage,
	}
}

// metricSeries computes one metric's {current, avg_1m, max_1m, avg_5m,
// max_5m, avg_10m, max_10m} over the *Known samples, windowed relative to
// the newest row's sampled_at. rows are sampled_at DESC (newest first).
func metricSeries(rows []ir.TargetMetricsHistoryRow, sel func(ir.TargetMetricsHistoryRow) (float64, bool)) map[string]any {
	out := map[string]any{}
	if len(rows) == 0 {
		return out
	}
	// newest is the reference instant for the windows (rows are DESC).
	newest := rows[0].SampledAt
	// current = newest *Known reading for this metric.
	for _, r := range rows {
		if v, ok := sel(r); ok {
			out["current"] = v
			break
		}
	}
	windows := []struct {
		suffix string
		dur    time.Duration
	}{
		{"1m", 1 * time.Minute},
		{"5m", 5 * time.Minute},
		{"10m", 10 * time.Minute},
	}
	for _, w := range windows {
		var sum float64
		var n int
		var maxV float64
		var sawMax bool
		floor := newest.Add(-w.dur)
		for _, r := range rows {
			// Inclusive of the floor instant so a sample exactly w old counts.
			if r.SampledAt.Before(floor) {
				continue
			}
			v, ok := sel(r)
			if !ok {
				continue
			}
			sum += v
			n++
			if !sawMax || v > maxV {
				maxV = v
				sawMax = true
			}
		}
		if n > 0 {
			out["avg_"+w.suffix] = sum / float64(n)
			out["max_"+w.suffix] = maxV
		}
	}
	return out
}

// probeAndWriteTargetHealth writes the ADR-0107 target-telemetry snapshot. It
// is best-effort and honest: no provider ⇒ a reason file; a provider with no
// fresh sample ⇒ {"fresh": false}; a fresh sample ⇒ the distilled
// CPU/mem/storage/lag/connection view with each value gated by its *Known
// flag (an unobserved metric is omitted, never reported as 0/idle).
func probeAndWriteTargetHealth(ctx context.Context, b *bundleWriter, name string, req Request) {
	if req.TargetTelemetry == nil {
		b.writeReason(name, "target telemetry not configured (no --planetscale-org / metrics token)")
		return
	}
	snap, ok := req.TargetTelemetry.Sample(ctx)
	if !ok {
		b.writeJSON(name, map[string]any{"fresh": false, "reason": "no fresh telemetry sample available"})
		return
	}
	out := map[string]any{
		"fresh":      true,
		"sampled_at": snap.SampledAt.UTC().Format(time.RFC3339),
	}
	if snap.CPUKnown {
		out["cpu_util"] = snap.CPUUtil
	}
	if snap.MemKnown {
		out["mem_util"] = snap.MemUtil
	}
	if snap.StorageKnown {
		out["storage_util"] = snap.StorageUtil
		out["storage_available_bytes"] = snap.StorageAvailableBytes
		out["storage_capacity_bytes"] = snap.StorageCapacityBytes
	}
	if snap.LagKnown {
		out["replica_lag_seconds"] = snap.ReplicaLagSeconds
	}
	if snap.ConnKnown {
		out["active_connections"] = snap.ActiveConnections
		out["max_connections"] = snap.MaxConnections
	}
	b.writeJSON(name, out)
}

// collectVerboseSections writes the PrivacyVerbose additions: per-
// table row counts (slow path, opt-in-only), log-file tail.
func collectVerboseSections(ctx context.Context, b *bundleWriter, req Request) {
	// Per-table row counts on the TARGET. Slow path — the operator
	// opted in by selecting PrivacyVerbose. Skipped if the target
	// can't be probed.
	if req.TargetEngine != nil && req.TargetDSN != "" {
		probeAndWriteRowCounts(ctx, b, req)
	}

	// Log-file tail. The operator must have configured a --log-file
	// path; sluice does NOT scrape the parent process's stderr.
	if req.LogFile != "" {
		if err := writeLogTail(b.zw, req.LogFile); err != nil {
			b.writeReason("logs/log_tail", fmt.Sprintf("read log file %q: %v", req.LogFile, err))
		} else {
			b.noteWritten("logs/log_tail.txt")
		}
	}
}

// probeAndWriteDiagnose runs ir.DiagnoseProber against the given
// engine + DSN. Errors are surfaced as a reason file rather than
// propagated.
func probeAndWriteDiagnose(ctx context.Context, b *bundleWriter, prefix string, engine ir.Engine, dsn, streamID string) {
	sr, err := engine.OpenSchemaReader(ctx, dsn)
	if err != nil {
		b.writeReason(prefix, fmt.Sprintf("open schema reader: %v", err))
		return
	}
	defer closeIfCloser(sr)

	prober, ok := sr.(ir.DiagnoseProber)
	if !ok {
		b.writeReason(prefix, fmt.Sprintf("engine %q does not implement ir.DiagnoseProber", engine.Name()))
		return
	}
	snap, err := prober.DiagnoseBundle(ctx, streamID)
	if err != nil {
		b.writeReason(prefix, fmt.Sprintf("diagnose probe: %v", err))
		return
	}
	b.writeJSON(prefix+".json", snap)
}

// probeAndWriteHealth mirrors `sluice sync health`'s probe surface
// against the same two-engine wire format. Failures are bundled as
// reason strings.
func probeAndWriteHealth(ctx context.Context, b *bundleWriter, name string, req Request) {
	// Target side — list streams, find the one matching req.StreamID.
	applier, err := req.TargetEngine.OpenChangeApplier(ctx, req.TargetDSN)
	if err != nil {
		b.writeReason(name, fmt.Sprintf("open target applier: %v", err))
		return
	}
	defer closeIfCloser(applier)

	streams, err := applier.ListStreams(ctx)
	if err != nil {
		b.writeReason(name, fmt.Sprintf("list streams: %v", err))
		return
	}
	var targetPos ir.Position
	var found bool
	var updatedAt time.Time
	for _, s := range streams {
		if s.StreamID == req.StreamID {
			targetPos = s.Position
			updatedAt = s.UpdatedAt
			found = true
			break
		}
	}

	out := map[string]any{
		"stream_id":  req.StreamID,
		"found":      found,
		"updated_at": updatedAt.UTC().Format(time.RFC3339),
	}

	// Source side health: type-assert HealthReporter, then
	// BytesLagReporter, then SlotSpillReporter.
	sr, err := req.SourceEngine.OpenSchemaReader(ctx, req.SourceDSN)
	if err != nil {
		out["source_probe_reason"] = fmt.Sprintf("open source schema reader: %v", err)
		b.writeJSON(name, out)
		return
	}
	defer closeIfCloser(sr)

	hr, ok := sr.(ir.HealthReporter)
	if !ok {
		out["source_probe_reason"] = fmt.Sprintf("engine %q does not implement ir.HealthReporter", req.SourceEngine.Name())
		b.writeJSON(name, out)
		return
	}
	pos, err := hr.SourceCurrentPosition(ctx)
	if err != nil {
		out["source_probe_reason"] = fmt.Sprintf("source-current-position: %v", err)
		b.writeJSON(name, out)
		return
	}
	out["source_position"] = pos.Token

	if lagger, ok := sr.(ir.BytesLagReporter); ok && targetPos.Token != "" {
		lag, lerr := lagger.LagBytes(ctx, targetPos, pos)
		if lerr == nil {
			out["lag_bytes"] = lag
		} else {
			out["lag_bytes_reason"] = lerr.Error()
		}
	}
	if spiller, ok := sr.(ir.SlotSpillReporter); ok {
		slot := req.SlotName
		if slot == "" && req.SourceEngine.Name() == "postgres" {
			slot = "sluice_slot"
		}
		if slot != "" {
			stats, statsOK, sperr := spiller.SlotSpillStats(ctx, slot)
			if sperr != nil {
				out["spill_reason"] = sperr.Error()
			} else if statsOK {
				out["spill_txns"] = stats.SpillTxns
				out["spill_bytes"] = stats.SpillBytes
			}
		}
	}

	b.writeJSON(name, out)
}

// probeAndWriteRowCounts writes per-table COUNT(*) on the target. The
// schema is read first to enumerate the filtered table list; counts
// are best-effort per table.
func probeAndWriteRowCounts(ctx context.Context, b *bundleWriter, req Request) {
	reader, err := req.TargetEngine.OpenSchemaReader(ctx, req.TargetDSN)
	if err != nil {
		b.writeReason("verbose/row_counts", fmt.Sprintf("open target schema reader: %v", err))
		return
	}
	defer closeIfCloser(reader)

	schema, err := reader.ReadSchema(ctx)
	if err != nil {
		b.writeReason("verbose/row_counts", fmt.Sprintf("read target schema: %v", err))
		return
	}

	rowReader, err := req.TargetEngine.OpenRowReader(ctx, req.TargetDSN)
	if err != nil {
		b.writeReason("verbose/row_counts", fmt.Sprintf("open target row reader: %v", err))
		return
	}
	defer closeIfCloser(rowReader)

	counter, ok := rowReader.(ir.RowCounter)
	if !ok {
		b.writeReason("verbose/row_counts",
			fmt.Sprintf("engine %q row reader does not implement ir.RowCounter", req.TargetEngine.Name()))
		return
	}

	type rowCountEntry struct {
		Schema string `json:"schema,omitempty"`
		Table  string `json:"table"`
		Count  int64  `json:"count,omitempty"`
		Reason string `json:"reason,omitempty"`
	}
	out := make([]rowCountEntry, 0, len(schema.Tables))
	for _, t := range schema.Tables {
		if t == nil {
			continue
		}
		entry := rowCountEntry{Schema: t.Schema, Table: t.Name}
		n, err := counter.CountRows(ctx, t)
		if err != nil {
			entry.Reason = err.Error()
		} else {
			entry.Count = n
		}
		out = append(out, entry)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Schema != out[j].Schema {
			return out[i].Schema < out[j].Schema
		}
		return out[i].Table < out[j].Table
	})
	b.writeJSON("verbose/row_counts.json", out)
}

// writeLogTail reads the last VerboseLogTailLines lines of path and
// writes them as logs/log_tail.txt. A naive whole-file read keeps the
// implementation small; large log files are bounded by the operator's
// log-rotation discipline.
func writeLogTail(zw *zip.Writer, path string) error {
	f, err := os.Open(path) //nolint:gosec // path is operator-supplied via --log-file
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	// Ring buffer of the last VerboseLogTailLines lines.
	lines := make([]string, 0, VerboseLogTailLines)
	sc := bufio.NewScanner(f)
	// Generous buffer for long structured-slog lines.
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		if len(lines) == VerboseLogTailLines {
			lines = lines[1:]
		}
		lines = append(lines, sc.Text())
	}
	if err := sc.Err(); err != nil {
		return err
	}

	hdr := &zip.FileHeader{Name: "logs/log_tail.txt", Method: zip.Deflate}
	w, err := zw.CreateHeader(hdr)
	if err != nil {
		return err
	}
	for _, line := range lines {
		if _, err := io.WriteString(w, line+"\n"); err != nil {
			return err
		}
	}
	return nil
}

// renderStreamStatuses formats StreamStatus rows for embedding. Time
// fields are RFC3339 UTC; Position is rendered as the engine-opaque
// token (NOT truncated — the bundle is for forensic inspection,
// readability matters less than completeness).
func renderStreamStatuses(rows []ir.StreamStatus) []map[string]any {
	out := make([]map[string]any, 0, len(rows))
	for _, r := range rows {
		out = append(out, map[string]any{
			"stream_id":              r.StreamID,
			"position_engine":        r.Position.Engine,
			"position_token":         r.Position.Token,
			"updated_at":             r.UpdatedAt.UTC().Format(time.RFC3339),
			"slot_name":              r.SlotName,
			"source_dsn_fingerprint": r.SourceDSNFingerprint,
			"target_schema":          r.TargetSchema,
		})
	}
	return out
}

// writeJSON encodes payload as indented JSON and writes it into zw at
// the named path. The leading "byte-order-mark"-free encoding is
// stable across Go versions; bundles ship as plain UTF-8 JSON.
// Encoding happens BEFORE the zip entry is created so an encode
// failure leaves no phantom empty entry masquerading as a section —
// the failure lands only in sections.json.
func writeJSON(zw *zip.Writer, name string, payload any) error {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetIndent("", "  ")
	if err := enc.Encode(payload); err != nil {
		return err
	}
	w, err := zw.CreateHeader(&zip.FileHeader{Name: name, Method: zip.Deflate})
	if err != nil {
		return err
	}
	_, err = w.Write(buf.Bytes())
	return err
}

// writeReason writes a `<prefix>/__skipped.txt` file carrying a short
// human-readable reason the section was omitted. Recipients can tell
// "section absent because engine doesn't implement the interface"
// from "section absent because probe failed" by reading the file.
func writeReason(zw *zip.Writer, prefix, reason string) error {
	name := prefix + "/__skipped.txt"
	if prefix != "" && prefix[len(prefix)-1] == '/' {
		name = prefix + "__skipped.txt"
	}
	w, err := zw.CreateHeader(&zip.FileHeader{Name: name, Method: zip.Deflate})
	if err != nil {
		return err
	}
	_, err = io.WriteString(w, reason+"\n")
	return err
}

// closeIfCloser is the standard `if c, ok := x.(io.Closer); ok` shape
// in a single-line helper. Errors are swallowed deliberately —
// closing a read-only reader after the bundle assembled successfully
// is best-effort.
func closeIfCloser(x any) {
	if c, ok := x.(io.Closer); ok {
		if err := c.Close(); err != nil {
			slog.Debug("diagnose: close failed", slog.String("err", err.Error()))
		}
	}
}
