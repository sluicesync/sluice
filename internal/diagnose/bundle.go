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
// failure (zip writer fails, the request is malformed); per-section
// probe failures are recorded in the bundle as section-level reason
// strings and DO NOT fail the overall write.
//
// Best-effort collection is the design tenet — see [DiagnoseProber]'s
// doc. The bundle exists to help the operator file a useful GH issue;
// refusing to write a bundle because one probe couldn't reach the
// source would defeat the purpose. The one structural error class is
// "the request itself is invalid" (no stream-id, no privacy level) —
// those refuse loudly.
func Write(ctx context.Context, w io.Writer, req Request) error {
	if req.StreamID == "" {
		return errors.New("diagnose: request StreamID is empty")
	}
	if req.PrivacyLevel == PrivacyUnset {
		return errors.New("diagnose: request PrivacyLevel is unset; expected basic|standard|verbose")
	}

	zw := zip.NewWriter(w)
	defer func() { _ = zw.Close() }()

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

	// PrivacyBasic and up: state-table dumps.
	if err := collectStateTables(ctx, zw, req); err != nil {
		// Already best-effort inside the helper; an error here means
		// the zip writer itself failed.
		return fmt.Errorf("diagnose: collect state tables: %w", err)
	}

	if req.PrivacyLevel >= PrivacyStandard {
		if err := collectStandardSections(ctx, zw, req); err != nil {
			return fmt.Errorf("diagnose: collect standard sections: %w", err)
		}
	}

	if req.PrivacyLevel >= PrivacyVerbose {
		if err := collectVerboseSections(ctx, zw, req); err != nil {
			return fmt.Errorf("diagnose: collect verbose sections: %w", err)
		}
	}

	return zw.Close()
}

// collectStateTables writes the PrivacyBasic state-table dumps:
// sluice_cdc_state row, sluice_cdc_schema_history rows (capped),
// sluice_shard_consolidation_lease rows. Each section probes a
// type-asserted interface on the target's ChangeApplier; engines that
// don't implement a section get a "reason" file rather than a missing
// section so the recipient can tell "absent because not supported" from
// "absent because the probe failed".
func collectStateTables(ctx context.Context, zw *zip.Writer, req Request) error {
	if req.TargetEngine == nil || req.TargetDSN == "" {
		return writeReason(zw, "state/", "target engine or DSN missing; state-table dump skipped")
	}
	applier, err := req.TargetEngine.OpenChangeApplier(ctx, req.TargetDSN)
	if err != nil {
		return writeReason(zw, "state/", fmt.Sprintf("open target applier: %v", err))
	}
	defer closeIfCloser(applier)

	// sluice_cdc_state — scoped to the request's stream-id.
	streams, lerr := applier.ListStreams(ctx)
	if lerr != nil {
		if err := writeReason(zw, "state/cdc_state", fmt.Sprintf("list streams: %v", lerr)); err != nil {
			return err
		}
	} else {
		var matched []ir.StreamStatus
		for _, s := range streams {
			if s.StreamID == req.StreamID {
				matched = append(matched, s)
			}
		}
		if err := writeJSON(zw, "state/cdc_state.json", renderStreamStatuses(matched)); err != nil {
			return err
		}
	}

	// sluice_cdc_schema_history — capped to SchemaHistoryRowCap.
	if reader, ok := applier.(ir.SchemaHistoryReader); ok {
		rows, herr := reader.ListSchemaHistory(ctx, req.StreamID, SchemaHistoryRowCap)
		if herr != nil {
			if err := writeReason(zw, "state/schema_history", fmt.Sprintf("list schema history: %v", herr)); err != nil {
				return err
			}
		} else if err := writeJSON(zw, "state/schema_history.json", rows); err != nil {
			return err
		}
	} else {
		if err := writeReason(zw, "state/schema_history",
			fmt.Sprintf("engine %q does not implement ir.SchemaHistoryReader", req.TargetEngine.Name())); err != nil {
			return err
		}
	}

	// sluice_shard_consolidation_lease — engine-wide listing per ADR-0054.
	if lister, ok := applier.(ir.ShardConsolidationLeaseLister); ok {
		leases, lerr := lister.ListLeases(ctx)
		if lerr != nil {
			if err := writeReason(zw, "state/shard_consolidation_lease",
				fmt.Sprintf("list leases: %v", lerr)); err != nil {
				return err
			}
		} else if err := writeJSON(zw, "state/shard_consolidation_lease.json", leases); err != nil {
			return err
		}
	} else {
		if err := writeReason(zw, "state/shard_consolidation_lease",
			fmt.Sprintf("engine %q does not implement ir.ShardConsolidationLeaseLister", req.TargetEngine.Name())); err != nil {
			return err
		}
	}

	return nil
}

// collectStandardSections writes the PrivacyStandard additions: CLI
// args (redacted), engine snapshots via ir.DiagnoseProber, health
// probes mirroring `sluice sync health`.
func collectStandardSections(ctx context.Context, zw *zip.Writer, req Request) error {
	// CLI args (redacted) + an effective-config snapshot the bundle
	// recipient can re-run the sluice invocation against (with
	// secret material stripped).
	if len(req.CLIArgs) > 0 {
		if err := writeJSON(zw, "config/cli_args.json", RedactCLIArgs(req.CLIArgs)); err != nil {
			return err
		}
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
	if err := writeJSON(zw, "engine/capabilities.json", caps); err != nil {
		return err
	}

	// Engine snapshots — ir.DiagnoseProber on each side.
	if req.SourceEngine != nil && req.SourceDSN != "" {
		probeAndWriteDiagnose(ctx, zw, "engine/source_diagnose", req.SourceEngine, req.SourceDSN, req.StreamID)
	}
	if req.TargetEngine != nil && req.TargetDSN != "" {
		probeAndWriteDiagnose(ctx, zw, "engine/target_diagnose", req.TargetEngine, req.TargetDSN, req.StreamID)
	}

	// Health probe mirrors `sluice sync health` JSON. Only fires
	// when source + target are both set (the cross-engine lag-bytes
	// needs both sides; the standalone target-side freshness lives
	// in the cdc_state dump already).
	if req.SourceEngine != nil && req.SourceDSN != "" && req.TargetEngine != nil && req.TargetDSN != "" {
		probeAndWriteHealth(ctx, zw, "health/sync_health.json", req)
	}

	// Target health — the ADR-0107 control-plane telemetry snapshot
	// (CPU/mem/storage/lag/conns). Always emitted at PrivacyStandard+ so
	// the recipient can tell "telemetry not configured" from "configured but
	// no fresh sample" from a real reading.
	probeAndWriteTargetHealth(ctx, zw, "health/target_health.json", req)

	return nil
}

// probeAndWriteTargetHealth writes the ADR-0107 target-telemetry snapshot. It
// is best-effort and honest: no provider ⇒ a reason file; a provider with no
// fresh sample ⇒ {"fresh": false}; a fresh sample ⇒ the distilled
// CPU/mem/storage/lag/connection view with each value gated by its *Known
// flag (an unobserved metric is omitted, never reported as 0/idle).
func probeAndWriteTargetHealth(ctx context.Context, zw *zip.Writer, name string, req Request) {
	if req.TargetTelemetry == nil {
		_ = writeReason(zw, name, "target telemetry not configured (no --planet-scale-org / metrics token)")
		return
	}
	snap, ok := req.TargetTelemetry.Sample(ctx)
	if !ok {
		_ = writeJSON(zw, name, map[string]any{"fresh": false, "reason": "no fresh telemetry sample available"})
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
	_ = writeJSON(zw, name, out)
}

// collectVerboseSections writes the PrivacyVerbose additions: per-
// table row counts (slow path, opt-in-only), log-file tail.
func collectVerboseSections(ctx context.Context, zw *zip.Writer, req Request) error {
	// Per-table row counts on the TARGET. Slow path — the operator
	// opted in by selecting PrivacyVerbose. Skipped if the target
	// can't be probed.
	if req.TargetEngine != nil && req.TargetDSN != "" {
		probeAndWriteRowCounts(ctx, zw, req)
	}

	// Log-file tail. The operator must have configured a --log-file
	// path; sluice does NOT scrape the parent process's stderr.
	if req.LogFile != "" {
		if err := writeLogTail(zw, req.LogFile); err != nil {
			if rerr := writeReason(zw, "logs/log_tail", fmt.Sprintf("read log file %q: %v", req.LogFile, err)); rerr != nil {
				return rerr
			}
		}
	}

	return nil
}

// probeAndWriteDiagnose runs ir.DiagnoseProber against the given
// engine + DSN. Errors are surfaced as a reason file rather than
// propagated.
func probeAndWriteDiagnose(ctx context.Context, zw *zip.Writer, prefix string, engine ir.Engine, dsn, streamID string) {
	sr, err := engine.OpenSchemaReader(ctx, dsn)
	if err != nil {
		_ = writeReason(zw, prefix, fmt.Sprintf("open schema reader: %v", err))
		return
	}
	defer closeIfCloser(sr)

	prober, ok := sr.(ir.DiagnoseProber)
	if !ok {
		_ = writeReason(zw, prefix, fmt.Sprintf("engine %q does not implement ir.DiagnoseProber", engine.Name()))
		return
	}
	snap, err := prober.DiagnoseBundle(ctx, streamID)
	if err != nil {
		_ = writeReason(zw, prefix, fmt.Sprintf("diagnose probe: %v", err))
		return
	}
	_ = writeJSON(zw, prefix+".json", snap)
}

// probeAndWriteHealth mirrors `sluice sync health`'s probe surface
// against the same two-engine wire format. Failures are bundled as
// reason strings.
func probeAndWriteHealth(ctx context.Context, zw *zip.Writer, name string, req Request) {
	// Target side — list streams, find the one matching req.StreamID.
	applier, err := req.TargetEngine.OpenChangeApplier(ctx, req.TargetDSN)
	if err != nil {
		_ = writeReason(zw, name, fmt.Sprintf("open target applier: %v", err))
		return
	}
	defer closeIfCloser(applier)

	streams, err := applier.ListStreams(ctx)
	if err != nil {
		_ = writeReason(zw, name, fmt.Sprintf("list streams: %v", err))
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
		_ = writeJSON(zw, name, out)
		return
	}
	defer closeIfCloser(sr)

	hr, ok := sr.(ir.HealthReporter)
	if !ok {
		out["source_probe_reason"] = fmt.Sprintf("engine %q does not implement ir.HealthReporter", req.SourceEngine.Name())
		_ = writeJSON(zw, name, out)
		return
	}
	pos, err := hr.SourceCurrentPosition(ctx)
	if err != nil {
		out["source_probe_reason"] = fmt.Sprintf("source-current-position: %v", err)
		_ = writeJSON(zw, name, out)
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

	_ = writeJSON(zw, name, out)
}

// probeAndWriteRowCounts writes per-table COUNT(*) on the target. The
// schema is read first to enumerate the filtered table list; counts
// are best-effort per table.
func probeAndWriteRowCounts(ctx context.Context, zw *zip.Writer, req Request) {
	reader, err := req.TargetEngine.OpenSchemaReader(ctx, req.TargetDSN)
	if err != nil {
		_ = writeReason(zw, "verbose/row_counts", fmt.Sprintf("open target schema reader: %v", err))
		return
	}
	defer closeIfCloser(reader)

	schema, err := reader.ReadSchema(ctx)
	if err != nil {
		_ = writeReason(zw, "verbose/row_counts", fmt.Sprintf("read target schema: %v", err))
		return
	}

	rowReader, err := req.TargetEngine.OpenRowReader(ctx, req.TargetDSN)
	if err != nil {
		_ = writeReason(zw, "verbose/row_counts", fmt.Sprintf("open target row reader: %v", err))
		return
	}
	defer closeIfCloser(rowReader)

	counter, ok := rowReader.(ir.RowCounter)
	if !ok {
		_ = writeReason(zw, "verbose/row_counts",
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
	_ = writeJSON(zw, "verbose/row_counts.json", out)
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
func writeJSON(zw *zip.Writer, name string, payload any) error {
	w, err := zw.CreateHeader(&zip.FileHeader{Name: name, Method: zip.Deflate})
	if err != nil {
		return err
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetIndent("", "  ")
	if err := enc.Encode(payload); err != nil {
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
