// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline"
)

// SyncHealthCmd implements `sluice sync health` (proto-ADR
// docs/dev/design/sync-health-monitoring.md).
//
// v0.13.0 (probe MVP): reads the target's sluice_cdc_state for the
// supplied --stream-id, computes wall-clock seconds-since-last-apply.
//
// v0.15.0 (Phase 2 source-side position comparison): when optional
// --source-driver + --source flags are supplied, also probes the
// source's current position and surfaces the source/target tokens
// + a byte-distance lag metric (PG only — MySQL GTID sets aren't
// byte-distance comparable).
//
// Exit codes:
//   - 0 healthy.
//   - 1 stale (a threshold was breached).
//   - 2 operational error (couldn't connect, stream not found, etc.).
type SyncHealthCmd struct {
	TargetDriver string `help:"Target engine name (e.g. mysql, postgres). See 'sluice engines'." required:"" placeholder:"NAME" group:"target"`
	Target       string `help:"Target database DSN." required:"" env:"SLUICE_TARGET" placeholder:"DSN" group:"target"`
	StreamID     string `help:"Stream identifier to probe. Required — point at the stream you want to monitor." required:"" placeholder:"ID"`

	SourceDriver string `help:"Source engine name (optional). When set together with --source, the probe also reads the source's current position and reports source/target tokens plus byte-distance lag (PG only). Without these, the probe stays target-side only — same as v0.13.0 / v0.14.x behavior." placeholder:"NAME" group:"source"`
	Source       string `help:"Source database DSN (optional). See --source-driver." env:"SLUICE_SOURCE" placeholder:"DSN" group:"source"`
	SlotName     string `help:"Replication-slot name on the source (PG-only, requires --source). Used to read per-slot CDC-decode spill counters from pg_stat_replication_slots (PG 14+). Defaults to 'sluice_slot' — sluice's built-in slot name — when --source-driver=postgres is set and this flag is unset." placeholder:"NAME" group:"source"`

	MaxStaleSeconds int   `help:"Threshold: exit 1 if target's last apply was more than N seconds ago. 0 disables the check (informational only)." default:"0" placeholder:"N"`
	MaxLagBytes     int64 `help:"Threshold (PG-only, requires --source): exit 1 if source LSN is more than N bytes ahead of target. 0 disables. MySQL leaves this informational; GTID sets aren't byte-distance comparable." default:"0" placeholder:"N"`

	Format string `help:"Output format: 'text' (default) or 'json' (machine-readable for alertmanager / scripting pipes)." default:"text" enum:"text,json" placeholder:"FORMAT"`
	Output string `help:"Write to FILE instead of stdout. Atomic." short:"o" placeholder:"FILE"`

	ControlKeyspace string `name:"control-keyspace" help:"MySQL/PlanetScale/Vitess target only: the unsharded sidecar keyspace the stream's control tables live in (see 'sync start --control-keyspace'). Omit to auto-detect on a sharded target. Empty + unsharded/non-Vitess target = the default keyspace." placeholder:"KEYSPACE"`
}

// HealthResult is the structured output of `sluice sync health`. Same
// shape across text + JSON renderers; the JSON encoder consumes this
// directly.
type HealthResult struct {
	StreamID              string `json:"stream_id"`
	Found                 bool   `json:"found"`
	Position              string `json:"position,omitempty"`
	UpdatedAt             string `json:"updated_at,omitempty"`
	SecondsSinceLastApply int64  `json:"seconds_since_last_apply,omitempty"`
	Threshold             int    `json:"max_stale_seconds_threshold,omitempty"`
	Stale                 bool   `json:"stale"`

	// Source-side fields (populated only when --source-driver +
	// --source were supplied). The orchestrator opens a SchemaReader
	// against the source, type-asserts to ir.HealthReporter, calls
	// SourceCurrentPosition(); engines without HealthReporter cause
	// a "source-probe-not-supported" reason to land in SourceProbeReason.
	SourcePosition       string `json:"source_position,omitempty"`
	SourceProbeAvailable bool   `json:"source_probe_available"`
	SourceProbeReason    string `json:"source_probe_reason,omitempty"`

	// LagBytes is populated only when both source and target sides
	// implement ir.BytesLagReporter (PG only as of v0.15.0). MySQL
	// leaves this -1 (sentinel for "not available on this engine").
	LagBytes        int64 `json:"lag_bytes,omitempty"`
	LagBytesIsAvail bool  `json:"lag_bytes_available"`
	LagThreshold    int64 `json:"max_lag_bytes_threshold,omitempty"`
	LagBytesStale   bool  `json:"lag_bytes_stale,omitempty"`

	// Spill stats (severity-B finding F2 of the 2026-05-22 PG-internals
	// research run). Pointer-omitempty: nil when the engine doesn't
	// implement [ir.SlotSpillReporter] (MySQL), when the PG view doesn't
	// exist (PG < 14), or when no decode has happened on the slot yet.
	// "Unavailable" surfaces as field absence rather than a misleading
	// 0 a careless reader could mistake for "definitely no spill."
	SpillTxns  *int64 `json:"spill_txns,omitempty"`
	SpillBytes *int64 `json:"spill_bytes,omitempty"`

	// Skipped-table ledger (audit C-11): the stream's durable
	// unknown-target-table skip records. A nonzero total TRIPS the
	// health check (exit 1) unconditionally — unlike the staleness
	// gauges there is no threshold flag, because a skipped table is
	// operator-induced drift that only add-table or an explicit filter
	// resolves.
	SkippedTables        []SkippedTableHealth `json:"skipped_tables,omitempty"`
	SkippedEventsTotal   int64                `json:"skipped_events_total,omitempty"`
	SkippedTablesTripped bool                 `json:"skipped_tables_tripped,omitempty"`
}

// SkippedTableHealth is one C-11 skip-ledger row in the health output.
type SkippedTableHealth struct {
	Table         string `json:"table"`
	SkipCount     int64  `json:"skip_count"`
	FirstPosition string `json:"first_position"`
	LastPosition  string `json:"last_position"`
	LastSkippedAt string `json:"last_skipped_at"`
}

// Run implements `sluice sync health`. Same boilerplate shape as
// SyncStatusCmd — open the target's ChangeApplier, list streams,
// filter to the requested stream, then evaluate thresholds.
func (s *SyncHealthCmd) Run(_ *Globals) error {
	target, err := resolveEngine(s.TargetDriver)
	if err != nil {
		return operationalError{err: fmt.Errorf("--target-driver: %w", err)}
	}

	writer, finalize, err := openVerifyOutput(s.Output)
	if err != nil {
		return operationalError{err: err}
	}
	var runErr error
	defer func() { _ = finalize(runErr) }()

	ctx := kongContext()
	if target, err = applyControlKeyspace(ctx, target, s.ControlKeyspace, s.Target); err != nil {
		return operationalError{err: err}
	}
	applier, err := target.OpenChangeApplier(ctx, s.Target)
	if err != nil {
		return operationalError{err: fmt.Errorf("open target applier: %w", err)}
	}
	defer func() {
		if c, ok := applier.(io.Closer); ok {
			_ = c.Close()
		}
	}()

	streams, err := applier.ListStreams(ctx)
	if err != nil {
		runErr = err
		return operationalError{err: fmt.Errorf("list streams: %w", err)}
	}

	result := evaluateHealth(streams, s.StreamID, s.MaxStaleSeconds, time.Now())

	// Audit C-11: fold the stream's durable unknown-target-table skip
	// ledger into the health verdict — a nonzero count trips (exit 1).
	// A ledger read failure is an operational error, same class as the
	// stream-list read above: health must never report "no skips" it
	// could not actually verify.
	if lister, ok := applier.(ir.SkippedTableLister); ok {
		records, err := lister.ListSkippedTables(ctx)
		if err != nil {
			runErr = err
			return operationalError{err: fmt.Errorf("list skipped tables: %w", err)}
		}
		applySkippedTablesHealth(&result, records, s.StreamID)
	}

	// Optional source-side probe (v0.15.0, fixed in v0.15.1 / Bug 32).
	// Only fires when the operator supplied both --source-driver AND
	// --source. Errors here populate SourceProbeReason but don't fail
	// the run — operators running cron probes shouldn't have target-
	// side monitoring break because the source-side connection is
	// transiently down.
	if s.SourceDriver != "" && s.Source != "" {
		// Recover the FULL target Position from the streams slice
		// (un-truncated) so engine-side LagBytes can parse the JSON
		// envelope correctly. The displayed result.Position is
		// truncated for readability and not safe to pass into the
		// engine's lag-bytes computation.
		var fullTargetPos ir.Position
		for _, st := range streams {
			if st.StreamID == s.StreamID {
				fullTargetPos = st.Position
				break
			}
		}
		probeSource(ctx, &result, s, target, fullTargetPos)
	}
	result.LagThreshold = s.MaxLagBytes
	if s.MaxLagBytes > 0 && result.LagBytesIsAvail && result.LagBytes > s.MaxLagBytes {
		result.LagBytesStale = true
	}

	if err := renderHealth(writer, result, s.Format); err != nil {
		runErr = err
		return operationalError{err: err}
	}

	if !result.Found {
		// "Not found" is also what a healthy COLD START looks like: the
		// stream's row is written only after the copy, the index build
		// and the FLOAT exact re-read all finish. Before 2026-09-08 this
		// error was the operator's whole picture during that window,
		// which reads as a dead run and is why one reached for strace.
		//
		// The exit status is deliberately unchanged — a cold start is
		// not a healthy STREAM, and a cron probe that started treating
		// it as one would go quiet exactly when a genuinely stuck cold
		// start needed attention. What changes is that the error says
		// which of the two situations this is, and how to tell whether
		// it is progressing.
		if cs, ok := coldStartInFlight(ctx, target, s.Target, s.StreamID); ok {
			return operationalError{err: fmt.Errorf(
				"stream %q is COLD STARTING (phase %s, last progress write %s ago) and has not written its "+
					"CDC anchor yet — expected during a cold start, not a stall. Its stream row appears only "+
					"after the copy, the index build and the FLOAT exact re-read finish; `sluice sync status` "+
					"shows the progress, and the PHASE advancing is what tells you it is working. That age "+
					"moves when the phase does, so a long copy of one large table holds it still on a "+
					"perfectly healthy run — it means the run is gone only if the phase has stopped too",
				s.StreamID, cs.phase, cs.age.Round(time.Second),
			)}
		}
		return operationalError{err: fmt.Errorf("stream %q not found on target", s.StreamID)}
	}
	switch {
	case result.Stale:
		return staleStreamError{streamID: s.StreamID, secondsAgo: result.SecondsSinceLastApply, threshold: s.MaxStaleSeconds}
	case result.LagBytesStale:
		return staleStreamError{streamID: s.StreamID, lagBytes: result.LagBytes, lagThreshold: s.MaxLagBytes}
	case result.SkippedTablesTripped:
		return skippedTablesError{streamID: s.StreamID, result: result}
	}
	return nil
}

// applySkippedTablesHealth folds the C-11 skip ledger into the health
// result: the stream's rows, the total, and the trip flag (nonzero
// total ⇒ tripped). Pure function — testable (and mutation-runnable)
// without a live target.
func applySkippedTablesHealth(r *HealthResult, records []ir.SkippedTableRecord, streamID string) {
	for _, rec := range records {
		if rec.StreamID != streamID || rec.SkipCount == 0 {
			continue
		}
		r.SkippedTables = append(r.SkippedTables, SkippedTableHealth{
			Table:         rec.Table,
			SkipCount:     rec.SkipCount,
			FirstPosition: rec.FirstPosition,
			LastPosition:  rec.LastPosition,
			LastSkippedAt: rec.LastSkippedAt.UTC().Format(time.RFC3339),
		})
		r.SkippedEventsTotal += rec.SkipCount
	}
	if r.SkippedEventsTotal > 0 {
		r.SkippedTablesTripped = true
	}
}

// skippedTablesError signals "the stream skipped CDC events for tables
// the target lacks" with exit code 1 — the same alerting class as
// staleStreamError, naming every table and the remedy.
type skippedTablesError struct {
	streamID string
	result   HealthResult
}

func (skippedTablesError) ExitCode() int { return 1 }

func (e skippedTablesError) Error() string {
	tables := make([]string, 0, len(e.result.SkippedTables))
	for _, t := range e.result.SkippedTables {
		tables = append(tables, fmt.Sprintf("%s (%d)", t.Table, t.SkipCount))
	}
	return fmt.Sprintf("stream %q skipped %d CDC event(s) for table(s) the target lacks: %s — %s",
		e.streamID, e.result.SkippedEventsTotal, strings.Join(tables, ", "), ir.SkippedTableRemedy)
}

// probeSource opens a SchemaReader on the source DSN, type-asserts
// to ir.HealthReporter (and ir.BytesLagReporter for PG), populates
// the source-side fields on result. Errors are caught and surfaced
// via SourceProbeReason rather than propagated; an unreachable
// source shouldn't break a target-side health probe.
//
// targetPos is the FULL un-truncated target Position from the
// stream's ListStreams entry. v0.15.1 / Bug 32: passing
// `result.Position` (the truncated-for-display string) into the
// engine's LagBytes broke on PG because PG positions are JSON
// envelopes, not bare LSNs. The engine now extracts LSN from
// either shape — but only if it gets the full Token, not a
// reconstructed-from-display string.
func probeSource(ctx context.Context, result *HealthResult, cfg *SyncHealthCmd, target ir.Engine, targetPos ir.Position) {
	source, err := resolveEngine(cfg.SourceDriver)
	if err != nil {
		result.SourceProbeReason = fmt.Sprintf("--source-driver: %v", err)
		return
	}
	sr, err := source.OpenSchemaReader(ctx, cfg.Source)
	if err != nil {
		result.SourceProbeReason = fmt.Sprintf("open source schema reader: %v", err)
		return
	}
	defer func() {
		if c, ok := sr.(io.Closer); ok {
			_ = c.Close()
		}
	}()
	hr, ok := sr.(ir.HealthReporter)
	if !ok {
		result.SourceProbeReason = fmt.Sprintf("source engine %q does not implement ir.HealthReporter", source.Name())
		return
	}
	pos, err := hr.SourceCurrentPosition(ctx)
	if err != nil {
		result.SourceProbeReason = fmt.Sprintf("source-current-position: %v", err)
		return
	}
	result.SourcePosition = truncatePositionToken(pos.Token, 60)
	result.SourceProbeAvailable = true

	// Byte-distance lag — only when source AND target both implement
	// BytesLagReporter (today: PG only).
	srcLag, srcOK := sr.(ir.BytesLagReporter)
	if !srcOK || !cfg.canComputeLagBytes(target) || targetPos.Token == "" {
		result.LagBytes = -1
		return
	}
	lag, err := srcLag.LagBytes(ctx, targetPos, pos)
	if err != nil {
		result.SourceProbeReason = fmt.Sprintf("lag-bytes: %v", err)
		result.LagBytes = -1
		return
	}
	result.LagBytes = lag
	result.LagBytesIsAvail = true

	// Spill stats (severity-B finding F2 of the 2026-05-22 PG-internals
	// research run). Engines that don't implement SlotSpillReporter
	// (MySQL) leave the pointers nil. PG < 14 (the view doesn't exist)
	// or a freshly-created slot with no decode yet also surface as nil
	// via ok=false — see the interface doc on `ir.SlotSpillReporter`
	// for the "no signal" cases.
	spiller, spillOK := sr.(ir.SlotSpillReporter)
	if !spillOK {
		return
	}
	slot := cfg.effectiveSlotName(source)
	if slot == "" {
		// Source engine that doesn't have a slot concept (today: only
		// PG has SlotSpillReporter, so this branch is defensive for
		// future engines).
		return
	}
	stats, statsOK, err := spiller.SlotSpillStats(ctx, slot)
	if err != nil {
		result.SourceProbeReason = fmt.Sprintf("slot-spill-stats: %v", err)
		return
	}
	if !statsOK {
		// "No signal" is not "no spill", and until now the two were
		// indistinguishable here: this branch returned leaving both
		// pointers nil and no reason, which reads exactly like a healthy
		// slot that has not spilled. The docs promise the fields are
		// omitted "so a careless reader can't mistake 'we can't tell' for
		// 'definitely no spill'" — omitting them is only half of keeping
		// that promise, and the diagnose bundle got the other half in the
		// same pass this branch did not (audit 2026-09-09 A0909-AQ-M-1's
		// sibling, found by the pre-tag docs-drift sweep).
		//
		// The reason names the slot, because the likeliest cause of an
		// absent row is a slot name that does not exist on this source —
		// which is the very defect A0909-AQ-M-1 was, and which an
		// operator can only diagnose if the name sluice looked for is
		// printed. PG < 14 has no pg_stat_replication_slots at all, and a
		// slot that exists but has never decoded also lands here, so the
		// wording covers all three rather than asserting one.
		result.SourceProbeReason = fmt.Sprintf(
			"slot-spill-stats: no row for replication slot %q — the slot does not exist on this source, "+
				"it has not decoded anything yet, or the server predates pg_stat_replication_slots (PG 14). "+
				"Spill counters are omitted rather than reported as zero", slot,
		)
		return
	}
	txns := stats.SpillTxns
	bytes := stats.SpillBytes
	result.SpillTxns = &txns
	result.SpillBytes = &bytes
}

// effectiveSlotName returns the slot this probe must look up: the
// operator's --slot-name under the sluice-prefix convention, or the
// engine's default when unset (empty on MySQL, which has no slot
// concept — the call site's check skips it).
//
// It delegates rather than deciding. The previous version returned
// s.SlotName VERBATIM, which is the audit 2026-09-09 A0909-AQ-M-1 bug:
// `sync start --slot-name shard_a` creates `sluice_shard_a`, so
// `sync health --slot-name shard_a` was querying a slot that does not
// exist, and the miss is SILENT — SlotSpillStats returns ok=false and
// the caller's "no signal" branch returns without a probe reason, so
// the counters were absent in a way indistinguishable from a slot that
// simply had not spilled. Its own doc-comment argued for duplicating
// the default constant here, and that duplication is where the prefix
// step went missing.
func (s *SyncHealthCmd) effectiveSlotName(source ir.Engine) string {
	return pipeline.SlotNameForSource(s.SlotName, source.Name())
}

// canComputeLagBytes reports whether the target engine ALSO supports
// BytesLagReporter (the source side is checked at the call site).
// Today's check is engine-name-based since both sides need the same
// position-shape; future engine pairs may extend this.
func (s *SyncHealthCmd) canComputeLagBytes(target ir.Engine) bool {
	return target.Name() == "postgres" && s.SourceDriver == "postgres"
}

// evaluateHealth filters the stream list to the requested ID and
// computes the freshness comparison against the threshold. Pure
// function — testable without a live target.
func evaluateHealth(streams []ir.StreamStatus, streamID string, maxStaleSeconds int, now time.Time) HealthResult {
	r := HealthResult{StreamID: streamID, Threshold: maxStaleSeconds}
	for _, st := range streams {
		if st.StreamID != streamID {
			continue
		}
		r.Found = true
		r.Position = truncatePositionToken(st.Position.Token, 60)
		r.UpdatedAt = st.UpdatedAt.UTC().Format(time.RFC3339)
		r.SecondsSinceLastApply = int64(now.Sub(st.UpdatedAt).Seconds())
		if maxStaleSeconds > 0 && r.SecondsSinceLastApply > int64(maxStaleSeconds) {
			r.Stale = true
		}
		return r
	}
	return r
}

func renderHealth(w io.Writer, r HealthResult, format string) error {
	switch strings.ToLower(strings.TrimSpace(format)) {
	case "json":
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(r)
	case "", "text":
		return renderHealthText(w, r)
	}
	return fmt.Errorf("unknown format %q (want 'text' or 'json')", format)
}

func renderHealthText(w io.Writer, r HealthResult) error {
	if !r.Found {
		_, err := fmt.Fprintf(w, "stream: %s\nfound:  false\n", r.StreamID)
		return err
	}
	state := "healthy"
	switch {
	case r.Stale:
		state = fmt.Sprintf("STALE (last apply %ds ago, threshold %ds)", r.SecondsSinceLastApply, r.Threshold)
	case r.LagBytesStale:
		state = fmt.Sprintf("STALE (lag %d bytes, threshold %d)", r.LagBytes, r.LagThreshold)
	case r.SkippedTablesTripped:
		state = fmt.Sprintf("SKIPPING (%d event(s) skipped for %d table(s) the target lacks)", r.SkippedEventsTotal, len(r.SkippedTables))
	}
	if _, err := fmt.Fprintf(w,
		"stream: %s\nfound: true\nstate: %s\nposition: %s\nupdated_at: %s\nseconds_since_last_apply: %d\n",
		r.StreamID, state, r.Position, r.UpdatedAt, r.SecondsSinceLastApply); err != nil {
		return err
	}
	if r.SourceProbeAvailable {
		if _, err := fmt.Fprintf(w, "source_position: %s\n", r.SourcePosition); err != nil {
			return err
		}
		if r.LagBytesIsAvail {
			if _, err := fmt.Fprintf(w, "lag_bytes: %d\n", r.LagBytes); err != nil {
				return err
			}
		} else {
			if _, err := fmt.Fprintln(w, "lag_bytes: unavailable (cross-engine or non-PG pair)"); err != nil {
				return err
			}
		}
		// F2: spill counters. Render only when present; "unavailable"
		// stays implicit (no line) so the absence of the line matches
		// the JSON omitempty shape.
		if r.SpillTxns != nil && r.SpillBytes != nil {
			if _, err := fmt.Fprintf(w, "spill_txns: %d\nspill_bytes: %d\n", *r.SpillTxns, *r.SpillBytes); err != nil {
				return err
			}
		}
	} else if r.SourceProbeReason != "" {
		if _, err := fmt.Fprintf(w, "source_probe: skipped (%s)\n", r.SourceProbeReason); err != nil {
			return err
		}
	}
	// Audit C-11: the skip-ledger block. Absent lines when the ledger
	// is empty, matching the JSON omitempty shape.
	for _, t := range r.SkippedTables {
		if _, err := fmt.Fprintf(w, "skipped_table: %s count=%d last_skipped_at=%s\n", t.Table, t.SkipCount, t.LastSkippedAt); err != nil {
			return err
		}
	}
	if r.SkippedTablesTripped {
		if _, err := fmt.Fprintf(w, "skipped_tables_remedy: %s\n", ir.SkippedTableRemedy); err != nil {
			return err
		}
	}
	return nil
}

// staleStreamError signals "threshold breached" with kong exit code 1.
// Mirrors driftError's shape for the diff command — short message on
// stderr; the structured detail is on stdout (or --output FILE).
//
// Carries either time-based staleness (secondsAgo + threshold) OR
// bytes-based staleness (lagBytes + lagThreshold) depending on which
// gauge tripped.
type staleStreamError struct {
	streamID     string
	secondsAgo   int64
	threshold    int
	lagBytes     int64
	lagThreshold int64
}

func (staleStreamError) ExitCode() int { return 1 }

func (e staleStreamError) Error() string {
	if e.lagThreshold > 0 {
		return fmt.Sprintf("stream %q stale: lag %d bytes, threshold %d",
			e.streamID, e.lagBytes, e.lagThreshold)
	}
	return fmt.Sprintf("stream %q stale: last apply %ds ago, threshold %ds",
		e.streamID, e.secondsAgo, e.threshold)
}

// coldStartProgress is the little that `sync health` needs to tell a
// cold start apart from a dead run: which phase, and how long since the
// last progress write.
type coldStartProgress struct {
	phase ir.MigrationPhase
	age   time.Duration
}

// coldStartInFlight reports whether streamID currently has a cold start
// writing progress rows on the target.
//
// Best-effort and deliberately silent on failure: this runs on the
// not-found path of a health probe, where the caller already has a
// correct error to return. An engine with no [ir.MigrationStateLister],
// an unopenable store, or a read error all mean "cannot tell" — and
// "cannot tell" must degrade to the plain not-found message rather than
// to a claim in either direction.
func coldStartInFlight(ctx context.Context, target ir.Engine, dsn, streamID string) (coldStartProgress, bool) {
	lister, err := openMigrationStateStoreForStatus(ctx, target, dsn)
	if err != nil || lister == nil {
		return coldStartProgress{}, false
	}
	if c, ok := lister.(io.Closer); ok {
		defer func() { _ = c.Close() }()
	}
	states, err := lister.List(ctx, syncMigrationIDPrefix+streamID)
	if err != nil || len(states) == 0 {
		return coldStartProgress{}, false
	}
	// List matches by PREFIX, so ask for the exact id rather than
	// trusting the first row: stream "prod" would otherwise be reported
	// as cold-starting because stream "prod-2" is.
	want := syncMigrationIDPrefix + streamID
	for _, st := range states {
		if st.MigrationID != want {
			continue
		}
		if st.Phase == ir.MigrationPhaseComplete {
			// The copy finished; whatever is happening now is the
			// post-copy work, and the plain not-found message is the
			// honest answer.
			return coldStartProgress{}, false
		}
		return coldStartProgress{phase: st.Phase, age: time.Since(st.UpdatedAt)}, true
	}
	return coldStartProgress{}, false
}
