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
		return operationalError{err: fmt.Errorf("stream %q not found on target", s.StreamID)}
	}
	switch {
	case result.Stale:
		return staleStreamError{streamID: s.StreamID, secondsAgo: result.SecondsSinceLastApply, threshold: s.MaxStaleSeconds}
	case result.LagBytesStale:
		return staleStreamError{streamID: s.StreamID, lagBytes: result.LagBytes, lagThreshold: s.MaxLagBytes}
	}
	return nil
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
		return
	}
	txns := stats.SpillTxns
	bytes := stats.SpillBytes
	result.SpillTxns = &txns
	result.SpillBytes = &bytes
}

// effectiveSlotName returns the operator-supplied --slot-name if non-
// empty, or the engine's default ("sluice_slot" on PG, empty on MySQL —
// MySQL has no slot concept, the call site's nil-check skips it). Kept
// out of the engine package because the default-slot identifier is also
// hard-coded in `internal/engines/postgres/cdc_reader.go`; the duplicate
// constant here is small and the alternative (exporting `defaultSlot`
// from the engine package) would couple cmd/ to the engine.
func (s *SyncHealthCmd) effectiveSlotName(source ir.Engine) string {
	if s.SlotName != "" {
		return s.SlotName
	}
	if source.Name() == "postgres" {
		return "sluice_slot"
	}
	return ""
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
