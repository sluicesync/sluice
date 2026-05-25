// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import (
	"context"
	"errors"
	"time"
)

// HealthReporter is the optional engine-side surface for liveness /
// lag reporting. Engines that already track CDC positions (MySQL
// binlog/VStream, PG pgoutput) implement it; consumers (the
// `sluice sync health` probe and the `--metrics-listen` Prometheus
// endpoint) type-assert from a SchemaReader and surface a clear
// "not supported on this engine" error when the assertion fails.
//
// v0.15.0 ships the minimum viable interface — `SourceCurrentPosition`
// only. Phase 3 of the sync-health monitoring proto-ADR
// (`docs/dev/design-sync-health-monitoring.md`) covers later additions
// like wall-clock-time-of-position and per-event lag arithmetic; the
// interface is small now and grows as operator demand surfaces.
//
// **Why not on SchemaReader directly:** SchemaReader's contract is
// "extract IR Schema from a live database." Health reporting is a
// different concern — it queries a SQL-server-state surface, not a
// catalog surface. Optional-interface assertion keeps both contracts
// clean and lets engines opt in without forcing every implementation
// to grow a new method.
type HealthReporter interface {
	// SourceCurrentPosition returns the source database's current
	// head position — the most recent committed event's position
	// (PG: pg_current_wal_lsn(); MySQL: gtid_executed snapshot).
	//
	// Used by `sluice sync health` to compute "how far behind is
	// the target's tracked position?" relative to where the source
	// stands right now. The engines also use [Position] to encode
	// their CDC stream cursors, so the returned value is comparable
	// to the StreamStatus.Position values already surfaced via
	// ListStreams.
	SourceCurrentPosition(ctx context.Context) (Position, error)
}

// BytesLagReporter is an extra optional surface for engines whose
// position is a linearly-orderable byte offset (Postgres LSN). Not
// implemented by MySQL since GTID sets are set-membership-comparable
// rather than byte-distance-comparable; cross-GTID-set arithmetic
// would require parsing GTID sets and computing transaction-count
// differences, which isn't operator-meaningful as a single integer.
//
// PG implements via `pg_wal_lsn_diff(lhs, rhs)` (returns numeric
// bytes between two LSNs). MySQL doesn't implement; the consumer
// surfaces "lag-bytes unavailable on this engine" when the
// assertion fails.
type BytesLagReporter interface {
	// LagBytes returns the byte-distance from earlier to later
	// (later − earlier). Returns negative or zero if earlier ≥ later.
	// Engines may return an error if the positions are malformed
	// or can't be compared.
	LagBytes(ctx context.Context, earlier, later Position) (int64, error)
}

// SpillStats carries the cumulative-since-slot-creation logical-decoding
// spill counters exposed by PG 14+'s `pg_stat_replication_slots` view
// (severity-B finding F2 of the 2026-05-22 PG-internals research run).
// Both fields are monotone counters — they accumulate over the slot's
// lifetime and reset to zero only when the slot is dropped + recreated.
//
// **Why operators care:** non-zero / growing spill means a CDC
// transaction exceeded `logical_decoding_work_mem` (default 64 MB) and
// PG spooled the un-emitted ReorderBufferChanges to disk under
// `pg_replslot/<slot>/snap/`. Sustained spill puts disk pressure on the
// source's replication-slot directory; if it fills, PG can invalidate
// the slot (wal_status → 'lost'), which is silent-loss-class. Surfacing
// spill in sluice's health surface gives operators a chance to bump
// `logical_decoding_work_mem` or split large transactions before the
// slot is evicted.
//
// **Engine-neutrality.** The struct lives in IR because the optional
// reporter interface needs a return type the orchestrator can render.
// PG is the only engine that produces these values today; MySQL has no
// analogue (binlog decoding doesn't spool to disk the same way).
type SpillStats struct {
	// SpillTxns is the cumulative count of transactions that spilled
	// out of memory during logical decoding for this slot.
	SpillTxns int64

	// SpillBytes is the cumulative bytes of decoded transaction data
	// that spilled to disk for this slot.
	SpillBytes int64
}

// SlotSpillReporter is an optional engine-side surface exposing the
// PG-14+ `pg_stat_replication_slots.spill_*` counters (severity-B
// finding F2 of the 2026-05-22 PG-internals research run). Today PG is
// the only implementer; MySQL has no analogue.
//
// **The boolean return.** The view exists on PG 14+; the per-slot row
// exists only after the slot has been used for decoding at least once.
// Either absence (view missing on PG < 14, or no row in the view) is
// surfaced via `ok=false`. The consumer then omits the corresponding
// JSON / Prometheus fields rather than falsely reporting "0 bytes
// spilled" (which a careless reader could mistake for "definitely no
// spill," when the real signal is "we can't tell").
//
// **Why a separate interface from HealthReporter.** HealthReporter is
// position-shaped (one method, takes no slot name). Spill stats are
// per-slot, so the call signature is different. Splitting keeps the
// existing interface focused and lets engines opt in to either
// independently.
type SlotSpillReporter interface {
	// SlotSpillStats returns the spill counters for the named slot.
	// ok=false when stats are unavailable (PG < 14, or the slot has no
	// row in `pg_stat_replication_slots` yet because no decode has
	// happened); the consumer treats this as "no signal" and omits
	// the fields from its output rather than emitting a misleading 0.
	SlotSpillStats(ctx context.Context, slotName string) (stats SpillStats, ok bool, err error)
}

// SlotHealth carries the per-probe snapshot of a Postgres replication
// slot's retention pressure and liveness — severity-A finding F13 of
// the 2026-05-22 Reddit-research run. The struct lives in IR because
// the optional reporter interface needs a return type the orchestrator
// can render and threshold-test without importing engine packages.
// Postgres is the only engine that produces these values today; MySQL
// binlog retention has a different shape (filename + position +
// expire_logs_days policy) and is tracked as a deferred follow-up.
//
// **Why operators care:** when a slot's restart_lsn falls behind the
// source's current WAL position, Postgres retains every WAL segment
// between the two. The retention size is bounded by the
// `max_slot_wal_keep_size` GUC; once a slot's lag exceeds that bound,
// Postgres invalidates the slot (wal_status → 'lost') and the consumer
// loses its CDC checkpoint without warning. F13's promise is to log a
// WARN before that happens so the operator can intervene (check the
// consumer, bump the bound, or accept a fresh re-snapshot).
//
// **Bounds are bytes.** Both LagBytes and MaxKeepSizeBytes are signed
// int64 byte counts. MaxKeepSizeBytes == -1 (the GUC's documented
// "unlimited" sentinel) signals no retention bound is in effect — the
// pressure-percentage warnings are skipped on that path because there's
// no ceiling to be close to.
//
// **Active liveness is observation-based.** Postgres exposes
// `pg_replication_slots.active` but no first-class "active_since" /
// "inactive_since" timestamp on the slot row itself; the
// `pg_stat_replication_slots.stats_reset` column tracks a different
// concept (stats counter reset). The threshold evaluator tracks the
// most recent observation of `active=true` in-process and computes
// inactivity duration relative to that.
type SlotHealth struct {
	// SlotName is the replication slot the snapshot describes.
	SlotName string

	// Active mirrors `pg_replication_slots.active`. true when a backend
	// is currently consuming from the slot; false when the slot exists
	// but has no consumer (sluice crashed, was paused, or the
	// replication connection dropped).
	Active bool

	// LagBytes is the WAL distance the slot is holding back —
	// `pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)`. Always >= 0
	// in practice (restart_lsn never advances past pg_current_wal_lsn);
	// a negative value would indicate a clock-or-LSN inconsistency
	// worth surfacing rather than silently clamping.
	LagBytes int64

	// MaxKeepSizeBytes is the `max_slot_wal_keep_size` GUC converted to
	// bytes. -1 means unlimited (the GUC's default and "no pressure
	// possible" sentinel); 0 means "slots cannot retain any WAL beyond
	// a checkpoint" which is an extreme operator-set value and is
	// treated as "every non-zero lag is over the bound" by the
	// percentage math; the evaluator special-cases 0 to avoid divide-
	// by-zero.
	MaxKeepSizeBytes int64

	// WALStatus mirrors `pg_replication_slots.wal_status` (PG 13+). One
	// of "reserved", "extended", "unreserved", "lost". Informational —
	// the threshold logic doesn't depend on it, but it's useful in the
	// WARN log line so operators can correlate sluice's view with the
	// raw PG view.
	WALStatus string
}

// SlotHealthReporter is an optional engine-side surface exposing
// retention-pressure + active-liveness signals for a replication slot
// (severity-A finding F13, ADR-0059). Today Postgres is the only
// implementer; MySQL's binlog-retention parallel has a different shape
// and is a separate task.
//
// **The boolean return.** ok=false when the named slot isn't present
// in `pg_replication_slots` (e.g. a fresh stream that hasn't completed
// its first START_REPLICATION call yet, or the operator dropped the
// slot out-of-band). Consumers treat this as "no signal" and skip the
// threshold evaluation for this tick rather than logging a misleading
// warning.
//
// Empty slotName returns an error — the caller didn't supply enough
// info to scope the query.
type SlotHealthReporter interface {
	SlotHealth(ctx context.Context, slotName string) (health SlotHealth, ok bool, err error)
}

// HeartbeatWriter is the optional engine-side surface for the source-
// side periodic heartbeat-table writer — severity-A finding F17 of the
// 2026-05-22 Reddit-research run, see ADR-0060. Engines implement it on
// the same value that satisfies [SchemaReader] (today Postgres and MySQL
// both implement; future engines opt in by satisfying the interface).
//
// **Why operators care:** when a source database has no writes for an
// extended period (off-hours, weekends, dev environments), the CDC
// position the consumer is reading from never advances. On Postgres this
// means the replication slot's `restart_lsn` doesn't move and PG's
// `max_slot_wal_keep_size` policy can eventually evict the slot. On
// MySQL it means the binlog position the consumer is reading from gets
// further from current; if `binlog_expire_logs_seconds` rotates past the
// consumer's position, the consumer loses its ability to resume.
//
// F17's promise: sluice periodically INSERTs a tiny heartbeat row into a
// sluice-owned table on the source. This generates WAL (PG) / binlog
// (MySQL) traffic so the consumer always sees forward progress, even
// against an otherwise-idle source. Operators don't have to manage this
// themselves.
//
// **Opt-in by default.** The heartbeat INSERTs are a behaviour change
// on the source DB (a new sluice-owned table, plus periodic writes
// against it). Operators must explicitly enable via
// `--source-heartbeat-interval=DUR` on the streamer; the empty / zero
// default leaves the engine surface untouched.
//
// **Loud failures are non-fatal.** When EnsureHeartbeatTable returns an
// insufficient-privilege error (PG SQLSTATE 42501 / MySQL Error 1142),
// the pipeline wiring WARNs once and skips the writer goroutine —
// operators on managed DBs / read-replicas where DDL is restricted get a
// stream that still works, just without F17's idle-source resilience.
// Engines surface this case via [ErrHeartbeatPermission] (errors.Is
// matchable) so the wiring can branch deterministically.
type HeartbeatWriter interface {
	// EnsureHeartbeatTable creates the sluice-owned heartbeat table on
	// the source if it doesn't exist. Idempotent — second-and-later
	// calls are no-ops courtesy of CREATE TABLE IF NOT EXISTS. The
	// tableName parameter is the operator-configurable identifier
	// (default `sluice_heartbeat`).
	//
	// Returns an error wrapping [ErrHeartbeatPermission] when the
	// connecting role lacks CREATE TABLE privilege; callers (pipeline
	// wiring) detect this via errors.Is and degrade to "WARN once,
	// skip the writer."
	EnsureHeartbeatTable(ctx context.Context, tableName string) error

	// WriteHeartbeat INSERTs a single row into the named heartbeat
	// table with the current server time and the supplied streamID.
	// Called on the heartbeat-loop's ticker tick.
	WriteHeartbeat(ctx context.Context, tableName, streamID string) error

	// PruneHeartbeat DELETEs rows older than olderThan from the named
	// heartbeat table. Bounded periodic prune so the table doesn't
	// grow unbounded over long-running streams. Returns the rows
	// deleted (operator-facing diagnostic on the next loop tick).
	// olderThan <= 0 is a no-op (returns 0, nil) — callers gate the
	// prune cadence at the pipeline layer.
	PruneHeartbeat(ctx context.Context, tableName string, olderThan time.Duration) (rowsDeleted int64, err error)
}

// ErrHeartbeatPermission is the sentinel engines wrap when the
// connecting role lacks CREATE TABLE / INSERT privilege on the source
// (PG SQLSTATE 42501 / MySQL Error 1142). The pipeline's heartbeat
// wiring errors.Is checks this sentinel to degrade to WARN-once-then-
// skip rather than failing the whole stream.
var ErrHeartbeatPermission = errors.New("heartbeat: insufficient privilege")
