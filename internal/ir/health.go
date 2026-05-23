// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import "context"

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
