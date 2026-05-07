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
