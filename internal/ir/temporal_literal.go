// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

// Engine-supplied temporal-literal coercion semantics (audit 2026-07-23 D0-5,
// owner call Q1: filtered replicas follow the SOURCE ENGINE's own comparison
// semantics).
//
// A `--where` predicate is evaluated in up to three places that must agree:
// the snapshot SELECT (the source server), an optional server-side stream
// filter (the PG 15+ publication row filter / the VStream filter rule), and
// the client-side row-move evaluator (internal/rowpredicate). For a temporal
// column compared against a literal FINER-grained than the column — a
// time-of-day on a DATE column, or more fractional-second digits than the
// engine's µs resolution — the engines resolve the mismatch in three
// DIFFERENT ways, all observed on real servers 2026-07-23:
//
//   - Postgres 16.14: the unknown-typed literal is CAST TO THE COLUMN's type.
//     On a DATE column the time-of-day is TRUNCATED (`d < '2026-01-15 12:00'`
//     plans and publishes as `d < '2026-01-15'::date`); on a timestamp column
//     fractional seconds round HALF-EVEN to µs ('.1234565' → .123456,
//     '.1234575' → .123458) with carry ('.9999995' → +1s). Comparison
//     precision is the TYPE's µs resolution — a typmod column
//     (timestamp(0)) does NOT truncate the literal to its typmod.
//   - MySQL 8.0.46: the DATE column is PROMOTED to datetime and compared as
//     the full instant (`d = '2026-01-15 08:30'` is FALSE, `d < '2026-01-15
//     12:00'` is TRUE); fractional seconds beyond 6 digits round HALF-UP
//     (away from zero: '.1234565' → .123457) with carry.
//   - MariaDB 11.8.8: promotes the DATE column like MySQL, but fractional
//     seconds beyond 6 digits are TRUNCATED ('.1234565' → .123456,
//     '.1234567' → .123456, '.9999995' → .999999 — no carry).
//
// The client evaluator must reproduce the source's rule or the snapshot and
// CDC legs of one sync classify the same row differently (the D0-5 stale-row
// defect: `d = '2024-01-01 08:30'` on a PG date column snapshot-copies the
// row, then the full-precision client evaluator drops all its CDC changes).
// The SOURCE engine names its rule here — the [CollationResolver] pattern
// applied to the temporal axis — and [rowpredicate.Compile] normalizes each
// temporal literal under it, so the engine-neutral evaluator carries no
// per-engine temporal reasoning. Pinned against all three real servers by
// internal/rowpredicate's temporal ground-truth matrix.

// TemporalLiteralSemantics names how the SOURCE engine coerces a temporal
// comparison whose literal is finer-grained than the column. See the package
// comment above for the observed per-engine behavior.
type TemporalLiteralSemantics uint8

const (
	// TemporalLiteralClientExact is the zero value: no engine lens — the
	// literal is compared at its full parsed precision. It is the safe
	// default for resolvers that do not implement [TemporalLiteralResolver]
	// (and for hand-built test ColumnInfos): the pre-Q1 behavior, with the
	// push-down classifier keeping finer-than-column literals OUT of any
	// server-side push-down envelope as its fail-closed belt.
	TemporalLiteralClientExact TemporalLiteralSemantics = iota
	// TemporalLiteralCastToColumn is the Postgres rule: the literal is cast
	// to the COLUMN's type — truncated to the date on a DATE column, and
	// fractional seconds rounded HALF-EVEN to the µs timestamp resolution.
	TemporalLiteralCastToColumn
	// TemporalLiteralPromoteRoundHalfUp is the MySQL rule: the DATE column
	// is promoted to datetime (the full instant is compared), and literal
	// fractional seconds beyond µs round HALF-UP (away from zero).
	TemporalLiteralPromoteRoundHalfUp
	// TemporalLiteralPromoteTruncate is the MariaDB rule: promotes like
	// MySQL, but literal fractional seconds beyond µs are TRUNCATED.
	TemporalLiteralPromoteTruncate
)

// TemporalLiteralResolver is the optional [CollationResolver] companion
// surface a source engine implements to name its temporal-literal coercion
// rule. A resolver that does not implement it gets
// [TemporalLiteralClientExact] (no normalization) — safe because the
// push-down classifiers fail closed on unnormalized finer-than-column
// literals, and the only engines eligible for continuous filtered sync
// (MySQL-family, Postgres) both implement it.
type TemporalLiteralResolver interface {
	ResolveTemporalLiteralSemantics() TemporalLiteralSemantics
}
