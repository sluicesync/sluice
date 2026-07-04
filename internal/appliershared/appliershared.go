// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Package appliershared holds the small helpers that were
// byte-identical between the engine ChangeAppliers before being
// hoisted here. Only helpers with zero engine-specific behaviour
// belong in this package; dialect knowledge (identifier quoting,
// placeholders, error classification) stays in the engine packages,
// and the AIMD batch-size controller has its own home in
// internal/appliercontrol (ADR-0052).
package appliershared

import (
	"context"
	"log/slog"
	"maps"
	"slices"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// Schema picks the schema name the applier uses in SQL. The applier's
// configured schema (derived from the target DSN) is authoritative —
// it is the destination database the operator pointed sluice at. The
// change's source-side schema is metadata only; using it would route
// writes to a same-named schema on the target, which is wrong
// whenever source and target schema names differ (e.g. source_db →
// target_db on the same instance, or any cross-engine pair such as
// MySQL source_db → PG public). changeSchema is honoured only as a
// fallback when the applier wasn't configured with one — which
// shouldn't happen in practice but keeps the function total.
func Schema(defaultSchema, changeSchema string) string {
	if defaultSchema != "" {
		return defaultSchema
	}
	return changeSchema
}

// RunWithDeadline runs f under a wall-clock deadline of `timeout`.
// Zero or negative timeout is a passthrough (f runs to completion
// inline). For positive timeouts, f runs in a goroutine and we race
// its return against a timer: whichever wins, wins.
//
// On timeout we return [context.DeadlineExceeded] (classified
// retriable by each engine's classifyApplierError) so the
// runWithRetry loop reopens the applier on a fresh connection. The
// orphaned f goroutine cannot be cancelled — it will eventually
// return when the underlying state (typically a TCP socket the caller
// closes via Close()) errors out. One orphaned goroutine per timeout
// event is the bounded cost of closing the silent-stall failure mode.
//
// Used by the engines' commitWithTimeout because
// [database/sql.Tx.Commit] takes no context. A package-level function
// so it's testable without constructing a real *sql.Tx; the watchdog
// race semantics are non-trivial enough to deserve direct coverage.
//
// Bug 56 (v0.52.1): the apply path's third TLS-read surface (after
// dispatch's tx.ExecContext + writePositionTx) is the implicit commit
// flush. Pre-v0.52.1 it had no deadline; goroutine pprof on a v0.52.0
// stall showed goroutine 1 blocked at tx.Commit() for >10 min.
func RunWithDeadline(timeout time.Duration, f func() error) error {
	if timeout <= 0 {
		return f()
	}
	done := make(chan error, 1)
	go func() { done <- f() }()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case err := <-done:
		return err
	case <-timer.C:
		return context.DeadlineExceeded
	}
}

// NonGeneratedRowKeys returns the row's keys in sorted order,
// filtering out any column the colTypes map identifies as a generated
// column (Column.GeneratedExpr non-empty). Generated columns cannot
// accept non-DEFAULT values on either MySQL (Error 3105) or PG
// (SQLSTATE 428C9) — INSERT/UPDATE SET against a generated column is
// a hard error on both engines, and including one in a WHERE
// predicate risks silent zero-rows-affected when the target's
// recomputation differs from the source's stored value (precision /
// NULL-coalescing differences are realistic).
//
// Mirrors the bulk-load writers' nonGeneratedColumns filter
// (row_reader.go in each engine); the GitHub issue #12 fix wired the
// CDC apply path to the same filter the bulk-load path already uses
// (ADR-0026:100).
//
// A nil or partial colTypes map (cache cold, column unknown) is
// tolerant: columns not in the map are treated as non-generated and
// included. This preserves the pre-fix shape for unit tests with
// hand-built fixtures and for the small race window before the
// applier's lazy cache populates.
func NonGeneratedRowKeys(row ir.Row, colTypes map[string]*ir.Column) []string {
	all := slices.Sorted(maps.Keys(row))
	if len(colTypes) == 0 {
		return all
	}
	out := make([]string, 0, len(all))
	for _, c := range all {
		col, ok := colTypes[c]
		if ok && col != nil && col.IsGenerated() {
			continue
		}
		out = append(out, c)
	}
	return out
}

// WarnKeyless emits the ADR-0089 keyless-table WARN: the one-time,
// per-table notice that a table with no PRIMARY KEY and no usable unique
// index is getting single-row (non-batched) apply because its INSERTs
// are not idempotent, so keyless CDC is at-least-once. The honest
// at-least-once wording is load-bearing (Bug 143 — the original claim
// that single-row apply "cannot duplicate" was false); centralising the
// prose here keeps the two engines' copies from drifting back to the
// over-promise independently.
//
// The caller owns the fire-once guard (each engine's markWarnedKeyless,
// which touches applier-private cache state under its own lock) and
// invokes this only on the first observation of qn. Two fragments stay
// engine-parameterised because they encode real per-engine semantics,
// NOT accidental drift:
//
//   - engine       — the log-line prefix engine name ("mysql"/"postgres").
//   - missingIndex — how the diagnosis names the absent index. MySQL's
//     ON DUPLICATE KEY UPDATE keys off any unique index, so "unique
//     index"; PG's dispatch needs a NOT-NULL unique index to compute a
//     usable conflict key, so "usable unique index".
//   - indexAdvice  — the remediation index phrasing, same distinction:
//     "a UNIQUE index" (MySQL) vs "NOT NULL UNIQUE index" (PG).
func WarnKeyless(ctx context.Context, engine, qn, missingIndex, indexAdvice string) {
	slog.WarnContext(ctx,
		engine+": applier: table has no PRIMARY KEY or "+missingIndex+" — its INSERTs are "+
			"not idempotent, so keyless CDC is at-least-once: a crash before the source "+
			"transaction's commit checkpoint re-inserts this table's rows from the interrupted "+
			"transaction on resume (keyed tables are exactly-once). Each change is applied as its "+
			"own transaction to bound the window, but rows in the same source transaction still "+
			"replay together. Add a PRIMARY KEY (or "+indexAdvice+") for exactly-once, batched "+
			"throughput (ADR-0089)",
		slog.String("table", qn))
}

// TruncateToken trims a position token to maxLen characters with an
// ellipsis when longer. Mirrors the streamer's truncateDryRunToken
// helper; kept here so the appliers don't import the pipeline
// package. Position tokens are JSON blobs that can run hundreds of
// bytes; the debug log line stays scannable.
func TruncateToken(token string, maxLen int) string {
	if len(token) <= maxLen {
		return token
	}
	return token[:maxLen-1] + "…"
}
