// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// # Per-index ALTER split for large index builds (ADR-0184)
//
// sluice defers secondary-index creation to a post-copy phase and, per
// ADR-0080, collapses all of a table's combinable indexes into ONE
// `ALTER TABLE t ADD INDEX …, ADD UNIQUE INDEX …, …` so InnoDB scans the
// table once instead of once per index. That single-scan win is real on
// ordinary tables and stays the default.
//
// On a LARGE table on a statement-time-limited target (PlanetScale/Vitess,
// which enforce queryserver-config-query-timeout ~900 s, errno 3024) that ONE
// statement blows past the wall — a real region move died on a 12.8 GB table's
// combined ALTER, and `--resume` re-issued the identical walling statement, so
// it never converged. This file conditionally SPLITS that build into one
// `ALTER TABLE … ADD <index>` per index: each statement stays under the wall
// (~1/N of the combined time) and, because each index is its own statement, a
// resume finishes only the indexes still missing instead of re-hitting a
// monolithic wall.
//
// The split engages ONLY when BOTH hold — a VStream flavor AND a table at or
// above [indexSplitPerIndexTableBytes]. Every non-Vitess target and every
// ordinary table keeps the ADR-0080 combined ALTER. The decision lives in the
// single [SchemaWriter.buildTableIndexes] chokepoint (via
// [SchemaWriter.emitIndexBuildStatements]), so it reaches every DIRECT deferred
// build — the whole-schema [CreateIndexes] (migrate + sync cold-start +
// backup/restore), the vanilla concurrent overlap workers, and the VStream
// serial build-as-copied path — identically. The one deferred-build path it
// deliberately does NOT touch is the deploy-request fallback
// (routeIndexJobToFallback, ADR-0148): VReplication runs that build async and
// unbounded by errno 3024, so the combined single-scan ALTER is the right shape
// there.
//
// This composes with, and does not replace, the other levers for the same
// wall: --upfront-indexes (build during copy), --planetscale-raise-query-timeout
// (ADR-0182), and the deploy-request build (ADR-0148, safe-migrations ON).

package mysql

import (
	"context"
	"log/slog"

	"sluicesync.dev/sluice/internal/ir"
)

// indexSplitPerIndexTableBytes is the information_schema DATA_LENGTH floor at
// or above which the deferred index build on a VStream (PlanetScale/Vitess)
// target is SPLIT into one ALTER per index (ADR-0184) instead of the ADR-0080
// combined ALTER.
//
// It is deliberately FAR below [indexFallbackHugeTableBytes] (64 GiB): that
// constant guards the ADR-0148 FK-metadata deploy-request shortcut and is
// intentionally conservative, so the 12.8 GB table that actually walled got no
// help from it. This trigger must catch that table, so it keys off a modest
// DATA_LENGTH floor tuned so the combined build's projected time already
// approaches the wall.
//
// Sizing rationale (the ground truth is a single point — a 12.8 GB table whose
// ONE combined ALTER hit the ~900 s wall, errno 3024). The combined build's
// time scales super-linearly with size, so a table meaningfully smaller than
// 12.8 GB still risks the wall on a slower tier or a cross-region move (higher
// latency, cold buffer pool). 8 GiB sits ~1.6× below the known-walling size —
// the combined build projects to roughly half the wall there and super-linear
// scaling plus a slow tier close the rest — while staying well above ordinary
// tables so the common case keeps ADR-0080's single-scan win. A mis-trigger in
// either direction is benign: below it the combined ALTER still runs (and if it
// walls, --resume + the ADR-0148/0182 levers still apply); above it the split's
// only cost is extra table scans on a memory-non-resident table — which is
// exactly the regime a table this large is already in — in exchange for bounded,
// individually resumable statements.
const indexSplitPerIndexTableBytes = 8 << 30 // 8 GiB

// shouldSplitIndexBuildPerIndex is the PURE ADR-0184 decision: split the
// deferred index build into one ALTER per index (true) instead of the ADR-0080
// combined ALTER (false). It splits ONLY when BOTH hold: the target is a
// statement-time-limited VStream flavor (PlanetScale/Vitess), AND the table's
// DATA_LENGTH is at or above [indexSplitPerIndexTableBytes]. No DB access and
// no side effects, so the flavor×size matrix is unit-testable without a
// container.
func shouldSplitIndexBuildPerIndex(flavor Flavor, dataLengthBytes int64) bool {
	return flavor.usesVStream() && dataLengthBytes >= indexSplitPerIndexTableBytes
}

// buildIndexStatementsFor is the PURE emission dispatcher behind
// [SchemaWriter.emitIndexBuildStatements]: given the flavor, the probed
// DATA_LENGTH and the (already-filtered) pending indexes, it returns the
// statements to execute and whether it took the ADR-0184 per-index split. The
// split branch emits via [emitCreateIndexesPerIndex]; the default via
// [emitCreateIndexesCombined]. Kept side-effect-free (no DB, no logging) so a
// unit test can pin, for every flavor×size cell, BOTH the split decision AND
// the emitted shape without a container.
func buildIndexStatementsFor(flavor Flavor, dataLengthBytes int64, tableName string, pending []*ir.Index, backslashEscapes bool) (stmts []string, split bool, err error) {
	if shouldSplitIndexBuildPerIndex(flavor, dataLengthBytes) {
		s, err := emitCreateIndexesPerIndex(tableName, pending, backslashEscapes)
		return s, true, err
	}
	s, err := emitCreateIndexesCombined(tableName, pending, backslashEscapes)
	return s, false, err
}

// emitIndexBuildStatements renders the ALTER statements one table's pending
// (already skip-filtered, PRIMARY-free, already-existing-index-excluded)
// secondary indexes should build with. It is the single decision point for the
// ADR-0080-combined vs ADR-0184-per-index choice, called from
// [SchemaWriter.buildTableIndexes] — the chokepoint every direct deferred build
// funnels through.
//
// The DATA_LENGTH probe runs ONLY on a VStream flavor (the only target the
// split can engage on), so a non-Vitess target never pays the extra
// information_schema read and its emission is byte-identical to before.
func (w *SchemaWriter) emitIndexBuildStatements(ctx context.Context, tableName string, pending []*ir.Index) ([]string, error) {
	var dataLen int64
	if w.flavor.usesVStream() {
		dataLen = w.tableDataLengthBytes(ctx, tableName)
	}
	stmts, split, err := buildIndexStatementsFor(w.flavor, dataLen, tableName, pending, w.emitter.backslashEscapes)
	if err != nil {
		return nil, err
	}
	if split {
		slog.InfoContext(ctx,
			"mysql: large table on a statement-time-limited target — splitting the deferred index build into one ALTER per index "+
				"to stay under the PlanetScale statement-time limit; total build time may rise but each statement is bounded "+
				"and individually resumable (ADR-0184)",
			slog.String("table", tableName),
			slog.Int("index_count", len(pending)),
			slog.Int64("data_length_bytes", dataLen),
			slog.Int64("split_threshold_bytes", indexSplitPerIndexTableBytes))
	}
	return stmts, nil
}
