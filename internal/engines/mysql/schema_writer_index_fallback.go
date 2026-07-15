// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// # Deploy-request index-build fallback (ADR-0148, roadmap item 67)
//
// PlanetScale kills a long synchronous `ALTER … ADD INDEX` at its
// max-statement-execution-time wall (errno 3024, ~900 s — live-proven on a
// 49 GB table), and a safe-migrations production branch refuses direct DDL
// outright (errno 1105 "direct DDL is disabled"). Either way the data is
// already copied and only the index is missing — the one recovery that
// needs no re-copy and no `--upfront-indexes` 11× load penalty is building
// that same index through PlanetScale's deploy-request workflow
// (VReplication: async, real wall-clock on large tables, but unbounded by
// the statement-time limit).
//
// This file is the ENGINE half of that fallback: classify the two walled
// error shapes, re-derive the table's still-pending index DDL, and hand it
// to the injected [ir.IndexBuildFallback] (the control-plane half lives in
// internal/planetscale/expandcontract, composed by the CLI — the engine
// never imports the control plane). With no fallback injected — every
// programmatic caller, every non-PlanetScale target — the wrapper is a
// pass-through and the direct build behaves byte-identically to before.
//
// The fallback NEVER makes a run fail where the status quo would not have:
// when it reports itself unavailable (safe migrations off, bad token —
// [ir.ErrIndexBuildFallbackUnavailable]) the original direct error
// surfaces unchanged, so the existing errno-3024 `--upfront-indexes` /
// `--resume` hint path is preserved for the no-token / no-safe-migrations
// case.

package mysql

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	gomysql "github.com/go-sql-driver/mysql"

	"sluicesync.dev/sluice/internal/ir"
)

// indexFallbackHugeTableBytes is the information_schema DATA_LENGTH
// threshold above which a fallback-armed writer SKIPS the doomed direct
// `ALTER … ADD INDEX` and routes the table straight to the deploy-request
// channel — the recorded ADR-0148 optimization that avoids burning the
// ~900 s statement-time attempt on a table that cannot possibly finish
// under it.
//
// The threshold is deliberately CONSERVATIVE (high): the single ground
// truth is a 49 GB table failing at ~901 s — barely over the wall — on a
// small tier, and larger tiers build faster, so only a table well past
// that size is "clearly huge" on ANY tier. A mis-route in either direction
// is harmless: below the threshold the direct attempt still runs (and the
// wall failure still engages the fallback, just after the burn); above it
// a table a big tier could have handled directly merely builds via the
// deploy request instead — same index, still correct. Note that on a
// safe-migrations branch the probe saves almost nothing (the direct
// attempt fails instantly with 1105, no burn) — its value is confined to
// the transitional states where direct DDL still executes.
const indexFallbackHugeTableBytes = 64 << 30 // 64 GiB

// SetIndexBuildFallback implements [ir.IndexBuildFallbackSetter]: the
// orchestrator threads the CLI-composed deploy-request fallback here
// before any index phase runs. nil (the default) keeps the direct-ALTER
// behaviour byte-identical.
func (w *SchemaWriter) SetIndexBuildFallback(f ir.IndexBuildFallback) {
	w.indexBuildFallback = f
}

// Compile-time proof the setter surface is exposed so the orchestrator's
// threading engages.
var _ ir.IndexBuildFallbackSetter = (*SchemaWriter)(nil)

// isIndexBuildWalled reports whether err is one of the two PlanetScale
// refusal shapes the deploy-request fallback exists for:
//
//   - errno 3024 (ER_QUERY_TIMEOUT): "Query execution was interrupted,
//     maximum statement execution time exceeded" — the ~900 s wall killing
//     a large deferred ADD INDEX.
//   - errno 1105 carrying "direct DDL is disabled": the safe-migrations
//     block. 1105 is Vitess's generic wrap-anything code, so the message
//     text is required — a bare 1105 stays out (the transient shapes it
//     also carries belong to classifyApplierError's retry path, not here).
//
// Everything else is NOT walled: a real DDL fault must fail loudly, and a
// classified reparent/grow transient must keep riding the ADR-0114 retry.
func isIndexBuildWalled(err error) bool {
	var mysqlErr *gomysql.MySQLError
	if !errors.As(err, &mysqlErr) {
		return false
	}
	switch mysqlErr.Number {
	case 3024:
		return true
	case 1105:
		return strings.Contains(strings.ToLower(mysqlErr.Message), "direct ddl is disabled")
	}
	return false
}

// buildTableIndexesWithDeployFallback wraps one per-table deferred index
// build (direct) with the ADR-0148 deploy-request fallback. The decision
// tree, in order:
//
//  1. No fallback injected → run direct, byte-identical to before.
//  2. Fallback armed AND the table is clearly huge (DATA_LENGTH probe) →
//     route straight to the fallback, skipping the doomed ~900 s direct
//     attempt. If the fallback reports itself unavailable, fall THROUGH to
//     the direct attempt (never worse than the status quo).
//  3. Direct attempt failed with a walled shape (errno 3024 / 1105
//     direct-DDL) → route the table's still-pending indexes to the
//     fallback. Unavailable → surface the ORIGINAL error so the existing
//     operator hint (--resume / --upfront-indexes / safe-migrations) fires
//     unchanged; a real fallback failure surfaces the fallback's coded
//     error, naming the direct failure it was recovering from.
func (w *SchemaWriter) buildTableIndexesWithDeployFallback(
	ctx context.Context,
	job indexBuildJob,
	direct func(ctx context.Context, job indexBuildJob) error,
) error {
	if w.indexBuildFallback == nil {
		return direct(ctx, job)
	}

	if w.indexFallbackTableClearlyHuge(ctx, job.tableName) {
		slog.InfoContext(ctx,
			"mysql: table is clearly past PlanetScale's statement-time wall; skipping the doomed direct index build and routing straight to a deploy request (ADR-0148)",
			slog.String("table", job.tableName),
			slog.Int64("huge_threshold_bytes", indexFallbackHugeTableBytes))
		switch err := w.routeIndexJobToFallback(ctx, job, nil); {
		case err == nil:
			return nil
		case errors.Is(err, ir.ErrIndexBuildFallbackUnavailable):
			slog.WarnContext(ctx,
				"mysql: deploy-request index fallback unavailable; attempting the direct index build after all",
				slog.String("table", job.tableName),
				slog.String("reason", err.Error()))
			// Fall through to the direct attempt below.
		default:
			return err
		}
	}

	err := direct(ctx, job)
	if err == nil || !isIndexBuildWalled(err) {
		return err
	}

	slog.WarnContext(ctx,
		"mysql: direct index build hit PlanetScale's wall; building via a deploy request instead — VReplication is async and unbounded by errno 3024, but real wall-clock on large tables (ADR-0148)",
		slog.String("table", job.tableName),
		slog.String("direct_error", err.Error()))
	switch ferr := w.routeIndexJobToFallback(ctx, job, err); {
	case ferr == nil:
		return nil
	case errors.Is(ferr, ir.ErrIndexBuildFallbackUnavailable):
		// Keep the pre-fallback surface intact: the original walled error
		// (and its registered hint) is what the operator sees; the WARN
		// names why the automatic recovery didn't engage.
		slog.WarnContext(ctx,
			"mysql: deploy-request index fallback unavailable; surfacing the direct index-build failure",
			slog.String("table", job.tableName),
			slog.String("reason", ferr.Error()))
		return err
	default:
		// Both errors ride the chain: ferr carries the coded
		// SLUICE-E-PS-DEPLOY-REQUEST-FAILED surface, err the walled direct
		// failure that engaged the fallback.
		return fmt.Errorf("mysql: deploy-request index fallback for %q failed (engaged because the direct build failed: %w): %w",
			job.tableName, err, ferr)
	}
}

// routeIndexJobToFallback re-derives the table's STILL-PENDING secondary
// indexes (the same detect-then-skip probe the direct build uses, so any
// index whose ALTER landed before the wall — or on a prior run — is never
// re-sent) and hands their DDL to the fallback in ONE call, so multiple
// failed indexes for a table batch into one dev branch / deploy request.
// The statements are the writer's own [emitCreateIndexesCombined] output —
// byte-identical to what the direct path would have executed.
func (w *SchemaWriter) routeIndexJobToFallback(ctx context.Context, job indexBuildJob, cause error) error {
	wanted := make([]catalogPair, 0, len(job.idxs))
	for _, idx := range job.idxs {
		wanted = append(wanted, catalogPair{table: job.tableName, name: idx.Name})
	}
	existing, err := probeCatalogPairs(ctx, w.db, w.schema, wanted, statisticsPairsQuery)
	if err != nil {
		return fmt.Errorf("mysql: index fallback: probe indexes on %q: %w", job.tableName, err)
	}
	pending := make([]*ir.Index, 0, len(job.idxs))
	for _, idx := range job.idxs {
		if _, ok := existing[foldCatalogPair(job.tableName, idx.Name)]; !ok {
			pending = append(pending, idx)
		}
	}
	if len(pending) == 0 {
		return nil
	}
	stmts, err := emitCreateIndexesCombined(job.tableName, pending)
	if err != nil {
		return err
	}
	return w.indexBuildFallback.BuildIndexDDL(ctx, job.tableName, stmts, cause)
}

// indexFallbackTableClearlyHuge answers the DATA_LENGTH pre-probe: one
// cheap information_schema read per fallback-armed table. The probe is an
// OPTIMIZATION only — any failure (or a NULL/absent row) reports
// not-huge and the normal direct attempt proceeds.
func (w *SchemaWriter) indexFallbackTableClearlyHuge(ctx context.Context, table string) bool {
	const q = `SELECT COALESCE(DATA_LENGTH, 0) FROM information_schema.TABLES
		WHERE table_schema = ? AND table_name = ?`
	var n int64
	if err := w.db.QueryRowContext(ctx, q, w.schema, table).Scan(&n); err != nil {
		slog.DebugContext(ctx, "mysql: index-fallback DATA_LENGTH probe failed; proceeding with the direct build",
			slog.String("table", table), slog.String("err", err.Error()))
		return false
	}
	return n >= indexFallbackHugeTableBytes
}
