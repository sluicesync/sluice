// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

// Materialized-view refresh path. Phase 2 of view support
// (`docs/dev/roadmap.md` item 13). Phase 1 emits `CREATE MATERIALIZED
// VIEW ... WITH DATA` so cold-start populates matviews from the
// just-loaded target tables; matviews don't auto-update on CDC
// traffic, so operators have asked for a sluice-native way to
// trigger `REFRESH MATERIALIZED VIEW`.
//
// Design choice (operator-cadence agnostic): one-shot subcommand
// `sluice matview refresh` that the operator drives from their own
// scheduler (cron, k8s CronJob, Airflow, etc.). Sluice deliberately
// doesn't own a refresh loop because (1) cadence is operator-policy,
// not data-mover concern, and (2) integrating with operator
// scheduling means the operator already has alerting, backoff, and
// observability for the cron itself.

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"time"
)

// MatviewRefreshOptions configures a single `sluice matview refresh`
// invocation. The orchestrator builds this from the CLI flags.
type MatviewRefreshOptions struct {
	// Schema is the PG schema namespace to scope the refresh to.
	// Empty defaults to "public" (matches sluice's other PG paths).
	Schema string

	// Matviews, when non-empty, filters the refresh to only these
	// matview names (within Schema). Empty means "refresh every
	// matview in Schema." Names are matched case-sensitively against
	// `pg_matviews.matviewname`.
	Matviews []string

	// Concurrently emits `REFRESH MATERIALIZED VIEW CONCURRENTLY`
	// instead of the locking refresh. Concurrent refresh requires a
	// unique index on the matview; we check pg_indexes for that
	// up-front and fall back to a clear error naming the matview if
	// no unique index exists. Reads keep working during a concurrent
	// refresh (the load-bearing operator win for non-trivial matviews).
	Concurrently bool
}

// MatviewRefreshResult summarises one refresh invocation's outcome.
// Each Matview row carries the operation timing + the wall-clock the
// refresh took, so operators piping the output through a metrics
// scraper can flag matview-level regressions.
type MatviewRefreshResult struct {
	Refreshed []MatviewRefreshTiming
	Skipped   []MatviewRefreshSkip
}

// MatviewRefreshTiming names a successfully-refreshed matview and how
// long the REFRESH command took.
type MatviewRefreshTiming struct {
	Schema   string
	Name     string
	Duration time.Duration
}

// MatviewRefreshSkip names a matview that the refresh path skipped +
// the reason. Today the only documented skip case is "--concurrently
// requested but no unique index"; future enhancements may add
// per-matview filter mismatches.
type MatviewRefreshSkip struct {
	Schema string
	Name   string
	Reason string
}

// RefreshMatviews lists the matviews in opts.Schema (filtered by
// opts.Matviews when non-empty) and runs `REFRESH MATERIALIZED VIEW
// [CONCURRENTLY] schema.name` against each. Returns the aggregate
// result so the CLI can render a human-readable summary.
//
// Order: matviews refresh alphabetically by name. Nested matview
// dependencies (matview A reads from matview B) are NOT auto-ordered
// in Phase 2 MVP — operators with nested matviews should either pass
// `--matview` repeatedly with the right ordering, or invoke the
// subcommand twice (idempotent). A dedicated dependency-ordered
// refresh is a Phase 2.1 follow-on if real workloads surface the
// need.
//
// On error: returns the partial result accumulated so far plus the
// error. Operators piping output to a scheduler can branch on the
// error to retry / alert; the partial result records which matviews
// completed before the failure.
func RefreshMatviews(ctx context.Context, db *sql.DB, opts MatviewRefreshOptions) (*MatviewRefreshResult, error) {
	schema := opts.Schema
	if schema == "" {
		schema = "public"
	}

	matviews, err := listMatviews(ctx, db, schema)
	if err != nil {
		return nil, fmt.Errorf("list matviews in schema %q: %w", schema, err)
	}

	filtered := filterMatviewsByName(matviews, opts.Matviews)
	if err := validateMatviewFilter(opts.Matviews, matviews); err != nil {
		return nil, err
	}

	result := &MatviewRefreshResult{}
	for _, mv := range filtered {
		if opts.Concurrently {
			hasUnique, err := matviewHasUniqueIndex(ctx, db, schema, mv)
			if err != nil {
				return result, fmt.Errorf("check unique index for matview %q.%q: %w", schema, mv, err)
			}
			if !hasUnique {
				result.Skipped = append(result.Skipped, MatviewRefreshSkip{
					Schema: schema,
					Name:   mv,
					Reason: "concurrent refresh requires a unique index on the matview; none found (create a unique index on one or more columns, or drop --concurrently)",
				})
				slog.Warn("matview refresh skipped — no unique index for concurrent refresh",
					"schema", schema, "matview", mv)
				continue
			}
		}

		stmt := buildRefreshStatement(schema, mv, opts.Concurrently)
		slog.Info("refreshing matview", "schema", schema, "matview", mv, "concurrently", opts.Concurrently)
		start := time.Now()
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return result, fmt.Errorf("refresh matview %q.%q: %w", schema, mv, err)
		}
		took := time.Since(start)
		result.Refreshed = append(result.Refreshed, MatviewRefreshTiming{
			Schema:   schema,
			Name:     mv,
			Duration: took,
		})
		slog.Info("matview refreshed", "schema", schema, "matview", mv, "duration_ms", took.Milliseconds())
	}
	return result, nil
}

// listMatviews returns the alphabetically-ordered list of matview
// names in `schema`. Uses pg_matviews (PG-supplied catalog view) for
// engine-version-portable lookup.
func listMatviews(ctx context.Context, db *sql.DB, schema string) ([]string, error) {
	const q = `
		SELECT matviewname
		FROM   pg_matviews
		WHERE  schemaname = $1
		ORDER  BY matviewname`
	rows, err := db.QueryContext(ctx, q, schema)
	if err != nil {
		return nil, fmt.Errorf("query pg_matviews: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("scan pg_matviews: %w", err)
		}
		out = append(out, name)
	}
	return out, rows.Err()
}

// filterMatviewsByName narrows `available` to only entries whose
// name appears in `requested`. Empty `requested` returns `available`
// unchanged.
func filterMatviewsByName(available, requested []string) []string {
	if len(requested) == 0 {
		return available
	}
	want := make(map[string]bool, len(requested))
	for _, n := range requested {
		want[n] = true
	}
	out := make([]string, 0, len(available))
	for _, mv := range available {
		if want[mv] {
			out = append(out, mv)
		}
	}
	return out
}

// validateMatviewFilter ensures every name in `requested` exists in
// `available`. Returns a clear operator-actionable error naming the
// missing matview(s); an empty `requested` is a no-op.
//
// Loud-failure on a typo: better to refuse the refresh than silently
// no-op when the operator misspelled a matview name.
func validateMatviewFilter(requested, available []string) error {
	if len(requested) == 0 {
		return nil
	}
	have := make(map[string]bool, len(available))
	for _, n := range available {
		have[n] = true
	}
	var missing []string
	for _, n := range requested {
		if !have[n] {
			missing = append(missing, n)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("matview refresh: %d requested matview(s) not found in target schema: %s",
			len(missing), strings.Join(missing, ", "))
	}
	return nil
}

// matviewHasUniqueIndex reports whether the matview at schema.name
// has a unique index, which is required by PG for `REFRESH
// MATERIALIZED VIEW CONCURRENTLY`. Used by the concurrent-refresh
// pre-check.
func matviewHasUniqueIndex(ctx context.Context, db *sql.DB, schema, name string) (bool, error) {
	const q = `
		SELECT EXISTS (
			SELECT 1
			FROM   pg_indexes
			WHERE  schemaname = $1
			  AND  tablename  = $2
			  AND  indexdef LIKE 'CREATE UNIQUE INDEX%'
		)`
	var present bool
	if err := db.QueryRowContext(ctx, q, schema, name).Scan(&present); err != nil {
		return false, fmt.Errorf("query pg_indexes: %w", err)
	}
	return present, nil
}

// buildRefreshStatement returns the `REFRESH MATERIALIZED VIEW
// [CONCURRENTLY] "schema"."name"` statement. Identifier quoting goes
// through the package's existing helper to handle reserved-word
// names + mixed-case identifiers correctly.
func buildRefreshStatement(schema, name string, concurrently bool) string {
	var b strings.Builder
	b.WriteString("REFRESH MATERIALIZED VIEW ")
	if concurrently {
		b.WriteString("CONCURRENTLY ")
	}
	b.WriteString(quoteIdent(schema))
	b.WriteByte('.')
	b.WriteString(quoteIdent(name))
	return b.String()
}
