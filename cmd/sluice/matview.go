// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	pgkms "sluicesync.dev/sluice/internal/engines/postgres"
	"sluicesync.dev/sluice/internal/progress"
)

// Matview pretty-view phases (ADR-0155 phase 2). `matview refresh` is
// CLI-orchestrated (a single engine call, no pipeline Run with a Progress
// field), so the CLI drives this one-phase checklist inline; on the
// non-TTY path the sink is [progress.Nop] and the historical stdout
// report is byte-identical.
var matviewPhaseRefresh = progress.Phase{Key: "refresh", Label: "Refresh"}

// matviewRefreshProgressSpec is the pretty-view spec for `sluice matview
// refresh`.
var matviewRefreshProgressSpec = progress.Spec{
	Title:      "sluice matview refresh",
	Phases:     []progress.Phase{matviewPhaseRefresh},
	LabelWidth: 12,
}

// MatviewCmd groups subcommands that operate on PostgreSQL materialized
// views. Today there's just `refresh` (Phase 2 of view support, see
// `docs/dev/roadmap.md` item 13); future phases may add `analyze` or
// `vacuum-cluster` companions if operator demand surfaces.
type MatviewCmd struct {
	Refresh MatviewRefreshCmd `cmd:"" help:"Refresh PostgreSQL materialized views on the target. Operator-driven (one-shot subcommand designed for cron / k8s CronJob / Airflow integration); sluice does not own the refresh loop."`
}

// MatviewRefreshCmd implements `sluice matview refresh`. Phase 2 of
// view support — Phase 1's `CREATE MATERIALIZED VIEW ... WITH DATA`
// populates the matview on cold-start; CDC traffic does NOT
// automatically refresh it. Operators wanting `REFRESH MATERIALIZED
// VIEW` on a cadence drive this subcommand from their own scheduler.
//
// PostgreSQL-only — MySQL has no materialized view concept, so the
// command refuses with a clear error on non-postgres targets.
type MatviewRefreshCmd struct {
	TargetDriver string `help:"Target engine name. Must be 'postgres' (matviews are PostgreSQL-only)." required:"" placeholder:"NAME" group:"target"`
	Target       string `help:"Target database DSN." required:"" env:"SLUICE_TARGET" placeholder:"DSN" group:"target"`

	TargetSchema string `help:"PostgreSQL schema to scope the refresh to. Defaults to 'public'. Matches sluice's other --target-schema flags (ADR-0031)." placeholder:"NAME" default:"public"`

	Matview []string `help:"Refresh only these matview names (comma-separated, repeatable). When empty, refreshes every matview in --target-schema. Names match pg_matviews.matviewname case-sensitively." sep:"," placeholder:"NAME"`

	Concurrently bool `help:"Emit REFRESH MATERIALIZED VIEW CONCURRENTLY. Requires a unique index on the matview (PG enforces this). Reads keep working during a concurrent refresh — recommended for non-trivial matviews where blocking-the-readers is unacceptable. Matviews without a unique index are skipped with a clear warning."`

	Format string `help:"Output format: 'text' (human-readable, default) or 'json' (machine-readable for tooling that pipes the result back into a metrics scraper)." default:"text" enum:"text,json" placeholder:"FORMAT"`
}

// Run implements `sluice matview refresh`.
//
// Lifecycle:
//   - Resolve target engine; refuse if not postgres.
//   - Open the target DSN with `database/sql` (the existing engine
//     plumbing).
//   - Call into the postgres package's [pgkms.RefreshMatviews].
//   - Render the result to stdout per --format.
//
// On any error: exit non-zero with the error rendered to stderr so
// the operator's cron job's exit-status branching gets a clean
// signal. The partial result (matviews refreshed before the failure)
// is preserved in the rendered output regardless.
func (m *MatviewRefreshCmd) Run(g *Globals) error {
	if m.TargetDriver != "postgres" {
		return fmt.Errorf("--target-driver=%q: matview refresh is PostgreSQL-only (MySQL has no materialized view concept)", m.TargetDriver)
	}
	if strings.TrimSpace(m.Target) == "" {
		return errors.New("--target is required")
	}

	db, err := sql.Open("pgx", m.Target)
	if err != nil {
		return fmt.Errorf("open target: %w", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping target: %w", err)
	}

	opts := pgkms.MatviewRefreshOptions{
		Schema:       m.TargetSchema,
		Matviews:     m.Matview,
		Concurrently: m.Concurrently,
	}

	// ADR-0155: pretty TTY view only for an interactive text run to stdout
	// (not --format json, and gated by the shared wantPrettyProgress). When
	// pretty, the live view owns stdout, so the per-matview report is
	// suppressed — the summary panel replaces it.
	pretty := (m.Format == "" || m.Format == "text") &&
		wantPrettyProgress(g, false, false, false)
	var (
		result *pgkms.MatviewRefreshResult
		sink   progress.Sink = progress.Nop{}
	)
	runErr := runWithProgress(pretty, cancel, matviewRefreshProgressSpec,
		func(s progress.Sink) { sink = s },
		func() error {
			sink.PhaseStarted(matviewPhaseRefresh)
			var e error
			result, e = pgkms.RefreshMatviews(ctx, db, opts)
			if e != nil {
				return e
			}
			sink.PhaseCompleted(matviewPhaseRefresh)
			sink.Summary(progress.Result{Fields: []progress.Field{
				{Label: "Refreshed", Value: progress.HumanCount(int64(len(result.Refreshed)))},
				{Label: "Skipped", Value: progress.HumanCount(int64(len(result.Skipped)))},
			}})
			return nil
		})
	// Render whatever was accumulated regardless of error — operators
	// piping the output to a metrics scraper benefit from seeing the
	// per-matview timing even on partial failures. Suppressed under the
	// pretty view, which owns stdout.
	if result != nil && !pretty {
		renderMatviewResult(result, m.Format)
	}
	return runErr
}

// renderMatviewResult prints the refresh outcome in the chosen format.
// Text format is human-readable; JSON is the machine-consumable shape
// for piping through `jq` / Prometheus metrics scrapers.
func renderMatviewResult(result *pgkms.MatviewRefreshResult, format string) {
	if format == "json" {
		renderMatviewResultJSON(result)
		return
	}
	if len(result.Refreshed) == 0 && len(result.Skipped) == 0 {
		fmt.Println("matview refresh: no matviews matched the filter (nothing refreshed)")
		return
	}
	for _, r := range result.Refreshed {
		fmt.Printf("refreshed: %s.%s (%s)\n", r.Schema, r.Name, r.Duration.Round(time.Millisecond))
	}
	for _, s := range result.Skipped {
		fmt.Printf("skipped:   %s.%s — %s\n", s.Schema, s.Name, s.Reason)
	}
	fmt.Printf("\nmatview refresh: %d refreshed, %d skipped\n", len(result.Refreshed), len(result.Skipped))
}

// renderMatviewResultJSON emits a stable JSON shape. Fields use
// snake_case to match sluice's other JSON-mode outputs (sync-health,
// schema preview).
func renderMatviewResultJSON(result *pgkms.MatviewRefreshResult) {
	var sb strings.Builder
	sb.WriteString(`{"refreshed":[`)
	for i, r := range result.Refreshed {
		if i > 0 {
			sb.WriteByte(',')
		}
		fmt.Fprintf(&sb, `{"schema":%q,"name":%q,"duration_ms":%d}`,
			r.Schema, r.Name, r.Duration.Milliseconds())
	}
	sb.WriteString(`],"skipped":[`)
	for i, s := range result.Skipped {
		if i > 0 {
			sb.WriteByte(',')
		}
		fmt.Fprintf(&sb, `{"schema":%q,"name":%q,"reason":%q}`,
			s.Schema, s.Name, s.Reason)
	}
	sb.WriteString(`]}`)
	fmt.Println(sb.String())
}
