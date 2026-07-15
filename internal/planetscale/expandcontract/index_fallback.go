// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// # Deploy-request index-build fallback — the control-plane half (ADR-0148)
//
// [IndexFallback] implements [ir.IndexBuildFallback]: when `migrate`'s
// deferred `ALTER … ADD INDEX` on a PlanetScale target hits the
// statement-time wall (errno 3024) or the safe-migrations direct-DDL
// block (errno 1105), the MySQL writer hands the table's still-pending
// index DDL here, and it builds them through PlanetScale's deploy-request
// workflow on a dev branch — composing the same [legRunner] machine
// (ADR-0165) the expand-contract legs and `sluice deploy-ddl` ride,
// freshness gate included. Lives in this package so it composes that
// machinery and the fakePS test harness directly; the engine-neutral
// pipeline and the mysql engine never import it — the CLI is the
// composer.
//
// Posture (the item-67 simplification of ADR-0148's open questions):
// safe migrations must ALREADY be ON — sluice never toggles it (the
// enable/disable propagation lag, ADR-0148 finding #7, makes any
// toggle-around design unsafe, and flipping it changes how every future
// schema change on the operator's branch must ship). When it is off, or
// the token/preflight fails, BuildIndexDDL reports
// [ir.ErrIndexBuildFallbackUnavailable] and the writer surfaces the
// ORIGINAL direct error with its existing --upfront-indexes / --resume
// hint — the no-token / no-safe-migrations path is byte-compatible with
// pre-fallback behaviour.
//
// Speed expectations, stated honestly: the deploy itself is VReplication
// — real wall-clock on large tables, but async and unbounded by errno
// 3024. The fallback's value is UNBLOCKING (the index lands on the
// already-copied data with no re-copy), not speed.

package expandcontract

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/planetscale/api"
)

// IndexFallback is the PlanetScale deploy-request implementation of
// [ir.IndexBuildFallback]. Construct once per migrate run (the CLI arms
// it only for a planetscale target with an org + service token) and let
// the pipeline thread it onto the target SchemaWriter.
type IndexFallback struct {
	// API is the shared control-plane client. Required.
	API *api.Client

	// Org, Database, Branch identify the PRODUCTION branch the migrate
	// targets (Branch defaults to "main"). Database is the PlanetScale
	// database name — the CLI derives it from the target DSN when the
	// operator doesn't pass it explicitly.
	Org      string
	Database string
	Branch   string

	// PollInterval / DeployTimeout shape the deploy-request polling.
	// Zero values default to 10s / 1h.
	PollInterval  time.Duration
	DeployTimeout time.Duration

	// ExecDDL overrides the branch-DDL executor (tests). nil = real.
	ExecDDL func(ctx context.Context, pw *api.BranchPassword, database, ddl string) error

	// preflightOnce caches the one control-plane preflight (branch
	// existence + safe migrations) across every table the run routes
	// here — the answer cannot change mid-run in a way sluice should
	// chase, and one GET per run keeps the fallback's control-plane
	// footprint flat.
	preflightOnce sync.Once
	preflightErr  error
}

// Compile-time proof the fallback satisfies the engine-facing surface.
var _ ir.IndexBuildFallback = (*IndexFallback)(nil)

// BuildIndexDDL implements [ir.IndexBuildFallback]: one dev branch + one
// deploy request per TABLE, carrying every still-pending index DDL
// statement for it (the writer batches combinable indexes into combined
// ALTERs already, so a multi-index table ships as one deploy).
func (f *IndexFallback) BuildIndexDDL(ctx context.Context, table string, ddls []string, cause error) error {
	if len(ddls) == 0 {
		return nil
	}
	if err := f.preflight(ctx); err != nil {
		return err
	}

	branchName := indexFallbackBranchName(table, ddls)
	slog.InfoContext(ctx,
		"planetscale: building indexes via a deploy request — VReplication is async and unbounded by the statement-time wall, but real wall-clock on large tables (ADR-0148)",
		slog.String("table", table),
		slog.Int("ddl_statements", len(ddls)),
		slog.String("dev_branch", branchName),
		slog.Bool("preemptive", cause == nil))

	out := &slogLineWriter{ctx: ctx}
	exec := f.execDDLFunc()
	r := &legRunner{
		api:           f.API,
		org:           f.Org,
		database:      f.Database,
		branch:        f.branch(),
		pollInterval:  f.pollInterval(),
		deployTimeout: f.deployTimeout(),
		out:           out,
		execDDL:       exec,
		name:          "index-fallback",
		errPrefix:     "index-fallback",
		passwordName:  "sluice-index-fallback",

		// migrate's recovery is always --resume: the index phase
		// re-probes the target and rebuilds only what is still missing,
		// so an already-deployed DR's indexes are detected and skipped.
		leftoverAdvice:        "continue with --resume (the index phase re-probes the target and rebuilds only what is still missing)",
		alreadyDeployedAdvice: "close the DR, delete the dev branch, and re-run with --resume — the index phase detects already-built indexes and skips them",
		reviewTimeoutAdvice:   "approve it and re-run with --resume",
		deployTimeoutAdvice:   "watch it at the URL and re-run with --resume once it completes — already-deployed indexes are detected and skipped",
	}
	// The leg machine applies ONE ddl before opening the deploy request;
	// a multi-index table's remainder (each FULLTEXT/SPATIAL must be its
	// own statement) rides the post-DDL stage hook, on the same branch
	// and password, so everything still ships in the ONE deploy request.
	if len(ddls) > 1 {
		rest := ddls[1:]
		r.stage = func(ctx context.Context, pw *api.BranchPassword) error {
			for _, ddl := range rest {
				if err := exec(ctx, pw, f.Database, ddl); err != nil {
					return fmt.Errorf("apply DDL on dev branch %q: %w", branchName, err)
				}
				fmt.Fprintf(out, "index-fallback: applied DDL on %q: %s\n", branchName, ddl)
			}
			return nil
		}
	}

	// Per-call cleanup: the dev branch is deleted on every exit path
	// (best-effort, cancel-immune — the branchCleanup contract).
	cleanup := &branchCleanup{
		api:      f.API,
		org:      f.Org,
		database: f.Database,
		out:      out,
		command:  "index-fallback",
	}
	defer cleanup.run(ctx)

	dr, err := r.run(ctx, branchName, ddls[0], cleanup)
	if err != nil {
		return err
	}
	slog.InfoContext(ctx, "planetscale: deploy-request index build complete",
		slog.String("table", table), slog.Int("deploy_request", dr.Number))
	return nil
}

// preflight runs the one cached control-plane gate, reusing the shared
// [preflightSafeMigrations] (token/org/database/branch resolve + the
// safe-migrations prerequisite, ADR-0148 finding #1 — never auto-enabled,
// findings #1/#7). Any failure is wrapped in the
// [ir.ErrIndexBuildFallbackUnavailable] shape — the writer then keeps the
// pre-fallback surface, so a broken token or a safe-migrations-off branch
// can never fail a migrate that would otherwise have surfaced the plain
// errno-3024 hint (the coded refusal inside is only ever logged, never
// the run error).
func (f *IndexFallback) preflight(ctx context.Context) error {
	f.preflightOnce.Do(func() {
		if err := preflightSafeMigrations(ctx, f.API, f.Org, f.Database, f.branch(), "index-fallback"); err != nil {
			f.preflightErr = fmt.Errorf("%w: %w", ir.ErrIndexBuildFallbackUnavailable, err)
		}
	})
	return f.preflightErr
}

func (f *IndexFallback) branch() string {
	if f.Branch == "" {
		return "main"
	}
	return f.Branch
}

func (f *IndexFallback) pollInterval() time.Duration {
	if f.PollInterval <= 0 {
		return 10 * time.Second
	}
	return f.PollInterval
}

func (f *IndexFallback) deployTimeout() time.Duration {
	if f.DeployTimeout <= 0 {
		return time.Hour
	}
	return f.DeployTimeout
}

func (f *IndexFallback) execDDLFunc() func(ctx context.Context, pw *api.BranchPassword, database, ddl string) error {
	if f.ExecDDL != nil {
		return f.ExecDDL
	}
	return execBranchDDL
}

// indexFallbackBranchName derives the DETERMINISTIC dev-branch name from
// the table + its pending DDL (the shared [legBranchName] scheme), so a
// crashed run's branch is found — and refused on — by name instead of
// minting sluice-branch litter. The DDL statements are hashed in order;
// a different pending set (some indexes landed before the wall) is a
// different branch, which is correct — its diff is different too.
func indexFallbackBranchName(table string, ddls []string) string {
	joined := ""
	for _, ddl := range ddls {
		joined += ddl + "\x00"
	}
	return legBranchName("index", table, joined)
}

// slogLineWriter adapts the legRunner's io.Writer narration to migrate's
// slog stream: each written line becomes one INFO record. The runner
// writes whole lines per step, so the trailing-newline trim is the only
// shaping needed.
type slogLineWriter struct {
	ctx context.Context
}

func (w *slogLineWriter) Write(p []byte) (int, error) {
	msg := string(p)
	for msg != "" && (msg[len(msg)-1] == '\n' || msg[len(msg)-1] == '\r') {
		msg = msg[:len(msg)-1]
	}
	if msg != "" {
		slog.InfoContext(w.ctx, "planetscale: "+msg)
	}
	return len(p), nil
}
