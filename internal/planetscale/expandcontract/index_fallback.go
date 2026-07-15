// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// # Deploy-request index-build fallback — the control-plane half (ADR-0148)
//
// [IndexFallback] implements [ir.IndexBuildFallback]: when `migrate`'s
// deferred `ALTER … ADD INDEX` on a PlanetScale target hits the
// statement-time wall (errno 3024) or the safe-migrations direct-DDL
// block (errno 1105), the MySQL writer hands the table's still-pending
// index DDL here, and it builds them through PlanetScale's deploy-request
// workflow on a dev branch — the [LegRunner] machine, freshness gate
// included. Lives in this package (not its own) so it composes the
// ADR-0162 machinery and the fakePS test harness directly; the
// engine-neutral pipeline and the mysql engine never import it — the CLI
// is the composer.
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

	runner := &LegRunner{
		API:           f.API,
		Org:           f.Org,
		Database:      f.Database,
		Branch:        f.Branch,
		Op:            "index-fallback",
		PollInterval:  f.PollInterval,
		DeployTimeout: f.DeployTimeout,
		Out:           &slogLineWriter{ctx: ctx},
		ExecDDL:       f.ExecDDL,
	}
	dr, err := runner.RunDDLLeg(ctx, branchName, ddls,
		"Then re-run with --resume: the index phase re-probes the target and rebuilds only what is still missing.")
	if err != nil {
		return err
	}
	slog.InfoContext(ctx, "planetscale: deploy-request index build complete",
		slog.String("table", table), slog.Int("deploy_request", dr.Number))
	return nil
}

// preflight runs the one cached control-plane gate: the token/org/
// database/branch must resolve and the production branch must have safe
// migrations enabled (the deploy-request prerequisite, ADR-0148 finding
// #1). Any failure is the [ir.ErrIndexBuildFallbackUnavailable] shape —
// the writer then keeps the pre-fallback surface, so a broken token can
// never fail a migrate that would otherwise have surfaced the plain
// errno-3024 hint.
func (f *IndexFallback) preflight(ctx context.Context) error {
	f.preflightOnce.Do(func() {
		br, err := f.API.GetBranch(ctx, f.Org, f.Database, f.branch())
		if err != nil {
			f.preflightErr = fmt.Errorf("%w: control-plane preflight of %s/%s branch %q failed: %w",
				ir.ErrIndexBuildFallbackUnavailable, f.Org, f.Database, f.branch(), err)
			return
		}
		if !br.SafeMigrations {
			// Never auto-enable (ADR-0162 posture; ADR-0148 findings
			// #1/#7): enabling changes how every future schema change on
			// the operator's branch must ship, and the toggle's
			// propagation lag makes a wrap-around flip unsafe.
			f.preflightErr = fmt.Errorf(
				"%w: branch %q of %s/%s does not have safe migrations enabled — PlanetScale refuses deploy requests into it, and sluice never enables the toggle for you; enable it (`pscale branch safe-migrations enable %s %s --org %s`) or use --upfront-indexes",
				ir.ErrIndexBuildFallbackUnavailable, f.branch(), f.Org, f.Database, f.Database, f.branch(), f.Org,
			)
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

// indexFallbackBranchName derives the DETERMINISTIC dev-branch name from
// the table + its pending DDL (the [legBranchName] scheme), so a crashed
// run's branch is found — and refused on — by name instead of minting
// sluice-branch litter. The DDL statements are hashed in order; a
// different pending set (some indexes landed before the wall) is a
// different branch, which is correct — its diff is different too.
func indexFallbackBranchName(table string, ddls []string) string {
	joined := ""
	for _, ddl := range ddls {
		joined += ddl + "\x00"
	}
	return legBranchName("index", table, joined)
}

// slogLineWriter adapts the LegRunner's io.Writer narration to migrate's
// slog stream: each written line becomes one INFO record. LegRunner
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
