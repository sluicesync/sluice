//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Bug 317 pre-land review pins: the shapes the independent review found
// the first cut did not cover — a table filter over a views-only
// namespace, the deferred pass's per-view progress and materialized
// views, a materialized view across --resume, the MySQL sync fan-out, and
// the dry-run view count. Same grading as the other Bug 317 files: each
// target view's rows against the source view's own rows.

package pipeline

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/pipeline/migcore"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

func mustTableFilter(t *testing.T, include, exclude []string) migcore.TableFilter {
	t.Helper()
	f, err := migcore.NewTableFilter(include, exclude)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func openBoth(t *testing.T, driver, srcDSN, dstDSN string) (src, dst *sql.DB) {
	t.Helper()
	src, err := sql.Open(driver, srcDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = src.Close() })
	dst, err = sql.Open(driver, dstDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dst.Close() })
	return src, dst
}

// TestMigrate_ViewsOnlyNamespace_MySQLToMySQL_FanoutUnderExcludeTable: an
// --exclude-table naming a table no namespace holds. The views-only
// namespaces reached the "table filter excluded every source table"
// refusal with zero tables and aborted the fan-out at a_rpt.
func TestMigrate_ViewsOnlyNamespace_MySQLToMySQL_FanoutUnderExcludeTable(t *testing.T) {
	srcServer, _, srcCleanup := startMySQL(t)
	defer srcCleanup()
	tgtServer, _, tgtCleanup := startMySQL(t)
	defer tgtCleanup()
	seedMySQLViewsOnlyFanout(t, srcServer)

	mysqlEng, _ := engines.Get("mysql")
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	mig := &Migrator{
		Source: mysqlEng, Target: mysqlEng,
		SourceDSN: serverDSN(t, srcServer), TargetDSN: serverDSN(t, tgtServer),
		DatabaseFilter: DatabaseFilter{Include: []string{"a_rpt", "b_mix", "m_app", "z_rpt"}},
		Filter:         mustTableFilter(t, nil, []string{"audit_log"}),
	}
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Bug 317 review: fan-out with --exclude-table over a views-only database: %v", err)
	}
	src, dst := openBoth(t, "mysql", serverDSN(t, srcServer), serverDSN(t, tgtServer))
	assertViewsMatchSource(t, ctx, src, dst, []string{"a_rpt.v", "b_mix.vm", "z_rpt.v"})
}

// TestMigrate_ViewsOnlyNamespace_PostgresToPostgres_FanoutUnderExcludeTable
// is the Postgres twin (a reader with the scope push-down).
func TestMigrate_ViewsOnlyNamespace_PostgresToPostgres_FanoutUnderExcludeTable(t *testing.T) {
	pgSource, pgTarget, cleanup := startPostgres(t)
	defer cleanup()
	seedPGViewsOnlyFanout(t, pgSource)

	pgEng, _ := engines.Get("postgres")
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	mig := &Migrator{
		Source: pgEng, Target: pgEng, SourceDSN: pgSource, TargetDSN: pgTarget,
		DatabaseFilter: DatabaseFilter{Include: []string{"a_rpt", "b_mix", "m_app", "z_rpt"}},
		Filter:         mustTableFilter(t, nil, []string{"audit_log"}),
	}
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Bug 317 review: fan-out with --exclude-table over a views-only schema: %v", err)
	}
	src, dst := openBoth(t, "pgx", pgSource, pgTarget)
	assertViewsMatchSource(t, ctx, src, dst, []string{"a_rpt.v", "b_mix.vm", "z_rpt.v"})
}

// TestMigrate_ViewsOnlyNamespace_PostgresDeferredPassMaterializedView: a_mv
// holds a MATERIALIZED view and a view reading z_late, which sorts later —
// round 1 creates the matview and fails the view, round 2 must create the
// view without the matview's second CREATE failing (PG has no CREATE OR
// REPLACE MATERIALIZED VIEW). Two layers keep that true — the pass never
// re-issues a created view, and the PG writer skips a matview that already
// exists with the same definition — so this cell is red only when BOTH are
// broken; each layer has its own pin (the unit
// TestCreateDeferredViews_CreatedMaterializedViewIsNotReissued, and
// ..._PostgresMaterializedViewAcrossResume).
func TestMigrate_ViewsOnlyNamespace_PostgresDeferredPassMaterializedView(t *testing.T) {
	pgSource, pgTarget, cleanup := startPostgres(t)
	defer cleanup()
	applyPGDDL(t, pgSource, `
		CREATE SCHEMA m_app;
		CREATE TABLE m_app.t (id INT PRIMARY KEY, k INT NOT NULL);
		INSERT INTO m_app.t VALUES (1, 10), (2, 20), (3, 30);
		CREATE SCHEMA z_late;
		CREATE VIEW z_late.v AS SELECT id FROM m_app.t WHERE k >= 20;
		CREATE SCHEMA a_mv;
		CREATE MATERIALIZED VIEW a_mv.mv AS SELECT id FROM m_app.t WHERE k <= 20;
		CREATE VIEW a_mv.late AS SELECT id FROM z_late.v;
	`)
	pgEng, _ := engines.Get("postgres")
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	mig := &Migrator{
		Source: pgEng, Target: pgEng, SourceDSN: pgSource, TargetDSN: pgTarget,
		DatabaseFilter: DatabaseFilter{Include: []string{"a_mv", "m_app", "z_late"}},
	}
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Bug 317 review: deferred views pass with a materialized view: %v", err)
	}
	src, dst := openBoth(t, "pgx", pgSource, pgTarget)
	assertViewsMatchSource(t, ctx, src, dst, []string{"a_mv.mv", "a_mv.late"})
}

// TestMigrate_ViewsOnlyNamespace_PostgresDeferredPassInterleaved: c_x and
// d_y read each other in both directions — c_x={a1→d_y.b1, a2},
// d_y={b1→c_x.a2, b2→c_x.a1}. Round 1 creates a2 and b1 and neither
// namespace completes, so per-namespace progress refused; per-view progress
// completes in round 2. Kept apart from any namespace that would make
// per-namespace progress in round 1, which would mask the defect.
func TestMigrate_ViewsOnlyNamespace_PostgresDeferredPassInterleaved(t *testing.T) {
	pgSource, pgTarget, cleanup := startPostgres(t)
	defer cleanup()
	applyPGDDL(t, pgSource, `
		CREATE SCHEMA m_app;
		CREATE TABLE m_app.t (id INT PRIMARY KEY, k INT NOT NULL);
		INSERT INTO m_app.t VALUES (1, 10), (2, 20), (3, 30);
		CREATE SCHEMA c_x;
		CREATE SCHEMA d_y;
		CREATE VIEW c_x.a2 AS SELECT id FROM m_app.t;
		CREATE VIEW d_y.b1 AS SELECT id FROM c_x.a2 WHERE id > 1;
		CREATE VIEW c_x.a1 AS SELECT id FROM d_y.b1;
		CREATE VIEW d_y.b2 AS SELECT id FROM c_x.a1 WHERE id < 3;
	`)
	pgEng, _ := engines.Get("postgres")
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	mig := &Migrator{
		Source: pgEng, Target: pgEng, SourceDSN: pgSource, TargetDSN: pgTarget,
		DatabaseFilter: DatabaseFilter{Include: []string{"c_x", "d_y", "m_app"}},
	}
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Bug 317 review: deferred views pass refused an interleaved cross-namespace graph: %v", err)
	}
	src, dst := openBoth(t, "pgx", pgSource, pgTarget)
	assertViewsMatchSource(t, ctx, src, dst, []string{"c_x.a1", "c_x.a2", "d_y.b1", "d_y.b2"})
}

// TestMigrate_ViewsOnlyNamespace_PostgresMaterializedViewAcrossResume: the
// first run's deferred pass creates a materialized view and then fails on a
// view whose relation the run does not copy. The --resume (with that view
// excluded) re-runs the pass from the full list; the matview already exists
// with the same definition and must be skipped, not re-issued into "already
// exists".
func TestMigrate_ViewsOnlyNamespace_PostgresMaterializedViewAcrossResume(t *testing.T) {
	pgSource, pgTarget, cleanup := startPostgres(t)
	defer cleanup()
	applyPGDDL(t, pgSource, `
		CREATE SCHEMA m_app;
		CREATE TABLE m_app.t (id INT PRIMARY KEY, k INT NOT NULL);
		INSERT INTO m_app.t VALUES (1, 10), (2, 20), (3, 30);
		CREATE SCHEMA z_gone;
		CREATE TABLE z_gone.t (id INT PRIMARY KEY);
		CREATE SCHEMA a_mv;
		CREATE MATERIALIZED VIEW a_mv.mv AS SELECT id FROM m_app.t WHERE k >= 20;
		CREATE VIEW a_mv.bad AS SELECT id FROM z_gone.t;
	`)
	pgEng, _ := engines.Get("postgres")
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	mig := &Migrator{
		Source: pgEng, Target: pgEng, SourceDSN: pgSource, TargetDSN: pgTarget,
		DatabaseFilter: DatabaseFilter{Include: []string{"a_mv", "m_app"}},
		MigrationID:    "mv-resume",
	}
	err := mig.Run(ctx)
	if err == nil || !strings.Contains(err.Error(), `view "bad"`) {
		t.Fatalf("first run: want the loud failure naming view \"bad\"; got %v", err)
	}
	vf, err := NewViewFilter(nil, []string{"bad"})
	if err != nil {
		t.Fatal(err)
	}
	mig.Resume = true
	mig.ViewFilter = vf
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Bug 317 review: --resume re-issued an existing materialized view: %v", err)
	}
	src, dst := openBoth(t, "pgx", pgSource, pgTarget)
	assertViewsMatchSource(t, ctx, src, dst, []string{"a_mv.mv"})
}

// TestMigrate_ViewsOnlyNamespace_FanoutDryRunCountsViews pins the dry-run
// plan: a views-only namespace now produces a plan carrying its view count
// (it used to return before the plan was built).
func TestMigrate_ViewsOnlyNamespace_FanoutDryRunCountsViews(t *testing.T) {
	pgSource, pgTarget, cleanup := startPostgres(t)
	defer cleanup()
	seedPGViewsOnlyFanout(t, pgSource)
	pgEng, _ := engines.Get("postgres")
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	var plans []*MigrationPlan
	mig := &Migrator{
		Source: pgEng, Target: pgEng, SourceDSN: pgSource, TargetDSN: pgTarget,
		DatabaseFilter: DatabaseFilter{Include: []string{"a_rpt", "b_mix", "m_app", "z_rpt"}},
		DryRun:         true,
		PlanSink:       func(p *MigrationPlan) { plans = append(plans, p) },
	}
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("dry run: %v", err)
	}
	views := 0
	for _, p := range plans {
		views += p.Views
	}
	if len(plans) != 4 || views != 3 {
		t.Fatalf("dry run produced %d plans carrying %d views; want 4 plans and 3 views (a_rpt, b_mix, z_rpt)", len(plans), views)
	}
}

// TestStreamer_ViewsOnlyNamespace_MySQLToMySQL_Fanout is the MySQL sync
// fan-out cell (the PG one lives beside the other sibling pins).
func TestStreamer_ViewsOnlyNamespace_MySQLToMySQL_Fanout(t *testing.T) {
	srcServer, _, srcCleanup := startMySQLBinlog(t)
	defer srcCleanup()
	tgtServer, tgtHomeDSN, tgtCleanup := startMySQLBinlog(t)
	defer tgtCleanup()
	seedMySQLViewsOnlyFanout(t, srcServer)
	mysqlEng, _ := engines.Get("mysql")

	streamCtx, streamCancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() {
		runErr <- (&Streamer{
			Source: mysqlEng, Target: mysqlEng,
			SourceDSN: serverDSN(t, srcServer), TargetDSN: tgtHomeDSN,
			StreamID:       "views-only-fanout-mysql",
			DatabaseFilter: DatabaseFilter{Include: []string{"a_rpt", "b_mix", "m_app", "z_rpt"}},
		}).Run(streamCtx)
	}()
	returned := false
	defer func() {
		streamCancel()
		if returned {
			return
		}
		select {
		case <-runErr:
		case <-time.After(20 * time.Second):
			t.Error("stream did not return after ctx cancel")
		}
	}()
	if !waitForMySQLDBScalar(t, serverDSN(t, tgtServer), "z_rpt", "SELECT COUNT(*) FROM v", 3, 120*time.Second) {
		select {
		case err := <-runErr:
			returned = true
			t.Fatalf("Bug 317: stream ended before z_rpt.v appeared on the target: %v", err)
		default:
		}
		t.Fatal("Bug 317: z_rpt.v never appeared on the target")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	src, dst := openBoth(t, "mysql", serverDSN(t, srcServer), serverDSN(t, tgtServer))
	assertViewsMatchSource(t, ctx, src, dst, []string{"a_rpt.v", "b_mix.vm", "z_rpt.v"})
}
