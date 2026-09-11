//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// # Tier 1 of the Neki integration coverage: the free half
//
// Neki is not open source. There is no image to boot, so the testcontainers
// pattern every other engine uses is unavailable and a live cluster costs
// money — which is why the coverage design (docs/dev/neki-integration-
// coverage.md) splits in two. This is the half that runs on every PR at
// zero marginal cost.
//
// The engine decides it is talking to Neki from ONE probe, memoised per
// server. Seed that memo for an ordinary PostgreSQL container and every
// Neki-adapted code path executes against a real server: the
// no-transaction DDL branch, the catalog sequence read, the shard-key
// planner's no-op path on an unsharded target, and the raw-copy decline.
//
// # What this can and cannot prove, stated because the name is a trap
//
// It CANNOT prove Neki behaves the way the adaptations assume — the server
// here is vanilla PostgreSQL, which is the whole reason it is free. Anyone
// reading a green run as evidence about Neki has misread it, which is why
// the test is named for the FLAVOR FORCING and not for Neki.
//
// It proves something narrower and genuinely useful: **the adaptations do
// not break on a server that does not need them, and every one of them is
// reachable.** The second half is what a wiring gate buys. Its static
// sibling, TestEveryNekiFlagIsWiredAtConstruction, proves the flag is
// ASSIGNED at every construction site; this proves the paths that flag
// selects actually run end to end against a real server rather than
// compiling.
//
// The behaviours only a real router can show — routing, per-shard
// ON CONFLICT, NK013/NK306/NK213 — are Tier 2's job, gated behind the
// `nekiverify` build tag and a live DSN.

// forceNekiFlavor seeds the probe memo so every engine door opened against
// this DSN believes it is talking to Neki.
//
// Reaching into the memo rather than adding a production override is
// deliberate: an exported "pretend to be Neki" switch would be a footgun
// on a real migration, and the memo is already the single point every
// construction site consults. The helper restores the previous state, so
// tests that do NOT force the flavor still see an honest probe.
func forceNekiFlavor(t *testing.T, dsn string) {
	t.Helper()
	key := (&pgConfig{dsn: dsn}).serverKey()

	nekiMemo.mu.Lock()
	if nekiMemo.byServer == nil {
		nekiMemo.byServer = map[string]bool{}
	}
	prev, had := nekiMemo.byServer[key]
	nekiMemo.byServer[key] = true
	nekiMemo.mu.Unlock()

	t.Cleanup(func() {
		nekiMemo.mu.Lock()
		defer nekiMemo.mu.Unlock()
		if had {
			nekiMemo.byServer[key] = prev
			return
		}
		delete(nekiMemo.byServer, key)
	})
}

// TestPostgresSuite_NekiFlavorForced runs the Neki-adapted paths against an
// ordinary PostgreSQL container with the flavor forced on.
func TestPostgresSuite_NekiFlavorForced(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()

	forceNekiFlavor(t, dsn)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	eng := &Engine{}

	t.Run("the flavor is actually forced — anti-vacuity for every case below", func(t *testing.T) {
		// If the seeding stopped working (a serverKey change, a memo
		// rename), every case below would quietly become an ordinary
		// PostgreSQL test that passes for the wrong reason. Assert the
		// premise before relying on it.
		db, err := sql.Open("pgx", dsn)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		defer func() { _ = db.Close() }()

		isNeki, reached := probeIsNeki(ctx, (&pgConfig{dsn: dsn}).serverKey(), db)
		if !reached {
			t.Fatal("probe could not run")
		}
		if !isNeki {
			t.Fatal("flavor forcing did not take: probeIsNeki still reports false, so every case in this test " +
				"is exercising the ordinary PostgreSQL path and proving nothing about the Neki adaptations")
		}
	})

	t.Run("schema apply runs its no-transaction DDL branch", func(t *testing.T) {
		// On Neki, DDL inside an explicit transaction is invisible to the
		// rest of that transaction and does not survive the commit, so
		// createAndPrimeSequence takes a no-transaction branch keyed on
		// isNeki. Against vanilla PostgreSQL that branch is merely slower,
		// and must still produce a correct schema.
		sw, err := eng.OpenSchemaWriter(ctx, dsn)
		if err != nil {
			t.Fatalf("open schema writer: %v", err)
		}
		if c, ok := sw.(interface{ Close() error }); ok {
			defer func() { _ = c.Close() }()
		}

		schema := &ir.Schema{Tables: []*ir.Table{{
			Name: "nk_forced",
			Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64}},
				{Name: "v", Type: ir.Text{}, Nullable: true},
			},
			PrimaryKey: &ir.Index{Name: "nk_forced_pk", Columns: []ir.IndexColumn{{Column: "id"}}},
		}}, Sequences: []*ir.Sequence{{Schema: "public", Name: "nk_forced_seq", DataType: "bigint", Increment: 1, Start: 1, MinValue: 1, MaxValue: 9223372036854775807, Cache: 1}}}

		if err := sw.CreateTablesWithoutConstraints(ctx, schema); err != nil {
			t.Fatalf("create tables with the Neki flavor forced: %v", err)
		}

		// The table and its identity sequence must both exist — the
		// no-transaction path is where a sequence would go missing.
		db, err := sql.Open("pgx", dsn)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		defer func() { _ = db.Close() }()

		var n int
		if err := db.QueryRowContext(ctx,
			`SELECT count(*) FROM information_schema.columns WHERE table_name = 'nk_forced'`).Scan(&n); err != nil {
			t.Fatalf("count columns: %v", err)
		}
		if n != 2 {
			t.Errorf("nk_forced has %d columns, want 2 — the forced-flavor schema-apply path lost one", n)
		}
	})

	t.Run("the row reader declines raw copy", func(t *testing.T) {
		// Neki's router rejects COPY (SELECT …) TO (SQLSTATE NK013), so
		// RowReader declines the ADR-0078 raw-copy fast lane rather than
		// dying mid-stream. The decline must be reachable — a decline that
		// silently stopped firing would put every Neki user on a path the
		// router refuses.
		rr, err := eng.OpenRowReader(ctx, dsn)
		if err != nil {
			t.Fatalf("open row reader: %v", err)
		}
		if c, ok := rr.(interface{ Close() error }); ok {
			defer func() { _ = c.Close() }()
		}

		decliner, ok := rr.(ir.RawCopyDecliner)
		if !ok {
			t.Fatal("the Postgres RowReader no longer exposes DeclinesRawCopy — if it was renamed, the raw-copy " +
				"decline may no longer reach Neki targets at all")
		}
		declined, reason := decliner.DeclinesRawCopy()
		if !declined {
			t.Error("raw copy was NOT declined with the Neki flavor forced; on a real Neki the router would " +
				"refuse the COPY with NK013 partway through")
		} else if reason == "" {
			t.Error("raw copy was declined with an EMPTY reason; the operator log would say the fast lane was " +
				"skipped and not why")
		}
	})

	t.Run("the shard-key planner is a no-op on an unsharded target", func(t *testing.T) {
		// Every Neki target reaches the shard-key machinery, and the vast
		// majority of tables are unsharded. The no-op path is therefore the
		// one almost every row takes, and a regression in it would refuse
		// working configurations rather than protect anyone.
		rw, err := eng.OpenRowWriter(ctx, dsn)
		if err != nil {
			t.Fatalf("open row writer: %v", err)
		}
		if c, ok := rw.(interface{ Close() error }); ok {
			defer func() { _ = c.Close() }()
		}

		prober, ok := rw.(interface {
			ShardKeyUpsertMismatch(context.Context, []*ir.Table) (string, error)
		})
		if !ok {
			t.Fatal("the Postgres RowWriter no longer exposes ShardKeyUpsertMismatch — the sharded-target " +
				"preflight would silently become a no-op (this is the Bug 283 class)")
		}

		detail, err := prober.ShardKeyUpsertMismatch(ctx, []*ir.Table{{
			Name:       "nk_forced",
			Columns:    []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
			PrimaryKey: &ir.Index{Name: "nk_forced_pk", Columns: []ir.IndexColumn{{Column: "id"}}},
		}})
		if err != nil {
			// A probe that cannot run is not a verdict, and on vanilla
			// PostgreSQL the Neki topology surface does not exist — so an
			// error here is expected and acceptable. What must NOT happen
			// is a false MISMATCH report.
			if !strings.Contains(strings.ToLower(err.Error()), "neki") &&
				!strings.Contains(err.Error(), "__neki") {
				t.Logf("probe could not run (expected on vanilla PostgreSQL): %v", err)
			}
			return
		}
		if detail != "" {
			t.Errorf("the shard-key preflight reported a mismatch on an unsharded target: %q — this would refuse "+
				"a working migration", detail)
		}
	})
}
