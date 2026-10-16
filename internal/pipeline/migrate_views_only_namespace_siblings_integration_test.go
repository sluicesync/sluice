//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Bug 317 sibling pins: the other entry points that read a views-only
// namespace — schema preview, schema diff, backup full → restore, and the
// sync cold start (fan-out and single-namespace). Same seed and same
// grading as migrate_views_only_namespace_integration_test.go: the target
// view's rows against the source view's own rows.

package pipeline

import (
	"bytes"
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// TestMigrate_ViewsOnlyNamespace_PostgresPreviewAndDiff pins the preview
// and diff siblings: both used to refuse a views-only schema outright
// ("source schema has no tables"), so its views could be neither planned
// nor checked. Diff is graded before and after the migrate: missing first,
// then clean.
func TestMigrate_ViewsOnlyNamespace_PostgresPreviewAndDiff(t *testing.T) {
	pgSource, pgTarget, cleanup := startPostgres(t)
	defer cleanup()
	seedPGViewsOnlyFanout(t, pgSource)
	pgEng, _ := engines.Get("postgres")
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	var out bytes.Buffer
	prev := &Previewer{
		Source: pgEng, Target: pgEng,
		SourceDSN: pgSource + "&schema=a_rpt", TargetDSN: pgTarget,
		TargetSchema: "a_rpt", Format: "text", Out: &out,
	}
	if err := prev.Run(ctx); err != nil {
		t.Fatalf("Previewer.Run on a views-only schema: %v", err)
	}
	if !strings.Contains(out.String(), "VIEW") {
		t.Errorf("preview of a views-only schema renders no view:\n%s", out.String())
	}

	differ := func() *Differ {
		return &Differ{
			Source: pgEng, Target: pgEng,
			SourceDSN: pgSource + "&schema=a_rpt", TargetDSN: pgTarget,
			TargetSchema: "a_rpt", Format: "text", Out: &bytes.Buffer{},
		}
	}
	applyPGDDL(t, pgTarget, `CREATE SCHEMA a_rpt`)
	before, err := differ().Run(ctx)
	if err != nil {
		t.Fatalf("Differ.Run on a views-only schema: %v", err)
	}
	if len(before.ViewsMissing) != 1 {
		t.Errorf("diff before migrate: views missing = %v; want [v]", before.ViewsMissing)
	}

	for _, schema := range []string{"m_app", "a_rpt"} {
		mig := &Migrator{
			Source: pgEng, Target: pgEng,
			SourceDSN: pgSource + "&schema=" + schema, TargetDSN: pgTarget, TargetSchema: schema,
		}
		if err := mig.Run(ctx); err != nil {
			t.Fatalf("Migrator.Run schema=%s: %v", schema, err)
		}
	}
	after, err := differ().Run(ctx)
	if err != nil {
		t.Fatalf("Differ.Run after migrate: %v", err)
	}
	if len(after.ViewsMissing) != 0 {
		t.Errorf("diff after migrate: views missing = %v; want none", after.ViewsMissing)
	}
}

// TestMigrate_ViewsOnlyNamespace_PostgresBackupRestore pins the backup
// sibling: `backup full` of a views-only schema has no early return, and
// its manifest carries the whole source schema, views included; restore
// creates them. Graded on the target view's rows against the source's.
func TestMigrate_ViewsOnlyNamespace_PostgresBackupRestore(t *testing.T) {
	pgSource, pgTarget, cleanup := startPostgres(t)
	defer cleanup()
	seedPGViewsOnlyFanout(t, pgSource)
	pgEng, _ := engines.Get("postgres")
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	mig := &Migrator{
		Source: pgEng, Target: pgEng,
		SourceDSN: pgSource + "&schema=m_app", TargetDSN: pgTarget, TargetSchema: "m_app",
	}
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run m_app: %v", err)
	}
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := (&backup.Backup{Source: pgEng, SourceDSN: pgSource + "&schema=a_rpt", Store: store, SluiceVersion: "test"}).Run(ctx); err != nil {
		t.Fatalf("Backup.Run on a views-only schema: %v", err)
	}
	if err := (&backup.Restore{Target: pgEng, TargetDSN: pgTarget, TargetSchema: "a_rpt", Store: store}).Run(ctx); err != nil {
		t.Fatalf("Restore.Run of a views-only backup: %v", err)
	}

	src, err := sql.Open("pgx", pgSource)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = src.Close() }()
	dst, err := sql.Open("pgx", pgTarget)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dst.Close() }()
	assertViewsMatchSource(t, ctx, src, dst, []string{"a_rpt.v"})
}

// TestStreamer_ViewsOnlyNamespace_PostgresToPostgres_Fanout is the sync
// fan-out sibling: the per-namespace cold start skipped a views-only schema
// ("database has no tables; skipping copy"), and created a mixed schema's
// cross-schema view before the schema it reads existed.
func TestStreamer_ViewsOnlyNamespace_PostgresToPostgres_Fanout(t *testing.T) {
	pgSource, pgTarget, cleanup := startPostgresLogical(t)
	defer cleanup()
	seedPGViewsOnlyFanout(t, pgSource)
	pgEng, _ := engines.Get("postgres")

	streamCtx, streamCancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() {
		runErr <- (&Streamer{
			Source: pgEng, Target: pgEng, SourceDSN: pgSource, TargetDSN: pgTarget,
			StreamID:       "views-only-fanout",
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

	// The views are created after every namespace's copy; wait on the
	// last-sorting one, then grade all three against the source.
	if !waitForPGScalar(t, pgTarget, `SELECT COUNT(*) FROM z_rpt.v`, 3, 90*time.Second) {
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
	src, err := sql.Open("pgx", pgSource)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = src.Close() }()
	dst, err := sql.Open("pgx", pgTarget)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dst.Close() }()
	assertViewsMatchSource(t, ctx, src, dst, []string{"a_rpt.v", "b_mix.vm", "z_rpt.v"})
}

// TestStreamer_ViewsOnlyNamespace_PostgresSingle_RefusesViewsOnly pins the
// single-namespace sync sibling (operator decision, Bug 317 review): a
// stream with no tables has no change stream to run and opens no target
// writer, so it cannot create the views — and exiting 0 with them absent
// reads as success to automation. It refuses, non-zero, with
// SYNC-VIEWS-ONLY-NAMESPACE naming the view. --skip-views is judged first:
// with the views filtered out, the plain "nothing to stream" return holds.
func TestStreamer_ViewsOnlyNamespace_PostgresSingle_RefusesViewsOnly(t *testing.T) {
	pgSource, pgTarget, cleanup := startPostgresLogical(t)
	defer cleanup()
	seedPGViewsOnlyFanout(t, pgSource)
	pgEng, _ := engines.Get("postgres")

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	newStreamer := func(skipViews bool) *Streamer {
		return &Streamer{
			Source: pgEng, Target: pgEng,
			SourceDSN: pgSource + "&schema=a_rpt", TargetDSN: pgTarget, TargetSchema: "a_rpt",
			StreamID: "views-only-single", SkipViews: skipViews,
		}
	}
	err := newStreamer(false).Run(ctx)
	if err == nil {
		t.Fatal("single-namespace sync of a views-only schema exited 0 with its view absent; want SYNC-VIEWS-ONLY-NAMESPACE")
	}
	if !strings.Contains(err.Error(), "SYNC-VIEWS-ONLY-NAMESPACE") || !strings.Contains(err.Error(), "(v)") {
		t.Fatalf("refusal lacks the marker or the view name: %v", err)
	}
	if err := newStreamer(true).Run(ctx); err != nil {
		t.Fatalf("--skip-views must leave nothing to refuse over; got %v", err)
	}
	// The dry run refuses what the real run refuses.
	dry := newStreamer(false)
	dry.DryRun = true
	if err := dry.Run(ctx); err == nil || !strings.Contains(err.Error(), "SYNC-VIEWS-ONLY-NAMESPACE") {
		t.Fatalf("sync --dry-run of a views-only schema: want SYNC-VIEWS-ONLY-NAMESPACE; got %v", err)
	}
}
