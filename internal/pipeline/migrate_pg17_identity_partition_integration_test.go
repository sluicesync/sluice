//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The value-fidelity review's BLOCKING finding on GC-3's reader refusal:
// PostgreSQL 17+ allows an identity column on a partitioned table, and
// the catalog then reports every partition CHILD's column as identity
// (is_identity = YES, identity_generation = ALWAYS) while the only
// deptype='i' sequence hangs off the ROOT's column. The first cut of the
// "identity column with no backing sequence" refusal fired on every such
// child — inside ReadSchema, before the Bug-100 partition preflight whose
// recovery is `--exclude-table=<parent>` — so the operator got a
// remedy-less message on a working configuration.
//
// Pinned on a stock postgres:17 (the per-PR pre-baked image is 16, which
// refuses identity on a partitioned table): ReadSchema succeeds, the
// child's Identity equals the root's options, and the documented
// `--exclude-table=<parent>` path still migrates end to end.

package pipeline

import (
	"context"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

func TestMigrate_PG17IdentityPartitionChild_ResolvesThroughRootAndExcludeParentWorks(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgresImage(t, "postgres:17")
	defer cleanup()

	applyPGDDL(t, sourceDSN, `
		CREATE TABLE events (
			id BIGINT GENERATED ALWAYS AS IDENTITY (INCREMENT BY 5 START WITH 10),
			k  INT NOT NULL,
			PRIMARY KEY (id, k)
		) PARTITION BY RANGE (k);
		CREATE TABLE events_lo PARTITION OF events FOR VALUES FROM (0) TO (10);
		CREATE TABLE events_hi PARTITION OF events FOR VALUES FROM (10) TO (20);
		INSERT INTO events (k) VALUES (1), (2), (15);
	`)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// The reader half: no refusal, and the child resolves through the root.
	sr, err := pgEng.OpenSchemaReader(ctx, sourceDSN)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	schema, err := sr.ReadSchema(ctx)
	migcore.CloseIf(sr)
	if err != nil {
		t.Fatalf("ReadSchema refused a partitioned identity table: %v", err)
	}
	want := &ir.IdentityOptions{Always: true, Start: 10, Increment: 5, MinValue: 1, MaxValue: 1<<63 - 1, Cache: 1}
	for _, name := range []string{"events", "events_lo", "events_hi"} {
		tbl := findTable(schema, name)
		if tbl == nil {
			t.Fatalf("table %s not read; have %v", name, targetTableNames(schema))
		}
		var id *ir.Column
		for _, c := range tbl.Columns {
			if c.Name == "id" {
				id = c
			}
		}
		if id == nil || id.Identity == nil {
			t.Fatalf("%s.id: Identity nil (child identity must resolve through the partition root)", name)
		}
		if *id.Identity != *want {
			t.Errorf("%s.id Identity = %+v; want the root's %+v", name, *id.Identity, *want)
		}
	}

	// The documented recovery: exclude the partitioned parent, migrate
	// the children as heaps. Each child re-creates its identity with the
	// root's options and ALWAYS restored.
	filter, err := migcore.NewTableFilter(nil, []string{"events"})
	if err != nil {
		t.Fatalf("NewTableFilter: %v", err)
	}
	mig := &Migrator{Source: pgEng, Target: pgEng, SourceDSN: sourceDSN, TargetDSN: targetDSN, Filter: filter}
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run with --exclude-table=events: %v", err)
	}
	if got := pgIdentitySequenceOptions(t, targetDSN, "events_lo", "id"); got != "10,5,1,9223372036854775807,1,false" {
		t.Errorf("target events_lo identity options = %q; want the root's 10,5,1,9223372036854775807,1,false", got)
	}
	if got := pgInsertSQLState(t, targetDSN, `INSERT INTO events_lo (id, k) VALUES (999, 3)`); got != "428C9" {
		t.Errorf("explicit id into events_lo: SQLSTATE %q; want 428C9 (ALWAYS restored on the migrated child)", got)
	}
	if got := pgInsertSQLState(t, targetDSN, `INSERT INTO events_lo (k) VALUES (3)`); got != "" {
		t.Errorf("generated id into events_lo: SQLSTATE %q; want success", got)
	}
}
