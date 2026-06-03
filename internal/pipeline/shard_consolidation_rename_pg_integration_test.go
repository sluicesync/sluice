//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0054 v0.78.0 task #22 — end-to-end RENAME COLUMN pin (PG → PG).
//
// Mirrors the Bug 83 PG end-to-end pattern (cold-start → CDC → in-
// flight DDL → assert target). Drives a live PG → PG streamer with
// live coordination engaged; the source issues an
// `ALTER TABLE ... RENAME COLUMN ... TO ...`; assertions verify the
// target schema reflects the rename, the data under the renamed
// column is preserved, and the lease row's applied state is
// recorded.
//
// "Validate end-to-end before building more" — task #22 closes one
// of the three sub-shapes ADR-0054's v1 catalog explicitly named as
// v1-deferred (the other two — CHECK constraint changes, generated-
// column changes — stay deferred). The pin is the real-engine wire-
// up the v1 catalog never had for RENAME.

package pipeline

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// TestStreamer_RenameColumn_PG_LiveCoordination drives a PG → PG
// streamer through cold-start, a live RENAME COLUMN on the source,
// and a follow-up INSERT under the new column name. Asserts the
// target schema landed the rename, the row data flowed under the
// renamed column, and the lease table recorded the applied state.
func TestStreamer_RenameColumn_PG_LiveCoordination(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
	defer cleanup()

	applyPGDDL(t, sourceDSN, `
		CREATE TABLE widgets (
			id INT PRIMARY KEY,
			name TEXT NOT NULL
		);
		ALTER TABLE widgets REPLICA IDENTITY FULL;
		INSERT INTO widgets (id, name) VALUES (1, 'alpha'), (2, 'beta');
	`)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	const streamID = "test-rename-pg"
	streamer := &Streamer{
		Source:    pgEng,
		Target:    pgEng,
		SourceDSN: sourceDSN,
		TargetDSN: targetDSN,
		StreamID:  streamID,
		InjectShardColumn: ShardColumnSpec{
			Name:  "source_shard_id",
			Value: "shard_a",
		},
		CoordinateLiveDDL: true,
		ShardCoordinationLease: LeaseConfig{
			LeaseDuration: 30 * time.Second,
			RenewDeadline: 20 * time.Second,
			RetryPeriod:   5 * time.Second,
		},
	}

	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()

	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	// Wait for bulk-copy.
	if !waitForPGRowCount(t, targetDSN, "widgets", 2, 30*time.Second) {
		t.Fatalf("phase A: bulk-copy never landed seed rows")
	}

	// Source RENAME + INSERT under the new column name.
	applyPGDDL(t, sourceDSN, `
		ALTER TABLE widgets RENAME COLUMN name TO product_name;
		INSERT INTO widgets (id, product_name) VALUES (3, 'gamma');
	`)

	// Wait for the post-DDL row to land. Without RENAME-shape
	// recognition the boundary would refuse loudly (combo
	// added=1+dropped=1 → Unrecognized), the apply would never
	// fire, and the gamma INSERT would crash the applier with
	// `column "product_name" does not exist`.
	if !waitForPGRowCount(t, targetDSN, "widgets", 3, 60*time.Second) {
		t.Fatalf("phase B: post-RENAME row never landed — RENAME shape may not be classified " +
			"as ShapeKindRenameColumn, or AlterRenameColumn failed to apply")
	}

	tgtDB, err := sql.Open("pgx", targetDSN)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = tgtDB.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Target schema reflects the rename: product_name present, name absent.
	var hasNew, hasOld int
	if err := tgtDB.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = 'widgets' AND column_name = 'product_name'`).Scan(&hasNew); err != nil {
		t.Fatalf("check product_name: %v", err)
	}
	if hasNew != 1 {
		t.Error("target widgets.product_name column missing — RENAME apply didn't fire")
	}
	if err := tgtDB.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = 'widgets' AND column_name = 'name'`).Scan(&hasOld); err != nil {
		t.Fatalf("check name: %v", err)
	}
	if hasOld != 0 {
		t.Error("target widgets.name column still present — RENAME left both columns")
	}

	// Original rows preserved under renamed column.
	var alpha, beta string
	if err := tgtDB.QueryRowContext(ctx, "SELECT product_name FROM widgets WHERE id = 1").Scan(&alpha); err != nil {
		t.Fatalf("read product_name @ id=1: %v", err)
	}
	if alpha != "alpha" {
		t.Errorf("widgets.product_name @ id=1 = %q, want alpha (data should survive rename)", alpha)
	}
	if err := tgtDB.QueryRowContext(ctx, "SELECT product_name FROM widgets WHERE id = 2").Scan(&beta); err != nil {
		t.Fatalf("read product_name @ id=2: %v", err)
	}
	if beta != "beta" {
		t.Errorf("widgets.product_name @ id=2 = %q, want beta", beta)
	}

	// Gamma row landed via the post-RENAME INSERT.
	var gamma string
	if err := tgtDB.QueryRowContext(ctx, "SELECT product_name FROM widgets WHERE id = 3").Scan(&gamma); err != nil {
		t.Fatalf("read product_name @ id=3: %v", err)
	}
	if gamma != "gamma" {
		t.Errorf("widgets.product_name @ id=3 = %q, want gamma", gamma)
	}

	// Lease row reflects applied state.
	const leaseQ = `SELECT applied_at IS NOT NULL, applied_schema_version
		FROM "public"."sluice_shard_consolidation_lease"
		WHERE target_table_full_name = $1`
	var (
		applied bool
		version int64
	)
	if err := tgtDB.QueryRowContext(ctx, leaseQ, "public.widgets").Scan(&applied, &version); err != nil {
		t.Fatalf("scan lease row: %v (a missing row means the RENAME boundary never routed)", err)
	}
	if !applied {
		t.Error("lease.applied_at should be set after the routed RENAME boundary")
	}
	if version < 1 {
		t.Errorf("lease.applied_schema_version = %d; want >= 1", version)
	}

	streamCancel()
	select {
	case <-runErr:
	case <-time.After(15 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}
}

// TestStreamer_RenameColumn_PG_LiveCoordination_TypeFamilyMatrix pins
// the Bug 86 fix (v0.78.1) and the Bug 74 "pin the class, not the
// representative" lesson: the v0.78.0 RENAME COLUMN pin used a single
// column type (TEXT NOT NULL) that did not exercise the
// SchemaReader-vs-pgoutput IR canonicalization asymmetry. Bug 86
// fired specifically when the table had a NULLABLE non-renamed column
// of types NUMERIC or TEXT — the cold-start SchemaReader populated
// Nullable=true, the pgoutput RelationMessage's projectRelation left
// Nullable=false (pgoutput cannot carry attnotnull), and the
// classifier's diffAlteredColumn surfaced a phantom
// ShapeKindAlterColumnNullability that combined with the RENAME's
// added=1/dropped=1 into the multi-shape combo refusal path.
//
// The matrix below covers the column-type families a real PG schema
// is likely to carry — every variant carries an EXTRA nullable
// non-renamed column whose type exercises a different family:
//
//   - VARCHAR(N): the original-pin coverage (the column type the
//     v0.78.0 test happened to use; ground-truth control for "type
//     families that round-trip stably").
//   - TEXT: Bug 86 catalogued case A3 — fixed by Nullable
//     normalization.
//   - NUMERIC(P,S): Bug 86 catalogued case A1 — fixed by Nullable
//     normalization.
//   - INTEGER: integer-family sanity coverage.
//   - TIMESTAMP: temporal-family coverage.
//   - BOOLEAN: leaf-type coverage.
//
// Each variant carries the same rename target (`name -> product_name`)
// with `name VARCHAR(64) NOT NULL`; the extra column is what varies.
// The extra column is left NULLABLE deliberately — that's what
// triggered Bug 86 (a NOT NULL extra column would have matched on
// both sides and masked the asymmetry, which is exactly what
// happened with the v0.78.0 release-pin's `name TEXT NOT NULL`
// fixture).
//
// See `BUG-CATALOG.md` (sluice-testing) for the upstream Bug 86
// catalogue.
func TestStreamer_RenameColumn_PG_LiveCoordination_TypeFamilyMatrix(t *testing.T) {
	cases := []struct {
		name      string
		extraCol  string // column name of the extra (non-renamed) column
		extraDecl string // its declared DDL — e.g. "NUMERIC(10,2)"
	}{
		// Bug 86 catalogued failures (pre-fix these refused; post-fix pass).
		{name: "extra_numeric_nullable", extraCol: "price", extraDecl: "NUMERIC(10,2)"},
		{name: "extra_text_nullable", extraCol: "description", extraDecl: "TEXT"},
		// Round-trip-stable type families (passed pre-fix; sanity coverage).
		{name: "extra_varchar_nullable", extraCol: "tagline", extraDecl: "VARCHAR(64)"},
		{name: "extra_integer_nullable", extraCol: "count", extraDecl: "INTEGER"},
		{name: "extra_timestamp_nullable", extraCol: "ts", extraDecl: "TIMESTAMP"},
		{name: "extra_boolean_nullable", extraCol: "flag", extraDecl: "BOOLEAN"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
			defer cleanup()

			applyPGDDL(t, sourceDSN, `
				CREATE TABLE widgets (
					id INT PRIMARY KEY,
					name VARCHAR(64) NOT NULL,
					`+tc.extraCol+` `+tc.extraDecl+`
				);
				ALTER TABLE widgets REPLICA IDENTITY FULL;
				INSERT INTO widgets (id, name) VALUES (1, 'alpha'), (2, 'beta');
			`)

			pgEng, ok := engines.Get("postgres")
			if !ok {
				t.Fatal("postgres engine not registered")
			}

			streamer := &Streamer{
				Source:    pgEng,
				Target:    pgEng,
				SourceDSN: sourceDSN,
				TargetDSN: targetDSN,
				StreamID:  "test-rename-pg-" + tc.name,
				InjectShardColumn: ShardColumnSpec{
					Name:  "source_shard_id",
					Value: "shard_a",
				},
				CoordinateLiveDDL: true,
				ShardCoordinationLease: LeaseConfig{
					LeaseDuration: 30 * time.Second,
					RenewDeadline: 20 * time.Second,
					RetryPeriod:   5 * time.Second,
				},
			}

			streamCtx, streamCancel := context.WithCancel(context.Background())
			defer streamCancel()

			runErr := make(chan error, 1)
			go func() { runErr <- streamer.Run(streamCtx) }()

			if !waitForPGRowCount(t, targetDSN, "widgets", 2, 30*time.Second) {
				t.Fatalf("phase A: bulk-copy never landed seed rows")
			}

			// Source RENAME + INSERT under the new column name. The
			// pre-Bug-86 failure shape: this RENAME would refuse with
			// `altered-col=true` because the unchanged extra column's
			// Nullable=true (cold-start) ≠ Nullable=false (pgoutput
			// projection) triggered a phantom ShapeKindAlterColumn-
			// Nullability that combined with added=1/dropped=1 into
			// the multi-shape combo refusal.
			applyPGDDL(t, sourceDSN, `
				ALTER TABLE widgets RENAME COLUMN name TO product_name;
				INSERT INTO widgets (id, product_name) VALUES (3, 'gamma');
			`)

			if !waitForPGRowCount(t, targetDSN, "widgets", 3, 60*time.Second) {
				t.Fatalf("phase B (%s): post-RENAME row never landed — Bug 86 fix may have regressed "+
					"(classifier likely surfaced phantom altered-col=true on the extra "+
					"nullable %s column)", tc.name, tc.extraDecl)
			}

			tgtDB, err := sql.Open("pgx", targetDSN)
			if err != nil {
				t.Fatalf("open target: %v", err)
			}
			defer func() { _ = tgtDB.Close() }()

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			var hasNew, hasOld int
			if err := tgtDB.QueryRowContext(ctx, `
				SELECT COUNT(*) FROM information_schema.columns
				WHERE table_schema = 'public' AND table_name = 'widgets' AND column_name = 'product_name'`).Scan(&hasNew); err != nil {
				t.Fatalf("check product_name: %v", err)
			}
			if hasNew != 1 {
				t.Errorf("target widgets.product_name column missing — RENAME apply didn't fire")
			}
			if err := tgtDB.QueryRowContext(ctx, `
				SELECT COUNT(*) FROM information_schema.columns
				WHERE table_schema = 'public' AND table_name = 'widgets' AND column_name = 'name'`).Scan(&hasOld); err != nil {
				t.Fatalf("check name: %v", err)
			}
			if hasOld != 0 {
				t.Errorf("target widgets.name column still present — RENAME left both columns")
			}
			streamCancel()
			select {
			case <-runErr:
			case <-time.After(15 * time.Second):
				t.Fatal("Streamer.Run did not return after ctx cancel")
			}
		})
	}
}
