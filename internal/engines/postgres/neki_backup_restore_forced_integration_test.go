//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// The free half of the backup/restore Neki coverage: every part of the two
// Tier-2 arms that is NOT about the router, run on every PR against an
// ordinary PostgreSQL container with [forceNekiFlavor] on.
//
// # Why this exists rather than "the live arm will tell us"
//
// The nekiverify suite's own history is the argument. Six MoveTables
// dispatches each failed at a different point, and every one of those
// failures was a HARNESS question — a signature, a return shape, a
// positional assumption — answered at the cost of a provisioned cluster and
// several minutes of wall clock. A paid run should be spent on questions only
// a router can answer.
//
// So the plumbing is proven here first: the SQLite source builds, the backup
// writes chunks, the restore applies a schema and copies rows and builds an
// index through the Neki-adapted engine code, the content digests line up, the
// backup-from-Postgres direction writes a manifest whose recorded row count
// matches an independent count, `backup verify --depth read` passes on the
// artifact, and the artifact's own chunk files read back byte-exact — with a
// standalone sequence deliberately standing on the source, which is the exact
// shape that made the live arm's read-back refuse on 2026-09-15.
// When the live arm then fails, the diff between "it worked here an hour ago"
// and "it failed there" is a platform finding rather than a coin flip.
//
// # What this CANNOT prove, stated because the name would otherwise mislead
//
// The server here is vanilla PostgreSQL. It does not route, it has no shards,
// it has no `__neki` schema, and it refuses nothing this suite cares about. A
// green run says the Neki-adapted code paths do not break on a server that
// does not need them, and says NOTHING about whether a router admits four
// concurrent COPYs, places a control table, or refuses a shard-key-less write.
// That is Tier 2's job, and the name of this test is deliberately
// `TestPostgresSuite_…` rather than `TestNekiverify_…` so nobody reads a green
// run as evidence about Neki.

func TestPostgresSuite_NekiBackupRestorePlumbing(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()

	forceNekiFlavor(t, dsn)

	// Generous: 20k rows through a SQLite write, a chunked backup, a
	// parallel restore and two full read-backs.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	t.Run("the flavor is actually forced — anti-vacuity for everything below", func(t *testing.T) {
		// Without this, a serverKey change or a memo rename turns the whole
		// test into an ordinary PostgreSQL round trip that passes for the
		// wrong reason, and the live arms would be the first thing to
		// notice. Same guard, same reason, as TestPostgresSuite_NekiFlavorForced.
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
			t.Fatal("flavor forcing did not take: probeIsNeki still reports false, so the backup and restore " +
				"below are exercising the ordinary PostgreSQL paths and prove nothing about the Neki-adapted ones")
		}
	})

	// The tenants are arbitrary here — nothing routes — but they must still
	// differ, because the generated corpus alternates between them and a
	// single value would collapse the composite primary key's first column
	// to a constant.
	spec := defaultNekiBackupRestoreSpec(11, 22)
	// The one leg a forced-flavor server cannot reach. A Neki target does not
	// run CREATE INDEX — it submits an online-DDL workflow through the
	// `__neki` schema — so with the index on, the restore below dies at
	// `create indexes` with SQLSTATE 3F000, `schema "__neki" does not exist`.
	// Measured 2026-09-14; see nekiBackupRestoreSpec.secondaryIndex. The
	// index-build leg of a Neki restore is therefore NOT COVERED per-PR, and
	// is the phase to read most carefully when the live arm reports.
	spec.secondaryIndex = false

	t.Run("restore core: a full backup restores into a Neki-flavored target byte-exact", func(t *testing.T) {
		res := nekiRestoreCoreIntoTarget(ctx, t, dsn, spec)

		// The copy ceiling reaches the resolver on any target the engine
		// believes is Neki, so the bound is checkable here even though the
		// server admits far more. This is the free half of the live arm's
		// (b) assertion — what is NOT free is whether the platform still
		// admits at least that many, which only a router can say.
		switch {
		case res.tableParallelism == 0:
			t.Errorf("restore's cross-table dispatch observer never fired, so neither this test nor the live arm "+
				"can report what concurrency a restore resolves against a Neki target. The seam is "+
				"backup.RestoreDispatchObserver; if it moved, nekiRestoreCoreIntoTarget needs rewiring. "+
				"(reason reported: %q)", res.dispatchReason)
		case res.tableParallelism > nekiConcurrentCopyLimit:
			t.Errorf("restore resolved table-parallelism %d with the Neki flavor forced; nekiConcurrentCopyLimit "+
				"is %d. CopyConcurrencyCeiling is not reaching restore's axis resolver — which is exactly the "+
				"defect restore_table_pool.go's comment records, and it is visible WITHOUT a cluster.",
				res.tableParallelism, nekiConcurrentCopyLimit)
		default:
			t.Logf("restore resolved table-parallelism %d (ceiling %d, reason %q)",
				res.tableParallelism, nekiConcurrentCopyLimit, res.dispatchReason)
		}
	})

	t.Run("backup core: a backup of those tables reads back byte-exact", func(t *testing.T) {
		// Runs second, on the tables the restore above created, mirroring
		// the live arms' ordering exactly.

		// A STANDALONE SEQUENCE on the source, and it is the point of this
		// leg rather than scenery.
		//
		// The live backup arm failed on run 34928571469 (2026-09-15) not on
		// the router but on its read-back, because an earlier arm had left
		// `nv_tx_seq` on the shared fixture: a standalone sequence rides a
		// backup no matter which TABLES the filter names, and the read-back
		// target refused it. That was a HARNESS question answered at the cost
		// of a provisioned cluster, which is exactly what this file exists to
		// prevent. So the per-PR run now reproduces the shape for free —
		// unowned (no OWNED BY), so migcore's owned-sequence pruning cannot
		// filter it out, and primed, so the manifest carries real options.
		const residueSeq = "nk_plumbing_standalone_seq"
		func() {
			db, err := sql.Open("pgx", dsn)
			if err != nil {
				t.Fatalf("open to create the standalone sequence: %v", err)
			}
			defer func() { _ = db.Close() }()
			if _, err := db.ExecContext(ctx, `CREATE SEQUENCE IF NOT EXISTS `+residueSeq+` START 5`); err != nil {
				t.Fatalf("create the standalone sequence this leg reproduces the live failure with: %v", err)
			}
			if _, err := db.ExecContext(ctx, `SELECT setval('`+residueSeq+`', 7, true)`); err != nil {
				t.Fatalf("prime the standalone sequence: %v", err)
			}
		}()

		res := nekiBackupCoreFromPG(ctx, t, dsn, []string{spec.table, spec.tableSmall}, 4_000)

		// ANTI-VACUITY for the paragraph above. If the backup stopped
		// capturing standalone sequences — a filter change, a reader change —
		// this leg would go on passing while no longer reproducing anything,
		// and the next live run would pay for the discovery again.
		carried := false
		for _, seq := range res.manifest.Schema.Sequences {
			if seq != nil && seq.Name == residueSeq {
				carried = true
				break
			}
		}
		if !carried {
			t.Errorf("the backup did NOT capture the standalone sequence %q (manifest carries %d sequence(s)), "+
				"so this leg no longer reproduces the shape that failed live on 2026-09-15 and the read-back "+
				"below proves nothing about it. Either the schema reader stopped reporting standalone "+
				"sequences or the table filter started pruning unowned ones — both are worth knowing.",
				residueSeq, len(res.manifest.Schema.Sequences))
		}

		// The raw-copy decline is what makes a Neki backup take the IR copy
		// path rather than `COPY (SELECT …) TO`, which the router refuses
		// with NK013. The live arm asserts it against a real router; asserting
		// it here too is what makes the live assertion's failure attributable
		// — if both go red, the decline broke in sluice, not on the platform.
		if rr, err := (Engine{}).OpenRowReader(ctx, dsn); err == nil {
			defer func() {
				if c, ok := rr.(interface{ Close() error }); ok {
					_ = c.Close()
				}
			}()
			decliner, ok := rr.(ir.RawCopyDecliner)
			if !ok {
				t.Fatal("the Postgres RowReader no longer exposes ir.RawCopyDecliner — the backup path's " +
					"Neki decline cannot be reached at all")
			}
			if declined, reason := decliner.DeclinesRawCopy(); !declined {
				t.Errorf("raw copy was NOT declined with the Neki flavor forced (reason %q); a real Neki backup "+
					"would take a lane the router refuses with NK013", reason)
			}
		} else {
			t.Fatalf("open row reader: %v", err)
		}

		for _, tbl := range []string{spec.table, spec.tableSmall} {
			t.Logf("backup core: %s — source %d rows digested %s, read back %d rows digested %s",
				tbl, res.sourceRows[tbl], res.sourceDigest[tbl], res.readbackRows[tbl], res.readbackHash[tbl])
		}
	})
}
