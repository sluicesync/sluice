//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
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

	// The stall diagnostic, exercised rather than merely compiled.
	//
	// WHAT THIS LEG REACHES, stated because the name could be read as broader:
	// the `pg_catalog.pg_stat_activity` FALLBACK only. A vanilla server has no
	// `__neki` schema, so the router query fails and the fallback answers —
	// which is precisely the half a live run never exercises. The router half
	// is reached only by a provisioned cluster, and is ungated here.
	//
	// It exists because the diagnostic it guards is only ever read during a
	// stall on a cluster that is deleted minutes later. A census helper with a
	// typo in its projection would be discovered exactly once, at the worst
	// possible time, and would then have to be fixed and re-dispatched.
	t.Run("the stall census reads back on a server with no __neki schema", func(t *testing.T) {
		db, err := sql.Open("pgx", dsn)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		defer func() { _ = db.Close() }()

		backends, source, err := nekiReadBackends(ctx, db)
		if err != nil {
			t.Fatalf("the backend census could not be taken at all: %v", err)
		}
		if !strings.Contains(source, "pg_stat_activity") {
			t.Errorf("the census answered from %q — on a server with no __neki schema the fallback is the only "+
				"view that can answer, so this leg is no longer exercising it", source)
		}
		// Anti-vacuity: this connection is itself a backend, and the query
		// excludes only the CURRENT pid. An empty census means the projection
		// ran and returned nothing, which would make a stall snapshot a blank
		// line at the moment it matters most.
		if len(backends) == 0 {
			t.Error("the census returned ZERO backends on a live server this test is connected to. The " +
				"projection runs and says nothing, so a stall snapshot would print an empty table")
		}
		t.Logf("%s", nekiRenderBackends(backends, source))
	})

	// The stall WATCHDOG, mutation-proved in both directions.
	//
	// The live arm's threshold is 60 s and the stall it exists for was seven
	// minutes, so the branch that fires can never be reached by a healthy run
	// — which means without this leg the only evidence that it fires at all
	// would be the next stalled dispatch, and a diagnostic that turns out not
	// to work is discovered exactly when it is needed. Both directions are
	// asserted because a watchdog that always fires is as useless as one that
	// never does.
	//
	// It is also the regression gate for a defect this very watch shipped in
	// its first cut: it observed the backup by tee-ing `slog`'s default
	// handler, which deadlocks the process (see
	// [nekiStoreProgressWatch]'s doc). These legs run the watch for real; a
	// return to a seam that hangs fails here in under two seconds rather than
	// consuming a live cluster's entire budget.
	t.Run("the stall watchdog fires on quiet and stays quiet while the store grows", func(t *testing.T) {
		db, err := sql.Open("pgx", dsn)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		defer func() { _ = db.Close() }()

		t.Run("it FIRES, and takes a real census, when the store stops growing", func(t *testing.T) {
			root := t.TempDir()
			var mu sync.Mutex
			var reasons []string
			w := nekiWatchStoreProgress(t, root, 200*time.Millisecond, func(why string) {
				mu.Lock()
				reasons = append(reasons, why)
				mu.Unlock()
				nekiLogBackendCensus(t, db, why)
			})
			time.Sleep(900 * time.Millisecond)
			fired := w.snapshotTaken()
			w.stop()

			if !fired {
				t.Error("the watchdog did NOT fire after 900ms of a store that never grew, against a 200ms " +
					"threshold. The live arm's 60s branch is therefore unproven, and the next stalled " +
					"dispatch would produce the same evidence-free log the diagnostic was added to prevent")
			}
			mu.Lock()
			defer mu.Unlock()
			// ONCE, not once per tick: a stalled router sampled every interval
			// for seven minutes buries the evidence under copies of itself.
			if len(reasons) != 1 {
				t.Errorf("the stall sample fired %d time(s) (%v), want exactly 1", len(reasons), reasons)
			}
		})

		t.Run("it does NOT fire while the store keeps growing", func(t *testing.T) {
			root := t.TempDir()
			fired := false
			w := nekiWatchStoreProgress(t, root, 200*time.Millisecond, func(string) { fired = true })
			// Write for longer than the threshold. A watch that fired here
			// would be reporting a stall on a run that is plainly working.
			for i := range 9 {
				if err := os.WriteFile(filepath.Join(root, fmt.Sprintf("chunk-%d", i)),
					make([]byte, 128*(i+1)), 0o600); err != nil {
					t.Fatalf("write a growing chunk: %v", err)
				}
				time.Sleep(100 * time.Millisecond)
			}
			w.stop()

			if fired {
				t.Error("the watchdog reported a stall while the store was growing on every poll, so its " +
					"firing says nothing about whether anything actually stopped")
			}
			if trace := w.progressTrace(); len(trace) < 2 {
				t.Errorf("the progress trace recorded %d sample(s) (%v) across nine writes — it is not "+
					"observing the store at all, which would make the non-firing above vacuous", len(trace), trace)
			}
		})
	})
}
