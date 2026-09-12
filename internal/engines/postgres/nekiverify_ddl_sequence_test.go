//go:build nekiverify

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"strings"
	"testing"
)

// Tier-2 coverage item #4: DDL-in-transaction, and the sequence catalog
// fallback.
//
// # Why this shares the refusal suite's database rather than provisioning one
//
// Every test FUNCTION in this suite provisions and destroys its own Neki
// database, which is minutes of wall clock and a real (prorated) bill. None
// of the premises below need a SHARDED database — they are router behaviours
// that hold on one shard as well as two — so provisioning a second database
// to check them would buy nothing but cost. They run as subtests of
// [TestNekiverify_ShardedRefusalPremises] against its fixture instead, which
// is why this is a helper and not a `Test…` function.
//
// # What is actually being checked, and which sluice code depends on it
//
// Two premises, both measured by hand on 2026-09-10 and both load-bearing:
//
//  1. DDL issued inside an explicit transaction is not visible to later
//     statements in that transaction and does not survive the commit
//     (reported to PlanetScale). [SchemaWriter.createAndPrimeSequence] gives up
//     its transaction on a Neki target BECAUSE of this. If the platform fixed
//     it, the branch is merely unnecessary — safe, and worth knowing. If the
//     AUTOCOMMIT form ever broke instead, sluice would stop being able to
//     create a primed sequence at all, which this catches loudly.
//
//  2. Reading a sequence AS A RELATION is refused with NK013 "as a relation",
//     while `pg_catalog.pg_sequences` is served. [readSequencePositionOn]
//     diverts to [readSequencePositionFromCatalog] on exactly that error, and
//     `isNekiRelationReadRefusal` keys on the SQLSTATE *and* the message
//     substring — so a message rewording is a premise change this suite
//     should report, not a silent loss of the fallback.
//
// The dangerous direction for #2 is specific and worth naming: if the router
// started ACCEPTING the relation read but answering something different from
// what PostgreSQL would answer, sluice would take the non-fallback path and
// trust the number. So the check is not "does it still error" alone — where
// the read succeeds, its answer is compared against pg_sequences.
//
// The lossy corner of that mapping (a `setval(n, false)` sequence reads back
// as `start_value`) is PostgreSQL's, not Neki's, and is pinned separately and
// for free by TestSequenceCatalogFallbackMatchesTheRelationRead on the
// ordinary integration shard. This file does not re-derive it; it checks the
// part that needs a router.
func nekiDDLAndSequencePremises(ctx context.Context, t *testing.T, db *sql.DB) {
	t.Helper()

	t.Run("PREMISE: DDL inside an explicit transaction does not survive", func(t *testing.T) {
		// Drop first so a re-run against a fixture that somehow kept state
		// cannot pass by finding yesterday's sequence.
		if _, err := db.ExecContext(ctx, `DROP SEQUENCE IF EXISTS nv_tx_seq`); err != nil {
			t.Fatalf("pre-drop: %v", err)
		}

		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		_, createErr := tx.ExecContext(ctx, `CREATE SEQUENCE nv_tx_seq START 5`)
		var setvalErr error
		if createErr == nil {
			_, setvalErr = tx.ExecContext(ctx, `SELECT setval('nv_tx_seq', 7, true)`)
		}
		commitErr := tx.Commit()
		if commitErr != nil {
			_ = tx.Rollback()
		}

		// Whatever the statements reported, the load-bearing question is what
		// the database holds AFTERWARDS — that is the independent expected
		// value, and it is what createAndPrimeSequence's branch is about.
		var exists bool
		if err := db.QueryRowContext(ctx,
			`SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_sequences
			                WHERE schemaname = 'public' AND sequencename = 'nv_tx_seq')`).
			Scan(&exists); err != nil {
			t.Fatalf("post-commit existence check: %v", err)
		}

		if exists {
			t.Logf("PREMISE CHANGED (in the SAFE direction): a sequence created inside an explicit "+
				"transaction SURVIVED the commit. createAndPrimeSequence's Neki branch gives up atomicity "+
				"for a hazard that may no longer exist — re-derive it and the reported finding before relying on this. "+
				"(create err: %v; setval err: %v; commit err: %v)", createErr, setvalErr, commitErr)
			// Clean up so the autocommit arm below starts from nothing.
			if _, err := db.ExecContext(ctx, `DROP SEQUENCE IF EXISTS nv_tx_seq`); err != nil {
				t.Fatalf("cleanup: %v", err)
			}
			return
		}
		t.Logf("premise holds: transactional DDL left no sequence behind (create err: %v; setval err: %v; "+
			"commit err: %v)", createErr, setvalErr, commitErr)
	})

	t.Run("the autocommit form sluice actually uses still works", func(t *testing.T) {
		// The anti-vacuity arm for the case above, and the one that matters
		// more day to day: the subtest above passes if DDL is broken in
		// EVERY form, which would mean sluice cannot create a sequence at
		// all. This proves the supported path is alive.
		if _, err := db.ExecContext(ctx, `CREATE SEQUENCE nv_tx_seq START 5`); err != nil {
			t.Fatalf("autocommit CREATE SEQUENCE failed — sluice cannot create a primed sequence on this "+
				"target at all: %v", err)
		}
		if _, err := db.ExecContext(ctx, `SELECT setval('nv_tx_seq', 7, true)`); err != nil {
			t.Fatalf("autocommit setval failed: %v", err)
		}
		lv, called, err := readSequencePositionFromCatalog(ctx, db, "public", "nv_tx_seq")
		if err != nil {
			t.Fatalf("read back through pg_sequences: %v", err)
		}
		if lv != 7 || !called {
			t.Errorf("after setval(7,true) the catalog reports (%d,%v), want (7,true) — the prime sluice "+
				"writes is not the position the target holds", lv, called)
		}
	})

	t.Run("PREMISE: reading a sequence AS A RELATION is refused with NK013", func(t *testing.T) {
		// isNekiRelationReadRefusal keys on SQLSTATE NK013 *and* the phrase
		// "as a relation". Both halves are premises: a code change and a
		// wording change each disarm the fallback, and a disarmed fallback
		// surfaces as a hard error on a path that used to work.
		var lv int64
		var called bool
		err := db.QueryRowContext(ctx, `SELECT last_value, is_called FROM public.nv_tx_seq`).
			Scan(&lv, &called)

		if err == nil {
			// The read is now SERVED. That is safe only if the answer is
			// right, so check it against the view rather than assuming.
			cLV, cCalled, cErr := readSequencePositionFromCatalog(ctx, db, "public", "nv_tx_seq")
			if cErr != nil {
				t.Fatalf("relation read succeeded but pg_sequences failed: %v", cErr)
			}
			if lv != cLV || called != cCalled {
				t.Errorf("PREMISE CHANGED, AND UNSAFELY: the router now SERVES the relation read but "+
					"answers (%d,%v) where pg_sequences says (%d,%v). sluice takes the non-fallback path "+
					"when the read succeeds and would trust the first number",
					lv, called, cLV, cCalled)
				return
			}
			t.Logf("PREMISE CHANGED (in the SAFE direction): the relation read is now served and agrees "+
				"with pg_sequences at (%d,%v). readSequencePositionFromCatalog is now unreachable on this "+
				"platform — it is not wrong, just unexercised, which is worth knowing before its own "+
				"premises are trusted", lv, called)
			return
		}

		if !isNekiRelationReadRefusal(err) {
			// Deliberately loud rather than tolerant: this is the case where
			// the fallback stops being reachable and a working path starts
			// erroring for operators.
			t.Fatalf("the relation read failed, but NOT with the refusal sluice diverts on — so "+
				"readSequencePositionOn will propagate this instead of falling back, and the re-prime path "+
				"breaks. Got: %v (isNekiRelationReadRefusal=false; it requires SQLSTATE NK013 and the "+
				"phrase %q)", err, "as a relation")
		}
		t.Logf("premise holds: %v", err)

		// And the whole point of the divert: the wrapper must ANSWER, not
		// propagate. This is the only place the two halves are bound
		// together — pinning the classifier and the view separately would
		// leave the wiring between them unchecked.
		gotLV, gotCalled, err := readSequencePositionOn(ctx, db, "public", "nv_tx_seq")
		if err != nil {
			t.Fatalf("readSequencePositionOn did not fall back on the refusal it is written for: %v", err)
		}
		if gotLV != 7 || !gotCalled {
			t.Errorf("fallback answered (%d,%v), want (7,true)", gotLV, gotCalled)
		}
	})

	t.Run("anti-vacuity: an unrelated relation read is NOT refused", func(t *testing.T) {
		// If the router refused every read of anything, the case above would
		// pass for the wrong reason. A plain table read must work.
		var n int
		if err := db.QueryRowContext(ctx, `SELECT count(*) FROM sk_good`).Scan(&n); err != nil {
			t.Fatalf("an ordinary table read failed, so the NK013 above proves nothing about sequences "+
				"specifically: %v", err)
		}
	})

	t.Run("anti-vacuity: the refusal message still carries its classifying phrase", func(t *testing.T) {
		// isNekiRelationReadRefusal's second half is a SUBSTRING match, which
		// is the fragile kind of premise. Assert the substring directly so a
		// rewording is reported as a wording change rather than showing up
		// later as a mysterious hard failure on the re-prime path.
		//
		// Deliberately NOT t.Skip: this workflow's fail-on-skip belt turns
		// any skip into a job failure, on the principle that a live suite
		// which skipped did not look. A premise that no longer applies is
		// reported with a log line and a return.
		err := db.QueryRowContext(ctx, `SELECT last_value FROM public.nv_tx_seq`).Scan(new(int64))
		if err == nil {
			t.Log("the relation read is served on this platform, so there is no message to classify; " +
				"the subtest above has already graded that change")
			return
		}
		if !strings.Contains(err.Error(), "as a relation") {
			t.Errorf("the refusal no longer contains the phrase isNekiRelationReadRefusal matches on: %v", err)
		}
	})
}
