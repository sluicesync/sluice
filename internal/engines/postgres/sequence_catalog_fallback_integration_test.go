//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The premise under readSequencePositionFromCatalog, held to a real server.
//
// That function exists because a PlanetScale Neki router refuses to read a
// sequence AS A RELATION (NK013), so the Neki fallback answers the same
// question through `pg_catalog.pg_sequences`. Its correctness rests entirely
// on a claim about POSTGRESQL — not about Neki — namely what `pg_sequences`
// reports for a sequence in each of its three reachable states. That is an
// environmental premise in the sense CLAUDE.md's premise-naming rule means,
// and until this file existed it was asserted in a comment and checked by
// nothing.
//
// Neki is not needed to check it: the view is PostgreSQL's, the mapping is
// PostgreSQL's, and Neki's only role is making the relation read unavailable.
// So this rides the ordinary postgres integration shard for free and, via the
// version matrix, across every server major we sweep — which matters, because
// a premise about a catalog view is exactly the kind of thing a major can
// change under us.
//
// WHAT IT FOUND, which is why it is worth reading before trusting the
// fallback (measured here and independently on real PostgreSQL 18.6):
// the mapping is NOT lossless. `setval(s, n, false)` leaves
// pg_sequences.last_value NULL, so the fallback reports (start_value, false)
// for a sequence whose true position is (n, false). The error is always
// BACKWARD — the fallback can under-report a position, never over-report it —
// which is why the test below asserts that direction explicitly rather than
// just asserting the disagreement. See docs/dev/audit-backlog.md for the
// reachability analysis and the proposed fix.

package postgres

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/sluicecode"
)

// The three states a sequence can be in, and what each read reports.
//
// The relation read is the GROUND TRUTH here — it is what vanilla PostgreSQL
// uses and what the fallback is standing in for — so every cell compares the
// fallback against it rather than against a hardcoded expectation. That is
// the "name the independent expected value" rule: if a future PostgreSQL
// changed both reads in the same way, this test should follow, not fail.
func TestSequenceCatalogFallbackMatchesTheRelationRead(t *testing.T) {
	dsn, cleanup := newSharedPGDB(t, "seq_catalog_fallback")
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	cases := []struct {
		name string
		// create overrides the default `CREATE SEQUENCE s_probe START 5`,
		// which is what the descending cells need.
		create string
		// increment is the sequence's direction, fed to the
		// increment-aware direction assertion below.
		increment int64
		// setup runs after the create.
		setup string
		// agrees is false for the ONE state where pg_sequences cannot
		// represent the truth. Spelled per-case rather than derived, so a
		// change in either direction is a test failure and not a silent
		// re-grading.
		agrees bool
		why    string
	}{
		{
			name:      "fresh, never called",
			increment: 1,
			agrees:    true,
			why: "pg_sequences.last_value is NULL and PostgreSQL's own definition of NULL here is " +
				"'not read from yet', which is precisely (start_value, is_called=false)",
		},
		{
			name:      "advanced by nextval",
			increment: 1,
			setup:     `SELECT nextval('s_probe'), nextval('s_probe')`,
			agrees:    true,
			why:       "a called sequence writes last_value to disk, so the view carries the real number",
		},
		{
			name:      "setval with is_called TRUE",
			increment: 1,
			setup:     `SELECT setval('s_probe', 9, true)`,
			agrees:    true,
			why:       "the common priming shape, and the one the fallback was built against",
		},
		{
			name:      "setval with is_called FALSE at a position above start",
			increment: 1,
			setup:     `SELECT setval('s_probe', 7, false)`,
			agrees:    false,
			why: "THE HOLE. pg_sequences.last_value stays NULL for any not-called sequence regardless of " +
				"where it was positioned, so the view cannot distinguish 'never used' from 'positioned at 7 " +
				"and not yet issued'. sluice reaches this state itself: setvalSequence writes is_called=false " +
				"whenever the source sequence was in that state",
		},
		{
			// The same state by a FAR commoner route, and the reason the hole
			// above is not exotic. The pre-tag review named it: an operator
			// resetting a sequence types RESTART WITH, not setval(…, false).
			name:      "ALTER SEQUENCE … RESTART WITH, the routine route into the same state",
			increment: 1,
			setup:     `ALTER SEQUENCE s_probe RESTART WITH 7`,
			agrees:    false,
			why: "RESTART WITH leaves (7, is_called=false) exactly as setval(7,false) does, so the view " +
				"reports NULL and the fallback answers with start_value — this cell exists to document " +
				"that the hole's precondition is an ordinary DBA action",
		},
		{
			name:      "DESCENDING sequence, fresh",
			create:    `CREATE SEQUENCE s_probe START -5 INCREMENT -1 MINVALUE -100 MAXVALUE -1`,
			increment: -1,
			agrees:    true,
			why:       "direction does not change what the view reports for a never-called sequence",
		},
		{
			name:      "DESCENDING sequence, advanced by nextval",
			create:    `CREATE SEQUENCE s_probe START -5 INCREMENT -1 MINVALUE -100 MAXVALUE -1`,
			increment: -1,
			setup:     `SELECT nextval('s_probe'), nextval('s_probe')`,
			agrees:    true,
			why:       "a called descending sequence writes its real (negative) last_value to disk",
		},
		{
			// The cell that makes the direction assertion above meaningful: a
			// naive `gotLV > trueLV` check reads BACKWARD here, because the
			// fallback's start_value (-5) is numerically GREATER than the
			// truth (-7) while being behind it in the sequence's direction.
			name:      "DESCENDING sequence, positioned not-called — the direction trap",
			create:    `CREATE SEQUENCE s_probe START -5 INCREMENT -1 MINVALUE -100 MAXVALUE -1`,
			increment: -1,
			setup:     `SELECT setval('s_probe', -7, false)`,
			agrees:    false,
			why: "same hole, mirrored: the fallback answers (-5,false) for a sequence at (-7,false), which " +
				"is an UNDER-report in the sequence's own direction even though -5 > -7 numerically",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := db.ExecContext(ctx, `DROP SEQUENCE IF EXISTS s_probe`); err != nil {
				t.Fatalf("drop: %v", err)
			}
			create := tc.create
			if create == "" {
				create = `CREATE SEQUENCE s_probe START 5`
			}
			if _, err := db.ExecContext(ctx, create); err != nil {
				t.Fatalf("create: %v", err)
			}
			if tc.setup != "" {
				if _, err := db.ExecContext(ctx, tc.setup); err != nil {
					t.Fatalf("setup %q: %v", tc.setup, err)
				}
			}

			// Ground truth: the relation read, which is what every non-Neki
			// target uses and what the fallback stands in for.
			var trueLV int64
			var trueCalled bool
			if err := db.QueryRowContext(ctx, `SELECT last_value, is_called FROM s_probe`).
				Scan(&trueLV, &trueCalled); err != nil {
				t.Fatalf("relation read: %v", err)
			}

			gotLV, gotCalled, err := readSequencePositionFromCatalog(ctx, db, "public", "s_probe")
			if err != nil {
				t.Fatalf("catalog fallback: %v", err)
			}

			agrees := gotLV == trueLV && gotCalled == trueCalled
			if agrees != tc.agrees {
				if tc.agrees {
					t.Fatalf("the catalog fallback disagreed with the relation read where it is documented "+
						"to agree: fallback=(%d,%v) relation=(%d,%v) — %s",
						gotLV, gotCalled, trueLV, trueCalled, tc.why)
				}
				t.Fatalf("the catalog fallback now AGREES where it is documented to lose information: "+
					"fallback=(%d,%v) relation=(%d,%v). That is good news, but the doc comment on "+
					"readSequencePositionFromCatalog and the audit-backlog entry both describe the hole "+
					"as present — re-derive them before relaxing this cell. %s",
					gotLV, gotCalled, trueLV, trueCalled, tc.why)
			}

			// The DIRECTION of the disagreement is the load-bearing half. A
			// fallback that under-reports makes the forward-only re-prime run
			// when it did not need to (idempotent). One that OVER-reports
			// would make it skip a prime that was needed, which is the silent
			// outcome — so pin that it never happens.
			//
			// Asked through sequencePositionBehind rather than as `gotLV >
			// trueLV`, because "ahead" is INCREMENT-AWARE: on a descending
			// sequence the ahead position is the smaller number, and the
			// naive comparison inverts. The pre-tag review caught that the
			// first cut of this assertion was numerically wrong for the
			// descending family — which the table below now covers.
			if !agrees && !sequencePositionBehind(tc.increment, gotLV, gotCalled, trueLV, trueCalled) {
				t.Errorf("the catalog fallback reported a position AT OR AHEAD of the truth "+
					"(fallback %d/%v vs truth %d/%v, increment %d); the forward-only re-prime would then "+
					"believe the target is further along than it is and skip a prime it needs",
					gotLV, gotCalled, trueLV, trueCalled, tc.increment)
			}
			if !agrees && gotCalled && !trueCalled {
				t.Errorf("the catalog fallback reported is_called=true for a not-called sequence; the next "+
					"value the target issues is %d, and a caller told otherwise will place a row on it", trueLV)
			}
		})
	}
}

// The privilege arm, which is the one that made this a HIGH rather than the
// MEDIUM its first grading claimed.
//
// `pg_sequences.last_value` is privilege-gated in the view's own definition:
//
//	CASE WHEN has_sequence_privilege(c.oid, 'SELECT,USAGE')
//	     THEN pg_sequence_last_value(c.oid::regclass) ELSE NULL END
//
// so a role without SELECT/USAGE reads NULL for a sequence at ANY position.
// Before the fix, the fallback mapped that to `(start_value, false)` — wrong
// by however far the sequence had advanced, and with is_called flipped from
// true to FALSE, which is the opposite of the "only ever under-reports by a
// bounded amount" story the first grading told.
//
// Why it matters more than the not-called hole: the magnitude is unbounded,
// and one of this reader's consumers is the SOURCE capture, whose number is
// written into the IR and primed onto the target. A source sequence at 10⁶
// read as `(start, false)` produces a target that re-issues a million values
// the copied rows already hold — at exit 0.
//
// On vanilla PostgreSQL the relation read fails first with "permission
// denied", which is loud, so the ambiguity is unreachable there. This test
// calls the fallback DIRECTLY for that reason: it is asserting what the
// function does when it is the only reader, which on Neki it always is.
func TestSequenceCatalogFallbackRefusesWhatItCannotRead(t *testing.T) {
	dsn, cleanup := newSharedPGDB(t, "seq_catalog_fallback_priv")
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	for _, stmt := range []string{
		`DROP SEQUENCE IF EXISTS s_priv`,
		`CREATE SEQUENCE s_priv START 5`,
		`SELECT nextval('s_priv')`,
		`SELECT nextval('s_priv')`,
		`DROP ROLE IF EXISTS sluice_seq_lowpriv`,
		`CREATE ROLE sluice_seq_lowpriv`,
		`GRANT USAGE ON SCHEMA public TO sluice_seq_lowpriv`,
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("setup %q: %v", stmt, err)
		}
	}
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), `DROP ROLE IF EXISTS sluice_seq_lowpriv`)
	})

	// The truth, read as the owner: the sequence has been called twice.
	var trueLV int64
	var trueCalled bool
	if err := db.QueryRowContext(ctx, `SELECT last_value, is_called FROM s_priv`).
		Scan(&trueLV, &trueCalled); err != nil {
		t.Fatalf("relation read as owner: %v", err)
	}
	if !trueCalled || trueLV <= 5 {
		t.Fatalf("fixture did not advance the sequence: (%d,%v)", trueLV, trueCalled)
	}

	// A dedicated connection pinned to the unprivileged role. SET ROLE on the
	// shared pool would leak into other tests.
	lowDB, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open low-priv conn: %v", err)
	}
	defer lowDB.Close()
	lowDB.SetMaxOpenConns(1)
	if _, err := lowDB.ExecContext(ctx, `SET ROLE sluice_seq_lowpriv`); err != nil {
		t.Fatalf("set role: %v", err)
	}

	// Anti-vacuity: the role must genuinely lack the privilege, or this test
	// is checking the owner path under a different name.
	var readable bool
	if err := lowDB.QueryRowContext(ctx,
		`SELECT has_sequence_privilege('public.s_priv', 'SELECT,USAGE')`).Scan(&readable); err != nil {
		t.Fatalf("privilege probe: %v", err)
	}
	if readable {
		t.Fatal("the low-privilege role CAN read the sequence, so this test proves nothing about the " +
			"ambiguity it exists for")
	}

	gotLV, gotCalled, err := readSequencePositionFromCatalog(ctx, lowDB, "public", "s_priv")
	if err == nil {
		t.Fatalf("the fallback INVENTED a position (%d,%v) for a sequence it cannot read, whose true "+
			"position is (%d,%v). Every consumer acts on that number — the source capture writes it into "+
			"the IR and the target is primed from it — so this is silent duplication at exit 0",
			gotLV, gotCalled, trueLV, trueCalled)
	}
	coded, ok := sluicecode.FromError(err)
	if !ok || coded.Code != sluicecode.CodeSequencePositionUnreadable {
		t.Fatalf("refused, but not with the coded refusal an operator can act on: %v", err)
	}
	for _, want := range []string{"s_priv", "SELECT"} {
		if !strings.Contains(coded.Hint, want) && !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal names neither the sequence nor the grant: %v / hint %q", err, coded.Hint)
		}
	}
}

// The narrow half of the Neki adaptation that IS checkable without Neki: the
// classifier only diverts to the fallback for the specific refusal, so every
// other failure of the relation read still surfaces unchanged.
//
// This is the sibling of the mapping premise above. A classifier that widened
// — swallowing a permission error, say — would turn a loud failure into a
// silently substituted position, and the substituted position is the one the
// re-prime acts on.
func TestSequenceRelationReadErrorsAreNotSwallowedAsTheNekiRefusal(t *testing.T) {
	dsn, cleanup := newSharedPGDB(t, "seq_catalog_fallback_errs")
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	// A sequence that does not exist: real PostgreSQL answers 42P01, which is
	// NOT the Neki refusal, so readSequencePositionOn must return it rather
	// than quietly consulting pg_sequences (which would return sql.ErrNoRows
	// wrapped, a different and less informative failure).
	_, _, err = readSequencePositionOn(ctx, db, "public", "no_such_sequence")
	if err == nil {
		t.Fatal("reading a nonexistent sequence succeeded")
	}
	if isNekiRelationReadRefusal(err) {
		t.Errorf("a missing-relation error was classified as the Neki relation-read refusal: %v", err)
	}
}
