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
	"testing"
	"time"
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
		// setup runs after CREATE SEQUENCE … START 5.
		setup string
		// agrees is false for the ONE state where pg_sequences cannot
		// represent the truth. Spelled per-case rather than derived, so a
		// change in either direction is a test failure and not a silent
		// re-grading.
		agrees bool
		why    string
	}{
		{
			name:   "fresh, never called",
			agrees: true,
			why: "pg_sequences.last_value is NULL and PostgreSQL's own definition of NULL here is " +
				"'not read from yet', which is precisely (start_value, is_called=false)",
		},
		{
			name:   "advanced by nextval",
			setup:  `SELECT nextval('s_probe'), nextval('s_probe')`,
			agrees: true,
			why:    "a called sequence writes last_value to disk, so the view carries the real number",
		},
		{
			name:   "setval with is_called TRUE",
			setup:  `SELECT setval('s_probe', 9, true)`,
			agrees: true,
			why:    "the common priming shape, and the one the fallback was built against",
		},
		{
			name:   "setval with is_called FALSE at a position above start",
			setup:  `SELECT setval('s_probe', 7, false)`,
			agrees: false,
			why: "THE HOLE. pg_sequences.last_value stays NULL for any not-called sequence regardless of " +
				"where it was positioned, so the view cannot distinguish 'never used' from 'positioned at 7 " +
				"and not yet issued'. sluice reaches this state itself: setvalSequence writes is_called=false " +
				"whenever the source sequence was in that state",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := db.ExecContext(ctx, `DROP SEQUENCE IF EXISTS s_probe`); err != nil {
				t.Fatalf("drop: %v", err)
			}
			if _, err := db.ExecContext(ctx, `CREATE SEQUENCE s_probe START 5`); err != nil {
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
			if !agrees && gotLV > trueLV {
				t.Errorf("the catalog fallback reported a position AHEAD of the truth (%d > %d); the "+
					"forward-only re-prime would then believe the target is further along than it is and "+
					"skip a prime the target needs", gotLV, trueLV)
			}
			if !agrees && gotCalled && !trueCalled {
				t.Errorf("the catalog fallback reported is_called=true for a not-called sequence; the next "+
					"value the target issues is %d, and a caller told otherwise will place a row on it", trueLV)
			}
		})
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
