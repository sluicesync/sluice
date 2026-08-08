//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The Postgres generated-row-identity class against a real server: the
// PREMISE the refusal rests on, the HARM it prevents, and the refusal
// itself across the replica-identity family.
//
// The harm proof and the refusal cannot ride the same path — once the
// reader refuses, no sluice-produced DELETE exists to over-delete with.
// So [TestGeneratedPrimaryKey_UnpublishedIdentityOverDeletes] takes its
// evidence entirely from the SERVERS: PostgreSQL's own catalog says
// which columns the publication carries, and the TARGET's own COUNT(*)
// says how many rows a WHERE over that subset removes. Neither number
// comes from sluice, which is the property the 2026-08-01 rule asks for.

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// genPKDDL is the over-delete shape: PRIMARY KEY (a, g) with g STORED
// GENERATED from c, and `a` deliberately NOT unique on its own — so
// losing g from the identity leaves a prefix that matches two rows.
//
// (A generated column derived from a PK member — g AS (a*2) — would be
// functionally dependent on it and could not show the over-delete at
// all. That near-miss is why the expression keys off c.)
const genPKDDL = `
	CREATE TABLE od (
		a INT  NOT NULL,
		c INT  NOT NULL,
		b TEXT,
		g INT GENERATED ALWAYS AS (c * 10) STORED,
		PRIMARY KEY (a, g)
	);
`

const genPKSeed = `INSERT INTO od(a, c, b) VALUES (1,1,'one'), (1,2,'two'), (2,3,'three');`

// TestGeneratedPrimaryKey_UnpublishedIdentityOverDeletes is the defect
// proof, and it binds the two halves that were each plausible alone:
// that PostgreSQL does not publish a generated PRIMARY KEY column, and
// that a WHERE built from what IS published removes more rows than the
// source did.
//
// Both are read off servers. The published column set comes from
// pg_publication_tables.attnames — PostgreSQL's own account of what the
// wire will carry — and the row counts come from the target database
// after the statement runs. sluice's reader is deliberately absent: it
// refuses this shape now, and a proof that depended on it would have to
// be deleted by its own fix.
func TestGeneratedPrimaryKey_UnpublishedIdentityOverDeletes(t *testing.T) {
	srcDSN, srcCleanup := newSharedPGDB(t, "source_db")
	defer srcCleanup()
	tgtDSN, tgtCleanup := newSharedPGDB(t, "target_db")
	defer tgtCleanup()

	applyPGSQL(t, srcDSN, genPKDDL)
	applyPGSQL(t, srcDSN, genPKSeed)
	applyPGSQL(t, srcDSN, `CREATE PUBLICATION gen_pk_probe FOR TABLE od;`)
	applyPGApplier(t, tgtDSN, genPKDDL)
	applyPGApplier(t, tgtDSN, genPKSeed)

	// Half 1 — the premise, from PostgreSQL's own catalog: the generated
	// PRIMARY KEY column is not published, and the table's replica
	// identity is the default (so nothing exotic is in play).
	published := pgQueryStrings(t, srcDSN, `
		SELECT unnest(attnames) FROM pg_publication_tables
		WHERE pubname = 'gen_pk_probe' AND tablename = 'od' ORDER BY 1`)
	if slices.Contains(published, "g") {
		t.Fatalf(
			"PREMISE BROKEN: this server publishes the generated column (attnames = %v). The refusal in "+
				"cdc_generated_pk.go is scoped to publications that do NOT — re-derive it.", published,
		)
	}
	identity := pgQueryString(t, srcDSN, `SELECT relreplident FROM pg_class WHERE relname = 'od'`)
	if identity != "d" {
		t.Fatalf("probe table's relreplident = %q; want the server default 'd'", identity)
	}
	t.Logf("PROVEN (premise): PRIMARY KEY is (a, g); publication carries %v — g is absent", published)

	// Half 2 — the harm, from the target's own row counts. The statement
	// is the WHERE an applier can build from `published`: every PK column
	// the publication actually carries, and no more.
	before := pgCount(t, tgtDSN, `SELECT count(*) FROM od`)
	applyPGApplier(t, tgtDSN, `DELETE FROM od WHERE a = 1;`) // the published half of (a, g)
	after := pgCount(t, tgtDSN, `SELECT count(*) FROM od`)

	// The source, meanwhile, deleted exactly one row by its real key.
	srcBefore := pgCount(t, srcDSN, `SELECT count(*) FROM od`)
	applyPGSQL(t, srcDSN, `DELETE FROM od WHERE a = 1 AND c = 1;`)
	srcAfter := pgCount(t, srcDSN, `SELECT count(*) FROM od`)

	if srcBefore-srcAfter != 1 {
		t.Fatalf("the source deleted %d rows; the probe is wrong, not the code", srcBefore-srcAfter)
	}
	if before-after <= srcBefore-srcAfter {
		t.Fatalf(
			"the partial-key WHERE removed %d target rows for %d source rows — no over-delete, so this "+
				"probe no longer demonstrates the class", before-after, srcBefore-srcAfter,
		)
	}
	t.Logf(
		"PROVEN (harm): one source DELETE (%d -> %d rows) becomes %d target deletions (%d -> %d) when the "+
			"WHERE is built from the published columns alone",
		srcBefore, srcAfter, before-after, before, after,
	)
}

// TestCDCReader_RefusesGeneratedIdentity walks the refusal family: the
// two key shapes that fail differently (part of the key generated, the
// whole key generated) × the three replica identities that can carry an
// old tuple. FULL is in the matrix deliberately — it is the remedy the
// item-93 refusal names and it does NOT rescue this shape, because
// pgoutput omits a generated column under FULL too.
//
// The CONTROLS are the point of the test as much as the cells: a
// generated column outside the key, an ordinary key, and a PK-less FULL
// table must all stream untouched. A refusal that fires on those would
// be a bigger problem than the defect.
func TestCDCReader_RefusesGeneratedIdentity(t *testing.T) {
	cases := []struct {
		name        string
		ddl         string
		dml         string
		wantRefusal bool
		msgHas      []string
	}{
		{
			name: "DEFAULT, part of the key generated",
			ddl: `CREATE TABLE t (a INT NOT NULL, c INT NOT NULL, b TEXT,
			        g INT GENERATED ALWAYS AS (c*10) STORED, PRIMARY KEY (a, g));`,
			dml:         `INSERT INTO t(a,c,b) VALUES (1,1,'x'); DELETE FROM t WHERE a=1;`,
			wantRefusal: true,
			msgHas:      []string{`"g"`, "NON-UNIQUE prefix"},
		},
		{
			name: "DEFAULT, the whole key generated",
			ddl: `CREATE TABLE t (a INT NOT NULL, b TEXT,
			        g INT GENERATED ALWAYS AS (a*2) STORED PRIMARY KEY);`,
			dml:         `INSERT INTO t(a,b) VALUES (1,'x'); DELETE FROM t WHERE a=1;`,
			wantRefusal: true,
			msgHas:      []string{`"g"`, "no published column carries the row identity at all"},
		},
		{
			name: "USING INDEX over a partly generated index",
			ddl: `CREATE TABLE t (a INT NOT NULL, c INT NOT NULL, b TEXT,
			        g INT GENERATED ALWAYS AS (c*10) STORED, PRIMARY KEY (a, g));
			      CREATE UNIQUE INDEX t_ag ON t(a, g);
			      ALTER TABLE t REPLICA IDENTITY USING INDEX t_ag;`,
			dml:         `INSERT INTO t(a,c,b) VALUES (1,1,'x'); DELETE FROM t WHERE a=1;`,
			wantRefusal: true,
			msgHas:      []string{`"g"`, "NON-UNIQUE prefix"},
		},
		{
			// FULL is NOT a rescue. Pre-fix this failed too, with an
			// unactionable "identity column missing from old tuple" out of
			// filterBeforeToKeyCols; the point of the cell is that it now
			// fails with a message naming the cause and the remedy.
			name: "FULL, part of the key generated",
			ddl: `CREATE TABLE t (a INT NOT NULL, c INT NOT NULL, b TEXT,
			        g INT GENERATED ALWAYS AS (c*10) STORED, PRIMARY KEY (a, g));
			      ALTER TABLE t REPLICA IDENTITY FULL;`,
			dml:         `INSERT INTO t(a,c,b) VALUES (1,1,'x'); DELETE FROM t WHERE a=1;`,
			wantRefusal: true,
			msgHas:      []string{`"g"`, "does not publish generated columns", "NON-UNIQUE prefix"},
		},
		{
			// CONTROL: a generated column that is not part of the identity
			// is the ordinary computed-column table, and it must be
			// invisible to this check.
			name: "CONTROL: a generated column OUTSIDE the key",
			ddl: `CREATE TABLE t (a INT PRIMARY KEY, b TEXT,
			        g INT GENERATED ALWAYS AS (a*2) STORED);`,
			dml: `INSERT INTO t(a,b) VALUES (1,'x'); DELETE FROM t WHERE a=1;`,
		},
		{
			name: "CONTROL: an ordinary composite key",
			ddl:  `CREATE TABLE t (a INT NOT NULL, c INT NOT NULL, b TEXT, PRIMARY KEY (a, c));`,
			dml:  `INSERT INTO t(a,c,b) VALUES (1,1,'x'); DELETE FROM t WHERE a=1;`,
		},
		{
			// CONTROL, added because a mutation run found it missing: a
			// generated column indexed by an index that is NOT the replica
			// identity. Widening the catalog query's identity predicate
			// from "the effective identity index" to "any index" passes
			// every other cell in this matrix and fails only here — which
			// is the definition of a cell the matrix needed.
			name: "CONTROL: a generated column in a NON-identity UNIQUE index",
			ddl: `CREATE TABLE t (a INT PRIMARY KEY, b TEXT,
			        g INT GENERATED ALWAYS AS (a*2) STORED);
			      CREATE UNIQUE INDEX t_g ON t(g);`,
			dml: `INSERT INTO t(a,b) VALUES (1,'x'); DELETE FROM t WHERE a=1;`,
		},
		{
			// CONTROL: PK-less FULL is legitimate (the operator set FULL
			// knowing there is no key) and has no identity index at all,
			// so the check must find nothing to grade.
			name: `CONTROL: PK-less REPLICA IDENTITY FULL with a generated column`,
			ddl: `CREATE TABLE t (a INT NOT NULL, b TEXT,
			        g INT GENERATED ALWAYS AS (a*2) STORED);
			      ALTER TABLE t REPLICA IDENTITY FULL;`,
			dml: `INSERT INTO t(a,b) VALUES (1,'x'); DELETE FROM t WHERE a=1;`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dsn, cleanup := newSharedPGDB(t, "source_db")
			defer cleanup()
			applyPGSQL(t, dsn, tc.ddl)

			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()

			eng := Engine{}
			rdr, err := eng.OpenCDCReader(ctx, dsn)
			if err != nil {
				t.Fatalf("OpenCDCReader: %v", err)
			}
			defer func() {
				if c, ok := rdr.(interface{ Close() error }); ok {
					_ = c.Close()
				}
			}()
			changes, err := rdr.StreamChanges(ctx, ir.Position{})
			if err != nil {
				t.Fatalf("StreamChanges: %v", err)
			}
			time.Sleep(300 * time.Millisecond)
			applyPGSQL(t, dsn, tc.dml)

			got := drainChanges(t, ctx, changes, 2, 20*time.Second)
			cdcRdr, _ := rdr.(*CDCReader)
			var streamErr error
			if cdcRdr != nil {
				streamErr = cdcRdr.Err()
			}

			if !tc.wantRefusal {
				if streamErr != nil {
					t.Fatalf("CONTROL over-refused: %v", streamErr)
				}
				if len(got) != 2 {
					t.Fatalf("CONTROL got %d changes; want 2 (INSERT + DELETE)", len(got))
				}
				del, ok := got[1].(ir.Delete)
				if !ok {
					t.Fatalf("CONTROL change[1] = %T; want ir.Delete", got[1])
				}
				t.Logf("CONTROL streams normally: Delete.Before = %#v", del.Before)
				return
			}

			if streamErr == nil {
				t.Fatalf(
					"the stream did not refuse; it emitted %d changes — this is the silent cell "+
						"(over-delete, or a zero-match drop with the position advancing)", len(got),
				)
			}
			var coded *sluicecode.CodedError
			if !errors.As(streamErr, &coded) || coded.Code != sluicecode.CodeCDCGeneratedPrimaryKey {
				t.Fatalf("refusal is not %s: %v", sluicecode.CodeCDCGeneratedPrimaryKey, streamErr)
			}
			for _, want := range tc.msgHas {
				if !strings.Contains(streamErr.Error(), want) {
					t.Errorf("refusal does not mention %q:\n%s", want, streamErr.Error())
				}
			}
			t.Logf("PROVEN (refusal): %v", streamErr)
		})
	}
}

// TestPreflightReplicaIdentity_GeneratedIdentity is the cold-start /
// add-table door: the refusal has to land BEFORE `EnsurePublication`,
// because on PostgreSQL 18 scoping a publication over this shape makes
// the server reject the OPERATOR'S OWN UPDATE/DELETE ("Replica identity
// must not contain unpublished generated columns" — verified on 18.4).
// That is the item-93 harm, one shape over, and it is why this check
// lives in the preflight as well as in the reader.
//
// Runs against the shared PG 16, where the same catalog read gives the
// same verdict for the pre-18 (silent) reason.
func TestPreflightReplicaIdentity_GeneratedIdentity(t *testing.T) {
	dsn, cleanup := newSharedPGDB(t, "source_db")
	defer cleanup()

	applyPGSQL(t, dsn, `
		CREATE TABLE gen_pk    (a INT NOT NULL, c INT NOT NULL, g INT GENERATED ALWAYS AS (c*10) STORED, PRIMARY KEY (a, g));
		CREATE TABLE gen_notpk (a INT PRIMARY KEY, g INT GENERATED ALWAYS AS (a*2) STORED);
		CREATE TABLE plain     (a INT PRIMARY KEY, b TEXT);
	`)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	eng := Engine{}
	sr, err := eng.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer func() {
		if c, ok := sr.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	pf, ok := sr.(ir.ReplicaIdentityPreflighter)
	if !ok {
		t.Fatal("the Postgres schema reader no longer satisfies ir.ReplicaIdentityPreflighter")
	}

	// CONTROLS first: a generated column outside the key, and an
	// ordinary key, must both pass. Asserting these before the refusal
	// keeps a blanket "everything is refused" bug from reading as a pass.
	if err := pf.PreflightReplicaIdentity(ctx, []string{"gen_notpk", "plain"}); err != nil {
		t.Fatalf("CONTROL over-refused: %v", err)
	}

	err = pf.PreflightReplicaIdentity(ctx, []string{"gen_pk", "plain"})
	if err == nil {
		t.Fatal("the preflight accepted a table whose PRIMARY KEY includes an unpublished generated column")
	}
	var coded *sluicecode.CodedError
	if !errors.As(err, &coded) || coded.Code != sluicecode.CodeSourceReplicaIdentity {
		t.Fatalf("refusal is not %s: %v", sluicecode.CodeSourceReplicaIdentity, err)
	}
	msg := err.Error()
	for _, want := range []string{"gen_pk", "GENERATED", "REPLICA IDENTITY FULL is NOT a fix here"} {
		if !strings.Contains(msg, want) {
			t.Errorf("refusal does not mention %q:\n%s", want, msg)
		}
	}
	if strings.Contains(msg, "plain") {
		t.Errorf("the refusal names an in-scope table that is fine:\n%s", msg)
	}
	t.Logf("PROVEN (preflight): %v", err)
}

func pgCount(t *testing.T, dsn, q string) int {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	var n int
	if err := db.QueryRow(q).Scan(&n); err != nil {
		t.Fatalf("count (%s): %v", q, err)
	}
	return n
}

func pgQueryStrings(t *testing.T, dsn, q string) []string {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	rows, err := db.Query(q)
	if err != nil {
		t.Fatalf("query %q: %v", q, err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	return out
}
