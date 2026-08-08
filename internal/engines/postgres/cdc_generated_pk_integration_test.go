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
//
// # The PostgreSQL 18 boundary, and why it is asserted rather than skipped
//
// cdc_generated_pk.go's safety argument cites a fact about the world:
// that PostgreSQL 18 closes the source-side half of this hole itself.
// That premise went unchecked and turned the whole file red on the
// PG-version matrix — every cell that DELETEs from the source hit
// SQLSTATE 42P10 on 18, 19beta1 and :latest, because on those servers
// PostgreSQL refuses the SOURCE APPLICATION'S OWN UPDATE/DELETE the
// moment a publication covers this shape.
//
// [TestPremise_PostgresRefusesSourceWritesToAGeneratedIdentity] is that
// premise-naming step: it measures the boundary in BOTH directions
// against whatever server the harness booted, so a future PostgreSQL
// that re-opened the hole fails the suite instead of quietly widening
// sluice's exposure. The tests below then consult the same measured
// constant rather than skipping, so the cells that CAN still run on 18+
// do run — in particular the over-refusal controls, which must keep
// controlling on every version.

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// pgVersionGeneratedIdentityWriteRefusal is the first server_version_num
// that refuses the SOURCE APPLICATION'S OWN UPDATE and DELETE on a table
// whose effective replica identity carries a STORED generated column the
// covering publication does not publish — PostgreSQL 18 (measured on
// 18.4; PG 17.6 and 16.x refuse nothing, measured the same way).
//
// It lives in the tests ON PURPOSE. The shipped refusal keys on the
// PUBLICATION — a generated identity column absent from the
// RelationMessage just received — and carries no version branch at all;
// see cdc_generated_pk.go's "What the refusal keys on" section. What the
// server version changes is not sluice's verdict but what a TEST can set
// up: below this floor the source can produce the DELETE that
// demonstrates the harm, and at or above it PostgreSQL refuses to.
const pgVersionGeneratedIdentityWriteRefusal = 180000

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

// TestPremise_PostgresRefusesSourceWritesToAGeneratedIdentity measures
// the PostgreSQL 18 boundary that cdc_generated_pk.go's safety argument
// cites, in both directions, on whichever server the harness booted.
//
// The argument being checked is the one in that file's closing
// parenthetical: "PostgreSQL 18 also closes the silent half at the
// source." Written down, it stopped anyone from asking WHEN, and under
// WHICH conditions — and the answer turns out to be narrower and wider
// than the sentence suggests in different places. Measured on 18.4, the
// server refuses the source's own UPDATE/DELETE with SQLSTATE 42P10
// when ALL THREE hold:
//
//  1. a publication covers the table AND publishes update or delete (an
//     insert-only publication does not trip it, nor does one scoped to
//     other tables, nor does having no publication at all);
//  2. that publication does not carry `publish_generated_columns`;
//  3. the table's EFFECTIVE replica identity contains a STORED generated
//     column — where under FULL the effective identity is EVERY column,
//     not the primary key, so a generated column nowhere near the key
//     still trips it.
//
// (3) is why this test carries the two FULL cells. It is also the
// known-limit the fix's commit recorded: PreflightReplicaIdentity grades
// a FULL table by its PRIMARY KEY, because widening it to every column
// would over-refuse every pre-18 server, where the shape genuinely
// works. The limit is deliberate; what was missing was anything that
// measures the rule it is a limit of.
//
// INSERT is never refused, on any version — which is what keeps the
// reader's own refusal reachable on 18+ (it fires on the
// RelationMessage, and an INSERT produces one), and is why the tests
// below drive their refusal cells with an INSERT rather than skipping.
func TestPremise_PostgresRefusesSourceWritesToAGeneratedIdentity(t *testing.T) {
	const (
		genInKey = `CREATE TABLE t (a INT NOT NULL, c INT NOT NULL, b TEXT,
		                  g INT GENERATED ALWAYS AS (c*10) STORED, PRIMARY KEY (a, g));`
		genInKeySeed = `INSERT INTO t(a,c,b) VALUES (1,1,'x'), (2,2,'y');`
		genOutOfKey  = `CREATE TABLE t (a INT PRIMARY KEY, b TEXT,
		                  g INT GENERATED ALWAYS AS (a*2) STORED);`
		genSeed = `INSERT INTO t(a,b) VALUES (1,'x'), (2,'y');`
	)

	cases := []struct {
		name string
		ddl  string
		seed string
		pub  string
		// refusesFrom18 is the CLAIM this cell pins: at or above
		// [pgVersionGeneratedIdentityWriteRefusal] PostgreSQL refuses the
		// source's own UPDATE/DELETE here, and below it does not. Both
		// halves are asserted, so a server that starts refusing early —
		// or stops refusing — fails this test.
		refusesFrom18 bool
		// pubCoversT / wireCarriesG bind the MECHANISM to the verdict.
		// Refusing and publishing-the-column are two separate facts, and
		// pinning each alone would leave the argument between them
		// unpinned: the whole reason PostgreSQL refuses is that the
		// column is missing from what the publication will carry. Where a
		// publication covers t, this asserts pg_publication_tables'
		// account of that against the verdict above.
		pubCoversT   bool
		wireCarriesG bool
		// minVersion gates a cell whose DDL does not PARSE on older
		// servers, which is a different thing from a cell whose verdict
		// differs; only `publish_generated_columns` needs it.
		minVersion int
	}{
		{
			name:          "a generated PRIMARY KEY column, published",
			ddl:           genInKey,
			seed:          genInKeySeed,
			pub:           `CREATE PUBLICATION prem FOR TABLE t;`,
			refusesFrom18: true,
			pubCoversT:    true,
		},
		{
			name: "a generated PRIMARY KEY column, NO publication at all",
			ddl:  genInKey,
			seed: genInKeySeed,
		},
		{
			name:       "a generated PRIMARY KEY column, INSERT-ONLY publication",
			ddl:        genInKey,
			seed:       genInKeySeed,
			pub:        `CREATE PUBLICATION prem FOR TABLE t WITH (publish = 'insert');`,
			pubCoversT: true,
		},
		{
			name: "a generated PRIMARY KEY column, publication scoped to ANOTHER table",
			ddl:  genInKey + `CREATE TABLE other (x INT PRIMARY KEY);`,
			seed: genInKeySeed,
			pub:  `CREATE PUBLICATION prem FOR TABLE other;`,
		},
		{
			// The remedy the refusal message names, ground-truthed: on 18+
			// a publication carrying the option publishes the column AND
			// the source's own writes go through again. Both halves, so
			// the sentence sluice prints to operators is a measured claim.
			name:         "a generated PRIMARY KEY column, publication WITH publish_generated_columns = stored",
			ddl:          genInKey,
			seed:         genInKeySeed,
			pub:          `CREATE PUBLICATION prem FOR TABLE t WITH (publish_generated_columns = stored);`,
			pubCoversT:   true,
			wireCarriesG: true,
			minVersion:   pgVersionGeneratedIdentityWriteRefusal,
		},
		{
			// CONTROL: PostgreSQL's rule is about the IDENTITY, not about
			// owning a generated column. An ordinary computed column on a
			// DEFAULT-identity table is untouched on every version.
			name:       "CONTROL: a generated column OUTSIDE the identity, published",
			ddl:        genOutOfKey,
			seed:       genSeed,
			pub:        `CREATE PUBLICATION prem FOR TABLE t;`,
			pubCoversT: true,
		},
		{
			// FULL widens the identity to every column, so this refuses on
			// 18 even though the generated column is not in the key — the
			// cell sluice's PK-scoped preflight deliberately does not grade.
			name:          "REPLICA IDENTITY FULL, generated column OUTSIDE the PRIMARY KEY, published",
			ddl:           genOutOfKey + `ALTER TABLE t REPLICA IDENTITY FULL;`,
			seed:          genSeed,
			pub:           `CREATE PUBLICATION prem FOR TABLE t;`,
			refusesFrom18: true,
			pubCoversT:    true,
		},
		{
			// The same rule reached from the PK-less side, and the reason
			// the PK-less FULL control below has to change shape on 18+.
			name: "PK-less REPLICA IDENTITY FULL with a generated column, published",
			ddl: `CREATE TABLE t (a INT NOT NULL, b TEXT,
			        g INT GENERATED ALWAYS AS (a*2) STORED);
			      ALTER TABLE t REPLICA IDENTITY FULL;`,
			seed:          genSeed,
			pub:           `CREATE PUBLICATION prem FOR TABLE t;`,
			refusesFrom18: true,
			pubCoversT:    true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// A FRESH database per cell, not a fresh table: a publication
			// is database-scoped, and a leftover FOR ALL TABLES from an
			// earlier cell silently turns every later "no publication"
			// cell into a published one. That contamination made the first
			// pass of this measurement read as "PostgreSQL 18 refuses
			// unconditionally," which is wrong.
			dsn, cleanup := newSharedPGDB(t, "source_db")
			defer cleanup()

			version := pgServerVersionNum(t, dsn)
			if tc.minVersion != 0 && version < tc.minVersion {
				t.Skipf("server_version_num=%d < %d: this cell's publication option does not exist here", version, tc.minVersion)
			}

			applyPGSQL(t, dsn, tc.ddl)
			applyPGSQL(t, dsn, tc.seed)
			if tc.pub != "" {
				applyPGSQL(t, dsn, tc.pub)
			}

			// The cell's setup is itself an assertion: a leaked
			// publication is exactly the contamination this per-cell
			// database exists to prevent, and it would read as a verdict
			// rather than as a setup bug.
			wantPubs := 0
			if tc.pub != "" {
				wantPubs = 1
			}
			if n := pgCount(t, dsn, `SELECT count(*) FROM pg_publication`); n != wantPubs {
				t.Fatalf("pg_publication has %d row(s); want %d — the cell's setup is not what it says it is", n, wantPubs)
			}
			if tc.pubCoversT {
				carried := pgQueryStrings(t, dsn, `
					SELECT unnest(attnames) FROM pg_publication_tables
					WHERE pubname = 'prem' AND tablename = 't' ORDER BY 1`)
				if got := slices.Contains(carried, "g"); got != tc.wireCarriesG {
					t.Errorf(
						"publication carries \"g\" = %v, want %v (attnames = %v) — the MECHANISM behind the verdict "+
							"below has changed, so the verdict means something different now",
						got, tc.wireCarriesG, carried,
					)
				}
			}

			wantRefused := tc.refusesFrom18 && version >= pgVersionGeneratedIdentityWriteRefusal
			delErr := pgExecErr(t, dsn, `DELETE FROM t WHERE a = 1;`)
			updErr := pgExecErr(t, dsn, `UPDATE t SET b = 'z' WHERE a = 2;`)

			for _, probe := range []struct {
				verb string
				err  error
			}{{"DELETE", delErr}, {"UPDATE", updErr}} {
				got := isUnpublishedGeneratedIdentityRefusal(probe.err)
				switch {
				case wantRefused && !got:
					t.Errorf(
						"server_version_num=%d: the source %s was NOT refused (err=%v). The premise in "+
							"cdc_generated_pk.go — that PostgreSQL %d+ closes this at the source — no longer holds, so "+
							"the shape is silent here and sluice's reader refusal is the ONLY thing standing in front "+
							"of it. Re-derive the boundary",
						version, probe.verb, probe.err, pgVersionGeneratedIdentityWriteRefusal,
					)
				case !wantRefused && probe.err != nil:
					t.Errorf(
						"server_version_num=%d: the source %s failed and this cell expects it to succeed: %v",
						version, probe.verb, probe.err,
					)
				}
			}
			t.Logf("MEASURED: server_version_num=%d, source UPDATE/DELETE refused=%v", version, wantRefused)
		})
	}
}

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

	// The probe publication has now done its whole job: `published`,
	// read above, is PostgreSQL's own account of what the wire will
	// carry, and that number is already in hand. Dropping it before the
	// source DELETE is what keeps this proof running on every server —
	// from PostgreSQL 18 on, a publication over this shape makes the
	// server refuse the SOURCE APPLICATION'S OWN DELETE (42P10), which
	// is measured in its own right by
	// [TestPremise_PostgresRefusesSourceWritesToAGeneratedIdentity].
	// Nothing about the harm depends on the publication still existing:
	// what is being demonstrated is what an applier does with a partial
	// key, and the target — which never had a publication — is where
	// that is counted.
	applyPGSQL(t, srcDSN, `DROP PUBLICATION gen_pk_probe;`)

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
//
// # The DML is split, because PostgreSQL 18+ will not run half of it
//
// Every cell used to drive the stream with `INSERT …; DELETE …`. From
// PostgreSQL 18 the server refuses the DELETE outright on exactly the
// cells whose identity carries an unpublished generated column — the
// reader's own publication is what arms the rule — and that turned the
// suite red on the PG-version matrix.
//
// The INSERT alone is sufficient to reach the refusal, and that is a
// property of WHERE the refusal lives rather than a convenience:
// refuseUnpublishedGeneratedIdentity is called from resolveIdentityKeyCols
// on the RelationMessage, which pgoutput sends ahead of the first tuple
// of ANY kind. So the refusal cells stream an INSERT on every version
// and assert identically on all of them.
//
// The DELETE is still attempted on every cell, and its outcome is
// ASSERTED rather than skipped: pgRefusesDeleteFrom18 is each cell's
// claim about the server, checked in both directions. That keeps the
// version boundary pinned here as well as in
// [TestPremise_PostgresRefusesSourceWritesToAGeneratedIdentity] — a
// PostgreSQL that stopped refusing would fail this test rather than
// quietly restoring a cell nobody was watching.
func TestCDCReader_RefusesGeneratedIdentity(t *testing.T) {
	cases := []struct {
		name        string
		ddl         string
		insert      string
		del         string
		wantRefusal bool
		// pgRefusesDeleteFrom18 says PostgreSQL itself refuses this
		// cell's source DELETE from [pgVersionGeneratedIdentityWriteRefusal]
		// on, because the reader's publication covers a table whose
		// effective replica identity carries an unpublished generated
		// column. True on every refusal cell — and on ONE control, the
		// PK-less FULL table, because under FULL the effective identity
		// is every column.
		pgRefusesDeleteFrom18 bool
		msgHas                []string
	}{
		{
			name: "DEFAULT, part of the key generated",
			ddl: `CREATE TABLE t (a INT NOT NULL, c INT NOT NULL, b TEXT,
			        g INT GENERATED ALWAYS AS (c*10) STORED, PRIMARY KEY (a, g));`,
			insert:                `INSERT INTO t(a,c,b) VALUES (1,1,'x');`,
			del:                   `DELETE FROM t WHERE a=1;`,
			wantRefusal:           true,
			pgRefusesDeleteFrom18: true,
			msgHas:                []string{`"g"`, "NON-UNIQUE prefix"},
		},
		{
			name: "DEFAULT, the whole key generated",
			ddl: `CREATE TABLE t (a INT NOT NULL, b TEXT,
			        g INT GENERATED ALWAYS AS (a*2) STORED PRIMARY KEY);`,
			insert:                `INSERT INTO t(a,b) VALUES (1,'x');`,
			del:                   `DELETE FROM t WHERE a=1;`,
			wantRefusal:           true,
			pgRefusesDeleteFrom18: true,
			msgHas:                []string{`"g"`, "no published column carries the row identity at all"},
		},
		{
			name: "USING INDEX over a partly generated index",
			ddl: `CREATE TABLE t (a INT NOT NULL, c INT NOT NULL, b TEXT,
			        g INT GENERATED ALWAYS AS (c*10) STORED, PRIMARY KEY (a, g));
			      CREATE UNIQUE INDEX t_ag ON t(a, g);
			      ALTER TABLE t REPLICA IDENTITY USING INDEX t_ag;`,
			insert:                `INSERT INTO t(a,c,b) VALUES (1,1,'x');`,
			del:                   `DELETE FROM t WHERE a=1;`,
			wantRefusal:           true,
			pgRefusesDeleteFrom18: true,
			msgHas:                []string{`"g"`, "NON-UNIQUE prefix"},
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
			insert:                `INSERT INTO t(a,c,b) VALUES (1,1,'x');`,
			del:                   `DELETE FROM t WHERE a=1;`,
			wantRefusal:           true,
			pgRefusesDeleteFrom18: true,
			msgHas:                []string{`"g"`, "does not publish generated columns", "NON-UNIQUE prefix"},
		},
		{
			// CONTROL: a generated column that is not part of the identity
			// is the ordinary computed-column table, and it must be
			// invisible to this check. Its DELETE runs on every version —
			// PostgreSQL's rule is about the IDENTITY, not about owning a
			// generated column.
			name: "CONTROL: a generated column OUTSIDE the key",
			ddl: `CREATE TABLE t (a INT PRIMARY KEY, b TEXT,
			        g INT GENERATED ALWAYS AS (a*2) STORED);`,
			insert: `INSERT INTO t(a,b) VALUES (1,'x');`,
			del:    `DELETE FROM t WHERE a=1;`,
		},
		{
			name:   "CONTROL: an ordinary composite key",
			ddl:    `CREATE TABLE t (a INT NOT NULL, c INT NOT NULL, b TEXT, PRIMARY KEY (a, c));`,
			insert: `INSERT INTO t(a,c,b) VALUES (1,1,'x');`,
			del:    `DELETE FROM t WHERE a=1;`,
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
			insert: `INSERT INTO t(a,b) VALUES (1,'x');`,
			del:    `DELETE FROM t WHERE a=1;`,
		},
		{
			// CONTROL: PK-less FULL is legitimate (the operator set FULL
			// knowing there is no key) and has no identity index at all,
			// so the check must find nothing to grade.
			//
			// The one control PostgreSQL 18 takes the DELETE away from:
			// under FULL the effective identity is every column, so the
			// generated one is in it. The control does NOT lapse there —
			// its load-bearing half is that sluice must not refuse, and an
			// INSERT reaches the RelationMessage that would carry a
			// refusal just as well as a DELETE does. What is lost on 18+ is
			// only the shape of Delete.Before, and that is stated at the
			// assertion rather than left for a reader to infer.
			name: `CONTROL: PK-less REPLICA IDENTITY FULL with a generated column`,
			ddl: `CREATE TABLE t (a INT NOT NULL, b TEXT,
			        g INT GENERATED ALWAYS AS (a*2) STORED);
			      ALTER TABLE t REPLICA IDENTITY FULL;`,
			insert:                `INSERT INTO t(a,b) VALUES (1,'x');`,
			del:                   `DELETE FROM t WHERE a=1;`,
			pgRefusesDeleteFrom18: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dsn, cleanup := newSharedPGDB(t, "source_db")
			defer cleanup()
			version := pgServerVersionNum(t, dsn)
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
			applyPGSQL(t, dsn, tc.insert)

			// The DELETE is attempted on every cell and its outcome
			// asserted against the cell's claim about the server, so the
			// version boundary is pinned in both directions here too.
			// StreamChanges has already ensured the publication, which is
			// what arms PostgreSQL 18's rule.
			wantDeleteRefused := tc.pgRefusesDeleteFrom18 && version >= pgVersionGeneratedIdentityWriteRefusal
			delErr := pgExecErr(t, dsn, tc.del)
			switch {
			case wantDeleteRefused && !isUnpublishedGeneratedIdentityRefusal(delErr):
				t.Fatalf(
					"server_version_num=%d: expected PostgreSQL to refuse the source DELETE for this shape and it "+
						"did not (err=%v) — the boundary in pgVersionGeneratedIdentityWriteRefusal has moved",
					version, delErr,
				)
			case !wantDeleteRefused && delErr != nil:
				t.Fatalf("server_version_num=%d: the source DELETE failed unexpectedly: %v", version, delErr)
			}

			wantChanges := 2 // INSERT + DELETE
			if wantDeleteRefused {
				wantChanges = 1 // INSERT alone; PostgreSQL refused the DELETE
			}
			got := drainChanges(t, ctx, changes, wantChanges, 20*time.Second)
			cdcRdr, _ := rdr.(*CDCReader)
			var streamErr error
			if cdcRdr != nil {
				streamErr = cdcRdr.Err()
			}

			if !tc.wantRefusal {
				if streamErr != nil {
					t.Fatalf("CONTROL over-refused: %v", streamErr)
				}
				if len(got) != wantChanges {
					t.Fatalf("CONTROL got %d changes; want %d", len(got), wantChanges)
				}
				if wantDeleteRefused {
					// PostgreSQL 18+ took the DELETE away from this
					// control. The half that matters is intact and
					// asserted: the reader saw the RelationMessage — the
					// only place the refusal can fire — and did not refuse.
					// Delete.Before is what goes unexercised here, and it
					// stays covered on PG 16/17.
					if _, ok := got[0].(ir.Insert); !ok {
						t.Fatalf("CONTROL change[0] = %T; want ir.Insert", got[0])
					}
					t.Logf(
						"CONTROL streams normally (INSERT only: server_version_num=%d refuses the source DELETE for "+
							"this shape, so Delete.Before is unexercised on this server)", version,
					)
					return
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
// must not contain unpublished generated columns" — the boundary
// [TestPremise_PostgresRefusesSourceWritesToAGeneratedIdentity]
// measures). That is the item-93 harm, one shape over, and it is why
// this check lives in the preflight as well as in the reader.
//
// Runs on whatever server the harness booted, and needs no version
// branch of its own: it reads the catalog and never writes a row, so
// the same query gives the same verdict on 16 (for the pre-18, silent
// reason) as on 18 (for the loud one).
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

// pgServerVersionNum reads the live server's server_version_num, so the
// boundary assertions above compare against the server they actually
// ran on rather than against the image name the harness was pointed at.
func pgServerVersionNum(t *testing.T, dsn string) int {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	v, err := serverVersionNum(ctx, db)
	if err != nil {
		t.Fatalf("serverVersionNum: %v", err)
	}
	return v
}

// pgExecErr runs a statement and RETURNS its error instead of failing
// the test — the whole point of the boundary cells is that a refusal is
// sometimes the expected outcome.
func pgExecErr(t *testing.T, dsn, sqlText string) error {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, execErr := db.ExecContext(ctx, sqlText)
	return execErr
}

// isUnpublishedGeneratedIdentityRefusal reports whether err is
// PostgreSQL 18+'s refusal of the source's own UPDATE/DELETE over an
// identity carrying an unpublished generated column.
//
// The DETAIL is checked as well as the SQLSTATE, deliberately. 42P10 is
// invalid_column_reference — a general-purpose code PostgreSQL raises
// for several unrelated mistakes, including ones a malformed test
// fixture would produce. Matching the code alone would let a broken
// setup read as a successful measurement of the boundary.
func isUnpublishedGeneratedIdentityRefusal(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	// Matched against the SAME literal the preflight quotes to operators
	// ([pgGeneratedIdentityRefusalDetail]) — that is what makes the quote
	// a measurement rather than a claim (Bug 235).
	return pgErr.Code == "42P10" && strings.Contains(pgErr.Detail, pgGeneratedIdentityRefusalDetail)
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
