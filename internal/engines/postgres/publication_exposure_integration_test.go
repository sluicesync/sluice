//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// A2-4b. The audit's whole job is to name the tables a FOR ALL TABLES
// publication will stop accepting UPDATE and DELETE on — and to name ONLY
// those, because a warning that lists tables which were never at risk is
// the false-alarm shape this project has spent a lot of time removing.
//
// So the at-risk set is ground-truthed against the server rather than
// reasoned about. Measured identically on 16.15, 17.11, 18.6 and
// 19beta1: pg_publication_tables for a FOR ALL TABLES publication
// resolves to exactly relkind='r' AND relpersistence='p'. An UNLOGGED
// table, a partitioned PARENT, a view and a materialized view are all
// outside it; a leaf partition is inside.
//
// This test asserts THREE halves against a live server, each grading the
// one before it with evidence the one before it does not have:
//
//  1. the audit's own answer — which tables it names, and with which
//     reason, so the assertion pins WHICH relreplident arm decided;
//  2. the publication's actual coverage, read from pg_publication_tables
//     rather than re-derived from the audit's own relkind/relpersistence
//     filter;
//  3. whether the server actually REFUSES the UPDATE. (2) proves a table
//     is published; only (3) proves it is broken, which is the thing the
//     warning claims. Without it "at risk" is a catalog inference and the
//     usability predicate is graded against nothing.
//
// Binding them here is what stops them drifting — a future PG that
// starts publishing unlogged tables would make the second half fail and
// tell the next reader exactly which assumption moved.
func TestAuditPublicationExposure_MatchesRealPublicationCoverage(t *testing.T) {
	ctx := context.Background()
	dsn, cleanup := startPostgresForCDC(t)
	defer cleanup()

	applyPGSQL(t, dsn, `
		CREATE SCHEMA selected;
		CREATE SCHEMA other;

		-- In the SELECTED set and ALLOWED by the filter: the refusing
		-- preflight grades this one, so this audit must stay silent about
		-- it or the operator is told the same thing twice.
		CREATE TABLE selected.nokey (a int, b text);

		-- THE THIRD CELL. In a SELECTED namespace but EXCLUDED by the
		-- operator's table filter. The refusing preflight applies that
		-- filter before it grades anything, so it never sees this table —
		-- and the first cut of this audit skipped selected namespaces
		-- wholesale, so it did not either. It is still in the FOR ALL
		-- TABLES publication, so its writes break with nothing said. This
		-- is the operator who followed the documented advice to take a
		-- problem table out of the sync.
		CREATE TABLE selected.excluded_nokey (a int, b text);

		-- Outside, and genuinely at risk: permanent, logged, no usable
		-- replica identity.
		CREATE TABLE other.nokey        (a int, b text);
		CREATE TABLE other.nothing      (a int primary key, b text);
		ALTER TABLE other.nothing REPLICA IDENTITY NOTHING;

		-- relreplident='i', BOTH arms. Nothing exercised this letter
		-- before, so the whole ri LATERAL and the case "i": branch were
		-- dead in this test.
		--
		-- MEASURED on 16.15, 17.11 and 18.6, because the reachable shapes
		-- here are narrower than they look: PostgreSQL REFUSES, at ALTER
		-- time, every index the usability predicate would reject —
		-- "cannot use partial index", "cannot use non-immediate index"
		-- (that is the DEFERRABLE one), "cannot use non-unique index",
		-- "cannot use invalid index" — and it also refuses to drop the
		-- NOT NULL afterwards ("column a is in index used as replica
		-- identity"). So a table that nominates a still-present index
		-- which fails the predicate is not reachable by any DDL the
		-- server accepts.
		--
		-- What IS reachable, and is the shape an operator actually
		-- creates, is DROPPING the nominated index: the drop succeeds,
		-- relreplident STAYS 'i', and the table now nominates nothing at
		-- all. That is other.ri_dropped, and the server refuses UPDATE on
		-- it — half (3) below is what proves that rather than assuming it.
		--
		-- The consequence for THIS test, stated so it is not mistaken for
		-- coverage it does not have: a mutation replacing the ri
		-- LATERAL's four-predicate expression with TRUE SURVIVES, because
		-- ri_dropped's LATERAL is empty and COALESCE decides. That is the
		-- predicate being unreachable, not the test missing it — the pk
		-- LATERAL's copy of the same expression IS graded, by
		-- other.pk_deferrable below.
		CREATE TABLE other.ri_usable (a int NOT NULL, b text);
		CREATE UNIQUE INDEX ri_usable_ux ON other.ri_usable (a);
		ALTER TABLE other.ri_usable REPLICA IDENTITY USING INDEX ri_usable_ux;

		CREATE TABLE other.ri_dropped (a int NOT NULL, b text);
		CREATE UNIQUE INDEX ri_dropped_ux ON other.ri_dropped (a);
		ALTER TABLE other.ri_dropped REPLICA IDENTITY USING INDEX ri_dropped_ux;
		DROP INDEX other.ri_dropped_ux;

		-- A PRIMARY KEY that EXISTS and is UNUSABLE. Every other at-risk
		-- table here has no index at all, so pk.usable came from COALESCE
		-- over an empty LATERAL and the four-predicate expression was
		-- never evaluated to false against a real index. A DEFERRABLE key
		-- has indimmediate=false, which is exactly the predicate
		-- PostgreSQL's own RelationGetIndexList applies before it will
		-- resolve REPLICA IDENTITY DEFAULT — so the key is there and
		-- publishes nothing.
		CREATE TABLE other.pk_deferrable (a int, b text,
			CONSTRAINT pk_deferrable_pk PRIMARY KEY (a) DEFERRABLE);

		-- Outside, and NOT at risk: each for its own reason.
		CREATE TABLE other.haspk       (a int primary key, b text);
		CREATE TABLE other.hasfull     (a int, b text);
		ALTER TABLE other.hasfull REPLICA IDENTITY FULL;
		CREATE UNLOGGED TABLE other.unlogged (a int, b text);

		-- The parent is outside the publication; the LEAF is inside. The
		-- leaf was the claim this test made in prose and pinned with
		-- nothing — other.parted had no partitions, so "a leaf partition
		-- is inside" was asserted by a comment and by no row.
		CREATE TABLE other.parted (a int, b text) PARTITION BY RANGE (a);
		CREATE TABLE other.parted_p1 PARTITION OF other.parted FOR VALUES FROM (0) TO (10);

		CREATE VIEW other.v AS SELECT 1 AS x;
		CREATE MATERIALIZED VIEW other.mv AS SELECT 1 AS x;

		-- Half (3) checks UPDATE, and PostgreSQL raises the
		-- replica-identity error per updated TUPLE — a zero-row UPDATE
		-- succeeds on a table that has no identity at all. One row each,
		-- so the probe cannot pass vacuously.
		INSERT INTO other.ri_usable    VALUES (1, 'x');
		INSERT INTO other.ri_dropped   VALUES (1, 'x');
		INSERT INTO other.pk_deferrable VALUES (1, 'x');
		INSERT INTO other.parted       VALUES (1, 'x');
		INSERT INTO other.haspk        VALUES (1, 'x');
	`)

	// The predicate the streamer builds: selected namespace AND allowed by
	// the operator's table filter. "selected.excluded_nokey" is in the
	// namespace but not allowed, so nothing else grades it.
	covered := func(namespace, table string) bool {
		return namespace == "selected" && table != "excluded_nokey"
	}
	auditor := openExposureAuditor(t, ctx, dsn)
	got, err := auditor.AuditPublicationExposure(ctx, covered)
	if err != nil {
		t.Fatalf("AuditPublicationExposure: %v", err)
	}

	// THE NIL ARM, which nothing exercised until 2026-09-04 and which the
	// whole backup path rests on. `backup full --chain-slot` passes nil
	// because it runs no replica-identity refusal at all, so nothing there
	// is graded by anything else and everything at risk must be named.
	//
	// nil has to mean COVER NOTHING. Read as "no predicate supplied, so
	// nothing to report" -- which is a perfectly plausible reading of
	// `covered != nil &&` -- the backup warning silently names nothing,
	// ever, while every other assertion in this file stays green because
	// they all pass a real predicate.
	unfiltered, err := auditor.AuditPublicationExposure(ctx, nil)
	if err != nil {
		t.Fatalf("AuditPublicationExposure(nil): %v", err)
	}
	if len(unfiltered) <= len(got) {
		t.Fatalf("a nil predicate returned %d rows and a real one returned %d; nil must cover NOTHING, so it "+
			"must report a strict superset -- the backup path passes nil and would otherwise warn about "+
			"nothing at all", len(unfiltered), len(got))
	}
	unfilteredNames := auditedReasons(t, unfiltered)
	for _, want := range []string{"selected.nokey", "other.nokey", "selected.excluded_nokey"} {
		if _, ok := unfilteredNames[want]; !ok {
			t.Errorf("a nil predicate did not name %s; with nothing covered, every at-risk table in the "+
				"database must be reported:\n%s", want, strings.Join(unfiltered, "\n"))
		}
	}

	// Names are compared EXACTLY, not with strings.Contains over the
	// joined report. "other.parted" is a prefix of "other.parted_p1" —
	// the parent must NOT be named and the leaf MUST be, and a Contains
	// check for the parent is satisfied by the leaf, so the substring
	// form would report the parent as covered and pass while the audit
	// was wrong about both.
	reason := auditedReasons(t, got)
	joined := strings.Join(got, "\n")

	// The reason is asserted too, because it names WHICH relreplident arm
	// decided. other.ri_dropped reaching the switch's default arm — with
	// "no primary key" — would be the audit answering the right question
	// by the wrong route, and a name-only assertion cannot see that.
	//
	// other.pk_deferrable was reported as "no primary key" when it HAS
	// one -- right classification, false sentence, and a warning that
	// contradicts the operator's own d output is how a true warning gets
	// dismissed as a bug in the tool. Closed 2026-09-04 by selecting the
	// PK's name so "absent" and "present but unusable" stop arriving as
	// the same boolean. The refusing sibling, replicaIdentityUsable, has
	// always drawn this distinction.
	mustName := map[string]string{
		"other.nokey":             "no primary key",
		"other.nothing":           "REPLICA IDENTITY NOTHING",
		"other.ri_dropped":        "REPLICA IDENTITY USING INDEX names an unusable index",
		"other.pk_deferrable":     "PRIMARY KEY pk_deferrable_pk is not usable as a replica identity (deferrable, invalid, partial or non-unique)",
		"other.parted_p1":         "no primary key",
		"selected.excluded_nokey": "no primary key",
	}
	mustNotName := []string{
		"selected.nokey",  // graded by the refusing preflight, not here
		"other.haspk",     // has a usable primary key
		"other.hasfull",   // REPLICA IDENTITY FULL is always sufficient
		"other.ri_usable", // USING INDEX naming an index the server accepts
		"other.unlogged",  // an unlogged table is not published
		"other.parted",    // a partitioned PARENT is not published
		"other.v",         // a view is not a table
		"other.mv",        // nor is a materialized view
	}
	for want, wantReason := range mustName {
		switch got, ok := reason[want]; {
		case !ok:
			t.Errorf("audit did not name %s, which a FOR ALL TABLES publication WILL break:\n%s", want, joined)
		case got != wantReason:
			t.Errorf("audit named %s with reason %q, want %q — the wrong relreplident arm decided:\n%s",
				want, got, wantReason, joined)
		}
	}
	for _, unwanted := range mustNotName {
		if _, ok := reason[unwanted]; ok {
			t.Errorf("audit named %s, which is NOT at risk — a warning listing safe tables is a false alarm:\n%s",
				unwanted, joined)
		}
	}

	// The second half: what the SERVER says the publication covers. The
	// audit's exclusions are only correct while this holds.
	applyPGSQL(t, dsn, `CREATE PUBLICATION a24b_probe FOR ALL TABLES;`)
	coveredByServer := append(publicationCoverage(t, dsn, "a24b_probe", "other"),
		publicationCoverage(t, dsn, "a24b_probe", "selected")...)
	inPublication := stringSet(coveredByServer)
	coveredJoined := strings.Join(coveredByServer, "\n")
	for _, want := range []string{
		"other.nokey", "other.nothing", "other.haspk", "other.hasfull",
		"other.ri_usable", "other.ri_dropped", "other.pk_deferrable",
		"other.parted_p1", "selected.excluded_nokey",
	} {
		if !inPublication[want] {
			t.Errorf("premise moved: the server does NOT publish %s, so this audit's relkind/relpersistence "+
				"filter no longer describes coverage:\n%s", want, coveredJoined)
		}
	}
	for _, unwanted := range []string{"other.unlogged", "other.parted", "other.v", "other.mv"} {
		if inPublication[unwanted] {
			t.Errorf("premise moved: the server DOES publish %s, so the audit is now under-reporting and an "+
				"operator will not be warned about it:\n%s", unwanted, coveredJoined)
		}
	}

	// The third half: the breakage itself. Being published is not the
	// same as being broken — other.ri_usable and other.haspk are both in
	// the publication and both keep working — so this is the only
	// evidence that the four-predicate usability expression is grading
	// what PostgreSQL grades. It is INDEPENDENT of both halves above: it
	// reads no catalog, it asks the server to perform the write.
	for _, tc := range []struct {
		table       string
		wantRefused bool
	}{
		{"other.ri_usable", false},    // a nominated index the server accepts
		{"other.haspk", false},        // a plain immediate PRIMARY KEY
		{"other.ri_dropped", true},    // relreplident='i', nothing nominated
		{"other.pk_deferrable", true}, // a PRIMARY KEY that is not immediate
		{"other.parted_p1", true},     // a leaf partition with no key
	} {
		assertPublishedUpdate(t, dsn, tc.table, tc.wantRefused)
	}

	// Anti-vacuity: a fixture that stopped creating tables would satisfy
	// every "must not name" assertion above by having nothing to name.
	// The floors track the fixture — six at-risk tables, ten published
	// across the two schemas.
	if len(got) < 6 {
		t.Fatalf("audit returned %d rows; the fixture is not producing at-risk tables and this test proves nothing", len(got))
	}
	if len(coveredByServer) < 10 {
		t.Fatalf("the publication covers %d tables across the two schemas; the fixture is not building the shapes this grades",
			len(coveredByServer))
	}
}

// openExposureAuditor opens the source SchemaReader through the ENGINE and
// returns it as the optional auditor surface, failing loudly when it is not
// implemented. An unimplemented optional surface is an INERT check, which is
// the exact failure the compile-time pin in capabilities_assert.go and this
// assertion both exist to refuse.
func openExposureAuditor(t *testing.T, ctx context.Context, dsn string) ir.PublicationExposureAuditor {
	t.Helper()
	sr, err := (Engine{}).OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	t.Cleanup(func() {
		if c, ok := sr.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	})
	a, ok := sr.(ir.PublicationExposureAuditor)
	if !ok {
		t.Fatal("the Postgres SchemaReader does not implement ir.PublicationExposureAuditor — the audit is inert")
	}
	return a
}

// auditedReasons splits the audit's "ns.table (reason)" lines into a
// name→reason map so both can be asserted exactly. Exactness is the point:
// see the comment at the call site for the other.parted / other.parted_p1
// prefix collision that a substring match gets wrong in the passing
// direction.
func auditedReasons(t *testing.T, got []string) map[string]string {
	t.Helper()
	out := make(map[string]string, len(got))
	for _, line := range got {
		open := strings.Index(line, " (")
		if open < 0 || !strings.HasSuffix(line, ")") {
			t.Fatalf("audit line %q is not the documented \"ns.table (reason)\" shape; this test parses it", line)
		}
		out[line[:open]] = line[open+2 : len(line)-1]
	}
	return out
}

// stringSet is the exact-membership counterpart for the server's coverage
// list, which carries the same parted/parted_p1 prefix collision.
func stringSet(in []string) map[string]bool {
	out := make(map[string]bool, len(in))
	for _, s := range in {
		out[s] = true
	}
	return out
}

// assertPublishedUpdate performs a real UPDATE and asserts whether the
// server refuses it for want of a replica identity. This is the audit's
// independent expected value: the audit reads pg_class/pg_index and
// predicts; this asks PostgreSQL to do the write and reports what it did.
//
// A fresh connection per statement — a failed UPDATE aborts the
// transaction, and reusing the session would make every later probe fail
// for the wrong reason.
func assertPublishedUpdate(t *testing.T, dsn, table string, wantRefused bool) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	res, err := db.Exec("UPDATE " + table + " SET b = b")
	switch {
	case wantRefused && err == nil:
		n, _ := res.RowsAffected()
		t.Errorf("UPDATE %s affected %d rows and was NOT refused; the audit warns about this table, so either the "+
			"warning is a false alarm or the server's replica-identity rule moved", table, n)
	case wantRefused && !strings.Contains(err.Error(), "does not have a replica identity"):
		t.Errorf("UPDATE %s failed for the wrong reason (%v); the probe is not measuring the replica-identity refusal", table, err)
	case !wantRefused && err != nil:
		t.Errorf("UPDATE %s was refused (%v), but the audit does NOT name it — the audit is under-reporting and an "+
			"operator will hit this with nothing pointing back at sluice", table, err)
	case !wantRefused:
		if n, _ := res.RowsAffected(); n == 0 {
			t.Errorf("UPDATE %s affected 0 rows, so it never reached the per-tuple replica-identity check and this "+
				"probe proves nothing", table)
		}
	}
}

// publicationCoverage asks the SERVER which tables a publication actually
// covers. This is the independent expected value the audit is graded
// against: it is read from pg_publication_tables, not derived from the same
// relkind/relpersistence filter the audit itself applies.
func publicationCoverage(t *testing.T, dsn, publication, namespace string) []string {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	rows, err := db.Query(
		"SELECT schemaname || '.' || tablename FROM pg_publication_tables WHERE pubname = $1 AND schemaname = $2 ORDER BY 1",
		publication, namespace,
	)
	if err != nil {
		t.Fatalf("read publication coverage: %v", err)
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
		t.Fatalf("iterate: %v", err)
	}
	return out
}
