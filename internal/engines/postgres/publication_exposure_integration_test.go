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
// This test asserts BOTH halves against a live server: the audit's own
// answer, and the publication's actual coverage that answer must track.
// Binding them here is what stops the two drifting — a future PG that
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

		-- Outside, and NOT at risk: each for its own reason.
		CREATE TABLE other.haspk       (a int primary key, b text);
		CREATE TABLE other.hasfull     (a int, b text);
		ALTER TABLE other.hasfull REPLICA IDENTITY FULL;
		CREATE UNLOGGED TABLE other.unlogged (a int, b text);
		CREATE TABLE other.parted (a int, b text) PARTITION BY RANGE (a);
		CREATE VIEW other.v AS SELECT 1 AS x;
		CREATE MATERIALIZED VIEW other.mv AS SELECT 1 AS x;
	`)

	// The predicate the streamer builds: selected namespace AND allowed by
	// the operator's table filter. "selected.excluded_nokey" is in the
	// namespace but not allowed, so nothing else grades it.
	covered := func(namespace, table string) bool {
		return namespace == "selected" && table != "excluded_nokey"
	}
	got, err := openExposureAuditor(t, ctx, dsn).AuditPublicationExposure(ctx, covered)
	if err != nil {
		t.Fatalf("AuditUnselectedNamespaceExposure: %v", err)
	}

	joined := strings.Join(got, "\n")
	mustName := []string{
		"other.nokey",
		"other.nothing",
		// The third cell: covered by neither the refusal nor, before this,
		// by the audit.
		"selected.excluded_nokey",
	}
	mustNotName := []string{
		"selected.nokey", // graded by the refusing preflight, not here
		"other.haspk",    // has a usable primary key
		"other.hasfull",  // REPLICA IDENTITY FULL is always sufficient
		"other.unlogged", // an unlogged table is not published
		"other.parted",   // a partitioned PARENT is not published
		"other.v",        // a view is not a table
		"other.mv",       // nor is a materialized view
	}
	for _, want := range mustName {
		if !strings.Contains(joined, want) {
			t.Errorf("audit did not name %s, which a FOR ALL TABLES publication WILL break:\n%s", want, joined)
		}
	}
	for _, unwanted := range mustNotName {
		if strings.Contains(joined, unwanted) {
			t.Errorf("audit named %s, which is NOT at risk — a warning listing safe tables is a false alarm:\n%s",
				unwanted, joined)
		}
	}

	// The second half: what the SERVER says the publication covers. The
	// audit's exclusions are only correct while this holds.
	applyPGSQL(t, dsn, `CREATE PUBLICATION a24b_probe FOR ALL TABLES;`)
	coveredByServer := append(publicationCoverage(t, dsn, "a24b_probe", "other"),
		publicationCoverage(t, dsn, "a24b_probe", "selected")...)
	coveredJoined := strings.Join(coveredByServer, "\n")
	for _, want := range []string{"other.nokey", "other.nothing", "other.haspk", "other.hasfull", "selected.excluded_nokey"} {
		if !strings.Contains(coveredJoined, want) {
			t.Errorf("premise moved: the server does NOT publish %s, so this audit's relkind/relpersistence "+
				"filter no longer describes coverage:\n%s", want, coveredJoined)
		}
	}
	for _, unwanted := range []string{"other.unlogged", "other.parted", "other.v", "other.mv"} {
		if strings.Contains(coveredJoined, unwanted) {
			t.Errorf("premise moved: the server DOES publish %s, so the audit is now under-reporting and an "+
				"operator will not be warned about it:\n%s", unwanted, coveredJoined)
		}
	}

	// Anti-vacuity: a fixture that stopped creating tables would satisfy
	// every "must not name" assertion above by having nothing to name.
	if len(got) < 3 {
		t.Fatalf("audit returned %d rows; the fixture is not producing at-risk tables and this test proves nothing", len(got))
	}
	if len(coveredByServer) < 5 {
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
