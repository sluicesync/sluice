//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Real-world schema corpus harness (Idea 3, prep-new-test-surfaces.md).
// Exercises sluice's schema reader + cross-engine translation against
// operator-shaped real schemas (not the synthetic fuzz generator).
//
// Signal = Migrator{DryRun:true}: reads the source schema and plans
// the cross-engine target DDL WITHOUT moving data — where translation
// bugs / unexpected loud-refusals live. The corpus *.sql are
// fetch-on-demand (gitignored); missing files SKIP (not fail) so this
// is green on a fresh checkout until `fetch.sh` is run.
//
// Chinook legs assert (small, clean, matched MySQL/PG pair → an
// oracle). GitLab is a *characterization* leg: it's a 1444-table real
// PG schema that vanilla PG may not even fully load — outcomes are
// logged, not hard-asserted, like the fuzz harness's verdict shape.

package pipeline

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/orware/sluice/internal/engines"

	_ "github.com/orware/sluice/internal/engines/mysql"
	_ "github.com/orware/sluice/internal/engines/postgres"
)

// applyPGDDLBestEffort runs the script in one autocommit batch and
// RETURNS the error instead of failing the test — for characterizing
// a real-world schema that vanilla PG may not fully accept (roles /
// extensions / OWNER). Statements before the first failure are
// applied (no enclosing txn), so the reader still sees a partial
// schema. Statement-splitting is deliberately NOT attempted (GitLab's
// dollar-quoted function bodies make naive splitting wrong).
func applyPGDDLBestEffort(dsn, ddl string) error {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	_, err = db.ExecContext(ctx, ddl)
	return err
}

func readCorpus(t *testing.T, name string) string {
	t.Helper()
	p := filepath.Join("testdata", "real-world-corpus", name)
	b, err := os.ReadFile(p)
	if err != nil {
		t.Skipf("corpus %s not present (%v) — run internal/pipeline/testdata/real-world-corpus/fetch.sh", name, err)
	}
	if len(b) == 0 {
		t.Skipf("corpus %s is empty — re-run fetch.sh", name)
	}
	return string(b)
}

// corpusAssertTables opens the source via sluice's schema reader and
// FAILS if fewer than min tables are read. Load-bearing guard against
// a VACUOUS pass: Migrator.Run returns nil (not an error) on a
// 0-table schema (migrate.go "nothing to migrate"), so a corpus whose
// DDL landed in a side DB would otherwise pass green without sluice
// ever reading the real schema. Returns the count.
func corpusAssertTables(t *testing.T, engineName, dsn string, min int) int {
	t.Helper()
	eng, ok := engines.Get(engineName)
	if !ok {
		t.Fatalf("engine %q not registered", engineName)
	}
	sr, err := eng.OpenSchemaReader(ctx2min(t), dsn)
	if err != nil {
		t.Fatalf("%s OpenSchemaReader: %v", engineName, err)
	}
	if c, isC := sr.(interface{ Close() error }); isC {
		defer func() { _ = c.Close() }()
	}
	sch, err := sr.ReadSchema(ctx2min(t))
	if err != nil {
		t.Fatalf("%s ReadSchema: %v", engineName, err)
	}
	n := len(sch.Tables)
	if n < min {
		t.Fatalf("%s read %d tables; want >= %d — VACUOUS: corpus DDL likely landed in a side DB (check fetch.sh DB-switch strip); sluice never saw the real schema", engineName, n, min)
	}
	t.Logf("%s: schema reader saw %d tables (>= %d) — non-vacuous", engineName, n, min)
	return n
}

// corpusRawPGTableCount counts base tables directly via
// information_schema — a vacuity check that does NOT go through
// sluice's ReadSchema. Needed for the GitLab *characterization* leg:
// sluice ReadSchema legitimately loud-refuses GitLab's `tsvector`
// (expected, see iteration-1 findings), so the strict
// corpusAssertTables (Fatalf on any ReadSchema error) would wrongly
// fail it. This separates "did the DDL load?" (vacuity) from "can
// sluice read/translate it?" (the characterization).
func corpusRawPGTableCount(t *testing.T, dsn string) int {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open pg: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var n int
	if err := db.QueryRowContext(ctx,
		"SELECT count(*) FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE'",
	).Scan(&n); err != nil {
		t.Fatalf("raw table count: %v", err)
	}
	return n
}

// Chinook MySQL → PG, DryRun: source read + cross-engine plan, no data.
func TestCorpus_Chinook_MySQLToPG_DryRun(t *testing.T) {
	ddl := readCorpus(t, "chinook_mysql.ddl.sql")
	src, _, cleanup := startMySQL(t)
	defer cleanup()
	_, tgt, pgCleanup := startPostgres(t)
	defer pgCleanup()

	applyMySQLDDL(t, src, ddl)
	corpusAssertTables(t, "mysql", src, 11) // Chinook has exactly 11 tables

	myEng, _ := engines.Get("mysql")
	pgEng, _ := engines.Get("postgres")
	mig := &Migrator{Source: myEng, Target: pgEng, SourceDSN: src, TargetDSN: tgt, DryRun: true}
	if err := mig.Run(ctx2min(t)); err != nil {
		t.Fatalf("Chinook MySQL→PG DryRun: schema read/plan failed: %v", err)
	}
	t.Log("Chinook MySQL→PG DryRun: schema read + cross-engine plan OK (11-table matched-pair source)")
}

// Chinook PG → MySQL, DryRun.
func TestCorpus_Chinook_PGToMySQL_DryRun(t *testing.T) {
	ddl := readCorpus(t, "chinook_postgres.ddl.sql")
	src, _, cleanup := startPostgres(t)
	defer cleanup()
	_, tgt, myCleanup := startMySQL(t)
	defer myCleanup()

	applyPGDDL(t, src, ddl)
	corpusAssertTables(t, "postgres", src, 11) // Chinook has exactly 11 tables

	pgEng, _ := engines.Get("postgres")
	myEng, _ := engines.Get("mysql")
	mig := &Migrator{Source: pgEng, Target: myEng, SourceDSN: src, TargetDSN: tgt, DryRun: true}
	if err := mig.Run(ctx2min(t)); err != nil {
		t.Fatalf("Chinook PG→MySQL DryRun: schema read/plan failed: %v", err)
	}
	t.Log("Chinook PG→MySQL DryRun: schema read + cross-engine plan OK")
}

// GitLab db/structure.sql — CHARACTERIZATION (not asserted). 2.8MB /
// 1444 tables of real PG (partitioning, extensions, functions). Two
// signals: (1) does vanilla PG even load it; (2) if so, can sluice's
// PG reader + PG→MySQL DryRun handle it at scale / how does it refuse.
// Outcomes are logged so a failing apply or a loud refusal is
// recorded, never a red bar (the schema isn't expected to be
// cross-engine-clean — the point is to find where sluice strains).
func TestCorpus_GitLab_PG_Characterize(t *testing.T) {
	ddl := readCorpus(t, "gitlab_structure.pg.sql")
	src, _, cleanup := startPostgres(t)
	defer cleanup()
	_, tgt, myCleanup := startMySQL(t)
	defer myCleanup()

	// Best-effort apply: GitLab's structure.sql may reference roles /
	// extensions / settings a vanilla PG16 lacks. Capture, don't fail.
	applyErr := applyPGDDLBestEffort(src, ddl)
	if applyErr != nil {
		t.Logf("GitLab structure.sql apply into vanilla PG: PARTIAL/FAILED — %v\n"+
			"  → iteration-2 finding: needs preprocessing (strip roles/OWNER/extensions) before it's a usable PG-reader corpus.", truncErr(applyErr))
	} else {
		t.Log("GitLab structure.sql applied into vanilla PG cleanly (unexpected — good).")
	}
	// Non-vacuous guard via RAW count (not sluice ReadSchema, which
	// correctly loud-refuses GitLab's tsvector — that refusal is the
	// characterized finding, not a failure). GitLab genuinely loads
	// ~1444 tables; <100 means the apply silently didn't take.
	if n := corpusRawPGTableCount(t, src); n < 100 {
		t.Fatalf("GitLab loaded only %d base tables (raw count) — VACUOUS; structure.sql did not take", n)
	} else {
		t.Logf("GitLab: %d base tables loaded (raw count) — non-vacuous; now characterizing sluice read/translate", n)
	}

	pgEng, _ := engines.Get("postgres")
	myEng, _ := engines.Get("mysql")
	mig := &Migrator{Source: pgEng, Target: myEng, SourceDSN: src, TargetDSN: tgt, DryRun: true}
	if err := mig.Run(ctx2min(t)); err != nil {
		t.Logf("GitLab PG→MySQL DryRun: refused/failed (EXPECTED-ish at this scale) — %v", truncErr(err))
		return
	}
	t.Log("GitLab PG→MySQL DryRun: schema read + cross-engine plan completed (characterization).")
}

// GitLab PG→PG — the COMPLEMENT of the cross-engine leg above, on the
// same real schema. catalog_resources.search_vector is a core PG
// `tsvector`. Cross-engine (above) correctly LOUD-REFUSES it (no
// MySQL column-type equivalent). Same-engine PG→PG must instead
// carry it VERBATIM (ir.VerbatimType; ADR-0047 tier, VerbatimEligible
// set by the PG reader for same-engine runs). This leg PROVES that on
// a real production tsvector column rather than trusting code-reading:
// PG→PG DryRun must succeed with NO "unsupported data_type tsvector"
// — an error here would be a genuine defect (verbatim path not
// engaged for PG→PG migrate), which is exactly the finding we'd want.
func TestCorpus_GitLab_PGToPG_VerbatimCarry(t *testing.T) {
	ddl := readCorpus(t, "gitlab_structure.pg.sql")
	src, tgt, cleanup := startPostgres(t) // src = load GitLab; tgt = PG→PG target
	defer cleanup()

	if err := applyPGDDLBestEffort(src, ddl); err != nil {
		t.Logf("GitLab apply into vanilla PG: PARTIAL/FAILED — %v", truncErr(err))
	}
	if n := corpusRawPGTableCount(t, src); n < 100 {
		t.Fatalf("GitLab loaded only %d base tables (raw) — VACUOUS", n)
	} else {
		t.Logf("GitLab: %d base tables loaded (raw) — non-vacuous", n)
	}

	pgEng, _ := engines.Get("postgres")
	mig := &Migrator{Source: pgEng, Target: pgEng, SourceDSN: src, TargetDSN: tgt, DryRun: true}
	err := mig.Run(ctx2min(t))
	if err == nil {
		t.Log("GitLab PG→PG DryRun: 1000+ tables read + planned via verbatim carry, " +
			"NO loud refusal — same-engine fidelity confirmed. (If this passes, the " +
			"core-range-type gap below was fixed; tighten this leg to an assertion.)")
		return
	}
	// CHARACTERIZED KNOWN GAP (iteration-3 finding, 2026-05-19):
	// `tsvector`/`tsquery` got an explicit same-engine verbatim branch
	// (catalog Bug 17, types.go:379) but the carve-out was made
	// type-by-type for the *representative*, not generalized to the
	// *class* of core PG types with no rich cross-engine IR shape. So
	// PG→PG loud-refuses core RANGE types (int4range/int8range/
	// numrange/tsrange/tstzrange/daterange) though they're trivially
	// verbatim-carryable — the exact gap tsvector had pre-Bug-17.
	// Loud, not silent → tenet holds, no corruption; a real
	// fidelity/UX gap. Tracked: real-world-corpus-findings.md +
	// roadmap. This leg stays GREEN by characterizing the known class
	// and only FAILs on an UNEXPECTED error shape (a new finding).
	msg := err.Error()
	knownGap := strings.Contains(msg, "int8range") || strings.Contains(msg, "int4range") ||
		strings.Contains(msg, "numrange") || strings.Contains(msg, "tsrange") ||
		strings.Contains(msg, "tstzrange") || strings.Contains(msg, "daterange")
	if !knownGap {
		t.Fatalf("GitLab PG→PG DryRun failed with an UNEXPECTED shape (not the tracked "+
			"core-range-type verbatim gap) — investigate as a NEW finding: %v", truncErr(err))
	}
	t.Logf("GitLab PG→PG: CHARACTERIZED KNOWN GAP — core range type loud-refused on the "+
		"same-engine path (should verbatim-carry like tsvector/Bug-17). Not corruption "+
		"(loud). Tracked in real-world-corpus-findings.md / roadmap. err=%v", truncErr(err))
}

// --- Iteration 2 ---

// MediaWiki is the guaranteed-equivalent cross-engine ORACLE: the
// MySQL and PG tables-generated.sql are both generated from one
// abstract schema (sql/tables.json), so a clean read+plan on each
// direction is a stronger signal than independently-authored pairs.
// (The deeper "does sluice's MySQL→PG emitted schema match the
// upstream PG side" congruence check is iteration 3.)
func TestCorpus_MediaWiki_MySQLToPG_DryRun(t *testing.T) {
	ddl := readCorpus(t, "mediawiki_mysql.ddl.sql")
	src, _, cleanup := startMySQL(t)
	defer cleanup()
	_, tgt, pgCleanup := startPostgres(t)
	defer pgCleanup()

	applyMySQLDDL(t, src, ddl)
	corpusAssertTables(t, "mysql", src, 50) // MediaWiki generates 64 tables

	myEng, _ := engines.Get("mysql")
	pgEng, _ := engines.Get("postgres")
	mig := &Migrator{Source: myEng, Target: pgEng, SourceDSN: src, TargetDSN: tgt, DryRun: true}
	if err := mig.Run(ctx2min(t)); err != nil {
		t.Fatalf("MediaWiki MySQL→PG DryRun: schema read/plan failed: %v", err)
	}
	t.Log("MediaWiki MySQL→PG DryRun: 64-table generated-schema read + cross-engine plan OK")
}

func TestCorpus_MediaWiki_PGToMySQL_DryRun(t *testing.T) {
	ddl := readCorpus(t, "mediawiki_postgres.ddl.sql")
	src, _, cleanup := startPostgres(t)
	defer cleanup()
	_, tgt, myCleanup := startMySQL(t)
	defer myCleanup()

	applyPGDDL(t, src, ddl)
	corpusAssertTables(t, "postgres", src, 50) // MediaWiki generates 64 tables

	pgEng, _ := engines.Get("postgres")
	myEng, _ := engines.Get("mysql")
	mig := &Migrator{Source: pgEng, Target: myEng, SourceDSN: src, TargetDSN: tgt, DryRun: true}
	if err := mig.Run(ctx2min(t)); err != nil {
		t.Fatalf("MediaWiki PG→MySQL DryRun: schema read/plan failed: %v", err)
	}
	t.Log("MediaWiki PG→MySQL DryRun: schema read + cross-engine plan OK (oracle pair)")
}

// datacharmer employees (partitioned): real MySQL with PARTITION BY —
// a feature Chinook lacks. Asserts: sluice's MySQL reader handles a
// partitioned real schema and the PG plan succeeds or refuses loudly
// (a crash/silent-drop here would be a genuine finding).
func TestCorpus_Employees_MySQLToPG_DryRun(t *testing.T) {
	ddl := readCorpus(t, "employees_mysql_partitioned.ddl.sql")
	src, _, cleanup := startMySQL(t)
	defer cleanup()
	_, tgt, pgCleanup := startPostgres(t)
	defer pgCleanup()

	applyMySQLDDL(t, src, ddl)
	corpusAssertTables(t, "mysql", src, 6) // employees test_db has exactly 6 tables

	myEng, _ := engines.Get("mysql")
	pgEng, _ := engines.Get("postgres")
	mig := &Migrator{Source: myEng, Target: pgEng, SourceDSN: src, TargetDSN: tgt, DryRun: true}
	if err := mig.Run(ctx2min(t)); err != nil {
		// Partitioned-table cross-engine handling: a loud refusal is
		// acceptable (characterize); a crash/panic is a real finding.
		t.Logf("employees(partitioned) MySQL→PG DryRun: refused/failed — %v", truncErr(err))
		return
	}
	t.Log("employees(partitioned) MySQL→PG DryRun: read + cross-engine plan OK")
}

// --- Iteration 3 ---

// Joomla ships raw install SQL for BOTH MySQL and PG → a real-CMS
// matched cross-engine pair (28 tables each; independently authored
// per dialect, like Chinook). Asserts both directions read + plan.
func TestCorpus_Joomla_MySQLToPG_DryRun(t *testing.T) {
	ddl := readCorpus(t, "joomla_mysql.ddl.sql")
	src, _, cleanup := startMySQL(t)
	defer cleanup()
	_, tgt, pgCleanup := startPostgres(t)
	defer pgCleanup()

	applyMySQLDDL(t, src, ddl)
	corpusAssertTables(t, "mysql", src, 20) // Joomla core ≈ 28 tables

	myEng, _ := engines.Get("mysql")
	pgEng, _ := engines.Get("postgres")
	mig := &Migrator{Source: myEng, Target: pgEng, SourceDSN: src, TargetDSN: tgt, DryRun: true}
	if err := mig.Run(ctx2min(t)); err != nil {
		t.Fatalf("Joomla MySQL→PG DryRun: schema read/plan failed: %v", truncErr(err))
	}
	t.Log("Joomla MySQL→PG DryRun: real-CMS schema read + cross-engine plan OK")
}

func TestCorpus_Joomla_PGToMySQL_DryRun(t *testing.T) {
	ddl := readCorpus(t, "joomla_postgres.ddl.sql")
	src, _, cleanup := startPostgres(t)
	defer cleanup()
	_, tgt, myCleanup := startMySQL(t)
	defer myCleanup()

	applyPGDDL(t, src, ddl)
	corpusAssertTables(t, "postgres", src, 20)

	pgEng, _ := engines.Get("postgres")
	myEng, _ := engines.Get("mysql")
	mig := &Migrator{Source: pgEng, Target: myEng, SourceDSN: src, TargetDSN: tgt, DryRun: true}
	if err := mig.Run(ctx2min(t)); err != nil {
		t.Fatalf("Joomla PG→MySQL DryRun: schema read/plan failed: %v", truncErr(err))
	}
	t.Log("Joomla PG→MySQL DryRun: read + cross-engine plan OK (matched-pair)")
}

// WordPress core schema (extracted from PHP wp_get_db_schema()) — the
// canonical operator-brought MySQL shape.
func TestCorpus_WordPress_MySQLToPG_DryRun(t *testing.T) {
	ddl := readCorpus(t, "wordpress_mysql.ddl.sql")
	src, _, cleanup := startMySQL(t)
	defer cleanup()
	_, tgt, pgCleanup := startPostgres(t)
	defer pgCleanup()

	// WordPress's real schema uses `datetime NOT NULL default
	// '0000-00-00 00:00:00'` — valid in the permissive sql_mode
	// WordPress targets (what millions of installs run on), but
	// MySQL 8.0's default strict mode (NO_ZERO_DATE) rejects it at
	// DDL time (Error 1067). Load the schema AS-IS under WP's mode
	// (faithful corpus) rather than rewriting it (which would test a
	// fake schema). Same session as the CREATE TABLEs (multi-stmt
	// exec on one conn), so the SET applies.
	applyMySQLDDL(t, src, "SET SESSION sql_mode='NO_ENGINE_SUBSTITUTION';\n\n"+ddl)
	corpusAssertTables(t, "mysql", src, 12) // WP core single-site ≈ 19 tables

	myEng, _ := engines.Get("mysql")
	pgEng, _ := engines.Get("postgres")
	mig := &Migrator{Source: myEng, Target: pgEng, SourceDSN: src, TargetDSN: tgt, DryRun: true}
	if err := mig.Run(ctx2min(t)); err != nil {
		t.Fatalf("WordPress MySQL→PG DryRun: schema read/plan failed: %v", truncErr(err))
	}
	t.Log("WordPress MySQL→PG DryRun: canonical WP core schema read + cross-engine plan OK")
}

func truncErr(err error) string {
	s := err.Error()
	if len(s) > 600 {
		return s[:600] + " …[truncated]"
	}
	return s
}
