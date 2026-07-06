//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Real-world corpus × restore-parity oracle (roadmap item 52 finishing
// touch #1 — "run item-51's dump-parity oracle over the corpus
// schemas"). Item 51 built the oracle against a hand-authored
// kitchen-sink seed; item 52 feeds it the ecological-validity inputs:
// real application schemas (Chinook, MediaWiki) carrying real DDL
// distributions no synthetic seed reproduces.
//
// Mechanism (identical to the kitchen-sink harness, corpus DDL as the
// only substitution):
//
//  1. Load the corpus schema into source_db.
//  2. Migrate source_db → parity_sluice through sluice (same-engine).
//  3. Migrate source_db → parity_{pgdump,mysqldump} through the
//     reference dumper (pg_dump|psql / mysqldump|mysql), run INSIDE the
//     container so client and server versions always match.
//  4. Dump both targets --schema-only, decompose into object-identity
//     statement sets, diff.
//  5. Every divergence is either allowlisted-with-citation
//     (backup.DumpParityCorpusAllowlist{PG,MySQL} — CLASS-keyed globs,
//     since corpus object names are arbitrary) or a NEW FINDING that
//     fails the leg. A genuine gap is a find to report; a real
//     degradation is allowlisted with a citation, never silently.
//
// These are DryRun-companion legs: the existing corpus DryRun/congruence
// legs prove sluice reads+plans+emits a structurally-congruent schema;
// these prove the emitted catalog matches the reference dumper's restore
// at pg_dump/mysqldump granularity.
//
// Scope: same-engine only (the oracle is a same-engine restore). Chinook
// (11 tables, clean, independently authored per dialect) is the tractable
// floor; MediaWiki (64 tables, generated-from-one-abstract-schema) is the
// larger real distribution.

package pipeline

import (
	"context"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/migcore"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// corpusDumpParityLine is one flattened divergence, engine-neutral so
// the PG and MySQL legs share the ledger walk without naming the backup
// package's unexported diff/statement types (they stay behind the `:=`
// inference in each run helper, exactly as the kitchen-sink harness does).
type corpusDumpParityLine struct {
	side   string
	key    string
	detail string
}

// corpusAllowlistMatch reports whether key is covered by the corpus
// allowlist and, if so, its reason + citation. Wrapping
// MatchDumpParityAllowlist here keeps the unexported allowlist-entry
// type behind inference; the shared reporter only sees strings.
func corpusAllowlistMatchPG(key string) (reason, citation string, ok bool) {
	e := backup.MatchDumpParityAllowlist(key, backup.DumpParityCorpusAllowlistPG)
	if e == nil {
		return "", "", false
	}
	return e.Reason, e.Citation, true
}

func corpusAllowlistMatchMySQL(key string) (reason, citation string, ok bool) {
	e := backup.MatchDumpParityAllowlist(key, backup.DumpParityCorpusAllowlistMySQL)
	if e == nil {
		return "", "", false
	}
	return e.Reason, e.Citation, true
}

// reportCorpusDumpParity walks the flattened ledger: every divergence is
// either allowlisted (logged; TRIAGE entries banner-marked) or a NEW
// FINDING that fails the leg. Mirrors the kitchen-sink harness's ledger
// walk exactly.
func reportCorpusDumpParity(t *testing.T, corpusFile string, empty bool, lines []corpusDumpParityLine, match func(key string) (reason, citation string, ok bool)) {
	t.Helper()
	if empty {
		t.Logf("%s dump parity: FULL PARITY (no divergences)", corpusFile)
		return
	}
	var unlisted int
	for _, ln := range lines {
		reason, citation, ok := match(ln.key)
		if !ok {
			unlisted++
			t.Errorf("UNLISTED PARITY DIVERGENCE [%s] %s\n  %s", ln.side, ln.key, ln.detail)
			continue
		}
		marker := "ALLOWLISTED"
		if citation == backup.DumpParityTriageCitation {
			marker = "TRIAGE (latent gap under investigation)"
		}
		t.Logf("%s [%s] %s\n  reason: %s\n  citation: %s\n  %s", marker, ln.side, ln.key, reason, citation, ln.detail)
	}
	t.Logf("%s dump parity ledger: %d divergence line(s), %d unlisted", corpusFile, len(lines), unlisted)
	if unlisted > 0 {
		t.Errorf("%s dump parity: %d divergence(s) not covered by the corpus allowlist — each is either a missing documented-degradation entry (cite it) or a latent bug (TRIAGE it and file the finding)", corpusFile, unlisted)
	}
}

// runCorpusDumpParityPG migrates a same-engine PG corpus schema through
// sluice and through pg_dump|psql, then diffs the two targets'
// --schema-only dumps. excludeTables filters tables sluice refuses (e.g.
// partitioned parents) off the sluice leg — the resulting oracle-side
// surplus is expected and covered by the allowlist. createFloor is the
// non-vacuous CREATE-statement floor (both sides must clear it, so a
// comparator that eats statements trips the guard instead of reading as
// parity).
func runCorpusDumpParityPG(t *testing.T, corpusFile string, excludeTables []string, createFloor int) {
	t.Helper()
	ddl := readCorpus(t, corpusFile)

	ctr, sourceDSN, cleanup := startDumpParityPostgres(t)
	defer cleanup()
	applyPGDDL(t, sourceDSN, ddl)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	sluiceDSN, err := buildPGDSN(sourceDSN, "parity_sluice")
	if err != nil {
		t.Fatalf("build sluice-target DSN: %v", err)
	}

	var filter migcore.TableFilter
	if len(excludeTables) > 0 {
		filter, err = migcore.NewTableFilter(nil, excludeTables)
		if err != nil {
			t.Fatalf("build filter: %v", err)
		}
	}
	mig := &Migrator{
		Source:    pgEng,
		Target:    pgEng,
		SourceDSN: sourceDSN,
		TargetDSN: sluiceDSN,
		Filter:    filter,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("%s: sluice PG→PG migrate failed: %v", corpusFile, truncErr(err))
	}

	// Oracle leg: pg_dump | psql entirely inside the container.
	dumpParityExec(t, ctx, ctr,
		"pg_dump -U test --schema-only source_db | psql -q -U test -v ON_ERROR_STOP=1 -d parity_pgdump")

	sluiceStmts := backup.ParseSchemaDump(dumpParitySchemaDump(t, ctx, ctr, "parity_sluice"))
	oracleStmts := backup.ParseSchemaDump(dumpParitySchemaDump(t, ctx, ctr, "parity_pgdump"))

	if n := backup.CountCreateStatements(oracleStmts); n < createFloor {
		t.Fatalf("vacuous-pass guard: %s oracle dump yielded %d CREATE statements; corpus declares >= %d — the comparator is eating statements", corpusFile, n, createFloor)
	}
	if n := backup.CountCreateStatements(sluiceStmts); n < createFloor {
		t.Fatalf("vacuous-pass guard: %s sluice dump yielded %d CREATE statements; corpus declares >= %d — the comparator is eating statements", corpusFile, n, createFloor)
	}

	diff := backup.DiffDumpStatements(sluiceStmts, oracleStmts)
	var lines []corpusDumpParityLine
	for _, s := range diff.OnlyInSluice {
		lines = append(lines, corpusDumpParityLine{"only-in-sluice", s.Key, "stmt: " + s.Body})
	}
	for _, s := range diff.OnlyInOracle {
		lines = append(lines, corpusDumpParityLine{"only-in-oracle", s.Key, "stmt: " + s.Body})
	}
	for _, m := range diff.Mismatched {
		lines = append(lines, corpusDumpParityLine{"mismatch", m.Key, "sluice: " + m.Sluice + "\n  oracle: " + m.Oracle})
	}
	reportCorpusDumpParity(t, corpusFile, diff.Empty(), lines, corpusAllowlistMatchPG)
}

// runCorpusDumpParityMySQL is the MySQL sibling of runCorpusDumpParityPG:
// migrate a same-engine MySQL corpus schema through sluice and through
// mysqldump|mysql, then diff the two targets' --no-data dumps. sqlMode,
// when non-empty, is applied as a leading `SET SESSION sql_mode=...` so a
// corpus using WordPress-class permissive-mode DDL (zero-date defaults)
// loads faithfully. tableFloor/columnFloor are the non-vacuous floors.
func runCorpusDumpParityMySQL(t *testing.T, corpusFile, sqlMode string, tableFloor, columnFloor int) {
	t.Helper()
	ddl := readCorpus(t, corpusFile)
	if sqlMode != "" {
		ddl = "SET SESSION sql_mode='" + sqlMode + "';\n\n" + ddl
	}

	ctr, sourceDSN, cleanup := startDumpParityMySQL(t)
	defer cleanup()
	applyMySQLDDL(t, sourceDSN, ddl)

	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	sluiceDSN, err := buildMySQLDSN(sourceDSN, "parity_sluice")
	if err != nil {
		t.Fatalf("build sluice-target DSN: %v", err)
	}

	mig := &Migrator{
		Source:    mysqlEng,
		Target:    mysqlEng,
		SourceDSN: sourceDSN,
		TargetDSN: sluiceDSN,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("%s: sluice MySQL→MySQL migrate failed: %v", corpusFile, truncErr(err))
	}

	// Oracle leg: mysqldump | mysql entirely inside the container. The
	// header FOREIGN_KEY_CHECKS=0 guard lets a FK-before-referent CREATE
	// TABLE restore cleanly.
	dumpParityMySQLExec(t, ctx, ctr,
		"mysqldump -uroot --no-data --skip-comments source_db | mysql -uroot parity_mysqldump")

	sluiceStmts := backup.ParseMySQLSchemaDump(dumpParityMySQLSchemaDump(t, ctx, ctr, "parity_sluice"))
	oracleStmts := backup.ParseMySQLSchemaDump(dumpParityMySQLSchemaDump(t, ctx, ctr, "parity_mysqldump"))

	if n := backup.CountMySQLTables(oracleStmts); n < tableFloor {
		t.Fatalf("vacuous-pass guard: %s oracle dump yielded %d tables; corpus declares >= %d — the comparator is eating statements", corpusFile, n, tableFloor)
	}
	if n := backup.CountMySQLColumns(oracleStmts); n < columnFloor {
		t.Fatalf("vacuous-pass guard: %s oracle dump yielded %d columns; corpus declares >= %d — the comparator is eating statements", corpusFile, n, columnFloor)
	}
	if n := backup.CountMySQLTables(sluiceStmts); n < tableFloor {
		t.Fatalf("vacuous-pass guard: %s sluice dump yielded %d tables; corpus declares >= %d — the comparator is eating statements", corpusFile, n, tableFloor)
	}
	if n := backup.CountMySQLColumns(sluiceStmts); n < columnFloor {
		t.Fatalf("vacuous-pass guard: %s sluice dump yielded %d columns; corpus declares >= %d — the comparator is eating statements", corpusFile, n, columnFloor)
	}

	diff := backup.DiffDumpStatements(sluiceStmts, oracleStmts)
	var lines []corpusDumpParityLine
	for _, s := range diff.OnlyInSluice {
		lines = append(lines, corpusDumpParityLine{"only-in-sluice", s.Key, "stmt: " + s.Body})
	}
	for _, s := range diff.OnlyInOracle {
		lines = append(lines, corpusDumpParityLine{"only-in-oracle", s.Key, "stmt: " + s.Body})
	}
	for _, m := range diff.Mismatched {
		lines = append(lines, corpusDumpParityLine{"mismatch", m.Key, "sluice: " + m.Sluice + "\n  oracle: " + m.Oracle})
	}
	reportCorpusDumpParity(t, corpusFile, diff.Empty(), lines, corpusAllowlistMatchMySQL)
}

// --- Chinook: the tractable floor (11 tables, independently authored) ---

func TestMigrate_Corpus_DumpParity_Chinook_PGToPG(t *testing.T) {
	runCorpusDumpParityPG(t, "chinook_postgres.ddl.sql", nil, 11)
}

func TestMigrate_Corpus_DumpParity_Chinook_MySQLToMySQL(t *testing.T) {
	runCorpusDumpParityMySQL(t, "chinook_mysql.ddl.sql", "", 11, 40)
}

// --- MediaWiki: the larger real distribution (64 tables, generated from
// one abstract schema). ---
//
// MediaWiki uses short, table-scoped index names (`wl_user` on
// `watchlist`) — exactly what sluice's deliberate pgIndexName
// qualification (GitHub #26; internal/engines/postgres/ddl_emit.go)
// renames on the PG target (`wl_user` -> `watchlist_wl_user`). Under the
// identity (verb+kind+NAME) key those renames diverged EVERY secondary
// index, which is why the corpus could not be wired here before. The
// comparator's Phase-2 body-equivalence pairing (Finding B;
// backup.pairRenamedIndexes) now recognizes a renamed-but-identical index
// as the same index and cancels it — WITHOUT masking a genuinely dropped
// index (a body signature that finds no partner still surfaces). That is
// the payoff that makes this leg tractable; a blanket `CREATE INDEX *`
// allowlist entry would have masked real drops, which is precisely what
// the pairing pass avoids.
func TestMigrate_Corpus_DumpParity_MediaWiki_PGToPG(t *testing.T) {
	runCorpusDumpParityPG(t, "mediawiki_postgres.ddl.sql", nil, 60)
}
