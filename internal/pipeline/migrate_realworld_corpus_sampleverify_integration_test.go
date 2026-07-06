//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Real-world corpus × data fidelity (roadmap item 52 finishing touch #2
// — "sample-row verify on the _DryRun legs"). The iteration-1..4 corpus
// legs are schema-only (Migrator{DryRun:true}): they prove sluice
// reads+plans+emits the real schema, but move no data. These legs
// upgrade a subset to a REAL same-engine migrate of a corpus schema
// carrying a small deterministic seeded row sample, then run `sluice
// verify` at both count depth and sample depth (row-content hashes) — so
// the corpus exercises data fidelity, not just schema translation.
//
// Same-engine on purpose: verify --depth=sample compares server-side row
// hashes and is same-engine-only (verify.go); count depth is the
// cross-engine-safe rollup, exercised here too. Chinook is the corpus
// member (small, clean, every table PK'd — so sample depth actually
// engages). The seed is intentionally tiny (a couple of PK'd parent
// tables, a handful of rows): the corpus tests fidelity, not throughput
// (item 52 gotcha 3).
//
// Vacuous-pass guard: the harness asserts the seeded table reports a
// non-zero SampleSize on the sample pass, so a verify that silently
// sampled nothing (e.g. PK not detected) fails loudly instead of reading
// as a clean pass.

package pipeline

import (
	"bytes"
	"testing"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// Deterministic seeds for two PK'd Chinook parent tables (no child rows,
// so no FK-referent seeding is needed). Values are the canonical Chinook
// first rows; the exact values don't matter, only that source and target
// carry the same bytes after a faithful migrate.
const (
	chinookMySQLSeed = "" +
		"INSERT INTO `Artist` (`ArtistId`, `Name`) VALUES " +
		"(1,'AC/DC'),(2,'Accept'),(3,'Aerosmith'),(4,'Alanis Morissette'),(5,'Alice In Chains');\n" +
		"INSERT INTO `Genre` (`GenreId`, `Name`) VALUES " +
		"(1,'Rock'),(2,'Jazz'),(3,'Metal'),(4,'Alternative & Punk'),(5,'Rock And Roll');\n"

	chinookPGSeed = "" +
		"INSERT INTO artist (artist_id, name) VALUES " +
		"(1,'AC/DC'),(2,'Accept'),(3,'Aerosmith'),(4,'Alanis Morissette'),(5,'Alice In Chains');\n" +
		"INSERT INTO genre (genre_id, name) VALUES " +
		"(1,'Rock'),(2,'Jazz'),(3,'Metal'),(4,'Alternative & Punk'),(5,'Rock And Roll');\n"

	corpusSeedRows = 5
)

// assertCorpusVerifyClean runs a count-depth then a sample-depth verify
// over the freshly-migrated same-engine pair and asserts both are clean.
// seededTable/seededRows pin the seeded table's counts and — on the
// sample pass — its SampleSize, so the check can't pass vacuously (a
// verify that sampled zero rows everywhere would otherwise read clean).
func assertCorpusVerifyClean(t *testing.T, eng ir.Engine, srcDSN, tgtDSN, seededTable string, seededRows int) {
	t.Helper()

	// Count depth (the cross-engine-safe rollup).
	var countBuf bytes.Buffer
	vc := &Verifier{Source: eng, Target: eng, SourceDSN: srcDSN, TargetDSN: tgtDSN, Out: &countBuf}
	rc, err := vc.Run(ctx2min(t))
	if err != nil {
		t.Fatalf("verify (count): %v", err)
	}
	if rc.HasMismatch() {
		t.Fatalf("verify (count): unexpected mismatch after a faithful corpus migrate; summary=%+v\n%s", rc.Summary, countBuf.String())
	}
	assertSeededTableCounts(t, rc, seededTable, seededRows)

	// Sample depth (server-side row-content hashes; same-engine only).
	var sampleBuf bytes.Buffer
	vs := &Verifier{Source: eng, Target: eng, SourceDSN: srcDSN, TargetDSN: tgtDSN, Depth: VerifyDepthSample, Out: &sampleBuf}
	rs, err := vs.Run(ctx2min(t))
	if err != nil {
		t.Fatalf("verify (sample): %v", err)
	}
	if rs.HasMismatch() {
		t.Fatalf("verify (sample): unexpected row-content mismatch after a faithful corpus migrate; summary=%+v\n%s", rs.Summary, sampleBuf.String())
	}

	// Vacuous-pass guard: the seeded table MUST have actually sampled its
	// rows, else "clean" means "compared nothing".
	var sampled int
	for _, tr := range rs.Tables {
		if tr.Name == seededTable {
			sampled = tr.SampleSize
		}
	}
	if sampled != seededRows {
		t.Fatalf("verify (sample): seeded table %q sampled %d rows; want %d — sample depth did not engage (PK not detected?), a clean verdict here would be vacuous\n%s",
			seededTable, sampled, seededRows, sampleBuf.String())
	}
	t.Logf("corpus sample-verify: count+sample clean; seeded %q pinned at %d rows both sides, sample depth engaged", seededTable, seededRows)
}

func assertSeededTableCounts(t *testing.T, r *VerifyResult, seededTable string, want int) {
	t.Helper()
	for _, tr := range r.Tables {
		if tr.Name != seededTable {
			continue
		}
		if tr.SourceRowCount != int64(want) || tr.TargetRowCount != int64(want) {
			t.Fatalf("seeded table %q: source=%d target=%d; want %d/%d",
				seededTable, tr.SourceRowCount, tr.TargetRowCount, want, want)
		}
		return
	}
	t.Fatalf("seeded table %q not found in verify result — migrate did not carry it", seededTable)
}

// TestMigrate_Corpus_SampleVerify_Chinook_MySQLToMySQL migrates the real
// Chinook MySQL schema (with a small seed) MySQL→MySQL and verifies row
// counts + sampled row content.
func TestMigrate_Corpus_SampleVerify_Chinook_MySQLToMySQL(t *testing.T) {
	ddl := readCorpus(t, "chinook_mysql.ddl.sql")
	src, tgt, cleanup := startMySQL(t)
	defer cleanup()

	applyMySQLDDL(t, src, ddl)
	applyMySQLDDL(t, src, chinookMySQLSeed)
	corpusAssertTables(t, "mysql", src, 11)

	myEng, _ := engines.Get("mysql")
	mig := &Migrator{Source: myEng, Target: myEng, SourceDSN: src, TargetDSN: tgt}
	if err := mig.Run(ctx2min(t)); err != nil {
		t.Fatalf("Chinook MySQL→MySQL migrate: %v", truncErr(err))
	}

	// Artist is PK'd (ArtistId) and seeded with corpusSeedRows rows.
	assertCorpusVerifyClean(t, myEng, src, tgt, "Artist", corpusSeedRows)
}

// TestMigrate_Corpus_SampleVerify_Chinook_PGToPG is the PG sibling.
func TestMigrate_Corpus_SampleVerify_Chinook_PGToPG(t *testing.T) {
	ddl := readCorpus(t, "chinook_postgres.ddl.sql")
	src, tgt, cleanup := startPostgres(t)
	defer cleanup()

	applyPGDDL(t, src, ddl)
	applyPGDDL(t, src, chinookPGSeed)
	corpusAssertTables(t, "postgres", src, 11)

	pgEng, _ := engines.Get("postgres")
	mig := &Migrator{Source: pgEng, Target: pgEng, SourceDSN: src, TargetDSN: tgt}
	if err := mig.Run(ctx2min(t)); err != nil {
		t.Fatalf("Chinook PG→PG migrate: %v", truncErr(err))
	}

	// artist is PK'd (artist_id) and seeded with corpusSeedRows rows.
	assertCorpusVerifyClean(t, pgEng, src, tgt, "artist", corpusSeedRows)
}
