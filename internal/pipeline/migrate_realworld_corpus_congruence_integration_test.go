//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Real-world schema corpus — RETIRED congruence oracle.
//
// This file once emitted sluice's MySQL→PG (and PG→MySQL) translation
// of the fetched expert-authored corpus schemas and asserted the
// emission was congruent with the OTHER-engine authored file that ships
// in the same corpus member (Chinook, MediaWiki, Joomla — the three DBs
// that ship BOTH dialects). That upstream-pair oracle has been retired.
// The subtests remain as documented t.Skip markers so the "why" is
// discoverable in test output; the DBs themselves still run in the
// migrate/DryRun corpus (migrate_realworld_corpus_integration_test.go)
// and the same-engine DumpParity corpus
// (migrate_realworld_corpus_dumpparity_integration_test.go).
//
// WHY RETIRED (all three reasons apply):
//
//  1. The pairs diverge by UPSTREAM AUTHORING CONVENTION, not by any
//     sluice error, so there is no congruent oracle to assert against:
//       - Chinook: MySQL PascalCase identifiers (Album, InvoiceLine) vs
//         PG snake_case (album, invoice_line) — the same 11 entities
//         named differently, so every table reads as missing+extra.
//       - MediaWiki: "both generated from one abstract schema" does NOT
//         imply column congruence — MediaWiki's PG adapter renders the
//         abstract binary/blob type as TEXT and its MW-timestamp type as
//         TIMESTAMPTZ, while the MySQL side uses VARBINARY(n). sluice
//         faithfully carries MySQL VARBINARY → PG bytea, so ~150 columns
//         diverge from the authored PG side by upstream design.
//       - Joomla: the PG author names indexes with idx_ prefixes, uses
//         different index names (cat_idx vs tag_idx), and adds a PG-only
//         functional index (lower(email)) the MySQL author never wrote.
//     sluice cannot and should not reconcile an upstream inconsistency.
//
//  2. GOLDEN-OUTPUT comparison (capture sluice's own emit once, diff
//     against that) is LICENSING-BLOCKED: sluice is Apache-2.0, but
//     Joomla/MediaWiki/WordPress are GPL-2.0+. A committed golden is
//     sluice's translation OF a GPL schema = a GPL-derivative committed
//     into an Apache repo. It also contradicts the corpus design: the
//     dir's .gitignore excludes every fetched schema (only MANIFEST.md +
//     fetch.sh are committed) precisely to keep upstream-licensed
//     content out of the tree. Nothing derived from the fetched schemas
//     may be committed.
//
//  3. Cross-engine translation is ALREADY validated authoritatively
//     elsewhere: migrate_cross_integration_test.go exercises real
//     MySQL↔PG round-trips on testcontainers with owned, Apache-clean
//     fixtures, and same-engine fidelity on these very corpus schemas is
//     covered by the DumpParity corpus. The retired upstream-pair oracle
//     added no coverage those don't provide — only false drift-red.
//
// See ./testdata/real-world-corpus/MANIFEST.md ("Why DumpParity-only
// for Chinook / MediaWiki / Joomla") for the same rationale in the
// corpus docs.

package pipeline

import "testing"

// skipRetiredCongruenceOracle marks a subtest as intentionally retired.
// The rationale lives in this file's package-doc block above; the one
// authoritative pointer is repeated here so `go test -run Congruence -v`
// output is self-explanatory.
func skipRetiredCongruenceOracle(t *testing.T) {
	t.Helper()
	t.Skip("retired: upstream corpus pairs diverge by authoring convention " +
		"(not sluice error); golden-output is licensing-blocked (Apache vs " +
		"GPL + the gitignore-all-schemas corpus design); cross-engine " +
		"translation is validated by migrate_cross_integration_test.go and " +
		"same-engine fidelity by the DumpParity corpus. See file-top doc + " +
		"testdata/real-world-corpus/MANIFEST.md.")
}

func TestMigrate_Corpus_Chinook_Congruence_MySQLEmittedVsAuthoredPG(t *testing.T) {
	skipRetiredCongruenceOracle(t)
}

func TestMigrate_Corpus_MediaWiki_Congruence_MySQLEmittedVsAuthoredPG(t *testing.T) {
	skipRetiredCongruenceOracle(t)
}

func TestMigrate_Corpus_Joomla_Congruence_MySQLEmittedVsAuthoredPG(t *testing.T) {
	skipRetiredCongruenceOracle(t)
}

func TestMigrate_Corpus_Chinook_Congruence_PGEmittedVsAuthoredMySQL(t *testing.T) {
	skipRetiredCongruenceOracle(t)
}

func TestMigrate_Corpus_MediaWiki_Congruence_PGEmittedVsAuthoredMySQL(t *testing.T) {
	skipRetiredCongruenceOracle(t)
}

func TestMigrate_Corpus_Joomla_Congruence_PGEmittedVsAuthoredMySQL(t *testing.T) {
	skipRetiredCongruenceOracle(t)
}
