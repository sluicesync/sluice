//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The migrate-then-diff round trip (Bug 234).
//
// This is the operator's own assertion, and the reason it exists as an
// INTEGRATION test rather than a unit one: the independent expected
// value is the TARGET CATALOG's read-back of a schema `sluice migrate`
// itself just created. Nothing in the diff path produces that read-back,
// so a symmetric bug in the expected-side builder cannot make it agree.
//
// Bug 234 stayed invisible because every `schema diff` test either used a
// same-engine pair (both sides spell the primary key the same way) or
// hand-built the target DDL to match what the assertions expected — see
// TestDiff_PostgresToMySQL, whose fixture carries a primary key on both
// sides and never looks at the index slices. Migrating first is what
// removes the hand.
//
// Both directions are asserted for each pair, because a diff that stops
// reporting real drift to silence a false positive is far worse than the
// false positive:
//
//	CLEAN — immediately after migrate, no drift (the exit-0 shape).
//	DIRTY — after dropping a real index, and separately a real primary
//	        key, on the target, that drift is still reported.

package pipeline

import (
	"bytes"
	"context"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	irdiff "sluicesync.dev/sluice/internal/ir/diff"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// runDiffForDrift runs the differ over an engine pair and returns the
// computed diff. Text format, so the renderer is exercised too — a panic
// or a malformed suggestion line for a role-matched primary key would
// surface here rather than only in the JSON shape.
func runDiffForDrift(ctx context.Context, t *testing.T, srcEngine, tgtEngine, srcDSN, tgtDSN string) *irdiff.SchemaDiff {
	t.Helper()
	src, ok := engines.Get(srcEngine)
	if !ok {
		t.Fatalf("%s engine not registered", srcEngine)
	}
	tgt, ok := engines.Get(tgtEngine)
	if !ok {
		t.Fatalf("%s engine not registered", tgtEngine)
	}
	var buf bytes.Buffer
	d := &Differ{
		Source:    src,
		Target:    tgt,
		SourceDSN: srcDSN,
		TargetDSN: tgtDSN,
		Format:    "text",
		Out:       &buf,
	}
	diff, err := d.Run(ctx)
	if err != nil {
		t.Fatalf("Differ.Run(%s -> %s): %v\noutput:\n%s", srcEngine, tgtEngine, err, buf.String())
	}
	if diff == nil {
		t.Fatalf("Differ.Run(%s -> %s) returned a nil diff", srcEngine, tgtEngine)
	}
	return diff
}

// assertNoIndexDrift is the narrower CLEAN assertion the mysql→postgres
// direction can currently make, and the narrowness is stated rather than
// implied.
//
// That direction has ONE axis of phantom drift left and it is not this
// one: [translate.retargetRuleFor] has no rule table for
// mysql→postgres — [translate.HasStorageShapeMapping] reports false for
// the pair and the migrate pre-create gate WARNs and skips its whole
// column compare because of it — so a MySQL `TEXT` lands as PG `TEXT`
// and reads back as `Text[long]` against an expected `Text[regular]`.
// Closing that means writing the reverse-direction rule table, which is
// its own chunk; until it lands, `schema diff` on a mysql→postgres pair
// still exits 1 over column TYPES. The index-name axis this file exists
// for is closed in both directions, and the postgres→mysql direction —
// the one Bug 234 was filed from — asserts the full exit-0 shape.
func assertNoIndexDrift(t *testing.T, diff *irdiff.SchemaDiff) {
	t.Helper()
	for _, td := range diff.TablesMismatched {
		if len(td.IndexesMissing) > 0 || len(td.IndexesExtra) > 0 || len(td.IndexesMismatched) > 0 {
			t.Errorf("table %q: index drift against a target migrate just created — missing=%v extra=%v mismatched=%+v",
				td.Name, td.IndexesMissing, td.IndexesExtra, td.IndexesMismatched)
		}
	}
	if len(diff.TablesMissing) > 0 || len(diff.TablesExtra) > 0 {
		t.Errorf("table-level drift against a target migrate just created: missing=%v extra=%v",
			diff.TablesMissing, diff.TablesExtra)
	}
}

// TestSchemaDiffAfterMigrate_PostgresToMySQL is the reported repro at
// the reported minimum size: a two-column table with nothing interesting
// in it. PostgreSQL names the key `plain_pkey`, MySQL calls it
// `PRIMARY`, and before the role match that pair alone produced
// `1 missing index, 1 extra index` and exit 1 on every run.
func TestSchemaDiffAfterMigrate_PostgresToMySQL(t *testing.T) {
	pgSource, _, pgCleanup := startPostgres(t)
	defer pgCleanup()
	_, mysqlTarget, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()

	applyPGDDL(t, pgSource, `
		CREATE TABLE plain (id INT PRIMARY KEY, v TEXT);
		INSERT INTO plain VALUES (1, 'a');

		CREATE TABLE keyed (
			id    INT PRIMARY KEY,
			email VARCHAR(120) NOT NULL
		);
		CREATE INDEX idx_email ON keyed (email);
	`)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	mig := &Migrator{Source: pgEng, Target: mysqlEng, SourceDSN: pgSource, TargetDSN: mysqlTarget}
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run: %v", err)
	}

	// CLEAN: the whole point — `migrate` then `schema diff` must agree.
	diff := runDiffForDrift(ctx, t, "postgres", "mysql", pgSource, mysqlTarget)
	if diff.HasChanges() {
		t.Fatalf("schema diff reported drift against a target migrate just created: %+v", diff.TablesMismatched)
	}

	// DIRTY (secondary index): a genuinely dropped index is still drift.
	applyMySQLDDL(t, mysqlTarget, "DROP INDEX `idx_email` ON `keyed`")
	diff = runDiffForDrift(ctx, t, "postgres", "mysql", pgSource, mysqlTarget)
	if !diff.HasChanges() {
		t.Fatal("schema diff reported NO drift after a real index was dropped on the target")
	}
	td := findTableDiff(*diff, "keyed")
	if td == nil || len(td.IndexesMissing) != 1 || td.IndexesMissing[0] != "idx_email" {
		t.Fatalf("expected idx_email in indexes_missing; got %+v", td)
	}

	// DIRTY (primary key): the harder half — the object that is now
	// matched by ROLE has to remain reportable when it is genuinely
	// absent.
	applyMySQLDDL(t, mysqlTarget, "ALTER TABLE `plain` DROP PRIMARY KEY")
	diff = runDiffForDrift(ctx, t, "postgres", "mysql", pgSource, mysqlTarget)
	td = findTableDiff(*diff, "plain")
	if td == nil || len(td.IndexesMissing) != 1 || td.IndexesMissing[0] != "plain_pkey" {
		t.Fatalf("expected plain_pkey in indexes_missing after DROP PRIMARY KEY; got %+v", td)
	}
}

// TestSchemaDiffAfterMigrate_MySQLToPostgres is the sibling direction,
// and it carries the OTHER half of the index-name axis: the Postgres
// writer table-prefixes a secondary index name (`idx_email` becomes
// `keyed_idx_email`), so before the expected side mirrored that
// transformation this pair reported one missing + one extra index for
// every non-prefixed index on every table, on top of the primary-key
// pair. `keyed_created_idx` is the control — already table-prefixed, so
// it passes through untouched and proves the rule is a transformation
// and not a blanket rename.
func TestSchemaDiffAfterMigrate_MySQLToPostgres(t *testing.T) {
	mysqlSource, _, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()
	_, pgTarget, pgCleanup := startPostgres(t)
	defer pgCleanup()

	applyMySQLDDL(t, mysqlSource, `
		CREATE TABLE plain (
			id INT NOT NULL PRIMARY KEY,
			v  TEXT
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

		CREATE TABLE keyed (
			id       INT NOT NULL PRIMARY KEY,
			email    VARCHAR(120) NOT NULL,
			nickname VARCHAR(120) NOT NULL,
			KEY idx_email (email),
			KEY keyed_nickname_idx (nickname)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`)

	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	mig := &Migrator{Source: mysqlEng, Target: pgEng, SourceDSN: mysqlSource, TargetDSN: pgTarget}
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run: %v", err)
	}

	diff := runDiffForDrift(ctx, t, "mysql", "postgres", mysqlSource, pgTarget)
	assertNoIndexDrift(t, diff)

	// DIRTY: the PREFIXED name is the one the target holds, so it is the
	// one the report must name — an operator who copy-pastes it has to
	// get an index that makes the next diff clean.
	applyPGDDL(t, pgTarget, `DROP INDEX "keyed_idx_email"`)
	diff = runDiffForDrift(ctx, t, "mysql", "postgres", mysqlSource, pgTarget)
	if !diff.HasChanges() {
		t.Fatal("schema diff reported NO drift after a real index was dropped on the target")
	}
	td := findTableDiff(*diff, "keyed")
	if td == nil || len(td.IndexesMissing) != 1 || td.IndexesMissing[0] != "keyed_idx_email" {
		t.Fatalf("expected keyed_idx_email in indexes_missing; got %+v", td)
	}

	// DIRTY (primary key), Postgres side. The expected side is the MySQL
	// source's spelling, so the report names `PRIMARY`.
	applyPGDDL(t, pgTarget, `ALTER TABLE "plain" DROP CONSTRAINT "plain_pkey"`)
	diff = runDiffForDrift(ctx, t, "mysql", "postgres", mysqlSource, pgTarget)
	td = findTableDiff(*diff, "plain")
	if td == nil || len(td.IndexesMissing) != 1 || td.IndexesMissing[0] != "PRIMARY" {
		t.Fatalf("expected PRIMARY in indexes_missing after DROP CONSTRAINT; got %+v", td)
	}
}
