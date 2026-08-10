//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Roadmap item 156 — a DOMAIN's CHECKs versus `schema diff`'s
// CheckConstraints comparison, PG → MySQL. CLOSED; this file was the
// characterization pin and is now the regression pin, inverted as its
// own instructions required.
//
// # The shape it closed
//
// `schema diff` builds its expected side from the SOURCE schema, and a
// domain's CHECKs ride the WRAPPER (`ir.Domain.Checks`), never
// `ir.Table.CheckConstraints` — which is the only place `diffChecks`
// looks. MySQL's emitter meanwhile turns the translatable ones into
// inline table-level CHECK clauses at migrate time
// (emitTableDefWithDomainChecks, via ir.DomainOf). So the target really
// did hold constraints the expected side had nowhere to put, and every
// one reported as ChecksExtra on a table sluice itself created.
//
// # How it is closed, and why it is not a suppression
//
// The MySQL SchemaWriter now answers [ir.EmittedCheckPredictor]: it
// states, by CALLING the same translator the emitter calls, which
// clauses it would inline. The diff attaches those to the expected side
// marked SluiceEmitted and matches them by CANONICALIZED EXPRESSION —
// not by name, which was the measured obstacle (MySQL numbers an inline
// CHECK `<table>_chk_N` by position, unpredictable from the source).
//
// Matching on the predicate is what keeps this from blinding the
// command, and the DIRTY halves below are the evidence: a DROPPED domain
// CHECK still reports missing, and a TAMPERED one reports as both
// missing and extra. A name-only match would have accepted both.
//
// # The independent expected value, named (the 2026-08-01 rule)
//
// Two, and neither is sluice's own diff. The anti-vacuity half reads
// information_schema on the MySQL target directly — MySQL's own catalog,
// so "the CHECKs landed" is the server's answer. The drift half is then
// `schema diff`'s report about those same constraints.
package pipeline

import (
	"context"
	"database/sql"
	"io"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	irdiff "sluicesync.dev/sluice/internal/ir/diff"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// domainCheckSeedDDL declares BOTH translatable DOMAIN CHECK shapes
// MySQL's emitter knows (domain_check_translate.go): the regex arm and
// the range arm. One would not do — they are separate translator arms
// producing separate clauses, and a comparison defect could plausibly
// reach one shape and not the other.
//
// An apostrophe-carrying pattern is deliberately NOT in this fixture,
// and the reason is a finding rather than an omission: MySQL's
// SchemaReader mangles it. information_schema renders such a CHECK_CLAUSE
// DOUBLE-escaped (`_utf8mb4\'^o\\\'brien$\'`), and
// normalizeMySQLExpressionText's convertMySQLEscapedApostrophes is a
// blind `\'`→`'` replace that does not un-double the backslashes, so the
// IR carries a malformed expression for any CHECK — or generated-column
// body — containing an apostrophe. That is a pre-existing SchemaReader
// defect with a wider blast radius than this chunk (the same
// normalization serves GENERATION_EXPRESSION), it needs its own escaping
// matrix, and it is filed rather than patched blind. Its effect HERE is
// loud and in the safe direction: such a constraint reports as one
// missing plus one extra rather than being silently matched.
// TestDomainRegexApostrophePredicateIsEnforcedAsWritten below pins the
// EMITTER half against the server itself, which is the half this chunk
// touched.
//
// `dc_optout` carries an UN-translatable check (a function call), which
// the emitter drops with a WARN. It is here as the control: its column
// must produce no target-side CHECK at all, so a run where every column
// reports drift is distinguishable from one where the emitter simply
// emitted nothing — and a predictor that hallucinated the dropped one
// would produce a permanent missing-on-target line.
const domainCheckSeedDDL = `
	CREATE DOMAIN dc_email  AS text    CHECK (VALUE ~ '^[a-z]+@example[.]com$');
	CREATE DOMAIN dc_pct    AS integer CHECK (VALUE >= 0 AND VALUE <= 100);
	CREATE DOMAIN dc_optout AS text    CHECK (length(VALUE) > 5);

	CREATE TABLE dcheck (
		id     BIGINT PRIMARY KEY,
		email  dc_email,
		pct    dc_pct,
		optout dc_optout
	);
`

// TestDiff_PGToMySQL_DomainChecksAreReconciled_Item156 migrates a
// CHECK-carrying DOMAIN schema to MySQL and asserts what `schema diff`
// says about the table sluice itself just created: nothing.
func TestDiff_PGToMySQL_DomainChecksAreReconciled_Item156(t *testing.T) {
	pgSource, _, pgCleanup := startPostgresWithExtensions(t, nil)
	defer pgCleanup()
	_, mysqlTarget, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()

	applyPGDDL(t, pgSource, domainCheckSeedDDL)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	myEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	m := &Migrator{
		Source: pgEng, Target: myEng,
		SourceDSN: pgSource, TargetDSN: mysqlTarget,
		MigrationID: "domain-check-diff",
	}
	if err := m.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run PG→MySQL: %v", err)
	}

	// ---- Anti-vacuity: the target must actually HOLD the checks. ----
	//
	// Asked of MySQL's catalog, not of sluice. If this is zero the rest
	// of the test proves nothing — an emitter that dropped every CHECK
	// would produce a clean diff for the wrong reason, and that is
	// exactly the shape that makes a gate worse than none.
	landed := mysqlCheckConstraintCount(t, ctx, mysqlTarget, "dcheck")
	if landed != 2 {
		t.Fatalf("information_schema reports %d CHECK constraint(s) on `dcheck`; want exactly 2 — the "+
			"regex domain and the range domain land, the un-translatable `dc_optout` one does not. "+
			"Any other count means the fixture no longer exercises what it claims: at 0 the clean-diff "+
			"assertion below would be comparing two empty sets, and at 3 the emitter has started "+
			"translating a check shape this pin's reasoning did not account for", landed)
	}

	// ---- CLEAN: the whole point of the command. ----
	diff := runDomainCheckDiff(ctx, t, pgEng, myEng, pgSource, mysqlTarget)
	if diff.HasChanges() {
		t.Fatalf("schema diff reported drift against a MySQL target migrate just created: %s\n%+v\n\n"+
			"Item 156 is the class this pin guards: a DOMAIN's translatable CHECKs land as auto-named "+
			"table-level CHECKs, and the expected side has to know sluice emits them",
			diff.Summary(), diff.TablesMismatched)
	}

	// ---- DIRTY 1: a DROPPED domain CHECK is still real drift. ----
	//
	// This is the assertion that separates a fix from a suppression. The
	// constraint enforces a value range the source enforces; a target
	// that stopped enforcing it accepts rows the source rejects.
	applyMySQLDDL(t, mysqlTarget, "ALTER TABLE `dcheck` DROP CHECK `dcheck_chk_2`")
	diff = runDomainCheckDiff(ctx, t, pgEng, myEng, pgSource, mysqlTarget)
	td := findTableDiff(*diff, "dcheck")
	if td == nil || len(td.ChecksMissing) != 1 {
		t.Fatalf("a DROPPED DOMAIN CHECK went unreported — the expected side is matching it against nothing, "+
			"or the emitted-check attachment has become a blanket suppression of target-side CHECKs: %+v", td)
	}
	t.Logf("dropped-check report: missing=%v", td.ChecksMissing)

	// ---- DIRTY 2: a TAMPERED domain CHECK is still real drift. ----
	//
	// Re-added under the same auto-name with a WIDER range. A name-only
	// match — the shape roadmap item 156 rejected on exactly this ground
	// — would report the target in sync.
	applyMySQLDDL(t, mysqlTarget, "ALTER TABLE `dcheck` ADD CONSTRAINT `dcheck_chk_2` CHECK (`pct` >= 0 AND `pct` <= 500)")
	diff = runDomainCheckDiff(ctx, t, pgEng, myEng, pgSource, mysqlTarget)
	td = findTableDiff(*diff, "dcheck")
	if td == nil || len(td.ChecksMissing) != 1 || len(td.ChecksExtra) != 1 {
		t.Fatalf("a WIDENED DOMAIN CHECK reported as in sync. The comparison is matching sluice-emitted "+
			"constraints by NAME rather than by predicate, so a target that accepts pct=500 while the source "+
			"refuses it looks clean: %+v", td)
	}
	t.Logf("tampered-check report: missing=%v extra=%v", td.ChecksMissing, td.ChecksExtra)
}

// TestDomainRegexApostrophePredicateIsEnforcedAsWritten is the
// independent evidence for the PG→MySQL DOMAIN-regex escaping fix, and
// the independent expected value it names is the SERVER'S OWN
// ENFORCEMENT — not a read-back, not sluice's diff, not a golden string.
//
// PG hands a DOMAIN CHECK body out through pg_get_constraintdef, so an
// apostrophe inside the pattern arrives DOUBLED (it is inside a SQL
// string literal). Re-quoting for MySQL without un-doubling compounded
// the escaping and landed a regex requiring TWO apostrophes: the target
// CHECK then REFUSED `o'brien`, a value the source's DOMAIN accepts.
// Shipped that way since v0.97.0.
//
// An INSERT is the right instrument here for a reason worth stating: the
// read-back path cannot serve as evidence, because MySQL's SchemaReader
// mangles an apostrophe-carrying CHECK_CLAUSE (see domainCheckSeedDDL's
// note). Asking the server whether it accepts the row rides neither the
// reader nor the emitter's own rendering.
//
// Both directions: the legal value must be ACCEPTED, and an illegal one
// must still be REFUSED — an emitter that dropped the CHECK entirely
// would satisfy the first half alone.
func TestDomainRegexApostrophePredicateIsEnforcedAsWritten(t *testing.T) {
	pgSource, _, pgCleanup := startPostgres(t)
	defer pgCleanup()
	_, mysqlTarget, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()

	applyPGDDL(t, pgSource, `
		CREATE DOMAIN dc_quote AS text CHECK (VALUE ~ '^o''brien$');
		CREATE TABLE quoted (id BIGINT PRIMARY KEY, who dc_quote);
	`)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	myEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	m := &Migrator{
		Source: pgEng, Target: myEng,
		SourceDSN: pgSource, TargetDSN: mysqlTarget,
		MigrationID: "domain-check-apostrophe",
	}
	if err := m.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run PG→MySQL: %v", err)
	}

	// Anti-vacuity: the CHECK has to be there at all, or both assertions
	// below pass for the wrong reason.
	if landed := mysqlCheckConstraintCount(t, ctx, mysqlTarget, "quoted"); landed != 1 {
		t.Fatalf("information_schema reports %d CHECK constraint(s) on `quoted`; want exactly 1", landed)
	}

	db, err := sql.Open("mysql", mysqlTarget)
	if err != nil {
		t.Fatalf("open mysql target: %v", err)
	}
	defer func() { _ = db.Close() }()

	if _, err := db.ExecContext(ctx, "INSERT INTO `quoted` (id, who) VALUES (1, ?)", "o'brien"); err != nil {
		t.Fatalf("the MySQL target REFUSED a value the source DOMAIN accepts: %v\n\n"+
			"The translated CHECK is enforcing a DIFFERENT predicate than the source's — the Bug 113 shape "+
			"the DOMAIN-check translator exists to prevent, arriving through the escaping rather than "+
			"through the grammar", err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO `quoted` (id, who) VALUES (2, ?)", "someone-else"); err == nil {
		t.Fatal("the MySQL target ACCEPTED a value the source DOMAIN rejects — the CHECK is not being " +
			"enforced at all, which would make the accept-half above meaningless")
	}
}

// runDomainCheckDiff runs the differ over the PG→MySQL pair and returns
// the diff. JSON format to keep the log quiet; the text renderer is
// exercised by the diff-after-migrate suite.
func runDomainCheckDiff(ctx context.Context, t *testing.T, src, tgt ir.Engine, srcDSN, tgtDSN string) *irdiff.SchemaDiff {
	t.Helper()
	d := &Differ{
		Source: src, Target: tgt,
		SourceDSN: srcDSN, TargetDSN: tgtDSN,
		Format: "json",
		Out:    io.Discard,
	}
	diff, err := d.Run(ctx)
	if err != nil {
		t.Fatalf("Differ.Run: %v", err)
	}
	return diff
}

// mysqlCheckConstraintCount asks MySQL's own catalog how many CHECK
// constraints a table carries. Deliberately not routed through sluice's
// SchemaReader: this is the independent evidence the diff assertion
// rests on, and reading it with the same component under test would put
// both halves on one producer.
func mysqlCheckConstraintCount(t *testing.T, ctx context.Context, dsn, table string) int {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open mysql target: %v", err)
	}
	defer func() { _ = db.Close() }()

	const q = `
		SELECT COUNT(*)
		FROM   information_schema.TABLE_CONSTRAINTS
		WHERE  TABLE_SCHEMA = DATABASE()
		  AND  TABLE_NAME = ?
		  AND  CONSTRAINT_TYPE = 'CHECK'`
	var n int
	if err := db.QueryRowContext(ctx, q, table).Scan(&n); err != nil {
		t.Fatalf("count target CHECK constraints: %v", err)
	}
	return n
}
