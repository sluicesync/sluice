//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Roadmap item 156 — a DOMAIN's CHECKs versus `schema diff`'s
// CheckConstraints comparison, PG → MySQL.
//
// `schema diff` builds its expected side with
// translate.RetargetForShapeCompare, which FLATTENS an ir.Domain to its
// base type — correct for the column's storage shape, and the whole
// point of item 153. But a domain's CHECKs ride the wrapper, and MySQL's
// emitter turns the translatable ones into inline table-level CHECK
// clauses at migrate time (emitTableDefWithDomainChecks, via
// ir.DomainOf). So the target really does hold constraints the flattened
// expected side cannot carry, and diffChecks reports every target-side
// check with no expected-side match as ChecksExtra.
//
// This is PRE-EXISTING, not an item-153 regression: the un-flattened
// wrapper never matched the table-level constraint either, because the
// expected side carried the CHECK inside ir.Domain.Checks and never in
// ir.Table.CheckConstraints, which is the only place diffChecks looks.
//
// # Why item 153's fixture could not have caught it
//
// retargetShapeSeedDDL's domains are deliberately CHECK-FREE — its own
// comment says so, for an unrelated reason (ir.Domain.String() prints
// the check count). A check-free domain emits no inline CHECK, so that
// fixture's target has nothing extra to report. This file supplies the
// CHECK-carrying half.
//
// # THIS FILE PINS A DEFECT, NOT A CONTRACT
//
// Read the assertion below carefully before trusting it: it asserts that
// `schema diff` DOES report the phantom drift, because on `main` today
// it does — ground-truthed 2026-08-07 against a real PG 16 + MySQL 8.0
// pair, which reported `dcheck_chk_1` and `dcheck_chk_2` as extra on a
// target sluice had just created. It is a characterization pin so the
// behaviour cannot drift in EITHER direction unobserved while item 156
// is open, and it must be INVERTED (to `len(ChecksExtra) == 0`) by
// whoever closes that item — a green run here after the fix is the
// signal that this file was not updated.
//
// # Why it is not fixed in the same pass that found it
//
// Two independent obstacles, both measured rather than assumed, and
// together they make this a chunk with a design decision rather than a
// hunk:
//
//   - NAMES. diffChecks matches by name. MySQL auto-names the emitter's
//     inline clause `<table>_chk_N`, numbered by position, so the
//     expected side cannot predict it. Making it predictable means
//     naming the constraint in emitTableDefWithDomainChecks — a DDL
//     BEHAVIOUR change that re-partitions every already-migrated target
//     (an old target's `dcheck_chk_1` then reads as extra AND the new
//     name as missing).
//   - EXPRESSIONS. Even with matching names, diffChecks compares Expr
//     byte-exactly after TrimSpace, and MySQL does not hand back what it
//     was given. Measured on MySQL 8.0: the clause sluice emits as
//     CHECK (REGEXP_LIKE(email, '^[a-z]+$')) reads back from
//     information_schema.CHECK_CONSTRAINTS as
//     regexp_like(email,_latin1\\'^[a-z]+$\\') — lowercased function,
//     backticked identifier, charset introducer, re-escaped literal.
//     Closing this needs a MySQL check-expression normalizer, which is
//     its own surface.
//
// # The independent expected value, named (the 2026-08-01 rule)
//
// Two, and neither is sluice's own diff. The anti-vacuity half reads
// information_schema on the MySQL target directly — MySQL's own catalog,
// so "the CHECKs landed" is the server's answer, not sluice's. The drift
// half is then `schema diff`'s report about those same constraints.
package pipeline

import (
	"context"
	"database/sql"
	"io"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// domainCheckSeedDDL declares BOTH translatable DOMAIN CHECK shapes
// MySQL's emitter knows (domain_check_translate.go): the regex arm and
// the range arm. One would not do — they are separate translator arms
// producing separate clauses, and a diff defect could plausibly reach
// one shape and not the other.
//
// `dc_optout` carries an UN-translatable check (a function call), which
// the emitter drops with a WARN. It is here as the control: its column
// must produce no target-side CHECK at all, so a run where every column
// reports drift is distinguishable from one where the emitter simply
// emitted nothing.
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

// TestDiff_PGToMySQL_DomainChecksAreReportedAsPhantomDrift_Item156
// migrates a CHECK-carrying DOMAIN schema to MySQL and pins what
// `schema diff` says about the table sluice itself just created.
//
// The name says what it pins. It is deliberately NOT phrased as
// "…AreNotPhantomDrift" — a gate whose name can be read as broader than
// the truth is the failure mode this project treats as worse than none,
// and a reader skimming test names must not come away believing the
// clean-diff property holds.
func TestDiff_PGToMySQL_DomainChecksAreReportedAsPhantomDrift_Item156(t *testing.T) {
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
			"Any other count means the fixture no longer exercises what it claims: at 0 the drift "+
			"assertion below would be comparing two empty sets, and at 3 the emitter has started "+
			"translating a check shape this pin's reasoning did not account for", landed)
	}

	// ---- The finding: does `schema diff` call them drift? ----
	d := &Differ{
		Source: pgEng, Target: myEng,
		SourceDSN: pgSource, TargetDSN: mysqlTarget,
		Format: "json",
		Out:    io.Discard,
	}
	diff, err := d.Run(ctx)
	if err != nil {
		t.Fatalf("Differ.Run: %v", err)
	}

	var found bool
	for _, td := range diff.TablesMismatched {
		if td.Name != "dcheck" {
			continue
		}
		found = true
		if len(td.ChecksExtra) != landed {
			t.Errorf("item 156 CHANGED: `schema diff` reports %d extra CHECK constraint(s) on `dcheck` "+
				"(%v); this pin recorded %d, one per DOMAIN CHECK the emitter inlined.\n\n"+
				"If the count is now 0 the defect is FIXED — invert this pin to assert "+
				"len(ChecksExtra)==0 and close roadmap item 156. If it is some other number, the "+
				"expected side has started carrying SOME of the domain checks, which is a partial fix "+
				"and the more dangerous state: the ones it still misses now look like real drift among "+
				"reconciled ones.", len(td.ChecksExtra), td.ChecksExtra, landed)
		}
		if len(td.ChecksMissing) > 0 {
			t.Errorf("`schema diff` reports %d MISSING CHECK constraint(s) on `dcheck`: %v — the expected "+
				"side has started asking the target for a constraint under a name the migrate path does "+
				"not emit, which is item 156's naming half half-done", len(td.ChecksMissing), td.ChecksMissing)
		}
	}
	if !found {
		t.Errorf("item 156 CHANGED: `schema diff` reports NO mismatch at all on `dcheck`, but this pin "+
			"recorded %d phantom extra CHECK constraint(s) there. If the defect is fixed, invert this "+
			"pin and close roadmap item 156", landed)
	}
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
