//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Two SQLite→Postgres DDL-fidelity surfaces that a rich-type promotion
// (`--infer-types`, ADR-0144) can invalidate, both surfaced by reviewing
// planetscale/cli#1299 against sluice:
//
//  1. BOOLEAN CHECK CONSTRAINTS. SQLite has no BOOLEAN type, so the
//     canonical idiom is `flag INTEGER CHECK (flag IN (0,1))`. When
//     --infer-types promotes that column to PG BOOLEAN, the CHECK is
//     carried across verbatim and Postgres REJECTS it — `operator does
//     not exist: boolean = integer`. Because constraints are created in
//     a deferred phase AFTER the bulk copy, this fails at the very END
//     of an otherwise-successful migration.
//
//  2. strftime() DEFAULTS. sluice's SQLite default translator recognises
//     only datetime/date/time('now'); every strftime() spelling falls to
//     the loud-WARN DROP path, so the migration succeeds but the DEFAULT
//     is silently absent on the target. Safe, but lossy — a
//     DEFAULT-omitting INSERT on the target after cutover gets NULL
//     where the source would have supplied a timestamp.
//
// Both are LOUD-or-LOSSY, not silent corruption: (1) fails the run, (2)
// warns and drops. Neither risks migrated row data — the copied rows
// carry explicit values.

package pipeline

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite" // pure-Go driver for seeding the temp source file

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
	_ "sluicesync.dev/sluice/internal/engines/sqlite"
)

// skipUntilSQLiteCheckINSupported gates both pins. They are WRITTEN AND
// CONFIRMED FAILING against the current tree (2026-07-22) — that is the
// point of them — but a red integration job on main helps nobody, so
// they are skipped until the fix lands.
//
// Confirmed failure on the pre-fix tree, for BOTH tests, at create-tables:
//
//	refuse loudly: CHECK constraint "" carries a non-portable SQLite
//	expression "is_in IN (0, 1)" with no provably-equivalent Postgres
//	translation
//
// Note the cause is NOT type coercion: `internal/translate/sqlite_expr.go`
// has no `IN` node at all, so a portable `col IN (0,1)` cannot be parsed
// and hits the catch-all refusal — blocking the migration outright, before
// --infer-types is ever relevant.
//
// REMOVING THIS SKIP IS PART OF THE FIX'S DEFINITION OF DONE. See the
// "SQLite CHECK-constraint `IN` support" roadmap entry.
const skipUntilSQLiteCheckINSupported = "gap not yet fixed: the SQLite expression translator has no IN node, " +
	"so a portable `CHECK (col IN (0,1))` is refused and the migration is blocked. " +
	"These pins are confirmed-failing by design; un-skip them as part of the fix. " +
	"See docs/dev/roadmap.md — SQLite CHECK-constraint IN support."

// seedCheckDefaultSource writes a SQLite file exercising the SQLite
// boolean idiom in every operator shape PG would reject after a BOOLEAN
// promotion, plus the strftime() DEFAULT spellings.
func seedCheckDefaultSource(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "checkdef.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open sqlite seed: %v", err)
	}
	defer func() { _ = db.Close() }()

	stmts := []string{
		// Every arm here is a boolean CANDIDATE (INTEGER, `is_`/`has_`
		// name hint, values only 0/1) so --infer-types promotes it; the
		// CHECK then has to be rewritten or PG rejects the constraint.
		`CREATE TABLE flags (
			id          INTEGER PRIMARY KEY,
			is_in       INTEGER CHECK (is_in IN (0, 1)),
			is_eq       INTEGER CHECK (is_eq = 1),
			is_ne       INTEGER CHECK (is_ne <> 0),
			is_bang     INTEGER CHECK (is_bang != 0),
			is_rev      INTEGER CHECK (1 = is_rev),
			plain_count INTEGER CHECK (plain_count IN (0, 1, 2))
		)`,
		`INSERT INTO flags VALUES (1, 1, 1, 1, 1, 1, 2)`,
		`INSERT INTO flags VALUES (2, 0, 1, 1, 1, 1, 0)`,

		// strftime() DEFAULT spellings. `created_at` is a temporal
		// candidate (values all carry an offset) so it promotes to
		// timestamptz; the DEFAULT has to translate or be dropped.
		`CREATE TABLE stamped (
			id         INTEGER PRIMARY KEY,
			created_at TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
			plain_at   TEXT DEFAULT (datetime('now'))
		)`,
		`INSERT INTO stamped (id, created_at, plain_at)
		 VALUES (1, '2024-01-15T10:30:00+00:00', '2024-01-15 10:30:00')`,
	}
	for _, s := range stmts {
		if _, err := db.ExecContext(context.Background(), s); err != nil {
			t.Fatalf("seed exec %q: %v", s, err)
		}
	}
	return path
}

// TestMigrate_InferTypes_BooleanCheckConstraint_SQLiteToPostgres is the
// regression gate for surface (1). It FAILS on pre-fix code: the migrate
// run itself errors in the deferred constraint phase because PG rejects
// `CHECK (is_in IN (0,1))` against a BOOLEAN column.
func TestMigrate_InferTypes_BooleanCheckConstraint_SQLiteToPostgres(t *testing.T) {
	t.Skip(skipUntilSQLiteCheckINSupported)

	src := seedCheckDefaultSource(t)
	_, pgTarget, pgCleanup := startPostgres(t)
	defer pgCleanup()

	sqliteEng, ok := engines.Get("sqlite")
	if !ok {
		t.Fatal("sqlite engine not registered")
	}
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	mig := &Migrator{
		Source:     sqliteEng,
		Target:     pgEng,
		SourceDSN:  src,
		TargetDSN:  pgTarget,
		InferTypes: true,
	}
	// The load-bearing assertion. Pre-fix this returns an error like
	// `operator does not exist: boolean = integer` from the deferred
	// ADD CONSTRAINT phase — AFTER every row has already been copied.
	if err := mig.Run(ctx2min(t)); err != nil {
		t.Fatalf("Migrator.Run (--infer-types SQLite→PG with boolean CHECK constraints): %v\n\n"+
			"This is the planetscale/cli#1299 shape: SQLite's canonical boolean idiom "+
			"`INTEGER CHECK (col IN (0,1))` survives a BOOLEAN promotion verbatim and PG "+
			"rejects it. The CHECK must be rewritten to boolean literals when the column "+
			"is boolean-coerced.", err)
	}

	pg, err := sql.Open("pgx", pgTarget)
	if err != nil {
		t.Fatalf("open pg: %v", err)
	}
	defer func() { _ = pg.Close() }()
	ctx := ctx2min(t)

	// The promotions actually happened — otherwise the run could pass
	// vacuously by never coercing anything to BOOLEAN.
	for _, col := range []string{"is_in", "is_eq", "is_ne", "is_bang", "is_rev"} {
		var got string
		if err := pg.QueryRowContext(ctx,
			`SELECT data_type FROM information_schema.columns
			 WHERE table_name = 'flags' AND column_name = $1`, col).Scan(&got); err != nil {
			t.Fatalf("query data_type for %s: %v", col, err)
		}
		if got != "boolean" {
			t.Errorf("flags.%s data_type = %q; want boolean (no promotion ⇒ this test proves nothing)", col, got)
		}
	}
	// The non-boolean column keeps its integer CHECK untouched.
	var plainType string
	if err := pg.QueryRowContext(ctx,
		`SELECT data_type FROM information_schema.columns
		 WHERE table_name = 'flags' AND column_name = 'plain_count'`).Scan(&plainType); err != nil {
		t.Fatalf("query data_type for plain_count: %v", err)
	}
	if plainType != "bigint" {
		t.Errorf("flags.plain_count data_type = %q; want bigint (values 0/1/2 are not boolean)", plainType)
	}

	// The rewritten constraints must still ENFORCE. A boolean CHECK that
	// silently became a tautology would be worse than the original bug.
	if _, err := pg.ExecContext(ctx,
		`INSERT INTO flags (id, is_in, is_eq, is_ne, is_bang, is_rev, plain_count)
		 VALUES (3, true, false, true, true, true, 1)`); err == nil {
		t.Error("PG accepted is_eq=false against CHECK (is_eq = 1) → the rewritten constraint does not enforce")
	}

	// And a conforming row is still accepted.
	if _, err := pg.ExecContext(ctx,
		`INSERT INTO flags (id, is_in, is_eq, is_ne, is_bang, is_rev, plain_count)
		 VALUES (4, false, true, true, true, true, 2)`); err != nil {
		t.Errorf("PG rejected a CONFORMING row — the rewritten constraint over-fires: %v", err)
	}

	// Row data survived.
	var n int
	if err := pg.QueryRowContext(ctx, `SELECT count(*) FROM flags WHERE id IN (1,2)`).Scan(&n); err != nil {
		t.Fatalf("count copied rows: %v", err)
	}
	if n != 2 {
		t.Errorf("copied rows = %d; want 2", n)
	}
}

// TestMigrate_InferTypes_StrftimeDefault_SQLiteToPostgres pins surface
// (2). Pre-fix the strftime() DEFAULT is dropped (loud WARN) so the
// target column has NO default; post-fix it translates to a valid PG
// expression. The `plain_at` arm is the control — already translated
// today, and must not regress.
func TestMigrate_InferTypes_StrftimeDefault_SQLiteToPostgres(t *testing.T) {
	t.Skip(skipUntilSQLiteCheckINSupported)

	src := seedCheckDefaultSource(t)
	_, pgTarget, pgCleanup := startPostgres(t)
	defer pgCleanup()

	sqliteEng, ok := engines.Get("sqlite")
	if !ok {
		t.Fatal("sqlite engine not registered")
	}
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	mig := &Migrator{
		Source:     sqliteEng,
		Target:     pgEng,
		SourceDSN:  src,
		TargetDSN:  pgTarget,
		InferTypes: true,
	}
	if err := mig.Run(ctx2min(t)); err != nil {
		t.Fatalf("Migrator.Run: %v", err)
	}

	pg, err := sql.Open("pgx", pgTarget)
	if err != nil {
		t.Fatalf("open pg: %v", err)
	}
	defer func() { _ = pg.Close() }()
	ctx := ctx2min(t)

	colDefault := func(col string) string {
		var d sql.NullString
		if err := pg.QueryRowContext(ctx,
			`SELECT column_default FROM information_schema.columns
			 WHERE table_name = 'stamped' AND column_name = $1`, col).Scan(&d); err != nil {
			t.Fatalf("query column_default for %s: %v", col, err)
		}
		return d.String
	}

	// Control: the already-supported spelling must keep working.
	if got := colDefault("plain_at"); got == "" {
		t.Error("stamped.plain_at lost its datetime('now') DEFAULT — regression in the existing translator")
	}

	// The gate: strftime() must translate rather than be dropped.
	if got := colDefault("created_at"); got == "" {
		t.Error("stamped.created_at has NO default on the target — the strftime() DEFAULT was DROPPED. " +
			"planetscale/cli#1299 translates these (e.g. the ISO strftime spelling → " +
			"date_trunc('second', now())); sluice currently recognises only datetime/date/time('now').")
	}

	// A DEFAULT-omitting INSERT must actually supply a value — the point
	// of translating rather than dropping.
	if _, err := pg.ExecContext(ctx, `INSERT INTO stamped (id) VALUES (2)`); err != nil {
		t.Fatalf("default-omitting INSERT: %v", err)
	}
	var nullCreated bool
	if err := pg.QueryRowContext(ctx,
		`SELECT created_at IS NULL FROM stamped WHERE id = 2`).Scan(&nullCreated); err != nil {
		t.Fatalf("read defaulted row: %v", err)
	}
	if nullCreated {
		t.Error("a DEFAULT-omitting INSERT left created_at NULL — the source would have supplied a timestamp")
	}
}
