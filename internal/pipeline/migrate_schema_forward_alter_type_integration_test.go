//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0091 (F7a) — default-on schema-change forwarding. ALTER COLUMN
// TYPE (a safe widening, INT→BIGINT) end-to-end across PG → PG,
// MySQL → MySQL, and one cross direction (MySQL → PG, exercising the
// translate.RetargetForEngine type rewrite on the destructive shape per
// ADR-0091 §5).
//
// ALTER COLUMN TYPE is a MUTATING shape and is seed-guarded at the first
// post-cold-start boundary (ADR-0091 §5b), so every test uses the
// prime-then-mutate pattern: a non-destructive ADD COLUMN flips the
// cache entry seed→CDC first, then the ALTER TYPE forwards on a genuine
// CDC→CDC boundary. The post-ALTER INSERT carries a value that overflows
// the OLD (INT) type but fits the NEW (BIGINT) type, proving the target
// column actually widened (not merely that the row landed).

package pipeline

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// bigVal overflows a signed 32-bit INT but fits a BIGINT — used to prove
// the target column genuinely widened.
const bigVal int64 = 5_000_000_000

// TestStreamer_SchemaForward_AlterType_PG widens an INT column to BIGINT
// on PG → PG and verifies the target column type changed and a
// post-ALTER row with a >32-bit value lands.
func TestStreamer_SchemaForward_AlterType_PG(t *testing.T) {
	// ADR-0091 F7a GAP #1 (fixed): the PG CDC reader now lets ALTER COLUMN
	// TYPE pass through to the forward intercept under
	// --schema-changes=forward. The post-ALTER INSERT carries a >32-bit
	// value that only fits BIGINT, proving the target column genuinely
	// widened (GAP #3 cache invalidation re-describes the column OID so the
	// pgx encode path uses the new int8 width).
	sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
	defer cleanup()

	applyPGDDL(t, sourceDSN, `
		CREATE TABLE widgets (
			id INT PRIMARY KEY,
			name TEXT NOT NULL,
			counter INT
		);
		ALTER TABLE widgets REPLICA IDENTITY FULL;
		INSERT INTO widgets (id, name, counter) VALUES (1, 'alpha', 10), (2, 'beta', 20);
	`)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	streamer := &Streamer{
		Source:    pgEng,
		Target:    pgEng,
		SourceDSN: sourceDSN,
		TargetDSN: targetDSN,
		StreamID:  "test-fwd-altertype-pg",
	}

	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()

	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	if !waitForPGRowCount(t, targetDSN, "widgets", 2, 30*time.Second) {
		t.Fatalf("phase A: bulk-copy never landed seed rows")
	}

	tgtDB, err := sql.Open("pgx", targetDSN)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = tgtDB.Close() }()

	// PRIME: flip seed→CDC (PG pure DDL needs a follow-on DML to surface).
	applyPGDDL(t, sourceDSN, `
		ALTER TABLE widgets ADD COLUMN _prime_col INT;
		INSERT INTO widgets (id, name, counter, _prime_col) VALUES (100, 'prime', 1, 1);
	`)
	if !waitForPGColumn(t, tgtDB, "widgets", "_prime_col", true, 60*time.Second) {
		t.Fatalf("prime: _prime_col never appeared on target — seed→CDC boundary not processed")
	}

	// Widen counter INT→BIGINT on a genuine CDC→CDC boundary; post-ALTER
	// INSERT carries a value that only fits BIGINT.
	applyPGDDL(t, sourceDSN, `
		ALTER TABLE widgets ALTER COLUMN counter TYPE BIGINT;
		INSERT INTO widgets (id, name, counter) VALUES (3, 'gamma', 5000000000);
	`)

	if !waitForPGRowID(t, tgtDB, "widgets", 3, 60*time.Second) {
		t.Fatalf("phase B: post-ALTER row never landed — ALTER TYPE forwarding broken")
	}

	if !waitForPGColumnType(t, tgtDB, "widgets", "counter", "bigint", 60*time.Second) {
		t.Errorf("target widgets.counter did not widen to bigint — ALTER TYPE did not forward")
	}

	assertPGCounter(t, tgtDB, 3, bigVal)

	streamCancel()
	select {
	case <-runErr:
	case <-time.After(15 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}
}

// TestStreamer_SchemaForward_AlterType_MySQL widens an INT column to
// BIGINT on MySQL → MySQL (MySQL emits MODIFY COLUMN; the apply path
// differs from PG's ALTER COLUMN … TYPE — pinned per target per the
// Bug 74 class discipline).
func TestStreamer_SchemaForward_AlterType_MySQL(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startMySQLBinlog(t)
	defer cleanup()

	applyDDLMySQL(t, sourceDSN, `
		CREATE TABLE widgets (
			id BIGINT NOT NULL PRIMARY KEY,
			name VARCHAR(64) NOT NULL,
			counter INT
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`)
	applyDDLMySQL(t, sourceDSN, "INSERT INTO widgets (id, name, counter) VALUES (1, 'alpha', 10), (2, 'beta', 20);")

	myEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}

	streamer := &Streamer{
		Source:    myEng,
		Target:    myEng,
		SourceDSN: sourceDSN,
		TargetDSN: targetDSN,
		StreamID:  "test-fwd-altertype-mysql",
	}

	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()

	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	if !waitForRowCountMySQL(t, targetDSN, "widgets", 2, 30*time.Second) {
		t.Fatalf("phase A: bulk-copy never landed seed rows")
	}

	tgtDB, err := sql.Open("mysql", targetDSN)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = tgtDB.Close() }()

	applyDDLMySQL(t, sourceDSN, "ALTER TABLE widgets ADD COLUMN _prime_col INT;")
	applyDDLMySQL(t, sourceDSN, "INSERT INTO widgets (id, name, counter, _prime_col) VALUES (100, 'prime', 1, 1);")
	if !waitForMySQLColumn(t, tgtDB, "widgets", "_prime_col", true, 60*time.Second) {
		t.Fatalf("prime: _prime_col never appeared on target — seed→CDC boundary not processed")
	}

	applyDDLMySQL(t, sourceDSN, "ALTER TABLE widgets MODIFY COLUMN counter BIGINT;")
	applyDDLMySQL(t, sourceDSN, "INSERT INTO widgets (id, name, counter) VALUES (3, 'gamma', 5000000000);")

	if !waitForMySQLRowID(t, tgtDB, "widgets", 3, 60*time.Second) {
		t.Fatalf("phase B: post-ALTER row never landed — ALTER TYPE forwarding broken")
	}

	if !waitForMySQLColumnType(t, tgtDB, "widgets", "counter", "bigint", 60*time.Second) {
		t.Errorf("target widgets.counter did not widen to bigint — MODIFY COLUMN did not forward")
	}

	assertMySQLCounter(t, tgtDB, 3, bigVal)

	streamCancel()
	select {
	case <-runErr:
	case <-time.After(15 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}
}

// TestStreamer_SchemaForward_AlterType_Cross_MySQLToPG widens INT→BIGINT
// from a MySQL source to a PG target — the cross-engine ALTER TYPE
// retarget (ADR-0091 §5): the source MySQL BIGINT is translated to the
// PG dialect before the target ALTER is issued.
func TestStreamer_SchemaForward_AlterType_Cross_MySQLToPG(t *testing.T) {
	// Regression pin for F7a GAP #3 (cross-engine ALTER TYPE convergence).
	// This previously false-succeeded: the intercept logged the ALTER as
	// applied, but the PG target column stayed int4, so the post-ALTER
	// >32-bit row failed to encode. Root cause was a stale applier
	// colTypeCache + pgx prepared-statement OID cache not refreshed on the
	// boundary (NOT the retarget). Fixed by invalidateTargetCachesForBoundary
	// + QueryExecModeExec re-describe on an actual signature change. This
	// test pins that the column truly widens and the >32-bit value lands.

	mysqlDSN, _, mysqlCleanup := startMySQLBinlog(t)
	defer mysqlCleanup()
	_, pgDSN, pgCleanup := startPostgresLogical(t)
	defer pgCleanup()

	applyDDLMySQL(t, mysqlDSN, `
		CREATE TABLE widgets (
			id BIGINT NOT NULL PRIMARY KEY,
			name VARCHAR(64) NOT NULL,
			counter INT
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`)
	applyDDLMySQL(t, mysqlDSN, "INSERT INTO widgets (id, name, counter) VALUES (1, 'alpha', 10), (2, 'beta', 20);")

	myEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	streamer := &Streamer{
		Source:    myEng,
		Target:    pgEng,
		SourceDSN: mysqlDSN,
		TargetDSN: pgDSN,
		StreamID:  "test-fwd-altertype-mypg",
	}

	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()

	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	if !waitForPGRowCount(t, pgDSN, "widgets", 2, 60*time.Second) {
		t.Fatalf("phase A: bulk-copy never landed seed rows on PG target")
	}

	tgtDB, err := sql.Open("pgx", pgDSN)
	if err != nil {
		t.Fatalf("open pg target: %v", err)
	}
	defer func() { _ = tgtDB.Close() }()

	applyDDLMySQL(t, mysqlDSN, "ALTER TABLE widgets ADD COLUMN _prime_col INT;")
	applyDDLMySQL(t, mysqlDSN, "INSERT INTO widgets (id, name, counter, _prime_col) VALUES (100, 'prime', 1, 1);")
	if !waitForPGColumn(t, tgtDB, "widgets", "_prime_col", true, 60*time.Second) {
		t.Fatalf("prime: _prime_col never appeared on PG target — seed→CDC boundary not processed")
	}

	applyDDLMySQL(t, mysqlDSN, "ALTER TABLE widgets MODIFY COLUMN counter BIGINT;")
	applyDDLMySQL(t, mysqlDSN, "INSERT INTO widgets (id, name, counter) VALUES (3, 'gamma', 5000000000);")

	if !waitForPGRowID(t, tgtDB, "widgets", 3, 60*time.Second) {
		t.Fatalf("phase B: post-ALTER row never landed on PG target — cross-engine ALTER TYPE forwarding broken")
	}

	if !waitForPGColumnType(t, tgtDB, "widgets", "counter", "bigint", 60*time.Second) {
		t.Errorf("PG target widgets.counter did not widen to bigint — cross-engine ALTER TYPE did not forward")
	}

	assertPGCounter(t, tgtDB, 3, bigVal)

	streamCancel()
	select {
	case <-runErr:
	case <-time.After(15 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}
}

// TestStreamer_SchemaForward_AlterType_TypmodOnly_PG pins the G3 typmod
// half of the table-rewrite class (capture-completeness sweep 2026-08-26)
// end-to-end, and specifically the CONVERGENCE claim: a typmod-only
// `ALTER COLUMN TYPE numeric(10,4) → numeric(10,1)` REWRITES every stored
// source value (12.3456 → 12.3, 99.9999 → 100.0 — PG's deterministic
// round-half-up cast) while logically decoding ZERO messages for the
// rewrite; the post-rewrite RelationMessage's new typmod is the only wire
// artifact. Under the ADR-0091 forward default the boundary must forward
// the same USING-less ALTER to the target, whose own rewrite applies the
// IDENTICAL deterministic cast to equal inputs — so the pre-rewrite
// target rows CONVERGE with the source's rewritten values. This is the
// one shape where forwarding genuinely heals the rewrite blindness; the
// same-type same-typmod `USING <expr>` sibling has no wire artifact at
// all and stays a documented gap (matrix + ADR-0091).
//
// Uses the same prime-then-mutate pattern as the tests above (ALTER TYPE
// is seed-guarded at the first post-cold-start boundary, ADR-0091 §5b).
func TestStreamer_SchemaForward_AlterType_TypmodOnly_PG(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
	defer cleanup()

	applyPGDDL(t, sourceDSN, `
		CREATE TABLE ledger (
			id INT PRIMARY KEY,
			amt NUMERIC(10,4)
		);
		ALTER TABLE ledger REPLICA IDENTITY FULL;
		INSERT INTO ledger (id, amt) VALUES (1, 12.3456), (2, 99.9999);
	`)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	streamer := &Streamer{
		Source:    pgEng,
		Target:    pgEng,
		SourceDSN: sourceDSN,
		TargetDSN: targetDSN,
		StreamID:  "test-fwd-altertype-typmod-pg",
	}

	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()

	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	if !waitForPGRowCount(t, targetDSN, "ledger", 2, 30*time.Second) {
		t.Fatalf("phase A: bulk-copy never landed seed rows")
	}

	tgtDB, err := sql.Open("pgx", targetDSN)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = tgtDB.Close() }()

	// PRIME: flip seed→CDC (a mutating shape at the first boundary is
	// seed-guarded; the ADD COLUMN boundary is the standard unlock).
	applyPGDDL(t, sourceDSN, `
		ALTER TABLE ledger ADD COLUMN _prime_col INT;
		INSERT INTO ledger (id, amt, _prime_col) VALUES (100, 1.0, 1);
	`)
	if !waitForPGColumn(t, tgtDB, "ledger", "_prime_col", true, 60*time.Second) {
		t.Fatalf("prime: _prime_col never appeared on target — seed→CDC boundary not processed")
	}

	// The typmod-only shrink: same OID (numeric), new modifier. Every
	// stored source value is rewritten (rounded) with zero decoded
	// messages; the follow-on INSERT surfaces the new RelationMessage.
	applyPGDDL(t, sourceDSN, `
		ALTER TABLE ledger ALTER COLUMN amt TYPE NUMERIC(10,1);
		INSERT INTO ledger (id, amt) VALUES (3, 5.5);
	`)

	if !waitForPGRowID(t, tgtDB, "ledger", 3, 60*time.Second) {
		t.Fatalf("phase B: post-ALTER row never landed — typmod-only ALTER TYPE forwarding broken")
	}

	// The target column's declared scale must have followed the source's.
	deadline := time.Now().Add(60 * time.Second)
	for {
		var precision, scale int
		err := tgtDB.QueryRow(
			`SELECT numeric_precision, numeric_scale FROM information_schema.columns
			  WHERE table_name = 'ledger' AND column_name = 'amt'`,
		).Scan(&precision, &scale)
		if err == nil && precision == 10 && scale == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("target ledger.amt = numeric(%d,%d), err=%v; want numeric(10,1) — the typmod-only ALTER did not forward", precision, scale, err)
		}
		time.Sleep(500 * time.Millisecond)
	}

	// CONVERGENCE: the pre-rewrite rows must hold the SAME post-cast
	// values on both sides — the source's own rewrite and the target's
	// forwarded rewrite applied the identical deterministic cast.
	assertLedgerAmtText(t, tgtDB, map[int]string{1: "12.3", 2: "100.0", 3: "5.5"})

	streamCancel()
	select {
	case <-runErr:
	case <-time.After(15 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}
}

// TestStreamer_SchemaForward_AlterType_VarcharShrink_PG is the second
// forward-convergence COMPOSITION family (audit 2026-08-27 A9): the
// capture-completeness matrix claims typmod forward convergence for the
// IR-carrying families (plural), but until this test only numeric's
// composition was pinned end-to-end — the forward-apply path renders
// per-family DDL from per-family typmod projections (the Bug 74
// family-dispatched shape: varchar packs n+4 where numeric packs
// (p<<16|s)+4, and the emitted ALTER renders `varchar(10)` from
// ir.Varchar{Length}, a different projection+render arm than numeric's).
// A varchar(20)→varchar(10) shrink rewrites the table on the source (PG
// re-checks every value against the new length; values that fit pass
// unchanged) with zero decoded messages; the forwarded USING-less ALTER
// must shrink the target's DECLARED type to match, and the pre-shrink
// rows must remain intact on both sides. interval/array — the families
// whose typmod never reaches the projection — are pinned separately as
// refusals (TestTypmodProjectionGate_EveryTypmodFamily, the A2 gate).
//
// Same prime-then-mutate pattern as the tests above (ALTER TYPE is
// seed-guarded at the first post-cold-start boundary, ADR-0091 §5b).
func TestStreamer_SchemaForward_AlterType_VarcharShrink_PG(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
	defer cleanup()

	applyPGDDL(t, sourceDSN, `
		CREATE TABLE tags (
			id INT PRIMARY KEY,
			label VARCHAR(20)
		);
		ALTER TABLE tags REPLICA IDENTITY FULL;
		INSERT INTO tags (id, label) VALUES (1, 'alpha'), (2, 'beta-tag');
	`)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	streamer := &Streamer{
		Source:    pgEng,
		Target:    pgEng,
		SourceDSN: sourceDSN,
		TargetDSN: targetDSN,
		StreamID:  "test-fwd-altertype-varchar-pg",
	}

	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()

	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	if !waitForPGRowCount(t, targetDSN, "tags", 2, 30*time.Second) {
		t.Fatalf("phase A: bulk-copy never landed seed rows")
	}

	tgtDB, err := sql.Open("pgx", targetDSN)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = tgtDB.Close() }()

	// PRIME: flip seed→CDC (a mutating shape at the first boundary is
	// seed-guarded; the ADD COLUMN boundary is the standard unlock).
	applyPGDDL(t, sourceDSN, `
		ALTER TABLE tags ADD COLUMN _prime_col INT;
		INSERT INTO tags (id, label, _prime_col) VALUES (100, 'prime', 1);
	`)
	if !waitForPGColumn(t, tgtDB, "tags", "_prime_col", true, 60*time.Second) {
		t.Fatalf("prime: _prime_col never appeared on target — seed→CDC boundary not processed")
	}

	// The varchar typmod-only shrink: same OID (1043), new modifier. Every
	// stored value fits varchar(10), so the source rewrite succeeds; the
	// follow-on INSERT surfaces the new RelationMessage.
	applyPGDDL(t, sourceDSN, `
		ALTER TABLE tags ALTER COLUMN label TYPE VARCHAR(10);
		INSERT INTO tags (id, label) VALUES (3, 'gamma');
	`)

	if !waitForPGRowID(t, tgtDB, "tags", 3, 60*time.Second) {
		t.Fatalf("phase B: post-ALTER row never landed — varchar typmod-only ALTER TYPE forwarding broken")
	}

	// The target column's DECLARED length must have followed the source's —
	// the composition half: the forwarded ALTER rendered varchar(10) from
	// the varchar projection arm, not merely "a row landed".
	deadline := time.Now().Add(60 * time.Second)
	for {
		var maxLen int
		err := tgtDB.QueryRow(
			`SELECT character_maximum_length FROM information_schema.columns
			  WHERE table_name = 'tags' AND column_name = 'label'`,
		).Scan(&maxLen)
		if err == nil && maxLen == 10 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("target tags.label character_maximum_length = %d, err=%v; want 10 — the varchar typmod-only ALTER did not forward", maxLen, err)
		}
		time.Sleep(500 * time.Millisecond)
	}

	// CONVERGENCE: pre-shrink rows hold identical values on both sides
	// (the shrink's re-check passes fitting values through unchanged —
	// loud on the source if any value did not fit, never a silent
	// truncation).
	assertTagsLabel(t, tgtDB, map[int]string{1: "alpha", 2: "beta-tag", 3: "gamma"})

	streamCancel()
	select {
	case <-runErr:
	case <-time.After(15 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}
}

// assertTagsLabel compares tags.label per id against want.
func assertTagsLabel(t *testing.T, db *sql.DB, want map[int]string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	for id, wantLabel := range want {
		var got string
		if err := db.QueryRowContext(ctx, "SELECT label FROM tags WHERE id=$1", id).Scan(&got); err != nil {
			t.Fatalf("scan label id=%d: %v", id, err)
		}
		if got != wantLabel {
			t.Errorf("tags.label for id=%d = %q; want %q (pre-shrink target rows did not survive the forwarded varchar shrink)", id, got, wantLabel)
		}
	}
}

// assertLedgerAmtText compares ledger.amt::text per id against want —
// text form so the numeric scale (12.3, not 12.3000) is part of the
// assertion.
func assertLedgerAmtText(t *testing.T, db *sql.DB, want map[int]string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	for id, wantText := range want {
		var got string
		if err := db.QueryRowContext(ctx, "SELECT amt::text FROM ledger WHERE id=$1", id).Scan(&got); err != nil {
			t.Fatalf("scan amt id=%d: %v", id, err)
		}
		if got != wantText {
			t.Errorf("ledger.amt for id=%d = %q; want %q (pre-rewrite target rows did not converge with the source's typmod rewrite)", id, got, wantText)
		}
	}
}

func assertPGCounter(t *testing.T, db *sql.DB, id int, want int64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var got int64
	if err := db.QueryRowContext(ctx, "SELECT counter FROM widgets WHERE id=$1", id).Scan(&got); err != nil {
		t.Fatalf("scan counter id=%d: %v", id, err)
	}
	if got != want {
		t.Errorf("widgets.counter for id=%d = %d; want %d (>32-bit value lost — column did not widen)", id, got, want)
	}
}

func assertMySQLCounter(t *testing.T, db *sql.DB, id int, want int64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var got int64
	if err := db.QueryRowContext(ctx, "SELECT counter FROM widgets WHERE id=?", id).Scan(&got); err != nil {
		t.Fatalf("scan counter id=%d: %v", id, err)
	}
	if got != want {
		t.Errorf("widgets.counter for id=%d = %d; want %d (>32-bit value lost — column did not widen)", id, got, want)
	}
}
