//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/engines/pgtrigger"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

// pgTriggerNumericCells is the family matrix for the postgres-trigger
// backup-chain numeric pin: every value is written AFTER the full, so it
// reaches the chain only through the trigger CDC reader's change payload —
// the path that hands non-integer numerics (and every jsonb number leaf) to
// the chunk codec as json.Number.
var pgTriggerNumericCells = []struct {
	id int
	nu string // numeric (unconstrained)
	np string // numeric(38,12)
	f8 string // double precision
	f4 string // real
	na string // numeric[]
	js string // jsonb
}{
	{101, "123456789012345678.123456789012", "123456789012345678.123456789012", "1.5", "1.25", "{1.1,22222222222222222222.2}", `{"big": 12345678901234567890, "dec": 0.1234567890123456789012345678901234567890}`},
	{102, "0.1234567890123456789012345678901234567890", "0.123456789012", "0.1", "0.1", "{{1.5,2.25},{-3.125,NULL}}", `{"n": [1.0, 2.50, 9007199254740993]}`},
	{103, "-98765432109876543210.5", "-12345678901234567890.5", "-2.5e-300", "3.4e38", "{0.000000000000000000001}", `{"neg": -0.0000000000000000001}`},
	{104, "1.500000", "1.500000000000", "123456789.123456789", "1", "{}", `{"zero": 0, "t": 1.500}`},
	{105, "12345678901234567890", "12345678901234567890", "9007199254740993", "16777217", "{12345678901234567890}", `{"i": 9223372036854775808}`},
	{106, "NaN", "0", "NaN", "Infinity", "{NaN}", `{"s": "1.5"}`},
}

// TestBackupChain_PGTrigger_NumericValuesRestoreExactly pins that a
// postgres-trigger chain restores every numeric family byte-for-byte:
// the chunk codec wrote the trigger reader's json.Number as a bare JSON
// number, and the change-chunk decoder read it back as a float64, so
// numeric/numeric[] and every jsonb number leaf lost digits at exit 0.
// The independent expected value is the source's own `col::text`.
func TestBackupChain_PGTrigger_NumericValuesRestoreExactly(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgres(t)
	defer cleanup()
	applyDDL(t, sourceDSN, `
		CREATE TABLE nums (
			id BIGINT PRIMARY KEY,
			nu NUMERIC, np NUMERIC(38,12), f8 DOUBLE PRECISION, f4 REAL,
			na NUMERIC[], js JSONB
		);
		INSERT INTO nums (id, nu, np, f8, f4, na, js) VALUES (1, 1, 1, 1, 1, '{1}', '{"seed": 1}');
		-- Written BEFORE the full: graded through the full's data chunks
		-- (the delegated postgres row reader), a different path.
		INSERT INTO nums (id, nu, np, f8, f4, na, js) VALUES (2,
			'98765432109876543210.123456789012345678901234567890', '-123456789012345678.123456789012',
			'0.1', '0.1', '{1.10,2.200}', '{"big": 12345678901234567890}');
	`)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	if _, err := pgtrigger.Setup(ctx, sourceDSN, pgtrigger.SetupOptions{Tables: []string{"nums"}, Schema: "public"}); err != nil {
		t.Fatalf("pgtrigger.Setup: %v", err)
	}
	src, ok := engines.Get(pgtrigger.EngineName)
	if !ok {
		t.Fatal("postgres-trigger engine not registered")
	}
	store := runTriggerChainFull(ctx, t, src, sourceDSN, pgtrigger.EngineName, func(tok string) (int64, error) {
		return pgtrigger.AppliedLastID(tok)
	})

	// Post-full writes: an INSERT per cell, then an UPDATE of the seed row
	// to the first cell's values so the UPDATE after-image path is graded
	// too. MaxChanges below counts exactly these.
	for _, c := range pgTriggerNumericCells {
		applyDDL(t, sourceDSN, fmt.Sprintf(
			`INSERT INTO nums (id, nu, np, f8, f4, na, js) VALUES (%d, '%s', '%s', '%s', '%s', '%s', '%s');`,
			c.id, c.nu, c.np, c.f8, c.f4, c.na, c.js,
		))
	}
	c0 := pgTriggerNumericCells[0]
	applyDDL(t, sourceDSN, fmt.Sprintf(
		`UPDATE nums SET nu = '%s', np = '%s', f8 = '%s', f4 = '%s', na = '%s', js = '%s' WHERE id = 1;`,
		c0.nu, c0.np, c0.f8, c0.f4, c0.na, c0.js,
	))

	runTriggerChainIncrementalAndRestoreN(ctx, t, src, sourceDSN, store, "postgres", targetDSN, len(pgTriggerNumericCells)+1)

	want := pgNumsText(t, sourceDSN)
	got := pgNumsText(t, targetDSN)
	if len(got) != len(want) {
		t.Fatalf("restored %d rows; source has %d", len(got), len(want))
	}
	for id, w := range want {
		g, ok := got[id]
		if !ok {
			t.Errorf("row %d missing from the restore", id)
			continue
		}
		for col, wv := range w {
			if g[col] != wv {
				t.Errorf("row %d %s: restored %q; source %q", id, col, g[col], wv)
			}
		}
	}
}

// pgNumsText reads every column of nums as the server's own text rendering.
func pgNumsText(t *testing.T, dsn string) map[int64]map[string]string {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	rows, err := db.QueryContext(context.Background(),
		`SELECT id, nu::text, np::text, f8::text, f4::text, na::text, js::text FROM nums ORDER BY id`)
	if err != nil {
		t.Fatalf("read nums: %v", err)
	}
	defer func() { _ = rows.Close() }()
	out := map[int64]map[string]string{}
	for rows.Next() {
		var id int64
		var nu, np, f8, f4, na, js sql.NullString
		if err := rows.Scan(&id, &nu, &np, &f8, &f4, &na, &js); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out[id] = map[string]string{"nu": nu.String, "np": np.String, "f8": f8.String, "f4": f4.String, "na": na.String, "js": js.String}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	return out
}

// pgTriggerNumericMySQLCells are the MySQL-target lane's values: every one
// fits its MySQL mapping exactly (unconstrained numeric → DECIMAL(65,30),
// numeric(38,12) → DECIMAL(38,12)), so a restored difference can only be
// the chain's own loss, not a target-type limit.
var pgTriggerNumericMySQLCells = []struct {
	id     int
	nu, np string
}{
	{201, "123456789012345678.123456789012", "123456789012345678.123456789012"},
	{202, "-98765432109876543210.123456789012345678901234567890", "-12345678901234567890.500000000000"},
	{203, "0.000000000000000000000000000001", "0.000000000001"},
	{204, "12345678901234567890123456789012345", "99999999999999999999999999.999999999999"},
	{205, "1.500000", "1.500000000000"},
}

// TestBackupChain_PGTrigger_NumericValuesRestoreExactly_ToMySQL is the
// MySQL-target lane of the same pin: the chain is restored through the
// MySQL applier, which receives the decoded values as json.Number exactly
// as it does from the live postgres-trigger change stream. Independent
// expected value: the SOURCE's own `col::text`, compared by exact decimal
// value (a DECIMAL(65,30) renders its scale's trailing zeros).
func TestBackupChain_PGTrigger_NumericValuesRestoreExactly_ToMySQL(t *testing.T) {
	sourceDSN, _, pgCleanup := startPostgres(t)
	defer pgCleanup()
	_, mysqlTarget, myCleanup := startMySQL(t)
	defer myCleanup()
	applyDDL(t, sourceDSN, `
		CREATE TABLE numsmy (id BIGINT PRIMARY KEY, nu NUMERIC, np NUMERIC(38,12));
		INSERT INTO numsmy (id, nu, np) VALUES (1, 1, 1);
	`)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	if _, err := pgtrigger.Setup(ctx, sourceDSN, pgtrigger.SetupOptions{Tables: []string{"numsmy"}, Schema: "public"}); err != nil {
		t.Fatalf("pgtrigger.Setup: %v", err)
	}
	src, ok := engines.Get(pgtrigger.EngineName)
	if !ok {
		t.Fatal("postgres-trigger engine not registered")
	}
	store := runTriggerChainFull(ctx, t, src, sourceDSN, pgtrigger.EngineName, func(tok string) (int64, error) {
		return pgtrigger.AppliedLastID(tok)
	})
	for _, c := range pgTriggerNumericMySQLCells {
		applyDDL(t, sourceDSN, fmt.Sprintf(`INSERT INTO numsmy (id, nu, np) VALUES (%d, '%s', '%s');`, c.id, c.nu, c.np))
	}
	runTriggerChainIncrementalAndRestoreN(ctx, t, src, sourceDSN, store, "mysql", mysqlTarget, len(pgTriggerNumericMySQLCells))

	want := numericColumnsText(t, "pgx", sourceDSN, `SELECT id, nu::text, np::text FROM numsmy ORDER BY id`)
	got := numericColumnsText(t, "mysql", mysqlTarget, `SELECT id, CAST(nu AS CHAR), CAST(np AS CHAR) FROM numsmy ORDER BY id`)
	if len(got) != len(want) {
		t.Fatalf("restored %d rows; source has %d", len(got), len(want))
	}
	for id, w := range want {
		for i, wv := range w {
			if g := got[id]; g == nil || canonicalDecimal(g[i]) != canonicalDecimal(wv) {
				t.Errorf("row %d col %d: restored %v; source %q", id, i, got[id], wv)
			}
		}
	}
}

// numericColumnsText reads (id, a, b) rows as text.
func numericColumnsText(t *testing.T, driver, dsn, query string) map[int64][]string {
	t.Helper()
	db, err := sql.Open(driver, dsn)
	if err != nil {
		t.Fatalf("open %s: %v", driver, err)
	}
	defer func() { _ = db.Close() }()
	rows, err := db.QueryContext(context.Background(), query)
	if err != nil {
		t.Fatalf("query %s: %v", driver, err)
	}
	defer func() { _ = rows.Close() }()
	out := map[int64][]string{}
	for rows.Next() {
		var id int64
		var a, b sql.NullString
		if err := rows.Scan(&id, &a, &b); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out[id] = []string{a.String, b.String}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	return out
}

// TestSyncFromBackup_PGTriggerChain_NumericValuesExact is the broker lane
// (`sync from-backup`): the same postgres-trigger chain, replayed onto a
// target seeded from its full, must land every numeric family exactly —
// the broker decodes change chunks through its own reader site, separate
// from chain restore's. Independent expected value: the source's own
// `col::text`.
func TestSyncFromBackup_PGTriggerChain_NumericValuesExact(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgres(t)
	defer cleanup()
	applyDDL(t, sourceDSN, `
		CREATE TABLE nums (
			id BIGINT PRIMARY KEY,
			nu NUMERIC, np NUMERIC(38,12), f8 DOUBLE PRECISION, f4 REAL,
			na NUMERIC[], js JSONB
		);
		INSERT INTO nums (id, nu, np, f8, f4, na, js) VALUES (1, 1, 1, 1, 1, '{1}', '{"seed": 1}');
	`)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	if _, err := pgtrigger.Setup(ctx, sourceDSN, pgtrigger.SetupOptions{Tables: []string{"nums"}, Schema: "public"}); err != nil {
		t.Fatalf("pgtrigger.Setup: %v", err)
	}
	src, ok := engines.Get(pgtrigger.EngineName)
	if !ok {
		t.Fatal("postgres-trigger engine not registered")
	}
	store := runTriggerChainFull(ctx, t, src, sourceDSN, pgtrigger.EngineName, func(tok string) (int64, error) {
		return pgtrigger.AppliedLastID(tok)
	})
	full, err := lineage.ReadManifest(ctx, store)
	if err != nil {
		t.Fatalf("read full manifest: %v", err)
	}
	// Seed the target from the full BEFORE any incremental exists: a
	// restore run after it would replay the whole chain and leave the
	// broker nothing to apply (a first cut of this lane did exactly that,
	// and passed without the broker decoding a single change).
	pgEng, _ := engines.Get("postgres")
	if err := (&backup.Restore{Target: pgEng, TargetDSN: targetDSN, Store: store}).Run(ctx); err != nil {
		t.Fatalf("seed restore of the full: %v", err)
	}
	if n := len(pgNumsText(t, targetDSN)); n != 1 {
		t.Fatalf("seeded target holds %d rows; want the full's 1 — the broker must be the only path to the rest", n)
	}
	for _, c := range pgTriggerNumericCells {
		applyDDL(t, sourceDSN, fmt.Sprintf(
			`INSERT INTO nums (id, nu, np, f8, f4, na, js) VALUES (%d, '%s', '%s', '%s', '%s', '%s', '%s');`,
			c.id, c.nu, c.np, c.f8, c.f4, c.na, c.js,
		))
	}
	runTriggerChainIncrementalAndRestoreN(ctx, t, src, sourceDSN, store, "", "", len(pgTriggerNumericCells))
	brokerCancel, brokerDone := runBrokerInGoroutine(t, targetDSN, "numbers-broker", store, brokerOpts{
		PollInterval: time.Second,
		AtChainID:    full.BackupID,
	})
	defer brokerCancel()

	want := pgNumsText(t, sourceDSN)
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) && len(pgNumsText(t, targetDSN)) < len(want) {
		time.Sleep(500 * time.Millisecond)
	}
	got := pgNumsText(t, targetDSN)
	brokerCancel()
	select {
	case err := <-brokerDone:
		if err != nil {
			t.Errorf("broker.Run = %v; want nil", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("broker did not exit within 15s of cancel")
	}
	if len(got) != len(want) {
		t.Fatalf("broker target has %d rows; source has %d", len(got), len(want))
	}
	for id, w := range want {
		for col, wv := range w {
			if got[id][col] != wv {
				t.Errorf("row %d %s: broker applied %q; source %q", id, col, got[id][col], wv)
			}
		}
	}
}

// canonicalDecimal strips the fractional trailing zeros (and a bare point)
// so a DECIMAL(65,30) rendering compares by exact value with PG's.
func canonicalDecimal(s string) string {
	if strings.Contains(s, ".") {
		s = strings.TrimRight(s, "0")
		s = strings.TrimSuffix(s, ".")
	}
	return s
}
