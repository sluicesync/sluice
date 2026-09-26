//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/engines/pgtrigger"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
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
	fa string // double precision[]
}{
	{101, "123456789012345678.123456789012", "123456789012345678.123456789012", "1.5", "1.25", "{1.1,22222222222222222222.2}", `{"big": 12345678901234567890, "dec": 0.1234567890123456789012345678901234567890}`, "{1.5,2.25}"},
	{102, "0.1234567890123456789012345678901234567890", "0.123456789012", "0.1", "0.1", "{{1.5,2.25},{-3.125,NULL}}", `{"n": [1.0, 2.50, 9007199254740993]}`, "{{0.1,NULL},{1.5e-7,1e21}}"},
	{103, "-98765432109876543210.5", "-12345678901234567890.5", "-2.5e-300", "3.4e38", "{0.000000000000000000001}", `{"neg": -0.0000000000000000001}`, "{-2.5e-300}"},
	{104, "1.500000", "1.500000000000", "123456789.123456789", "1", "{}", `{"zero": 0, "t": 1.500}`, "{}"},
	{105, "12345678901234567890", "12345678901234567890", "9007199254740993", "16777217", "{12345678901234567890}", `{"i": 9223372036854775808}`, "{9007199254740993}"},
	{106, "NaN", "0", "NaN", "Infinity", "{NaN}", `{"s": "1.5"}`, "{NaN,Infinity}"},
	// Below the smallest float64 (1e-400 → the float64 decode returned 0,
	// nil — silently), a trailing-zero scale on an UNCONSTRAINED numeric
	// (10.50 → 10.5), and a subnormal-range magnitude (1.234567e-320, where a
	// float64 keeps only a few significant digits).
	{107, "1e-400", "0", "1.234567e-320", "1.5e-7", "{1e-400,10.50}", `{"u": 1e-400, "t": 10.50}`, "{5e-324,NULL}"},
	{108, "10.50", "10.500000000000", "1e21", "1.4e-45", "{1.234567e-320}", `{"sub": 1.234567e-320}`, "{1.234567e-320}"},
	{109, "1.234567e-320", "0.000000000001", "5e-324", "3.4028235e38", "{10.50,1.500}", `{"z": 0.0}`, "{0.1,123456789.123456789}"},
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
			na NUMERIC[], js JSONB, fa DOUBLE PRECISION[]
		);
		INSERT INTO nums (id, nu, np, f8, f4, na, js, fa) VALUES (1, 1, 1, 1, 1, '{1}', '{"seed": 1}', '{1}');
		-- Written BEFORE the full: graded through the full's data chunks
		-- (the delegated postgres row reader), a different path.
		INSERT INTO nums (id, nu, np, f8, f4, na, js, fa) VALUES (2,
			'98765432109876543210.123456789012345678901234567890', '-123456789012345678.123456789012',
			'0.1', '0.1', '{1.10,2.200}', '{"big": 12345678901234567890}', '{0.1,1.5e-7}');
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
			`INSERT INTO nums (id, nu, np, f8, f4, na, js, fa) VALUES (%d, '%s', '%s', '%s', '%s', '%s', '%s', '%s');`,
			c.id, c.nu, c.np, c.f8, c.f4, c.na, c.js, c.fa,
		))
	}
	c0 := pgTriggerNumericCells[0]
	applyDDL(t, sourceDSN, fmt.Sprintf(
		`UPDATE nums SET nu = '%s', np = '%s', f8 = '%s', f4 = '%s', na = '%s', js = '%s', fa = '%s' WHERE id = 1;`,
		c0.nu, c0.np, c0.f8, c0.f4, c0.na, c0.js, c0.fa,
	))

	runTriggerChainIncrementalAndRestoreN(ctx, t, src, sourceDSN, store, "postgres", targetDSN, len(pgTriggerNumericCells)+1)
	if got := incrementalFormatVersions(ctx, t, store); len(got) != 1 || got[0] != irbackup.FormatVersionExactNumbers {
		t.Errorf("incremental format versions = %v; want one segment stamped FormatVersionExactNumbers=%d — its chunks carry "+
			"exact-text numbers a pre-v0.156.4 reader would round, so it must refuse the segment instead",
			got, irbackup.FormatVersionExactNumbers)
	}

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

// incrementalFormatVersions returns the recorded FormatVersion of every
// incremental manifest in store.
func incrementalFormatVersions(ctx context.Context, t *testing.T, store irbackup.Store) []int {
	t.Helper()
	records, err := lineage.ListAllManifestsViaWalk(ctx, store)
	if err != nil {
		t.Fatalf("ListAllManifestsViaWalk: %v", err)
	}
	var out []int
	for _, r := range records {
		if r.Manifest.Kind == irbackup.BackupKindIncremental {
			out = append(out, r.Manifest.FormatVersion)
		}
	}
	return out
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
		`SELECT id, nu::text, np::text, f8::text, f4::text, na::text, js::text, fa::text FROM nums ORDER BY id`)
	if err != nil {
		t.Fatalf("read nums: %v", err)
	}
	defer func() { _ = rows.Close() }()
	out := map[int64]map[string]string{}
	for rows.Next() {
		var id int64
		var nu, np, f8, f4, na, js, fa sql.NullString
		if err := rows.Scan(&id, &nu, &np, &f8, &f4, &na, &js, &fa); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out[id] = map[string]string{"nu": nu.String, "np": np.String, "f8": f8.String, "f4": f4.String, "na": na.String, "js": js.String, "fa": fa.String}
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

// TestBackupChain_PGTrigger_FloatsAndArraysRestoreExactly_ToMySQL extends
// the MySQL lane to the other number families a postgres-trigger chain
// hands the MySQL applier as json.Number: float8 → DOUBLE, real → FLOAT and
// numeric[] → JSON. Values are chosen so the target type can hold each one
// exactly, so a difference is the chain's loss and not the mapping's.
// Independent expected value: the source's own `col::text`, compared by
// value (DOUBLE renders its own shortest spelling; a FLOAT is read as a
// DOUBLE because MySQL displays FLOAT to six significant digits; a JSON array's
// elements are compared as exact decimals).
func TestBackupChain_PGTrigger_FloatsAndArraysRestoreExactly_ToMySQL(t *testing.T) {
	sourceDSN, _, pgCleanup := startPostgres(t)
	defer pgCleanup()
	_, mysqlTarget, myCleanup := startMySQL(t)
	defer myCleanup()
	applyDDL(t, sourceDSN, `
		CREATE TABLE floatsmy (id BIGINT PRIMARY KEY, f8 DOUBLE PRECISION, f4 REAL, na NUMERIC[]);
		INSERT INTO floatsmy (id, f8, f4, na) VALUES (1, 1, 1, '{1}');
	`)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	if _, err := pgtrigger.Setup(ctx, sourceDSN, pgtrigger.SetupOptions{Tables: []string{"floatsmy"}, Schema: "public"}); err != nil {
		t.Fatalf("pgtrigger.Setup: %v", err)
	}
	src, ok := engines.Get(pgtrigger.EngineName)
	if !ok {
		t.Fatal("postgres-trigger engine not registered")
	}
	store := runTriggerChainFull(ctx, t, src, sourceDSN, pgtrigger.EngineName, func(tok string) (int64, error) {
		return pgtrigger.AppliedLastID(tok)
	})
	cells := []struct{ id, f8, f4, na string }{
		{"301", "1.5e-7", "1.25", "{1.5,2.25,-3.125}"},
		{"302", "0.1", "0.1", "{10.50,NULL}"},
		{"303", "-2.5e-300", "3.4e38", "{0.000001}"},
		{"304", "1e21", "1.5e-7", "{}"},
		{"305", "123456789.123456789", "16777217", "{12345.6789}"},
	}
	for _, c := range cells {
		applyDDL(t, sourceDSN, fmt.Sprintf(`INSERT INTO floatsmy (id, f8, f4, na) VALUES (%s, '%s', '%s', '%s');`, c.id, c.f8, c.f4, c.na))
	}
	runTriggerChainIncrementalAndRestoreN(ctx, t, src, sourceDSN, store, "mysql", mysqlTarget, len(cells))

	want := floatRowsText(t, "pgx", sourceDSN, `SELECT id, f8::text, f4::text, array_to_json(na)::text FROM floatsmy ORDER BY id`)
	got := floatRowsText(t, "mysql", mysqlTarget, `SELECT id, CAST(f8 AS CHAR), CAST(CAST(f4 AS DOUBLE) AS CHAR), CAST(na AS CHAR) FROM floatsmy ORDER BY id`)
	if len(got) != len(want) {
		t.Fatalf("restored %d rows; source has %d", len(got), len(want))
	}
	for id, w := range want {
		g := got[id]
		if g == nil {
			t.Errorf("row %d missing from the restore", id)
			continue
		}
		if !sameFloat(g[0], w[0], 64) {
			t.Errorf("row %d f8: restored %q; source %q", id, g[0], w[0])
		}
		if !sameFloat(g[1], w[1], 32) {
			t.Errorf("row %d f4: restored %q; source %q", id, g[1], w[1])
		}
		if !sameJSONNumbers(t, g[2], w[2]) {
			t.Errorf("row %d na: restored %q; source %q", id, g[2], w[2])
		}
	}
}

// TestBackupChain_PGTrigger_OverScaleRestoreRefuses_ToMySQL pins the win the
// exact read buys on a MySQL target: a numeric with 31 fractional digits used
// to reach the MySQL applier as a float64 and land rounded; it now reaches it
// as the exact json.Number, and the DECIMAL scale guard refuses it with
// DECIMAL-SCALE-EXCEEDED instead of rounding it into DECIMAL(65,30).
func TestBackupChain_PGTrigger_OverScaleRestoreRefuses_ToMySQL(t *testing.T) {
	sourceDSN, _, pgCleanup := startPostgres(t)
	defer pgCleanup()
	_, mysqlTarget, myCleanup := startMySQL(t)
	defer myCleanup()
	applyDDL(t, sourceDSN, `
		CREATE TABLE overscale (id BIGINT PRIMARY KEY, nu NUMERIC);
		INSERT INTO overscale (id, nu) VALUES (1, 1);
	`)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	if _, err := pgtrigger.Setup(ctx, sourceDSN, pgtrigger.SetupOptions{Tables: []string{"overscale"}, Schema: "public"}); err != nil {
		t.Fatalf("pgtrigger.Setup: %v", err)
	}
	src, ok := engines.Get(pgtrigger.EngineName)
	if !ok {
		t.Fatal("postgres-trigger engine not registered")
	}
	store := runTriggerChainFull(ctx, t, src, sourceDSN, pgtrigger.EngineName, func(tok string) (int64, error) {
		return pgtrigger.AppliedLastID(tok)
	})
	applyDDL(t, sourceDSN, `INSERT INTO overscale (id, nu) VALUES (2, '0.1234567890123456789012345678901');`)
	runTriggerChainIncrementalAndRestoreN(ctx, t, src, sourceDSN, store, "", "", 1)

	eng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	err := (&backup.ChainRestore{Target: eng, TargetDSN: mysqlTarget, Store: store}).Run(ctx)
	if err == nil || !strings.Contains(err.Error(), "DECIMAL-SCALE-EXCEEDED") {
		t.Fatalf("chain restore of a 31-fractional-digit numeric into MySQL = %v; want a DECIMAL-SCALE-EXCEEDED refusal (the value "+
			"must not be rounded into DECIMAL(65,30))", err)
	}
}

// floatRowsText reads (id, a, b, c) rows as text.
func floatRowsText(t *testing.T, driver, dsn, query string) map[int64][]string {
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
		var a, b, c sql.NullString
		if err := rows.Scan(&id, &a, &b, &c); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out[id] = []string{a.String, b.String, c.String}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	return out
}

// sameFloat compares two server renderings of a float by value at the
// column's own width.
func sameFloat(a, b string, bits int) bool {
	x, errA := strconv.ParseFloat(a, bits)
	y, errB := strconv.ParseFloat(b, bits)
	return errA == nil && errB == nil && x == y
}

// sameJSONNumbers compares two JSON arrays element by element as exact
// decimals (a number or a numeric string on either side; null matches null).
func sameJSONNumbers(t *testing.T, a, b string) bool {
	t.Helper()
	parse := func(s string) []any {
		if s == "" {
			return nil
		}
		dec := json.NewDecoder(strings.NewReader(s))
		dec.UseNumber()
		var v []any
		if err := dec.Decode(&v); err != nil {
			t.Fatalf("parse JSON array %q: %v", s, err)
		}
		return v
	}
	x, y := parse(a), parse(b)
	if len(x) != len(y) {
		return false
	}
	for i := range x {
		if (x[i] == nil) != (y[i] == nil) {
			return false
		}
		if x[i] == nil {
			continue
		}
		ra, okA := new(big.Rat).SetString(fmt.Sprint(x[i]))
		rb, okB := new(big.Rat).SetString(fmt.Sprint(y[i]))
		if !okA || !okB || ra.Cmp(rb) != 0 {
			return false
		}
	}
	return true
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
			na NUMERIC[], js JSONB, fa DOUBLE PRECISION[]
		);
		INSERT INTO nums (id, nu, np, f8, f4, na, js, fa) VALUES (1, 1, 1, 1, 1, '{1}', '{"seed": 1}', '{1}');
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
			`INSERT INTO nums (id, nu, np, f8, f4, na, js, fa) VALUES (%d, '%s', '%s', '%s', '%s', '%s', '%s', '%s');`,
			c.id, c.nu, c.np, c.f8, c.f4, c.na, c.js, c.fa,
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
