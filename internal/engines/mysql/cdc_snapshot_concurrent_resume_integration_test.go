//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration test for ADR-0111 native-MySQL resumable cold-copy via
// re-snapshot-from-cursor. Boots the shared MySQL container, opens a
// CONCURRENT snapshot (copy_table_parallelism=2), injects a CLASSIFIED
// source-read drop mid-copy on ONE table, and asserts the genuine recovery:
//
//   - the reader RE-SNAPSHOTS (real FTWRL on the container → P′) instead of
//     aborting the whole copy;
//   - completed tables are SKIPPED, the dropped KEYED table RESUMES from its
//     PK cursor (WHERE pk > lastpk) and converges to the EXACT source count
//     and value set (no gap, no dup — byte-identical to a clean run);
//   - the CDC anchor (stream.Position) stays at the ORIGINAL position P across
//     the recovery (the §3 value-fidelity invariant — NEVER advanced to P′);
//   - a KEYLESS-table variant re-reads from the start (at-least-once, Bug 143:
//     loss-free, every source value present).

package mysql

import (
	"context"
	"fmt"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// keyedSnapTable builds the IR table for a KEYED resume table: id PK + a value
// column, WITH the PrimaryKey index set so tableHasOrderablePK is true and the
// reader takes the cursor-paginated resume path. (concSnapTable omits the PK,
// which would route to the keyless path.)
func keyedSnapTable(name string) *ir.Table {
	return &ir.Table{
		Name: name,
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64, AutoIncrement: true}},
			{Name: "v", Type: ir.Varchar{Length: 255}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
	}
}

// TestNativeConcurrentResume_KeyedFromCursor is the load-bearing ADR-0111
// pin: a mid-copy source-read drop on a KEYED table re-snapshots and resumes
// from the PK cursor, converging exactly while the CDC anchor stays at P.
func TestNativeConcurrentResume_KeyedFromCursor(t *testing.T) {
	dsn, cleanup := startMySQLForSnapshotCDC(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE r_a (id BIGINT NOT NULL AUTO_INCREMENT, v VARCHAR(255), PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		CREATE TABLE r_b (id BIGINT NOT NULL AUTO_INCREMENT, v VARCHAR(255), PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		CREATE TABLE r_c (id BIGINT NOT NULL AUTO_INCREMENT, v VARCHAR(255), PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		CREATE TABLE r_d (id BIGINT NOT NULL AUTO_INCREMENT, v VARCHAR(255), PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`
	applyMySQLSnap(t, dsn, seedDDL)

	tables := []string{"r_a", "r_b", "r_c", "r_d"}
	// r_a is large enough that an injected drop after 40 rows lands MID-table.
	wantCounts := map[string]int{"r_a": 120, "r_b": 11, "r_c": 13, "r_d": 17}
	for tbl, n := range wantCounts {
		var b []byte
		for i := 0; i < n; i++ {
			b = append(b, []byte(fmt.Sprintf("INSERT INTO %s (v) VALUES ('%s-%d');", tbl, tbl, i))...)
		}
		applyMySQLSnap(t, dsn, string(b))
	}

	// Shrink the page size so a 120-row table pages multiple times → the drop
	// lands mid-table with a non-empty cursor (the resume-WHERE-pk>cursor path).
	defer restoreBatchSize(nativeResumeBatchSize)
	nativeResumeBatchSize = 30

	// Inject ONE classified mid-table drop on r_a after 40 rows. After it
	// fires once it disarms, so the recovery's re-read succeeds.
	var fired bool
	concurrentDropInjector = func(tableName string, rowsHandedOff int) error {
		if !fired && tableName == "r_a" && rowsHandedOff >= 40 {
			fired = true
			return &retriableTestErr{} // classified → triggers re-snapshot recovery
		}
		return nil
	}
	defer func() { concurrentDropInjector = nil }()

	eng := Engine{Flavor: FlavorVanilla}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	concDSN := dsn + "&copy_table_parallelism=2"
	stream, err := eng.OpenSnapshotStreamForTables(ctx, concDSN, tables)
	if err != nil {
		t.Fatalf("OpenSnapshotStreamForTables(concurrent): %v", err)
	}
	defer func() { _ = stream.Close() }()

	// Capture the ORIGINAL CDC anchor P before any read/recovery.
	anchorBefore := stream.Position

	// Drain r_a FIRST — this is the table the drop fires on. The recovery is
	// transparent: ReadRows produces ONE continuous channel that re-snapshots
	// + resumes from the cursor under the hood.
	rowsA := drainAllRows(t, ctx, stream.Rows, keyedSnapTable("r_a"))
	if !fired {
		t.Fatal("drop injector never fired; the test did not exercise the recovery path")
	}
	if got := len(rowsA); got != wantCounts["r_a"] {
		t.Fatalf("r_a count after resume = %d; want %d (a gap or dup across the re-snapshot resume — silent loss)", got, wantCounts["r_a"])
	}
	assertNoDupNoGapKeyed(t, rowsA, wantCounts["r_a"])

	// The remaining tables drain cleanly (they were not dropped); exact counts.
	for _, tbl := range []string{"r_b", "r_c", "r_d"} {
		if got := len(drainAllRows(t, ctx, stream.Rows, keyedSnapTable(tbl))); got != wantCounts[tbl] {
			t.Errorf("table %q count = %d; want %d", tbl, got, wantCounts[tbl])
		}
	}

	// THE VALUE-FIDELITY INVARIANT (ADR-0111 §3): the CDC anchor MUST be
	// unchanged across the recovery — NEVER advanced to P′.
	if stream.Position != anchorBefore {
		t.Fatalf("CDC anchor CHANGED across re-snapshot recovery:\n before=%+v\n after =%+v\n"+
			"anchoring at the later position would SKIP changes on completed tables — SILENT LOSS (ADR-0111 §3)",
			anchorBefore, stream.Position)
	}

	// And CDC from that ORIGINAL anchor still streams cleanly: a post-snapshot
	// insert surfaces (the handoff survived the re-snapshot).
	applyMySQLSnap(t, dsn, "INSERT INTO r_b (v) VALUES ('post-snapshot-after-resume');")
	changes, err := stream.Changes.StreamChanges(ctx, stream.Position)
	if err != nil {
		t.Fatalf("StreamChanges from original anchor: %v", err)
	}
	got := drainSnapshotChanges(t, ctx, changes, 1, 30*time.Second)
	if len(got) != 1 {
		t.Fatalf("CDC from original anchor got %d changes; want 1 (clean handoff survived the re-snapshot)", len(got))
	}
}

// TestNativeConcurrentResume_KeylessAtLeastOnce pins the keyless-table
// variant: a mid-copy drop on a keyless table re-snapshots and re-reads from
// the start → at-least-once (loss-free, possible dups, Bug 143). Every source
// value MUST be present; the CDC anchor stays at P.
func TestNativeConcurrentResume_KeylessAtLeastOnce(t *testing.T) {
	dsn, cleanup := startMySQLForSnapshotCDC(t)
	defer cleanup()

	// k_none has NO primary key (keyless); k_keyed is a sibling so the scope
	// has >1 table (the concurrent opener gates on it).
	const seedDDL = `
		CREATE TABLE k_none (v VARCHAR(255)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		CREATE TABLE k_keyed (id BIGINT NOT NULL AUTO_INCREMENT, v VARCHAR(255), PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`
	applyMySQLSnap(t, dsn, seedDDL)

	const keylessN = 50
	srcVals := map[string]bool{}
	var b []byte
	for i := 0; i < keylessN; i++ {
		v := fmt.Sprintf("k-%d", i)
		srcVals[v] = true
		b = append(b, []byte(fmt.Sprintf("INSERT INTO k_none (v) VALUES ('%s');", v))...)
	}
	applyMySQLSnap(t, dsn, string(b))
	applyMySQLSnap(t, dsn, "INSERT INTO k_keyed (v) VALUES ('x'), ('y');")

	// Inject ONE drop on the keyless table after 20 rows → re-snapshot +
	// re-read from start.
	var fired bool
	concurrentDropInjector = func(tableName string, rowsHandedOff int) error {
		if !fired && tableName == "k_none" && rowsHandedOff >= 20 {
			fired = true
			return &retriableTestErr{}
		}
		return nil
	}
	defer func() { concurrentDropInjector = nil }()

	eng := Engine{Flavor: FlavorVanilla}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	concDSN := dsn + "&copy_table_parallelism=2"
	stream, err := eng.OpenSnapshotStreamForTables(ctx, concDSN, []string{"k_none", "k_keyed"})
	if err != nil {
		t.Fatalf("OpenSnapshotStreamForTables(concurrent): %v", err)
	}
	defer func() { _ = stream.Close() }()

	anchorBefore := stream.Position

	// k_none is keyless: build the IR table WITHOUT a PrimaryKey so the reader
	// takes the keyless full-scan + restart-from-start recovery path.
	keylessTbl := &ir.Table{
		Name:    "k_none",
		Columns: []*ir.Column{{Name: "v", Type: ir.Varchar{Length: 255}}},
	}
	rows := drainAllRows(t, ctx, stream.Rows, keylessTbl)
	if !fired {
		t.Fatal("drop injector never fired; the keyless recovery path was not exercised")
	}

	// At-least-once: every source value present (loss-free); count >= source
	// (dups allowed — the keyless contract on recovery).
	seen := map[string]bool{}
	for _, r := range rows {
		if v, ok := r["v"].(string); ok {
			seen[v] = true
		}
	}
	for v := range srcVals {
		if !seen[v] {
			t.Errorf("keyless recovery LOST source value %q (loss-free is violated)", v)
		}
	}
	if len(rows) < keylessN {
		t.Errorf("keyless recovery emitted %d rows; want >= %d (at-least-once)", len(rows), keylessN)
	}

	// CDC anchor unchanged across the keyless recovery too.
	if stream.Position != anchorBefore {
		t.Fatalf("CDC anchor CHANGED across keyless re-snapshot recovery: before=%+v after=%+v", anchorBefore, stream.Position)
	}
}

// TestNativeConcurrentResume_KeyedFamilies closes the Bug-74 coverage gap the
// value-fidelity review flagged: the genuine drop→re-snapshot→resume-from-cursor
// round-trip exercised for NON-INTEGER keyed PKs — a TEMPORAL PK (DATETIME(6),
// whose cursor takes the load-bearing CAST-to-CHAR path in row_reader_batch) and
// a COMPOSITE (BIGINT, VARCHAR) PK (whose cursor is a multi-column
// row-constructor `(a,b) > (?,?)`). Each must converge to its EXACT PK set with
// no gap/dup across the recovery, anchor unchanged.
func TestNativeConcurrentResume_KeyedFamilies(t *testing.T) {
	dsn, cleanup := startMySQLForSnapshotCDC(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE r_ts   (ts DATETIME(6) NOT NULL, v VARCHAR(255), PRIMARY KEY (ts)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		CREATE TABLE r_comp (a BIGINT NOT NULL, b VARCHAR(64) NOT NULL, v VARCHAR(255), PRIMARY KEY (a, b)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`
	applyMySQLSnap(t, dsn, seedDDL)

	const nTs, nComp = 80, 80
	var tb []byte
	for i := 0; i < nTs; i++ {
		// Distinct, ordered microsecond timestamps so the keyed cursor advances.
		tb = append(tb, []byte(fmt.Sprintf("INSERT INTO r_ts (ts, v) VALUES ('2020-01-01 00:00:00.%06d', 'ts-%d');", i+1, i))...)
	}
	applyMySQLSnap(t, dsn, string(tb))
	var cb []byte
	for i := 0; i < nComp; i++ {
		// (a,b) tuples distinct on the composite key (a alone repeats across b).
		cb = append(cb, []byte(fmt.Sprintf("INSERT INTO r_comp (a, b, v) VALUES (%d, 'b%d', 'c-%d');", i%10, i, i))...)
	}
	applyMySQLSnap(t, dsn, string(cb))

	// Shrink the page size so the drop lands mid-table with a non-empty cursor.
	defer restoreBatchSize(nativeResumeBatchSize)
	nativeResumeBatchSize = 30

	// Re-arming injector: ONE mid-table drop on EACH family table.
	firedTs, firedComp := false, false
	concurrentDropInjector = func(tableName string, rowsHandedOff int) error {
		if !firedTs && tableName == "r_ts" && rowsHandedOff >= 40 {
			firedTs = true
			return &retriableTestErr{}
		}
		if !firedComp && tableName == "r_comp" && rowsHandedOff >= 40 {
			firedComp = true
			return &retriableTestErr{}
		}
		return nil
	}
	defer func() { concurrentDropInjector = nil }()

	eng := Engine{Flavor: FlavorVanilla}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	concDSN := dsn + "&copy_table_parallelism=2"
	stream, err := eng.OpenSnapshotStreamForTables(ctx, concDSN, []string{"r_ts", "r_comp"})
	if err != nil {
		t.Fatalf("OpenSnapshotStreamForTables(concurrent): %v", err)
	}
	defer func() { _ = stream.Close() }()
	anchorBefore := stream.Position

	tsTbl := &ir.Table{
		Name:       "r_ts",
		Columns:    []*ir.Column{{Name: "ts", Type: ir.DateTime{}}, {Name: "v", Type: ir.Varchar{Length: 255}}},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "ts"}}},
	}
	compTbl := &ir.Table{
		Name: "r_comp",
		Columns: []*ir.Column{
			{Name: "a", Type: ir.Integer{Width: 64}},
			{Name: "b", Type: ir.Varchar{Length: 64}},
			{Name: "v", Type: ir.Varchar{Length: 255}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "a"}, {Column: "b"}}},
	}

	tsRows := drainAllRows(t, ctx, stream.Rows, tsTbl)
	compRows := drainAllRows(t, ctx, stream.Rows, compTbl)
	if !firedTs || !firedComp {
		t.Fatalf("drop injectors did not both fire (ts=%v comp=%v); recovery not exercised for both families", firedTs, firedComp)
	}
	assertExactDistinct(t, "r_ts", tsRows, func(r ir.Row) string { return fmt.Sprintf("%v", r["ts"]) }, nTs)
	assertExactDistinct(t, "r_comp", compRows, func(r ir.Row) string { return fmt.Sprintf("%v|%v", r["a"], r["b"]) }, nComp)

	if stream.Position != anchorBefore {
		t.Fatalf("CDC anchor CHANGED across family re-snapshot recovery: before=%+v after=%+v (silent loss, ADR-0111 §3)", anchorBefore, stream.Position)
	}
}

// assertExactDistinct verifies the drained rows form EXACTLY wantN distinct PK
// keys (keyFn) with no gap (count == wantN) and no dup (distinct == count) —
// byte-identical to a clean run across the re-snapshot resume.
func assertExactDistinct(t *testing.T, table string, rows []ir.Row, keyFn func(ir.Row) string, wantN int) {
	t.Helper()
	seen := make(map[string]int, len(rows))
	for _, r := range rows {
		seen[keyFn(r)]++
	}
	if len(rows) != wantN {
		t.Errorf("%s: %d rows after resume; want %d (gap or dup)", table, len(rows), wantN)
	}
	if len(seen) != wantN {
		t.Errorf("%s: %d DISTINCT keys after resume; want %d (a dup or a missing key = gap/dup across the cursor resume)", table, len(seen), wantN)
	}
	for k, c := range seen {
		if c != 1 {
			t.Errorf("%s: key %q appears %d times after resume (dup across re-snapshot)", table, k, c)
		}
	}
}

// assertNoDupNoGapKeyed checks a keyed table's drained rows form the exact
// contiguous id set [1..wantN] with no duplicate and no gap (byte-identical to
// a clean run).
func assertNoDupNoGapKeyed(t *testing.T, rows []ir.Row, wantN int) {
	t.Helper()
	seen := make(map[int64]int, len(rows))
	for _, r := range rows {
		id, ok := toInt64(r["id"])
		if !ok {
			t.Fatalf("row id not int-like: %#v", r["id"])
		}
		seen[id]++
	}
	for id := int64(1); id <= int64(wantN); id++ {
		switch seen[id] {
		case 0:
			t.Errorf("id %d MISSING after resume (gap — silent loss)", id)
		case 1:
			// exactly once — correct
		default:
			t.Errorf("id %d appears %d times after resume (dup)", id, seen[id])
		}
	}
}

// toInt64 coerces a driver-decoded id value (int64 / int / uint64) to int64.
func toInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case int64:
		return n, true
	case int:
		return int64(n), true
	case uint64:
		return int64(n), true
	case int32:
		return int64(n), true
	default:
		return 0, false
	}
}

// restoreBatchSize restores the page-size knob after a test shrinks it.
func restoreBatchSize(v int) { nativeResumeBatchSize = v }
