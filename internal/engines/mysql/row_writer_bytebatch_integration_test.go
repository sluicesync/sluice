//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration pins for the ADR-0150 byte-targeted INSERT batching,
// against a real MySQL container. Two legs:
//
//   - a value-matrix table (the same IR-typed value contract shape as
//     TestRowWriter_RoundTrip) large enough to force multiple
//     byte-target flushes, written through the PlanetScale-flavor
//     writer (BulkLoadBatchedInsert — the PS bulk-load path this
//     chunk exists for) and read back byte-exact;
//   - a >1 MiB single row, which must ship alone (never split, never
//     refused) and land byte-exact.
//
// The bulkFlushHookForTest seam pins that the byte-targeted
// composition is actually taken on the real write path — statements
// well past the old 500-row cap — not just that the rows arrive.

package mysql

import (
	"context"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestRowWriter_ByteTargetedBatch_ValueMatrixRoundTrip streams a
// value-matrix corpus through the PlanetScale-flavor batched-INSERT
// path and asserts (a) at least one composed statement carries more
// rows than the pre-ADR-0150 500-row cap, (b) no statement exceeds the
// composer's ceilings, and (c) every value reads back byte-exact.
func TestRowWriter_ByteTargetedBatch_ValueMatrixRoundTrip(t *testing.T) {
	dsn, cleanup := startMySQL(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	const ddl = `
		CREATE TABLE bb_matrix (
			id          BIGINT UNSIGNED NOT NULL,
			active      TINYINT(1)      NOT NULL,
			name        VARCHAR(64)     NOT NULL,
			price       DECIMAL(10,2)   NOT NULL,
			role        ENUM('admin','user','guest') NOT NULL,
			tags        SET('go','sql','mysql','postgres') NOT NULL,
			payload     JSON            NULL,
			data        BLOB            NULL,
			note        VARCHAR(600)    NOT NULL,
			created_at  TIMESTAMP(0)    NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`
	applyDDL(t, dsn, ddl)

	sr, err := Engine{}.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer closeIf(sr)
	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	table := findTable(schema, "bb_matrix")
	if table == nil {
		t.Fatalf("bb_matrix table not found; have %v", tableNames(schema))
	}

	// ~3,000 rows at ~550 estimated bytes each ≈ 1.6 MiB — enough to
	// cross the 1 MiB byte target mid-stream, so the corpus composes
	// into (at least) one target-sized statement plus a remainder.
	const total = 3000
	createdAt := time.Date(2026, 7, 4, 12, 34, 56, 0, time.UTC)
	roles := []string{"admin", "user", "guest"}
	makeRow := func(i int) ir.Row {
		row := ir.Row{
			"id":     uint64(i + 1),
			"active": i%2 == 0,
			"name":   "user-" + strings.Repeat("n", i%17),
			"price":  "19.95",
			"role":   roles[i%len(roles)],
			"tags":   []string{"go", "sql"},
			// Single-key JSON: MySQL's binary JSON re-emits object keys
			// sorted (length, then alphabetical), so a multi-key literal
			// would not compare byte-exact even when faithfully stored.
			"payload":    []byte(`{"plan": "` + strings.Repeat("f", i%5+1) + `"}`),
			"data":       []byte{0xde, 0xad, byte(i), byte(i >> 8)},
			"note":       strings.Repeat("x", 500),
			"created_at": createdAt,
		}
		if i%7 == 0 { // NULL legs stay in the matrix
			row["payload"] = nil
			row["data"] = nil
		}
		return row
	}

	// PlanetScale flavor ⇒ BulkLoadBatchedInsert: the exact PS
	// bulk-load path (vanilla would take LOAD DATA on this container).
	rwGeneric, err := Engine{Flavor: FlavorPlanetScale}.OpenRowWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenRowWriter: %v", err)
	}
	defer closeIf(rwGeneric)
	rw := rwGeneric.(*RowWriter)
	if !rw.tierCPUBoundTarget {
		t.Errorf("PlanetScale-flavor OpenRowWriter did not wire tierCPUBoundTarget (the ADR-0150 operator hint gate)")
	}

	var flushRows []int
	bulkFlushHookForTest = func(rows int, _ int64) { flushRows = append(flushRows, rows) }
	defer func() { bulkFlushHookForTest = nil }()

	in := make(chan ir.Row, 64)
	go func() {
		defer close(in)
		for i := 0; i < total; i++ {
			in <- makeRow(i)
		}
	}()
	if err := rw.WriteRows(ctx, table, in); err != nil {
		t.Fatalf("WriteRows: %v", err)
	}

	// Composition: the byte target must have fired past the old cap.
	sum, maxRows := 0, 0
	for _, n := range flushRows {
		sum += n
		if n > maxRows {
			maxRows = n
		}
	}
	if sum != total {
		t.Errorf("flush hook saw %d rows across %d statements; want %d (no loss, no dup)", sum, len(flushRows), total)
	}
	if maxRows <= 500 {
		t.Errorf("largest composed statement carried %d rows (statements: %v); want >500 — the ADR-0150 byte target should out-batch the old row cap on this corpus", maxRows, flushRows)
	}
	if maxRows > defaultMaxRowsPerBatch {
		t.Errorf("a statement carried %d rows, past the %d safety ceiling", maxRows, defaultMaxRowsPerBatch)
	}

	// Fidelity: read back and compare field-by-field.
	rr, err := Engine{}.OpenRowReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenRowReader: %v", err)
	}
	defer closeIf(rr)
	out, err := rr.ReadRows(ctx, table)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	var got []ir.Row
	for row := range out {
		got = append(got, row)
	}
	if rrConcrete, ok := rr.(*RowReader); ok {
		if err := rrConcrete.Err(); err != nil {
			t.Fatalf("Err after streaming: %v", err)
		}
	}
	if len(got) != total {
		t.Fatalf("read back %d rows; want %d", len(got), total)
	}
	// Rows come back in PK order; the corpus was written id=1..total.
	mismatches := 0
	for i := 0; i < total && mismatches < 10; i++ {
		want := makeRow(i)
		for col, wantVal := range want {
			if !rowValueEqual(got[i][col], wantVal) {
				t.Errorf("row[%d].%s = %#v (%T); want %#v (%T)",
					i, col, got[i][col], got[i][col], wantVal, wantVal)
				mismatches++
			}
		}
	}
}

// TestRowWriter_ByteTargetedBatch_OversizeRowShipsAlone pins the
// never-split contract end-to-end: a first row whose value bytes alone
// exceed the 1 MiB target ships as a one-row statement (the following
// small rows land in the next statement), and the >1 MiB value reads
// back byte-exact.
func TestRowWriter_ByteTargetedBatch_OversizeRowShipsAlone(t *testing.T) {
	dsn, cleanup := startMySQL(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	const ddl = `
		CREATE TABLE bb_oversize (
			id    BIGINT   NOT NULL,
			body  LONGTEXT NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`
	applyDDL(t, dsn, ddl)

	sr, err := Engine{}.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer closeIf(sr)
	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	table := findTable(schema, "bb_oversize")
	if table == nil {
		t.Fatalf("bb_oversize table not found; have %v", tableNames(schema))
	}

	rwGeneric, err := Engine{Flavor: FlavorPlanetScale}.OpenRowWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenRowWriter: %v", err)
	}
	defer closeIf(rwGeneric)
	rw := rwGeneric.(*RowWriter)

	var flushRows []int
	bulkFlushHookForTest = func(rows int, _ int64) { flushRows = append(flushRows, rows) }
	defer func() { bulkFlushHookForTest = nil }()

	// ~2 MiB of deterministic, position-sensitive content so a silent
	// truncation or reorder cannot compare equal.
	huge := strings.Repeat("0123456789abcdef", (2<<20)/16)
	rows := []ir.Row{
		{"id": int64(1), "body": huge},
		{"id": int64(2), "body": "small-after-1"},
		{"id": int64(3), "body": "small-after-2"},
	}

	in := make(chan ir.Row, len(rows))
	for _, r := range rows {
		in <- r
	}
	close(in)
	if err := rw.WriteRows(ctx, table, in); err != nil {
		t.Fatalf("WriteRows: %v", err)
	}

	// The oversize first row fills the batch on its own → statement of
	// 1; the two small rows drain on the final flush → statement of 2.
	if len(flushRows) != 2 || flushRows[0] != 1 || flushRows[1] != 2 {
		t.Errorf("statement composition %v; want [1 2] (oversize row ships alone, small rows drain together)", flushRows)
	}

	rr, err := Engine{}.OpenRowReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenRowReader: %v", err)
	}
	defer closeIf(rr)
	out, err := rr.ReadRows(ctx, table)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	var got []ir.Row
	for row := range out {
		got = append(got, row)
	}
	if rrConcrete, ok := rr.(*RowReader); ok {
		if err := rrConcrete.Err(); err != nil {
			t.Fatalf("Err after streaming: %v", err)
		}
	}
	if len(got) != len(rows) {
		t.Fatalf("read back %d rows; want %d", len(got), len(rows))
	}
	for i, want := range rows {
		for col, wantVal := range want {
			if rowValueEqual(got[i][col], wantVal) {
				continue
			}
			if wantStr, ok := wantVal.(string); ok {
				gotStr, _ := got[i][col].(string)
				t.Errorf("row[%d].%s mismatch (got len %d, want len %d)",
					i, col, len(gotStr), len(wantStr))
				continue
			}
			t.Errorf("row[%d].%s = %#v; want %#v", i, col, got[i][col], wantVal)
		}
	}
}
