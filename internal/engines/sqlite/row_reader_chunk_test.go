// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// schemaTable opens path, reads the schema, and returns the named table
// (with its PrimaryKey populated) so the batched-read helpers have a real
// *ir.Table to drive.
func schemaTable(t *testing.T, path, name string) *ir.Table {
	t.Helper()
	eng := Engine{}
	ctx := context.Background()
	sr, err := eng.OpenSchemaReader(ctx, path)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer func() { _ = sr.(*SchemaReader).Close() }()
	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	tbl := tableByName(schema, name)
	if tbl == nil {
		t.Fatalf("table %q missing", name)
	}
	return tbl
}

// openReader opens a fresh RowReader over path (caller closes).
func openReader(t *testing.T, path string) *RowReader {
	t.Helper()
	eng := Engine{}
	rr, err := eng.OpenRowReader(context.Background(), path)
	if err != nil {
		t.Fatalf("OpenRowReader: %v", err)
	}
	return rr.(*RowReader)
}

// pkOf extracts a row's PK tuple in PK-column order.
func pkOf(row ir.Row, table *ir.Table) []any {
	out := make([]any, len(table.PrimaryKey.Columns))
	for i, c := range table.PrimaryKey.Columns {
		out[i] = row[c.Column]
	}
	return out
}

// readBoundedAll drains a whole half-open (after, upTo] chunk through
// ReadRowsBatchBounded, looping the PK cursor with a deliberately SMALL
// page limit so the cursor+bound interaction is exercised across multiple
// pages (the way the orchestrator's copyChunk loop does). Returns every
// row in PK order.
func readBoundedAll(t *testing.T, rr *RowReader, table *ir.Table, after, upTo []any, limit int) []ir.Row {
	t.Helper()
	ctx := context.Background()
	var out []ir.Row
	cursor := after
	for {
		ch, err := rr.ReadRowsBatchBounded(ctx, table, cursor, upTo, limit)
		if err != nil {
			t.Fatalf("ReadRowsBatchBounded: %v", err)
		}
		n := 0
		for row := range ch {
			out = append(out, row)
			cursor = pkOf(row, table)
			n++
		}
		if err := rr.Err(); err != nil {
			t.Fatalf("reader Err: %v", err)
		}
		if n < limit {
			break
		}
	}
	return out
}

// pkKey renders a PK tuple as a stable string key for set membership.
func pkKey(pk []any) string {
	parts := make([]string, len(pk))
	for i, v := range pk {
		if b, ok := v.([]byte); ok {
			parts[i] = fmt.Sprintf("blob:%x", b)
			continue
		}
		parts[i] = fmt.Sprintf("%T:%v", v, v)
	}
	return fmt.Sprint(parts)
}

// TestReadRowsBatch_NilCombinations pins the after/upTo nil matrix on a
// single integer PK: (nil,nil) == full table in PK order; a lower bound
// excludes <= after; an inclusive upper bound clips at upTo; both together
// read the open-low/closed-high window.
func TestReadRowsBatch_NilCombinations(t *testing.T) {
	path := seedDB(
		t,
		`CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)`,
		`INSERT INTO t (id, v) VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')`,
	)
	tbl := schemaTable(t, path, "t")
	rr := openReader(t, path)
	defer func() { _ = rr.Close() }()
	ctx := context.Background()

	collect := func(ch <-chan ir.Row) []int64 {
		var ids []int64
		for row := range ch {
			ids = append(ids, row["id"].(int64))
		}
		if err := rr.Err(); err != nil {
			t.Fatalf("Err: %v", err)
		}
		return ids
	}

	// ReadRowsBatch(nil) == full table in order.
	ch, err := rr.ReadRowsBatch(ctx, tbl, nil, 100)
	if err != nil {
		t.Fatalf("ReadRowsBatch: %v", err)
	}
	if got := collect(ch); !equalInt64(got, []int64{1, 2, 3, 4, 5}) {
		t.Errorf("ReadRowsBatch(nil) = %v; want 1..5", got)
	}

	// ReadRowsBatchBounded(nil,nil) identical to ReadRowsBatch.
	ch, _ = rr.ReadRowsBatchBounded(ctx, tbl, nil, nil, 100)
	if got := collect(ch); !equalInt64(got, []int64{1, 2, 3, 4, 5}) {
		t.Errorf("Bounded(nil,nil) = %v; want 1..5", got)
	}

	// Lower bound only: id > 2.
	ch, _ = rr.ReadRowsBatchBounded(ctx, tbl, []any{int64(2)}, nil, 100)
	if got := collect(ch); !equalInt64(got, []int64{3, 4, 5}) {
		t.Errorf("Bounded(after=2,nil) = %v; want 3,4,5", got)
	}

	// Upper bound only, INCLUSIVE: id <= 3.
	ch, _ = rr.ReadRowsBatchBounded(ctx, tbl, nil, []any{int64(3)}, 100)
	if got := collect(ch); !equalInt64(got, []int64{1, 2, 3}) {
		t.Errorf("Bounded(nil,upTo=3) = %v; want 1,2,3 (inclusive)", got)
	}

	// Both: 2 < id <= 4.
	ch, _ = rr.ReadRowsBatchBounded(ctx, tbl, []any{int64(2)}, []any{int64(4)}, 100)
	if got := collect(ch); !equalInt64(got, []int64{3, 4}) {
		t.Errorf("Bounded(after=2,upTo=4) = %v; want 3,4", got)
	}
}

// TestReadRowsBatch_NoPK pins the no-PK refusal: the orchestrator falls
// back to single-reader on this error rather than emitting malformed SQL.
func TestReadRowsBatch_NoPK(t *testing.T) {
	path := seedDB(
		t,
		`CREATE TABLE nopk (a INTEGER, b TEXT)`,
		`INSERT INTO nopk (a,b) VALUES (1,'x')`,
	)
	tbl := schemaTable(t, path, "nopk")
	rr := openReader(t, path)
	defer func() { _ = rr.Close() }()
	if _, err := rr.ReadRowsBatch(context.Background(), tbl, nil, 10); err == nil {
		t.Fatal("ReadRowsBatch on a no-PK table: err = nil; want a loud refusal")
	}
	if _, err := rr.ReadRowsBatchBounded(context.Background(), tbl, nil, nil, 10); err == nil {
		t.Fatal("ReadRowsBatchBounded on a no-PK table: err = nil; want a loud refusal")
	}
}

// TestRangeBounds pins MIN/MAX on an integer PK and the empty-table
// (nil,nil) signal.
func TestRangeBounds(t *testing.T) {
	path := seedDB(
		t,
		`CREATE TABLE t (id INTEGER PRIMARY KEY)`,
		`INSERT INTO t (id) VALUES (3),(7),(42)`,
		`CREATE TABLE empty (id INTEGER PRIMARY KEY)`,
	)
	rr := openReader(t, path)
	defer func() { _ = rr.Close() }()
	ctx := context.Background()

	tbl := schemaTable(t, path, "t")
	lo, hi, err := rr.RangeBounds(ctx, tbl, "id")
	if err != nil {
		t.Fatalf("RangeBounds: %v", err)
	}
	if lo != int64(3) || hi != int64(42) {
		t.Errorf("RangeBounds = (%v,%v); want (3,42)", lo, hi)
	}

	et := schemaTable(t, path, "empty")
	lo, hi, err = rr.RangeBounds(ctx, et, "id")
	if err != nil {
		t.Fatalf("RangeBounds(empty): %v", err)
	}
	if lo != nil || hi != nil {
		t.Errorf("RangeBounds(empty) = (%v,%v); want (nil,nil)", lo, hi)
	}
}

// TestCountRows pins the exact count for CountRows and EstimateRowCount.
func TestCountRows(t *testing.T) {
	path := seedDB(
		t,
		`CREATE TABLE t (id INTEGER PRIMARY KEY)`,
		`INSERT INTO t (id) VALUES (1),(2),(3),(4)`,
	)
	rr := openReader(t, path)
	defer func() { _ = rr.Close() }()
	ctx := context.Background()
	tbl := schemaTable(t, path, "t")

	if n, err := rr.CountRows(ctx, tbl); err != nil || n != 4 {
		t.Errorf("CountRows = (%d,%v); want (4,nil)", n, err)
	}
	if n, err := rr.EstimateRowCount(ctx, tbl); err != nil || n != 4 {
		t.Errorf("EstimateRowCount = (%d,%v); want (4,nil)", n, err)
	}
}

// keysetCase describes one PK family for the exactly-once partition pin.
type keysetCase struct {
	name    string
	ddl     string
	insert  string
	pkNames []string
}

// TestKeyset_ExactlyOncePartition is the CORE correctness pin (the Bug-74
// silent-row-loss class): for every PK family — integer, BINARY TEXT,
// NOCASE TEXT, BLOB, and composite — the n-1 sampled boundaries split the
// table into half-open (boundary[k-1], boundary[k]] chunks whose union,
// read back via ReadRowsBatchBounded, reconstructs EVERY row EXACTLY ONCE
// (no row in zero chunks = loss; none in two = dup). The NOCASE case is
// the one that would silently drop a boundary-straddling row if the clip
// and ORDER BY disagreed on collation; it must pass.
func TestKeyset_ExactlyOncePartition(t *testing.T) {
	cases := []keysetCase{
		{
			name:    "int_pk",
			ddl:     `CREATE TABLE t (id INTEGER PRIMARY KEY)`,
			insert:  intInserts(50),
			pkNames: []string{"id"},
		},
		{
			name:    "text_pk_binary",
			ddl:     `CREATE TABLE t (k TEXT PRIMARY KEY)`,
			insert:  textInserts(50, false),
			pkNames: []string{"k"},
		},
		{
			name:    "text_pk_nocase",
			ddl:     `CREATE TABLE t (k TEXT COLLATE NOCASE PRIMARY KEY)`,
			insert:  textInserts(50, true),
			pkNames: []string{"k"},
		},
		{
			name:    "blob_pk",
			ddl:     `CREATE TABLE t (b BLOB PRIMARY KEY)`,
			insert:  blobInserts(50),
			pkNames: []string{"b"},
		},
		{
			name:    "composite_pk",
			ddl:     `CREATE TABLE t (g INTEGER NOT NULL, k TEXT NOT NULL, PRIMARY KEY (g, k))`,
			insert:  compositeInserts(),
			pkNames: []string{"g", "k"},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			path := seedDB(t, c.ddl, c.insert)
			tbl := schemaTable(t, path, "t")
			if len(tbl.PrimaryKey.Columns) != len(c.pkNames) {
				t.Fatalf("PK cols = %d; want %d", len(tbl.PrimaryKey.Columns), len(c.pkNames))
			}
			rr := openReader(t, path)
			defer func() { _ = rr.Close() }()
			ctx := context.Background()

			total, err := rr.CountRows(ctx, tbl)
			if err != nil {
				t.Fatalf("CountRows: %v", err)
			}

			// Ground truth: every PK read unbounded.
			wantRows := readBoundedAll(t, rr, tbl, nil, nil, 7)
			if int64(len(wantRows)) != total {
				t.Fatalf("unbounded read = %d rows; CountRows = %d", len(wantRows), total)
			}
			want := map[string]int{}
			for _, row := range wantRows {
				want[pkKey(pkOf(row, tbl))]++
			}

			const n = 4
			bounds, err := rr.SampleKeysetBoundaries(ctx, tbl, c.pkNames, n)
			if err != nil {
				t.Fatalf("SampleKeysetBoundaries: %v", err)
			}
			if len(bounds) == 0 || len(bounds) > n-1 {
				t.Fatalf("boundaries = %d; want 1..%d", len(bounds), n-1)
			}
			// Boundaries must be strictly ascending in PK order (they are the
			// interior split points). Confirmed by the partition check below;
			// also assert each boundary tuple has the right width.
			for i, b := range bounds {
				if len(b) != len(c.pkNames) {
					t.Fatalf("boundary[%d] width = %d; want %d", i, len(b), len(c.pkNames))
				}
			}

			// Assemble half-open chunks: (nil,b0], (b0,b1], ..., (blast,nil].
			got := map[string]int{}
			lower := []any(nil)
			ranges := make([][2][]any, 0, len(bounds)+1)
			for _, b := range bounds {
				ranges = append(ranges, [2][]any{lower, b})
				lower = b
			}
			ranges = append(ranges, [2][]any{lower, nil})

			for _, rg := range ranges {
				rows := readBoundedAll(t, rr, tbl, rg[0], rg[1], 7)
				for _, row := range rows {
					got[pkKey(pkOf(row, tbl))]++
				}
			}

			// Exactly-once: every wanted PK appears exactly once; nothing extra.
			if len(got) != len(want) {
				t.Errorf("distinct PKs read = %d; want %d (loss or dup)", len(got), len(want))
			}
			var dups, missing []string
			for k := range want {
				switch got[k] {
				case 1: // exactly once — good
				case 0:
					missing = append(missing, k)
				default:
					dups = append(dups, fmt.Sprintf("%s x%d", k, got[k]))
				}
			}
			sort.Strings(dups)
			sort.Strings(missing)
			if len(missing) > 0 {
				t.Errorf("%d PK(s) landed in NO chunk (silent loss): %v", len(missing), missing)
			}
			if len(dups) > 0 {
				t.Errorf("%d PK(s) landed in MULTIPLE chunks (dup): %v", len(dups), dups)
			}
		})
	}
}

// equalInt64 reports slice equality.
func equalInt64(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// intInserts builds an INSERT of ids 1..count (already PK-ordered).
func intInserts(count int) string {
	var b bytes.Buffer
	b.WriteString("INSERT INTO t (id) VALUES ")
	for i := 1; i <= count; i++ {
		if i > 1 {
			b.WriteString(",")
		}
		fmt.Fprintf(&b, "(%d)", i)
	}
	return b.String()
}

// textInserts builds count distinct TEXT keys. With mixedCase=true the keys
// alternate upper/lower first-letter case so the BINARY order (uppercase
// before lowercase in ASCII) DIFFERS from the NOCASE order — the case that
// catches a clip/ORDER-BY collation divergence. Keys stay distinct under
// NOCASE (required for a NOCASE PK) by suffixing a unique index.
func textInserts(count int, mixedCase bool) string {
	var b bytes.Buffer
	b.WriteString("INSERT INTO t (k) VALUES ")
	for i := 0; i < count; i++ {
		if i > 0 {
			b.WriteString(",")
		}
		prefix := "key"
		if mixedCase && i%2 == 0 {
			prefix = "KEY"
		}
		fmt.Fprintf(&b, "('%s_%03d')", prefix, i)
	}
	return b.String()
}

// blobInserts builds count distinct 2-byte BLOB keys.
func blobInserts(count int) string {
	var b bytes.Buffer
	b.WriteString("INSERT INTO t (b) VALUES ")
	for i := 0; i < count; i++ {
		if i > 0 {
			b.WriteString(",")
		}
		fmt.Fprintf(&b, "(x'%04x')", i)
	}
	return b.String()
}

// compositeInserts builds a (g, k) composite-PK corpus that needs a true
// row-value comparison to descend: several k values per g.
func compositeInserts() string {
	var b bytes.Buffer
	b.WriteString("INSERT INTO t (g, k) VALUES ")
	first := true
	for g := 1; g <= 5; g++ {
		for j := 0; j < 8; j++ {
			if !first {
				b.WriteString(",")
			}
			first = false
			fmt.Fprintf(&b, "(%d,'tag_%03d')", g, j)
		}
	}
	return b.String()
}

// TestKeyset_FewerThanNBoundaries pins that a tiny / heavily-duplicate
// table returns fewer than n-1 boundaries WITHOUT error (the orchestrator
// collapses to fewer chunks).
func TestKeyset_FewerThanNBoundaries(t *testing.T) {
	path := seedDB(
		t,
		`CREATE TABLE t (id INTEGER PRIMARY KEY)`,
		`INSERT INTO t (id) VALUES (1),(2)`,
	)
	tbl := schemaTable(t, path, "t")
	rr := openReader(t, path)
	defer func() { _ = rr.Close() }()
	bounds, err := rr.SampleKeysetBoundaries(context.Background(), tbl, []string{"id"}, 8)
	if err != nil {
		t.Fatalf("SampleKeysetBoundaries: %v", err)
	}
	if len(bounds) >= 8 {
		t.Errorf("boundaries = %d; want fewer than n-1 (table has 2 rows)", len(bounds))
	}
}

// TestKeyset_EmptyTable pins that an empty table yields zero boundaries and
// no error.
func TestKeyset_EmptyTable(t *testing.T) {
	path := seedDB(t, `CREATE TABLE t (id INTEGER PRIMARY KEY)`)
	tbl := schemaTable(t, path, "t")
	rr := openReader(t, path)
	defer func() { _ = rr.Close() }()
	bounds, err := rr.SampleKeysetBoundaries(context.Background(), tbl, []string{"id"}, 4)
	if err != nil {
		t.Fatalf("SampleKeysetBoundaries(empty): %v", err)
	}
	if len(bounds) != 0 {
		t.Errorf("boundaries = %d; want 0 for empty table", len(bounds))
	}
}

// TestDumpSource_RoutesToSingleReader pins the `.sql`-dump routing decision:
// a materialized dump reader (r.tempPath != "") must NOT be chunked, because
// the orchestrator would re-materialize an independent temp DB per chunk.
// EstimateRowCount returns 0 ("no estimate → single-reader"), and RangeBounds
// / SampleKeysetBoundaries report "no chunking" defensively — so even a caller
// that bypassed the estimate cannot chunk a dump. The batched read surfaces
// still WORK on the dump (the single-reader path uses ReadRows, not these, but
// they must not error).
func TestDumpSource_RoutesToSingleReader(t *testing.T) {
	path := writeDump(t, "d1export.sql", d1LikeDump)
	tbl := schemaTable(t, path, "users")
	rr := openReader(t, path)
	defer func() { _ = rr.Close() }()
	if rr.tempPath == "" {
		t.Fatal("dump reader should own a materialized tempPath")
	}
	ctx := context.Background()

	if n, err := rr.EstimateRowCount(ctx, tbl); err != nil || n != 0 {
		t.Errorf("EstimateRowCount(dump) = (%d,%v); want (0,nil) to route single-reader", n, err)
	}
	if n, err := rr.CountRows(ctx, tbl); err != nil || n != 0 {
		t.Errorf("CountRows(dump) = (%d,%v); want (0,nil)", n, err)
	}
	lo, hi, err := rr.RangeBounds(ctx, tbl, "id")
	if err != nil || lo != nil || hi != nil {
		t.Errorf("RangeBounds(dump) = (%v,%v,%v); want (nil,nil,nil)", lo, hi, err)
	}
	bounds, err := rr.SampleKeysetBoundaries(ctx, tbl, []string{"id"}, 4)
	if err != nil || len(bounds) != 0 {
		t.Errorf("SampleKeysetBoundaries(dump) = (%v,%v); want (empty,nil)", bounds, err)
	}
}

// blobOrderSanity guards the BLOB PK assumption that SQLite orders BLOBs by
// memcmp (BINARY) — the order the keyset partition relies on. (Defensive: a
// driver change would surface here, not as a silent loss.)
func TestBlobMemcmpOrder(t *testing.T) {
	a, b := []byte{0x00, 0x10}, []byte{0x00, 0x09}
	if bytes.Compare(a, b) <= 0 {
		t.Fatal("sanity: 0x0010 should sort after 0x0009 under memcmp")
	}
}
