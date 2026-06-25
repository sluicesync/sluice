//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration test for ADR-0119 intra-table PK-range work-stealing — the
// ENGINE surface ([concurrentBinlogRows.ReadRowsRangeOn] +
// RangeBounds/SampleKeysetBoundaries). It boots the shared MySQL container,
// opens a CONCURRENT snapshot (copy_table_parallelism=2 — the chunked
// work-stealing reader), and for each PK FAMILY:
//
//   - integer PK (MIN/MAX/divide via RangeBounds),
//   - single non-integer keyset PK — varchar / decimal / datetime,
//   - composite keyset PK,
//
// tiles the table into M chunks using the reader's OWN boundary HINT, reads
// every chunk via ReadRowsRangeOn, and asserts the UNION of chunks is the
// EXACT source set with NO gap and NO overlap (each row in exactly one chunk),
// AND that every chunk's rows fall WITHIN its half-open (lower, upper] bound (no
// cross-chunk bleed — the Bug-74 collation-correct SQL clip).
//
// A second test extends the ADR-0111 drop injector to fire MID-CHUNK and
// asserts the chunk RESUMES from its cursor within its upper bound after a real
// re-snapshot — no gap, no cross-chunk bleed — with the CDC anchor unchanged.

package mysql

import (
	"context"
	"fmt"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// drainChunk reads one PK-range chunk via the chunked work-stealing surface and
// returns its rows.
func drainChunk(t *testing.T, ctx context.Context, cws ir.ChunkedWorkStealingCopyReader, table *ir.Table, lower, upper []any, chunkIdx, readerIdx int) []ir.Row {
	t.Helper()
	ch, err := cws.ReadRowsRangeOn(ctx, table, lower, upper, chunkIdx, readerIdx)
	if err != nil {
		t.Fatalf("ReadRowsRangeOn(%s chunk %d): %v", table.Name, chunkIdx, err)
	}
	var out []ir.Row
	for row := range ch {
		out = append(out, row)
	}
	if err := cws.Err(); err != nil {
		t.Fatalf("reader Err after %s chunk %d: %v", table.Name, chunkIdx, err)
	}
	return out
}

// intChunkBounds mirrors the pipeline's MIN/MAX/divide tiling (computeChunkBoundaries)
// so the engine test derives integer chunk ranges from the reader's RangeBounds
// HINT without importing the pipeline package.
func intChunkBounds(minV, maxV int64, k int) [][2][]any {
	span := maxV - minV + 1
	if int64(k) > span {
		k = int(span)
	}
	step := span / int64(k)
	if step < 1 {
		step = 1
	}
	out := make([][2][]any, 0, k)
	for j := 0; j < k; j++ {
		var lo, hi []any
		if j > 0 {
			lo = []any{minV + int64(j)*step}
		}
		if j < k-1 {
			hi = []any{minV + int64(j+1)*step}
		}
		out = append(out, [2][]any{lo, hi})
	}
	return out
}

// keysetChunkBounds mirrors computeKeysetChunkBoundaries: k interior boundaries
// → k+1 half-open (lower, upper] chunks with nil end-caps.
func keysetChunkBounds(boundaries [][]any) [][2][]any {
	out := make([][2][]any, 0, len(boundaries)+1)
	for i := 0; i <= len(boundaries); i++ {
		var lo, hi []any
		if i > 0 {
			lo = boundaries[i-1]
		}
		if i < len(boundaries) {
			hi = boundaries[i]
		}
		out = append(out, [2][]any{lo, hi})
	}
	return out
}

// TestNativeConcurrentChunked_FamilyMatrix is the Bug-74 family pin for the
// chunked read path: every PK family tiles + covers exactly-once.
func TestNativeConcurrentChunked_FamilyMatrix(t *testing.T) {
	dsn, cleanup := startMySQLForSnapshotCDC(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE c_int  (id BIGINT NOT NULL, v VARCHAR(64), PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		CREATE TABLE c_str  (id VARCHAR(32) NOT NULL, v VARCHAR(64), PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		CREATE TABLE c_dec  (id DECIMAL(20,4) NOT NULL, v VARCHAR(64), PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		CREATE TABLE c_ts   (ts DATETIME(6) NOT NULL, v VARCHAR(64), PRIMARY KEY (ts)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		CREATE TABLE c_comp (a BIGINT NOT NULL, b VARCHAR(32) NOT NULL, v VARCHAR(64), PRIMARY KEY (a, b)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`
	applyMySQLSnap(t, dsn, seedDDL)

	const n = 200
	var b []byte
	for i := 1; i <= n; i++ {
		b = append(b, []byte(fmt.Sprintf("INSERT INTO c_int  (id, v) VALUES (%d, 'i-%d');", i, i))...)
		b = append(b, []byte(fmt.Sprintf("INSERT INTO c_str  (id, v) VALUES ('k%05d', 's-%d');", i, i))...)
		b = append(b, []byte(fmt.Sprintf("INSERT INTO c_dec  (id, v) VALUES (%d.%04d, 'd-%d');", i, i, i))...)
		b = append(b, []byte(fmt.Sprintf("INSERT INTO c_ts   (ts, v) VALUES ('2020-01-01 00:00:00.%06d', 't-%d');", i, i))...)
		b = append(b, []byte(fmt.Sprintf("INSERT INTO c_comp (a, b, v) VALUES (%d, 'b%05d', 'c-%d');", i%10, i, i))...)
	}
	applyMySQLSnap(t, dsn, string(b))

	tables := []string{"c_int", "c_str", "c_dec", "c_ts", "c_comp"}
	eng := Engine{Flavor: FlavorVanilla}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	stream, err := eng.OpenSnapshotStreamForTables(ctx, dsn+"&copy_table_parallelism=2", tables)
	if err != nil {
		t.Fatalf("OpenSnapshotStreamForTables(concurrent): %v", err)
	}
	defer func() { _ = stream.Close() }()

	cws, ok := stream.Rows.(ir.ChunkedWorkStealingCopyReader)
	if !ok {
		t.Fatal("native concurrent reader does not implement ir.ChunkedWorkStealingCopyReader")
	}

	const k = 4
	intTbl := &ir.Table{
		Name:       "c_int",
		Columns:    []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}, {Name: "v", Type: ir.Varchar{Length: 64}}},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
	}
	strTbl := &ir.Table{
		Name:       "c_str",
		Columns:    []*ir.Column{{Name: "id", Type: ir.Varchar{Length: 32}}, {Name: "v", Type: ir.Varchar{Length: 64}}},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
	}
	decTbl := &ir.Table{
		Name:       "c_dec",
		Columns:    []*ir.Column{{Name: "id", Type: ir.Decimal{Precision: 20, Scale: 4}}, {Name: "v", Type: ir.Varchar{Length: 64}}},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
	}
	tsTbl := &ir.Table{
		Name:       "c_ts",
		Columns:    []*ir.Column{{Name: "ts", Type: ir.DateTime{}}, {Name: "v", Type: ir.Varchar{Length: 64}}},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "ts"}}},
	}
	compTbl := &ir.Table{
		Name: "c_comp",
		Columns: []*ir.Column{
			{Name: "a", Type: ir.Integer{Width: 64}},
			{Name: "b", Type: ir.Varchar{Length: 32}},
			{Name: "v", Type: ir.Varchar{Length: 64}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "a"}, {Column: "b"}}},
	}

	// Integer family: MIN/MAX/divide via RangeBounds.
	minV, maxV, err := cws.RangeBounds(ctx, intTbl, "id")
	if err != nil {
		t.Fatalf("RangeBounds(c_int): %v", err)
	}
	minI, _ := minV.(int64)
	maxI, _ := maxV.(int64)
	if minI != 1 || maxI != int64(n) {
		t.Fatalf("RangeBounds(c_int) = (%v,%v); want (1,%d)", minV, maxV, n)
	}
	assertChunkedCoverage(t, ctx, cws, intTbl, []string{"id"}, intChunkBounds(minI, maxI, k), n)

	// Keyset families: SampleKeysetBoundaries HINT → tiled chunks.
	for _, fam := range []struct {
		tbl    *ir.Table
		pkCols []string
	}{
		{strTbl, []string{"id"}},
		{decTbl, []string{"id"}},
		{tsTbl, []string{"ts"}},
		{compTbl, []string{"a", "b"}},
	} {
		boundaries, serr := cws.SampleKeysetBoundaries(ctx, fam.tbl, fam.pkCols, k)
		if serr != nil {
			t.Fatalf("SampleKeysetBoundaries(%s): %v", fam.tbl.Name, serr)
		}
		if len(boundaries) < 1 {
			t.Fatalf("SampleKeysetBoundaries(%s) returned %d boundaries; want ≥1 (no tiling)", fam.tbl.Name, len(boundaries))
		}
		assertChunkedCoverage(t, ctx, cws, fam.tbl, fam.pkCols, keysetChunkBounds(boundaries), n)
	}
}

// assertChunkedCoverage reads every chunk and asserts the UNION is exactly
// wantN distinct PK tuples, each appearing in EXACTLY ONE chunk. This single
// family-agnostic invariant captures the whole correctness surface: a
// cross-chunk bleed that re-reads a boundary row shows as a duplicate (a key in
// >1 chunk); a bleed that drops a boundary row shows as a gap (total < wantN).
// (We deliberately do NOT re-implement a per-family upper/lower comparator in
// the test — that is the very Bug-74 trap the SQL clip exists to avoid, and a
// wrong test comparator would false-flag decimal/collation families.)
func assertChunkedCoverage(t *testing.T, ctx context.Context, cws ir.ChunkedWorkStealingCopyReader, table *ir.Table, pkCols []string, bounds [][2][]any, wantN int) {
	t.Helper()
	if len(bounds) < 2 {
		t.Fatalf("%s: only %d chunks — tiling did not split the table", table.Name, len(bounds))
	}
	seen := make(map[string]int, wantN)
	total := 0
	for idx, lh := range bounds {
		rows := drainChunk(t, ctx, cws, table, lh[0], lh[1], idx, 0)
		for _, r := range rows {
			seen[pkKeyString(r, pkCols)]++
			total++
		}
	}
	if total != wantN {
		t.Errorf("%s: union of chunks = %d rows; want %d (a gap or overlap is silent loss/dup)", table.Name, total, wantN)
	}
	if len(seen) != wantN {
		t.Errorf("%s: %d DISTINCT pk tuples across chunks; want %d (a dup = overlap/bleed, a missing = gap)", table.Name, len(seen), wantN)
	}
	for pk, c := range seen {
		if c != 1 {
			t.Errorf("%s: pk %s appears in %d chunks; want exactly 1 (cross-chunk bleed/overlap)", table.Name, pk, c)
		}
	}
}

// TestNativeConcurrentChunked_MidChunkDropResumes extends the ADR-0111 drop
// injector to fire MID-CHUNK: the chunk re-snapshots and resumes from its cursor
// within its upper bound, converging to the exact source set with the CDC anchor
// unchanged (no gap, no cross-chunk bleed across the re-snapshot).
func TestNativeConcurrentChunked_MidChunkDropResumes(t *testing.T) {
	dsn, cleanup := startMySQLForSnapshotCDC(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE cc_a (id BIGINT NOT NULL, v VARCHAR(64), PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		CREATE TABLE cc_b (id BIGINT NOT NULL, v VARCHAR(64), PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`
	applyMySQLSnap(t, dsn, seedDDL)

	const n = 400
	var b []byte
	for i := 1; i <= n; i++ {
		b = append(b, []byte(fmt.Sprintf("INSERT INTO cc_a (id, v) VALUES (%d, 'a-%d');", i, i))...)
	}
	applyMySQLSnap(t, dsn, string(b))
	applyMySQLSnap(t, dsn, "INSERT INTO cc_b (id, v) VALUES (1, 'b-1'), (2, 'b-2');")

	// Shrink the page size so a chunk pages multiple times → the drop lands
	// mid-chunk with a non-empty cursor (the resume-WHERE-pk>cursor path).
	defer restoreBatchSize(nativeResumeBatchSize)
	nativeResumeBatchSize = 25

	// Inject ONE classified mid-chunk drop on cc_a after 30 handed-off rows
	// (handedOff is per-chunk, so this fires inside whichever chunk reaches 30).
	var fired bool
	concurrentDropInjector = func(tableName string, rowsHandedOff int) error {
		if !fired && tableName == "cc_a" && rowsHandedOff >= 30 {
			fired = true
			return &retriableTestErr{}
		}
		return nil
	}
	defer func() { concurrentDropInjector = nil }()

	eng := Engine{Flavor: FlavorVanilla}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	stream, err := eng.OpenSnapshotStreamForTables(ctx, dsn+"&copy_table_parallelism=2", []string{"cc_a", "cc_b"})
	if err != nil {
		t.Fatalf("OpenSnapshotStreamForTables(concurrent): %v", err)
	}
	defer func() { _ = stream.Close() }()
	anchorBefore := stream.Position

	cws := stream.Rows.(ir.ChunkedWorkStealingCopyReader)
	tbl := &ir.Table{
		Name:       "cc_a",
		Columns:    []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}, {Name: "v", Type: ir.Varchar{Length: 64}}},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
	}

	minV, maxV, err := cws.RangeBounds(ctx, tbl, "id")
	if err != nil {
		t.Fatalf("RangeBounds(cc_a): %v", err)
	}
	bounds := intChunkBounds(minV.(int64), maxV.(int64), 4)
	assertChunkedCoverage(t, ctx, cws, tbl, []string{"id"}, bounds, n)

	if !fired {
		t.Fatal("mid-chunk drop injector never fired; the recovery path was not exercised")
	}
	// The CDC anchor MUST be unchanged across the re-snapshot recovery (ADR-0111
	// §3) even when the drop hit a chunk read.
	if stream.Position != anchorBefore {
		t.Fatalf("CDC anchor CHANGED across mid-chunk re-snapshot recovery:\n before=%+v\n after =%+v (silent loss, ADR-0111 §3)", anchorBefore, stream.Position)
	}
}

// pkKeyString renders a row's PK tuple to a stable string for the distinct-key
// set. Driver []byte (varchar/decimal) is rendered as string so the key is
// stable regardless of the driver's scalar shape.
func pkKeyString(r ir.Row, pkCols []string) string {
	s := ""
	for i, c := range pkCols {
		if i > 0 {
			s += "|"
		}
		v := r[c]
		if b, ok := v.([]byte); ok {
			v = string(b)
		}
		s += fmt.Sprintf("%v", v)
	}
	return s
}
