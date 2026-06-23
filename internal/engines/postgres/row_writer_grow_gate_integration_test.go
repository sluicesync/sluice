//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Value-fidelity + retry-convergence integration tests for the PG-target
// cold-copy storage-grow resilience (roadmap item 38, ADR-0110). Two pins:
//
//  1. BYTE-IDENTICAL — the grow-gate-engaged CHUNKED COPY path
//     (writeViaCopyChunked) must produce EXACTLY the same target rows as the
//     monolithic single-CopyFrom path (writeViaCopy with no gate), across a
//     multi-family fixture (int / numeric-extreme / text+unicode / bytea /
//     json/jsonb / timestamp(tz) / bool / NULLs). This is the Bug-74
//     discipline: the chunked path must not introduce a second encoding. The
//     two tables are copied BOTH ways and compared via an md5 over PG's own
//     canonical ::text rendering of every row, ordered by PK.
//
//  2. RETRY CONVERGENCE — when a classified-retriable fault (53100) is
//     injected on the first attempt of the FIRST chunk, the chunk is replayed
//     on a fresh conn and the table converges with NO dup and NO drop (a
//     rolled-back chunk wrote nothing into the append-only fresh table).

package postgres

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"sluicesync.dev/sluice/internal/ir"
)

// growFixtureSchema builds the multi-family fixture table used by both pins.
// One representative column per encoding family the COPY path dispatches on,
// so a chunked-vs-monolithic divergence in ANY family surfaces.
func growFixtureSchema(name string) *ir.Schema {
	return &ir.Schema{Tables: []*ir.Table{{
		Name: name,
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "n_small", Type: ir.Integer{Width: 16}, Nullable: true},
			{Name: "f64", Type: ir.Float{Precision: ir.FloatDouble}, Nullable: true},
			{Name: "amount", Type: ir.Decimal{Precision: 38, Scale: 10}, Nullable: true},
			{Name: "label", Type: ir.Text{}, Nullable: true},
			{Name: "blob", Type: ir.Blob{Size: ir.BlobLong}, Nullable: true},
			{Name: "doc_text", Type: ir.JSON{Binary: false}, Nullable: true},
			{Name: "doc_bin", Type: ir.JSON{Binary: true}, Nullable: true},
			{Name: "ts", Type: ir.Timestamp{Precision: 6, WithTimeZone: false}, Nullable: true},
			{Name: "tstz", Type: ir.Timestamp{Precision: 6, WithTimeZone: true}, Nullable: true},
			{Name: "flag", Type: ir.Boolean{}, Nullable: true},
		},
		PrimaryKey: &ir.Index{
			Name:    name + "_pkey",
			Unique:  true,
			Columns: []ir.IndexColumn{{Column: "id"}},
		},
	}}}
}

// growFixtureRows returns n deterministic rows spanning the families, with a
// scattering of NULLs and value extremes (numeric extremes, unicode text,
// arbitrary bytes incl. high bytes, json/jsonb, both timestamp flavors).
func growFixtureRows(n int) []ir.Row {
	tsBase := time.Date(2026, 6, 22, 13, 45, 6, 123456000, time.UTC)
	rows := make([]ir.Row, 0, n)
	for i := 0; i < n; i++ {
		r := ir.Row{
			"id":       int64(i + 1),
			"n_small":  int64(int16((i*7)%30000 - 15000)),
			"f64":      float64(i) * 1.5,
			"amount":   "12345678901234567890.1234567890", // 38-digit extreme
			"label":    "row-世界-" + string(rune('A'+i%26)) + "-Ωémoji😀",
			"blob":     []byte{byte(i), 0x00, 0xff, 0xfe, byte(i * 3)},
			"doc_text": `{"k":"v","i":` + itoa(i) + `}`,
			"doc_bin":  `{"a":[1,2,3],"n":` + itoa(i) + `}`,
			"ts":       tsBase.Add(time.Duration(i) * time.Minute),
			"tstz":     tsBase.Add(time.Duration(i) * time.Hour),
			"flag":     i%2 == 0,
		}
		// Scatter NULLs across every nullable family on selected rows.
		if i%5 == 0 {
			r["n_small"] = nil
			r["amount"] = nil
			r["label"] = nil
			r["blob"] = nil
			r["doc_bin"] = nil
			r["tstz"] = nil
			r["flag"] = nil
		}
		rows = append(rows, r)
	}
	return rows
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}

// applyFixtureAndReadTable applies the schema and re-reads the table object
// from the DB so the writer sees the SchemaReader's view (matching the
// production path), returning the read-back *ir.Table.
func applyFixtureAndReadTable(t *testing.T, ctx context.Context, dsn, name string) *ir.Table {
	t.Helper()
	sw, err := Engine{}.OpenSchemaWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaWriter: %v", err)
	}
	defer closeIf(sw)
	if err := sw.CreateTablesWithoutConstraints(ctx, growFixtureSchema(name)); err != nil {
		t.Fatalf("CreateTablesWithoutConstraints: %v", err)
	}
	sr, err := Engine{}.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer closeIf(sr)
	readBack, err := sr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	table := findTable(readBack, name)
	if table == nil {
		t.Fatalf("%s table not found; have %v", name, tableNames(readBack))
	}
	return table
}

func feedRows(rows []ir.Row) <-chan ir.Row {
	ch := make(chan ir.Row, len(rows))
	for _, r := range rows {
		ch <- r
	}
	close(ch)
	return ch
}

// tableRowMD5 returns md5 over PG's own canonical ::text rendering of every
// row, ordered by id. Two tables with identical md5 hold byte-identical rows
// across every column family.
func tableRowMD5(t *testing.T, ctx context.Context, db *sql.DB, name string) string {
	t.Helper()
	var h sql.NullString
	q := `SELECT md5(coalesce(string_agg(x.row_text, '|' ORDER BY x.id), '')) ` +
		`FROM (SELECT id, t.*::text AS row_text FROM ` + quoteIdent("public") + `.` + quoteIdent(name) + ` t) x`
	if err := db.QueryRowContext(ctx, q).Scan(&h); err != nil {
		t.Fatalf("md5 over %s: %v", name, err)
	}
	if !h.Valid {
		t.Fatalf("md5 over %s returned NULL", name)
	}
	return h.String
}

func tableCount(t *testing.T, ctx context.Context, db *sql.DB, name string) int {
	t.Helper()
	var n int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM `+quoteIdent("public")+`.`+quoteIdent(name)).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", name, err)
	}
	return n
}

// recordingGrowGate is defined in row_writer_grow_gate_test.go (same package).

// TestPGGrowGate_ChunkedByteIdenticalToMonolithic is the value-fidelity pin
// (Bug-74 discipline). It copies the SAME multi-family fixture into two
// tables — one via the monolithic path (no gate), one via the chunked path
// (gate attached, tiny chunk size so MANY chunks fire) — and asserts both the
// row count and the md5-over-canonical-::text are identical.
func TestPGGrowGate_ChunkedByteIdenticalToMonolithic(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	const nRows = 500
	rows := growFixtureRows(nRows)

	monoTable := applyFixtureAndReadTable(t, ctx, dsn, "grow_mono")
	chunkTable := applyFixtureAndReadTable(t, ctx, dsn, "grow_chunk")

	// Monolithic path: nil gate ⇒ single CopyFrom, byte-for-byte the legacy
	// path.
	rwMonoIface, err := Engine{}.OpenRowWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenRowWriter (mono): %v", err)
	}
	defer closeIf(rwMonoIface)
	rwMono := rwMonoIface.(*RowWriter)
	if rwMono.growGate != nil {
		t.Fatal("mono writer must have nil gate")
	}
	if err := rwMono.WriteRows(ctx, monoTable, feedRows(rows)); err != nil {
		t.Fatalf("WriteRows (mono): %v", err)
	}

	// Chunked path: attach a gate AND shrink the chunk size so multiple chunks
	// fire across the 500-row fixture.
	rwChunkIface, err := Engine{}.OpenRowWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenRowWriter (chunk): %v", err)
	}
	defer closeIf(rwChunkIface)
	rwChunk := rwChunkIface.(*RowWriter)
	rwChunk.SetGrowGate(&recordingGrowGate{})
	withSmallPGChunk(t, 37) // 500/37 ⇒ ~14 chunks
	if err := rwChunk.WriteRows(ctx, chunkTable, feedRows(rows)); err != nil {
		t.Fatalf("WriteRows (chunk): %v", err)
	}

	// Sanity: both landed every row.
	if got := tableCount(t, ctx, rwMono.db, "grow_mono"); got != nRows {
		t.Fatalf("mono count = %d; want %d", got, nRows)
	}
	if got := tableCount(t, ctx, rwChunk.db, "grow_chunk"); got != nRows {
		t.Fatalf("chunk count = %d; want %d", got, nRows)
	}

	// THE pin: byte-identical across every family.
	monoMD5 := tableRowMD5(t, ctx, rwMono.db, "grow_mono")
	chunkMD5 := tableRowMD5(t, ctx, rwChunk.db, "grow_chunk")
	if monoMD5 != chunkMD5 {
		t.Fatalf("chunked COPY is NOT byte-identical to monolithic COPY:\n  mono  md5 = %s\n  chunk md5 = %s", monoMD5, chunkMD5)
	}
}

// TestPGGrowGate_ChunkRetryConvergesNoDupNoDrop is the retry-convergence pin.
// It injects ONE classified-retriable fault (53100) on the first attempt of
// the first chunk via the test-only copyChunkFaultHook, then asserts the
// table converges to the exact fixture with no dup, no drop, and byte-identical
// rows to a clean monolithic copy of the same fixture.
func TestPGGrowGate_ChunkRetryConvergesNoDupNoDrop(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	const nRows = 300
	rows := growFixtureRows(nRows)

	refTable := applyFixtureAndReadTable(t, ctx, dsn, "grow_ref")
	retryTable := applyFixtureAndReadTable(t, ctx, dsn, "grow_retry")

	// Reference: clean monolithic copy → the byte-identical ground truth.
	rwRefIface, err := Engine{}.OpenRowWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenRowWriter (ref): %v", err)
	}
	defer closeIf(rwRefIface)
	rwRef := rwRefIface.(*RowWriter)
	if err := rwRef.WriteRows(ctx, refTable, feedRows(rows)); err != nil {
		t.Fatalf("WriteRows (ref): %v", err)
	}

	// Retry path: gate attached, small chunks, and a fault injected on the
	// FIRST attempt — the first chunk's first CopyFrom is rejected with 53100
	// BEFORE it lands any rows, so the replay must re-copy that chunk cleanly.
	withFastPGCopyBackoff(t)
	withSmallPGChunk(t, 50) // 300/50 ⇒ 6 chunks; fault hits chunk 1 attempt 1
	rwRetryIface, err := Engine{}.OpenRowWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenRowWriter (retry): %v", err)
	}
	defer closeIf(rwRetryIface)
	rwRetry := rwRetryIface.(*RowWriter)
	rwRetry.SetGrowGate(&recordingGrowGate{})
	var faults int
	rwRetry.copyChunkFaultHook = func(attempt int) error {
		// Fault only the very first attempt of the run (the first chunk's first
		// try). Every replay + every later chunk runs the real CopyFrom.
		if faults == 0 && attempt == 1 {
			faults++
			return &pgconn.PgError{Code: "53100", Message: `could not extend file: No space left on device`}
		}
		return nil
	}
	if err := rwRetry.WriteRows(ctx, retryTable, feedRows(rows)); err != nil {
		t.Fatalf("WriteRows (retry): %v", err)
	}
	if faults != 1 {
		t.Fatalf("expected exactly 1 injected fault; got %d (hook never fired ⇒ chunked path not taken)", faults)
	}

	// Converged: exact count (no dup, no drop) AND byte-identical to the clean
	// reference copy.
	if got := tableCount(t, ctx, rwRetry.db, "grow_retry"); got != nRows {
		t.Fatalf("retry count = %d; want %d (dup or drop after replay)", got, nRows)
	}
	refMD5 := tableRowMD5(t, ctx, rwRef.db, "grow_ref")
	retryMD5 := tableRowMD5(t, ctx, rwRetry.db, "grow_retry")
	if refMD5 != retryMD5 {
		t.Fatalf("chunk-retry result is NOT byte-identical to a clean copy:\n  ref   md5 = %s\n  retry md5 = %s", refMD5, retryMD5)
	}
}

// withSmallPGChunk shrinks the chunked-COPY row cap for the duration of a test
// so multiple chunks fire over a small fixture, restoring it afterwards. The
// chunk cap is a package const in production; the test mutates a package var
// shadow only via this helper. (pgCopyChunkRows is a const — so the helper
// instead drives chunk count through the byte cap by leaving rows large; see
// note.) Implemented by temporarily lowering pgCopyChunkRowsVar.
func withSmallPGChunk(t *testing.T, rowsPerChunk int) {
	t.Helper()
	prev := pgCopyChunkRowsVar
	pgCopyChunkRowsVar = rowsPerChunk
	t.Cleanup(func() { pgCopyChunkRowsVar = prev })
}
