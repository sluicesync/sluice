//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Value-fidelity + retry-convergence integration tests for the PG-target
// cold-copy storage-grow resilience (roadmap item 38, ADR-0110). Two pins:
//
//  1. BYTE-IDENTICAL — the grow-gate-engaged CHUNKED COPY path
//     (writeViaCopyChunked) must produce EXACTLY the same target rows as the
//     monolithic single-CopyFrom path (writeViaCopy with no gate), across a
//     multi-family fixture spanning every distinct pgx encode branch:
//     int / numeric-extreme / text+unicode / bytea / json/jsonb /
//     timestamp(tz) / bool / NULLs, PLUS the Bug-74-corollary families —
//     arrays (int[], text[], the EXACT numeric[][] multi-dim Bug-74 case, and
//     a NULL-element text[]), the string-shaped non-text OIDs (uuid / inet /
//     cidr / macaddr), the temporal leaves (date / time / timetz — timetz
//     exercises the per-conn registerPGTimetzCodec registration that must fire
//     on BOTH paths via copyFromOnSQLConn), and bit / varbit. This is the
//     Bug-74 discipline: the chunked path must not introduce a second
//     encoding. The two tables are copied BOTH ways and compared via an md5
//     over PG's own canonical ::text rendering of every row, ordered by PK.
//     (pgvector / hstore are NOT in this fixture — the shared test PG image
//     lacks both extensions; they have dedicated end-to-end pins on the
//     prebaked pgvector image. See the fixture-schema NOTE.)
//
//  2. RETRY CONVERGENCE — when a classified-retriable fault (53100) is
//     injected on the first attempt of the FIRST chunk, the chunk is replayed
//     on a fresh conn and the table converges with NO dup and NO drop (a
//     rolled-back chunk wrote nothing into the append-only fresh table).

package postgres

import (
	"context"
	"database/sql"
	"errors"
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

			// --- Bug-74-corollary families: each is a DISTINCT pgx codec /
			// encode branch the chunked path must prove byte-identical to the
			// monolithic path (a green test on one family does NOT cover the
			// others — the driver wire path differs by target OID). ---

			// Arrays (the literal Bug-74 family). int[] (native int8 leaf),
			// text[] (string-leaf Text codec), numeric[][] (the EXACT Bug-74
			// multi-dimensional case — a wrong leaf silently flattens ≥2-D),
			// and a text[] with a NULL element (typed-nil *T leaf round-trip).
			{Name: "arr_int", Type: ir.Array{Element: ir.Integer{Width: 64}}, Nullable: true},
			{Name: "arr_text", Type: ir.Array{Element: ir.Text{}}, Nullable: true},
			{Name: "arr_num2d", Type: ir.Array{Element: ir.Decimal{Precision: 20, Scale: 4}}, Nullable: true},
			{Name: "arr_text_null", Type: ir.Array{Element: ir.Text{}}, Nullable: true},

			// String-shaped non-text OIDs (each a distinct element codec OID).
			{Name: "u", Type: ir.UUID{}, Nullable: true},
			{Name: "ip", Type: ir.Inet{}, Nullable: true},
			{Name: "net", Type: ir.Cidr{}, Nullable: true},
			{Name: "mac", Type: ir.Macaddr{}, Nullable: true},

			// Temporal leaves. timetz exercises the per-conn
			// registerPGTimetzCodec registration that must fire identically on
			// BOTH paths via copyFromOnSQLConn (pgx ships no timetz codec — a
			// dropped registration aborts the COPY with "cannot find encode
			// plan").
			{Name: "d", Type: ir.Date{}, Nullable: true},
			{Name: "tod", Type: ir.Time{Precision: 6, WithTimeZone: false}, Nullable: true},
			{Name: "todtz", Type: ir.Time{Precision: 6, WithTimeZone: true}, Nullable: true},

			// bit / varbit (pgtype.Bits — left-aligned wire format + value-bit
			// length, catalog Bug 62/75).
			{Name: "bits", Type: ir.Bit{Length: 8, Varying: false}, Nullable: true},
			{Name: "vbits", Type: ir.Bit{Length: 16, Varying: true}, Nullable: true},

			// NOTE: pgvector (`vector`) and hstore — the HIGHEST
			// "refactor dropped a codec" risk, since they exercise the
			// per-conn codec-registration extraction in copyFromOnSQLConn — are
			// deliberately NOT in this fixture. The shared test PG image
			// (newSharedPGDB) carries neither extension, so CREATE TABLE would
			// fail. They have dedicated end-to-end pins in
			// change_applier_pipelined_ext_integration_test.go (on the prebaked
			// pgvector image). The timetz column above already exercises the
			// same per-conn registration seam (copyFromOnSQLConn) on the
			// standard rig, so the "registration must fire on both paths"
			// contract is covered here.
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
			"doc_text": `{"k":"v","i":` + growItoa(i) + `}`,
			"doc_bin":  `{"a":[1,2,3],"n":` + growItoa(i) + `}`,
			"ts":       tsBase.Add(time.Duration(i) * time.Minute),
			"tstz":     tsBase.Add(time.Duration(i) * time.Hour),
			"flag":     i%2 == 0,

			// Arrays — []any (nested for multi-dim), the IR canonical form.
			"arr_int":  []any{int64(i), int64(i + 1), int64(i + 2)},
			"arr_text": []any{"a-" + growItoa(i), "b-世界", "c-Ω"},
			// numeric[][] — the EXACT Bug-74 multi-dimensional case (2×2).
			"arr_num2d": []any{
				[]any{growItoa(i) + ".0001", growItoa(i) + ".0002"},
				[]any{growItoa(i) + ".0003", growItoa(i) + ".0004"},
			},
			// text[] with a NULL element (middle slot).
			"arr_text_null": []any{"x-" + growItoa(i), nil, "z-" + growItoa(i)},

			// String-shaped non-text OIDs (canonical text form, as the readers
			// emit under pgx stdlib mode).
			"u":   fmtUUID(i),
			"ip":  fmtInet(i),
			"net": fmtCidr(i),
			"mac": fmtMac(i),

			// Temporal: date (time.Time), time-of-day + timetz (canonical text).
			"d":     time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, i),
			"tod":   fmtTimeOfDay(i),
			"todtz": fmtTimeTZ(i),

			// bit(8) fixed + varbit(16) — canonical '0'/'1' bit-strings.
			"bits":  fmtBits8(i),
			"vbits": fmtVarbits(i),
		}
		// Scatter NULLs across every nullable family on selected rows — incl.
		// every new family, so the NULL-element / NULL-array shape variants are
		// exercised on both paths.
		if i%5 == 0 {
			r["n_small"] = nil
			r["amount"] = nil
			r["label"] = nil
			r["blob"] = nil
			r["doc_bin"] = nil
			r["tstz"] = nil
			r["flag"] = nil
			r["arr_int"] = nil
			r["arr_text"] = nil
			r["arr_num2d"] = nil
			r["arr_text_null"] = nil
			r["u"] = nil
			r["ip"] = nil
			r["net"] = nil
			r["mac"] = nil
			r["d"] = nil
			r["tod"] = nil
			r["todtz"] = nil
			r["bits"] = nil
			r["vbits"] = nil
		}
		rows = append(rows, r)
	}
	return rows
}

func growItoa(i int) string {
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

// Per-family deterministic value formatters for the Bug-74-corollary
// columns. Each returns the value in the canonical text/Go shape the COPY
// encode path expects (uuid/inet/cidr/macaddr/timetz/time as their canonical
// PG text strings; bit/varbit as '0'/'1' bit-strings). Both the monolithic and
// the chunked path encode the SAME bytes, so byte-identity is the assertion;
// the formatters only need to produce values PG accepts.

func fmtUUID(i int) string {
	// A valid v4-shaped uuid that varies per row in the last group.
	return "0000ffff-0000-4000-8000-" + fmtHex12(i)
}

func fmtHex12(i int) string {
	const hexdigits = "0123456789abcdef"
	var b [12]byte
	for p := 11; p >= 0; p-- {
		b[p] = hexdigits[i&0xf]
		i >>= 4
	}
	return string(b[:])
}

func fmtInet(i int) string {
	// IPv4 host address, varying in the low octet.
	return "192.168.1." + growItoa(i%256)
}

func fmtCidr(i int) string {
	// IPv4 network spec (host bits zero — PG canonicalizes cidr).
	return "10." + growItoa(i%256) + ".0.0/16"
}

func fmtMac(i int) string {
	return "08:00:2b:01:02:" + fmtHex2(i)
}

func fmtHex2(i int) string {
	const hexdigits = "0123456789abcdef"
	v := i & 0xff
	return string([]byte{hexdigits[v>>4], hexdigits[v&0xf]})
}

func fmtTimeOfDay(i int) string {
	// HH:MM:SS.ffffff — the IR canonical time-of-day form.
	return fmt2(i%24) + ":" + fmt2((i*7)%60) + ":" + fmt2((i*13)%60) + ".123456"
}

func fmtTimeTZ(i int) string {
	// HH:MM:SS+ZZ — canonical timetz text (exercises registerPGTimetzCodec).
	return fmt2(i%24) + ":" + fmt2((i*7)%60) + ":" + fmt2((i*13)%60) + "+05"
}

func fmt2(i int) string {
	s := growItoa(i)
	if len(s) < 2 {
		return "0" + s
	}
	return s
}

func fmtBits8(i int) string {
	// 8-char '0'/'1' string for a fixed bit(8).
	var b [8]byte
	for p := 7; p >= 0; p-- {
		b[p] = byte('0' + (i & 1))
		i >>= 1
	}
	return string(b[:])
}

func fmtVarbits(i int) string {
	// A varying-length bit-string (1..16 bits) so varbit's value-bit-length
	// (not the declared width) is exercised — catalog Bug 75.
	n := 1 + (i % 16)
	b := make([]byte, n)
	for p := n - 1; p >= 0; p-- {
		b[p] = byte('0' + (i & 1))
		i >>= 1
	}
	return string(b)
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

// TestPGGrowGate_TerminalErrorMidChunkSurfacesLoudly_NotComplete pins the
// loud-fail-safe contract on the chunked path with a REAL db: a NON-retriable
// (terminal) error injected on a LATER chunk (chunk 2 of N) — after chunk 1 has
// really landed its rows — makes WriteRows return a non-nil error. The table
// copy is NOT reported complete; chunk-everywhere does not silently treat a
// partial table as done. (The operator recovers via the existing --reset/resume
// re-copy of the whole table.)
//
// A terminal 42501 (insufficient_privilege) is classified non-retriable by
// classifyApplierError, so the chunk-2 fault is surfaced without retry. The
// hook fires per-chunk on attempt 1; we count chunk boundaries and fault the
// SECOND chunk. We then assert the table is PARTIAL (chunk 1's rows landed,
// chunk 2 onward did not) — i.e. the run is incomplete and loud, never a silent
// full-table success.
func TestPGGrowGate_TerminalErrorMidChunkSurfacesLoudly_NotComplete(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	const nRows = 300
	rows := growFixtureRows(nRows)

	table := applyFixtureAndReadTable(t, ctx, dsn, "grow_terminal")

	withFastPGCopyBackoff(t)
	withSmallPGChunk(t, 50) // 300/50 ⇒ 6 chunks; the terminal hits chunk 2

	rwIface, err := Engine{}.OpenRowWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenRowWriter: %v", err)
	}
	defer closeIf(rwIface)
	rw := rwIface.(*RowWriter)
	rw.SetGrowGate(&recordingGrowGate{})

	terminal := &pgconn.PgError{Code: "42501", Message: "permission denied for table grow_terminal"}
	var chunkSeq int
	rw.copyChunkFaultHook = func(attempt int) error {
		// attempt resets to 1 per chunk; count chunk boundaries by attempt==1.
		if attempt == 1 {
			chunkSeq++
		}
		if chunkSeq == 2 {
			// Chunk 2: a TERMINAL error before any conn is acquired — chunk 1 has
			// already landed its rows against the real db.
			return terminal
		}
		return nil // every other chunk runs the real CopyFrom
	}

	err = rw.WriteRows(ctx, table, feedRows(rows))
	if err == nil {
		t.Fatal("WriteRows must return the terminal mid-chunk error LOUDLY (got nil — silent partial-as-complete)")
	}
	if !errors.Is(err, terminal) {
		t.Errorf("the terminal error must propagate verbatim; got %v", err)
	}

	// The table is PARTIAL: chunk 1's 50 rows landed (a real CopyFrom), chunk 2
	// onward did not. The key contract is incompleteness surfaced loudly, NOT
	// the exact count — assert it is strictly between 0 and the full fixture.
	got := tableCount(t, ctx, rw.db, "grow_terminal")
	if got <= 0 || got >= nRows {
		t.Errorf("partial-table count = %d; want 0 < count < %d (chunk 1 landed, the terminal aborted before the full table)", got, nRows)
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
