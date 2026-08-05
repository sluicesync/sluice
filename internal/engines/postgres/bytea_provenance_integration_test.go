//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The item-135 bytea provenance matrix, and the premise pin underneath it.
//
// `bytea` is the one value family where Postgres's TEXT rendering and a
// genuine binary value are mutually ambiguous by content: `\xdead` is both
// the hex spelling of two bytes and six perfectly ordinary bytes. The
// decoder used to decide by looking at the content, so the SCAN path
// silently shrank the binary reading — measured on real PG 16 + pgx
// v5.10.0: 6 bytes → 2, and the bare 2-byte `\x` → ZERO. Provenance now
// decides, and this file is the ground truth that it decides correctly on
// every door the value can arrive through.
//
// **The independent expected value** (the 2026-08-01 rule) is the server's
// own `encode(v,'hex')` / `octet_length(v)` / `array_dims(v)`, read back as
// ordinary text and integer columns. Those never touch decodeBytea, so a
// green run here is evidence about the decoder rather than the decoder
// agreeing with itself.
//
// **Why the premise pin exists.** The whole fix rests on two facts about
// pgx that nothing in this repo asserted: a scalar `bytea` scanned into
// `*any` arrives as raw `[]byte`, and a `bytea[]` arrives as a STRING in
// PG array-text form with every element already hex-rendered. The second
// is why the audit's own proposed fix ("hex-decode only in decodeTuple")
// would have been a regression — the array lane is correct today precisely
// BECAUSE it hex-decodes on the scan path. Neither fact is sluice's to
// choose, so both get asserted against a real server.
//
// `bytea[]` had ZERO value-level coverage anywhere in the repo before this
// file — only an OID-parity pin — which is a large part of why the defect
// survived.

package postgres

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"sluicesync.dev/sluice/internal/ir"
)

// byteaProvCase is one row of the value-shape matrix. sql is the value
// expression as written into the fixture; everything expected of it is
// derived from the server afterwards, never from a hand-written want.
type byteaProvCase struct {
	name string
	// sql is the bytea expression for column b.
	sql string
	// arr is the bytea[] expression for column ba ("NULL" for none).
	arr string
}

// byteaProvCases is the value-shape half of the matrix. The first four
// are the COLLISION cells — server bytes that spell `\x`+even-hex, which
// the pre-fix content sniffing shrank. The next two are the mutation
// controls: the neighbours whose survival made the sniffing look safe
// (they are not legal hex, so the old code copied them verbatim and got
// the right answer for the wrong reason). The rest are the ordinary
// shapes a bytea column carries.
var byteaProvCases = []byteaProvCase{
	{
		name: `collision: \xdead as 6 bytes`,
		sql:  `convert_to('\xdead','UTF8')`,
		arr:  `ARRAY[convert_to('\xdead','UTF8'), ''::bytea, '\x00ff'::bytea]`,
	},
	{
		name: `collision: bare \x as 2 bytes (decoded to ZERO pre-fix)`,
		sql:  `convert_to('\x','UTF8')`,
		arr:  `ARRAY[ARRAY[convert_to('\xdead','UTF8'), '\xbeef'::bytea], ARRAY[''::bytea, '\x00'::bytea]]`,
	},
	{
		name: `collision: \xdeadbeef as 10 bytes`,
		sql:  `convert_to('\xdeadbeef','UTF8')`,
		arr:  `ARRAY[convert_to('\xdead','UTF8'), NULL, ''::bytea]`,
	},
	{
		name: `collision: uppercase \xDEAD as 6 bytes`,
		sql:  `convert_to('\xDEAD','UTF8')`,
		arr:  `NULL`,
	},
	{
		name: `control: invalid hex \xzz (4 bytes) — right answer pre-fix too`,
		sql:  `convert_to('\xzz','UTF8')`,
		arr:  `NULL`,
	},
	{
		name: `control: odd-length \xabc (5 bytes) — right answer pre-fix too`,
		sql:  `convert_to('\xabc','UTF8')`,
		arr:  `NULL`,
	},
	{
		name: "genuine binary",
		sql:  `'\x00ff1080'::bytea`,
		arr:  `NULL`,
	},
	{
		name: "empty",
		sql:  `''::bytea`,
		arr:  `ARRAY[]::bytea[]`,
	},
	{
		name: "embedded NUL",
		sql:  `'\x610062'::bytea`,
		arr:  `NULL`,
	},
	{
		name: "NULL",
		sql:  `NULL`,
		arr:  `NULL`,
	},
}

const byteaProvDDL = `
	CREATE TABLE bytea_prov (
		id BIGINT PRIMARY KEY,
		b  bytea,
		ba bytea[]
	);
	ALTER TABLE bytea_prov REPLICA IDENTITY FULL;
`

// byteaProvTable is the IR shape the readers are driven with.
func byteaProvTable() *ir.Table {
	cols := []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 64}},
		{Name: "b", Type: ir.Blob{Size: ir.BlobLong}, Nullable: true},
		{Name: "ba", Type: ir.Array{Element: ir.Blob{Size: ir.BlobLong}}, Nullable: true},
	}
	return &ir.Table{
		Schema:     "public",
		Name:       "bytea_prov",
		Columns:    cols,
		PrimaryKey: &ir.Index{Name: "bytea_prov_pkey", Columns: []ir.IndexColumn{{Column: "id"}}, Unique: true},
	}
}

// byteaProvInsert returns the fixture INSERT for the matrix.
func byteaProvInsert() string {
	var sb strings.Builder
	sb.WriteString("INSERT INTO bytea_prov (id, b, ba) VALUES\n")
	for i, c := range byteaProvCases {
		if i > 0 {
			sb.WriteString(",\n")
		}
		fmt.Fprintf(&sb, "\t(%d, %s, %s)", i+1, c.sql, c.arr)
	}
	sb.WriteString(";")
	return sb.String()
}

// byteaGroundTruth is the server's own answer for one row, read back
// through text/int columns that never reach decodeBytea.
type byteaGroundTruth struct {
	scalarHex string // "" + scalarNull for SQL NULL
	scalarLen int
	scalarNil bool

	arrDims  string   // PG array_dims, e.g. "[1:3]" / "[1:2][1:2]"
	arrElems []string // row-major element hex; "<NULL>" for a NULL element
	arrNil   bool
}

// readByteaGroundTruth asks the server what it holds. This is the
// INDEPENDENT expected value the matrix compares against — no sluice
// decode participates in producing it.
func readByteaGroundTruth(t *testing.T, dsn string) map[int64]byteaGroundTruth {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rows, err := db.QueryContext(ctx, `
		SELECT id,
		       encode(b,'hex'),
		       octet_length(b),
		       ba IS NULL,
		       array_dims(ba),
		       (SELECT string_agg(coalesce(encode(e,'hex'),'<NULL>'), ',' ORDER BY o)
		          FROM unnest(ba) WITH ORDINALITY AS u(e,o))
		FROM bytea_prov
		ORDER BY id`)
	if err != nil {
		t.Fatalf("ground-truth query: %v", err)
	}
	defer func() { _ = rows.Close() }()

	out := map[int64]byteaGroundTruth{}
	for rows.Next() {
		var (
			id     int64
			bHex   sql.NullString
			bLen   sql.NullInt64
			baNull bool
			dims   sql.NullString
			elems  sql.NullString
			gt     byteaGroundTruth
		)
		if err := rows.Scan(&id, &bHex, &bLen, &baNull, &dims, &elems); err != nil {
			t.Fatalf("ground-truth scan: %v", err)
		}
		gt.scalarNil = !bHex.Valid
		gt.scalarHex = bHex.String
		gt.scalarLen = int(bLen.Int64)
		// array_dims is NULL for BOTH a NULL array and an EMPTY one, so
		// the two are told apart by `ba IS NULL` rather than by dims.
		gt.arrNil = baNull
		gt.arrDims = dims.String
		if elems.Valid && elems.String != "" {
			gt.arrElems = strings.Split(elems.String, ",")
		} else {
			gt.arrElems = []string{}
		}
		out[id] = gt
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("ground-truth iterate: %v", err)
	}
	if len(out) != len(byteaProvCases) {
		t.Fatalf("ground truth has %d rows; want %d", len(out), len(byteaProvCases))
	}
	return out
}

// flattenByteaArray walks a decoded nested []any in row-major order,
// rendering each leaf the way `encode(e,'hex')` would.
func flattenByteaArray(t *testing.T, label string, v any, out *[]string) {
	t.Helper()
	elems, ok := v.([]any)
	if !ok {
		t.Fatalf("%s: array node is %T (%#v); want []any", label, v, v)
	}
	for _, e := range elems {
		switch leaf := e.(type) {
		case nil:
			*out = append(*out, "<NULL>")
		case []any:
			flattenByteaArray(t, label, leaf, out)
		case []byte:
			*out = append(*out, hex.EncodeToString(leaf))
		default:
			t.Fatalf("%s: array leaf is %T (%#v); want []byte", label, e, e)
		}
	}
}

// decodedArrayDims renders a decoded nested []any the way PG's
// array_dims would, so the shape can be compared without trusting
// sluice's own notion of dimensionality. An empty array has no dims,
// matching array_dims's NULL.
func decodedArrayDims(v any) string {
	var dims []string
	node := v
	for {
		elems, ok := node.([]any)
		if !ok || len(elems) == 0 {
			return strings.Join(dims, "")
		}
		dims = append(dims, fmt.Sprintf("[1:%d]", len(elems)))
		node = elems[0]
	}
}

// assertByteaRowsMatchServer compares decoded rows against the server's
// own rendering, per lane.
func assertByteaRowsMatchServer(t *testing.T, lane string, rows []ir.Row, want map[int64]byteaGroundTruth) {
	t.Helper()
	if len(rows) != len(byteaProvCases) {
		t.Fatalf("%s: got %d rows; want %d", lane, len(rows), len(byteaProvCases))
	}
	seen := map[int64]bool{}
	for _, row := range rows {
		id, ok := row["id"].(int64)
		if !ok {
			t.Fatalf("%s: row id is %T; want int64", lane, row["id"])
		}
		seen[id] = true
		gt, known := want[id]
		if !known {
			t.Fatalf("%s: decoded an id (%d) the server does not hold", lane, id)
		}
		label := fmt.Sprintf("%s/id=%d (%s)", lane, id, byteaProvCases[id-1].name)

		// ---- scalar bytea ----
		switch b := row["b"].(type) {
		case nil:
			if !gt.scalarNil {
				t.Errorf("%s: b decoded to NULL; server holds %q", label, gt.scalarHex)
			}
		case []byte:
			if gt.scalarNil {
				t.Errorf("%s: b decoded to %d bytes; server holds NULL", label, len(b))
				continue
			}
			if got := hex.EncodeToString(b); got != gt.scalarHex {
				t.Errorf("%s: b = %s (%d bytes); server holds %s (%d bytes)",
					label, got, len(b), gt.scalarHex, gt.scalarLen)
			}
			if len(b) != gt.scalarLen {
				t.Errorf("%s: b is %d bytes; server's octet_length is %d",
					label, len(b), gt.scalarLen)
			}
		default:
			t.Errorf("%s: b decoded to %T; want []byte", label, row["b"])
		}

		// ---- bytea[] ----
		if row["ba"] == nil {
			if !gt.arrNil {
				t.Errorf("%s: ba decoded to NULL; server holds dims %q elems %v",
					label, gt.arrDims, gt.arrElems)
			}
			continue
		}
		if gt.arrNil {
			t.Errorf("%s: ba decoded to %#v; server holds NULL", label, row["ba"])
			continue
		}
		var flat []string
		flattenByteaArray(t, label, row["ba"], &flat)
		if strings.Join(flat, ",") != strings.Join(gt.arrElems, ",") {
			t.Errorf("%s: ba elements = %v; server holds %v", label, flat, gt.arrElems)
		}
		if got := decodedArrayDims(row["ba"]); got != gt.arrDims {
			t.Errorf("%s: ba dims = %q; server's array_dims is %q", label, got, gt.arrDims)
		}
	}
	for id := range want {
		if !seen[id] {
			t.Errorf("%s: never decoded id %d", lane, id)
		}
	}
}

// TestPremise_ByteaScanShapes is the premise-naming step for item 135.
//
// The fix's safety argument cites two facts about pgx's `*any` scan, and
// both are facts about the world outside this repo: a scalar `bytea`
// arrives as raw `[]byte` (so the binary lane must copy verbatim), and a
// `bytea[]` arrives as a STRING in PG array-text form with every element
// already hex-rendered (so the array lane must hex-decode ON THE SCAN
// PATH — the half the audit's proposed fix would have removed). Neither
// was asserted anywhere before this test.
//
// Both are checked across every pgx exec mode, because the exec mode is
// exactly the kind of thing that changes a driver's decode shape without
// changing a line of sluice.
func TestPremise_ByteaScanShapes(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()

	applyDDL(t, dsn, byteaProvDDL+`
		INSERT INTO bytea_prov (id, b, ba) VALUES
			(1, convert_to('\xdead','UTF8'), ARRAY[convert_to('\xdead','UTF8'), '\x00ff'::bytea]);
	`)

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	modes := []struct {
		name string
		mode pgx.QueryExecMode
	}{
		{"cache_statement", pgx.QueryExecModeCacheStatement},
		{"cache_describe", pgx.QueryExecModeCacheDescribe},
		{"describe_exec", pgx.QueryExecModeDescribeExec},
		{"exec", pgx.QueryExecModeExec},
		{"simple_protocol", pgx.QueryExecModeSimpleProtocol},
	}
	for _, m := range modes {
		m := m
		t.Run(m.name, func(t *testing.T) {
			var scalar, arr any
			if err := db.QueryRowContext(
				ctx,
				`SELECT b, ba FROM bytea_prov WHERE id = 1`, m.mode,
			).Scan(&scalar, &arr); err != nil {
				t.Fatalf("scan: %v", err)
			}

			// Premise 1 — a scalar bytea is RAW BYTES. `\xdead` stored
			// as six bytes must arrive as six bytes; if pgx ever
			// delivered the text rendering here, the binary lane's
			// verbatim copy would be storing ASCII.
			b, ok := scalar.([]byte)
			if !ok {
				t.Fatalf("scalar bytea scanned as %T; the binary lane's premise is that it is []byte", scalar)
			}
			if string(b) != `\xdead` {
				t.Fatalf("scalar bytea = %q (%d bytes); want the 6 raw bytes `\\xdead`", b, len(b))
			}

			// Premise 2 — a bytea[] is PG ARRAY TEXT with every element
			// hex-rendered. This is what makes hex-decoding on the SCAN
			// path load-bearing rather than a bug, and it is the exact
			// fact the audit's proposed fix contradicted.
			//
			// The assertion is the FULL literal on purpose, because it
			// BINDS the two premises rather than pinning them apart:
			// the array's first element is the SAME six stored bytes as
			// the scalar above, and here they arrive hex-rendered
			// (5c7864656164) with the rendering's own backslash escaped
			// by array_out. Two facts pinned separately would leave the
			// argument that connects them unpinned.
			s, ok := arr.(string)
			if !ok {
				t.Fatalf("bytea[] scanned as %T; the array lane's premise is that it is a string in PG array-text form", arr)
			}
			const wantArrayText = `{"\\x5c7864656164","\\x00ff"}`
			if s != wantArrayText {
				t.Fatalf("bytea[] scanned as %q; want %q", s, wantArrayText)
			}
		})
	}
}

// TestByteaProvenance_ScanLanes is the scan half of the item-135 matrix:
// provenance rows scan-scalar (ReadRows) and chunked scan
// (ReadRowsBatch / ReadRowsBatchBounded — the door the orchestrator's
// within-table chunking uses) × every value shape, plus the bytea[]
// shapes 1-D / 2-D / NULL-element / empty / NULL.
//
// Scope, stated rather than implied: BOTH scan doors funnel through the
// single RowReader.stream, so this covers every non-raw-COPY scan path
// this engine has — the PG reader implements no work-stealing or range
// reader. The raw-COPY passthrough lane never decodes a value at all and
// is therefore out of scope by construction, but it is NOT a blanket
// "PG→PG is immune": rawCopyGate refuses raw copy for --redact,
// --type-override, --expr-override, --inject-shard-column and --where,
// and identityProjection routes any table carrying an ir.Array column
// (i.e. every bytea[] table) to this IR path regardless.
//
// The row count is a THRESHOLD for the ORCHESTRATOR, not a property of
// this reader — driving ReadRowsBatch directly exercises the identical
// readRowsBatch → stream path without seeding 100k rows. The orchestrator
// actually routing there is pinned in internal/pipeline.
func TestByteaProvenance_ScanLanes(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()

	applyDDL(t, dsn, byteaProvDDL)
	applyDDL(t, dsn, byteaProvInsert())
	want := readByteaGroundTruth(t, dsn)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	rr, err := Engine{}.OpenRowReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenRowReader: %v", err)
	}
	defer func() {
		if c, ok := rr.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	tbl := byteaProvTable()

	t.Run("scan-scalar/ReadRows", func(t *testing.T) {
		assertByteaRowsMatchServer(t, "ReadRows", drainAllRows(t, ctx, rr, tbl), want)
	})

	batched, ok := rr.(ir.BoundedBatchedRowReader)
	if !ok {
		t.Fatal("PG RowReader no longer implements ir.BoundedBatchedRowReader; the chunked-scan lane is unpinned")
	}

	t.Run("chunked-scan/ReadRowsBatch", func(t *testing.T) {
		// Two pages, so the cursor predicate is genuinely exercised.
		var got []ir.Row
		var after []any
		for {
			ch, err := batched.ReadRowsBatch(ctx, tbl, after, 4)
			if err != nil {
				t.Fatalf("ReadRowsBatch: %v", err)
			}
			n := 0
			for row := range ch {
				got = append(got, row)
				after = []any{row["id"]}
				n++
			}
			if n < 4 {
				break
			}
		}
		assertByteaRowsMatchServer(t, "ReadRowsBatch", got, want)
	})

	t.Run("chunked-scan/ReadRowsBatchBounded", func(t *testing.T) {
		ch, err := batched.ReadRowsBatchBounded(ctx, tbl, nil, []any{int64(len(byteaProvCases))}, 1000)
		if err != nil {
			t.Fatalf("ReadRowsBatchBounded: %v", err)
		}
		var got []ir.Row
		for row := range ch {
			got = append(got, row)
		}
		assertByteaRowsMatchServer(t, "ReadRowsBatchBounded", got, want)
	})
}

// TestByteaProvenance_CDCTuple is the text half of the matrix: the
// pgoutput tuple door, where the value genuinely IS PG's `\x`-hex text
// and MUST be hex-decoded. It is the lane the old sniffing decoder got
// right, and the reason the fix could not simply stop hex-decoding.
func TestByteaProvenance_CDCTuple(t *testing.T) {
	dsn, cleanup := startPostgresForCDC(t)
	defer cleanup()

	applyPGSQL(t, dsn, byteaProvDDL)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	rdr, err := Engine{}.OpenCDCReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}
	defer func() {
		if c, ok := rdr.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	changes, err := rdr.StreamChanges(ctx, ir.Position{})
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}
	time.Sleep(200 * time.Millisecond)

	applyPGSQL(t, dsn, byteaProvInsert())
	want := readByteaGroundTruth(t, dsn)

	got := drainChanges(t, ctx, changes, len(byteaProvCases), 60*time.Second)
	if len(got) != len(byteaProvCases) {
		if r, ok := rdr.(*CDCReader); ok && r.Err() != nil {
			t.Fatalf("got %d changes; want %d (stream error: %v)", len(got), len(byteaProvCases), r.Err())
		}
		t.Fatalf("got %d changes; want %d", len(got), len(byteaProvCases))
	}

	rows := make([]ir.Row, 0, len(got))
	for _, c := range got {
		ins, ok := c.(ir.Insert)
		if !ok {
			t.Fatalf("change = %T; want ir.Insert", c)
		}
		rows = append(rows, ins.Row)
	}
	assertByteaRowsMatchServer(t, "pgoutputTuple", rows, want)
}
