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
		// The arr here is the 3-D cell audit B-2d named as absent: Bug 74's
		// numeric[][] flatten was invisible at 1-D, and a codec that
		// preserves one nesting level can still lose the second. It also
		// carries a NULL element and an uppercase-collision leaf, so the
		// multi-dim path is graded on the same ambiguities as the scalars.
		name: `collision: uppercase \xDEAD as 6 bytes`,
		sql:  `convert_to('\xDEAD','UTF8')`,
		arr: `ARRAY[ARRAY[ARRAY[convert_to('\xDEAD','UTF8'), '\xbeef'::bytea], ARRAY[NULL::bytea, ''::bytea]],` +
			` ARRAY[ARRAY['\x00'::bytea, '\xff01'::bytea], ARRAY['\xdead'::bytea, '\x00ff'::bytea]]]`,
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
		// 2-D with a NULL element — case 2's 2-D array is all non-NULL, and
		// a NULL inside a nested dimension takes a different token path in
		// both pgx's array text and the pgoutput literal than a NULL in a
		// 1-D array (case 3).
		name: "genuine binary",
		sql:  `'\x00ff1080'::bytea`,
		arr:  `ARRAY[ARRAY['\x00ff'::bytea, NULL], ARRAY[''::bytea, '\xdead'::bytea]]`,
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

// assertByteaArrayShapeFloor is the anti-vacuity floor for the bytea[]
// half of the matrix (audit B-2d). It derives the shape inventory from
// the SERVER's own answers — never from the case table, which is what it
// is checking — and fails loudly if any required shape stopped being
// seeded: someone simplifying byteaProvCases must not be able to quietly
// turn the multi-dim / NULL-element / empty cells vacuous while every
// remaining assertion stays green.
func assertByteaArrayShapeFloor(t *testing.T, want map[int64]byteaGroundTruth) {
	t.Helper()
	var d1, d2, d3, nullElem, empty, nilArr bool
	for _, gt := range want {
		if gt.arrNil {
			nilArr = true
			continue
		}
		switch strings.Count(gt.arrDims, "[") {
		case 1:
			d1 = true
		case 2:
			d2 = true
		case 3:
			d3 = true
		case 0:
			// array_dims is NULL for an empty array; arrNil already
			// separated the NULL-array case above.
			empty = true
		}
		for _, e := range gt.arrElems {
			if e == "<NULL>" {
				nullElem = true
			}
		}
	}
	if !d1 || !d2 || !d3 || !nullElem || !empty || !nilArr {
		t.Fatalf("bytea[] shape floor (audit B-2d): 1-D=%v 2-D=%v 3-D=%v NULL-element=%v empty=%v NULL-array=%v — "+
			"every shape must be seeded or the matrix has gone vacuous", d1, d2, d3, nullElem, empty, nilArr)
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

// TestByteaProvenance_ExecModeShapeMatrix closes audit B-2d's Postgres
// half: bytea[] × pgx exec mode was pinned only on the 1-D, two-element,
// non-NULL shape (TestPremise_ByteaScanShapes binds THAT literal on
// purpose and keeps it), and Bug 74 originated in a driver-side
// difference — pgtype.Array[*string] flattening numeric[][] to 1-D —
// that no sluice code path could see. Exec mode × shape is therefore the
// cell that matters: the driver's decode route is chosen per mode and
// per target OID, so a green 1-D cell says nothing about 2-D, 3-D,
// NULL-element or empty deliveries.
//
// Each cell scans the array into *any in one exec mode, asserts the
// premise shape (a STRING in PG array-text form — the fact the array
// lane's scan-path hex-decode rests on), routes it through the REAL
// decoder (decodeValueFromBinary with ir.Array{Element: ir.Blob{}} —
// the same call the RowReader makes), and compares dimensionality and
// per-element bytes against the server's own array_dims/encode answers.
// A flatten, a dropped or mangled element, or a mode-dependent delivery
// change all fail here by name.
//
// The independent expected value (the 2026-08-01 rule) is again the
// server's array_dims + per-element encode(e,'hex'), read back through
// plain text columns that never reach decodeBytea.
func TestByteaProvenance_ExecModeShapeMatrix(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()

	// The shape axis. NULL-element cells put the NULL both in a flat
	// array and inside a nested dimension (different token paths in the
	// array text); the NULL-array row is the control that separates "no
	// array" from "an array with nothing in it".
	shapes := []struct {
		name string
		sql  string
	}{
		{"1-D", `ARRAY[convert_to('\xdead','UTF8'), ''::bytea, '\x00ff'::bytea]`},
		{"2-D", `ARRAY[ARRAY[convert_to('\xdead','UTF8'), '\xbeef'::bytea], ARRAY[''::bytea, '\x00'::bytea]]`},
		{
			"3-D",
			`ARRAY[ARRAY[ARRAY['\xde'::bytea, convert_to('\x','UTF8')], ARRAY[NULL::bytea, ''::bytea]],` +
				` ARRAY[ARRAY['\x00ff'::bytea, '\xad'::bytea], ARRAY[convert_to('\xDEAD','UTF8'), '\xff'::bytea]]]`,
		},
		{"NULL-element", `ARRAY[convert_to('\xdead','UTF8'), NULL, ''::bytea]`},
		{"2-D-NULL-element", `ARRAY[ARRAY['\xdead'::bytea, NULL], ARRAY[NULL::bytea, '\x00'::bytea]]`},
		{"empty", `ARRAY[]::bytea[]`},
		{"NULL-array", `NULL`},
	}

	applyDDL(t, dsn, `CREATE TABLE bytea_mode (id BIGINT PRIMARY KEY, ba bytea[]);`)
	var sb strings.Builder
	sb.WriteString("INSERT INTO bytea_mode (id, ba) VALUES\n")
	for i, sh := range shapes {
		if i > 0 {
			sb.WriteString(",\n")
		}
		fmt.Fprintf(&sb, "\t(%d, %s)", i+1, sh.sql)
	}
	sb.WriteString(";")
	applyDDL(t, dsn, sb.String())

	want := readByteaModeGroundTruth(t, dsn, len(shapes))

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
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

	cells := 0
	for _, m := range modes {
		m := m
		for i, sh := range shapes {
			i, sh := i, sh
			t.Run(m.name+"/"+sh.name, func(t *testing.T) {
				id := int64(i + 1)
				gt, known := want[id]
				if !known {
					t.Fatalf("no ground truth for id %d", id)
				}

				// The id is interpolated rather than bound on purpose: the
				// axis under test is the exec mode's decode route, and
				// binding a parameter changes what simple_protocol does
				// with the STATEMENT, which is not the thing being graded.
				var raw any
				if err := db.QueryRowContext(
					ctx, fmt.Sprintf(`SELECT ba FROM bytea_mode WHERE id = %d`, id), m.mode,
				).Scan(&raw); err != nil {
					t.Fatalf("scan: %v", err)
				}

				label := fmt.Sprintf("%s/%s", m.name, sh.name)
				if raw == nil {
					if !gt.isNil {
						t.Fatalf("%s: scanned NULL; server holds dims %q elems %v", label, gt.dims, gt.elems)
					}
					cells++
					return
				}
				if gt.isNil {
					t.Fatalf("%s: scanned %#v; server holds NULL", label, raw)
				}

				// Premise extension: the array-text-string delivery that
				// TestPremise_ByteaScanShapes pins for the 1-D shape must
				// hold for EVERY shape in EVERY mode — it is what makes
				// the scan-path hex-decode sound.
				if _, ok := raw.(string); !ok {
					t.Fatalf("%s: bytea[] scanned as %T; the scan lane's premise is a string in PG array-text form", label, raw)
				}

				decoded, err := decodeValueFromBinary(raw, ir.Array{Element: ir.Blob{Size: ir.BlobLong}})
				if err != nil {
					t.Fatalf("%s: decode: %v", label, err)
				}
				var flat []string
				flattenByteaArray(t, label, decoded, &flat)
				if strings.Join(flat, ",") != strings.Join(gt.elems, ",") {
					t.Errorf("%s: elements = %v; server holds %v", label, flat, gt.elems)
				}
				if got := decodedArrayDims(decoded); got != gt.dims {
					t.Errorf("%s: dims = %q; server's array_dims is %q — a dimensionality change is exactly Bug 74's shape",
						label, got, gt.dims)
				}
				cells++
			})
		}
	}

	// Anti-vacuity floor: 5 modes × 7 shapes. A refactor that quietly
	// drops shapes or modes must fail here rather than shrink coverage.
	if min := 30; cells < min {
		t.Fatalf("exec-mode × shape matrix exercised %d cells; want >= %d — the matrix has gone vacuous", cells, min)
	}
}

// byteaArrayShapeTruth is the server's own answer for one bytea_mode row.
type byteaArrayShapeTruth struct {
	dims  string   // PG array_dims; "" for empty and NULL
	elems []string // row-major element hex; "<NULL>" for a NULL element
	isNil bool
}

// readByteaModeGroundTruth reads the exec-mode matrix's independent
// expected values, the same way readByteaGroundTruth does for the main
// fixture: through text/int columns that never reach decodeBytea.
func readByteaModeGroundTruth(t *testing.T, dsn string, wantRows int) map[int64]byteaArrayShapeTruth {
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
		       ba IS NULL,
		       array_dims(ba),
		       (SELECT string_agg(coalesce(encode(e,'hex'),'<NULL>'), ',' ORDER BY o)
		          FROM unnest(ba) WITH ORDINALITY AS u(e,o))
		FROM bytea_mode
		ORDER BY id`)
	if err != nil {
		t.Fatalf("ground-truth query: %v", err)
	}
	defer func() { _ = rows.Close() }()

	out := map[int64]byteaArrayShapeTruth{}
	for rows.Next() {
		var (
			id     int64
			baNull bool
			dims   sql.NullString
			elems  sql.NullString
			gt     byteaArrayShapeTruth
		)
		if err := rows.Scan(&id, &baNull, &dims, &elems); err != nil {
			t.Fatalf("ground-truth scan: %v", err)
		}
		gt.isNil = baNull
		gt.dims = dims.String
		if elems.Valid && elems.String != "" {
			gt.elems = strings.Split(elems.String, ",")
		} else {
			gt.elems = []string{}
		}
		out[id] = gt
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("ground-truth iterate: %v", err)
	}
	if len(out) != wantRows {
		t.Fatalf("ground truth has %d rows; want %d", len(out), wantRows)
	}
	return out
}

// TestByteaProvenance_ScanLanes is the scan half of the item-135 matrix:
// provenance rows scan-scalar (ReadRows) and chunked scan
// (ReadRowsBatch / ReadRowsBatchBounded — the door the orchestrator's
// within-table chunking uses) × every value shape, plus the bytea[]
// shapes 1-D / 2-D / 3-D / NULL-element (1-D and nested) / empty / NULL,
// held to a floor by assertByteaArrayShapeFloor.
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
	assertByteaArrayShapeFloor(t, want)

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
	assertByteaArrayShapeFloor(t, want)

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
