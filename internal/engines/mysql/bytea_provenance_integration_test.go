//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The real-server half of item 135 / audit B-2 on the MySQL side, and the
// gate proposal G-4's MySQL row: a bytea family matrix ground-truthed with
// OCTET_LENGTH().
//
// # Why a real server is load-bearing here
//
// The unit matrix (bytea_provenance_test.go) grades [prepareValue] in
// isolation. What it cannot see is what the DRIVER and the SERVER then do
// with the value it produced — and the three MySQL write cores hand their
// prepared values over by three different routes: bound parameters in a
// multi-row INSERT, a tab-separated LOAD DATA stream with its own escaping
// layer, and the applier's own statement builder. A value that survives
// prepareValue and is then mangled by tsvEncode is the same corruption
// with a different author, and only a real server can tell us.
//
// # The independent expected value (the 2026-08-01 rule)
//
// Every assertion compares against the server's own OCTET_LENGTH() and
// HEX(), read back through a plain database/sql scan that touches none of
// sluice's write path. It is not "the writer agrees with the writer": the
// number that would have exposed the pre-fix bug is a LENGTH, and the
// server is the only thing that can report it.
//
// # The roster this covers, stated rather than implied
//
// All three MySQL write cores, because a guard on two of three is the
// shape that keeps costing this project (CLAUDE.md's sibling-sweep step):
//
//	batched multi-row INSERT — Engine{Flavor: FlavorPlanetScale}.OpenRowWriter
//	LOAD DATA INFILE         — Engine{Flavor: FlavorVanilla}.OpenRowWriter
//	ChangeApplier            — Engine{}.OpenChangeApplier (the CDC door)
//
// The CDC door is the one that matters most for B-2: `--type-override`
// rewrites the type the cold-start READER decodes with, so on `migrate`
// the PG reader hands back []byte and this branch is never reached. The
// two CDC doors take their column types from the SOURCE (pgoutput's
// Relation message; pgtrigger's untyped JSON payload) and are blind to the
// override — which is why cold-start stored six bytes and CDC then shrank
// the same cell to two.

package mysql

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// byteaProvIntegrationDDL is the fixture. Three binary families so the
// matrix is per-family rather than per-representative (the Bug 74
// lesson); VARBINARY/BLOB are variable-length so OCTET_LENGTH reports the
// stored length exactly, and BINARY(16) is included as the fixed-width
// family whose zero-padding is a DIFFERENT (and expected) length answer.
const byteaProvIntegrationDDL = `
	CREATE TABLE bytea_prov (
		id     BIGINT       NOT NULL,
		b      LONGBLOB     NULL,
		vb     VARBINARY(64) NULL,
		PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
`

// byteaProvIntegrationCase is one row of the value matrix. Each case is
// written twice: once on a NATIVELY-binary column (a PG `bytea` captured
// by the pgtrigger reader, which surfaces it as `\x`-hex text) and once
// on a column an override MADE binary (a PG `text` column under
// `--type-override=col=bytea`, whose value is its own bytes).
type byteaProvIntegrationCase struct {
	name string
	// in is the string the reader handed the writer.
	in string
	// wantNativeHex is the uppercase hex the server must hold for the
	// natively-binary reading, or "" when that reading must be REFUSED.
	wantNativeHex string
	wantRefusal   bool
}

// byteaProvIntegrationCases is the shape axis: spells `\x`+valid hex,
// bare `\x`, `\x`+odd-length hex, `\x`+non-hex, empty,
// genuinely-hex-encoded — plus ordinary text, the shape an overridden
// column most often carries.
var byteaProvIntegrationCases = []byteaProvIntegrationCase{
	{name: `collision \xdead`, in: `\xdead`, wantNativeHex: "DEAD"},
	{name: `collision bare \x`, in: `\x`, wantNativeHex: ""},
	{name: `collision \xdeadbeef`, in: `\xdeadbeef`, wantNativeHex: "DEADBEEF"},
	{name: "genuinely hex-encoded", in: `\x00ff1080`, wantNativeHex: "00FF1080"},
	{name: "hex with embedded NUL", in: `\x610062`, wantNativeHex: "610062"},
	// `\x`-prefixed but unparseable: a rendering ATTEMPT with no faithful
	// reading, refused by name.
	{name: `\x + odd-length hex`, in: `\xabc`, wantRefusal: true},
	{name: `\x + non-hex`, in: `\xzz`, wantRefusal: true},
	// No `\x` prefix: never a rendering, so the bytes are the only
	// reading and they pass through on BOTH provenances. See the WART
	// note in prepareValue for why the scalar branch does not refuse
	// these the way byteaArrayLeaf does.
	{name: "empty", in: "", wantNativeHex: ""},
	{name: "escape rendering", in: `\001\002abc`, wantNativeHex: "5C3030315C303032616263"},
	{name: "ordinary text", in: "hello", wantNativeHex: "68656C6C6F"},
}

// byteaProvTable builds the IR table the writers are driven with.
// overridden selects the column provenance: when true, both binary
// columns carry a SourceColumnType of ir.Text — exactly what
// translate.ApplyMappings records for `--type-override=col=bytea` on a
// PG `text` column (pinned in internal/translate by
// TestApplyMappings_RecordsSourceType).
func byteaProvTable(overridden bool) *ir.Table {
	var srcType ir.Type
	if overridden {
		srcType = ir.Text{Size: ir.TextLong}
	}
	return &ir.Table{
		Name: "bytea_prov",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "b", Type: ir.Blob{Size: ir.BlobLong}, Nullable: true, SourceColumnType: srcType},
			{Name: "vb", Type: ir.Varbinary{Length: 64}, Nullable: true, SourceColumnType: srcType},
		},
		PrimaryKey: &ir.Index{
			Name:    "PRIMARY",
			Columns: []ir.IndexColumn{{Column: "id"}},
			Unique:  true,
		},
	}
}

// byteaProvRows builds one ir.Row per case, both binary columns carrying
// the same value so a per-column divergence is visible.
func byteaProvRows(cases []byteaProvIntegrationCase) []ir.Row {
	out := make([]ir.Row, 0, len(cases))
	for i, c := range cases {
		out = append(out, ir.Row{"id": int64(i + 1), "b": c.in, "vb": c.in})
	}
	return out
}

// storedBytea is the server's own answer for one row: OCTET_LENGTH and
// HEX, read back through a plain scan that no sluice write code touches.
type storedBytea struct {
	bLen  int
	bHex  string
	vbLen int
	vbHex string
}

func readStoredBytea(t *testing.T, ctx context.Context, dsn string) map[int64]storedBytea {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	rows, err := db.QueryContext(ctx,
		`SELECT id, OCTET_LENGTH(b), HEX(b), OCTET_LENGTH(vb), HEX(vb) FROM bytea_prov ORDER BY id`)
	if err != nil {
		t.Fatalf("ground-truth query: %v", err)
	}
	defer func() { _ = rows.Close() }()

	out := map[int64]storedBytea{}
	for rows.Next() {
		var (
			id int64
			s  storedBytea
		)
		if err := rows.Scan(&id, &s.bLen, &s.bHex, &s.vbLen, &s.vbHex); err != nil {
			t.Fatalf("ground-truth scan: %v", err)
		}
		out[id] = s
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("ground-truth iterate: %v", err)
	}
	return out
}

// assertStoredBytea compares the server's stored bytes against want,
// naming OCTET_LENGTH explicitly because the pre-fix failure was a LENGTH
// (6 → 2, and the bare `\x` → ZERO): a value that goes EMPTY rather than
// wrong is exactly what a hex comparison alone reads past.
func assertStoredBytea(t *testing.T, lane string, got map[int64]storedBytea, cases []byteaProvIntegrationCase, wantHex func(byteaProvIntegrationCase) string) {
	t.Helper()
	if len(got) != len(cases) {
		t.Fatalf("%s: server holds %d rows; want %d", lane, len(got), len(cases))
	}
	for i, c := range cases {
		id := int64(i + 1)
		s, ok := got[id]
		if !ok {
			t.Errorf("%s: no row id=%d (%s)", lane, id, c.name)
			continue
		}
		want := wantHex(c)
		wantLen := len(want) / 2
		if s.bHex != want || s.bLen != wantLen {
			t.Errorf("%s/%s: LONGBLOB stored HEX=%s OCTET_LENGTH=%d; want HEX=%s OCTET_LENGTH=%d",
				lane, c.name, s.bHex, s.bLen, want, wantLen)
		}
		if s.vbHex != want || s.vbLen != wantLen {
			t.Errorf("%s/%s: VARBINARY stored HEX=%s OCTET_LENGTH=%d; want HEX=%s OCTET_LENGTH=%d",
				lane, c.name, s.vbHex, s.vbLen, want, wantLen)
		}
	}
}

// nativeHex is the expected hex for the natively-binary reading.
func nativeHex(c byteaProvIntegrationCase) string { return c.wantNativeHex }

// verbatimHex is the expected hex for the overridden reading: the source
// string's OWN bytes, which is the reading the pre-fix writer destroyed
// whenever they happened to spell `\x`+hex.
func verbatimHex(c byteaProvIntegrationCase) string {
	return strings.ToUpper(hex.EncodeToString([]byte(c.in)))
}

// acceptedCases are the cases the natively-binary lane stores rather than
// refuses. The refused ones are asserted separately (a refusal writes no
// row, so it cannot share a stored-bytes assertion).
func acceptedCases() []byteaProvIntegrationCase {
	out := make([]byteaProvIntegrationCase, 0, len(byteaProvIntegrationCases))
	for _, c := range byteaProvIntegrationCases {
		if !c.wantRefusal {
			out = append(out, c)
		}
	}
	return out
}

// byteaProvWriter is one write core under test.
//
// gradesOverride says whether the core can see the column provenance at
// all. Both RowWriter cores are driven with the caller's *ir.Table, which
// is the MAPPED schema in production — so SourceColumnType reaches them.
// The ChangeApplier is not: [ir.Insert] carries a table NAME, and the
// applier resolves column descriptors from the TARGET's
// information_schema, where an override has left no trace. That is not a
// gap in this test; it is the reason
// preflightBinaryTypeOverrideOnCDC (internal/pipeline) refuses the
// combination outright, so the applier can never be handed it.
type byteaProvWriter struct {
	name           string
	gradesOverride bool
	write          func(t *testing.T, ctx context.Context, dsn string, table *ir.Table, rows []ir.Row) error
}

func byteaProvWriteCores() []byteaProvWriter {
	return []byteaProvWriter{
		{
			name:           "batched-insert",
			gradesOverride: true,
			write: func(t *testing.T, ctx context.Context, dsn string, table *ir.Table, rows []ir.Row) error {
				t.Helper()
				return byteaProvWriteRows(t, ctx, Engine{Flavor: FlavorPlanetScale}, dsn, table, rows)
			},
		},
		{
			name:           "load-data-infile",
			gradesOverride: true,
			write: func(t *testing.T, ctx context.Context, dsn string, table *ir.Table, rows []ir.Row) error {
				t.Helper()
				return byteaProvWriteRows(t, ctx, Engine{Flavor: FlavorVanilla}, dsn, table, rows)
			},
		},
		{
			// gradesOverride:false — see the struct doc. The applier reads
			// column types from the target, so it is exempt HERE and
			// covered by a pipeline refusal instead.
			name: "change-applier",
			write: func(t *testing.T, ctx context.Context, dsn string, table *ir.Table, rows []ir.Row) error {
				t.Helper()
				return byteaProvApply(t, ctx, dsn, table, rows)
			},
		},
	}
}

func byteaProvWriteRows(t *testing.T, ctx context.Context, eng Engine, dsn string, table *ir.Table, rows []ir.Row) error {
	t.Helper()
	rw, err := eng.OpenRowWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenRowWriter: %v", err)
	}
	defer closeIf(rw)
	in := make(chan ir.Row, len(rows))
	for _, r := range rows {
		in <- r
	}
	close(in)
	return rw.WriteRows(ctx, table, in)
}

func byteaProvApply(t *testing.T, ctx context.Context, dsn string, table *ir.Table, rows []ir.Row) error {
	t.Helper()
	applier, err := Engine{}.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer closeIf(applier)
	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}
	ch := make(chan ir.Change, len(rows))
	for i, r := range rows {
		ch <- ir.Insert{
			Position: ir.Position{Engine: "mysql", Token: fmt.Sprintf("bytea-prov-%d", i)},
			// ir.Insert carries a table NAME, not a descriptor — the
			// applier resolves column types from the target itself. That
			// is precisely why this core cannot grade the override lane.
			Table: table.Name,
			Row:   r,
		}
	}
	close(ch)
	return applier.Apply(ctx, "bytea-prov-stream", ch)
}

// TestByteaProvenance_MySQLWriteCores is the matrix: three write cores ×
// two column provenances × the value shapes, ground-truthed with
// OCTET_LENGTH().
func TestByteaProvenance_MySQLWriteCores(t *testing.T) {
	for _, core := range byteaProvWriteCores() {
		core := core
		t.Run(core.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
			defer cancel()

			// ---- Natively binary: the value IS PG's bytea rendering. ----
			t.Run("natively-binary", func(t *testing.T) {
				dsn, cleanup := newSharedDB(t, "bytea_prov_native_"+strings.ReplaceAll(core.name, "-", "_"))
				defer cleanup()
				applyDDL(t, dsn, byteaProvIntegrationDDL)

				accepted := acceptedCases()
				if err := core.write(t, ctx, dsn, byteaProvTable(false), byteaProvRows(accepted)); err != nil {
					t.Fatalf("write accepted renderings: %v", err)
				}
				assertStoredBytea(t, core.name+"/native", readStoredBytea(t, ctx, dsn), accepted, nativeHex)
			})

			// ---- Overridden: --type-override made a text column binary,
			// so the value is the SOURCE's own bytes and must land verbatim.
			// This is the cell audit B-2 named; pre-fix, the four collision
			// shapes here stored SHORTER values than the source held.
			if !core.gradesOverride {
				t.Log("core does not see ir.Column.SourceColumnType (target-derived " +
					"descriptors); the combination is refused in the pipeline instead — " +
					"see preflightBinaryTypeOverrideOnCDC")
				return
			}
			t.Run("overridden-from-text", func(t *testing.T) {
				dsn, cleanup := newSharedDB(t, "bytea_prov_ovr_"+strings.ReplaceAll(core.name, "-", "_"))
				defer cleanup()
				applyDDL(t, dsn, byteaProvIntegrationDDL)

				if err := core.write(t, ctx, dsn, byteaProvTable(true), byteaProvRows(byteaProvIntegrationCases)); err != nil {
					t.Fatalf("write overridden column: %v", err)
				}
				assertStoredBytea(t, core.name+"/overridden",
					readStoredBytea(t, ctx, dsn), byteaProvIntegrationCases, verbatimHex)
			})
		})
	}
}

// TestByteaProvenance_MySQLRefusesUnreadableRendering is the loud-failure
// half. On a NATIVELY-binary column a rendering that is not `\x`+even-hex
// has no faithful reading — verbatim would store the ASCII of the
// rendering — so it must fail by name rather than land.
//
// Asserted per write core, because a refusal that fires in prepareValue
// but is swallowed by one core's error path is the same silent corruption
// with a longer stack.
func TestByteaProvenance_MySQLRefusesUnreadableRendering(t *testing.T) {
	for _, core := range byteaProvWriteCores() {
		core := core
		t.Run(core.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
			defer cancel()
			for _, c := range byteaProvIntegrationCases {
				if !c.wantRefusal {
					continue
				}
				c := c
				t.Run(c.name, func(t *testing.T) {
					dsn, cleanup := newSharedDB(t, "bytea_prov_refuse")
					defer cleanup()
					applyDDL(t, dsn, byteaProvIntegrationDDL)

					err := core.write(t, ctx, dsn,
						byteaProvTable(false),
						byteaProvRows([]byteaProvIntegrationCase{c}))
					if err == nil {
						t.Fatalf("wrote %q to a natively-binary column without a refusal; "+
							"server holds %+v", c.in, readStoredBytea(t, ctx, dsn))
					}
					// The code must survive each core's own error
					// wrapping — a refusal whose code is lost on the way
					// out reaches the operator as a bare decode string
					// with no remedy, which is most of the value gone.
					ce, ok := sluicecode.FromError(err)
					if !ok || ce.Code != sluicecode.CodeValueByteaTextUnrecognized {
						t.Errorf("refusal for %q = %v; want %s to survive the core's wrapping",
							c.in, err, sluicecode.CodeValueByteaTextUnrecognized)
					}
				})
			}
		})
	}
}
