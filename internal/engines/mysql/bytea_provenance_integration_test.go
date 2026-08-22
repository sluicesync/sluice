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
// That claim was FALSE as first written, and the correction is the
// reason [byteaProvWriter.statementCounter] exists: writeLoadData falls
// back to writeBatched when the server reports `local_infile=OFF`, which
// is MySQL 8's default and what the shared container boots with, so both
// "LOAD DATA" subtests were byte-identical re-runs of the batched core
// and the TSV escaping layer named above as a distinct corruption author
// went unexercised. Each core now (a) prepares its own path — the LOAD
// DATA lane flips `local_infile` and SKIPS if it cannot — and (b) proves
// the path was taken by requiring the SERVER's own `Com_load` /
// `Com_insert` counter to advance across the write. A subtest that
// silently falls back is indistinguishable from one that passes.
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
// stored length exactly, and BINARY(16) is the fixed-width family whose
// zero-padding is a DIFFERENT (and expected) length answer — which is
// precisely why it belongs here: OCTET_LENGTH is the number the pre-fix
// bug moved, and BINARY is the one column type where that number is not
// the decoded length. (It was named in this comment and missing from the
// DDL until the 2026-08-07 review; a fixture comment that overstates its
// own coverage is the same defect as a gate name that does.)
const byteaProvIntegrationDDL = `
	CREATE TABLE bytea_prov (
		id     BIGINT        NOT NULL,
		b      LONGBLOB      NULL,
		vb     VARBINARY(64) NULL,
		bx     BINARY(16)    NULL,
		PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
`

// byteaProvFixedWidth is the declared width of the BINARY column, in
// bytes. Every accepted case value is shorter than this (the longest is
// 11 bytes), so the fixed-width expectation is always "the value, then
// zero padding" and never a strict-mode length refusal.
const byteaProvFixedWidth = 16

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
	// Uppercase and mixed-case hex (audit B-2d): PG's own emitters render
	// lowercase, but PG *accepts* uppercase bytea input, so an upstream
	// producer or a hand-written trigger can legitimately hand this lane
	// `\xDEAD` — the Postgres matrix has this cell and MySQL did not. On
	// the natively-binary reading it must decode to the same two bytes as
	// `\xdead`; on the overridden reading its verbatim bytes DIFFER from
	// the lowercase spelling's, which is what makes it a distinct cell
	// rather than a re-run.
	{name: `collision uppercase \xDEAD`, in: `\xDEAD`, wantNativeHex: "DEAD"},
	{name: `collision mixed-case \xDeAd`, in: `\xDeAd`, wantNativeHex: "DEAD"},
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
			{Name: "bx", Type: ir.Binary{Length: byteaProvFixedWidth}, Nullable: true, SourceColumnType: srcType},
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
		out = append(out, ir.Row{"id": int64(i + 1), "b": c.in, "vb": c.in, "bx": c.in})
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
	bxLen int
	bxHex string
}

func readStoredBytea(t *testing.T, ctx context.Context, dsn string) map[int64]storedBytea {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	rows, err := db.QueryContext(ctx,
		`SELECT id, OCTET_LENGTH(b), HEX(b), OCTET_LENGTH(vb), HEX(vb), OCTET_LENGTH(bx), HEX(bx) `+
			`FROM bytea_prov ORDER BY id`)
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
		if err := rows.Scan(&id, &s.bLen, &s.bHex, &s.vbLen, &s.vbHex, &s.bxLen, &s.bxHex); err != nil {
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
		// The fixed-width family answers with a DIFFERENT length: MySQL
		// right-pads BINARY(N) with 0x00, so OCTET_LENGTH is always N and
		// the padding is what carries the decoded length. A hex compare
		// alone would read past a value that lost bytes and gained zeros.
		wantPadded := want + strings.Repeat("00", byteaProvFixedWidth-wantLen)
		if s.bxHex != wantPadded || s.bxLen != byteaProvFixedWidth {
			t.Errorf("%s/%s: BINARY(%d) stored HEX=%s OCTET_LENGTH=%d; want HEX=%s OCTET_LENGTH=%d",
				lane, c.name, byteaProvFixedWidth, s.bxHex, s.bxLen, wantPadded, byteaProvFixedWidth)
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
// statementCounter is the MySQL GLOBAL STATUS variable whose increase
// proves THIS core's statement actually reached the server — the
// independent evidence that the subtest is not a re-run of a sibling
// core. It is load-bearing for `load-data-infile`: writeLoadData falls
// back to writeBatched when the server has `local_infile=OFF` (MySQL 8's
// default, and the shared container passes no `--local-infile`), so both
// LOAD DATA subtests were byte-identical re-runs of the batched core and
// the TSV escaping layer this file's header names as a distinct
// corruption author was never exercised. A subtest that silently falls
// back is indistinguishable from one that passes.
//
// prepare is the server-side setup that makes the core's own path
// REACHABLE (again: local_infile), and skips the subtest when it cannot
// be granted rather than grading the wrong code path.
type byteaProvWriter struct {
	name             string
	gradesOverride   bool
	statementCounter string
	prepare          func(t *testing.T, ctx context.Context, dsn string)
	write            func(t *testing.T, ctx context.Context, dsn string, table *ir.Table, rows []ir.Row) error
}

func byteaProvWriteCores() []byteaProvWriter {
	return []byteaProvWriter{
		{
			name:             "batched-insert",
			gradesOverride:   true,
			statementCounter: "Com_insert",
			write: func(t *testing.T, ctx context.Context, dsn string, table *ir.Table, rows []ir.Row) error {
				t.Helper()
				return byteaProvWriteRows(t, ctx, Engine{Flavor: FlavorPlanetScale}, dsn, table, rows)
			},
		},
		{
			name:             "load-data-infile",
			gradesOverride:   true,
			statementCounter: "Com_load",
			prepare:          enableLocalInfileOrSkip,
			write: func(t *testing.T, ctx context.Context, dsn string, table *ir.Table, rows []ir.Row) error {
				t.Helper()
				return byteaProvWriteRows(t, ctx, Engine{Flavor: FlavorVanilla}, dsn, table, rows)
			},
		},
		{
			// gradesOverride:false — see the struct doc. The applier reads
			// column types from the target, so it is exempt HERE and
			// covered by a pipeline refusal instead.
			name:             "change-applier",
			statementCounter: "Com_insert",
			write: func(t *testing.T, ctx context.Context, dsn string, table *ir.Table, rows []ir.Row) error {
				t.Helper()
				return byteaProvApply(t, ctx, dsn, table, rows)
			},
		},
	}
}

// enableLocalInfileOrSkip flips the server-global `local_infile` so
// writeLoadData takes its own path instead of the batched fallback. Same
// shape (and same skip-if-denied reasoning) as the LOAD DATA pin in
// connect_sqlmode_integration_test.go: on a server where the flip is not
// grantable the LOAD DATA lane is simply not testable, and grading the
// batched core under the LOAD DATA name is worse than skipping.
func enableLocalInfileOrSkip(t *testing.T, ctx context.Context, dsn string) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open for local_infile: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.ExecContext(ctx, "SET GLOBAL local_infile = 1"); err != nil {
		t.Skipf("LOAD DATA lane needs SET GLOBAL local_infile = 1; cannot grant: %v", err)
	}
}

// readGlobalStatus reads one global status counter. GLOBAL rather than
// session because the writer runs on its OWN pooled connection — a
// session counter read from the test's connection would always be zero,
// which is exactly the vacuous-green shape this check exists to avoid.
//
// The name is interpolated rather than bound, because neither
// parameterised form works for this read: `SHOW GLOBAL STATUS LIKE ?`
// is a syntax error (1064 — SHOW takes no placeholders), and
// `performance_schema.global_status` carries no `Com_*` rows at all on
// MySQL 8 (they are command counters, not server status), so a bound
// query against it returns sql.ErrNoRows. The values come from
// [byteaProvWriter.statementCounter], a closed set of literals in this
// file, and the pattern guard below keeps it that way.
func readGlobalStatus(t *testing.T, ctx context.Context, dsn, name string) int64 {
	t.Helper()
	for _, r := range name {
		if r != '_' && (r < 'A' || r > 'z') {
			t.Fatalf("status counter %q is not a bare identifier", name)
		}
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open for status: %v", err)
	}
	defer func() { _ = db.Close() }()
	var (
		varName string
		value   int64
	)
	if err := db.QueryRowContext(ctx, "SHOW GLOBAL STATUS LIKE '"+name+"'").Scan(&varName, &value); err != nil {
		t.Fatalf("read global status %q: %v", name, err)
	}
	return value
}

// byteaProvAssertPathProbe writes ONE accepted value through the core on
// a throwaway database and requires the core's own statement counter to
// advance. It exists for the refusal test, where the refusal precedes
// the statement and so leaves no counter to read.
func byteaProvAssertPathProbe(t *testing.T, ctx context.Context, core byteaProvWriter) {
	t.Helper()
	if core.statementCounter == "" {
		return
	}
	dsn, cleanup := newSharedDB(t, "bytea_prov_probe_"+strings.ReplaceAll(core.name, "-", "_"))
	defer cleanup()
	if core.prepare != nil {
		core.prepare(t, ctx, dsn)
	}
	applyDDL(t, dsn, byteaProvIntegrationDDL)

	probe := []byteaProvIntegrationCase{{name: "path probe", in: `\x00ff1080`, wantNativeHex: "00FF1080"}}
	before := readGlobalStatus(t, ctx, dsn, core.statementCounter)
	if err := core.write(t, ctx, dsn, byteaProvTable(false), byteaProvRows(probe)); err != nil {
		t.Fatalf("path probe write: %v", err)
	}
	assertStatementCounterAdvanced(t, core, before, readGlobalStatus(t, ctx, dsn, core.statementCounter))
}

// assertStatementCounterAdvanced is the "was this path actually taken"
// half. The oracle is the SERVER's own statement counter, not anything
// sluice reports about itself and not the `@@local_infile` probe the
// writer consults — a check that re-asked the writer's own question
// would confirm the fallback rather than catch it.
func assertStatementCounterAdvanced(t *testing.T, core byteaProvWriter, before, after int64) {
	t.Helper()
	if core.statementCounter == "" || after > before {
		return
	}
	t.Errorf("%s: the server's %s did not advance (%d → %d) — this core did NOT take its own "+
		"statement path. For load-data-infile that means writeLoadData fell back to writeBatched "+
		"(server local_infile=OFF), so this subtest is a duplicate of the batched core and the TSV "+
		"escaping layer went unexercised.", core.name, core.statementCounter, before, after)
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

// assertByteaProvCaseFloor is the anti-vacuity floor for the value-shape
// axis (audit B-2d). It derives the inventory from the case table's own
// values — a lowercase `\x`-hex collision, an UPPERCASE one, and a total
// count — so a simplification that drops the uppercase cell (the exact
// cell B-2d found missing) fails loudly instead of shrinking coverage.
func assertByteaProvCaseFloor(t *testing.T) {
	t.Helper()
	if got := len(byteaProvWriteCores()); got < 3 {
		t.Fatalf("write-core roster has %d cores; want >= 3 (batched / LOAD DATA / applier)", got)
	}
	if got, min := len(byteaProvIntegrationCases), 12; got < min {
		t.Fatalf("case table has %d shapes; want >= %d — the matrix has gone vacuous", got, min)
	}
	var lower, upper bool
	for _, c := range byteaProvIntegrationCases {
		if c.wantRefusal || !strings.HasPrefix(c.in, `\x`) || len(c.in) == 2 {
			continue
		}
		body := c.in[2:]
		if body == strings.ToLower(body) {
			lower = true
		} else {
			upper = true
		}
	}
	if !lower || !upper {
		t.Fatalf("collision-cell floor: lowercase=%v uppercase=%v — both hex spellings must be present (audit B-2d)", lower, upper)
	}
}

// TestByteaProvenance_MySQLWriteCores is the matrix: three write cores ×
// two column provenances × the value shapes, ground-truthed with
// OCTET_LENGTH().
func TestByteaProvenance_MySQLWriteCores(t *testing.T) {
	assertByteaProvCaseFloor(t)
	for _, core := range byteaProvWriteCores() {
		core := core
		t.Run(core.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
			defer cancel()

			// ---- Natively binary: the value IS PG's bytea rendering. ----
			t.Run("natively-binary", func(t *testing.T) {
				dsn, cleanup := newSharedDB(t, "bytea_prov_native_"+strings.ReplaceAll(core.name, "-", "_"))
				defer cleanup()
				if core.prepare != nil {
					core.prepare(t, ctx, dsn)
				}
				applyDDL(t, dsn, byteaProvIntegrationDDL)

				accepted := acceptedCases()
				before := readGlobalStatus(t, ctx, dsn, core.statementCounter)
				if err := core.write(t, ctx, dsn, byteaProvTable(false), byteaProvRows(accepted)); err != nil {
					t.Fatalf("write accepted renderings: %v", err)
				}
				assertStatementCounterAdvanced(t, core, before, readGlobalStatus(t, ctx, dsn, core.statementCounter))
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
				if core.prepare != nil {
					core.prepare(t, ctx, dsn)
				}
				applyDDL(t, dsn, byteaProvIntegrationDDL)

				before := readGlobalStatus(t, ctx, dsn, core.statementCounter)
				if err := core.write(t, ctx, dsn, byteaProvTable(true), byteaProvRows(byteaProvIntegrationCases)); err != nil {
					t.Fatalf("write overridden column: %v", err)
				}
				assertStatementCounterAdvanced(t, core, before, readGlobalStatus(t, ctx, dsn, core.statementCounter))
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
//
// The refusal fires BEFORE the statement reaches the server, so this test
// cannot use the statement counter on the refused write itself — nothing
// executes. It runs a one-row PATH PROBE first instead: an accepted value
// through the same core, with the server's counter checked around it. If
// the probe shows this core silently falling back to a sibling's path,
// every refusal below is being graded on the wrong code path.
func TestByteaProvenance_MySQLRefusesUnreadableRendering(t *testing.T) {
	for _, core := range byteaProvWriteCores() {
		core := core
		t.Run(core.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
			defer cancel()
			byteaProvAssertPathProbe(t, ctx, core)
			for _, c := range byteaProvIntegrationCases {
				if !c.wantRefusal {
					continue
				}
				c := c
				t.Run(c.name, func(t *testing.T) {
					dsn, cleanup := newSharedDB(t, "bytea_prov_refuse")
					defer cleanup()
					if core.prepare != nil {
						core.prepare(t, ctx, dsn)
					}
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
