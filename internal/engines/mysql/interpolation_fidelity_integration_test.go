//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Interpolation-encoder fidelity pins (audit N-15a / ADR-0153), against a
// real MySQL container.
//
// go-sql-driver's `interpolateParams=true` replaces the two-round-trip
// COM_STMT_PREPARE + COM_STMT_EXECUTE with a single COM_QUERY whose values
// the DRIVER encodes into SQL text — a second, independent value encoder on
// the write path. Before sluice defaults the PlanetScale/Vitess flavors onto
// it (ADR-0153; −33% bulk copy / −26% CDC burst drain at ~100 ms RTT on the
// 2026-07-08 real-PS bench), every value family must be proven byte-exact
// against the binary-protocol control ON THE TARGET — the Bug-74 "pin the
// class, not the representative" discipline: the interpolation encoder
// dispatches on the Go driver-value family (int64 / uint64 / float64 / bool
// / string / []byte / time.Time / nil), and a green pin on one family says
// nothing about the others.
//
// Matrix shape: each family × {binary protocol, interpolation} × {bulk
// batched-INSERT path, CDC coalesced multirow apply path} → byte-identical
// stored bytes (SELECT * RawBytes snapshots — the server's canonical text /
// binary forms — compared cell-by-cell, hex-dumped on mismatch). The two
// write paths build their SQL differently (buildBatchInsert vs
// buildMultiRowInsertSQL) but bind values through the same prepareValue
// codec; the differential proves the DRIVER-side encoding is equivalent on
// both.
//
// Families covered (the 9 benched + the bench's flagged residuals):
// microsecond temporals (DATETIME(6)/TIMESTAMP(6), plus DATE/TIME(6)/YEAR),
// DECIMAL extremes (20,6 and 65,30), DOUBLE extremes (±max, denormal, −0,
// ±Inf/NaN refusal), VARBINARY/BLOB with NUL + 0x5C + 0x27 + high bytes,
// 4-byte UTF-8 + quotes/backslashes/newlines/NUL, BIGINT (UNSIGNED)
// extremes, per-column NULLs, JSON, BIT(1/8/64), SET, ENUM, GEOMETRY
// (sluice's SRID-prefixed-WKB house form, prepareValue).
//
// Zero-date interaction (ADR-0127): no --zero-date mode ever writes a
// literal zero date — `error` refuses at decode, `null` writes NULL, `epoch`
// writes the 1970-01-01 00:00:01 UTC sentinel — so the write-side matrix is
// the NULL legs plus the zeroDateEpochValue row below. (The driver's own
// interpolation of a ZERO time.Time as '0000-00-00' is unreachable from
// sluice: no decode path produces a zero time.Time.)
//
// The NO_BACKSLASH_ESCAPES leg and the max_allowed_packet headroom legs
// (ADR-0153 guard ground truth) live in this file too, because they are
// interpolation-encoder fidelity questions of the same kind.

package mysql

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// interpFidelityDDL renders the family-matrix table under the given name.
// Every non-PK column is NULLable so the NULL leg runs per column.
func interpFidelityDDL(table string) string {
	return fmt.Sprintf(`
		CREATE TABLE %s (
			id      BIGINT NOT NULL,
			dt6     DATETIME(6) NULL,
			ts6     TIMESTAMP(6) NULL,
			dcol    DATE NULL,
			tm6     TIME(6) NULL,
			yr      YEAR NULL,
			dec206  DECIMAL(20,6) NULL,
			dec6530 DECIMAL(65,30) NULL,
			dbl     DOUBLE NULL,
			vb      VARBINARY(255) NULL,
			bl      BLOB NULL,
			vc      VARCHAR(255) NULL,
			bi      BIGINT NULL,
			ubi     BIGINT UNSIGNED NULL,
			flag    TINYINT(1) NULL,
			js      JSON NULL,
			b1      BIT(1) NULL,
			b8      BIT(8) NULL,
			b64     BIT(64) NULL,
			st      SET('a','b','c d','x-y') NULL,
			en      ENUM('alpha','beta','g mma') NULL,
			geo     GEOMETRY NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`, table)
}

// pointWKB returns the little-endian WKB encoding of POINT(x y) — the raw
// ir.Geometry value form (docs/value-types.md); prepareValue prepends the
// SRID for MySQL's internal layout.
func pointWKB(x, y float64) []byte {
	b := make([]byte, 21)
	b[0] = 1 // little-endian
	binary.LittleEndian.PutUint32(b[1:5], 1)
	binary.LittleEndian.PutUint64(b[5:13], math.Float64bits(x))
	binary.LittleEndian.PutUint64(b[13:21], math.Float64bits(y))
	return b
}

// allBytes returns the 256-byte 0x00..0xFF corpus — every possible octet
// through the escaper, including NUL, backslash, quotes, ^Z, CR/LF.
func allBytes() []byte {
	b := make([]byte, 256)
	for i := range b {
		b[i] = byte(i)
	}
	return b
}

// mkInterpFidelityRows is the bulk-path corpus: IR value shapes the MySQL
// reader / cross-engine translate layer actually produces. Each row leans on
// one family's extremes; every nullable column is NULL somewhere.
func mkInterpFidelityRows() []ir.Row {
	nullRow := func(id int64) ir.Row {
		return ir.Row{
			"id": id, "dt6": nil, "ts6": nil, "dcol": nil, "tm6": nil, "yr": nil,
			"dec206": nil, "dec6530": nil, "dbl": nil, "vb": nil, "bl": nil,
			"vc": nil, "bi": nil, "ubi": nil, "flag": nil, "js": nil,
			"b1": nil, "b8": nil, "b64": nil, "st": nil, "en": nil, "geo": nil,
		}
	}
	with := func(id int64, over ir.Row) ir.Row {
		row := nullRow(id)
		for k, v := range over {
			row[k] = v
		}
		return row
	}
	return []ir.Row{
		// Temporal maxima: DATETIME(6) and TIMESTAMP(6) at their upper
		// bounds, leap-day DATE, TIME(6) range max, YEAR max.
		with(1, ir.Row{
			"dt6":  time.Date(9999, 12, 31, 23, 59, 59, 999999000, time.UTC),
			"ts6":  time.Date(2038, 1, 19, 3, 14, 7, 999999000, time.UTC),
			"dcol": time.Date(2024, 2, 29, 0, 0, 0, 0, time.UTC),
			"tm6":  "838:59:59.000000",
			"yr":   int64(2155),
		}),
		// Temporal minima: DATETIME floor, TIMESTAMP epoch edge
		// (1970-01-01 00:00:01.000001), negative TIME, YEAR min.
		with(2, ir.Row{
			"dt6":  time.Date(1000, 1, 1, 0, 0, 0, 0, time.UTC),
			"ts6":  time.Date(1970, 1, 1, 0, 0, 1, 1000, time.UTC),
			"dcol": time.Date(1000, 1, 1, 0, 0, 0, 0, time.UTC),
			"tm6":  "-838:59:59.000000",
			"yr":   int64(1901),
		}),
		// The --zero-date=epoch substitute sentinel (ADR-0127): the ONLY
		// non-NULL value shape a zero date can reach the write path as.
		with(3, ir.Row{"dt6": zeroDateEpochValue, "ts6": zeroDateEpochValue}),
		// Trigger-CDC string temporals (the task-#72 shapes): bare ISO
		// string and offset-suffixed string (stripped by prepareValue).
		with(4, ir.Row{
			"dt6": "2026-02-02 02:02:02.020202",
			"ts6": "2026-02-02 02:02:02.020202+00",
			"tm6": "12:34:56.789012",
		}),
		// DECIMAL extremes, exact text (IR-canonical decimal form).
		with(5, ir.Row{
			"dec206":  "99999999999999.999999",
			"dec6530": strings.Repeat("9", 35) + "." + strings.Repeat("9", 30),
		}),
		with(6, ir.Row{"dec206": "-99999999999999.999999", "dec6530": "-0." + strings.Repeat("0", 29) + "1"}),
		with(7, ir.Row{"dec206": "0.000001", "dec6530": "0." + strings.Repeat("0", 30)}),
		// DOUBLE extremes: ±max, smallest denormal, smallest normal,
		// negative zero, mid-range exponents.
		with(8, ir.Row{"dbl": math.MaxFloat64}),
		with(9, ir.Row{"dbl": -math.MaxFloat64}),
		with(10, ir.Row{"dbl": math.SmallestNonzeroFloat64}),
		with(11, ir.Row{"dbl": 2.2250738585072014e-308}),
		with(12, ir.Row{"dbl": math.Copysign(0, -1)}),
		with(13, ir.Row{"dbl": 1e300}),
		with(14, ir.Row{"dbl": -1e-300}),
		// Binary: every octet 0x00..0xFF in BLOB; escape-relevant bytes
		// (NUL, ', ", \, ^Z, \n, \r, high bit) in VARBINARY; empty-vs-NULL.
		with(15, ir.Row{
			"vb": []byte{0x00, 0x27, 0x5c, 0x22, 0x1a, 0x0a, 0x0d, 0x09, 0x08, 0x80, 0xfe, 0xff},
			"bl": allBytes(),
		}),
		with(16, ir.Row{"vb": []byte{}, "bl": []byte{}}),
		// Strings: 4-byte UTF-8, quotes, backslashes, control chars, NUL,
		// SQL-comment lookalikes (the interpolator's lexer states).
		with(17, ir.Row{"vc": "🐘🚀 'q' \"dq\" \\back\\ \n\t\r 中文 кирилл `tick`"}),
		with(18, ir.Row{"vc": "a\x00nul; -- #x /*y*/ ?"}),
		// Integer extremes, signed and unsigned, plus bool→TINYINT(1).
		with(19, ir.Row{"bi": int64(math.MaxInt64), "ubi": uint64(math.MaxUint64), "flag": true}),
		with(20, ir.Row{"bi": int64(math.MinInt64), "ubi": uint64(0), "flag": false}),
		// JSON: escapes, emoji, nested array, negative fraction. (MySQL
		// normalizes binary JSON server-side — identically for both
		// protocols, which is exactly what the differential compares.)
		with(21, ir.Row{"js": []byte(`{"a\\b": "c\"d\ne", "emoji": "🐘", "n": -0.5, "arr": [1, null, "x"]}`)}),
		with(22, ir.Row{"js": []byte(`"bare-string"`)}),
		// BIT(N): IR-canonical '0'/'1' bit strings for N>1 (b64 all-ones
		// is the uint64 max edge through prepareValue's BitStringToUint64);
		// BIT(1) is ir.Boolean in the IR (types.go), so b1 carries bool.
		with(23, ir.Row{"b1": true, "b8": "10100101", "b64": strings.Repeat("1", 64)}),
		with(24, ir.Row{"b1": false, "b8": "00000000", "b64": "0"}),
		// SET ([]string IR form and pre-joined string form) and ENUM,
		// including labels with a space.
		with(25, ir.Row{"st": []string{"a", "c d", "x-y"}, "en": "g mma"}),
		with(26, ir.Row{"st": "b,x-y", "en": "alpha"}),
		// GEOMETRY: raw WKB through prepareValue's SRID-prefix house form.
		with(27, ir.Row{"geo": pointWKB(1.5, -2.25)}),
		with(28, ir.Row{"geo": pointWKB(math.MaxFloat64, math.SmallestNonzeroFloat64)}),
		// All-NULL row (every family's NULL leg in one statement) and a
		// dense row (every family non-NULL in one statement).
		nullRow(29),
		with(30, ir.Row{
			"dt6":  time.Date(2026, 7, 8, 1, 2, 3, 4000, time.UTC),
			"ts6":  time.Date(2026, 7, 8, 1, 2, 3, 4000, time.UTC),
			"dcol": time.Date(2026, 7, 8, 0, 0, 0, 0, time.UTC), "tm6": "01:02:03.000004",
			"yr": int64(2026), "dec206": "12345678.900000", "dec6530": "1.5",
			"dbl": 3.141592653589793, "vb": []byte("v\x00b"), "bl": []byte{0xde, 0xad},
			"vc": "dense", "bi": int64(-42), "ubi": uint64(42), "flag": true,
			"js": []byte(`{"k": 1}`), "b1": true, "b8": "1", "b64": "101",
			"st": []string{"a"}, "en": "beta", "geo": pointWKB(0, 0),
		}),
	}
}

// tableCell is one column of one row of a stored-bytes snapshot.
type tableCell struct {
	null bool
	b    []byte
}

// snapshotTableBytes reads SELECT * ORDER BY id via RawBytes — the server's
// canonical wire text for every stored value (raw bytes for binary columns)
// — the ground-truth form the A/B comparison runs over.
func snapshotTableBytes(t *testing.T, dsn, table string) (cols []string, rows [][]tableCell) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("snapshot open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	res, err := db.QueryContext(ctx, "SELECT * FROM "+quoteIdent(table)+" ORDER BY id")
	if err != nil {
		t.Fatalf("snapshot query %s: %v", table, err)
	}
	defer func() { _ = res.Close() }()
	cols, err = res.Columns()
	if err != nil {
		t.Fatalf("snapshot columns: %v", err)
	}
	for res.Next() {
		raw := make([]sql.RawBytes, len(cols))
		dest := make([]any, len(cols))
		for i := range raw {
			dest[i] = &raw[i]
		}
		if err := res.Scan(dest...); err != nil {
			t.Fatalf("snapshot scan: %v", err)
		}
		row := make([]tableCell, len(cols))
		for i, rb := range raw {
			if rb == nil {
				row[i] = tableCell{null: true}
			} else {
				row[i] = tableCell{b: append([]byte(nil), rb...)}
			}
		}
		rows = append(rows, row)
	}
	if err := res.Err(); err != nil {
		t.Fatalf("snapshot rows.Err: %v", err)
	}
	return cols, rows
}

// compareSnapshots asserts two stored-bytes snapshots are cell-for-cell
// identical, hex-dumping the diverging family on mismatch (the STOP signal
// the N-15a task contract requires: exact bytes, named column).
func compareSnapshots(t *testing.T, label string, cols []string, ctl, itp [][]tableCell) {
	t.Helper()
	if len(ctl) != len(itp) {
		t.Fatalf("%s: row count diverges: binary-protocol control %d rows, interpolation %d rows", label, len(ctl), len(itp))
	}
	for r := range ctl {
		for c := range cols {
			a, b := ctl[r][c], itp[r][c]
			if a.null != b.null || !bytes.Equal(a.b, b.b) {
				t.Errorf("%s: FAMILY DIVERGENCE at row %d column %q:\n  binary protocol: null=%v hex=%s\n  interpolation:   null=%v hex=%s",
					label, r, cols[c], a.null, hex.EncodeToString(a.b), b.null, hex.EncodeToString(b.b))
			}
		}
	}
}

// writeRowsBatched streams rows through the PlanetScale-flavor batched-
// INSERT bulk path (buildBatchInsert → flattenArgs → prepareValue) into
// table on dsn — the exact write path the ADR-0153 flavor default targets.
func writeRowsBatched(t *testing.T, ctx context.Context, dsn string, table *ir.Table, rows []ir.Row) error {
	t.Helper()
	rw, err := Engine{Flavor: FlavorPlanetScale}.OpenRowWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenRowWriter(%s): %v", table.Name, err)
	}
	defer closeIf(rw)
	in := make(chan ir.Row, len(rows))
	for _, r := range rows {
		in <- r
	}
	close(in)
	return rw.WriteRows(ctx, table, in)
}

// readTableIR reads the named table's IR descriptor from the live schema.
func readTableIR(t *testing.T, ctx context.Context, dsn, name string) *ir.Table {
	t.Helper()
	sr, err := Engine{}.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer closeIf(sr)
	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	table := findTable(schema, name)
	if table == nil {
		t.Fatalf("table %s not found; have %v", name, tableNames(schema))
	}
	return table
}

// TestInterpolation_BulkWrite_FamilyMatrix_ByteExact is the Phase-1 bulk-path
// pin: the full family corpus written through the batched-INSERT path under
// the binary protocol (explicit interpolateParams=false) and under client-
// side interpolation (explicit interpolateParams=true) must land with
// byte-identical stored values.
func TestInterpolation_BulkWrite_FamilyMatrix_ByteExact(t *testing.T) {
	dsn, cleanup := newSharedDB(t, "interp_bulk")
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	applyDDL(t, dsn, interpFidelityDDL("if_ctl"))
	applyDDL(t, dsn, interpFidelityDDL("if_itp"))
	tblCtl := readTableIR(t, ctx, dsn, "if_ctl")
	tblItp := readTableIR(t, ctx, dsn, "if_itp")

	rows := mkInterpFidelityRows()
	if err := writeRowsBatched(t, ctx, dsn+"&interpolateParams=false", tblCtl, rows); err != nil {
		t.Fatalf("binary-protocol control write: %v", err)
	}
	if err := writeRowsBatched(t, ctx, dsn+"&interpolateParams=true", tblItp, rows); err != nil {
		t.Fatalf("interpolation write: %v", err)
	}

	cols, ctl := snapshotTableBytes(t, dsn, "if_ctl")
	_, itp := snapshotTableBytes(t, dsn, "if_itp")
	if len(ctl) != len(rows) {
		t.Fatalf("control wrote %d rows; want %d", len(ctl), len(rows))
	}
	compareSnapshots(t, "bulk batched-INSERT", cols, ctl, itp)
}

// TestInterpolation_BulkWrite_NaNInfRefusal pins the not-a-number posture on
// the real write path: MySQL DOUBLE holds neither NaN nor ±Inf, and BOTH
// protocols must refuse such a row loudly, immediately, and IDENTICALLY —
// the SLUICE-E-VALUE-UNREPRESENTABLE guard fires in prepareValue before the
// driver sees the value. The guard exists because the two protocols' SERVER
// failure shapes differ dangerously: interpolation renders the bare literal
// `NaN`, drawing Error 1054 ER_BAD_FIELD_ERROR — the code the Bug-F8
// schema-drift self-healing deliberately classifies as RETRIABLE — so an
// unguarded NaN spun the cold-copy reparent-retry window (~30 min; observed
// live when this pin first ran against the unguarded code) instead of
// failing fast.
func TestInterpolation_BulkWrite_NaNInfRefusal(t *testing.T) {
	dsn, cleanup := newSharedDB(t, "interp_nan")
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	for _, tc := range []struct {
		name string
		val  float64
	}{
		{"nan", math.NaN()},
		{"posinf", math.Inf(1)},
		{"neginf", math.Inf(-1)},
	} {
		for _, proto := range []string{"false", "true"} {
			table := fmt.Sprintf("nan_%s_%s", tc.name, proto)
			applyDDL(t, dsn, fmt.Sprintf("CREATE TABLE %s (id BIGINT NOT NULL, dbl DOUBLE NULL, PRIMARY KEY (id)) ENGINE=InnoDB;", table))
			tbl := readTableIR(t, ctx, dsn, table)
			start := time.Now()
			err := writeRowsBatched(t, ctx, dsn+"&interpolateParams="+proto, tbl, []ir.Row{{"id": int64(1), "dbl": tc.val}})
			if err == nil {
				// A nil error would mean the value was silently coerced —
				// check what actually landed and fail naming it.
				_, got := snapshotTableBytes(t, dsn, table)
				t.Errorf("%s interpolateParams=%s: write of %v succeeded (stored: %v); want loud refusal", tc.name, proto, tc.val, got)
				continue
			}
			if !strings.Contains(err.Error(), "SLUICE-E-VALUE-UNREPRESENTABLE") {
				t.Errorf("%s interpolateParams=%s: refusal %q; want the SLUICE-E-VALUE-UNREPRESENTABLE guard (a server-side error here means the guard did not fire before the driver)", tc.name, proto, err)
			}
			// The refusal must be immediate — not a ridden-out retry window.
			if elapsed := time.Since(start); elapsed > 10*time.Second {
				t.Errorf("%s interpolateParams=%s: refusal took %s; the retry classifier is riding an unrepresentable value again", tc.name, proto, elapsed)
			}
		}
	}
}

// mkInterpCDCStream is the CDC-path corpus: the same families in the value
// shapes the CDC readers hand the applier (temporals as strings, JSON as
// string/[]byte, BIT as '0'/'1' strings, SET as joined string), driven as a
// coalescable insert run + non-PK updates + deletes so the ADR-0139 multirow
// upsert path, the update upsert path, and the coalesced DELETE...IN path all
// engage.
func mkInterpCDCStream() []ir.Change {
	var ev []ir.Change
	seq := 0
	tok := func() string { seq++; return fmt.Sprintf("if-%06d", seq) }
	ins := func(row ir.Row) {
		ev = append(ev, ir.Insert{Position: ir.Position{Engine: engineNameMySQL, Token: tok()}, Schema: "target_db", Table: "cdc_if", Row: row})
	}

	base := func(id int64) ir.Row {
		return ir.Row{
			"id": id, "dt6": nil, "ts6": nil, "dcol": nil, "tm6": nil, "yr": nil,
			"dec206": nil, "dec6530": nil, "dbl": nil, "vb": nil, "bl": nil,
			"vc": nil, "bi": nil, "ubi": nil, "flag": nil, "js": nil,
			"b1": nil, "b8": nil, "b64": nil, "st": nil, "en": nil, "geo": nil,
		}
	}
	with := func(id int64, over ir.Row) ir.Row {
		row := base(id)
		for k, v := range over {
			row[k] = v
		}
		return row
	}

	// A contiguous same-shape insert run (coalesces into multi-row INSERTs).
	ins(with(1, ir.Row{"dt6": "9999-12-31 23:59:59.999999", "ts6": "2038-01-19 03:14:07.999999", "dcol": "2024-02-29", "tm6": "838:59:59.000000", "yr": int64(2155)}))
	ins(with(2, ir.Row{"dt6": "1000-01-01 00:00:00.000000", "ts6": "1970-01-01 00:00:01.000001", "tm6": "-838:59:59.000000", "yr": int64(1901)}))
	ins(with(3, ir.Row{"dec206": "-99999999999999.999999", "dec6530": strings.Repeat("9", 35) + "." + strings.Repeat("9", 30)}))
	ins(with(4, ir.Row{"dbl": math.MaxFloat64, "bi": int64(math.MinInt64), "ubi": uint64(math.MaxUint64), "flag": false}))
	ins(with(5, ir.Row{"dbl": math.SmallestNonzeroFloat64}))
	ins(with(6, ir.Row{"dbl": math.Copysign(0, -1)}))
	ins(with(7, ir.Row{"vb": []byte{0x00, 0x27, 0x5c, 0x22, 0x1a, 0x0a, 0x80, 0xff}, "bl": allBytes()}))
	ins(with(8, ir.Row{"vc": "🐘 'q' \"dq\" \\b\\ \nnl 中文", "js": `{"a\\b": "c\"d", "e": [1, null]}`}))
	ins(with(9, ir.Row{"js": []byte(`{"emoji": "🚀", "n": -0.5}`)}))
	ins(with(10, ir.Row{"b1": true, "b8": "10100101", "b64": strings.Repeat("1", 64)}))
	ins(with(11, ir.Row{"st": "a,c d", "en": "g mma"}))
	ins(with(12, ir.Row{"st": []string{"b", "x-y"}, "en": "alpha"}))
	ins(with(13, ir.Row{"geo": pointWKB(1.5, -2.25)}))
	ins(with(14, ir.Row{"dt6": "2026-02-02 02:02:02.020202+00"})) // trigger-CDC offset shape
	ins(base(15))                                                 // all-NULL leg

	// Non-PK updates: after-image upserts over several families.
	upd := func(id int64, after ir.Row) {
		ev = append(ev, ir.Update{
			Position: ir.Position{Engine: engineNameMySQL, Token: tok()},
			Schema:   "target_db", Table: "cdc_if",
			Before: ir.Row{"id": id}, After: after,
		})
	}
	upd(1, with(1, ir.Row{"vc": "upd\\ated'", "vb": []byte{0x5c, 0x00}, "dbl": -1e-300}))
	upd(3, with(3, ir.Row{"dec206": "0.000001", "js": `{"u": true}`}))
	upd(10, with(10, ir.Row{"b64": "0", "b8": "11111111"}))
	upd(13, with(13, ir.Row{"geo": pointWKB(0, 0), "st": []string{"a"}}))

	// Deletes (the coalesced DELETE ... IN path binds PK args).
	for _, id := range []int64{5, 9} {
		ev = append(ev, ir.Delete{Position: ir.Position{Engine: engineNameMySQL, Token: tok()}, Schema: "target_db", Table: "cdc_if", Before: ir.Row{"id": id}})
	}
	// A fresh insert run after the boundaries.
	ins(with(16, ir.Row{"vc": "post-delete", "flag": true, "bi": int64(math.MaxInt64)}))
	return ev
}

// TestInterpolation_CDCApply_FamilyMatrix_ByteExact is the Phase-1 CDC-path
// pin: the same change stream applied through the coalescing multirow apply
// path under both protocols must produce byte-identical target state, and
// the multi-row path must actually engage on both passes.
func TestInterpolation_CDCApply_FamilyMatrix_ByteExact(t *testing.T) {
	stream := mkInterpCDCStream()

	run := func(dsnParam string) (cols []string, snap [][]tableCell) {
		dsn, cleanup := startMySQLForApplier(t)
		defer cleanup()
		applyMySQLApplier(t, dsn, interpFidelityDDL("cdc_if"))

		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()
		a, err := Engine{Flavor: FlavorPlanetScale}.OpenChangeApplier(ctx, dsn+dsnParam)
		if err != nil {
			t.Fatalf("OpenChangeApplier: %v", err)
		}
		defer closeApplier(a)

		multi := 0
		multiRowFlushHookForTest = func(rows int) {
			if rows > 1 {
				multi++
			}
		}
		defer func() { multiRowFlushHookForTest = nil }()

		pumpBatchedChangesPipelined(t, ctx, a, "interp-fid", stream, 200)
		if multi == 0 {
			t.Errorf("interpolateParams%s: no multi-row coalesced flush engaged — the pin did not exercise buildMultiRowInsertSQL", dsnParam)
		}
		cols, snap = snapshotTableBytes(t, dsn, "cdc_if")
		return cols, snap
	}

	cols, ctl := run("&interpolateParams=false")
	_, itp := run("&interpolateParams=true")
	if len(ctl) == 0 {
		t.Fatal("control pass applied no rows")
	}
	compareSnapshots(t, "CDC coalesced apply", cols, ctl, itp)
}

// TestInterpolation_NBE_SessionMode_ByteExact ground-truths the ADR-0153
// NO_BACKSLASH_ESCAPES guard question on a real server: go-sql-driver v1.10
// switches its interpolation escaper per the SERVER-reported session status
// flag (escapeStringQuotes / escapeBytesQuotes — quote-doubling, backslash
// literal — when NBE is on), so interpolation under NBE is claimed CORRECT,
// not refused and not silently mis-escaped. This pin holds that claim to
// stored bytes: a backslash/quote/NUL-heavy corpus written under a session
// sql_mode carrying NO_BACKSLASH_ESCAPES must be byte-identical across
// protocols. If this test ever fails, ADR-0153's posture flips to a loud
// connect-time refusal — do not weaken the assertion.
func TestInterpolation_NBE_SessionMode_ByteExact(t *testing.T) {
	dsn, cleanup := newSharedDB(t, "interp_nbe")
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	const ddlT = `CREATE TABLE %s (
		id BIGINT NOT NULL, vc VARCHAR(255) NULL, vb VARBINARY(255) NULL, js JSON NULL,
		PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`
	applyDDL(t, dsn, fmt.Sprintf(ddlT, "nbe_ctl"))
	applyDDL(t, dsn, fmt.Sprintf(ddlT, "nbe_itp"))
	tblCtl := readTableIR(t, ctx, dsn, "nbe_ctl")
	tblItp := readTableIR(t, ctx, dsn, "nbe_itp")

	rows := []ir.Row{
		{"id": int64(1), "vc": `back\slash \\double \n literal`, "vb": []byte{0x5c, 0x5c, 0x00, 0x27}, "js": nil},
		{"id": int64(2), "vc": "quotes ' '' \" and \x00 nul", "vb": []byte(`\x27\`), "js": `{"k": "a\\b'c"}`},
		{"id": int64(3), "vc": `trailing backslash \`, "vb": nil, "js": nil},
		{"id": int64(4), "vc": nil, "vb": []byte{}, "js": `"\\"`},
	}

	nbeMode := defaultStrictSQLMode + ",NO_BACKSLASH_ESCAPES"
	write := func(param string, tbl *ir.Table) {
		t.Helper()
		eng := Engine{Flavor: FlavorPlanetScale}.WithSQLMode(nbeMode)
		rw, err := eng.OpenRowWriter(ctx, dsn+param)
		if err != nil {
			t.Fatalf("OpenRowWriter(%s): %v", tbl.Name, err)
		}
		defer closeIf(rw)
		in := make(chan ir.Row, len(rows))
		for _, r := range rows {
			in <- r
		}
		close(in)
		if err := rw.WriteRows(ctx, tbl, in); err != nil {
			t.Fatalf("WriteRows(%s, %s): %v", tbl.Name, param, err)
		}
	}
	write("&interpolateParams=false", tblCtl)
	write("&interpolateParams=true", tblItp)

	cols, ctl := snapshotTableBytes(t, dsn, "nbe_ctl")
	_, itp := snapshotTableBytes(t, dsn, "nbe_itp")
	compareSnapshots(t, "NO_BACKSLASH_ESCAPES session", cols, ctl, itp)

	// Sanity: the raw backslash really was stored raw (NBE semantics held
	// on the wire — guards the pin against a silently-non-NBE session).
	if len(ctl) > 0 && !bytes.Equal(ctl[0][1].b, []byte(`back\slash \\double \n literal`)) {
		t.Errorf("NBE control row stored %q; the session did not run under NO_BACKSLASH_ESCAPES semantics", ctl[0][1].b)
	}
}

// TestInterpolation_PacketHeadroom ground-truths the ADR-0153 packet
// question: interpolated SQL text inflates binary-heavy values (worst case
// ~2× via escaping), so the pin holds two claims on a real server:
//
//  1. margin: the ADR-0150 ~1 MiB statement byte target leaves so much
//     room under every max_allowed_packet (MySQL default 64 MiB,
//     PlanetScale 16 MiB+) that a maximally escape-inflating multi-MiB
//     value still lands byte-exact under interpolation;
//  2. boundary: when the interpolated text WOULD exceed the driver's
//     maxAllowedPacket, go-sql-driver returns driver.ErrSkip and
//     database/sql transparently falls back to the prepared (binary)
//     path — the row still lands byte-exact; there is no silent loss and
//     no spurious "packet too large" from the inflation.
func TestInterpolation_PacketHeadroom(t *testing.T) {
	dsn, cleanup := newSharedDB(t, "interp_pkt")
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	const ddlT = "CREATE TABLE %s (id BIGINT NOT NULL, bl LONGBLOB NULL, PRIMARY KEY (id)) ENGINE=InnoDB;"

	// Maximally inflating payload: every byte needs escaping (' and \),
	// so interpolated text is ~2× the raw size.
	inflating := func(n int) []byte {
		b := make([]byte, n)
		for i := range b {
			if i%2 == 0 {
				b[i] = 0x27
			} else {
				b[i] = 0x5c
			}
		}
		return b
	}

	// Leg 1 — margin: 2 MiB raw (≈4 MiB interpolated) through the normal
	// interp DSN vs the binary control.
	applyDDL(t, dsn, fmt.Sprintf(ddlT, "pkt_ctl"))
	applyDDL(t, dsn, fmt.Sprintf(ddlT, "pkt_itp"))
	big := inflating(2 << 20)
	rowsBig := []ir.Row{{"id": int64(1), "bl": big}}
	if err := writeRowsBatched(t, ctx, dsn+"&interpolateParams=false", readTableIR(t, ctx, dsn, "pkt_ctl"), rowsBig); err != nil {
		t.Fatalf("margin control write: %v", err)
	}
	if err := writeRowsBatched(t, ctx, dsn+"&interpolateParams=true", readTableIR(t, ctx, dsn, "pkt_itp"), rowsBig); err != nil {
		t.Fatalf("margin interpolation write: %v", err)
	}
	cols, ctl := snapshotTableBytes(t, dsn, "pkt_ctl")
	_, itp := snapshotTableBytes(t, dsn, "pkt_itp")
	compareSnapshots(t, "packet margin (2 MiB inflating blob)", cols, ctl, itp)

	// Leg 2 — boundary: cap the DRIVER's maxAllowedPacket at 1 MiB; a
	// 700 KiB payload interpolates to ~1.4 MiB (> cap) but is well under
	// the cap raw. The driver must ErrSkip → prepared fallback → land.
	applyDDL(t, dsn, fmt.Sprintf(ddlT, "pkt_fb"))
	edge := inflating(700 << 10)
	err := writeRowsBatched(t, ctx, dsn+"&interpolateParams=true&maxAllowedPacket=1048576",
		readTableIR(t, ctx, dsn, "pkt_fb"), []ir.Row{{"id": int64(1), "bl": edge}})
	if err != nil {
		t.Fatalf("boundary write should fall back to the prepared path, got: %v", err)
	}
	_, fb := snapshotTableBytes(t, dsn, "pkt_fb")
	if len(fb) != 1 || fb[0][1].null || !bytes.Equal(fb[0][1].b, edge) {
		t.Errorf("boundary fallback row corrupt: rows=%d", len(fb))
	}
}

// TestInterpolation_ReadRowsBatch_DecodeParity pins the READ side of the
// flavor default: with interpolation on, arg-bearing SELECTs (the PK-paged
// ReadRowsBatch the chunked cold-copy uses) travel the TEXT protocol instead
// of the prepared/binary one, so the driver hands sluice text wire forms.
// The decode layer must produce identical IR values (same Go type, same
// value) either way. ReadRows full scans are included for completeness
// (arg-less — text protocol on both DSNs by construction).
func TestInterpolation_ReadRowsBatch_DecodeParity(t *testing.T) {
	dsn, cleanup := newSharedDB(t, "interp_read")
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	applyDDL(t, dsn, interpFidelityDDL("if_read"))
	tbl := readTableIR(t, ctx, dsn, "if_read")
	rows := mkInterpFidelityRows()
	if err := writeRowsBatched(t, ctx, dsn+"&interpolateParams=false", tbl, rows); err != nil {
		t.Fatalf("seed write: %v", err)
	}

	read := func(param string) []ir.Row {
		t.Helper()
		rr, err := Engine{}.OpenRowReader(ctx, dsn+param)
		if err != nil {
			t.Fatalf("OpenRowReader(%s): %v", param, err)
		}
		defer closeIf(rr)
		reader := rr.(*RowReader)
		var out []ir.Row
		// Two PK-paged reads: page 2's `after` cursor binds a `?` arg —
		// the statement shape whose protocol the DSN param flips.
		var after []any
		for {
			ch, err := reader.ReadRowsBatch(ctx, tbl, after, 12)
			if err != nil {
				t.Fatalf("ReadRowsBatch(%s): %v", param, err)
			}
			n := 0
			for row := range ch {
				out = append(out, row)
				n++
			}
			if err := reader.Err(); err != nil {
				t.Fatalf("ReadRowsBatch(%s) stream: %v", param, err)
			}
			if n == 0 {
				break
			}
			after = []any{out[len(out)-1]["id"]}
		}
		return out
	}

	ctl := read("&interpolateParams=false")
	itp := read("&interpolateParams=true")
	if len(ctl) != len(rows) || len(itp) != len(rows) {
		t.Fatalf("read %d (binary) / %d (interp) rows; want %d", len(ctl), len(itp), len(rows))
	}
	for i := range ctl {
		for col, cv := range ctl[i] {
			iv := itp[i][col]
			if readShapesEquivalent(cv, iv) {
				continue
			}
			if fmt.Sprintf("%T", cv) != fmt.Sprintf("%T", iv) || !rowValueEqual(iv, cv) {
				t.Errorf("read parity: row %d col %q: binary=%#v (%T), interpolation=%#v (%T)", i, col, cv, cv, iv, iv)
			}
		}
	}
}

// readShapesEquivalent tolerates the ONE contract-permitted Go-shape wobble
// between the two result protocols: integer values. docs/value-types.md
// lines the contract out as int64 for values ≤ MaxInt64 and uint64 above it,
// and decodeInteger deliberately keeps a driver-supplied []byte for very
// large values ("callers can parse on demand") — so the binary protocol
// yields int64 / []byte where the text protocol yields uint64 for the same
// stored value. That wobble PRE-DATES the ADR-0153 flip (full scans have
// always been text, chunked reads binary); the flip moves chunked reads onto
// the shapes the majority full-scan path already produces. The VALUES must
// still agree exactly — only the {int64, uint64, decimal-ASCII []byte}
// carrier may differ. Everything else stays type-strict.
func readShapesEquivalent(a, b any) bool {
	canon := func(v any) (string, bool) {
		switch n := v.(type) {
		case int64:
			return fmt.Sprintf("%d", n), true
		case uint64:
			return fmt.Sprintf("%d", n), true
		case []byte:
			s := string(n)
			if s == "" {
				return "", false
			}
			for i, c := range s {
				if (c < '0' || c > '9') && !(i == 0 && c == '-') {
					return "", false
				}
			}
			return s, true
		}
		return "", false
	}
	ca, oka := canon(a)
	cb, okb := canon(b)
	return oka && okb && ca == cb
}
