//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Adversarial value-fidelity corpus for the mydumper dump→restore
// boundary — the flat-file sibling of the pipeline's
// migrate_adversarial_corpus_* suites, pushing the same worst-case
// value families through a REAL mydumper dump (both literal encodings:
// the default backslash-escape shape and --hex-blob) and a migrate into
// a real MySQL target.
//
// This deliberately does NOT reuse the live-path-equivalence oracle of
// mydumper_integration_test.go: that oracle compares two sluice-migrated
// targets, so a value both paths alter identically compares clean (the
// audit-2026-08-05 C-14 trap — it happened for geometry SRIDs). Here the
// independent expected value for every cell is the HAND-WRITTEN want,
// probed by direct SQL against the dump-migrated target — never through
// sluice's own readers. The dump itself is written by real mydumper
// (v1.0.3), not by sluice, so writer and reader are independent
// implementations on both sides of the file.
//
// What the probes cover is the dump reader + the MySQL row writer; the
// writer half is already corpus-pinned by the pipeline's PG→MySQL
// adversarial suite with these same values, so a failure here localizes
// to the dump lexer/decoder.
//
// Sibling enumeration for the boundary (fixed-or-exempt):
//   - escape + hex-blob literal encodings: BOTH legs below (different
//     lexer paths for every string/binary cell).
//   - gzip/zstd compression legs: EXEMPT here — byte-identical SQL text
//     after decompression, pinned by mydumper_integration_test.go.
//   - geometry (SRID + WKB through a real dump): EXEMPT here — pinned by
//     the geo table in mydumper_integration_test.go incl. the C-14
//     declared-SRID shape.
//   - FLOAT beyond ~6 significant digits: EXEMPT as a value cell — the
//     loss happens AT DUMP TIME inside mysqld's float→text formatter
//     (ADR-0161 §4, WARNed by warnIfSingleFloatColumns); the f4 cell
//     stays within the faithful range. DOUBLE cells deliberately demand
//     full 17-digit fidelity.
//   - invalid UTF-8 in TEXT: EXEMPT — a utf8mb4 source column cannot
//     hold it (server-refused at seed time), so the reachable carrier is
//     the binary family, covered incl. NUL/0xFF runs; non-UTF-8 dump
//     charsets are refused at the SET NAMES / schema gate (ADR-0161 §5,
//     Bug 188 leg).
//   - zero dates: the REFUSAL leg below — the mydumper engine has no
//     --zero-date plumbing by design (ADR-0161 §7), so the sentinel must
//     surface as a loud, value-naming error with zero rows landed. The
//     unit pin (row_reader_test.go) uses a hand-written fixture; this
//     leg proves real mydumper dumps zero dates in the shape the
//     refusal fires on.

package mydumper

import (
	"context"
	"crypto/md5"
	"database/sql"
	"fmt"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// advDumpCmp selects how a probed value compares to want.
type advDumpCmp int

const (
	advDumpCmpText advDumpCmp = iota
	// advDumpCmpFloat64 parses got and want and compares IEEE-754 bits
	// (MySQL 8 CONCAT() renders shortest-round-trip forms; exponent
	// spelling differs from Go's, bit identity does not).
	advDumpCmpFloat64
	advDumpCmpFloat32
)

// advDumpCell is one corpus cell: a source column with a worst-case
// value, and the hand-written target-side ground truth.
type advDumpCell struct {
	family string
	col    string
	ddl    string
	lit    string // source SQL literal ("NULL" for seedVal cells)

	probe string // target-side SQL expr; %s = column name
	want  string
	cmp   advDumpCmp

	seedVal any // applied via driver param after the seed INSERT
}

// Anti-vacuity floor: a shrunken corpus fails loudly before probing.
const (
	advDumpMinFamilies = 10
	advDumpMinCells    = 45
)

// advDumpEscText is the escape-class worst case for TEXT: every byte
// mydumper backslash-escapes (quote, double quote, backslash, NUL via
// the binary family, LF, CR, TAB, ctrl-Z) around multi-byte UTF-8.
const advDumpEscText = "it's \"q\" \\back \n\r\t\x1a end\U0001F34A"

// advDumpCorpus builds the corpus. A function because the big
// text/blob cells derive seedVal + MD5 wants at build time.
func advDumpCorpus() []advDumpCell {
	bigText := strings.Repeat("héllo wörld \U0001F980 snøw ", 10000) // ~250KB multi-byte UTF-8
	bigBlob := advDumpBlobPattern(2 << 20)                           // 2 MiB incl. NUL + 0xFF runs
	ffBlob := make([]byte, 256)
	for i := range ffBlob {
		ffBlob[i] = 0xFF
	}
	deepDoc := strings.Repeat(`{"a":`, 40) + "1" + strings.Repeat("}", 40)
	deepPath := "$" + strings.Repeat(".a", 40)

	return []advDumpCell{
		// ---- integer ----
		{family: "integer", col: "i64_min", ddl: "BIGINT NOT NULL", lit: "-9223372036854775808", probe: "CAST(%s AS CHAR)", want: "-9223372036854775808"},
		{family: "integer", col: "i64_max", ddl: "BIGINT NOT NULL", lit: "9223372036854775807", probe: "CAST(%s AS CHAR)", want: "9223372036854775807"},
		// 2^53+1: a float64 anywhere in the pipe lands …992.
		{family: "integer", col: "i64_2p53", ddl: "BIGINT NOT NULL", lit: "9007199254740993", probe: "CAST(%s AS CHAR)", want: "9007199254740993"},
		{family: "integer", col: "u64_max", ddl: "BIGINT UNSIGNED NOT NULL", lit: "18446744073709551615", probe: "CAST(%s AS CHAR)", want: "18446744073709551615"},
		// 2^63: the parseExactInteger int64→uint64 widen boundary.
		{family: "integer", col: "u64_mid", ddl: "BIGINT UNSIGNED NOT NULL", lit: "9223372036854775808", probe: "CAST(%s AS CHAR)", want: "9223372036854775808"},
		{family: "integer", col: "u64_atcap", ddl: "BIGINT UNSIGNED NOT NULL", lit: "9223372036854775807", probe: "CAST(%s AS CHAR)", want: "9223372036854775807"},
		// ZEROFILL: mydumper dumps the DISPLAY form ("00000042"); the
		// VALUE 42 is the ground truth, never the padded rendering.
		{family: "integer", col: "u_zerofill", ddl: "INT(8) UNSIGNED ZEROFILL NOT NULL", lit: "42", probe: "CAST(%s AS CHAR)", want: "42"},
		{family: "integer", col: "y_min", ddl: "YEAR NOT NULL", lit: "1901", probe: "CAST(%s AS CHAR)", want: "1901"},
		{family: "integer", col: "y_max", ddl: "YEAR NOT NULL", lit: "2155", probe: "CAST(%s AS CHAR)", want: "2155"},

		// ---- decimal ----
		{
			family: "decimal", col: "dec_max", ddl: "DECIMAL(65,30) NOT NULL",
			lit:   "'99999999999999999999999999999999999.999999999999999999999999999999'",
			probe: "CAST(%s AS CHAR)", want: "99999999999999999999999999999999999.999999999999999999999999999999",
		},
		{
			family: "decimal", col: "dec_negmax", ddl: "DECIMAL(65,30) NOT NULL",
			lit:   "'-12345678901234567890123456789012345.123456789012345678901234567890'",
			probe: "CAST(%s AS CHAR)", want: "-12345678901234567890123456789012345.123456789012345678901234567890",
		},
		{family: "decimal", col: "dec_ten", ddl: "DECIMAL(2,0) NOT NULL", lit: "10", probe: "CAST(%s AS CHAR)", want: "10"},
		{family: "decimal", col: "dec_nine", ddl: "DECIMAL(2,0) NOT NULL", lit: "9", probe: "CAST(%s AS CHAR)", want: "9"},
		{family: "decimal", col: "dec_frac", ddl: "DECIMAL(10,5) NOT NULL", lit: "'-0.00001'", probe: "CAST(%s AS CHAR)", want: "-0.00001"},

		// ---- float (DOUBLE at full 17-digit fidelity; -0.0e0 because a
		// bare -0.0 literal is typed DECIMAL, which has no signed zero) ----
		{family: "float", col: "f8_negzero", ddl: "DOUBLE NOT NULL", lit: "-0.0e0", probe: "CONCAT(%s)", want: "-0", cmp: advDumpCmpFloat64},
		{family: "float", col: "f8_17sig", ddl: "DOUBLE NOT NULL", lit: "0.1234567890123456789", probe: "CONCAT(%s)", want: "0.1234567890123456789", cmp: advDumpCmpFloat64},
		{family: "float", col: "f8_max", ddl: "DOUBLE NOT NULL", lit: "1.7976931348623157e308", probe: "CONCAT(%s)", want: "1.7976931348623157e308", cmp: advDumpCmpFloat64},
		{family: "float", col: "f8_minnormal", ddl: "DOUBLE NOT NULL", lit: "2.2250738585072014e-308", probe: "CONCAT(%s)", want: "2.2250738585072014e-308", cmp: advDumpCmpFloat64},
		{family: "float", col: "f8_subnormal", ddl: "DOUBLE NOT NULL", lit: "5e-324", probe: "CONCAT(%s)", want: "5e-324", cmp: advDumpCmpFloat64},
		{family: "float", col: "f4_pi", ddl: "FLOAT NOT NULL", lit: "3.14159", probe: "CONCAT(%s)", want: "3.14159", cmp: advDumpCmpFloat32},

		// ---- text ----
		{family: "text", col: "t_emoji", ddl: "VARCHAR(191) NOT NULL", lit: "'crab \U0001F980 café ✅'", probe: "%s", want: "crab \U0001F980 café ✅"},
		// Decomposed e + U+0308 (6 bytes) must not unicode-normalize.
		{family: "text", col: "t_combining", ddl: "VARCHAR(64) NOT NULL", lit: "'noël'", probe: "CONCAT(%s,'#',LENGTH(%s))", want: "noël#6"},
		{family: "text", col: "t_trailing", ddl: "VARCHAR(32) NOT NULL", lit: "'pad   '", probe: "CONCAT('[',%s,']#',CHAR_LENGTH(%s))", want: "[pad   ]#6"},
		{family: "text", col: "t_zwj", ddl: "VARCHAR(64) NOT NULL", lit: "'\U0001F468‍\U0001F469‍\U0001F467‍\U0001F466'", probe: "CONCAT(LENGTH(%s),'#',%s)", want: "25#\U0001F468‍\U0001F469‍\U0001F467‍\U0001F466"},
		// U+0000 IS a legal MySQL text character; mydumper escapes it \0.
		{family: "text", col: "t_nul", ddl: "VARCHAR(20) NOT NULL", lit: "CONCAT('a',CHAR(0),'b')", probe: "CONCAT(LOWER(HEX(%s)),'#',LENGTH(%s))", want: "610062#3"},
		{
			family: "text", col: "t_esc", ddl: "VARCHAR(64) NULL", lit: "NULL", seedVal: advDumpEscText,
			probe: "CONCAT(LOWER(HEX(%s)),'#',LENGTH(%s))",
			want:  fmt.Sprintf("%x#%d", advDumpEscText, len(advDumpEscText)),
		},
		{
			family: "text", col: "t_big", ddl: "MEDIUMTEXT NULL", lit: "NULL", seedVal: bigText,
			probe: "CONCAT(MD5(%s),'#',LENGTH(%s))",
			want:  advDumpMD5([]byte(bigText)) + "#" + strconv.Itoa(len(bigText)),
		},

		// ---- binary (BINARY(8) zero-pads at the SOURCE; the padded
		// form is the faithful value; interior NULs are data) ----
		{family: "binary", col: "bin_pad", ddl: "BINARY(8) NOT NULL", lit: "X'DEAD'", probe: "CONCAT(LOWER(HEX(%s)),'#',LENGTH(%s))", want: "dead000000000000#8"},
		{family: "binary", col: "bin_zeros", ddl: "BINARY(8) NOT NULL", lit: "X'0000000000000000'", probe: "CONCAT(LOWER(HEX(%s)),'#',LENGTH(%s))", want: "0000000000000000#8"},
		{family: "binary", col: "bin_midnul", ddl: "BINARY(8) NOT NULL", lit: "X'DE00AD'", probe: "CONCAT(LOWER(HEX(%s)),'#',LENGTH(%s))", want: "de00ad0000000000#8"},
		// VARBINARY trailing 0x00 is DATA (no pad reconstruction).
		{family: "binary", col: "vb_nul", ddl: "VARBINARY(16) NOT NULL", lit: "X'00FF7F00'", probe: "CONCAT(LOWER(HEX(%s)),'#',LENGTH(%s))", want: "00ff7f00#4"},
		// Every byte mydumper's default leg backslash-escapes, as bytes.
		{family: "binary", col: "vb_esc", ddl: "VARBINARY(32) NOT NULL", lit: "X'0027225C0A0D091A'", probe: "CONCAT(LOWER(HEX(%s)),'#',LENGTH(%s))", want: "0027225c0a0d091a#8"},
		{
			family: "binary", col: "blob_ff", ddl: "BLOB NULL", lit: "NULL", seedVal: ffBlob,
			probe: "CONCAT(MD5(%s),'#',LENGTH(%s))", want: advDumpMD5(ffBlob) + "#256",
		},
		{
			family: "binary", col: "blob_big", ddl: "MEDIUMBLOB NULL", lit: "NULL", seedVal: bigBlob,
			probe: "CONCAT(MD5(%s),'#',LENGTH(%s))", want: advDumpMD5(bigBlob) + "#" + strconv.Itoa(len(bigBlob)),
		},

		// ---- temporal (seeded + probed under time_zone='+00:00') ----
		{family: "temporal", col: "dt_min", ddl: "DATETIME NOT NULL", lit: "'1000-01-01 00:00:00'", probe: "CAST(%s AS CHAR)", want: "1000-01-01 00:00:00"},
		{family: "temporal", col: "dt_max", ddl: "DATETIME(6) NOT NULL", lit: "'9999-12-31 23:59:59.999999'", probe: "CAST(%s AS CHAR)", want: "9999-12-31 23:59:59.999999"},
		{family: "temporal", col: "dt_frac", ddl: "DATETIME(6) NOT NULL", lit: "'2026-03-08 02:30:00.123456'", probe: "CAST(%s AS CHAR)", want: "2026-03-08 02:30:00.123456"},
		{family: "temporal", col: "ts_floor", ddl: "TIMESTAMP NOT NULL", lit: "'1970-01-01 00:00:01'", probe: "CAST(%s AS CHAR)", want: "1970-01-01 00:00:01"},
		{family: "temporal", col: "ts_max32", ddl: "TIMESTAMP NOT NULL", lit: "'2038-01-19 03:14:07'", probe: "CAST(%s AS CHAR)", want: "2038-01-19 03:14:07"},
		{family: "temporal", col: "d_min", ddl: "DATE NOT NULL", lit: "'1000-01-01'", probe: "CAST(%s AS CHAR)", want: "1000-01-01"},
		{family: "temporal", col: "d_max", ddl: "DATE NOT NULL", lit: "'9999-12-31'", probe: "CAST(%s AS CHAR)", want: "9999-12-31"},
		{family: "temporal", col: "tm_frac", ddl: "TIME(6) NOT NULL", lit: "'23:59:59.999999'", probe: "CAST(%s AS CHAR)", want: "23:59:59.999999"},
		// MySQL TIME is a ±838h DURATION; MySQL→MySQL must carry it.
		{family: "temporal", col: "tm_durmax", ddl: "TIME NOT NULL", lit: "'838:59:59'", probe: "CAST(%s AS CHAR)", want: "838:59:59"},
		{family: "temporal", col: "tm_durneg", ddl: "TIME(3) NOT NULL", lit: "'-838:59:59.000'", probe: "CAST(%s AS CHAR)", want: "-838:59:59.000"},

		// ---- boolean ----
		{family: "boolean", col: "b_true", ddl: "TINYINT(1) NOT NULL", lit: "1", probe: "CAST(%s AS CHAR)", want: "1"},
		{family: "boolean", col: "b_false", ddl: "TINYINT(1) NOT NULL", lit: "0", probe: "CAST(%s AS CHAR)", want: "0"},

		// ---- bit ----
		{family: "bit", col: "bit1", ddl: "BIT(1) NOT NULL", lit: "b'1'", probe: "LOWER(HEX(%s))", want: "1"},
		{family: "bit", col: "bit5", ddl: "BIT(5) NOT NULL", lit: "b'10101'", probe: "LOWER(HEX(%s))", want: "15"},
		// All 64 bits set: the checkBitWidth ceiling, and eight 0xFF
		// bytes through the escape leg's raw-byte string.
		{family: "bit", col: "bit64", ddl: "BIT(64) NOT NULL", lit: "b'" + strings.Repeat("1", 64) + "'", probe: "LOWER(HEX(%s))", want: "ffffffffffffffff"},

		// ---- enum / set ----
		{family: "enumset", col: "e_quote", ddl: "ENUM('alpha','it''s') NOT NULL", lit: "'it''s'", probe: "CONCAT(%s)", want: "it's"},
		{family: "enumset", col: "s_all", ddl: "SET('a','b','c') NOT NULL", lit: "'c,a,b'", probe: "CONCAT(%s)", want: "a,b,c"},
		{family: "enumset", col: "s_empty", ddl: "SET('a','b','c') NOT NULL", lit: "''", probe: "CONCAT(%s)", want: ""},

		// ---- json ----
		{family: "json", col: "j_bigint", ddl: "JSON NOT NULL", lit: `'{"n":9007199254740993}'`, probe: "JSON_UNQUOTE(JSON_EXTRACT(%s,'$.n'))", want: "9007199254740993"},
		{family: "json", col: "j_frac", ddl: "JSON NOT NULL", lit: `'{"x":0.30000000000000004}'`, probe: "JSON_UNQUOTE(JSON_EXTRACT(%s,'$.x'))", want: "0.30000000000000004"},
		{
			family: "json", col: "j_unicode", ddl: "JSON NOT NULL",
			lit:   "'{\"ké\":\"vé\",\"emoji\":\"\U0001F980\"}'",
			probe: "CONCAT(JSON_UNQUOTE(JSON_EXTRACT(%s,'$.\"ké\"')),'#',JSON_UNQUOTE(JSON_EXTRACT(%s,'$.emoji')))",
			want:  "vé#\U0001F980",
		},
		// Quotes + backslashes + a newline INSIDE a JSON string INSIDE
		// the dump's SQL string literal — double-escaping, the lexer's
		// worst case. Built with JSON_OBJECT so the source truth is
		// unambiguous; probed as hex.
		{
			family: "json", col: "j_esc", ddl: "JSON NOT NULL",
			// CHAR(10 USING utf8mb4), not bare CHAR(10): CHAR() returns a
			// BINARY string, which turns the whole CONCAT binary and makes
			// JSON_OBJECT store an opaque base64:type15 value instead of a
			// JSON string (ground-truthed on mysql:8.0 — the dump path
			// carried that opaque form faithfully too).
			lit:   `JSON_OBJECT('q', CONCAT('a"b\\c', CHAR(10 USING utf8mb4), 'd'))`,
			probe: "LOWER(HEX(JSON_UNQUOTE(JSON_EXTRACT(%s,'$.q'))))",
			want:  fmt.Sprintf("%x", "a\"b\\c\nd"),
		},
		{family: "json", col: "j_deep", ddl: "JSON NOT NULL", lit: "'" + deepDoc + "'", probe: "JSON_UNQUOTE(JSON_EXTRACT(%s,'" + deepPath + "'))", want: "1"},
	}
}

// TestMydumperAdversarialCorpus_RealDumpToMySQL seeds the corpus on a
// live MySQL, dumps it with REAL mydumper in both literal encodings,
// migrates each dump into a fresh MySQL database, and ground-truths
// every cell by direct SQL against the hand-written want. The
// zero-date leg pins the loud refusal through a real dump.
func TestMydumperAdversarialCorpus_RealDumpToMySQL(t *testing.T) {
	cells := advDumpCorpus()
	advDumpAssertFloor(t, cells)

	ctx := context.Background()
	mysqlC, rootDSN := startMySQLIT(t)
	advDumpSeed(t, rootDSN, cells)

	dumperC := startDumperIT(t)
	sourceIP, err := mysqlC.ContainerIP(ctx)
	if err != nil {
		t.Fatalf("mysql container IP: %v", err)
	}

	dumpEng := mustEngine(t, "mydumper")
	mysqlEng := mustEngine(t, "mysql")

	// adv_zerodate rides the same real dump but is EXCLUDED from the
	// value legs (it must refuse); the refusal leg includes only it.
	valueFilter, err := migcore.NewTableFilter(nil, []string{"adv_zerodate"})
	if err != nil {
		t.Fatal(err)
	}
	zeroFilter, err := migcore.NewTableFilter([]string{"adv_zerodate"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	legs := []struct{ name, flags string }{
		{"escape", ""}, // default backslash-escaped binary
		{"hex", "--hex-blob"},
	}
	for _, leg := range legs {
		leg := leg
		dumpDir := runRealMydumper(t, dumperC, sourceIP, "adv-"+leg.name, leg.flags)

		t.Run("probe-"+leg.name, func(t *testing.T) {
			target := createMySQLDB(t, rootDSN, "adv_"+leg.name)
			runMigrate(t, dumpEng, mysqlEng, dumpDir, target, valueFilter)
			advDumpProbeCells(t, target, "adv", cells, 1)
		})

		t.Run("zero-date-refusal-"+leg.name, func(t *testing.T) {
			target := createMySQLDB(t, rootDSN, "adv_zd_"+leg.name)
			err := runMigrateErr(t, dumpEng, mysqlEng, dumpDir, target, zeroFilter)
			if err == nil {
				advDumpDumpZeroDateTarget(t, target)
				t.Fatal("HEADLINE: migrating a real-mydumper dump holding '0000-00-00' exited CLEAN — " +
					"a value with no valid calendar form landed on an exit-0 path (see dumped target rows above)")
			}
			// The unhandled zeroDateValueError names the raw value; the
			// mydumper engine has no --zero-date plumbing (ADR-0161 §7),
			// so loud-with-the-value is the whole contract.
			if !strings.Contains(err.Error(), "0000-00-00") {
				t.Fatalf("zero-date refusal does not name the value:\n%v", err)
			}
			if n, ok := advDumpCountIfExists(t, target, "adv_zerodate"); ok && n != 0 {
				t.Errorf("refusal landed %d row(s); want 0", n)
			}
			t.Logf("zero-date refusal (%s leg): %v", leg.name, err)
		})
	}
}

// ---------------------------------------------------------------------------
// corpus plumbing
// ---------------------------------------------------------------------------

func advDumpAssertFloor(t *testing.T, cells []advDumpCell) {
	t.Helper()
	families := map[string]int{}
	for _, c := range cells {
		families[c.family]++
	}
	if len(families) < advDumpMinFamilies || len(cells) < advDumpMinCells {
		t.Fatalf("anti-vacuity floor: corpus has %d families / %d cells; floor is %d/%d (families: %v)",
			len(families), len(cells), advDumpMinFamilies, advDumpMinCells, families)
	}
}

// advDumpSeed creates + seeds the corpus tables on the source: the adv
// value table under a UTC session, and adv_zerodate under sql_mode=”
// (the only way a real source holds '0000-00-00').
func advDumpSeed(t *testing.T, dsn string, cells []advDumpCell) {
	t.Helper()
	var b strings.Builder
	b.WriteString("SET SESSION time_zone = '+00:00';\n")
	b.WriteString("CREATE TABLE adv (\n\tid BIGINT NOT NULL,\n")
	for _, c := range cells {
		fmt.Fprintf(&b, "\t%s %s,\n", c.col, c.ddl)
	}
	b.WriteString("\tPRIMARY KEY (id)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n")

	cols := []string{"id"}
	vals := []string{"1"}
	for _, c := range cells {
		cols = append(cols, c.col)
		vals = append(vals, c.lit)
	}
	fmt.Fprintf(&b, "INSERT INTO adv (%s) VALUES (%s);\n", strings.Join(cols, ", "), strings.Join(vals, ", "))

	b.WriteString(`SET SESSION sql_mode='';
CREATE TABLE adv_zerodate (
  id BIGINT NOT NULL,
  d  DATE NOT NULL,
  dt DATETIME NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
INSERT INTO adv_zerodate VALUES (1, '0000-00-00', '0000-00-00 00:00:00');
`)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.Exec(b.String()); err != nil {
		t.Fatalf("seed adversarial corpus: %v", err)
	}
	for _, c := range cells {
		if c.seedVal == nil {
			continue
		}
		if _, err := db.Exec(fmt.Sprintf("UPDATE adv SET %s = ? WHERE id = ?", c.col), c.seedVal, 1); err != nil {
			t.Fatalf("seed adv.%s via param: %v", c.col, err)
		}
	}
}

// advDumpProbeCells ground-truths every cell against the dump-migrated
// target via direct SQL on a UTC-pinned session. One subtest per cell;
// a mismatch names the family and both values.
func advDumpProbeCells(t *testing.T, dsn, table string, cells []advDumpCell, id int) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("conn target: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if _, err := conn.ExecContext(ctx, "SET SESSION time_zone = '+00:00'"); err != nil {
		t.Fatalf("pin session UTC: %v", err)
	}

	for _, c := range cells {
		c := c
		t.Run(c.family+"/"+c.col, func(t *testing.T) {
			expr := strings.ReplaceAll(c.probe, "%s", c.col)
			qctx, qcancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer qcancel()
			var got sql.NullString
			q := fmt.Sprintf("SELECT %s FROM %s WHERE id = ?", expr, table)
			if err := conn.QueryRowContext(qctx, q, id).Scan(&got); err != nil {
				t.Fatalf("probe %s: %v\nquery: %s", c.col, err, q)
			}
			if !got.Valid {
				t.Fatalf("probe %s returned SQL NULL; want %q (value silently dropped?)", c.col, c.want)
			}
			if !advDumpCompare(c.cmp, got.String, c.want) {
				t.Errorf("SILENT VALUE ALTERATION on %s (family %s):\n  target holds: %q\n  want:         %q\n  probe: %s",
					c.col, c.family, got.String, c.want, expr)
			}
		})
	}
}

func advDumpCompare(mode advDumpCmp, got, want string) bool {
	switch mode {
	case advDumpCmpFloat64:
		g, gerr := strconv.ParseFloat(got, 64)
		w, werr := strconv.ParseFloat(want, 64)
		return gerr == nil && werr == nil && math.Float64bits(g) == math.Float64bits(w)
	case advDumpCmpFloat32:
		g, gerr := strconv.ParseFloat(got, 32)
		w, werr := strconv.ParseFloat(want, 32)
		return gerr == nil && werr == nil && math.Float32bits(float32(g)) == math.Float32bits(float32(w))
	default:
		return got == want
	}
}

func advDumpMD5(b []byte) string {
	sum := md5.Sum(b)
	return fmt.Sprintf("%x", sum)
}

// advDumpBlobPattern builds an n-byte adversarial blob with NUL runs,
// 0xFF runs, and every byte value (an LCG walk), so any transcoding or
// escape mishandling anywhere in the dump pipe changes the MD5.
func advDumpBlobPattern(n int) []byte {
	out := make([]byte, n)
	state := uint32(0x2545F491)
	for i := range out {
		switch {
		case i%1024 < 16:
			out[i] = 0x00
		case i%1024 >= 1008:
			out[i] = 0xFF
		default:
			state = state*1664525 + 1013904223
			out[i] = byte(state >> 24)
		}
	}
	return out
}

// advDumpCountIfExists returns the row count of table on the MySQL
// target, or ok=false when the table does not exist.
func advDumpCountIfExists(t *testing.T, dsn, table string) (int, bool) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var exists int
	if err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?",
		table).Scan(&exists); err != nil {
		t.Fatalf("probe table existence: %v", err)
	}
	if exists == 0 {
		return 0, false
	}
	var n int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n, true
}

// advDumpDumpZeroDateTarget logs whatever landed for the zero-date
// refusal table so an exit-0 failure names the corrupt values.
func advDumpDumpZeroDateTarget(t *testing.T, dsn string) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	rows, err := db.QueryContext(ctx, "SELECT id, CAST(d AS CHAR), CAST(dt AS CHAR) FROM adv_zerodate ORDER BY id")
	if err != nil {
		t.Logf("dump adv_zerodate: %v", err)
		return
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var id int
		var d, dt sql.NullString
		if err := rows.Scan(&id, &d, &dt); err == nil {
			t.Logf("  target adv_zerodate: id=%d d=%q dt=%q", id, d.String, dt.String)
		}
	}
	_ = rows.Err()
}
