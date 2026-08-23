//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Adversarial value-fidelity corpus — SQLite directions. See
// adversarial_corpus_shared_integration_test.go for the corpus model.
//
// SQLite is unlike the MySQL/PG corpora in two load-bearing ways:
//
//  1. Values carry their own storage class independent of the declared
//     type, so the corpus has TWO axes: worst-case values (this file's
//     clean cells) and worst-case STORAGE-CLASS anarchy (a TEXT value in
//     an INTEGER column, …) which must refuse loudly (the refusal
//     matrix).
//  2. One source corpus feeds two target dialects, so each source cell
//     carries a per-target (probe, want) pair; a cell a target cannot
//     faithfully hold names the target-specific reason and moves to the
//     refusal matrix instead ([advSQLiteCell.pgSkip]/[advSQLiteCell.mySkip]).
//
// Unconstructible cells, with evidence (the sibling enumeration):
//
//   - float NaN: SQLite stores NaN as SQL NULL (ground-truthed on
//     modernc: `SELECT 0.0/0.0` scans as nil), so no SQLite source can
//     deliver a NaN.
//   - float -0.0: SQLite normalizes -0.0 to +0.0 at storage — a bound
//     -0.0 driver param reads back with bits 0x0 (ground-truthed on
//     modernc 2026-08-22). No SQLite source can deliver a negative
//     zero; the SQLite-as-TARGET round pins what happens to a PG -0.0
//     on the way IN (see advCorpusPGToSQLite).
//   - zero dates: SQLite has no zero-date concept; '0000-00-00' TEXT in
//     a declared DATE column matches no ISO layout and is a loud decode
//     refusal (covered by the refusal matrix's r_bad_iso cell).

package pipeline

import (
	"context"
	"database/sql"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "modernc.org/sqlite" // pure-Go driver for seeding + probing temp source/target files

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
	_ "sluicesync.dev/sluice/internal/engines/sqlite"
)

// Anti-vacuity floors. The SQLite source corpus spans 7 families; the
// PG-target view keeps every family, the MySQL view drops only
// named-skip cells (NUL text and Inf move to the refusal matrix there).
const (
	advSQLiteMinFamilies = 7
	advSQLiteMinCellsPG  = 30
	advSQLiteMinCellsMy  = 28

	advPG2SQLiteMinFamilies = 8
	advPG2SQLiteMinCells    = 18
)

// advSQLiteCell is one source cell with per-target ground truth.
type advSQLiteCell struct {
	family string
	col    string
	ddl    string // SQLite declared type
	lit    string // SQLite literal for the seed INSERT

	seedVal any // driver-param seeded (large values); lit must be "NULL"

	pgProbe string
	pgWant  string
	pgCmp   advCmp
	pgSkip  string // non-empty: cell absent from the PG corpus, reason named

	myProbe string
	myWant  string
	myCmp   advCmp
	mySkip  string // non-empty: cell absent from the MySQL corpus, reason named

	cdcSkip string
}

// advDec30 renders the MySQL DECIMAL(65,30) text of a plain decimal
// literal: SQLite NUMERIC maps to ir.Decimal{Unconstrained}, which the
// MySQL writer emits as DECIMAL(65,30) (Bug 69), so CAST(col AS CHAR)
// renders scale-30 trailing zeros.
func advDec30(lit string) string {
	intPart, frac, _ := strings.Cut(lit, ".")
	if len(frac) > 30 {
		panic("advDec30: fraction longer than MySQL's max scale: " + lit)
	}
	return intPart + "." + frac + strings.Repeat("0", 30-len(frac))
}

// advCorpusSQLiteCells is the single source-of-truth cell list for the
// SQLite-source corpus (both targets + the trigger-CDC round derive
// their views from it, so the three lanes can never drift on the
// source values).
func advCorpusSQLiteCells() []advSQLiteCell {
	bigText := strings.Repeat("héllo wörld 🦀 snøw ", 1500) // ~40KB multi-byte UTF-8
	bigBlob := advBlobPattern(1 << 20)                     // 1 MiB, NUL + 0xFF runs
	allBytes := make([]byte, 256)
	for i := range allBytes {
		allBytes[i] = byte(i)
	}

	return []advSQLiteCell{
		// ---- integer (SQLite INTEGER is always 64-bit signed) ----
		{
			family: "integer", col: "i64_min", ddl: "INTEGER NOT NULL", lit: "-9223372036854775808",
			pgProbe: "%s::text", pgWant: "-9223372036854775808",
			myProbe: "CAST(%s AS CHAR)", myWant: "-9223372036854775808",
		},
		{
			family: "integer", col: "i64_max", ddl: "INTEGER NOT NULL", lit: "9223372036854775807",
			pgProbe: "%s::text", pgWant: "9223372036854775807",
			myProbe: "CAST(%s AS CHAR)", myWant: "9223372036854775807",
		},
		// 2^53 + 1 — the headline: any JSON/float64 hop lands …992.
		{
			family: "integer", col: "i53_plus1", ddl: "INTEGER NOT NULL", lit: "9007199254740993",
			pgProbe: "%s::text", pgWant: "9007199254740993",
			myProbe: "CAST(%s AS CHAR)", myWant: "9007199254740993",
		},
		{
			family: "integer", col: "i53_neg", ddl: "INTEGER NOT NULL", lit: "-9007199254740993",
			pgProbe: "%s::text", pgWant: "-9007199254740993",
			myProbe: "CAST(%s AS CHAR)", myWant: "-9007199254740993",
		},
		{
			family: "integer", col: "i_snowflake", ddl: "BIGINT NOT NULL", lit: "1837113234971131904",
			pgProbe: "%s::text", pgWant: "1837113234971131904",
			myProbe: "CAST(%s AS CHAR)", myWant: "1837113234971131904",
		},

		// ---- float (REAL; PG bit-exact via float8send, MySQL via
		// shortest-round-trip bit compare) ----
		{
			family: "float", col: "f8_17sig", ddl: "REAL NOT NULL", lit: "0.1234567890123456789",
			pgProbe: "encode(float8send(%s),'hex')", pgWant: advFloat64Bits("0.1234567890123456789"),
			myProbe: "CONCAT(%s)", myWant: "0.1234567890123456789", myCmp: advCmpFloat64,
		},
		{
			family: "float", col: "f8_max", ddl: "REAL NOT NULL", lit: "1.7976931348623157e308",
			pgProbe: "encode(float8send(%s),'hex')", pgWant: advFloat64Bits("1.7976931348623157e308"),
			myProbe: "CONCAT(%s)", myWant: "1.7976931348623157e308", myCmp: advCmpFloat64,
		},
		{
			family: "float", col: "f8_subnormal", ddl: "REAL NOT NULL", lit: "5e-324",
			pgProbe: "encode(float8send(%s),'hex')", pgWant: advFloat64Bits("5e-324"),
			myProbe: "CONCAT(%s)", myWant: "5e-324", myCmp: advCmpFloat64,
		},
		{
			family: "float", col: "f8_third", ddl: "DOUBLE NOT NULL", lit: "0.30000000000000004",
			pgProbe: "encode(float8send(%s),'hex')", pgWant: advFloat64Bits("0.30000000000000004"),
			myProbe: "CONCAT(%s)", myWant: "0.30000000000000004", myCmp: advCmpFloat64,
		},
		// ±Inf: representable in SQLite REAL (literal 9e999) and PG
		// float8; MySQL DOUBLE cannot hold it → MySQL refusal matrix.
		{
			family: "float", col: "f8_inf", ddl: "REAL NOT NULL", lit: "9e999",
			pgProbe: "%s::text", pgWant: "Infinity",
			mySkip: "MySQL DOUBLE cannot store Inf; pinned as a loud refusal in the MySQL refusal matrix",
		},
		{
			family: "float", col: "f8_neginf", ddl: "REAL NOT NULL", lit: "-9e999",
			pgProbe: "%s::text", pgWant: "-Infinity",
			mySkip: "MySQL DOUBLE cannot store -Inf; pinned as a loud refusal in the MySQL refusal matrix",
		},

		// ---- text ----
		{
			family: "text", col: "t_emoji", ddl: "TEXT NOT NULL", lit: "'crab 🦀 café ✅'",
			pgProbe: "%s", pgWant: "crab 🦀 café ✅",
			myProbe: "CONCAT(%s)", myWant: "crab 🦀 café ✅",
		},
		// Decomposed combining diaeresis (e + U+0308), 6 bytes — must
		// NOT be unicode-normalized to the 5-byte composed form.
		{
			family: "text", col: "t_combining", ddl: "TEXT NOT NULL", lit: "'noël'",
			pgProbe: "%s || '#' || octet_length(%s)::text", pgWant: "noël#6",
			myProbe: "CONCAT(%s,'#',LENGTH(%s))", myWant: "noël#6",
		},
		{
			family: "text", col: "t_trailing", ddl: "TEXT NOT NULL", lit: "'pad   '",
			pgProbe: "'[' || %s || ']#' || length(%s)::text", pgWant: "[pad   ]#6",
			myProbe: "CONCAT('[',%s,']#',CHAR_LENGTH(%s))", myWant: "[pad   ]#6",
		},
		// ZWJ family sequence: 4 emoji joined by 3 U+200D = 25 bytes.
		{
			family: "text", col: "t_zwj", ddl: "TEXT NOT NULL", lit: "'👨‍👩‍👧‍👦'",
			pgProbe: "octet_length(%s)::text || '#' || %s", pgWant: "25#👨‍👩‍👧‍👦",
			myProbe: "CONCAT(LENGTH(%s),'#',%s)", myWant: "25#👨‍👩‍👧‍👦",
		},
		// Digit string with leading zeros: TEXT storage must stay text,
		// never numeric-normalized to "123".
		{
			family: "text", col: "t_digits", ddl: "TEXT NOT NULL", lit: "'00123'",
			pgProbe: "%s", pgWant: "00123",
			myProbe: "CONCAT(%s)", myWant: "00123",
		},
		// Empty string is NOT NULL — the empty-vs-nil decode edge.
		{
			family: "text", col: "t_empty", ddl: "TEXT NOT NULL", lit: "''",
			pgProbe: "'[' || %s || ']#' || length(%s)::text", pgWant: "[]#0",
			myProbe: "CONCAT('[',%s,']#',CHAR_LENGTH(%s))", myWant: "[]#0",
		},
		// A JSON document with a > 2^53 number, carried as TEXT: must
		// land byte-exact, never re-parsed/rounded by anything.
		{
			family: "text", col: "t_json_bigint", ddl: "TEXT NOT NULL", lit: `'{"n":9007199254740993}'`,
			pgProbe: "%s", pgWant: `{"n":9007199254740993}`,
			myProbe: "CONCAT(%s)", myWant: `{"n":9007199254740993}`,
		},
		// Embedded NUL: PG text cannot hold it (SLUICE-E-VALUE-NUL-BYTE,
		// pinned in the PG refusal matrix); MySQL TEXT carries it.
		{
			family: "text", col: "t_nul", ddl: "TEXT NOT NULL", lit: "CAST(x'610062' AS TEXT)",
			pgSkip:  "PG text cannot store a NUL byte; pinned as SLUICE-E-VALUE-NUL-BYTE in the PG refusal matrix",
			myProbe: "HEX(%s)", myWant: "610062",
			cdcSkip: "the CDC round targets PG, where the NUL cell is a refusal, not a landing",
		},
		{
			family: "text", col: "t_big", ddl: "TEXT", lit: "NULL", seedVal: bigText,
			pgProbe: "md5(%s) || '#' || octet_length(%s)::text",
			pgWant:  advMD5([]byte(bigText)) + "#" + advItoa(len(bigText)),
			myProbe: "CONCAT(MD5(%s),'#',LENGTH(%s))",
			myWant:  advMD5([]byte(bigText)) + "#" + advItoa(len(bigText)),
		},

		// ---- binary ----
		{
			family: "binary", col: "b_nulrun", ddl: "BLOB NOT NULL", lit: "x'00FF7F00'",
			pgProbe: "encode(%s,'hex') || '#' || octet_length(%s)::text", pgWant: "00ff7f00#4",
			myProbe: "CONCAT(LOWER(HEX(%s)),'#',LENGTH(%s))", myWant: "00ff7f00#4",
		},
		// Empty blob is NOT NULL — the []byte{} vs nil decode edge.
		{
			family: "binary", col: "b_empty", ddl: "BLOB NOT NULL", lit: "x''",
			pgProbe: "octet_length(%s)::text", pgWant: "0",
			myProbe: "CONCAT(LENGTH(%s))", myWant: "0",
		},
		{
			family: "binary", col: "b_allbytes", ddl: "BLOB", lit: "NULL", seedVal: allBytes256(),
			pgProbe: "md5(%s) || '#' || octet_length(%s)::text",
			pgWant:  advMD5(allBytes256()) + "#256",
			myProbe: "CONCAT(MD5(%s),'#',LENGTH(%s))",
			myWant:  advMD5(allBytes256()) + "#256",
		},
		{
			family: "binary", col: "b_big", ddl: "BLOB", lit: "NULL", seedVal: bigBlob,
			pgProbe: "md5(%s) || '#' || octet_length(%s)::text",
			pgWant:  advMD5(bigBlob) + "#" + advItoa(len(bigBlob)),
			myProbe: "CONCAT(MD5(%s),'#',LENGTH(%s))",
			myWant:  advMD5(bigBlob) + "#" + advItoa(len(bigBlob)),
		},

		// ---- decimal (NUMERIC affinity → ir.Decimal{Unconstrained} →
		// PG bare numeric / MySQL DECIMAL(65,30), Bug 69) ----
		// 19.99 is stored by SQLite as the binary REAL; the decode's
		// shortest-round-trip 'f' rendering must recover "19.99", not
		// the 19.989999999999998 expansion (Bug 162's shape).
		{
			family: "decimal", col: "dec_1999", ddl: "NUMERIC NOT NULL", lit: "19.99",
			pgProbe: "%s::text", pgWant: "19.99",
			myProbe: "CAST(%s AS CHAR)", myWant: advDec30("19.99"),
		},
		// Magnitude ≥ 1e6: the strconv 'g' verb would render exponent
		// notation here and abort pgx's numeric COPY encode (Bug 163) —
		// this cell is the mutation tripwire for that verb.
		{
			family: "decimal", col: "dec_big7", ddl: "NUMERIC NOT NULL", lit: "1234567.89",
			pgProbe: "%s::text", pgWant: "1234567.89",
			myProbe: "CAST(%s AS CHAR)", myWant: advDec30("1234567.89"),
		},
		{
			family: "decimal", col: "dec_bigint", ddl: "NUMERIC NOT NULL", lit: "9007199254740993",
			pgProbe: "%s::text", pgWant: "9007199254740993",
			myProbe: "CAST(%s AS CHAR)", myWant: advDec30("9007199254740993."),
		},
		{
			family: "decimal", col: "dec_tiny", ddl: "NUMERIC NOT NULL", lit: "0.0000000001",
			pgProbe: "%s::text", pgWant: "0.0000000001",
			myProbe: "CAST(%s AS CHAR)", myWant: advDec30("0.0000000001"),
		},

		// ---- temporal (declared DATE/DATETIME/TIME, ISO TEXT storage,
		// the ADR-0129 default encoding) ----
		{
			family: "temporal", col: "d_min", ddl: "DATE NOT NULL", lit: "'0001-01-01'",
			pgProbe: "%s::text", pgWant: "0001-01-01",
			myProbe: "CAST(%s AS CHAR)", myWant: "0001-01-01",
		},
		{
			family: "temporal", col: "d_max", ddl: "DATE NOT NULL", lit: "'9999-12-31'",
			pgProbe: "%s::text", pgWant: "9999-12-31",
			myProbe: "CAST(%s AS CHAR)", myWant: "9999-12-31",
		},
		{
			family: "temporal", col: "dt_frac", ddl: "DATETIME NOT NULL", lit: "'2026-03-08 02:30:00.123456'",
			pgProbe: "to_char(%s,'YYYY-MM-DD HH24:MI:SS.US')", pgWant: "2026-03-08 02:30:00.123456",
			myProbe: "CAST(%s AS CHAR)", myWant: "2026-03-08 02:30:00.123456",
		},
		{
			family: "temporal", col: "dt_max", ddl: "DATETIME NOT NULL", lit: "'9999-12-31 23:59:59.999999'",
			pgProbe: "to_char(%s,'YYYY-MM-DD HH24:MI:SS.US')", pgWant: "9999-12-31 23:59:59.999999",
			myProbe: "CAST(%s AS CHAR)", myWant: "9999-12-31 23:59:59.999999",
		},
		{
			family: "temporal", col: "tm_frac", ddl: "TIME NOT NULL", lit: "'23:59:59.999999'",
			pgProbe: "%s::text", pgWant: "23:59:59.999999",
			myProbe: "CAST(%s AS CHAR)", myWant: "23:59:59.999999",
		},

		// ---- boolean (declared BOOLEAN, ADR-0129 fixed 0/1/truthy-text
		// rule) ----
		{
			family: "boolean", col: "bo_true", ddl: "BOOLEAN NOT NULL", lit: "1",
			pgProbe: "%s::text", pgWant: "true",
			myProbe: "CONCAT(%s)", myWant: "1",
		},
		{
			family: "boolean", col: "bo_false", ddl: "BOOLEAN NOT NULL", lit: "0",
			pgProbe: "%s::text", pgWant: "false",
			myProbe: "CONCAT(%s)", myWant: "0",
		},
		// TEXT-class truthy value in a BOOLEAN column (NUMERIC affinity
		// keeps 'true' as TEXT storage) — the cross-class arm of the
		// fixed boolean rule.
		{
			family: "boolean", col: "bo_text", ddl: "BOOLEAN NOT NULL", lit: "'true'",
			pgProbe: "%s::text", pgWant: "true",
			myProbe: "CONCAT(%s)", myWant: "1",
		},
	}
}

// allBytes256 is the 0x00..0xFF byte ramp (every byte value once).
func allBytes256() []byte {
	b := make([]byte, 256)
	for i := range b {
		b[i] = byte(i)
	}
	return b
}

// advSQLiteCorpusView projects the shared cell list into a per-target
// []advCell, dropping (and logging) the named-skip cells for that
// target. target is "pg" or "mysql".
func advSQLiteCorpusView(t *testing.T, target string) []advCell {
	t.Helper()
	src := advCorpusSQLiteCells()
	out := make([]advCell, 0, len(src))
	skipped := 0
	for _, c := range src {
		var probe, want, skip string
		var cmp advCmp
		switch target {
		case "pg":
			probe, want, cmp, skip = c.pgProbe, c.pgWant, c.pgCmp, c.pgSkip
		case "mysql":
			probe, want, cmp, skip = c.myProbe, c.myWant, c.myCmp, c.mySkip
		default:
			t.Fatalf("advSQLiteCorpusView: unknown target %q", target)
		}
		if skip != "" {
			skipped++
			t.Logf("%s corpus skips %s/%s: %s", target, c.family, c.col, skip)
			continue
		}
		out = append(out, advCell{
			family: c.family, col: c.col, ddl: c.ddl, lit: c.lit,
			probe: probe, want: want, cmp: cmp,
			seedVal: c.seedVal, cdcSkip: c.cdcSkip,
		})
	}
	// A named skip must be the exception: more than 2 per target means
	// the corpus is quietly narrowing.
	if skipped > 2 {
		t.Fatalf("%s corpus view dropped %d cells; at most 2 named skips allowed", target, skipped)
	}
	return out
}

// advSeedSQLiteCorpus writes the corpus into a fresh temp SQLite file
// and returns its path. Cells are seeded row-by-row via literals, with
// seedVal cells applied through driver params.
func advSeedSQLiteCorpus(t *testing.T, cells []advCell, id int) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "adv_corpus.db")
	db := advOpenSQLite(t, path)
	advExecSQLite(t, db, advBuildCreate("adv_corpus", cells, false))
	advExecSQLite(t, db, advBuildInsert("adv_corpus", cells, id))
	advSeedParams(t, db, "adv_corpus", cells, id, false)
	if err := db.Close(); err != nil {
		t.Fatalf("close sqlite seed: %v", err)
	}
	return path
}

// advOpenSQLite opens a modernc connection to a SQLite file with a busy
// timeout (so a concurrent poller never turns into SQLITE_BUSY flake).
func advOpenSQLite(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", path+"?_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatalf("open sqlite %s: %v", path, err)
	}
	return db
}

// advExecSQLite runs one statement against a SQLite handle.
func advExecSQLite(t *testing.T, db *sql.DB, stmt string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, stmt); err != nil {
		t.Fatalf("sqlite exec %q: %v", stmt, err)
	}
}

// TestMigrate_AdversarialCorpus_SQLiteToPostgres is the cold-copy round
// for the SQLite→PG direction: seed the corpus file, migrate, run
// sluice verify (count), then ground-truth every cell against the real
// PG target by direct SQL.
func TestMigrate_AdversarialCorpus_SQLiteToPostgres(t *testing.T) {
	cells := advSQLiteCorpusView(t, "pg")
	advAssertCorpusFloor(t, "sqlite→pg", cells, advSQLiteMinFamilies, advSQLiteMinCellsPG)

	src := advSeedSQLiteCorpus(t, cells, 1)
	_, pgTarget, pgCleanup := startPostgres(t)
	defer pgCleanup()

	sqliteEng, _ := engines.Get("sqlite")
	pgEng, _ := engines.Get("postgres")
	mig := &Migrator{
		Source: sqliteEng, Target: pgEng,
		SourceDSN: src, TargetDSN: pgTarget,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run (adversarial corpus must migrate cleanly): %v", err)
	}

	advRunVerify(t, &Verifier{
		Source: sqliteEng, Target: pgEng,
		SourceDSN: src, TargetDSN: pgTarget,
	})

	conn := advOpenConn(t, "pgx", pgTarget)
	advProbeCells(t, conn, "adv_corpus", cells, 1, true)
}

// TestMigrate_AdversarialCorpus_SQLiteToMySQL is the cold-copy round
// for the SQLite→MySQL direction.
func TestMigrate_AdversarialCorpus_SQLiteToMySQL(t *testing.T) {
	cells := advSQLiteCorpusView(t, "mysql")
	advAssertCorpusFloor(t, "sqlite→mysql", cells, advSQLiteMinFamilies, advSQLiteMinCellsMy)

	src := advSeedSQLiteCorpus(t, cells, 1)
	_, mysqlTarget, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()

	sqliteEng, _ := engines.Get("sqlite")
	mysqlEng, _ := engines.Get("mysql")
	mig := &Migrator{
		Source: sqliteEng, Target: mysqlEng,
		SourceDSN: src, TargetDSN: mysqlTarget,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run (adversarial corpus must migrate cleanly): %v", err)
	}

	advRunVerify(t, &Verifier{
		Source: sqliteEng, Target: mysqlEng,
		SourceDSN: src, TargetDSN: mysqlTarget,
	})

	conn := advOpenConn(t, "mysql", mysqlTarget, advMySQLUTCSession...)
	advProbeCells(t, conn, "adv_corpus", cells, 1, false)
}

// advSQLiteRefusal is one must-refuse-loudly cell for a SQLite source:
// the storage-class anarchy SQLite permits (and the two target-specific
// unrepresentable values). Each cell gets its OWN temp source file so
// the refusal is isolated. Landing ANYTHING silently is the headline
// failure.
type advSQLiteRefusal struct {
	name   string
	create string // source CREATE TABLE (table name must be r_cell)
	insert string // source INSERT
	target string // "pg" or "mysql"
	alt    []string
}

// TestMigrate_AdversarialCorpusRefusals_SQLite pins the must-refuse
// cells. Storage-class anarchy refusals fire in the SQLite READER
// (target-independent — run against PG once; the reader path is shared
// verbatim with a MySQL target run). The NUL / invalid-UTF-8 / Inf
// cells are target-side and pinned per target.
func TestMigrate_AdversarialCorpusRefusals_SQLite(t *testing.T) {
	_, pgTarget, pgCleanup := startPostgres(t)
	defer pgCleanup()
	_, mysqlTarget, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()

	sqliteEng, _ := engines.Get("sqlite")
	pgEng, _ := engines.Get("postgres")
	mysqlEng, _ := engines.Get("mysql")

	refusals := []advSQLiteRefusal{
		// ---- reader-side storage-class anarchy (target-independent) ----
		{
			name:   "text_in_integer_column",
			create: `CREATE TABLE r_cell (id INTEGER PRIMARY KEY, v INTEGER NOT NULL)`,
			insert: `INSERT INTO r_cell VALUES (1, 'not-a-number')`,
			target: "pg",
			alt:    []string{"storage-class mismatch"},
		},
		{
			name:   "real_in_integer_column",
			create: `CREATE TABLE r_cell (id INTEGER PRIMARY KEY, v INTEGER NOT NULL)`,
			insert: `INSERT INTO r_cell VALUES (1, 3.5)`,
			target: "pg",
			alt:    []string{"storage-class mismatch"},
		},
		{
			name:   "blob_in_text_column",
			create: `CREATE TABLE r_cell (id INTEGER PRIMARY KEY, v TEXT NOT NULL)`,
			insert: `INSERT INTO r_cell VALUES (1, x'deadbeef')`,
			target: "pg",
			alt:    []string{"storage-class mismatch"},
		},
		{
			name:   "non_boolean_in_boolean_column",
			create: `CREATE TABLE r_cell (id INTEGER PRIMARY KEY, v BOOLEAN NOT NULL)`,
			insert: `INSERT INTO r_cell VALUES (1, 2)`,
			target: "pg",
			alt:    []string{"boolean decode mismatch"},
		},
		{
			name:   "zero_date_text_in_date_column",
			create: `CREATE TABLE r_cell (id INTEGER PRIMARY KEY, v DATE NOT NULL)`,
			insert: `INSERT INTO r_cell VALUES (1, '0000-00-00')`,
			target: "pg",
			alt:    []string{"matches no ISO layout"},
		},
		// ---- target-side unrepresentable values ----
		{
			name:   "nul_byte_in_text_to_pg",
			create: `CREATE TABLE r_cell (id INTEGER PRIMARY KEY, v TEXT NOT NULL)`,
			insert: `INSERT INTO r_cell VALUES (1, CAST(x'610062' AS TEXT))`,
			target: "pg",
			alt:    []string{"SLUICE-E-VALUE-NUL-BYTE", "nul byte"},
		},
		{
			name:   "invalid_utf8_text_to_pg",
			create: `CREATE TABLE r_cell (id INTEGER PRIMARY KEY, v TEXT NOT NULL)`,
			insert: `INSERT INTO r_cell VALUES (1, CAST(x'FFFE61' AS TEXT))`,
			target: "pg",
			alt:    []string{"invalid byte sequence", "utf8", "utf-8"},
		},
		{
			name:   "invalid_utf8_text_to_mysql",
			create: `CREATE TABLE r_cell (id INTEGER PRIMARY KEY, v TEXT NOT NULL)`,
			insert: `INSERT INTO r_cell VALUES (1, CAST(x'FFFE61' AS TEXT))`,
			target: "mysql",
			alt:    []string{"incorrect string value", "utf8", "utf-8", "1366"},
		},
		{
			name:   "float_inf_to_mysql",
			create: `CREATE TABLE r_cell (id INTEGER PRIMARY KEY, v REAL NOT NULL)`,
			insert: `INSERT INTO r_cell VALUES (1, 9e999)`,
			target: "mysql",
			alt:    []string{"inf", "illegal", "out of range", "nan"},
		},
	}
	if len(refusals) < 6 {
		t.Fatalf("anti-vacuity floor: refusal matrix has %d cells; floor is 6", len(refusals))
	}

	for _, rc := range refusals {
		rc := rc
		t.Run(rc.target+"/"+rc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "refusal.db")
			db := advOpenSQLite(t, path)
			advExecSQLite(t, db, rc.create)
			advExecSQLite(t, db, rc.insert)
			if err := db.Close(); err != nil {
				t.Fatalf("close refusal seed: %v", err)
			}

			targetEng, targetDSN := pgEng, pgTarget
			if rc.target == "mysql" {
				targetEng, targetDSN = mysqlEng, mysqlTarget
			}
			mig := &Migrator{
				Source: sqliteEng, Target: targetEng,
				SourceDSN: path, TargetDSN: targetDSN,
				// Distinct per cell: a refusal records a partial-migration
				// ledger row on the target, and the next cell's run would
				// otherwise be told to --resume it.
				MigrationID: "adv-sqlite-refusal-" + rc.name,
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()
			err := mig.Run(ctx)
			if err == nil {
				// EXIT-0 PATH: the value either landed altered (the
				// headline) or faithfully (a mapping change this pin must
				// be updated for — loudly, not silently).
				advDumpSQLiteRefusalTarget(t, rc.target, targetDSN)
				t.Fatalf("HEADLINE: migrate of %s exited CLEAN — a value with no faithful representation "+
					"landed on an exit-0 path (silent loss/alteration). See dumped target value above.", rc.name)
			}
			advAssertRefusal(t, err, "", rc.alt)

			// The refusal must precede the bad row's write: zero rows on
			// the target (the table may or may not exist, depending on
			// how early the refusal fired).
			var n int
			var ok bool
			if rc.target == "pg" {
				n, ok = advCountIfExists(t, targetDSN, "r_cell")
			} else {
				n, ok = advMySQLCountIfExists(t, targetDSN, "r_cell")
			}
			if ok && n != 0 {
				t.Errorf("refusal %s left %d row(s) on the target; want 0 (refusal must precede the write)", rc.name, n)
			}
			// Drop the leftover empty table so the next cell starts clean.
			advDropRefusalTable(t, rc.target, targetDSN)
			t.Logf("refusal %s: %v", rc.name, err)
		})
	}
}

// advDropRefusalTable clears the shared refusal table between cells.
func advDropRefusalTable(t *testing.T, target, dsn string) {
	t.Helper()
	driver := "pgx"
	if target == "mysql" {
		driver = "mysql"
	}
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, _ = db.ExecContext(ctx, "DROP TABLE IF EXISTS r_cell")
}

// advDumpSQLiteRefusalTarget logs whatever landed for a refusal cell so
// an exit-0 failure names the corrupt value in the test output.
func advDumpSQLiteRefusalTarget(t *testing.T, target, dsn string) {
	t.Helper()
	driver := "pgx"
	q := "SELECT id, v::text FROM r_cell ORDER BY id"
	if target == "mysql" {
		driver = "mysql"
		q = "SELECT id, CAST(v AS CHAR) FROM r_cell ORDER BY id"
	}
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		t.Logf("dump r_cell: %v", err)
		return
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var id int
		var v sql.NullString
		if err := rows.Scan(&id, &v); err == nil {
			t.Logf("  target r_cell: id=%d v=%q", id, v.String)
		}
	}
	_ = rows.Err()
}

// advCorpusPGToSQLite is the SQLite-as-TARGET corpus: PG-source cells
// whose probes run on the target SQLite FILE through the modernc driver
// — an independent reader from sluice's own writer (per the new-surface
// checklist), asserting the exact storage class AND value each family
// lands as (per ADR-0134: decimal/JSON/UUID/temporal → TEXT, bool →
// INTEGER 0/1, bytes → BLOB).
func advCorpusPGToSQLite() []advCell {
	return []advCell{
		{
			family: "integer", col: "i64_min", ddl: "BIGINT NOT NULL", lit: "-9223372036854775808",
			probe: "typeof(%s) || '#' || CAST(%s AS TEXT)", want: "integer#-9223372036854775808",
		},
		{
			family: "integer", col: "i64_max", ddl: "BIGINT NOT NULL", lit: "9223372036854775807",
			probe: "typeof(%s) || '#' || CAST(%s AS TEXT)", want: "integer#9223372036854775807",
		},
		{
			family: "integer", col: "i53_plus1", ddl: "BIGINT NOT NULL", lit: "9007199254740993",
			probe: "typeof(%s) || '#' || CAST(%s AS TEXT)", want: "integer#9007199254740993",
		},
		// -0.0: PG float8 holds the signed zero; SQLite REAL storage
		// normalizes it to +0.0 (ground-truthed on modernc — a bound
		// -0.0 reads back with bits 0). If this cell ever lands "-0"
		// SQLite started preserving the sign; if it lands "0" the
		// normalization is the documented target wart (the SQLite twin
		// of ADR-0066's trigger-engine -0→+0 note).
		{
			family: "float", col: "f8_negzero", ddl: "DOUBLE PRECISION NOT NULL", lit: "'-0'::float8",
			probe: "typeof(%s) || '#' || CAST(%s AS TEXT)", want: "real#0.0",
		},
		// Probes use CAST(… AS TEXT) — round-trip-exact on SQLite ≥ 3.43
		// (empirically 0 misses over a 5k random-bits sweep on the bundled
		// modernc build) — NOT format('%.Ng'), whose rendering is no
		// longer digit-faithful on those versions (it renders
		// 0.30000000000000004 as "0.3" at ANY precision N).
		{
			family: "float", col: "f8_17sig", ddl: "DOUBLE PRECISION NOT NULL", lit: "0.1234567890123456789",
			probe: "CAST(%s AS TEXT)", want: "0.12345678901234568",
		},
		{
			family: "float", col: "f8_subnormal", ddl: "DOUBLE PRECISION NOT NULL", lit: "'5e-324'::float8",
			probe: "CAST(%s AS TEXT)", want: "4.9406564584124654e-324",
		},
		{
			family: "float", col: "f8_third", ddl: "DOUBLE PRECISION NOT NULL", lit: "0.30000000000000004",
			probe: "CAST(%s AS TEXT)", want: "0.30000000000000004",
		},
		// Decimal → TEXT affinity, byte-exact (Bug 162: NUMERIC affinity
		// would corrupt 19.99 → 19.989999999999998).
		{
			family: "decimal", col: "dec_1999", ddl: "NUMERIC(10,2) NOT NULL", lit: "19.99",
			probe: "typeof(%s) || '#' || CAST(%s AS TEXT)", want: "text#19.99",
		},
		{
			family: "decimal", col: "dec_max", ddl: "NUMERIC(65,30) NOT NULL",
			lit:   "'99999999999999999999999999999999999.999999999999999999999999999999'",
			probe: "typeof(%s) || '#' || CAST(%s AS TEXT)",
			want:  "text#99999999999999999999999999999999999.999999999999999999999999999999",
		},
		{
			family: "text", col: "t_emoji", ddl: "TEXT NOT NULL", lit: "'crab 🦀 café ✅'",
			probe: "typeof(%s) || '#' || %s", want: "text#crab 🦀 café ✅",
		},
		{
			family: "text", col: "t_trailing", ddl: "VARCHAR(32) NOT NULL", lit: "'pad   '",
			probe: "'[' || %s || ']#' || length(CAST(%s AS BLOB))", want: "[pad   ]#6",
		},
		{
			family: "text", col: "t_empty", ddl: "TEXT NOT NULL", lit: "''",
			probe: "typeof(%s) || '#' || length(%s)", want: "text#0",
		},
		{
			family: "binary", col: "by_nul", ddl: "BYTEA NOT NULL", lit: `'\x00ff7f00'`,
			probe: "typeof(%s) || '#' || lower(hex(%s))", want: "blob#00ff7f00",
		},
		{
			family: "binary", col: "by_empty", ddl: "BYTEA NOT NULL", lit: `'\x'`,
			probe: "typeof(%s) || '#' || length(%s)", want: "blob#0",
		},
		{
			family: "boolean", col: "bo_true", ddl: "BOOLEAN NOT NULL", lit: "true",
			probe: "typeof(%s) || '#' || CAST(%s AS TEXT)", want: "integer#1",
		},
		{
			family: "boolean", col: "bo_false", ddl: "BOOLEAN NOT NULL", lit: "false",
			probe: "typeof(%s) || '#' || CAST(%s AS TEXT)", want: "integer#0",
		},
		{
			family: "temporal", col: "d_date", ddl: "DATE NOT NULL", lit: "'2026-02-28'",
			probe: "typeof(%s) || '#' || %s", want: "text#2026-02-28",
		},
		{
			family: "temporal", col: "ts_frac", ddl: "TIMESTAMP(6) NOT NULL", lit: "'2026-03-08 02:30:00.123456'",
			probe: "typeof(%s) || '#' || %s", want: "text#2026-03-08 02:30:00.123456",
		},
		// timestamptz: the UTC instant is what SQLite stores (tz-naive
		// target, ADR-0134) — seeded at +02:00, must land shifted to UTC.
		{
			family: "temporal", col: "tstz_inst", ddl: "TIMESTAMPTZ NOT NULL", lit: "'2026-03-08 12:30:00.123456+02'",
			probe: "typeof(%s) || '#' || %s", want: "text#2026-03-08 10:30:00.123456",
		},
		{
			family: "temporal", col: "tod_frac", ddl: "TIME(6) NOT NULL", lit: "'23:59:59.999999'",
			probe: "typeof(%s) || '#' || %s", want: "text#23:59:59.999999",
		},
		// JSON (json, not jsonb: byte-preserving on the PG side) with a
		// > 2^53 number: must land as verbatim TEXT, never re-encoded.
		{
			family: "json", col: "j_bigint", ddl: "JSON NOT NULL", lit: `'{"n":9007199254740993}'`,
			probe: "typeof(%s) || '#' || %s", want: `text#{"n":9007199254740993}`,
		},
		{
			family: "uuid", col: "u_id", ddl: "UUID NOT NULL", lit: "'01234567-89ab-cdef-0123-456789abcdef'",
			probe: "typeof(%s) || '#' || %s", want: "text#01234567-89ab-cdef-0123-456789abcdef",
		},
	}
}

// TestMigrate_AdversarialCorpus_PostgresToSQLiteTarget migrates the
// PG-source corpus into a SQLite FILE target and ground-truths every
// cell by direct modernc SQL against the file (typeof + exact text/hex)
// — the write-side encoder pass for the SQLite target engine.
func TestMigrate_AdversarialCorpus_PostgresToSQLiteTarget(t *testing.T) {
	cells := advCorpusPGToSQLite()
	advAssertCorpusFloor(t, "pg→sqlite", cells, advPG2SQLiteMinFamilies, advPG2SQLiteMinCells)

	pgSource, _, pgCleanup := startPostgres(t)
	defer pgCleanup()
	dst := filepath.Join(t.TempDir(), "target.db")

	advSeedPGCorpus(t, pgSource, "adv_corpus", cells, 1)

	pgEng, _ := engines.Get("postgres")
	sqliteEng, _ := engines.Get("sqlite")
	mig := &Migrator{
		Source: pgEng, Target: sqliteEng,
		SourceDSN: pgSource, TargetDSN: dst,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run (PG corpus → SQLite target must migrate cleanly): %v", err)
	}

	if fi, err := os.Stat(dst); err != nil || fi.Size() == 0 {
		t.Fatalf("target SQLite file missing/empty after migrate: %v", err)
	}
	conn := advOpenConn(t, "sqlite", dst)
	advProbeCells(t, conn, "adv_corpus", cells, 1, false)
}

// advSQLiteNegZeroPremise is referenced by the corpus doc comment above:
// the -0.0 normalization claim is ground-truthed here (cheaply, in the
// same build) rather than asserted from memory — if a modernc/SQLite
// upgrade starts preserving the sign, this fails and the f8_negzero
// cell's want flips to "real#-0".
func TestMigrate_AdversarialCorpusSQLiteNegZeroPremise(t *testing.T) {
	path := filepath.Join(t.TempDir(), "negzero.db")
	db := advOpenSQLite(t, path)
	defer func() { _ = db.Close() }()
	advExecSQLite(t, db, `CREATE TABLE nz (id INTEGER PRIMARY KEY, r REAL)`)
	advExecSQLite(t, db, `INSERT INTO nz (id) VALUES (1)`)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, `UPDATE nz SET r = ? WHERE id = 1`, math.Copysign(0, -1)); err != nil {
		t.Fatalf("bind -0.0: %v", err)
	}
	var f float64
	if err := db.QueryRowContext(ctx, `SELECT r FROM nz WHERE id = 1`).Scan(&f); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if math.Signbit(f) {
		t.Fatal("SQLite now PRESERVES -0.0 — the corpus premise changed: " +
			"add an f8_negzero source cell to advCorpusSQLiteCells and flip " +
			"advCorpusPGToSQLite's f8_negzero want to \"real#-0\"")
	}
	if f != 0 {
		t.Fatalf("read back %v; want a zero", f)
	}
}
