//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Adversarial value-fidelity corpus, MySQL → Postgres direction. See
// adversarial_corpus_shared_integration_test.go for the corpus model
// and the out-of-scope enumeration.

package pipeline

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/sluicecode"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// advCorpusFloorM2P is the anti-vacuity floor for this direction:
// families / cells below this fail loudly before any probing.
const (
	advM2PMinFamilies = 10
	advM2PMinCells    = 45
)

// advCorpusMySQLToPG builds the MySQL-source corpus. A function (not a
// package var) because the blob/text cells derive seedVal + MD5 wants
// at build time.
func advCorpusMySQLToPG() []advCell {
	bigText := strings.Repeat("héllo wörld 🦀 snøw ", 1500) // ~40KB of multi-byte UTF-8
	bigBlob := advBlobPattern(2 << 20)                      // 2 MiB, NUL + 0xFF runs
	ffBlob := make([]byte, 256)
	for i := range ffBlob {
		ffBlob[i] = 0xFF
	}
	deepDoc, deepPGPath, _ := advDeepJSON(40)

	return []advCell{
		// ---- text ----
		{family: "text", col: "t_emoji", ddl: "VARCHAR(191) NOT NULL", lit: "'crab 🦀 café ✅'",
			probe: "%s", want: "crab 🦀 café ✅"},
		// Decomposed combining diaeresis (e + U+0308), 6 bytes — must
		// NOT be unicode-normalized to the 5-byte composed form.
		{family: "text", col: "t_combining", ddl: "VARCHAR(64) NOT NULL", lit: "'noe\u0308l'",
			probe: "%s || '#' || octet_length(%s)::text", want: "noe\u0308l#6"},
		{family: "text", col: "t_trailing", ddl: "VARCHAR(32) NOT NULL", lit: "'pad   '",
			probe: "'[' || %s || ']#' || length(%s)::text", want: "[pad   ]#6"},
		// ZWJ family sequence: 4 emoji joined by 3 U+200D = 25 bytes.
		{family: "text", col: "t_zwj", ddl: "VARCHAR(64) NOT NULL", lit: "'👨\u200d👩\u200d👧\u200d👦'",
			probe: "octet_length(%s)::text || '#' || %s", want: "25#👨\u200d👩\u200d👧\u200d👦"},
		{family: "text", col: "t_case", ddl: "VARCHAR(32) NOT NULL", lit: "'Straße'",
			probe: "%s", want: "Straße"},
		{family: "text", col: "t_big", ddl: "MEDIUMTEXT NULL", lit: "NULL", seedVal: bigText,
			probe: "md5(%s) || '#' || octet_length(%s)::text",
			want:  advMD5([]byte(bigText)) + "#" + advItoa(len(bigText))},

		// ---- decimal ----
		{family: "decimal", col: "dec_max", ddl: "DECIMAL(65,30) NOT NULL",
			lit:   "'99999999999999999999999999999999999.999999999999999999999999999999'",
			probe: "%s::text", want: "99999999999999999999999999999999999.999999999999999999999999999999"},
		{family: "decimal", col: "dec_negmax", ddl: "DECIMAL(65,30) NOT NULL",
			lit:   "'-12345678901234567890123456789012345.123456789012345678901234567890'",
			probe: "%s::text", want: "-12345678901234567890123456789012345.123456789012345678901234567890"},
		// "10" vs "9": bytewise order ≠ numeric order — the pair that
		// catches a codec quietly comparing/serializing digits as text.
		{family: "decimal", col: "dec_ten", ddl: "DECIMAL(2,0) NOT NULL", lit: "10", probe: "%s::text", want: "10"},
		{family: "decimal", col: "dec_nine", ddl: "DECIMAL(2,0) NOT NULL", lit: "9", probe: "%s::text", want: "9"},
		{family: "decimal", col: "dec_frac", ddl: "DECIMAL(10,5) NOT NULL", lit: "'-0.00001'",
			probe: "%s::text", want: "-0.00001"},

		// ---- float (bit-exact via PG float8send/float4send; the
		// expected bits come from Go's parse of the same literal) ----
		// NOTE the -0.0e0 float literal: MySQL types a bare -0.0 as
		// DECIMAL, which has no signed zero, so the SOURCE would hold
		// +0.0 before sluice ever read it (ground-truthed on mysql:8.0).
		{family: "float", col: "f8_negzero", ddl: "DOUBLE NOT NULL", lit: "-0.0e0",
			probe: "encode(float8send(%s),'hex')", want: "8000000000000000"},
		{family: "float", col: "f8_17sig", ddl: "DOUBLE NOT NULL", lit: "0.1234567890123456789",
			probe: "encode(float8send(%s),'hex')", want: advFloat64Bits("0.1234567890123456789")},
		{family: "float", col: "f8_max", ddl: "DOUBLE NOT NULL", lit: "1.7976931348623157e308",
			probe: "encode(float8send(%s),'hex')", want: advFloat64Bits("1.7976931348623157e308")},
		{family: "float", col: "f8_minnormal", ddl: "DOUBLE NOT NULL", lit: "2.2250738585072014e-308",
			probe: "encode(float8send(%s),'hex')", want: advFloat64Bits("2.2250738585072014e-308")},
		{family: "float", col: "f8_subnormal", ddl: "DOUBLE NOT NULL", lit: "5e-324",
			probe: "encode(float8send(%s),'hex')", want: advFloat64Bits("5e-324")},
		{family: "float", col: "f4_pi", ddl: "FLOAT NOT NULL", lit: "3.14159",
			probe: "encode(float4send(%s),'hex')", want: advFloat32Bits("3.14159")},

		// ---- integer ----
		{family: "integer", col: "i8_min", ddl: "TINYINT NOT NULL", lit: "-128", probe: "%s::text", want: "-128"},
		{family: "integer", col: "i8_max", ddl: "TINYINT NOT NULL", lit: "127", probe: "%s::text", want: "127"},
		{family: "integer", col: "i16_min", ddl: "SMALLINT NOT NULL", lit: "-32768", probe: "%s::text", want: "-32768"},
		{family: "integer", col: "i16_max", ddl: "SMALLINT NOT NULL", lit: "32767", probe: "%s::text", want: "32767"},
		{family: "integer", col: "i24_min", ddl: "MEDIUMINT NOT NULL", lit: "-8388608", probe: "%s::text", want: "-8388608"},
		{family: "integer", col: "i24_max", ddl: "MEDIUMINT NOT NULL", lit: "8388607", probe: "%s::text", want: "8388607"},
		{family: "integer", col: "i32_min", ddl: "INT NOT NULL", lit: "-2147483648", probe: "%s::text", want: "-2147483648"},
		{family: "integer", col: "i32_max", ddl: "INT NOT NULL", lit: "2147483647", probe: "%s::text", want: "2147483647"},
		{family: "integer", col: "i64_min", ddl: "BIGINT NOT NULL", lit: "-9223372036854775808", probe: "%s::text", want: "-9223372036854775808"},
		{family: "integer", col: "i64_max", ddl: "BIGINT NOT NULL", lit: "9223372036854775807", probe: "%s::text", want: "9223372036854775807"},
		{family: "integer", col: "u32_max", ddl: "INT UNSIGNED NOT NULL", lit: "4294967295", probe: "%s::text", want: "4294967295"},
		// The unsigned-64 ceiling that still fits int64 — clean cell;
		// the above-ceiling refusal lives in the refusal matrix.
		{family: "integer", col: "u64_atcap", ddl: "BIGINT UNSIGNED NOT NULL", lit: "9223372036854775807",
			probe: "%s::text", want: "9223372036854775807"},
		// ZEROFILL is display-only; the VALUE must land as 42, never
		// the zero-padded rendering.
		{family: "integer", col: "u_zerofill", ddl: "INT(8) UNSIGNED ZEROFILL NOT NULL", lit: "42",
			probe: "%s::text", want: "42"},
		{family: "integer", col: "y_max", ddl: "YEAR NOT NULL", lit: "2155", probe: "%s::text", want: "2155"},

		// ---- temporal (seeded under session time_zone='+00:00') ----
		{family: "temporal", col: "dt_frac", ddl: "DATETIME(6) NOT NULL", lit: "'2026-03-08 02:30:00.123456'",
			probe: "to_char(%s,'YYYY-MM-DD HH24:MI:SS.US')", want: "2026-03-08 02:30:00.123456"},
		{family: "temporal", col: "dt_min", ddl: "DATETIME NOT NULL", lit: "'1000-01-01 00:00:00'",
			probe: "to_char(%s,'YYYY-MM-DD HH24:MI:SS')", want: "1000-01-01 00:00:00"},
		{family: "temporal", col: "dt_max", ddl: "DATETIME(6) NOT NULL", lit: "'9999-12-31 23:59:59.999999'",
			probe: "to_char(%s,'YYYY-MM-DD HH24:MI:SS.US')", want: "9999-12-31 23:59:59.999999"},
		// The 2026 US spring-forward instant (10:30 UTC = inside the
		// 02:00–03:00 America/Los_Angeles local-time gap).
		{family: "temporal", col: "ts_dst", ddl: "TIMESTAMP(6) NOT NULL", lit: "'2026-03-08 10:30:00.123456'",
			probe: "to_char(%s AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SS.US')", want: "2026-03-08 10:30:00.123456"},
		// MySQL TIMESTAMP's floor instant.
		{family: "temporal", col: "ts_floor", ddl: "TIMESTAMP NOT NULL", lit: "'1970-01-01 00:00:01'",
			probe: "to_char(%s AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SS')", want: "1970-01-01 00:00:01"},
		{family: "temporal", col: "d_min", ddl: "DATE NOT NULL", lit: "'1000-01-01'", probe: "%s::text", want: "1000-01-01"},
		{family: "temporal", col: "d_max", ddl: "DATE NOT NULL", lit: "'9999-12-31'", probe: "%s::text", want: "9999-12-31"},
		{family: "temporal", col: "t_frac", ddl: "TIME(6) NOT NULL", lit: "'23:59:59.999999'",
			probe: "%s::text", want: "23:59:59.999999"},

		// ---- boolean (TINYINT(1) / BIT(1) conventions) ----
		{family: "boolean", col: "b_true", ddl: "TINYINT(1) NOT NULL", lit: "1", probe: "%s::text", want: "true"},
		{family: "boolean", col: "b_false", ddl: "TINYINT(1) NOT NULL", lit: "0", probe: "%s::text", want: "false"},
		{family: "boolean", col: "b_bit1", ddl: "BIT(1) NOT NULL", lit: "b'1'", probe: "%s::text", want: "true"},

		// ---- bit strings ----
		{family: "bit", col: "bit8", ddl: "BIT(8) NOT NULL", lit: "b'00001010'", probe: "%s::text", want: "00001010"},
		{family: "bit", col: "bit64", ddl: "BIT(64) NOT NULL", lit: "b'" + strings.Repeat("1", 64) + "'",
			probe: "%s::text", want: strings.Repeat("1", 64)},

		// ---- enum / set ----
		{family: "enumset", col: "e_first", ddl: "ENUM('alpha','beta','gamma') NOT NULL", lit: "'alpha'",
			probe: "%s::text", want: "alpha"},
		{family: "enumset", col: "e_last", ddl: "ENUM('alpha','beta','gamma') NOT NULL", lit: "'gamma'",
			probe: "%s::text", want: "gamma"},
		// Inserted out of declaration order; MySQL normalizes SET
		// storage to declaration order — 'a,b,c' is the source truth.
		{family: "enumset", col: "s_all", ddl: "SET('a','b','c') NOT NULL", lit: "'c,a,b'",
			probe: "%s::text", want: "{a,b,c}"},
		{family: "enumset", col: "s_empty", ddl: "SET('a','b','c') NOT NULL", lit: "''",
			probe: "%s::text", want: "{}"},

		// ---- json ----
		{family: "json", col: "j_unicode", ddl: "JSON NOT NULL", lit: `'{"ké":"vé","emoji":"🦀"}'`,
			probe: "(%s->>'ké') || '#' || (%s->>'emoji')", want: "vé#🦀"},
		// 2^53+1: JSON codecs that route numbers through float64
		// silently land …992. The exact digits are the assertion.
		{family: "json", col: "j_bigint", ddl: "JSON NOT NULL", lit: `'{"n":9007199254740993}'`,
			probe: "%s->>'n'", want: "9007199254740993"},
		{family: "json", col: "j_deep", ddl: "JSON NOT NULL", lit: "'" + deepDoc + "'",
			probe: "%s #>> '" + deepPGPath + "'", want: "1"},
		// MySQL 8.0 normalizes duplicate keys last-wins at INSERT; the
		// source truth is 2 and must stay 2.
		{family: "json", col: "j_dup", ddl: "JSON NOT NULL", lit: `'{"k":1,"k":2}'`,
			probe: "%s->>'k'", want: "2"},
		{family: "json", col: "j_frac", ddl: "JSON NOT NULL", lit: `'{"x":0.30000000000000004}'`,
			probe: "%s->>'x'", want: "0.30000000000000004"},

		// ---- binary ----
		{family: "binary", col: "vb_nul", ddl: "VARBINARY(16) NOT NULL", lit: "X'00FF7F00'",
			probe: "encode(%s,'hex') || '#' || octet_length(%s)::text", want: "00ff7f00#4"},
		// BINARY(8) zero-pads at the SOURCE; the padded form is the
		// faithful value.
		{family: "binary", col: "bin_pad", ddl: "BINARY(8) NOT NULL", lit: "X'DEAD'",
			probe: "encode(%s,'hex') || '#' || octet_length(%s)::text", want: "dead000000000000#8"},
		{family: "binary", col: "blob_ff", ddl: "BLOB NULL", lit: "NULL", seedVal: ffBlob,
			probe: "md5(%s) || '#' || octet_length(%s)::text", want: advMD5(ffBlob) + "#256"},
		{family: "binary", col: "blob_big", ddl: "MEDIUMBLOB NULL", lit: "NULL", seedVal: bigBlob,
			probe: "md5(%s) || '#' || octet_length(%s)::text", want: advMD5(bigBlob) + "#" + advItoa(len(bigBlob))},
	}
}

// advItoa keeps the corpus builders readable.
func advItoa(n int) string { return strconv.Itoa(n) }

// advSeedMySQLCorpus creates + seeds the corpus table on a MySQL
// source: session pinned to UTC, strict sql_mode left at the server
// default, seedVal cells applied via driver params.
func advSeedMySQLCorpus(t *testing.T, dsn, table string, cells []advCell, id int) {
	t.Helper()
	applyMySQLDDL(t, dsn, "SET SESSION time_zone = '+00:00';\n"+advBuildCreate(table, cells, true)+"\n"+advBuildInsert(table, cells, id))
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open mysql source: %v", err)
	}
	defer func() { _ = db.Close() }()
	advSeedParams(t, db, table, cells, id, false)
}

// advInsertMySQLCorpusRow inserts one more corpus row (CDC round).
func advInsertMySQLCorpusRow(t *testing.T, dsn, table string, cells []advCell, id int) {
	t.Helper()
	applyMySQLDDL(t, dsn, "SET SESSION time_zone = '+00:00';\n"+advBuildInsert(table, cells, id))
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open mysql source: %v", err)
	}
	defer func() { _ = db.Close() }()
	advSeedParams(t, db, table, cells, id, false)
}

// TestMigrate_AdversarialCorpus_MySQLToPostgres is the cold-copy round:
// seed the corpus on MySQL, migrate, run sluice verify (count+sample),
// then ground-truth every cell against the real PG target by direct
// SQL. Any silently altered value fails the cell's subtest loudly.
func TestMigrate_AdversarialCorpus_MySQLToPostgres(t *testing.T) {
	cells := advCorpusMySQLToPG()
	advAssertCorpusFloor(t, "mysql→pg", cells, advM2PMinFamilies, advM2PMinCells)

	mysqlSource, _, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()
	_, pgTarget, pgCleanup := startPostgres(t)
	defer pgCleanup()

	advSeedMySQLCorpus(t, mysqlSource, "adv_corpus", cells, 1)

	mysqlEng, _ := engines.Get("mysql")
	pgEng, _ := engines.Get("postgres")
	mig := &Migrator{
		Source: mysqlEng, Target: pgEng,
		SourceDSN: mysqlSource, TargetDSN: pgTarget,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run (adversarial corpus must migrate cleanly): %v", err)
	}

	advRunVerify(t, &Verifier{
		Source: mysqlEng, Target: pgEng,
		SourceDSN: mysqlSource, TargetDSN: pgTarget,
	})

	conn := advOpenConn(t, "pgx", pgTarget)
	advProbeCells(t, conn, "adv_corpus", cells, 1, true)
}

// advM2PRefusal is one must-refuse-loudly cell: a value with no
// faithful PG representation (or one whose faithful path requires an
// explicit operator override). Landing ANYTHING silently is the
// headline failure.
type advM2PRefusal struct {
	name string
	// seed runs against the MySQL source (its own table per cell).
	seed string
	// table is the cell's table name (used for the Filter + the
	// target-empty assertion).
	table string
	// code is the expected sluicecode (empty = no code pinned yet).
	code sluicecode.Code
	// alt are accepted error-text substrings when no coded error is
	// found in the chain (loud-but-uncoded refusals).
	alt []string
}

// TestMigrate_AdversarialCorpusRefusals_MySQLToPostgres pins the
// must-refuse cells: out-of-{0,1} TINYINT(1), NUL bytes in text,
// BIGINT UNSIGNED above int64, zero dates, and >24h TIME durations.
// Each cell migrates in isolation (Filter) and must fail loudly with
// ZERO rows landed — an exit-0 with an altered value is the worst
// outcome this suite exists to catch.
func TestMigrate_AdversarialCorpusRefusals_MySQLToPostgres(t *testing.T) {
	mysqlSource, _, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()
	_, pgTarget, pgCleanup := startPostgres(t)
	defer pgCleanup()

	mysqlEng, _ := engines.Get("mysql")
	pgEng, _ := engines.Get("postgres")

	refusals := []advM2PRefusal{
		{
			name:  "tinyint1_out_of_range",
			table: "r_tinyint1",
			seed: `CREATE TABLE r_tinyint1 (id INT PRIMARY KEY, v TINYINT(1) NOT NULL);
			       INSERT INTO r_tinyint1 VALUES (1, 5);`,
			code: sluicecode.CodeValueTinyint1Range,
		},
		{
			name:  "nul_byte_in_text",
			table: "r_nul",
			seed: `CREATE TABLE r_nul (id INT PRIMARY KEY, v VARCHAR(20) NOT NULL);
			       INSERT INTO r_nul VALUES (1, CONCAT('a', CHAR(0), 'b'));`,
			code: sluicecode.CodeValueNULByte,
		},
		{
			name:  "bigint_unsigned_above_int64",
			table: "r_u64",
			seed: `CREATE TABLE r_u64 (id INT PRIMARY KEY, v BIGINT UNSIGNED NOT NULL);
			       INSERT INTO r_u64 VALUES (1, 18446744073709551615);`,
			// The COPY-encode refusal names the exact value and the
			// int64 ceiling; the decimal(20,0) override guidance is
			// printed as a schema-preflight NOTICE alongside it.
			alt: []string{"maximum value for int64", "18446744073709551615", "decimal(20,0)"},
		},
		{
			name:  "zero_date_default_policy",
			table: "r_zerodate",
			seed: `SET SESSION sql_mode='';
			       CREATE TABLE r_zerodate (id INT PRIMARY KEY, v DATE NOT NULL);
			       INSERT INTO r_zerodate VALUES (1, '0000-00-00');`,
			code: sluicecode.CodeValueZeroDate,
		},
		{
			name:  "time_duration_beyond_24h",
			table: "r_timedur",
			seed: `CREATE TABLE r_timedur (id INT PRIMARY KEY, v TIME NOT NULL);
			       INSERT INTO r_timedur VALUES (1, '838:59:59');`,
			alt: []string{"838", "time", "interval"},
		},
		{
			name:  "time_duration_negative",
			table: "r_timeneg",
			seed: `CREATE TABLE r_timeneg (id INT PRIMARY KEY, v TIME NOT NULL);
			       INSERT INTO r_timeneg VALUES (1, '-12:30:00');`,
			alt: []string{"-12:30", "time", "interval"},
		},
	}
	if len(refusals) < 4 {
		t.Fatalf("anti-vacuity floor: refusal matrix has %d cells; floor is 4", len(refusals))
	}

	for _, rc := range refusals {
		rc := rc
		t.Run(rc.name, func(t *testing.T) {
			applyMySQLDDL(t, mysqlSource, rc.seed)

			filter, err := migcore.NewTableFilter([]string{rc.table}, nil)
			if err != nil {
				t.Fatalf("NewTableFilter: %v", err)
			}
			mig := &Migrator{
				Source: mysqlEng, Target: pgEng,
				SourceDSN: mysqlSource, TargetDSN: pgTarget,
				Filter: filter,
				// Distinct per cell: a refusal records a partial-
				// migration ledger row on the target, and the next
				// cell's run would otherwise be told to --resume it.
				MigrationID: "adv-refusal-" + rc.table,
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()
			err = mig.Run(ctx)
			if err == nil {
				// EXIT-0 PATH: the value either landed altered (the
				// headline) or faithfully (a mapping change this pin
				// must be updated for — loudly, not silently).
				dumpTargetCell(t, pgTarget, rc.table)
				t.Fatalf("HEADLINE: migrate of %s exited CLEAN — a value with no faithful PG representation "+
					"landed on an exit-0 path (silent loss/alteration). See dumped target value above.", rc.name)
			}
			advAssertRefusal(t, err, rc.code, rc.alt)

			// The refusal must have fired before the bad row was
			// written: zero rows on the target (table may or may not
			// exist depending on how early the refusal fired).
			if n, ok := advCountIfExists(t, pgTarget, rc.table); ok && n != 0 {
				t.Errorf("refusal %s left %d row(s) on the target; want 0 (refusal must precede the write)", rc.name, n)
			}
			t.Logf("refusal %s: %v", rc.name, err)
		})
	}

	// Zero-date epoch mode is the sanctioned carry path: the same
	// zero-date value under --zero-date=epoch lands as the documented
	// 1970-01-01 00:00:01 sentinel (date: 1970-01-01), loudly chosen,
	// never guessed.
	t.Run("zero_date_epoch_mode", func(t *testing.T) {
		applyMySQLDDL(t, mysqlSource, `SET SESSION sql_mode='';
			CREATE TABLE r_zerodate_epoch (id INT PRIMARY KEY, d DATE NOT NULL, dt DATETIME NOT NULL);
			INSERT INTO r_zerodate_epoch VALUES (1, '0000-00-00', '0000-00-00 00:00:00');`)

		zdc, ok := mysqlEng.(interface {
			WithZeroDate(string) (ir.Engine, error)
		})
		if !ok {
			t.Fatal("mysql engine does not expose WithZeroDate")
		}
		epochEng, err := zdc.WithZeroDate("epoch")
		if err != nil {
			t.Fatalf("WithZeroDate(epoch): %v", err)
		}

		filter, err := migcore.NewTableFilter([]string{"r_zerodate_epoch"}, nil)
		if err != nil {
			t.Fatalf("NewTableFilter: %v", err)
		}
		mig := &Migrator{
			Source: epochEng, Target: pgEng,
			SourceDSN: mysqlSource, TargetDSN: pgTarget,
			Filter:      filter,
			MigrationID: "adv-refusal-zerodate-epoch",
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		if err := mig.Run(ctx); err != nil {
			t.Fatalf("Migrator.Run (--zero-date=epoch): %v", err)
		}

		conn := advOpenConn(t, "pgx", pgTarget)
		var d, dt string
		qctx, qcancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer qcancel()
		if err := conn.QueryRowContext(qctx,
			`SELECT d::text, to_char(dt,'YYYY-MM-DD HH24:MI:SS') FROM r_zerodate_epoch WHERE id = 1`).Scan(&d, &dt); err != nil {
			t.Fatalf("probe epoch sentinels: %v", err)
		}
		if d != "1970-01-01" {
			t.Errorf("zero DATE under epoch mode = %q; want 1970-01-01", d)
		}
		if dt != "1970-01-01 00:00:01" {
			t.Errorf("zero DATETIME under epoch mode = %q; want 1970-01-01 00:00:01 (the documented sentinel)", dt)
		}
	})
}

// TestMigrate_AdversarialCorpusBackupRound_MySQLToPostgres pushes the
// corpus through the backup snapshot codec: full backup of the MySQL
// source → cross-engine restore into a fresh PG target → the same
// direct-SQL ground truth. The backup archive is a serialization
// boundary (a codec per the new-surface checklist) distinct from both
// the bulk-copy and CDC paths.
func TestMigrate_AdversarialCorpusBackupRound_MySQLToPostgres(t *testing.T) {
	cells := advCorpusMySQLToPG()
	advAssertCorpusFloor(t, "mysql→pg backup", cells, advM2PMinFamilies, advM2PMinCells)

	mysqlSource, _, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()
	_, pgTarget, pgCleanup := startPostgres(t)
	defer pgCleanup()

	advSeedMySQLCorpus(t, mysqlSource, "adv_corpus", cells, 1)

	mysqlEng, _ := engines.Get("mysql")
	pgEng, _ := engines.Get("postgres")

	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if err := (&backup.Backup{
		Source: mysqlEng, SourceDSN: mysqlSource, Store: store, SluiceVersion: "test",
	}).Run(ctx); err != nil {
		t.Fatalf("Backup.Run: %v", err)
	}
	if err := (&backup.Restore{
		Target: pgEng, TargetDSN: pgTarget, Store: store,
	}).Run(ctx); err != nil {
		t.Fatalf("Restore.Run (MySQL corpus → PG): %v", err)
	}

	conn := advOpenConn(t, "pgx", pgTarget)
	advProbeCells(t, conn, "adv_corpus", cells, 1, true)
}

// advAssertRefusal requires err to carry the expected sluicecode (any
// CodedError in the unwrap chain), or — when no code is pinned — one of
// the accepted substrings. Fails with the full error text either way so
// the report shows the operator-visible message.
func advAssertRefusal(t *testing.T, err error, code sluicecode.Code, alt []string) {
	t.Helper()
	if code != "" {
		// Walk the whole unwrap chain: an outer wrapper (e.g. the
		// bulk-copy table-failed code) may enclose the value refusal,
		// and errors.As alone would stop at the first CodedError.
		for e := err; e != nil; e = errors.Unwrap(e) {
			if ce, ok := e.(*sluicecode.CodedError); ok && ce.Code == code {
				return
			}
		}
		// Fall back to the code string appearing in text.
		if strings.Contains(err.Error(), string(code)) {
			return
		}
		t.Fatalf("refusal error does not carry %s:\n%v", code, err)
	}
	msg := strings.ToLower(err.Error())
	for _, a := range alt {
		if strings.Contains(msg, strings.ToLower(a)) {
			return
		}
	}
	t.Fatalf("refusal error names none of %v (operators need an actionable hint):\n%v", alt, err)
}

// advCountIfExists returns the row count of table on the PG target, or
// ok=false when the table does not exist.
func advCountIfExists(t *testing.T, dsn, table string) (int, bool) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open pg: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var exists bool
	if err := db.QueryRowContext(ctx,
		`SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = $1)`, table).Scan(&exists); err != nil {
		t.Fatalf("probe table existence: %v", err)
	}
	if !exists {
		return 0, false
	}
	var n int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n, true
}

// dumpTargetCell logs whatever landed for a refusal table so an exit-0
// failure names the corrupt value in the test output.
func dumpTargetCell(t *testing.T, dsn, table string) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	rows, err := db.QueryContext(ctx, "SELECT id, v::text FROM "+table+" ORDER BY id")
	if err != nil {
		t.Logf("dump %s: %v", table, err)
		return
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var id int
		var v sql.NullString
		if err := rows.Scan(&id, &v); err == nil {
			t.Logf("  target %s: id=%d v=%q", table, id, v.String)
		}
	}
	_ = rows.Err()
}
