//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Adversarial value-fidelity corpus, Postgres → MySQL direction. See
// adversarial_corpus_shared_integration_test.go for the corpus model
// and the out-of-scope enumeration.

package pipeline

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/sluicecode"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

const (
	advP2MMinFamilies = 12
	advP2MMinCells    = 38
)

// advMySQLUTCSession pins the MySQL probe session to UTC so TIMESTAMP
// renderings are deterministic.
var advMySQLUTCSession = []string{"SET SESSION time_zone = '+00:00'"}

// advCorpusPGToMySQL builds the PG-source corpus. The enum column type
// (adv_mood) is created by advSeedPGCorpus.
func advCorpusPGToMySQL() []advCell {
	bigText := strings.Repeat("héllo wörld 🦀 snøw ", 1500)
	bigBytes := advBlobPattern(2 << 20)
	deepDoc, _, deepMySQLPath := advDeepJSON(40)

	return []advCell{
		// ---- text ----
		{family: "text", col: "t_emoji", ddl: "VARCHAR(191) NOT NULL", lit: "'crab 🦀 café ✅'",
			probe: "%s", want: "crab 🦀 café ✅"},
		{family: "text", col: "t_combining", ddl: "VARCHAR(64) NOT NULL", lit: "'noël'",
			probe: "CONCAT(%s,'#',LENGTH(%s))", want: "noël#6"},
		{family: "text", col: "t_trailing", ddl: "VARCHAR(32) NOT NULL", lit: "'pad   '",
			probe: "CONCAT('[',%s,']#',CHAR_LENGTH(%s))", want: "[pad   ]#6"},
		{family: "text", col: "t_zwj", ddl: "VARCHAR(64) NOT NULL", lit: "'👨‍👩‍👧‍👦'",
			probe: "CONCAT(LENGTH(%s),'#',%s)", want: "25#👨‍👩‍👧‍👦"},
		{family: "text", col: "t_big", ddl: "TEXT NULL", lit: "NULL", seedVal: bigText,
			probe: "CONCAT(MD5(%s),'#',LENGTH(%s))",
			want:  advMD5([]byte(bigText)) + "#" + advItoa(len(bigText))},

		// ---- decimal ----
		{family: "decimal", col: "dec_max", ddl: "NUMERIC(65,30) NOT NULL",
			lit:   "'99999999999999999999999999999999999.999999999999999999999999999999'",
			probe: "CAST(%s AS CHAR)", want: "99999999999999999999999999999999999.999999999999999999999999999999"},
		{family: "decimal", col: "dec_negmax", ddl: "NUMERIC(65,30) NOT NULL",
			lit:   "'-12345678901234567890123456789012345.123456789012345678901234567890'",
			probe: "CAST(%s AS CHAR)", want: "-12345678901234567890123456789012345.123456789012345678901234567890"},
		{family: "decimal", col: "dec_ten", ddl: "NUMERIC(2,0) NOT NULL", lit: "10", probe: "CAST(%s AS CHAR)", want: "10"},
		{family: "decimal", col: "dec_nine", ddl: "NUMERIC(2,0) NOT NULL", lit: "9", probe: "CAST(%s AS CHAR)", want: "9"},
		// Constrained scale renders padded on BOTH engines ('1.5000') —
		// the padding is the declared scale, not an alteration.
		{family: "decimal", col: "dec_pad", ddl: "NUMERIC(10,4) NOT NULL", lit: "1.5",
			probe: "CAST(%s AS CHAR)", want: "1.5000"},

		// ---- float (MySQL 8 renders shortest-round-trip; compare
		// IEEE bits after a Go parse of both sides) ----
		{family: "float", col: "f8_negzero", ddl: "DOUBLE PRECISION NOT NULL", lit: "'-0'::float8",
			probe: "CONCAT(%s)", want: "-0", cmp: advCmpFloat64},
		{family: "float", col: "f8_17sig", ddl: "DOUBLE PRECISION NOT NULL", lit: "0.1234567890123456789",
			probe: "CONCAT(%s)", want: "0.1234567890123456789", cmp: advCmpFloat64},
		{family: "float", col: "f8_max", ddl: "DOUBLE PRECISION NOT NULL", lit: "1.7976931348623157e308",
			probe: "CONCAT(%s)", want: "1.7976931348623157e308", cmp: advCmpFloat64},
		// The subnormal floor: silent flush-to-zero anywhere in the
		// pipe (or in the target session) shows as bits 0000….
		{family: "float", col: "f8_subnormal", ddl: "DOUBLE PRECISION NOT NULL", lit: "5e-324",
			probe: "CONCAT(%s)", want: "5e-324", cmp: advCmpFloat64},
		{family: "float", col: "f4_pi", ddl: "REAL NOT NULL", lit: "3.14159",
			probe: "CONCAT(%s)", want: "3.14159", cmp: advCmpFloat32},

		// ---- integer ----
		{family: "integer", col: "i16_min", ddl: "SMALLINT NOT NULL", lit: "-32768", probe: "CAST(%s AS CHAR)", want: "-32768"},
		{family: "integer", col: "i16_max", ddl: "SMALLINT NOT NULL", lit: "32767", probe: "CAST(%s AS CHAR)", want: "32767"},
		{family: "integer", col: "i32_min", ddl: "INTEGER NOT NULL", lit: "-2147483648", probe: "CAST(%s AS CHAR)", want: "-2147483648"},
		{family: "integer", col: "i32_max", ddl: "INTEGER NOT NULL", lit: "2147483647", probe: "CAST(%s AS CHAR)", want: "2147483647"},
		{family: "integer", col: "i64_min", ddl: "BIGINT NOT NULL", lit: "-9223372036854775808", probe: "CAST(%s AS CHAR)", want: "-9223372036854775808"},
		{family: "integer", col: "i64_max", ddl: "BIGINT NOT NULL", lit: "9223372036854775807", probe: "CAST(%s AS CHAR)", want: "9223372036854775807"},

		// ---- temporal ----
		// The 2026 US spring-forward instant; MySQL TIMESTAMP stores
		// UTC — probed under a pinned UTC session.
		{family: "temporal", col: "tstz_dst", ddl: "TIMESTAMPTZ(6) NOT NULL",
			lit:   "'2026-03-08 10:30:00.123456+00'",
			probe: "CAST(%s AS CHAR)", want: "2026-03-08 10:30:00.123456"},
		{family: "temporal", col: "ts_naive", ddl: "TIMESTAMP(6) NOT NULL",
			lit:   "'2026-03-08 02:30:00.123456'",
			probe: "CAST(%s AS CHAR)", want: "2026-03-08 02:30:00.123456"},
		{family: "temporal", col: "d_floor", ddl: "DATE NOT NULL", lit: "'1000-01-01'",
			probe: "CAST(%s AS CHAR)", want: "1000-01-01"},
		// Go's zero time.Time IS these two values (PG DATE '0001-01-01'
		// / timestamp '0001-01-01 00:00:00' decode to exactly
		// time.Time{}), and go-sql-driver serializes any IsZero() time
		// as MySQL's '0000-00-00' sentinel — the corpus's first
		// production catch (2026-08-22): a false refusal under the
		// strict writer session, a SILENT rewrite under a relaxed
		// --mysql-sql-mode. prepareValue now string-encodes the zero
		// instant; these cells pin the faithful landing end-to-end on
		// the cold-copy, CDC, and backup rounds.
		{family: "temporal", col: "d_year1", ddl: "DATE NOT NULL", lit: "'0001-01-01'",
			probe: "CAST(%s AS CHAR)", want: "0001-01-01"},
		{family: "temporal", col: "ts_year1", ddl: "TIMESTAMP(0) NOT NULL", lit: "'0001-01-01 00:00:00'",
			probe: "CAST(%s AS CHAR)", want: "0001-01-01 00:00:00"},
		{family: "temporal", col: "d_max", ddl: "DATE NOT NULL", lit: "'9999-12-31'",
			probe: "CAST(%s AS CHAR)", want: "9999-12-31"},
		{family: "temporal", col: "t_frac", ddl: "TIME(6) NOT NULL", lit: "'23:59:59.999999'",
			probe: "CAST(%s AS CHAR)", want: "23:59:59.999999"},
		// PG allows the 24:00:00 midnight-of-next-day literal; MySQL
		// TIME holds it as a duration — faithful either way. TIME(0)
		// so the target renders without a fractional tail (an
		// unspecified-precision PG time maps to MySQL TIME(6), whose
		// '.000000' rendering is declared scale, not alteration).
		{family: "temporal", col: "t_24", ddl: "TIME(0) NOT NULL", lit: "'24:00:00'",
			probe: "CAST(%s AS CHAR)", want: "24:00:00"},

		// ---- boolean ----
		{family: "boolean", col: "b_true", ddl: "BOOLEAN NOT NULL", lit: "true", probe: "CONCAT(%s)", want: "1"},
		{family: "boolean", col: "b_false", ddl: "BOOLEAN NOT NULL", lit: "false", probe: "CONCAT(%s)", want: "0"},

		// ---- bit strings ----
		{family: "bit", col: "bit8", ddl: "BIT(8) NOT NULL", lit: "B'00001010'",
			probe: "LPAD(BIN(%s+0),8,'0')", want: "00001010"},
		// bit varying lands as fixed BIT(12), zero-extended (catalog
		// Bug 75); BIN(+0) round-trips the value faithfully.
		{family: "bit", col: "vbit", ddl: "BIT VARYING(12) NOT NULL", lit: "B'101'",
			probe: "BIN(%s+0)", want: "101"},

		// ---- enum (PG enum type → MySQL ENUM) ----
		{family: "enumset", col: "e_first", ddl: "adv_mood NOT NULL", lit: "'alpha'",
			probe: "CONCAT(%s)", want: "alpha"},
		{family: "enumset", col: "e_last", ddl: "adv_mood NOT NULL", lit: "'gamma'",
			probe: "CONCAT(%s)", want: "gamma"},

		// ---- uuid ----
		{family: "uuid", col: "u_canon", ddl: "UUID NOT NULL", lit: "'01234567-89ab-cdef-0123-456789abcdef'",
			probe: "CONCAT(%s)", want: "01234567-89ab-cdef-0123-456789abcdef"},
		// PG canonicalizes an uppercase literal to lowercase at INSERT;
		// lowercase is the source truth (the ir.UUID contract).
		{family: "uuid", col: "u_upper", ddl: "UUID NOT NULL", lit: "'ABCDEF01-2345-6789-ABCD-EF0123456789'",
			probe: "CONCAT(%s)", want: "abcdef01-2345-6789-abcd-ef0123456789"},

		// ---- inet / cidr / macaddr ----
		{family: "net", col: "inet4", ddl: "INET NOT NULL", lit: "'192.168.1.1'",
			probe: "CONCAT(%s)", want: "192.168.1.1"},
		{family: "net", col: "inet6", ddl: "INET NOT NULL", lit: "'2001:db8::1'",
			probe: "CONCAT(%s)", want: "2001:db8::1"},
		{family: "net", col: "cidr16", ddl: "CIDR NOT NULL", lit: "'10.1.0.0/16'",
			probe: "CONCAT(%s)", want: "10.1.0.0/16"},
		{family: "net", col: "mac6", ddl: "MACADDR NOT NULL", lit: "'08:00:2b:01:02:03'",
			probe: "CONCAT(%s)", want: "08:00:2b:01:02:03"},
		// PG itself widens an EUI-48 literal to EUI-64 on macaddr8
		// input — the widened form is the source truth.
		{family: "net", col: "mac8", ddl: "MACADDR8 NOT NULL", lit: "'08:00:2b:01:02:03'",
			probe: "CONCAT(%s)", want: "08:00:2b:ff:fe:01:02:03"},

		// ---- arrays (→ MySQL JSON; structure + elements probed via
		// JSON functions — dims preserved as nesting, the Bug-74
		// numeric[][] family included) ----
		{family: "array", col: "arr_int_2d", ddl: "INT[][] NOT NULL", lit: "'{{1,2},{3,4}}'",
			probe: "CONCAT(JSON_LENGTH(%s),'#',JSON_LENGTH(%s,'$[0]'),'#',JSON_UNQUOTE(JSON_EXTRACT(%s,'$[1][1]')))",
			want:  "2#2#4"},
		{family: "array", col: "arr_num_2d", ddl: "NUMERIC[][] NOT NULL", lit: "'{{1.5,2.25},{3.125,4.0625}}'",
			probe: "CONCAT(JSON_LENGTH(%s),'#',JSON_LENGTH(%s,'$[0]'),'#',JSON_UNQUOTE(JSON_EXTRACT(%s,'$[1][0]')))",
			want:  "2#2#3.125"},
		{family: "array", col: "arr_int_3d", ddl: "INT[][][] NOT NULL", lit: "'{{{1,2},{3,4}},{{5,6},{7,8}}}'",
			probe: "CONCAT(JSON_LENGTH(%s),'#',JSON_LENGTH(%s,'$[0]'),'#',JSON_LENGTH(%s,'$[0][0]'),'#',JSON_UNQUOTE(JSON_EXTRACT(%s,'$[1][1][1]')))",
			want:  "2#2#2#8"},
		{family: "array", col: "arr_null", ddl: "INT[] NOT NULL", lit: "'{1,NULL,3}'",
			probe: "CONCAT(JSON_LENGTH(%s),'#',JSON_EXTRACT(%s,'$[1]'))", want: "3#null"},
		{family: "array", col: "arr_empty", ddl: "INT[] NOT NULL", lit: "'{}'",
			probe: "CAST(JSON_LENGTH(%s) AS CHAR)", want: "0"},
		// Elements that stress the array-literal lexer: commas, double
		// quotes, braces, emoji.
		{family: "array", col: "arr_text", ddl: "TEXT[] NOT NULL",
			lit:   `ARRAY['a,b','c"d','{e}','🦀']`,
			probe: "CONCAT(JSON_LENGTH(%s),'#',JSON_UNQUOTE(JSON_EXTRACT(%s,'$[1]')),'#',JSON_UNQUOTE(JSON_EXTRACT(%s,'$[2]')))",
			want:  `4#c"d#{e}`},

		// ---- binary ----
		{family: "binary", col: "ba_nul", ddl: "BYTEA NOT NULL", lit: `'\x00ff7f00'`,
			probe: "CONCAT(HEX(%s),'#',OCTET_LENGTH(%s))", want: "00FF7F00#4"},
		{family: "binary", col: "ba_big", ddl: "BYTEA NULL", lit: "NULL", seedVal: bigBytes,
			probe: "CONCAT(MD5(%s),'#',OCTET_LENGTH(%s))", want: advMD5(bigBytes) + "#" + advItoa(len(bigBytes))},

		// ---- json ----
		{family: "json", col: "jb_unicode", ddl: "JSONB NOT NULL", lit: `'{"ké":"vé","emoji":"🦀"}'`,
			probe: `CONCAT(JSON_UNQUOTE(JSON_EXTRACT(%s,'$."ké"')),'#',JSON_UNQUOTE(JSON_EXTRACT(%s,'$.emoji')))`,
			want:  "vé#🦀"},
		{family: "json", col: "jb_bigint", ddl: "JSONB NOT NULL", lit: `'{"n":9007199254740993}'`,
			probe: "JSON_EXTRACT(%s,'$.n')", want: "9007199254740993"},
		{family: "json", col: "jb_deep", ddl: "JSONB NOT NULL", lit: "'" + deepDoc + "'",
			probe: "JSON_EXTRACT(%s,'" + deepMySQLPath + "')", want: "1"},
	}
}

// advSeedPGCorpus creates + seeds the corpus table on a PG source.
func advSeedPGCorpus(t *testing.T, dsn, table string, cells []advCell, id int) {
	t.Helper()
	applyPGDDL(t, dsn, `DO $$ BEGIN
		IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'adv_mood') THEN
			CREATE TYPE adv_mood AS ENUM ('alpha','beta','gamma');
		END IF;
	END $$;`+"\n"+advBuildCreate(table, cells, false)+"\n"+advBuildInsert(table, cells, id))
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open pg source: %v", err)
	}
	defer func() { _ = db.Close() }()
	advSeedParams(t, db, table, cells, id, true)
}

// advInsertPGCorpusRow inserts one more corpus row (CDC round).
func advInsertPGCorpusRow(t *testing.T, dsn, table string, cells []advCell, id int) {
	t.Helper()
	applyPGDDL(t, dsn, advBuildInsert(table, cells, id))
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open pg source: %v", err)
	}
	defer func() { _ = db.Close() }()
	advSeedParams(t, db, table, cells, id, true)
}

// TestMigrate_AdversarialCorpus_PostgresToMySQL is the cold-copy round
// for the PG-source direction: migrate, sluice verify, then per-cell
// ground truth against the real MySQL target by direct SQL.
func TestMigrate_AdversarialCorpus_PostgresToMySQL(t *testing.T) {
	cells := advCorpusPGToMySQL()
	advAssertCorpusFloor(t, "pg→mysql", cells, advP2MMinFamilies, advP2MMinCells)

	pgSource, _, pgCleanup := startPostgres(t)
	defer pgCleanup()
	_, mysqlTarget, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()

	advSeedPGCorpus(t, pgSource, "adv_corpus", cells, 1)

	pgEng, _ := engines.Get("postgres")
	mysqlEng, _ := engines.Get("mysql")
	mig := &Migrator{
		Source: pgEng, Target: mysqlEng,
		SourceDSN: pgSource, TargetDSN: mysqlTarget,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run (adversarial corpus must migrate cleanly): %v", err)
	}

	advRunVerify(t, &Verifier{
		Source: pgEng, Target: mysqlEng,
		SourceDSN: pgSource, TargetDSN: mysqlTarget,
	})

	conn := advOpenConn(t, "mysql", mysqlTarget, advMySQLUTCSession...)
	advProbeCells(t, conn, "adv_corpus", cells, 1, false)
}

// advP2MRefusal is a must-not-land-wrong cell for the PG→MySQL
// direction. When faithfulWant is non-empty an exit-0 migrate is
// acceptable ONLY if the probe returns exactly that value (the "MySQL
// happens to hold it" case); anything else on an exit-0 path is the
// silent-loss headline.
type advP2MRefusal struct {
	name         string
	table        string
	seed         string
	code         sluicecode.Code
	alt          []string
	faithfulCol  string
	faithfulWant string
}

// TestMigrate_AdversarialCorpusRefusals_PostgresToMySQL pins the
// values with no (or uncertain) MySQL representation: float NaN/±Inf
// (SLUICE-E-VALUE-UNREPRESENTABLE), numeric NaN, INTERVAL columns, and
// the below-floor DATE / pre-1970 TIMESTAMPTZ boundary cells that must
// either refuse loudly or land byte-faithfully — never clamp.
func TestMigrate_AdversarialCorpusRefusals_PostgresToMySQL(t *testing.T) {
	pgSource, _, pgCleanup := startPostgres(t)
	defer pgCleanup()
	_, mysqlTarget, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()

	pgEng, _ := engines.Get("postgres")
	mysqlEng, _ := engines.Get("mysql")

	refusals := []advP2MRefusal{
		{
			name: "float_nan", table: "r_nan",
			seed: `CREATE TABLE r_nan (id INT PRIMARY KEY, v DOUBLE PRECISION NOT NULL);
			       INSERT INTO r_nan VALUES (1, 'NaN');`,
			code: sluicecode.CodeValueUnrepresentable,
		},
		{
			name: "float_pos_infinity", table: "r_pinf",
			seed: `CREATE TABLE r_pinf (id INT PRIMARY KEY, v DOUBLE PRECISION NOT NULL);
			       INSERT INTO r_pinf VALUES (1, 'Infinity');`,
			code: sluicecode.CodeValueUnrepresentable,
		},
		{
			name: "float_neg_infinity", table: "r_ninf",
			seed: `CREATE TABLE r_ninf (id INT PRIMARY KEY, v DOUBLE PRECISION NOT NULL);
			       INSERT INTO r_ninf VALUES (1, '-Infinity');`,
			code: sluicecode.CodeValueUnrepresentable,
		},
		{
			name: "numeric_nan", table: "r_numnan",
			seed: `CREATE TABLE r_numnan (id INT PRIMARY KEY, v NUMERIC(10,2) NOT NULL);
			       INSERT INTO r_numnan VALUES (1, 'NaN');`,
			alt: []string{"nan", "unrepresentable", "numeric"},
		},
		{
			name: "interval_column", table: "r_interval",
			seed: `CREATE TABLE r_interval (id INT PRIMARY KEY, v INTERVAL NOT NULL);
			       INSERT INTO r_interval VALUES (1, INTERVAL '1 day 02:03:04');`,
			alt: []string{"interval", "no mysql equivalent", "duration"},
		},
		// Boundary cell: refusing loudly OR landing byte-faithfully
		// are both acceptable; clamping/wrapping is the headline.
		// (The '0001-01-01' zero-time boundary graduated to the main
		// corpus — d_year1 / ts_year1 — once the prepareValue string-
		// encoding fix made it land faithfully.)
		{
			name: "timestamptz_pre_epoch", table: "r_preepoch",
			seed: `CREATE TABLE r_preepoch (id INT PRIMARY KEY, v TIMESTAMPTZ NOT NULL);
			       INSERT INTO r_preepoch VALUES (1, '1969-12-31 23:59:59+00');`,
			alt:         []string{"1969", "timestamp", "range"},
			faithfulCol: "v", faithfulWant: "1969-12-31 23:59:59",
		},
	}
	if len(refusals) < 5 {
		t.Fatalf("anti-vacuity floor: refusal matrix has %d cells; floor is 5", len(refusals))
	}

	for _, rc := range refusals {
		rc := rc
		t.Run(rc.name, func(t *testing.T) {
			applyPGDDL(t, pgSource, rc.seed)

			filter, err := migcore.NewTableFilter([]string{rc.table}, nil)
			if err != nil {
				t.Fatalf("NewTableFilter: %v", err)
			}
			mig := &Migrator{
				Source: pgEng, Target: mysqlEng,
				SourceDSN: pgSource, TargetDSN: mysqlTarget,
				Filter: filter,
				// Distinct per cell — see the M2P refusal harness note.
				MigrationID: "adv-refusal-" + rc.table,
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()
			err = mig.Run(ctx)
			if err == nil {
				if rc.faithfulWant == "" {
					dumpMySQLTargetCell(t, mysqlTarget, rc.table)
					t.Fatalf("HEADLINE: migrate of %s exited CLEAN — a value with no faithful MySQL representation "+
						"landed on an exit-0 path (silent loss/alteration). See dumped target value above.", rc.name)
				}
				// Boundary cell: exit-0 is fine ONLY byte-faithfully.
				conn := advOpenConn(t, "mysql", mysqlTarget, advMySQLUTCSession...)
				var got sql.NullString
				qctx, qcancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer qcancel()
				q := "SELECT CAST(" + rc.faithfulCol + " AS CHAR) FROM " + rc.table + " WHERE id = 1"
				if err := conn.QueryRowContext(qctx, q).Scan(&got); err != nil {
					t.Fatalf("probe %s after exit-0: %v", rc.name, err)
				}
				if !got.Valid || got.String != rc.faithfulWant {
					t.Fatalf("HEADLINE: boundary value %s landed ALTERED on an exit-0 path: target holds %q; want %q or a loud refusal",
						rc.name, got.String, rc.faithfulWant)
				}
				t.Logf("boundary %s: landed byte-faithfully (%q) — acceptable", rc.name, got.String)
				return
			}
			advAssertRefusal(t, err, rc.code, rc.alt)
			if n, ok := advMySQLCountIfExists(t, mysqlTarget, rc.table); ok && n != 0 {
				t.Errorf("refusal %s left %d row(s) on the target; want 0", rc.name, n)
			}
			t.Logf("refusal %s: %v", rc.name, err)
		})
	}
}

// TestMigrate_AdversarialCorpusBackupRound_PostgresToMySQL pushes the
// PG corpus through the backup snapshot codec into a MySQL target.
func TestMigrate_AdversarialCorpusBackupRound_PostgresToMySQL(t *testing.T) {
	cells := advCorpusPGToMySQL()
	advAssertCorpusFloor(t, "pg→mysql backup", cells, advP2MMinFamilies, advP2MMinCells)

	pgSource, _, pgCleanup := startPostgres(t)
	defer pgCleanup()
	_, mysqlTarget, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()

	advSeedPGCorpus(t, pgSource, "adv_corpus", cells, 1)

	pgEng, _ := engines.Get("postgres")
	mysqlEng, _ := engines.Get("mysql")

	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if err := (&backup.Backup{
		Source: pgEng, SourceDSN: pgSource, Store: store, SluiceVersion: "test",
	}).Run(ctx); err != nil {
		t.Fatalf("Backup.Run: %v", err)
	}
	if err := (&backup.Restore{
		Target: mysqlEng, TargetDSN: mysqlTarget, Store: store,
	}).Run(ctx); err != nil {
		t.Fatalf("Restore.Run (PG corpus → MySQL): %v", err)
	}

	conn := advOpenConn(t, "mysql", mysqlTarget, advMySQLUTCSession...)
	advProbeCells(t, conn, "adv_corpus", cells, 1, false)
}

// advMySQLCountIfExists mirrors advCountIfExists for a MySQL target.
func advMySQLCountIfExists(t *testing.T, dsn, table string) (int, bool) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open mysql: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var exists int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?`, table).Scan(&exists); err != nil {
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

// dumpMySQLTargetCell logs whatever landed for a refusal table on a
// MySQL target so an exit-0 failure names the corrupt value.
func dumpMySQLTargetCell(t *testing.T, dsn, table string) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	rows, err := db.QueryContext(ctx, "SELECT id, CAST(v AS CHAR) FROM "+table+" ORDER BY id")
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
