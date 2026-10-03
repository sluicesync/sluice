//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The GC-44 availability gate: the anti-phantom family matrix.
//
// The target-witnessed first boundary (schema_forward_witness.go) checks a
// table's first schema boundary of every resumed stream against the target.
// Postgres and VStream take that boundary on EVERY resume, and the binlog
// lane does too once armed for first touch, so a single family whose
// change-stream projection and target read-back disagree — a mapping that
// is not idempotent, a collation decoration, an enum's labels, a domain's
// wrapper, an array element's modifier, unsigned, zerofill — would refuse
// a healthy stream on every restart.
//
// So, per direction (mysql→postgres, postgres→postgres, postgres→mysql,
// mysql→mysql): a table carrying one column of every family sluice maps
// for that pair, a cold start, a clean stop, a restart, and one row with a
// value in every column. The verdict must be a MATCH for the table — zero
// forwards, zero refusals, zero "cannot witness" and zero "target-only"
// warnings — and the row must land. The match line itself is required
// (anti-vacuity: a matrix that never reached the witness would pass every
// other assertion).
//
// Geometry on a Postgres target needs PostGIS and lives in the
// postgis-tagged sibling; it is in the mysql→mysql table here.

package pipeline

import (
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/config"
)

// twfbMySQLFamilyDDL carries every arm of internal/engines/mysql
// translateType a vanilla MySQL 8 source produces, plus the decorations the
// design names: unsigned, zerofill, per-column collation and charset,
// generated STORED and VIRTUAL, defaults, ENUM, SET, JSON.
const twfbMySQLFamilyDDL = `
	CREATE TABLE fam (
		id            INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		c_bool        TINYINT(1),
		c_tinyint     TINYINT,
		c_tinyint_u   TINYINT UNSIGNED,
		c_smallint    SMALLINT,
		c_smallint_u  SMALLINT UNSIGNED,
		c_mediumint   MEDIUMINT,
		c_mediumint_u MEDIUMINT UNSIGNED,
		c_int         INT,
		c_int_u       INT UNSIGNED,
		c_int_zf      INT(6) ZEROFILL,
		c_bigint      BIGINT,
		c_bigint_u    BIGINT UNSIGNED,
		c_year        YEAR,
		c_decimal     DECIMAL(10,2),
		c_decimal_w   DECIMAL(65,30),
		c_decimal_u   DECIMAL(8,3) UNSIGNED,
		c_float       FLOAT,
		c_double      DOUBLE,
		c_bit1        BIT(1),
		c_bit8        BIT(8),
		c_char        CHAR(10),
		c_varchar     VARCHAR(50),
		c_vc_bin      VARCHAR(20) COLLATE utf8mb4_bin,
		c_vc_latin1   VARCHAR(20) CHARACTER SET latin1,
		c_tinytext    TINYTEXT,
		c_text        TEXT,
		c_mediumtext  MEDIUMTEXT,
		c_longtext    LONGTEXT,
		c_binary      BINARY(16),
		c_varbinary   VARBINARY(64),
		c_tinyblob    TINYBLOB,
		c_blob        BLOB,
		c_mediumblob  MEDIUMBLOB,
		c_longblob    LONGBLOB,
		c_date        DATE,
		c_time        TIME,
		c_time3       TIME(3),
		c_datetime    DATETIME,
		c_datetime6   DATETIME(6),
		c_ts          TIMESTAMP NULL,
		c_ts6         TIMESTAMP(6) NULL,
		c_enum        ENUM('red','green'),
		c_set         SET('a','b','c'),
		c_json        JSON,
		d_int         INT NOT NULL DEFAULT 7,
		d_varchar     VARCHAR(20) NOT NULL DEFAULT 'hi',
		d_ts          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		g_stored      INT AS (c_int + 1) STORED,
		g_virtual     INT AS (c_int * 2) VIRTUAL
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	INSERT INTO fam (id) VALUES (1);
`

// twfbMySQLFamilyProbe writes a value into every non-generated column.
const twfbMySQLFamilyProbe = `
	INSERT INTO fam (id, c_bool, c_tinyint, c_tinyint_u, c_smallint, c_smallint_u, c_mediumint, c_mediumint_u,
		c_int, c_int_u, c_int_zf, c_bigint, c_bigint_u, c_year, c_decimal, c_decimal_w, c_decimal_u, c_float, c_double,
		c_bit1, c_bit8, c_char, c_varchar, c_vc_bin, c_vc_latin1, c_tinytext, c_text, c_mediumtext, c_longtext,
		c_binary, c_varbinary, c_tinyblob, c_blob, c_mediumblob, c_longblob, c_date, c_time, c_time3, c_datetime,
		c_datetime6, c_ts, c_ts6, c_enum, c_set, c_json)
	VALUES (2, 1, -5, 200, -300, 60000, -70000, 16000000,
		-5, 4000000000, 42, -9000000000, 9000000000000000000, 2026, 12.34, 1.5, 1.5, 1.5, 2.25,
		b'1', b'10101010', 'ch', 'vc', 'Bin', 'lat', 't', 't', 't', 't',
		0x0102, 0x0102, 0x0102, 0x0102, 0x0102, 0x0102, '2026-10-02', '10:11:12', '10:11:12.345', '2026-10-02 10:11:12',
		'2026-10-02 10:11:12.345678', '2026-10-02 10:11:12', '2026-10-02 10:11:12.345678', 'green', 'a,c', '{"k": 1}');
`

// twfbMySQLGeometryDDL is the spatial family, for the MySQL target only.
const twfbMySQLGeometryDDL = `
	CREATE TABLE fam_geo (
		id      INT NOT NULL PRIMARY KEY,
		c_geom  GEOMETRY NULL,
		c_point POINT NULL,
		c_srid  POINT SRID 0 NULL
	) ENGINE=InnoDB;
	INSERT INTO fam_geo (id) VALUES (1);
`

const twfbMySQLGeometryProbe = `
	INSERT INTO fam_geo VALUES (2, ST_GeomFromText('LINESTRING(0 0, 1 1)'), ST_GeomFromText('POINT(1 2)'), ST_GeomFromText('POINT(3 4)'));
`

// twfbPGFamilyDDL carries every family the Postgres reader maps that the
// pair can create: the integer widths and identity/serial, numeric bounded
// and bare, floats, the character family with and without length and with a
// COLLATE, bytea, bit/varbit, every temporal at bare / (0) / (3), the
// zone pairs, interval, json/jsonb, uuid, the network family, arrays (each
// element family, a modifier on the element, 2-D), an enum, a domain, and a
// generated column.
const twfbPGFamilyDDL = `
	CREATE TYPE fam_mood AS ENUM ('sad', 'ok', 'happy');
	CREATE DOMAIN fam_posint AS integer CHECK (VALUE > 0);
	CREATE TABLE fam (
		id             bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
		c_bool         boolean,
		c_int2         smallint,
		c_int4         integer,
		c_int8         bigint,
		c_numeric      numeric(12,3),
		c_numeric_bare numeric,
		c_real         real,
		c_double       double precision,
		c_char         char(5),
		c_varchar      varchar(40),
		c_varchar_bare varchar,
		c_text         text,
		c_text_c       text COLLATE "C",
		c_bytea        bytea,
		c_bit          bit(8),
		c_varbit       varbit(16),
		c_date         date,
		c_time         time,
		c_time0        time(0),
		c_time3        time(3),
		c_timetz       timetz,
		c_ts           timestamp,
		c_ts0          timestamp(0),
		c_ts3          timestamp(3),
		c_tstz         timestamptz,
		c_tstz0        timestamptz(0),
		c_json         json,
		c_jsonb        jsonb,
		c_uuid         uuid,
		c_inet         inet,
		c_cidr         cidr,
		c_macaddr      macaddr,
		c_macaddr8     macaddr8,
		c_int_arr      integer[],
		c_text_arr     text[],
		c_num_arr      numeric(10,2)[],
		c_ts_arr       timestamp(3)[],
		c_vc_arr       varchar(20)[],
		c_int_arr2     integer[][],
		c_enum         fam_mood,
		c_domain       fam_posint,
		g_stored       integer GENERATED ALWAYS AS (c_int4 + 1) STORED
	);
	INSERT INTO fam (id) VALUES (1);
`

const twfbPGFamilyProbe = `
	INSERT INTO fam (id, c_bool, c_int2, c_int4, c_int8, c_numeric, c_numeric_bare, c_real, c_double,
		c_char, c_varchar, c_varchar_bare, c_text, c_text_c, c_bytea, c_bit, c_varbit,
		c_date, c_time, c_time0, c_time3, c_timetz, c_ts, c_ts0, c_ts3, c_tstz, c_tstz0,
		c_json, c_jsonb, c_uuid, c_inet, c_cidr, c_macaddr, c_macaddr8,
		c_int_arr, c_text_arr, c_num_arr, c_ts_arr, c_vc_arr, c_int_arr2, c_enum, c_domain)
	VALUES (2, true, -3, -5, -9000000000, 123.456, 1.25, 1.5, 2.25,
		'ch', 'vc', 'vcb', 't', 'tc', '\x0102', B'10101010', B'101',
		'2026-10-02', '10:11:12.345678', '10:11:12', '10:11:12.345', '10:11:12+02', '2026-10-02 10:11:12.345678',
		'2026-10-02 10:11:12', '2026-10-02 10:11:12.345', '2026-10-02 10:11:12.345678+00', '2026-10-02 10:11:12+00',
		'{"k": 1}', '{"k": 1}', '9b2ec2a6-4a8e-4b6c-9a3e-0f0e0d0c0b0a', '10.0.0.1', '10.0.0.0/8', '08:00:2b:01:02:03', '08:00:2b:01:02:03:04:05',
		'{1,2}', '{a,b}', '{1.25,2.50}', '{"2026-10-02 10:11:12.345"}', '{x,y}', '{{1,2},{3,4}}', 'ok', 7);
`

// twfbPGOnlyDDL carries the families with no MySQL form or with a MySQL
// form the pair refuses — the verbatim-carried family (ADR-0051 /
// ADR-0070), interval, and a second auto column (serial beside the
// identity key; MySQL allows one auto column) — for the same-engine pair.
const twfbPGOnlyDDL = `
	CREATE TABLE fam_pgonly (
		id         bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
		c_serial   serial,
		c_tsvector tsvector,
		c_int4rng  int4range,
		c_tstzrng  tstzrange,
		c_xml      xml,
		c_money    money,
		c_interval interval
	);
	INSERT INTO fam_pgonly (id) VALUES (1);
`

const twfbPGOnlyProbe = `
	INSERT INTO fam_pgonly (id, c_tsvector, c_int4rng, c_tstzrng, c_xml, c_money, c_interval)
	VALUES (2, 'a fat cat', '[1,5)', '[2026-10-02 00:00+00,2026-10-03 00:00+00)', '<a>x</a>', 12.34, '1 day 02:03:04');
`

// The --type-override cells (GC-44 review item 3). An overridden column's
// type is the operator's, and the target emitter renders some overrides in
// ways no source rendering predicts — a `json` override reads back as
// binary JSON on a MySQL target, a `mediumtext` one as `text` on a Postgres
// target, and `smallint` on a TINYINT(1) column crosses families against
// the change stream's boolean. Each override family is here, per direction,
// so a witness that compared an overridden column's type would refuse or
// forward on the restart.
const twfbMySQLOverrideDDL = `
	CREATE TABLE fam_ovr (
		id     INT NOT NULL PRIMARY KEY,
		o_bool TINYINT(1),
		o_vc   VARCHAR(50),
		o_json LONGTEXT,
		o_mt   TEXT
	) ENGINE=InnoDB;
	INSERT INTO fam_ovr (id) VALUES (1);
`

// o_bool carries NULL: the binlog CDC decode still applies the boolean
// convention to a TINYINT(1) whatever the override says, so a 2 refuses
// (SLUICE-E-VALUE-TINYINT1-RANGE) and a 1 reaches a Postgres smallint as a
// bool and refuses at encode — both loud, both outside this matrix's
// subject (the column's TYPE is), and filed as GC-44 F11
// (docs/type-mapping.md scopes the smallint escape hatch to a bulk migrate,
// but `sync` accepts it).
const twfbMySQLOverrideProbe = `INSERT INTO fam_ovr VALUES (2, NULL, 'vc', '{"k": 1}', 'mt');`

// twfbPGOverrideDDL has no TINYINT(1)-shaped column: the smallint override
// is the MySQL boolean-convention escape hatch.
const twfbPGOverrideDDL = `
	CREATE TABLE fam_ovr (
		id     bigint PRIMARY KEY,
		o_vc   varchar(50),
		o_json text,
		o_mt   text
	);
	INSERT INTO fam_ovr (id) VALUES (1);
`

const twfbPGOverrideProbe = `INSERT INTO fam_ovr VALUES (2, 'vc', '{"k": 1}', 'mt');`

// twfbOverrides is the override list for a source family; bool adds the
// TINYINT(1) → smallint override a MySQL-family source carries.
func twfbOverrides(withBool bool) []config.Mapping {
	m := []config.Mapping{
		{Table: "fam_ovr", Column: "o_vc", TargetType: "varchar", TargetTypeOptions: map[string]any{"length": 200}},
		{Table: "fam_ovr", Column: "o_json", TargetType: "json"},
		{Table: "fam_ovr", Column: "o_mt", TargetType: "mediumtext"},
	}
	if withBool {
		m = append(m, config.Mapping{Table: "fam_ovr", Column: "o_bool", TargetType: "smallint"})
	}
	return m
}

var (
	twfbMySQLOverrideTable = twfbFamilyTable{"fam_ovr", twfbMySQLOverrideDDL, twfbMySQLOverrideProbe}
	twfbPGOverrideTable    = twfbFamilyTable{"fam_ovr", twfbPGOverrideDDL, twfbPGOverrideProbe}
)

// twfbFamilyTable is one table of a direction's matrix.
type twfbFamilyTable struct {
	name, ddl, probe string
}

// runTWFBFamilyMatrix is the cell: cold start, stop, restart, one row per
// table, and the verdict.
func runTWFBFamilyMatrix(t *testing.T, cell twfbCell, tables []twfbFamilyTable) {
	t.Helper()
	for _, tb := range tables {
		cell.src.exec(t, tb.ddl)
	}
	// One cold start copies every table.
	cell.coldStartAndStop(t, tables[0].name, 1)
	for _, tb := range tables {
		if !cell.tgt.waitRow(t, tb.name, 1, nil, 60*time.Second) {
			t.Fatalf("the cold start never delivered %s", tb.name)
		}
	}

	logs := twfbCaptureLogs(t)
	run := startTWFBRun(cell.streamer())
	for _, tb := range tables {
		cell.src.exec(t, tb.probe)
	}
	for _, tb := range tables {
		if !cell.tgt.waitRow(t, tb.name, 2, run, 120*time.Second) {
			err := run.stop(t)
			t.Fatalf("the probe row of %s never landed after the restart (stream: %v)\n%s",
				tb.name, err, divergenceLines(logs.String()))
		}
	}
	if err := run.stop(t); err != nil {
		t.Errorf("the restarted stream returned %v", err)
	}

	out := logs.String()
	if lines := divergenceLines(out); lines != "" {
		t.Errorf("PHANTOM: a healthy resume refused or forwarded:\n%s", lines)
	}
	// A stream that forwards nothing judges its boundaries through the
	// unforwarded-stream check (GC-44 F5), which logs its own lines.
	match, phantoms := twfbLogMatch, []string{twfbLogForwarded, twfbLogUnwitnessed, twfbLogTargetOnly, twfbLogTargetWider}
	if cell.unforwarded() {
		match, phantoms = refuseLogMatch, []string{refuseLogUnwitnessed, refuseLogTargetOnly, refuseLogAhead}
	}
	for _, tb := range tables {
		for _, marker := range phantoms {
			if lines := logLinesFor(logs, marker, tb.name); len(lines) > 0 {
				t.Errorf("PHANTOM on %s: %s", tb.name, strings.Join(lines, "\n"))
			}
		}
		matched := logLinesFor(logs, match, tb.name)
		if len(matched) == 0 {
			t.Errorf("VACUOUS: no first-boundary match was logged for %s — the witness never checked it", tb.name)
		}
		for _, line := range matched {
			t.Logf("witnessed: %s", line)
		}
	}
}

// divergenceLines extracts every refusal / forward line from the logs.
func divergenceLines(out string) string {
	var b strings.Builder
	for _, line := range strings.Split(out, "\n") {
		// The unforwarded-stream check's refusal is its ERROR line; the
		// multi-database engage WARN names the marker without refusing.
		if strings.Contains(line, resumeDivergenceMarker) || strings.Contains(line, `msg="schema change refused"`) ||
			strings.Contains(line, "target DDL applied") ||
			strings.Contains(line, "target ALTER applied") || strings.Contains(line, twfbLogForwarded) {
			b.WriteString(line)
			b.WriteByte('\n')
		}
	}
	return b.String()
}

func twfbFamilyMatrixMySQLToPostgres(t *testing.T, schemaChanges string) {
	srcDSN, _, srcCleanup := startMySQLBinlog(t)
	defer srcCleanup()
	_, tgtDSN, tgtCleanup := startPostgres(t)
	defer tgtCleanup()
	runTWFBFamilyMatrix(t, twfbCell{
		src: twfbDB{"mysql", srcDSN}, tgt: twfbDB{"postgres", tgtDSN}, streamID: "twfb-fam-m2p" + schemaChanges, schemaChanges: schemaChanges,
		mappings: twfbOverrides(true),
	}, []twfbFamilyTable{{"fam", twfbMySQLFamilyDDL, twfbMySQLFamilyProbe}, twfbMySQLOverrideTable})
}

func twfbFamilyMatrixMySQLToMySQL(t *testing.T, schemaChanges string) {
	srcDSN, tgtDSN, cleanup := startMySQLBinlog(t)
	defer cleanup()
	runTWFBFamilyMatrix(t, twfbCell{
		src: twfbDB{"mysql", srcDSN}, tgt: twfbDB{"mysql", tgtDSN}, streamID: "twfb-fam-m2m" + schemaChanges, schemaChanges: schemaChanges,
		mappings: twfbOverrides(true),
	}, []twfbFamilyTable{
		{"fam", twfbMySQLFamilyDDL, twfbMySQLFamilyProbe},
		{"fam_geo", twfbMySQLGeometryDDL, twfbMySQLGeometryProbe},
		twfbMySQLOverrideTable,
	})
}

func twfbFamilyMatrixPostgresToPostgres(t *testing.T, schemaChanges string) {
	srcDSN, tgtDSN, cleanup := startPostgresLogical(t)
	defer cleanup()
	runTWFBFamilyMatrix(t, twfbCell{
		src: twfbDB{"postgres", srcDSN}, tgt: twfbDB{"postgres", tgtDSN}, streamID: "twfb-fam-p2p" + schemaChanges, schemaChanges: schemaChanges,
		mappings: twfbOverrides(false),
	}, []twfbFamilyTable{
		{"fam", twfbPGFamilyDDL, twfbPGFamilyProbe},
		{"fam_pgonly", twfbPGOnlyDDL, twfbPGOnlyProbe},
		twfbPGOverrideTable,
	})
}

func twfbFamilyMatrixPostgresToMySQL(t *testing.T, schemaChanges string) {
	srcDSN, _, srcCleanup := startPostgresLogical(t)
	defer srcCleanup()
	_, tgtDSN, tgtCleanup := startMySQL(t)
	defer tgtCleanup()
	// The timetz column stays in the TABLE (its type is witnessed) but out
	// of the probe ROW: a value carrying an offset is refused by the MySQL
	// target's TIME at apply (Error 1292) — a loud, pre-existing value-path
	// refusal outside this matrix's subject, recorded in the GC-44 report.
	probe := strings.NewReplacer(" c_timetz,", "", " '10:11:12+02',", "").Replace(twfbPGFamilyProbe)
	runTWFBFamilyMatrix(t, twfbCell{
		src: twfbDB{"postgres", srcDSN}, tgt: twfbDB{"mysql", tgtDSN}, streamID: "twfb-fam-p2m" + schemaChanges, schemaChanges: schemaChanges,
		mappings: twfbOverrides(false),
	}, []twfbFamilyTable{{"fam", twfbPGFamilyDDL, probe}, twfbPGOverrideTable})
}

// twfbMariaDBOnlyDDL carries the MariaDB-native families: JSON (a LONGTEXT
// alias with an auto json_valid CHECK, recovered as JSON by the schema
// reader but read as LONGTEXT by the binlog boundary — the phantom the
// lens's JSON/long-TEXT rule exists for, found by
// TestStreamer_MariaDBToPostgres), UUID, INET4 and INET6.
const twfbMariaDBOnlyDDL = `
	CREATE TABLE fam_maria (
		id      INT NOT NULL PRIMARY KEY,
		c_json  JSON,
		c_uuid  UUID,
		c_inet4 INET4,
		c_inet6 INET6
	) ENGINE=InnoDB;
	INSERT INTO fam_maria (id) VALUES (1);
`

const twfbMariaDBOnlyProbe = `
	INSERT INTO fam_maria VALUES (2, '{"k": 1}', '9b2ec2a6-4a8e-4b6c-9a3e-0f0e0d0c0b0a', '10.0.0.1', '2001:db8::1');
`

func twfbFamilyMatrixMariaDBToPostgres(t *testing.T, schemaChanges string) {
	srcDSN, srcCleanup := startMariaDBBinlog(t)
	defer srcCleanup()
	_, tgtDSN, tgtCleanup := startPostgres(t)
	defer tgtCleanup()
	runTWFBFamilyMatrix(t, twfbCell{
		src: twfbDB{"mariadb", srcDSN}, tgt: twfbDB{"postgres", tgtDSN}, streamID: "twfb-fam-maria2p" + schemaChanges, schemaChanges: schemaChanges,
		mappings: twfbOverrides(true),
	}, []twfbFamilyTable{
		{"fam", twfbMySQLFamilyDDL, twfbMySQLFamilyProbe},
		{"fam_maria", twfbMariaDBOnlyDDL, twfbMariaDBOnlyProbe},
		twfbMySQLOverrideTable,
	})
}

func twfbFamilyMatrixMariaDBToMySQL(t *testing.T, schemaChanges string) {
	srcDSN, srcCleanup := startMariaDBBinlog(t)
	defer srcCleanup()
	_, tgtDSN, tgtCleanup := startMySQL(t)
	defer tgtCleanup()
	// The unsigned columns stay in the TABLE (their types are witnessed) but
	// carry in-range-for-signed values in the probe ROW: a MariaDB source's
	// INT UNSIGNED above 2^31-1 is refused by a MySQL target at CDC apply
	// (Error 1264) — loud, pre-existing (measured with first touch disarmed
	// too), outside this matrix's subject; filed as GC-44 F9.
	probe := strings.NewReplacer("4000000000,", "4000,", "9000000000000000000,", "9000,", "16000000,", "1600,", " 200,", " 20,", "60000,", "600,").
		Replace(twfbMySQLFamilyProbe)
	runTWFBFamilyMatrix(t, twfbCell{
		src: twfbDB{"mariadb", srcDSN}, tgt: twfbDB{"mysql", tgtDSN}, streamID: "twfb-fam-maria2m" + schemaChanges, schemaChanges: schemaChanges,
		mappings: twfbOverrides(true),
	}, []twfbFamilyTable{
		{"fam", twfbMySQLFamilyDDL, probe},
		{"fam_maria", twfbMariaDBOnlyDDL, twfbMariaDBOnlyProbe},
		twfbMySQLOverrideTable,
	})
}

// The forward (default) arm of each direction: the TWFB availability gate.

func TestTWFBFamilyMatrix_MySQLToPostgres(t *testing.T) {
	twfbFamilyMatrixMySQLToPostgres(t, "")
}

func TestTWFBFamilyMatrix_MySQLToMySQL(t *testing.T) {
	twfbFamilyMatrixMySQLToMySQL(t, "")
}

func TestTWFBFamilyMatrix_PostgresToPostgres(t *testing.T) {
	twfbFamilyMatrixPostgresToPostgres(t, "")
}

func TestTWFBFamilyMatrix_PostgresToMySQL(t *testing.T) {
	twfbFamilyMatrixPostgresToMySQL(t, "")
}

func TestTWFBFamilyMatrix_MariaDBToPostgres(t *testing.T) {
	twfbFamilyMatrixMariaDBToPostgres(t, "")
}

func TestTWFBFamilyMatrix_MariaDBToMySQL(t *testing.T) {
	twfbFamilyMatrixMariaDBToMySQL(t, "")
}

// The --schema-changes=refuse arm (GC-44 F5): the same tables through the
// unforwarded-stream check (schema_change_refuse.go), which takes the
// binlog lane's first-touch boundary too, so a phantom there would refuse a
// healthy refuse-mode stream on every restart.

func TestStreamer_RefuseFamilyMatrix_MySQLToPostgres(t *testing.T) {
	twfbFamilyMatrixMySQLToPostgres(t, "refuse")
}

func TestStreamer_RefuseFamilyMatrix_MySQLToMySQL(t *testing.T) {
	twfbFamilyMatrixMySQLToMySQL(t, "refuse")
}

func TestStreamer_RefuseFamilyMatrix_PostgresToPostgres(t *testing.T) {
	twfbFamilyMatrixPostgresToPostgres(t, "refuse")
}

func TestStreamer_RefuseFamilyMatrix_PostgresToMySQL(t *testing.T) {
	twfbFamilyMatrixPostgresToMySQL(t, "refuse")
}

func TestStreamer_RefuseFamilyMatrix_MariaDBToPostgres(t *testing.T) {
	twfbFamilyMatrixMariaDBToPostgres(t, "refuse")
}

func TestStreamer_RefuseFamilyMatrix_MariaDBToMySQL(t *testing.T) {
	twfbFamilyMatrixMariaDBToMySQL(t, "refuse")
}
