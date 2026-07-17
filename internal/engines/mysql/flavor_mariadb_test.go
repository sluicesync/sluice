// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"database/sql"
	"strings"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"

	"sluicesync.dev/sluice/internal/ir"
)

// NOTE: the former TestMariaDBPreflightCDCScope pinned the Phase-3
// add-table refusal for native uuid/inet columns. ADR-0171 lifts that
// refusal (the binlog decode now handles those types), so the test — and
// the Engine.PreflightCDCScope method it exercised — are gone. The native
// binlog decode is pinned in value_decode_mariadb_test.go (unit, the
// byte→text family matrix) and the mariadb CDC integration suite
// (src==dst on the real target).

// TestMariaDBCapabilities pins the load-bearing pieces of the mariadb
// declaration (roadmap item 73): bulk source+target with the LOAD DATA
// path, CDC honestly absent, textual JSON, and — since Phase 2 — native
// geometry (SRID via REF_SYSTEM_ID), UUID, and INET.
func TestMariaDBCapabilities(t *testing.T) {
	caps := FlavorMariaDB.capabilities()
	if caps.BulkLoad != ir.BulkLoadLoadDataInfile {
		t.Errorf("mariadb BulkLoad = %v; want LoadDataInfile (verified live via the restore probe leg)", caps.BulkLoad)
	}
	if caps.CDC != ir.CDCBinlog {
		t.Errorf("mariadb CDC = %v; want CDCBinlog (domain-GTID binlog CDC shipped in item 73 Phase 3, ADR-0170)", caps.CDC)
	}
	if caps.JSONSupport != ir.JSONText {
		t.Errorf("mariadb JSONSupport = %v; want JSONText (MariaDB JSON is a LONGTEXT alias)", caps.JSONSupport)
	}
	// Phase 2: geometry (SRID via REF_SYSTEM_ID), UUID, and INET are now
	// declared. Geometry SRID read-back is proven live in the integration
	// suite (a POINT with SRID 4326 must not read back as 0).
	if !caps.SupportedTypes.Has(ir.ExtGeometry) {
		t.Error("mariadb should declare Geometry support (Phase 2: SRID recovered from REF_SYSTEM_ID)")
	}
	if !caps.SupportedTypes.Has(ir.ExtUUID) {
		t.Error("mariadb should declare UUID support (Phase 2: native uuid type)")
	}
	if !caps.SupportedTypes.Has(ir.ExtInet) {
		t.Error("mariadb should declare Inet support (Phase 2: native inet6/inet4 types)")
	}
	if !caps.SupportedTypes.Has(ir.ExtEnum) || !caps.SupportedTypes.Has(ir.ExtSet) {
		t.Error("mariadb should declare ENUM and SET support")
	}
	if caps.DDLDialect != ir.DDLDialectMySQL {
		t.Errorf("mariadb DDLDialect = %v; want DDLDialectMySQL", caps.DDLDialect)
	}
	if FlavorMariaDB.usesVStream() {
		t.Error("FlavorMariaDB.usesVStream() = true; want false")
	}
	if caps.CDCPositionCommitsAfterRows {
		t.Error("mariadb CDCPositionCommitsAfterRows = true; want false (MariaDB binlog, like MySQL, " +
			"stamps the GTID event BEFORE the transaction's rows — positions do not commit after rows)")
	}
}

// TestMariaDBUpsertSpelling pins the flavor → spelling wiring and the
// two rendered fragments every upsert builder composes from.
func TestMariaDBUpsertSpelling(t *testing.T) {
	if got := FlavorMariaDB.upsertSpelling(); got != upsertValuesFunc {
		t.Fatalf("FlavorMariaDB.upsertSpelling() = %v; want upsertValuesFunc", got)
	}
	for _, f := range []Flavor{FlavorVanilla, FlavorPlanetScale, FlavorVitess} {
		if got := f.upsertSpelling(); got != upsertRowAlias {
			t.Errorf("%s.upsertSpelling() = %v; want upsertRowAlias", f, got)
		}
	}
	if got := upsertRowAlias.clauseOpen(); got != " AS new ON DUPLICATE KEY UPDATE " {
		t.Errorf("row-alias clauseOpen = %q", got)
	}
	if got := upsertValuesFunc.clauseOpen(); got != " ON DUPLICATE KEY UPDATE " {
		t.Errorf("VALUES() clauseOpen = %q", got)
	}
	if got := upsertRowAlias.newRowRef("`v`"); got != "new.`v`" {
		t.Errorf("row-alias newRowRef = %q", got)
	}
	if got := upsertValuesFunc.newRowRef("`v`"); got != "VALUES(`v`)" {
		t.Errorf("VALUES() newRowRef = %q", got)
	}
}

// TestTranslateMariaDBDefault_ParityMatrix is the Bug-74-discipline pin
// for the defaults shim: every COLUMN_DEFAULT shape × type family the
// scoping + implementation probes cataloged, asserting the IR default
// produced from MariaDB's reported form equals the IR the SAME logical
// schema produces via [translateDefault] from MySQL 8's reported form.
// The reported forms below are verbatim ground truth captured side by
// side on mariadb:11.4.12 / mariadb:10.11.18 (identical) and mysql:8.4;
// the integration matrix re-derives them from live servers.
func TestTranslateMariaDBDefault_ParityMatrix(t *testing.T) {
	valid := func(s string) sql.NullString { return sql.NullString{String: s, Valid: true} }
	null := sql.NullString{}

	cases := []struct {
		name string
		typ  ir.Type

		// MariaDB's reported (COLUMN_DEFAULT, extra).
		mdbDef   sql.NullString
		mdbExtra string

		// MySQL 8's reported (COLUMN_DEFAULT, extra) for the same
		// declared default.
		myDef   sql.NullString
		myExtra string

		want ir.DefaultValue
	}{
		{
			name:   "no default, NOT NULL",
			typ:    ir.Integer{Width: 32},
			mdbDef: null, myDef: null,
			want: ir.DefaultNone{},
		},
		{
			name:   "nullable defaultless / DEFAULT NULL — the string-NULL hazard",
			typ:    ir.Varchar{Length: 20},
			mdbDef: valid("NULL"), myDef: null,
			want: ir.DefaultNone{},
		},
		{
			name:   "string literal — quoted on MariaDB, bare on MySQL",
			typ:    ir.Varchar{Length: 20},
			mdbDef: valid("'abc'"), myDef: valid("abc"),
			want: ir.DefaultLiteral{Value: "abc"},
		},
		{
			name:   "string literal with embedded quote — '' doubling",
			typ:    ir.Varchar{Length: 20},
			mdbDef: valid("'it''s'"), myDef: valid("it's"),
			want: ir.DefaultLiteral{Value: "it's"},
		},
		{
			name:   "the literal STRING 'NULL' stays a string",
			typ:    ir.Varchar{Length: 20},
			mdbDef: valid("'NULL'"), myDef: valid("NULL"),
			want: ir.DefaultLiteral{Value: "NULL"},
		},
		{
			name:   "empty-string default",
			typ:    ir.Varchar{Length: 20},
			mdbDef: valid("''"), myDef: valid(""),
			want: ir.DefaultLiteral{Value: ""},
		},
		{
			name:   "string literal with escaped newline (MariaDB escape-encodes control chars)",
			typ:    ir.Varchar{Length: 20},
			mdbDef: valid(`'a\nb'`), myDef: valid("a\nb"),
			want: ir.DefaultLiteral{Value: "a\nb"},
		},
		{
			name:   "positive integer",
			typ:    ir.Integer{Width: 32},
			mdbDef: valid("42"), myDef: valid("42"),
			want: ir.DefaultLiteral{Value: "42"},
		},
		{
			name:   "negative integer",
			typ:    ir.Integer{Width: 32},
			mdbDef: valid("-7"), myDef: valid("-7"),
			want: ir.DefaultLiteral{Value: "-7"},
		},
		{
			name:   "decimal",
			typ:    ir.Decimal{Precision: 10, Scale: 2},
			mdbDef: valid("9.99"), myDef: valid("9.99"),
			want: ir.DefaultLiteral{Value: "9.99"},
		},
		{
			name:   "float (1e3 evaluates to 1000 on both)",
			typ:    ir.Float{Precision: ir.FloatDouble},
			mdbDef: valid("1000"), myDef: valid("1000"),
			want: ir.DefaultLiteral{Value: "1000"},
		},
		{
			name:   "YEAR",
			typ:    ir.Integer{Width: 16},
			mdbDef: valid("2024"), myDef: valid("2024"),
			want: ir.DefaultLiteral{Value: "2024"},
		},
		{
			name:   "boolean TINYINT(1) DEFAULT TRUE",
			typ:    ir.Boolean{},
			mdbDef: valid("1"), myDef: valid("1"),
			want: ir.DefaultLiteral{Value: "1"},
		},
		{
			name:    "CURRENT_TIMESTAMP — keyword on MySQL, function-call on MariaDB, extra EMPTY on MariaDB",
			typ:     ir.Timestamp{WithTimeZone: true},
			mdbDef:  valid("current_timestamp()"),
			myDef:   valid("CURRENT_TIMESTAMP"),
			myExtra: "DEFAULT_GENERATED",
			want:    ir.DefaultExpression{Expr: "CURRENT_TIMESTAMP", Dialect: "mysql"},
		},
		{
			name:    "CURRENT_TIMESTAMP(3) keeps its precision",
			typ:     ir.DateTime{Precision: 3},
			mdbDef:  valid("current_timestamp(3)"),
			myDef:   valid("CURRENT_TIMESTAMP(3)"),
			myExtra: "DEFAULT_GENERATED",
			want:    ir.DefaultExpression{Expr: "CURRENT_TIMESTAMP(3)", Dialect: "mysql"},
		},
		{
			name:     "ON UPDATE variant — extra differs, default identical",
			typ:      ir.Timestamp{WithTimeZone: true},
			mdbDef:   valid("current_timestamp()"),
			mdbExtra: "on update current_timestamp()",
			myDef:    valid("CURRENT_TIMESTAMP"),
			myExtra:  "DEFAULT_GENERATED on update CURRENT_TIMESTAMP",
			want:     ir.DefaultExpression{Expr: "CURRENT_TIMESTAMP", Dialect: "mysql"},
		},
		{
			name:    "function expression default (uuid())",
			typ:     ir.Char{Length: 36},
			mdbDef:  valid("uuid()"),
			myDef:   valid("uuid()"),
			myExtra: "DEFAULT_GENERATED",
			want:    ir.DefaultExpression{Expr: "uuid()", Dialect: "mysql"},
		},
		{
			name:    "arithmetic expression default",
			typ:     ir.Integer{Width: 32},
			mdbDef:  valid("(1 + 1)"),
			myDef:   valid("(1 + 1)"),
			myExtra: "DEFAULT_GENERATED",
			want:    ir.DefaultExpression{Expr: "(1 + 1)", Dialect: "mysql"},
		},
		{
			name:   "BIT(1) → Boolean: decimal collapse (catalog #4)",
			typ:    ir.Boolean{},
			mdbDef: valid("b'1'"), myDef: valid("b'1'"),
			want: ir.DefaultLiteral{Value: "1"},
		},
		{
			name:   "BIT(8) keeps the bit literal (catalog Bug 62)",
			typ:    ir.Bit{Length: 8},
			mdbDef: valid("b'10100101'"), myDef: valid("b'10100101'"),
			want: ir.DefaultExpression{Expr: "b'10100101'", Dialect: bitLiteralDialect},
		},
		{
			name:   "BINARY literal — quoted raw bytes on MariaDB, bare hex on MySQL",
			typ:    ir.Binary{Length: 2},
			mdbDef: valid("'AB'"), myDef: valid("0x4142"),
			want: ir.DefaultExpression{Expr: "0x4142", Dialect: hexLiteralDialect},
		},
		{
			name:   "VARBINARY with control bytes (raw inside MariaDB's quotes)",
			typ:    ir.Varbinary{Length: 4},
			mdbDef: valid("'\x01\x02'"), myDef: valid("0x0102"),
			want: ir.DefaultExpression{Expr: "0x0102", Dialect: hexLiteralDialect},
		},
		{
			name: "VARBINARY with trailing NUL — MariaDB escape-encodes, no C-truncation",
			typ:  ir.Varbinary{Length: 4},
			// X'2700' → 0x27 0x00 → MariaDB reports '''\0' (doubled quote
			// for 0x27, \0 for the NUL). MySQL truncates this shape in
			// information_schema (the recoverFromShowCreate path repairs
			// it there), so the MySQL column is compared against the
			// recovered value's hex form directly.
			mdbDef: valid(`'''\0'`), myDef: valid("0x2700"),
			want: ir.DefaultExpression{Expr: "0x2700", Dialect: hexLiteralDialect},
		},
		{
			name:   "ENUM label default",
			typ:    ir.Enum{Values: []string{"red", "green"}},
			mdbDef: valid("'red'"), myDef: valid("red"),
			want: ir.DefaultLiteral{Value: "red"},
		},
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			gotMdb := translateMariaDBDefault(c.mdbDef, c.mdbExtra, c.typ)
			if gotMdb != c.want {
				t.Errorf("translateMariaDBDefault(%q) = %#v; want %#v", c.mdbDef.String, gotMdb, c.want)
			}
			gotMy := translateDefault(c.myDef, c.myExtra, c.typ)
			if gotMy != c.want {
				t.Errorf("translateDefault(%q) [MySQL 8 form] = %#v; want %#v — the parity anchor drifted", c.myDef.String, gotMy, c.want)
			}
		})
	}
}

// TestTranslateMariaDBDefault_MalformedQuoted pins the loud fall-back:
// an unterminated / trailing-garbage quoted default carries verbatim
// (the target rejects it loudly) rather than being silently repaired.
func TestTranslateMariaDBDefault_MalformedQuoted(t *testing.T) {
	for _, raw := range []string{"'unterminated", "'trailing' garbage"} {
		got := translateMariaDBDefault(sql.NullString{String: raw, Valid: true}, "", ir.Varchar{Length: 20})
		if got != (ir.DefaultLiteral{Value: raw}) {
			t.Errorf("translateMariaDBDefault(%q) = %#v; want verbatim DefaultLiteral", raw, got)
		}
	}
}

func TestMariaDBNumericDefault(t *testing.T) {
	yes := []string{"0", "42", "-7", "+3", "9.99", "-0.5", "1e3", "1.5E-2", "2024", "1."}
	no := []string{"", "-", ".", ".5e", "abc", "1+1", "(1 + 1)", "uuid()", "0x41", "b'1'", "1e", "1e+", "--1"}
	for _, s := range yes {
		if !mariadbNumericDefault(s) {
			t.Errorf("mariadbNumericDefault(%q) = false; want true", s)
		}
	}
	for _, s := range no {
		if mariadbNumericDefault(s) {
			t.Errorf("mariadbNumericDefault(%q) = true; want false", s)
		}
	}
}

func TestCanonMariaDBTimestampExpr(t *testing.T) {
	cases := map[string]string{
		"current_timestamp()":     "CURRENT_TIMESTAMP",
		"current_timestamp(3)":    "CURRENT_TIMESTAMP(3)",
		"current_timestamp(6)":    "CURRENT_TIMESTAMP(6)",
		"CURRENT_TIMESTAMP()":     "CURRENT_TIMESTAMP",
		"current_timestamp(x)":    "current_timestamp(x)", // non-numeric arg: untouched
		"curdate()":               "curdate()",            // only the CURRENT_TIMESTAMP family diverges
		"uuid()":                  "uuid()",
		"current_timestamp_ish()": "current_timestamp_ish()",
	}
	for in, want := range cases {
		if got := canonMariaDBTimestampExpr(in); got != want {
			t.Errorf("canonMariaDBTimestampExpr(%q) = %q; want %q", in, got, want)
		}
	}
}

func TestParseMariaDBVersion(t *testing.T) {
	cases := []struct {
		in           string
		major, minor int
		ok           bool
	}{
		{"11.4.12-MariaDB-ubu2404", 11, 4, true},
		{"10.11.18-MariaDB-ubu2204-log", 10, 11, true},
		{"5.5.5-10.6.7-MariaDB", 10, 6, true},
		{"8.4.10", 0, 0, false},
		{"8.0.46-0ubuntu0.22.04.1", 0, 0, false},
	}
	for _, c := range cases {
		major, minor, ok := parseMariaDBVersion(c.in)
		if ok != c.ok || major != c.major || minor != c.minor {
			t.Errorf("parseMariaDBVersion(%q) = (%d, %d, %v); want (%d, %d, %v)", c.in, major, minor, ok, c.major, c.minor, c.ok)
		}
	}
}

// TestMariaDBCollationRemaps pins the cross-family maps in both
// directions and that they are exact mirrors of each other.
func TestMariaDBCollationRemaps(t *testing.T) {
	if got := FlavorMariaDB.crossFlavorCollationRemap()["utf8mb4_0900_ai_ci"]; got != "utf8mb4_uca1400_ai_ci" {
		t.Errorf("mariadb remap of utf8mb4_0900_ai_ci = %q", got)
	}
	if got := FlavorVanilla.crossFlavorCollationRemap()["utf8mb4_uca1400_ai_ci"]; got != "utf8mb4_0900_ai_ci" {
		t.Errorf("vanilla remap of utf8mb4_uca1400_ai_ci = %q", got)
	}
	// Mirror-completeness: every entry in one map reverses in the other.
	for from, to := range mariadbTargetCollations {
		if back, ok := mysqlTargetCollations[to]; !ok || back != from {
			t.Errorf("mariadbTargetCollations[%q] = %q does not mirror in mysqlTargetCollations (got %q, %v)", from, to, back, ok)
		}
	}
	for from, to := range mysqlTargetCollations {
		if back, ok := mariadbTargetCollations[to]; !ok || back != from {
			t.Errorf("mysqlTargetCollations[%q] = %q does not mirror in mariadbTargetCollations (got %q, %v)", from, to, back, ok)
		}
	}
	// A language-specific 0900 collation is deliberately NOT mapped —
	// it must fail loudly on a pre-11.4 target, never guess.
	if _, ok := mariadbTargetCollations["utf8mb4_de_pb_0900_ai_ci"]; ok {
		t.Error("language-specific 0900 collations must not be remapped")
	}
	// general_ci exists on both families: never remapped.
	if _, ok := mariadbTargetCollations["utf8mb4_general_ci"]; ok {
		t.Error("utf8mb4_general_ci must pass through unmapped")
	}
	if _, ok := mysqlTargetCollations["utf8mb4_general_ci"]; ok {
		t.Error("utf8mb4_general_ci must pass through unmapped")
	}
}

// TestEmittableCollation_Remap pins the emitter composition: PG
// collations still drop, remapped collations swap, everything else
// passes through — and the nil-remap emitter (stdEmitter / unit
// constructions) stays byte-identical to the historical behavior.
func TestEmittableCollation_Remap(t *testing.T) {
	mariadbEmitter := newMySQLEmitterForFlavor(nil, FlavorMariaDB)
	vanillaEmitter := newMySQLEmitterForFlavor(nil, FlavorVanilla)

	if got := mariadbEmitter.emittableCollation("utf8mb4", "utf8mb4_0900_ai_ci"); got != "utf8mb4_uca1400_ai_ci" {
		t.Errorf("mariadb emitter: 0900_ai_ci → %q; want utf8mb4_uca1400_ai_ci", got)
	}
	if got := vanillaEmitter.emittableCollation("utf8mb4", "utf8mb4_uca1400_ai_ci"); got != "utf8mb4_0900_ai_ci" {
		t.Errorf("vanilla emitter: uca1400_ai_ci → %q; want utf8mb4_0900_ai_ci", got)
	}
	if got := mariadbEmitter.emittableCollation("utf8mb4", "utf8mb4_general_ci"); got != "utf8mb4_general_ci" {
		t.Errorf("mariadb emitter: general_ci → %q; want passthrough", got)
	}
	// PG-dialect collation still drops (charset-paired rule).
	if got := mariadbEmitter.emittableCollation("", "en_US"); got != "" {
		t.Errorf("mariadb emitter: PG collation en_US → %q; want dropped", got)
	}
	// nil-remap emitter: byte-identical to mysqlEmittableCollation.
	if got := stdEmitter.emittableCollation("utf8mb4", "utf8mb4_0900_ai_ci"); got != "utf8mb4_0900_ai_ci" {
		t.Errorf("stdEmitter: 0900_ai_ci → %q; want passthrough (no remap)", got)
	}
}

// TestMariaDBCatalogQueries pins that the vanilla query text is
// byte-identical to the historical constants (the MySQL-8 path must
// not drift) and that the mariadb variants drop exactly the two
// MySQL-8-only projections.
func TestMariaDBCatalogQueries(t *testing.T) {
	const wantColumns = `
		SELECT
			table_name,
			column_name,
			ordinal_position,
			column_default,
			is_nullable,
			LOWER(data_type),
			character_maximum_length,
			numeric_precision,
			numeric_scale,
			datetime_precision,
			IFNULL(character_set_name, ''),
			IFNULL(collation_name, ''),
			IFNULL(srs_id, 0),
			column_type,
			IFNULL(extra, ''),
			IFNULL(column_comment, ''),
			IFNULL(generation_expression, '')
		FROM   information_schema.columns
		WHERE  table_schema = ?
		ORDER  BY table_name, ordinal_position`
	if got := columnsQuery(FlavorVanilla); got != wantColumns {
		t.Errorf("columnsQuery(vanilla) drifted from the historical constant:\n got: %s\nwant: %s", got, wantColumns)
	}
	const wantIndexes = `
		SELECT
			table_name,
			index_name,
			non_unique,
			LOWER(IFNULL(index_type, '')),
			column_name,
			IFNULL(expression, ''),
			seq_in_index,
			IFNULL(sub_part, 0),
			IFNULL(collation, '')
		FROM   information_schema.statistics
		WHERE  table_schema = ?
		ORDER  BY table_name, index_name, seq_in_index`
	if got := indexesQuery(FlavorVanilla); got != wantIndexes {
		t.Errorf("indexesQuery(vanilla) drifted from the historical constant:\n got: %s\nwant: %s", got, wantIndexes)
	}
	// The two MySQL-8-only projections gate on the flavor; the
	// constant substitutions keep the projection COUNT (the Scan
	// destinations are shared).
	if q := columnsQuery(FlavorMariaDB); strings.Contains(q, "srs_id") || !strings.Contains(q, "\n\t\t\t0,\n") {
		t.Errorf("mariadb columnsQuery should select the constant 0 in place of srs_id:\n%s", q)
	}
	if q := indexesQuery(FlavorMariaDB); strings.Contains(q, "IFNULL(expression") || !strings.Contains(q, "\n\t\t\t'',\n") {
		t.Errorf("mariadb indexesQuery should select the constant '' in place of expression:\n%s", q)
	}

	// Bug 198: the check-constraints join is disambiguated by table_name
	// ONLY on MariaDB (its constraint names are unique per-table, and its
	// check_constraints carries table_name); MySQL 8's must NOT reference
	// cc.table_name (no such column there — it would be a hard SQL error).
	const wantChecks = `
		SELECT
			tc.table_name,
			cc.constraint_name,
			cc.check_clause
		FROM   information_schema.check_constraints cc
		JOIN   information_schema.table_constraints  tc
		  ON   tc.constraint_schema = cc.constraint_schema
		 AND   tc.constraint_name   = cc.constraint_name
		WHERE  tc.table_schema    = ?
		  AND  tc.constraint_type = 'CHECK'
		ORDER  BY tc.table_name, cc.constraint_name`
	if got := checkConstraintsQuery(FlavorVanilla); got != wantChecks {
		t.Errorf("checkConstraintsQuery(vanilla) drifted / references cc.table_name (MySQL 8 has no such column):\n got: %s\nwant: %s", got, wantChecks)
	}
	if q := checkConstraintsQuery(FlavorMariaDB); !strings.Contains(q, "cc.table_name       = tc.table_name") {
		t.Errorf("mariadb checkConstraintsQuery must disambiguate the join by table_name (Bug 198 fan-out):\n%s", q)
	}
	if q := checkConstraintsQuery(FlavorPlanetScale); strings.Contains(q, "cc.table_name") {
		t.Errorf("non-mariadb checkConstraintsQuery must NOT reference cc.table_name:\n%s", q)
	}
}

// TestMariaDBCDCEnabled pins Phase 3 (ADR-0170): the mariadb flavor now
// declares binlog CDC, so the Phase-1 coded refusal
// (CodeCDCMariaDBUnsupported via the [ir.CDCUnsupportedExplainer] hook) is
// gone. The flavor takes the binlog reader path (goMySQLFlavor →
// mysql.MariaDBFlavor), and — since every mysql-family flavor now supports
// CDC — the Engine no longer implements the CDC-unsupported explainer at
// all, so a pipeline preflight falls through to the real CDC support
// rather than a flavor-specific refusal.
func TestMariaDBCDCEnabled(t *testing.T) {
	if got := FlavorMariaDB.capabilities().CDC; got != ir.CDCBinlog {
		t.Fatalf("mariadb CDC = %v; want CDCBinlog (Phase 3, ADR-0170)", got)
	}
	// The reader dispatches to the MariaDB go-mysql flavor (domain-GTID
	// MariadbGTIDSet), not the MySQL one.
	if got := (&CDCReader{flavor: FlavorMariaDB}).goMySQLFlavor(); got != mysql.MariaDBFlavor {
		t.Errorf("mariadb goMySQLFlavor() = %q; want %q", got, mysql.MariaDBFlavor)
	}
	if got := (&CDCReader{flavor: FlavorVanilla}).goMySQLFlavor(); got != mysql.MySQLFlavor {
		t.Errorf("vanilla goMySQLFlavor() = %q; want %q", got, mysql.MySQLFlavor)
	}
	// The Phase-1 CDC-unsupported explainer hook was removed for all
	// mysql-family flavors — none of them declares CDCNone anymore.
	if _, ok := any(Engine{Flavor: FlavorMariaDB}).(ir.CDCUnsupportedExplainer); ok {
		t.Error("Engine still implements ir.CDCUnsupportedExplainer; the hook should be gone now that every flavor supports CDC")
	}
}

// TestMariaDBUpsertBuilders_BothSpellings is the byte-exact pin over
// every upsert-builder × spelling cell (the item-73 "class, not
// representative" sweep of the `AS new` emission sites: applier
// single/multi-row, position write, schema history, migrate-state, and
// the batched-insert row writer).
func TestMariaDBUpsertBuilders_BothSpellings(t *testing.T) {
	t.Run("onDuplicateKeyUpdateClause", func(t *testing.T) {
		cols := []string{"id", "email", "active"}
		pk := []string{"id"}
		if got, want := onDuplicateKeyUpdateClause(cols, pk, upsertRowAlias),
			" AS new ON DUPLICATE KEY UPDATE `email` = new.`email`, `active` = new.`active`"; got != want {
			t.Errorf("row alias:\n got: %s\nwant: %s", got, want)
		}
		if got, want := onDuplicateKeyUpdateClause(cols, pk, upsertValuesFunc),
			" ON DUPLICATE KEY UPDATE `email` = VALUES(`email`), `active` = VALUES(`active`)"; got != want {
			t.Errorf("VALUES():\n got: %s\nwant: %s", got, want)
		}
		// All-PK degenerate no-op assignment.
		if got, want := onDuplicateKeyUpdateClause([]string{"a", "b"}, []string{"a", "b"}, upsertValuesFunc),
			" ON DUPLICATE KEY UPDATE `a` = VALUES(`a`)"; got != want {
			t.Errorf("VALUES() all-PK:\n got: %s\nwant: %s", got, want)
		}
		// Keyless full-row SET-list.
		if got, want := onDuplicateKeyUpdateClause([]string{"x", "y"}, nil, upsertValuesFunc),
			" ON DUPLICATE KEY UPDATE `x` = VALUES(`x`), `y` = VALUES(`y`)"; got != want {
			t.Errorf("VALUES() keyless:\n got: %s\nwant: %s", got, want)
		}
	})

	t.Run("buildInsertSQL", func(t *testing.T) {
		row := ir.Row{"id": 1, "v": "a"}
		gotSQL, _, err := buildInsertSQL("src", "t", row, []string{"id"}, nil, upsertValuesFunc)
		if err != nil {
			t.Fatal(err)
		}
		want := "INSERT INTO `src`.`t` (`id`, `v`) VALUES (?, ?) ON DUPLICATE KEY UPDATE `v` = VALUES(`v`)"
		if gotSQL != want {
			t.Errorf("mariadb single-row upsert:\n got: %s\nwant: %s", gotSQL, want)
		}
	})

	t.Run("writePositionUpsertSQL", func(t *testing.T) {
		got := writePositionUpsertSQL("", upsertValuesFunc)
		want := "INSERT INTO `sluice_cdc_state` " +
			"(stream_id, source_position, slot_name, source_dsn_fingerprint, target_schema, rows_applied) " +
			"VALUES (?, ?, NULLIF(?, ''), NULLIF(?, ''), NULLIF(?, ''), ?) ON DUPLICATE KEY UPDATE " +
			"source_position = VALUES(source_position), " +
			"slot_name = COALESCE(VALUES(slot_name), `sluice_cdc_state`.slot_name), " +
			"source_dsn_fingerprint = COALESCE(VALUES(source_dsn_fingerprint), `sluice_cdc_state`.source_dsn_fingerprint), " +
			"target_schema = COALESCE(VALUES(target_schema), `sluice_cdc_state`.target_schema), " +
			"rows_applied = COALESCE(`sluice_cdc_state`.rows_applied, 0) + VALUES(rows_applied)"
		if got != want {
			t.Errorf("mariadb position upsert:\n got: %s\nwant: %s", got, want)
		}
	})

	t.Run("schemaVersionUpsertSQL", func(t *testing.T) {
		got := schemaVersionUpsertSQL("", upsertValuesFunc)
		want := "INSERT INTO `sluice_cdc_schema_history` " +
			"(version_key, stream_id, schema_name, table_name, anchor_position, ir_schema_json, source_engine) " +
			"VALUES (?, ?, ?, ?, ?, ?, NULLIF(?, '')) ON DUPLICATE KEY UPDATE " +
			"ir_schema_json = VALUES(ir_schema_json), " +
			"source_engine = COALESCE(VALUES(source_engine), `sluice_cdc_schema_history`.source_engine)"
		if got != want {
			t.Errorf("mariadb schema-history upsert:\n got: %s\nwant: %s", got, want)
		}
		// Row-alias shape is byte-identical to the pre-item-73 inline statement.
		gotAlias := schemaVersionUpsertSQL("", upsertRowAlias)
		wantAlias := "INSERT INTO `sluice_cdc_schema_history` " +
			"(version_key, stream_id, schema_name, table_name, anchor_position, ir_schema_json, source_engine) " +
			"VALUES (?, ?, ?, ?, ?, ?, NULLIF(?, '')) AS new ON DUPLICATE KEY UPDATE " +
			"ir_schema_json = new.ir_schema_json, " +
			"source_engine = COALESCE(new.source_engine, `sluice_cdc_schema_history`.source_engine)"
		if gotAlias != wantAlias {
			t.Errorf("row-alias schema-history upsert drifted:\n got: %s\nwant: %s", gotAlias, wantAlias)
		}
	})

	t.Run("migration_state SQL", func(t *testing.T) {
		s := newMigrationStateStore(nil, upsertValuesFunc)
		wantHdr := "INSERT INTO `sluice_migrate_state` " +
			"(migration_id, phase, table_progress, state_format, last_error) " +
			"VALUES (?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE " +
			"phase = VALUES(phase), " +
			"table_progress = VALUES(table_progress), " +
			"state_format = VALUES(state_format), " +
			"last_error = VALUES(last_error)"
		if got := s.shared.SQL.UpsertHeader; got != wantHdr {
			t.Errorf("mariadb migrate-state header upsert:\n got: %s\nwant: %s", got, wantHdr)
		}
		wantProg := "INSERT INTO `sluice_migrate_table_progress` " +
			"(migration_id, table_name, progress) " +
			"VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE progress = VALUES(progress)"
		if got := s.shared.SQL.UpsertProgressRow; got != wantProg {
			t.Errorf("mariadb migrate-state progress upsert:\n got: %s\nwant: %s", got, wantProg)
		}
		// Row-alias shapes stay byte-identical to the pre-item-73 statements.
		a := newMigrationStateStore(nil, upsertRowAlias)
		wantAliasHdr := "INSERT INTO `sluice_migrate_state` " +
			"(migration_id, phase, table_progress, state_format, last_error) " +
			"VALUES (?, ?, ?, ?, ?) AS new ON DUPLICATE KEY UPDATE " +
			"phase = new.phase, " +
			"table_progress = new.table_progress, " +
			"state_format = new.state_format, " +
			"last_error = new.last_error"
		if got := a.shared.SQL.UpsertHeader; got != wantAliasHdr {
			t.Errorf("row-alias migrate-state header upsert drifted:\n got: %s\nwant: %s", got, wantAliasHdr)
		}
	})

	t.Run("buildBatchUpsert", func(t *testing.T) {
		table := &ir.Table{
			Name: "users",
			Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64}},
				{Name: "email", Type: ir.Varchar{Length: 255}},
			},
			PrimaryKey: &ir.Index{Name: "PRIMARY", Columns: []ir.IndexColumn{{Column: "id"}}},
		}
		got := buildBatchUpsert(table, 2, []string{"id"}, upsertValuesFunc)
		want := "INSERT INTO `users` (`id`, `email`) VALUES (?, ?), (?, ?) ON DUPLICATE KEY UPDATE `email` = VALUES(`email`)"
		if got != want {
			t.Errorf("mariadb batch upsert:\n got: %s\nwant: %s", got, want)
		}
	})
}
