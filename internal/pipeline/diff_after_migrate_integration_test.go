//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The migrate-then-diff round trip (Bug 234).
//
// This is the operator's own assertion, and the reason it exists as an
// INTEGRATION test rather than a unit one: the independent expected
// value is the TARGET CATALOG's read-back of a schema `sluice migrate`
// itself just created. Nothing in the diff path produces that read-back,
// so a symmetric bug in the expected-side builder cannot make it agree.
//
// Bug 234 stayed invisible because every `schema diff` test either used a
// same-engine pair (both sides spell the primary key the same way) or
// hand-built the target DDL to match what the assertions expected — see
// TestDiff_PostgresToMySQL, whose fixture carries a primary key on both
// sides and never looks at the index slices. Migrating first is what
// removes the hand.
//
// Both directions are asserted for each pair, because a diff that stops
// reporting real drift to silence a false positive is far worse than the
// false positive:
//
//	CLEAN — immediately after migrate, no drift (the exit-0 shape).
//	DIRTY — after dropping a real index, a real primary key, or genuinely
//	        ALTERing a column's type on the target, that drift is still
//	        reported.
//
// Roadmap item 158 added the column-TYPE half for mysql→postgres and, with
// it, TestSchemaDiffAfterMigrate_MySQLToPostgres_TypeFamilyMatrix — the
// same round trip over every family internal/engines/mysql.translateType
// produces, which is where the rule table itself was derived from.

package pipeline

import (
	"bytes"
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	irdiff "sluicesync.dev/sluice/internal/ir/diff"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// runDiffForDrift runs the differ over an engine pair and returns the
// computed diff. Text format, so the renderer is exercised too — a panic
// or a malformed suggestion line for a role-matched primary key would
// surface here rather than only in the JSON shape.
func runDiffForDrift(ctx context.Context, t *testing.T, srcEngine, tgtEngine, srcDSN, tgtDSN string) *irdiff.SchemaDiff {
	t.Helper()
	src, ok := engines.Get(srcEngine)
	if !ok {
		t.Fatalf("%s engine not registered", srcEngine)
	}
	tgt, ok := engines.Get(tgtEngine)
	if !ok {
		t.Fatalf("%s engine not registered", tgtEngine)
	}
	var buf bytes.Buffer
	d := &Differ{
		Source:    src,
		Target:    tgt,
		SourceDSN: srcDSN,
		TargetDSN: tgtDSN,
		Format:    "text",
		Out:       &buf,
	}
	diff, err := d.Run(ctx)
	if err != nil {
		t.Fatalf("Differ.Run(%s -> %s): %v\noutput:\n%s", srcEngine, tgtEngine, err, buf.String())
	}
	if diff == nil {
		t.Fatalf("Differ.Run(%s -> %s) returned a nil diff", srcEngine, tgtEngine)
	}
	return diff
}

// assertNoDrift is the exit-0 shape: `sluice migrate` then `sluice schema
// diff` must agree, in full, on a target sluice itself just created.
//
// Both directions can make this assertion since roadmap item 158 wrote
// the mysql→postgres column-TYPE rule table. Through v0.117.0 that
// direction could only assert the narrower index-name half, because a
// MySQL `TEXT` landed as PG `TEXT` and read back as `Text[long]` against
// an expected `Text[regular]` — sixteen columns' worth of phantom type
// drift across four families.
func assertNoDrift(t *testing.T, diff *irdiff.SchemaDiff) {
	t.Helper()
	if diff.HasChanges() {
		t.Fatalf("schema diff reported drift against a target migrate just created: %s\n%+v",
			diff.Summary(), diff.TablesMismatched)
	}
}

// TestSchemaDiffAfterMigrate_PostgresToMySQL is the reported repro at
// the reported minimum size: a two-column table with nothing interesting
// in it. PostgreSQL names the key `plain_pkey`, MySQL calls it
// `PRIMARY`, and before the role match that pair alone produced
// `1 missing index, 1 extra index` and exit 1 on every run.
func TestSchemaDiffAfterMigrate_PostgresToMySQL(t *testing.T) {
	pgSource, _, pgCleanup := startPostgres(t)
	defer pgCleanup()
	_, mysqlTarget, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()

	applyPGDDL(t, pgSource, `
		CREATE TABLE plain (id INT PRIMARY KEY, v TEXT);
		INSERT INTO plain VALUES (1, 'a');

		CREATE TABLE keyed (
			id    INT PRIMARY KEY,
			email VARCHAR(120) NOT NULL
		);
		CREATE INDEX idx_email ON keyed (email);

		-- Row-level security is Postgres-only. MySQL's SchemaWriter WARNs
		-- once and creates the table without it, and MySQL's SchemaReader
		-- can never report it back — so before the target-cannot-hold
		-- suppression this table alone made every post-migrate diff of this
		-- pair report RLSMismatched plus a missing policy, permanently, with
		-- no action on the target able to close it (Bug 234's deferred list).
		--
		-- ENABLE without FORCE deliberately: FORCE subjects the OWNER to the
		-- policy, which would make the fixture's own bulk copy depend on the
		-- predicate and turn a diff test into a row-visibility test.
		CREATE TABLE secured (
			id        INT PRIMARY KEY,
			tenant_id INT NOT NULL
		);
		ALTER TABLE secured ENABLE ROW LEVEL SECURITY;
		CREATE POLICY tenant_isolation ON secured USING (tenant_id > 0);

		-- Bug 241: user CHECKs whose predicates the target server
		-- RE-RENDERS — a numeric literal wearing a cast (PG stores
		-- (0)::numeric, MySQL cast(0 as decimal(10,0))) and length()
		-- (MySQL renders char_length()) — used to phantom as CHECK
		-- mismatches on the target migrate itself just created. The
		-- CLEAN assertion below is the pin.
		CREATE TABLE bounded (
			id INT PRIMARY KEY,
			v  NUMERIC(12,3),
			s  VARCHAR(20),
			CONSTRAINT ck_v   CHECK (v > 0),
			CONSTRAINT ck_len CHECK (length(s) > 2)
		);
	`)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	mig := &Migrator{Source: pgEng, Target: mysqlEng, SourceDSN: pgSource, TargetDSN: mysqlTarget}
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run: %v", err)
	}

	// CLEAN: the whole point — `migrate` then `schema diff` must agree.
	diff := runDiffForDrift(ctx, t, "postgres", "mysql", pgSource, mysqlTarget)
	assertNoDrift(t, diff)

	// DIRTY (secondary index): a genuinely dropped index is still drift.
	applyMySQLDDL(t, mysqlTarget, "DROP INDEX `idx_email` ON `keyed`")
	diff = runDiffForDrift(ctx, t, "postgres", "mysql", pgSource, mysqlTarget)
	if !diff.HasChanges() {
		t.Fatal("schema diff reported NO drift after a real index was dropped on the target")
	}
	td := findTableDiff(*diff, "keyed")
	if td == nil || len(td.IndexesMissing) != 1 || td.IndexesMissing[0] != "idx_email" {
		t.Fatalf("expected idx_email in indexes_missing; got %+v", td)
	}

	// DIRTY (primary key): the harder half — the object that is now
	// matched by ROLE has to remain reportable when it is genuinely
	// absent.
	applyMySQLDDL(t, mysqlTarget, "ALTER TABLE `plain` DROP PRIMARY KEY")
	diff = runDiffForDrift(ctx, t, "postgres", "mysql", pgSource, mysqlTarget)
	td = findTableDiff(*diff, "plain")
	if td == nil || len(td.IndexesMissing) != 1 || td.IndexesMissing[0] != "plain_pkey" {
		t.Fatalf("expected plain_pkey in indexes_missing after DROP PRIMARY KEY; got %+v", td)
	}

	// DIRTY (Bug 241's guard direction): the canonical comparison folds
	// server RENDERINGS, not predicates — a WEAKENED name-matched user
	// CHECK must still report.
	applyMySQLDDL(t, mysqlTarget, "ALTER TABLE `bounded` DROP CHECK `ck_v`")
	applyMySQLDDL(t, mysqlTarget, "ALTER TABLE `bounded` ADD CONSTRAINT `ck_v` CHECK (`v` > -1000)")
	diff = runDiffForDrift(ctx, t, "postgres", "mysql", pgSource, mysqlTarget)
	td = findTableDiff(*diff, "bounded")
	if td == nil || len(td.ChecksMismatched) != 1 || td.ChecksMismatched[0].Name != "ck_v" {
		t.Fatalf("a WEAKENED name-matched user CHECK went unreported — the Bug 241 fold has over-folded: %+v", td)
	}
}

// TestSchemaDiffAfterMigrate_MySQLToPostgres is the sibling direction,
// and it carries the OTHER half of the index-name axis: the Postgres
// writer table-prefixes a secondary index name (`idx_email` becomes
// `keyed_idx_email`), so before the expected side mirrored that
// transformation this pair reported one missing + one extra index for
// every non-prefixed index on every table, on top of the primary-key
// pair. `keyed_created_idx` is the control — already table-prefixed, so
// it passes through untouched and proves the rule is a transformation
// and not a blanket rename.
func TestSchemaDiffAfterMigrate_MySQLToPostgres(t *testing.T) {
	mysqlSource, _, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()
	_, pgTarget, pgCleanup := startPostgres(t)
	defer pgCleanup()

	applyMySQLDDL(t, mysqlSource, `
		CREATE TABLE plain (
			id INT NOT NULL PRIMARY KEY,
			v  TEXT
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

		CREATE TABLE keyed (
			id       INT NOT NULL PRIMARY KEY,
			email    VARCHAR(120) NOT NULL,
			nickname VARCHAR(120) NOT NULL,
			KEY idx_email (email),
			KEY keyed_nickname_idx (nickname)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

		-- Bug 241, this direction's arms: MySQL stores the numeric CHECK
		-- with cast(0 as decimal(10,0)) and length() as char_length();
		-- the PG target re-renders them as (0)::numeric and length().
		CREATE TABLE bounded (
			id INT NOT NULL PRIMARY KEY,
			v  DECIMAL(12,3),
			s  VARCHAR(20),
			CONSTRAINT ck_v   CHECK (v > 0),
			CONSTRAINT ck_len CHECK (char_length(s) > 2)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

		-- DIFF-2 (measured 2026-08-14): the fold corpus — every portable
		-- CHECK family the measurement pass found the two servers
		-- re-render differently, in one table, migrated for real. The CLEAN
		-- assertion below is the e2e half of the fold pins; the verbatim
		-- measured rendering pairs live in the check_expr_norm unit matrix.
		-- (12 constraints for 13 measured families: ceil and ceiling are one
		-- arm here because MySQL renders BOTH spellings back as ceiling —
		-- the spelling pair itself is pinned in the unit matrix.)
		-- ck_f_round_dbl is Bug 252's exact repro (v0.126.0 cycle): MySQL
		-- materializes round(d2) as round(d2, 0), and PG's TWO-arg round
		-- is numeric-only — so on the DOUBLE column the passed-through
		-- CHECK died at CREATE TABLE (42883) while preview said nothing.
		-- The translator now strips the materialized zero scale; this
		-- arm pins the DOUBLE member of the class beside the DECIMAL
		-- representative (the pin-the-class rule — the two columns hit
		-- DIFFERENT PG overload sets on an identical sluice code path).
		CREATE TABLE folded (
			id INT NOT NULL PRIMARY KEY,
			v  INT,
			w  INT,
			s  VARCHAR(20),
			d  DECIMAL(10,2),
			d2 DOUBLE,
			CONSTRAINT ck_f_round_dbl CHECK (round(d2) >= -100000),
			CONSTRAINT ck_f_modfn    CHECK (mod(v, 3) >= 0),
			CONSTRAINT ck_f_between  CHECK (v BETWEEN 1 AND 10),
			CONSTRAINT ck_f_not      CHECK (NOT (v = 5)),
			CONSTRAINT ck_f_like     CHECK (s LIKE 'a%'),
			CONSTRAINT ck_f_notlike  CHECK (s NOT LIKE 'z%'),
			CONSTRAINT ck_f_substr   CHECK (substring(s, 1, 2) <> 'zz'),
			CONSTRAINT ck_f_trim     CHECK (trim(s) = s),
			CONSTRAINT ck_f_nullif   CHECK (nullif(v, -1) IS NOT NULL),
			CONSTRAINT ck_f_round    CHECK (round(d) >= -100000),
			CONSTRAINT ck_f_floor    CHECK (floor(d) >= -100000),
			CONSTRAINT ck_f_ceil     CHECK (ceil(d) >= -100000),
			CONSTRAINT ck_f_greatest CHECK (greatest(v, w) >= -100000)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

		-- DIFF-2 translator pairs (the residue the fold deliberately did
		-- not cover, closed by Options.TranslateExpectedCheckExpr): the
		-- constructs the MySQL->PG expression translator REWRITES, so the
		-- target catalog holds a different spelling than any fold could
		-- reconcile. The diff now renders the expected side through the
		-- writer's own translator; these must diff CLEAN after a real
		-- migrate. ck_t_log is the 2026-08-14 overload corpus's one
		-- SILENT cell (MySQL log = natural, PG log = base-10), landed
		-- correctly as ln() by the same translator.
		CREATE TABLE translated (
			id INT NOT NULL PRIMARY KEY,
			j  JSON,
			dt DATETIME,
			d2 DOUBLE,
			CONSTRAINT ck_t_jext    CHECK (json_extract(j, '$.k') IS NOT NULL),
			CONSTRAINT ck_t_junq    CHECK (json_unquote(json_extract(j, '$.k')) <> 'x'),
			CONSTRAINT ck_t_datefmt CHECK (date_format(dt, '%Y') <> '1900'),
			CONSTRAINT ck_t_log     CHECK (log(d2 + 100001) >= 0)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`)

	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	mig := &Migrator{Source: mysqlEng, Target: pgEng, SourceDSN: mysqlSource, TargetDSN: pgTarget}
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run: %v", err)
	}

	diff := runDiffForDrift(ctx, t, "mysql", "postgres", mysqlSource, pgTarget)
	assertNoDrift(t, diff)

	// DIRTY: the PREFIXED name is the one the target holds, so it is the
	// one the report must name — an operator who copy-pastes it has to
	// get an index that makes the next diff clean.
	applyPGDDL(t, pgTarget, `DROP INDEX "keyed_idx_email"`)
	diff = runDiffForDrift(ctx, t, "mysql", "postgres", mysqlSource, pgTarget)
	if !diff.HasChanges() {
		t.Fatal("schema diff reported NO drift after a real index was dropped on the target")
	}
	td := findTableDiff(*diff, "keyed")
	if td == nil || len(td.IndexesMissing) != 1 || td.IndexesMissing[0] != "keyed_idx_email" {
		t.Fatalf("expected keyed_idx_email in indexes_missing; got %+v", td)
	}

	// DIRTY (primary key), Postgres side. The expected side is the MySQL
	// source's spelling, so the report names `PRIMARY`.
	applyPGDDL(t, pgTarget, `ALTER TABLE "plain" DROP CONSTRAINT "plain_pkey"`)
	diff = runDiffForDrift(ctx, t, "mysql", "postgres", mysqlSource, pgTarget)
	td = findTableDiff(*diff, "plain")
	if td == nil || len(td.IndexesMissing) != 1 || td.IndexesMissing[0] != "PRIMARY" {
		t.Fatalf("expected PRIMARY in indexes_missing after DROP CONSTRAINT; got %+v", td)
	}

	// DIRTY (DIFF-2's guard direction, ×2): the fold set must not have
	// widened the false-clean window into meaning. A BETWEEN bound
	// widened on the target, and a LIKE pattern changed on the target,
	// must each still report under the same constraint name.
	applyPGDDL(t, pgTarget, `ALTER TABLE "folded" DROP CONSTRAINT "ck_f_between"`)
	applyPGDDL(t, pgTarget, `ALTER TABLE "folded" ADD CONSTRAINT "ck_f_between" CHECK (v >= 1 AND v <= 100)`)
	diff = runDiffForDrift(ctx, t, "mysql", "postgres", mysqlSource, pgTarget)
	td = findTableDiff(*diff, "folded")
	if td == nil || len(td.ChecksMismatched) != 1 || td.ChecksMismatched[0].Name != "ck_f_between" {
		t.Fatalf("a WIDENED between bound under the same name went unreported — the DIFF-2 between fold has over-folded: %+v", td)
	}
	applyPGDDL(t, pgTarget, `ALTER TABLE "folded" DROP CONSTRAINT "ck_f_between"`)
	applyPGDDL(t, pgTarget, `ALTER TABLE "folded" ADD CONSTRAINT "ck_f_between" CHECK (v >= 1 AND v <= 10)`)

	applyPGDDL(t, pgTarget, `ALTER TABLE "folded" DROP CONSTRAINT "ck_f_like"`)
	applyPGDDL(t, pgTarget, `ALTER TABLE "folded" ADD CONSTRAINT "ck_f_like" CHECK (s LIKE 'b%')`)
	diff = runDiffForDrift(ctx, t, "mysql", "postgres", mysqlSource, pgTarget)
	td = findTableDiff(*diff, "folded")
	if td == nil || len(td.ChecksMismatched) != 1 || td.ChecksMismatched[0].Name != "ck_f_like" {
		t.Fatalf("a CHANGED like pattern under the same name went unreported — the DIFF-2 like fold has over-folded: %+v", td)
	}
	applyPGDDL(t, pgTarget, `ALTER TABLE "folded" DROP CONSTRAINT "ck_f_like"`)
	applyPGDDL(t, pgTarget, `ALTER TABLE "folded" ADD CONSTRAINT "ck_f_like" CHECK (s LIKE 'a%')`)

	// DIRTY (translator-pair guard): the translate-expected hook must
	// not certify a genuinely different predicate. A changed JSON path
	// under the same name still reports through the translation.
	applyPGDDL(t, pgTarget, `ALTER TABLE "translated" DROP CONSTRAINT "ck_t_jext"`)
	applyPGDDL(t, pgTarget, `ALTER TABLE "translated" ADD CONSTRAINT "ck_t_jext" CHECK ((j -> 'OTHER') IS NOT NULL)`)
	diff = runDiffForDrift(ctx, t, "mysql", "postgres", mysqlSource, pgTarget)
	td = findTableDiff(*diff, "translated")
	if td == nil || len(td.ChecksMismatched) != 1 || td.ChecksMismatched[0].Name != "ck_t_jext" {
		t.Fatalf("a CHANGED JSON path under the same name went unreported — the translate-expected hook has over-folded: %+v", td)
	}
}

// TestSchemaDiffAfterMigrate_PostgresToPostgres_RowLevelSecurityStillCompared
// is the over-suppression half of the RLS fix, at the WIRING layer.
//
// `Differ.Run` decides whether to compare row-level security from
// [ir.Capabilities.PostgresBackend]. The unit pins in internal/ir/diff
// grade the comparison given the option; nothing there grades the
// DERIVATION, so a wiring that suppressed unconditionally would pass every
// one of them — two facts with nothing binding them, which is the gap
// CLAUDE.md's premise-naming step names.
//
// This binds them: a real Postgres target, migrated by sluice with its RLS
// intact, then genuinely stripped. That drift is the silent-loss the B-10
// comparison exists for (every tenant reads every tenant's rows), and it
// must survive the suppression a MySQL target gets.
func TestSchemaDiffAfterMigrate_PostgresToPostgres_RowLevelSecurityStillCompared(t *testing.T) {
	pgSource, pgTarget, pgCleanup := startPostgres(t)
	defer pgCleanup()

	applyPGDDL(t, pgSource, `
		CREATE TABLE secured (
			id        INT PRIMARY KEY,
			tenant_id INT NOT NULL
		);
		ALTER TABLE secured ENABLE ROW LEVEL SECURITY;
		CREATE POLICY tenant_isolation ON secured USING (tenant_id > 0);
	`)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	mig := &Migrator{Source: pgEng, Target: pgEng, SourceDSN: pgSource, TargetDSN: pgTarget}
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run: %v", err)
	}

	assertNoDrift(t, runDiffForDrift(ctx, t, "postgres", "postgres", pgSource, pgTarget))

	// DIRTY, the policy: a dropped policy is a widened predicate.
	applyPGDDL(t, pgTarget, `DROP POLICY tenant_isolation ON secured`)
	diff := runDiffForDrift(ctx, t, "postgres", "postgres", pgSource, pgTarget)
	td := findTableDiff(*diff, "secured")
	if td == nil || len(td.PoliciesMissing) != 1 || td.PoliciesMissing[0] != "tenant_isolation" {
		t.Fatalf("a policy dropped on a POSTGRES target went unreported; the target-cannot-hold-RLS "+
			"suppression has reached a target that CAN hold it: %+v", td)
	}

	// DIRTY, the flag: the headline B-10 case.
	applyPGDDL(t, pgTarget, `ALTER TABLE secured DISABLE ROW LEVEL SECURITY`)
	diff = runDiffForDrift(ctx, t, "postgres", "postgres", pgSource, pgTarget)
	td = findTableDiff(*diff, "secured")
	if td == nil || !td.RLSMismatched {
		t.Fatalf("row-level security switched OFF on a POSTGRES target reported IN SYNC — every tenant now "+
			"reads every tenant's rows and `schema diff` says nothing: %+v", td)
	}
}

// mysqlTypeFamilyMatrixDDL carries one column of EVERY arm of
// internal/engines/mysql.translateType, which is what makes this the
// derivation of the item-158 rule table rather than a spot check.
//
// Two arms are deliberately absent and both are named rather than
// implied. ir.Geometry needs a PostGIS target and belongs in a
// postgis-tagged sibling; ir.Set gets its own test below because the
// membership CHECK sluice emits beside it is a separate, still-open axis.
// The MariaDB-native UUID / INET4 / INET6 arms are unreachable on a
// vanilla MySQL 8 source and are pinned in the translate unit matrix.
const mysqlTypeFamilyMatrixDDL = `
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
		c_bigint      BIGINT,
		c_bigint_u    BIGINT UNSIGNED,
		c_year        YEAR,
		c_decimal     DECIMAL(10,2),
		c_float       FLOAT,
		c_double      DOUBLE,
		c_bit8        BIT(8),
		c_char        CHAR(10),
		c_varchar     VARCHAR(50),
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
		c_time        TIME(3),
		c_datetime    DATETIME(6),
		c_ts          TIMESTAMP NULL,
		c_ts6         TIMESTAMP(6) NULL,
		c_enum        ENUM('red','green'),
		c_json        JSON,
		d_int         INT NOT NULL DEFAULT 7,
		d_varchar     VARCHAR(20) NOT NULL DEFAULT 'hi',
		d_ts          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		g_stored      INT AS (c_int + 1) STORED
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
`

// TestSchemaDiffAfterMigrate_MySQLToPostgres_TypeFamilyMatrix is the
// independent evidence for roadmap item 158's rule table, and the reason
// the table was DERIVED rather than mirrored from
// [translate.retargetPGtoMySQL].
//
// The independent expected value, named (the 2026-08-01 rule): the
// PostgreSQL CATALOG's own read-back of a schema `sluice migrate` just
// created. Nothing in the diff path produces that read-back, so a
// symmetric error in the expected-side builder cannot make it agree —
// which is exactly what a unit test over hand-written `want` types
// cannot promise, since the same hand writes both sides.
//
// Sixteen of these columns moved shape before item 158, across four
// families: integers (no unsigned, three widths), the TEXT tiers, the
// whole binary family, and SET. Each family gets a paired DIRTY case
// below, because a rule that silences a phantom line by also silencing
// real drift is strictly worse than the phantom.
func TestSchemaDiffAfterMigrate_MySQLToPostgres_TypeFamilyMatrix(t *testing.T) {
	mysqlSource, _, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()
	_, pgTarget, pgCleanup := startPostgres(t)
	defer pgCleanup()

	applyMySQLDDL(t, mysqlSource, mysqlTypeFamilyMatrixDDL)

	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	mig := &Migrator{Source: mysqlEng, Target: pgEng, SourceDSN: mysqlSource, TargetDSN: pgTarget}
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run: %v", err)
	}

	// CLEAN — the operator's assertion, over every family at once.
	assertNoDrift(t, runDiffForDrift(ctx, t, "mysql", "postgres", mysqlSource, pgTarget))

	// DIRTY — one genuine ALTER per rewritten family. Each is a change an
	// operator could really make, and each is one the corresponding rule
	// could have swallowed: the integer collapse could hide a re-widening,
	// the TEXT-tier collapse could hide a length constraint appearing, and
	// the binary collapse could hide a bytea column becoming text.
	for _, tc := range []struct {
		name   string
		alter  string
		column string
	}{
		{
			"integer width re-widened on the target",
			`ALTER TABLE fam ALTER COLUMN c_tinyint TYPE BIGINT`,
			"c_tinyint",
		},
		{
			"unsigned column narrowed back on the target",
			`ALTER TABLE fam ALTER COLUMN c_int_u TYPE INTEGER`,
			"c_int_u",
		},
		{
			"TEXT gained a length constraint on the target",
			`ALTER TABLE fam ALTER COLUMN c_text TYPE VARCHAR(20)`,
			"c_text",
		},
		{
			"binary column became text on the target",
			`ALTER TABLE fam ALTER COLUMN c_varbinary TYPE TEXT USING encode(c_varbinary, 'hex')`,
			"c_varbinary",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			applyPGDDL(t, pgTarget, tc.alter)
			diff := runDiffForDrift(ctx, t, "mysql", "postgres", mysqlSource, pgTarget)
			td := findTableDiff(*diff, "fam")
			if td == nil {
				t.Fatalf("no table diff for fam after %q — a REAL column-type change went unreported, which is "+
					"the failure mode item 158's rule table must not have introduced", tc.alter)
			}
			var found bool
			for _, cd := range td.ColumnsMismatched {
				if cd.Name == tc.column {
					found = true
					t.Logf("reported: %s expected=%s actual=%s", cd.Name, cd.ExpectedType, cd.ActualType)
				}
			}
			if !found {
				t.Errorf("column %q is NOT in columns_mismatched after %q. `sluice schema diff` would tell the "+
					"operator this target is in sync while its column type has genuinely changed — a "+
					"compare-lane rule has over-reached.\nreported: %+v", tc.column, tc.alter, td.ColumnsMismatched)
			}
		})
	}
}

// TestSchemaDiffAfterMigrate_MySQLToPostgres_EmittedChecksAreReconciled
// closes the CHECK axis item 158 left open — the mirror of roadmap item
// 156, one target over — and it carries BOTH constraints the Postgres
// writer synthesizes, not one.
//
// This file's earlier version was a CHARACTERIZATION pin asserting the
// defect was present, and it instructed its own inversion. Inverted here,
// with the sibling it named only implicitly.
//
// # The two, and why one would not have done
//
// internal/engines/postgres appends TWO kinds of table-level CHECK the
// source never declared, in two separate emitter loops:
//
//   - `<t>_<c>_set` for a MySQL `SET` column, carrying the member list
//     the column loses when it lands as TEXT[];
//   - `<t>_<c>_enum_chk` for a GENERATED enum column (Bug 25), carrying
//     the value list the column loses when it lands as TEXT.
//
// Item 158's own residual note named only the first. The second has the
// identical shape and was reported as phantom drift the identical way —
// plus a phantom TYPE line, since a generated enum column reads back as
// `Text[long]` against an expected `Enum{…}`. That is a COLUMN-level
// emitter effect, not a type-level one, which is why it needed
// translate's target-emit-shape pass rather than an arm in a rule table.
//
// # Both directions, because the alternative is a blindfold
//
// PostgreSQL does not return the expression it was given — `("flags" <@
// ARRAY['email','sms']::TEXT[])` reads back as `(flags <@ ARRAY['email'::
// text, 'sms'::text])`, and an `IN (…)` list comes back as `= ANY
// (ARRAY[…])` — so the match is on a canonicalized predicate. A name-only
// match would have been cheaper and would have accepted a TAMPERED
// membership list as in sync; the DIRTY halves are what rule that out.
func TestSchemaDiffAfterMigrate_MySQLToPostgres_EmittedChecksAreReconciled(t *testing.T) {
	mysqlSource, _, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()
	_, pgTarget, pgCleanup := startPostgres(t)
	defer pgCleanup()

	applyMySQLDDL(t, mysqlSource, `
		CREATE TABLE prefs (
			id     INT NOT NULL PRIMARY KEY,
			flags  SET('email','sms'),
			mood   ENUM('happy','sad'),
			g_mood ENUM('happy','sad') AS (mood) STORED
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`)

	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	mig := &Migrator{Source: mysqlEng, Target: pgEng, SourceDSN: mysqlSource, TargetDSN: pgTarget}
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run: %v", err)
	}

	// Anti-vacuity, asked of PostgreSQL's catalog rather than of sluice:
	// a run where the emitter created NO constraints would produce a
	// clean diff for the wrong reason, which is the shape that makes a
	// gate worse than none.
	landed := pgCheckConstraintNames(t, ctx, pgTarget, "prefs")
	if len(landed) != 2 {
		t.Fatalf("pg_constraint reports %d CHECK constraint(s) on `prefs` (%v); want exactly 2 — the SET "+
			"membership CHECK and the Bug-25 generated-enum CHECK. `mood` is the control: a NON-generated "+
			"enum column keeps the native PG enum type and needs no CHECK, so a count of 3 means the emitter "+
			"has started constraining it and this pin's reasoning no longer holds", len(landed), landed)
	}

	assertNoDrift(t, runDiffForDrift(ctx, t, "mysql", "postgres", mysqlSource, pgTarget))

	// DIRTY 1 — the membership CHECK DROPPED. The target now accepts any
	// text array, including members the MySQL source would reject.
	applyPGDDL(t, pgTarget, `ALTER TABLE "prefs" DROP CONSTRAINT "prefs_flags_set"`)
	diff := runDiffForDrift(ctx, t, "mysql", "postgres", mysqlSource, pgTarget)
	td := findTableDiff(*diff, "prefs")
	if td == nil || len(td.ChecksMissing) != 1 || td.ChecksMissing[0] != "prefs_flags_set" {
		t.Fatalf("a DROPPED SET-membership CHECK went unreported — the emitted-check attachment has become a "+
			"blanket suppression of target-side CHECKs: %+v", td)
	}

	// DIRTY 2 — re-added under the same name with an EXTRA member. Same
	// name, different predicate: exactly what a name-only match would
	// have accepted.
	applyPGDDL(t, pgTarget,
		`ALTER TABLE "prefs" ADD CONSTRAINT "prefs_flags_set" CHECK ("flags" <@ ARRAY['email','sms','fax']::TEXT[])`)
	diff = runDiffForDrift(ctx, t, "mysql", "postgres", mysqlSource, pgTarget)
	td = findTableDiff(*diff, "prefs")
	if td == nil || (len(td.ChecksMismatched) == 0 && len(td.ChecksMissing) == 0) {
		t.Fatalf("a WIDENED SET-membership CHECK reported as in sync. The comparison is matching sluice-emitted "+
			"constraints by NAME rather than by predicate, so a target that accepts 'fax' while the source "+
			"cannot store it looks clean: %+v", td)
	}
	t.Logf("tampered-check report: mismatched=%+v missing=%v extra=%v",
		td.ChecksMismatched, td.ChecksMissing, td.ChecksExtra)

	// DIRTY 3 — the generated-enum value list widened, the sibling
	// constraint, tampered the same way. Enumerated rather than assumed
	// to travel with its sibling: item 158 is the precedent for a fix
	// reaching one of these and silently missing the other.
	applyPGDDL(t, pgTarget, `ALTER TABLE "prefs" DROP CONSTRAINT "prefs_g_mood_enum_chk"`)
	applyPGDDL(t, pgTarget,
		`ALTER TABLE "prefs" ADD CONSTRAINT "prefs_g_mood_enum_chk" CHECK ("g_mood" IN ('happy','sad','livid'))`)
	diff = runDiffForDrift(ctx, t, "mysql", "postgres", mysqlSource, pgTarget)
	td = findTableDiff(*diff, "prefs")
	if td == nil || (len(td.ChecksMismatched) == 0 && len(td.ChecksMissing) == 0) {
		t.Fatalf("a WIDENED generated-enum CHECK reported as in sync: %+v", td)
	}
}

// pgCheckConstraintNames asks PostgreSQL's own catalog which CHECK
// constraints a table carries. Deliberately not routed through sluice's
// SchemaReader: this is the independent evidence the clean-diff
// assertion rests on, and reading it with a component under test would
// put both halves on one producer.
func pgCheckConstraintNames(t *testing.T, ctx context.Context, dsn, table string) []string {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open pg target: %v", err)
	}
	defer func() { _ = db.Close() }()

	const q = `
		SELECT con.conname
		FROM   pg_constraint con
		JOIN   pg_class rel ON rel.oid = con.conrelid
		JOIN   pg_namespace n ON n.oid = rel.relnamespace
		WHERE  con.contype = 'c' AND n.nspname = 'public' AND rel.relname = $1
		ORDER  BY con.conname`
	rows, err := db.QueryContext(ctx, q, table)
	if err != nil {
		t.Fatalf("list target CHECK constraints: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan constraint name: %v", err)
		}
		out = append(out, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate constraint names: %v", err)
	}
	return out
}

// TestSchemaDiffAfterMigrate_Postgres_NotInGuttedConstraintReportsDrift
// is the H-1 durable gate (audit 2026-08-14, silent-loss): PostgreSQL
// renders `CHECK (i NOT IN (1,2))` as `CHECK ((NOT (i = ANY (ARRAY[1,
// 2]))))`. Operator negation across the quantifier is INVALID — the
// gutted target `CHECK (i <> ANY (ARRAY[1,2]))` ACCEPTS 1 and 2 where the
// source REJECTS them — and a fold that pushed the NOT onto the operator
// would canonicalize the two equal, so `schema diff` would certify a
// materially different (data-admitting) CHECK as in sync. This grounds
// the fix against real PG's own rendering: migrate a NOT IN constraint,
// gut it on the target, and assert the diff REPORTS the mismatch.
func TestSchemaDiffAfterMigrate_Postgres_NotInGuttedConstraintReportsDrift(t *testing.T) {
	pgSource, pgTarget, pgCleanup := startPostgres(t)
	defer pgCleanup()

	applyPGDDL(t, pgSource, `
		CREATE TABLE guarded (
			id INT PRIMARY KEY,
			i  INT,
			CONSTRAINT ck_notin CHECK (i NOT IN (1, 2))
		);
	`)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	mig := &Migrator{Source: pgEng, Target: pgEng, SourceDSN: pgSource, TargetDSN: pgTarget}
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run: %v", err)
	}

	// CLEAN: the migrated NOT IN constraint diffs in sync against itself.
	assertNoDrift(t, runDiffForDrift(ctx, t, "postgres", "postgres", pgSource, pgTarget))

	// GUT IT: replace the target constraint with the operator-negated
	// form that a quantifier-blind fold would have accepted. It admits
	// i=1 and i=2 — the exact rows the source rejects — so the diff MUST
	// report drift.
	applyPGDDL(t, pgTarget, `ALTER TABLE "guarded" DROP CONSTRAINT "ck_notin"`)
	applyPGDDL(t, pgTarget, `ALTER TABLE "guarded" ADD CONSTRAINT "ck_notin" CHECK (i <> ANY (ARRAY[1, 2]))`)
	diff := runDiffForDrift(ctx, t, "postgres", "postgres", pgSource, pgTarget)
	td := findTableDiff(*diff, "guarded")
	if td == nil || (len(td.ChecksMismatched) == 0 && len(td.ChecksMissing) == 0 && len(td.ChecksExtra) == 0) {
		t.Fatalf("a GUTTED NOT IN constraint (i <> ANY vs NOT (i = ANY)) reported IN SYNC — schema diff would certify a "+
			"data-admitting CHECK as unchanged (H-1 silent-loss): %+v", td)
	}
	// Sanity: on real PG the gutted constraint genuinely admits i=1.
	if _, err := execPGReturningErr(t, pgTarget, `INSERT INTO "guarded" (id, i) VALUES (100, 1)`); err != nil {
		t.Fatalf("the gutted target constraint should ACCEPT i=1 (that is why it is a silent-loss); insert failed: %v", err)
	}
}

// execPGReturningErr runs one statement and returns its error (nil on
// success) without failing the test — for asserting a constraint's
// accept/reject behaviour directly.
func execPGReturningErr(t *testing.T, dsn, stmt string) (sql.Result, error) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return db.ExecContext(ctx, stmt)
}
