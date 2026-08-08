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

// TestSchemaDiffAfterMigrate_MySQLToPostgres_SetCheckIsPhantomDrift is a
// CHARACTERIZATION test and says so in its own name: it asserts the
// defect IS present.
//
// Item 158 closed the TYPE axis for a MySQL `SET` column — the expected
// side now names `Array<Text[long]>`, which is what a Postgres catalog
// reads back for the TEXT[] sluice lands. It did NOT close the CHECK
// axis: internal/engines/postgres emits `CONSTRAINT "<table>_<column>_set"
// CHECK (<column> <@ ARRAY[…])` beside that column to carry the source's
// member list, the expected side has nowhere to carry a table-level CHECK
// the SOURCE never had, and `diffChecks` reports it as ChecksExtra on
// every run.
//
// This is roadmap item 156's mirror. The obstacle measured there applies
// here too and only halfway: the constraint NAME is deterministic (unlike
// MySQL's positional `<table>_chk_N`), but PostgreSQL does not return the
// expression it was given — `("c_set" <@ ARRAY['x','y']::TEXT[])` reads
// back as `(c_set <@ ARRAY['x'::text, 'y'::text])` — so matching still
// needs an expression normalizer, and a name-only match would accept a
// TAMPERED predicate as in sync. That is the decision this test defers.
//
// INVERT THIS TEST when the CHECK axis is closed, in both directions: the
// partial-fix state, where the type is reconciled and the check is not, is
// the dangerous one because the remaining line looks like real drift.
func TestSchemaDiffAfterMigrate_MySQLToPostgres_SetCheckIsPhantomDrift(t *testing.T) {
	mysqlSource, _, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()
	_, pgTarget, pgCleanup := startPostgres(t)
	defer pgCleanup()

	applyMySQLDDL(t, mysqlSource, `
		CREATE TABLE prefs (
			id    INT NOT NULL PRIMARY KEY,
			flags SET('email','sms')
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
	td := findTableDiff(*diff, "prefs")
	if td == nil {
		t.Fatalf("no table diff for prefs at all — the SET membership CHECK residual this test characterizes "+
			"has been CLOSED. Invert this test: assert the diff is clean.\n%s", diff.Summary())
	}

	// The TYPE axis IS closed. If this fires, item 158's ir.Set arm is gone
	// and the test below would be characterizing the wrong defect.
	for _, cd := range td.ColumnsMismatched {
		if cd.Name == "flags" {
			t.Errorf("column flags reported type drift (expected=%s actual=%s); item 158's ir.Set arm closed "+
				"that axis and this test's premise is that only the CHECK remains", cd.ExpectedType, cd.ActualType)
		}
	}

	// The CHECK axis is NOT.
	if len(td.ChecksExtra) != 1 || td.ChecksExtra[0] != "prefs_flags_set" {
		t.Fatalf("expected exactly the phantom membership CHECK `prefs_flags_set` in checks_extra; got %v.\n"+
			"If it is now absent the residual is closed — invert this test and update roadmap item 158's "+
			"residual note. If a DIFFERENT name appeared, the emitter's constraint naming changed and every "+
			"already-migrated target will report the old name as extra AND the new one as missing",
			td.ChecksExtra)
	}
	t.Logf("characterized residual (item 156's mirror): checks_extra=%v", td.ChecksExtra)
}
