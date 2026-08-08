//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Roadmap item 153 — the pre-create shape gate's EXPECTED side, on a
// real MySQL server.
//
// The gate (ADR-0166, internal/pipeline/migrate_existing_tables.go) and
// `sluice schema diff` both build their expected side with
// translate.RetargetForEngine and compare it against the target
// catalog's read-back. Any family the retarget does not rewrite to the
// shape MySQL actually lands compares as DRIFT, and the gate refuses —
// so a second `migrate` over a target sluice itself created is refused
// on a column that is perfectly correct. On v0.116.1 that was every
// plain `json` column and every DOMAIN column.
//
// # What this reaches, stated so the name cannot be read as broader
//
// The PG → MySQL pair only, which is the one pair
// [translate.retargetRuleFor] has a rule table for. Within it: the
// `migrate` entry point's gate (the Streamer cold start shares the same
// [existingTablesGate] core — see streamer_shape_gate_mysql_integration_test.go)
// and the `schema diff` command. Geometry is NOT here — it needs
// PostGIS, so its cell lives in the postgis-tagged sibling
// migrate_pg_retarget_shape_postgis_integration_test.go, and this file's
// roster gate refuses to let that stay implied.
//
// # The independent expected value, named (the 2026-08-01 rule)
//
// The expected side is sluice's retarget; the actual side is MySQL's own
// information_schema, read back through the MySQL engine's SchemaReader
// — a different producer entirely, and the same one the gate itself
// consults. Nothing here compares sluice's output against sluice's other
// output. The re-run assertion is stronger still: its verdict is the
// server's, since a genuine shape conflict fails the CREATE or the copy.
package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"sort"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	irdiff "sluicesync.dev/sluice/internal/ir/diff"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/translate"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// retargetShapeExtensions are the PG extensions the fixture needs on
// both the source DDL and the source SchemaReader. Both ship in the
// stock contrib bundle the pre-baked image carries.
var retargetShapeExtensions = []string{"hstore", "citext"}

// retargetShapeSeedDDL declares every family the PG→MySQL retarget rule
// table touches, TWICE: once bare (table `rsplain`) and once through a
// `CREATE DOMAIN` wrapper (table `rsdom`), with identical column names
// so one roster grades both.
//
// The DOMAIN half is not decoration. `ir.Domain` is a WRAPPER, so a
// family that retargets correctly bare can still be un-retargeted
// through a domain — which is exactly the product axis Bug 233
// established and the half of item 153 that has nothing to do with
// `json`.
//
// The domains carry no CHECKs deliberately: `ir.Domain.String()`
// includes the check COUNT, so a wrapper with checks and a wrapper
// without produce different expected renderings, and this fixture must
// not accidentally pass by comparing the easy one. The CHECK-carrying
// wrapper is covered next door by domainBytesArraySeedDDL.
const retargetShapeSeedDDL = `
	CREATE DOMAIN rs_json   AS json;
	CREATE DOMAIN rs_jsonb  AS jsonb;
	CREATE DOMAIN rs_hstore AS hstore;
	CREATE DOMAIN rs_citext AS citext;
	CREATE DOMAIN rs_vcwide AS varchar(20000);
	CREATE DOMAIN rs_vcnarr AS varchar(64);
	CREATE DOMAIN rs_text   AS text;
	CREATE DOMAIN rs_bit    AS bit(8);
	CREATE DOMAIN rs_varbit AS bit varying(8);
	CREATE DOMAIN rs_uuid   AS uuid;
	CREATE DOMAIN rs_inet   AS inet;
	CREATE DOMAIN rs_cidr   AS cidr;
	CREATE DOMAIN rs_mac    AS macaddr;
	CREATE DOMAIN rs_jarr   AS json[];
	CREATE DOMAIN rs_tarr   AS text[];

	CREATE TABLE rsplain (
		id       INT PRIMARY KEY,
		c_json   json,
		c_jsonb  jsonb,
		c_hstore hstore,
		c_citext citext,
		c_vcwide varchar(20000),
		c_vcnarr varchar(64),
		c_text   text,
		c_bit    bit(8),
		c_varbit bit varying(8),
		c_uuid   uuid,
		c_inet   inet,
		c_cidr   cidr,
		c_mac    macaddr,
		c_jarr   json[],
		c_tarr   text[]
	);

	CREATE TABLE rsdom (
		id       INT PRIMARY KEY,
		c_json   rs_json,
		c_jsonb  rs_jsonb,
		c_hstore rs_hstore,
		c_citext rs_citext,
		c_vcwide rs_vcwide,
		c_vcnarr rs_vcnarr,
		c_text   rs_text,
		c_bit    rs_bit,
		c_varbit rs_varbit,
		c_uuid   rs_uuid,
		c_inet   rs_inet,
		c_cidr   rs_cidr,
		c_mac    rs_mac,
		c_jarr   rs_jarr,
		c_tarr   rs_tarr
	);

	INSERT INTO rsplain VALUES (
		1, '{"a":1}', '{"b":2}', '"k"=>"v"', 'MiXeD',
		repeat('w', 20000), 'narrow', 'plain text',
		B'10101010', B'0011',
		'0b7f1c4e-9d2a-4f3b-8c1d-5e6f7a8b9c0d',
		'10.1.2.3', '10.1.0.0/16', '08:00:2b:01:02:03',
		ARRAY['{"x":1}'::json], ARRAY['a','b']
	);

	INSERT INTO rsdom SELECT * FROM rsplain;
`

// retargetShapeFamilies is the roster this file grades: one entry per
// column in the fixture, naming the family it stands for. It is
// hand-written on purpose — it is the STATEMENT of coverage — and
// TestRetargetShapeRosterCoversTheRuleTable (unit, in
// internal/translate) is what stops it from silently falling behind the
// rule table.
var retargetShapeFamilies = []string{
	"c_json", "c_jsonb", "c_hstore", "c_citext",
	"c_vcwide", "c_vcnarr", "c_text",
	"c_bit", "c_varbit",
	"c_uuid", "c_inet", "c_cidr", "c_mac",
	"c_jarr", "c_tarr",
}

// TestMigrate_PGToMySQL_RetargetedShapeMatchesTheCatalogReadBack is item
// 153's gate. Three assertions over one migrated fixture, in increasing
// order of how close they sit to the operator:
//
//  1. per-column: the retargeted expected shape equals the MySQL
//     catalog's read-back, for every family × {bare, DOMAIN};
//  2. `sluice schema diff` reports no type drift on those columns —
//     the filing's claim that the command shares this comparison;
//  3. the one that matters: `migrate` run a SECOND time over the same
//     target does not refuse.
//
// (3) subsumes (1) operationally but not diagnostically: a failure in
// (3) alone says "some column differs", while (1) names which family
// and which type path.
func TestMigrate_PGToMySQL_RetargetedShapeMatchesTheCatalogReadBack(t *testing.T) {
	pgSource, _, pgCleanup := startPostgresWithExtensions(t, retargetShapeExtensions)
	defer pgCleanup()
	_, mysqlTarget, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()

	applyPGDDL(t, pgSource, retargetShapeSeedDDL)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	myEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	newMigrator := func(id string) *Migrator {
		return &Migrator{
			Source: pgEng, Target: myEng,
			SourceDSN: pgSource, TargetDSN: mysqlTarget,
			MigrationID:         id,
			EnabledPGExtensions: retargetShapeExtensions,
		}
	}

	if err := newMigrator("retarget-shape-first").Run(ctx); err != nil {
		t.Fatalf("first Migrator.Run PG→MySQL: %v", err)
	}

	t.Run("retargeted expected equals the catalog read-back", func(t *testing.T) {
		retargetShapeCase{
			sourceEngine: pgEng, sourceDSN: pgSource,
			targetEngine: myEng, targetDSN: mysqlTarget,
			extensions: retargetShapeExtensions,
			bareTable:  "rsplain", domainTable: "rsdom",
			families: retargetShapeFamilies,
		}.assertParity(t, ctx)
	})

	t.Run("schema diff reports no type drift", func(t *testing.T) {
		d := &Differ{
			Source: pgEng, Target: myEng,
			SourceDSN: pgSource, TargetDSN: mysqlTarget,
			EnabledPGExtensions: retargetShapeExtensions,
			Format:              "json",
			Out:                 io.Discard,
		}
		diff, err := d.Run(ctx)
		if err != nil {
			t.Fatalf("Differ.Run: %v", err)
		}
		for _, td := range diff.TablesMismatched {
			if td.Name != "rsplain" && td.Name != "rsdom" {
				continue
			}
			for _, cd := range td.ColumnsMismatched {
				if cd.ExpectedType == cd.ActualType {
					continue
				}
				t.Errorf("`schema diff` reports PHANTOM type drift on %s.%s: expected %q, target has %q — "+
					"the command shares the shape gate's retargeted expected side, so a family the retarget "+
					"does not rewrite is reported as drift on a column sluice itself created",
					td.Name, cd.Name, cd.ExpectedType, cd.ActualType)
			}
		}
	})

	// The operator's own assertion. The target keeps the tables the
	// first run created and loses their rows — the "re-run after
	// something went wrong" workflow, and the one shape that reaches the
	// pre-create gate at all (`--reset-target-data` DROPS the tables, so
	// it never gets there, and a populated target is refused earlier by
	// the Bug-9 cold-start preflight).
	t.Run("a second migrate over the same target does not refuse", func(t *testing.T) {
		db, err := sql.Open("mysql", mysqlTarget)
		if err != nil {
			t.Fatalf("open mysql target: %v", err)
		}
		defer func() { _ = db.Close() }()
		for _, tbl := range []string{"rsplain", "rsdom"} {
			if _, err := db.ExecContext(ctx, "TRUNCATE TABLE "+tbl); err != nil {
				t.Fatalf("truncate %s: %v", tbl, err)
			}
		}

		if err := newMigrator("retarget-shape-second").Run(ctx); err != nil {
			t.Fatalf("second Migrator.Run over the SAME target: %v\n\n"+
				"This is roadmap item 153's shape if it names SLUICE-E-TARGET-TABLE-SHAPE-MISMATCH: "+
				"the pre-create gate built its expected side with a retarget that has no arm for the "+
				"family named in the message, so it compared an un-retargeted expected against MySQL's "+
				"read-back and refused a table sluice itself created", err)
		}

		// The re-run must also have re-landed the rows — a gate that
		// "passes" by skipping every table would satisfy the line above.
		for _, tbl := range []string{"rsplain", "rsdom"} {
			var n int
			if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+tbl).Scan(&n); err != nil {
				t.Fatalf("count %s: %v", tbl, err)
			}
			if n != 1 {
				t.Errorf("after the second migrate, %s holds %d row(s); want 1 — the re-run was accepted but "+
					"copied nothing", tbl, n)
			}
		}
	})
}

// retargetShapeCase is the per-column half, factored out so the
// postgis-tagged geometry sibling grades its cell the same way rather
// than growing its own comparison.
type retargetShapeCase struct {
	sourceEngine ir.Engine
	sourceDSN    string
	targetEngine ir.Engine
	targetDSN    string

	// extensions are enabled on the SOURCE SchemaReader.
	extensions []string

	// bareTable / domainTable hold the same column roster, declared
	// directly and through a `CREATE DOMAIN` wrapper.
	bareTable   string
	domainTable string

	// families is the shared column roster — the STATEMENT of which
	// families this case grades.
	families []string
}

// assertParity rebuilds precisely what [existingTablesGate.plan] builds
// — the source schema through translate.RetargetForShapeCompare — and
// runs the gate's own irdiff.TableColumnShape against the target's
// catalog read-back. Any mismatch here IS the refusal the operator gets.
func (c retargetShapeCase) assertParity(t *testing.T, ctx context.Context) {
	t.Helper()

	source := readSchemaWithExtensions(t, ctx, c.sourceEngine, c.sourceDSN, c.extensions)
	actual := readSchemaWithExtensions(t, ctx, c.targetEngine, c.targetDSN, nil)
	expected := translate.RetargetForShapeCompare(source, c.sourceEngine.Name(), c.targetEngine.Name())

	expTables := tablesByNameForShape(expected)
	actTables := tablesByNameForShape(actual)

	graded := 0
	for _, name := range []string{c.bareTable, c.domainTable} {
		exp, ok := expTables[name]
		if !ok {
			t.Fatalf("source schema has no table %q — the fixture did not apply", name)
		}
		act, ok := actTables[name]
		if !ok {
			t.Fatalf("target has no table %q — the migrate did not create the fixture", name)
		}
		for _, mm := range irdiff.TableColumnShapeWithOptions(exp, act, irdiff.ShapeCompareOptions{}) {
			t.Errorf("%s.%s: retargeted expected %s, %s read back %s — the pre-create shape gate refuses "+
				"on exactly this comparison, so this column false-refuses every re-run",
				name, mm.Column, mm.Expected, c.targetEngine.Name(), mm.Actual)
		}
		graded += len(exp.Columns)
	}

	// Anti-vacuity: a fixture that failed to apply, or a reader that
	// returned an empty schema, would otherwise grade nothing and pass.
	if graded < 2*len(c.families) {
		t.Fatalf("only %d columns were graded; the fixture declares %d families in 2 type paths and this "+
			"assertion is grading almost nothing", graded, len(c.families))
	}
	c.assertFixtureCoversFamilies(t, tablesByNameForShape(source))
}

// assertFixtureCoversFamilies fails when a roster family is missing from
// the source schema the reader actually returned — the fixture DDL
// applying is not the same fact as the reader surfacing every column,
// and a family silently dropped at the read boundary would make its cell
// vacuous rather than failing. The domain table gets the extra check
// that its columns really did read back WRAPPED: if they stopped, it
// would silently become a second copy of the bare table and the whole
// type-path axis would vanish while every assertion passed.
func (c retargetShapeCase) assertFixtureCoversFamilies(t *testing.T, source map[string]*ir.Table) {
	t.Helper()
	for _, name := range []string{c.bareTable, c.domainTable} {
		tbl, ok := source[name]
		if !ok {
			t.Fatalf("source schema has no table %q", name)
		}
		have := make(map[string]ir.Type, len(tbl.Columns))
		for _, col := range tbl.Columns {
			have[col.Name] = col.Type
		}
		var missing, unwrapped []string
		for _, fam := range c.families {
			typ, ok := have[fam]
			if !ok {
				missing = append(missing, fam)
				continue
			}
			if _, isDomain := typ.(ir.Domain); name == c.domainTable && !isDomain {
				unwrapped = append(unwrapped, fam)
			}
		}
		if len(missing) > 0 {
			sort.Strings(missing)
			t.Fatalf("source table %q is missing roster column(s) %s — those cells graded nothing",
				name, strings.Join(missing, ", "))
		}
		if len(unwrapped) > 0 {
			sort.Strings(unwrapped)
			t.Fatalf("column(s) %s of %q did NOT read back as ir.Domain; the DOMAIN type path is not being "+
				"exercised for them", strings.Join(unwrapped, ", "), name)
		}
	}
}

func tablesByNameForShape(s *ir.Schema) map[string]*ir.Table {
	out := make(map[string]*ir.Table, len(s.Tables))
	for _, tbl := range s.Tables {
		if tbl != nil {
			out[tbl.Name] = tbl
		}
	}
	return out
}

// readSchemaWithExtensions opens an engine's SchemaReader, enables the
// named PG extensions when the reader is extension-aware, and reads.
func readSchemaWithExtensions(
	t *testing.T, ctx context.Context, eng ir.Engine, dsn string, extensions []string,
) *ir.Schema {
	t.Helper()
	rdr, err := eng.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader(%s): %v", eng.Name(), err)
	}
	defer migcore.CloseIf(rdr)
	if len(extensions) > 0 {
		if aware, ok := rdr.(ir.ExtensionAware); ok {
			if err := aware.EnableExtensions(ctx, extensions); err != nil {
				t.Fatalf("EnableExtensions(%s): %v", eng.Name(), err)
			}
		}
	}
	s, err := rdr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema(%s): %v", eng.Name(), err)
	}
	return s
}

// startPostgresWithExtensions is [startPostgresWithExtension] for more
// than one extension on the SOURCE database. The fixture needs hstore
// and citext together and the existing helper takes a single name.
func startPostgresWithExtensions(t *testing.T, extensions []string) (sourceDSN, targetDSN string, cleanup func()) {
	t.Helper()
	sourceDSN, targetDSN, cleanup = startPostgres(t)
	var ddl strings.Builder
	for _, ext := range extensions {
		fmt.Fprintf(&ddl, "CREATE EXTENSION IF NOT EXISTS %s;\n", ext)
	}
	applyPGDDL(t, sourceDSN, ddl.String())
	return sourceDSN, targetDSN, cleanup
}
