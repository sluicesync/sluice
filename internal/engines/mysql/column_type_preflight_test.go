// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"reflect"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/translate"
	"sluicesync.dev/sluice/internal/typematrix"
)

// preflightFlavors is every registered MySQL-dialect flavor. The gates below
// run over ALL of them rather than a representative, because the emitter this
// preflight delegates to is flavor-parameterised — the collation remap and the
// MariaDB geometry spelling both come from the flavor — and a green matrix on
// vanilla says nothing about MariaDB. Same Bug-74 reasoning, one axis over from
// types.
var preflightFlavors = []Flavor{FlavorVanilla, FlavorPlanetScale, FlavorVitess, FlavorMariaDB}

func oneColumnSchema(t ir.Type) (*ir.Schema, *ir.Table) {
	tbl := &ir.Table{
		Name:    "t",
		Columns: []*ir.Column{{Name: "c", Type: t, Nullable: true}},
	}
	return &ir.Schema{Tables: []*ir.Table{tbl}}, tbl
}

// THE PREFLIGHT AND THE EMITTER MUST AGREE, SHAPE BY SHAPE, FLAVOR BY FLAVOR,
// IN BOTH DIRECTIONS.
//
// Both sides run the flavor's own emitter — the preflight through
// [Engine.PreflightColumnTypes], the late side through
// [mysqlEmitter.emitTableDefWithDomainChecks] — so what this actually pins is
// that the preflight's WALK reaches every column the table emitter renders, and
// that it refuses nothing more. The over-refusal direction is the one that
// breaks working migrations, and the matrix deliberately carries the PG-native
// shapes MySQL auto-emits rather than refuses (uuid → CHAR(36), inet →
// VARCHAR(45), array → JSON) so that direction has something to catch.
func TestPreflightColumnTypesAgreesWithTheTableEmitter(t *testing.T) {
	for _, flavor := range preflightFlavors {
		t.Run(flavor.String(), func(t *testing.T) {
			eng := Engine{Flavor: flavor}
			emitter := newMySQLEmitterForFlavor(nil, flavor)
			var refused, accepted int
			for _, c := range typematrix.Cases() {
				schema, tbl := oneColumnSchema(c.Type)
				preflightErr := eng.PreflightColumnTypes(schema)
				_, emitErr := emitter.emitTableDefWithDomainChecks(tbl, true)

				switch {
				case preflightErr != nil && emitErr == nil:
					t.Errorf("%s: the PREFLIGHT refused a type the emitter renders happily — an "+
						"over-refusal, which breaks migrations that work today.\npreflight: %v",
						c.Name, preflightErr)
				case preflightErr == nil && emitErr != nil:
					t.Errorf("%s: the EMITTER refuses this type and the preflight does not — the "+
						"refusal is back to firing at CREATE TABLE, after the plan and with earlier "+
						"tables already created.\nemitter: %v", c.Name, emitErr)
				case preflightErr != nil:
					refused++
				default:
					accepted++
				}
			}
			if refused == 0 {
				t.Error("no shape in the matrix was refused; MySQL has no INTERVAL, no uncatalogued " +
					"PG extension type and no negative DECIMAL scale, and must refuse those")
			}
			if accepted == 0 {
				t.Error("no shape in the matrix was accepted; the preflight is refusing everything")
			}
		})
	}
}

// THE PREMISE THE RESTORE LANES REST ON, MADE CHECKABLE.
//
// `restore` and `chain restore` hand this preflight the manifest's
// SOURCE-shaped schema and run [translate.RetargetForEngine] afterwards. That
// ordering is only safe if the retarget can never turn a refusal into an
// acceptance — i.e. if every type the PG→MySQL rule table rewrites was already
// renderable unrewritten. It is true today because `retargetPGtoMySQL` was
// written as a mirror of this engine's auto-emit arms, and "written as a
// mirror" is a hypothesis until something fails when it stops being one.
//
// This drives the REAL retarget over the whole matrix and compares the
// preflight's verdict on both sides. A rule added there for a type this emitter
// refuses (or an arm removed here) fails immediately, on the lane where the
// consequence would otherwise be an over-refusal of a restore that used to work.
func TestPreflightColumnTypes_IsRetargetInvariant(t *testing.T) {
	eng := Engine{}
	var rewritten int
	for _, c := range typematrix.Cases() {
		raw, _ := oneColumnSchema(c.Type)
		retargeted := translate.RetargetForEngine(raw, "postgres", "mysql")
		if !reflect.DeepEqual(retargeted.Tables[0].Columns[0].Type, c.Type) {
			rewritten++
		}
		rawErr := eng.PreflightColumnTypes(raw)
		retargetedErr := eng.PreflightColumnTypes(retargeted)
		if (rawErr != nil) != (retargetedErr != nil) {
			t.Errorf("%s: the preflight's verdict CHANGES across translate.RetargetForEngine "+
				"(pre-retarget err=%v, post-retarget err=%v).\n\nThe restore lanes preflight the "+
				"manifest's source-shaped schema and retarget afterwards, so a divergence here is a "+
				"false refusal (or a missed one) on every cross-engine restore carrying this shape.",
				c.Name, rawErr, retargetedErr)
		}
	}
	// Anti-vacuity: if the rule table stopped matching anything, the test above
	// would compare each schema against itself and prove nothing.
	if rewritten < 5 {
		t.Errorf("translate.RetargetForEngine rewrote only %d of %d matrix shapes; the PG→MySQL rule "+
			"table covers uuid, inet, cidr, macaddr, wide varchar, json, varying bit, array and two "+
			"extensions, so this gate is no longer exercising the retarget", rewritten, len(typematrix.Cases()))
	}
}

// The refusal must name the TABLE and the COLUMN, not just the type.
func TestPreflightColumnTypes_NamesTheTableAndColumn(t *testing.T) {
	schema := &ir.Schema{Tables: []*ir.Table{{
		Name:    "shifts",
		Columns: []*ir.Column{{Name: "duration", Type: ir.Interval{}}},
	}}}
	err := Engine{}.PreflightColumnTypes(schema)
	if err == nil {
		t.Fatal("MySQL has no INTERVAL type and must refuse the column")
	}
	for _, want := range []string{"shifts", "duration", "INTERVAL"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal does not mention %q; got: %v", want, err)
		}
	}
}

// The control: the PG-native shapes MySQL auto-emits must NOT be refused. This
// is the over-refusal direction at the schema level — every one of these
// columns migrates PG → MySQL today.
func TestPreflightColumnTypes_AcceptsThePGNativeAutoEmits(t *testing.T) {
	schema := &ir.Schema{Tables: []*ir.Table{{
		Name: "hosts",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.UUID{}},
			{Name: "addr", Type: ir.Inet{}},
			{Name: "net", Type: ir.Cidr{}},
			{Name: "mac", Type: ir.Macaddr{}},
			{Name: "tags", Type: ir.Array{Element: ir.Text{}}},
			{Name: "attrs", Type: ir.ExtensionType{Extension: "hstore", Name: "hstore"}},
			{Name: "label", Type: ir.ExtensionType{Extension: "citext", Name: "citext"}},
			{Name: "shape", Type: ir.Geometry{Subtype: ir.GeometryPoint, SRID: 4326}},
		},
	}}}
	for _, flavor := range preflightFlavors {
		if err := (Engine{Flavor: flavor}).PreflightColumnTypes(schema); err != nil {
			t.Errorf("%s: every column here has a documented MySQL auto-emit (v0.7.0 / ADR-0032 / "+
				"ADR-0035) and must not be refused: %v", flavor, err)
		}
	}
}

// Nil-shaped input must not turn a preflight into a panic on a path that
// previously reached the emitter.
func TestPreflightColumnTypes_SkipsNilShapes(t *testing.T) {
	schema := &ir.Schema{Tables: []*ir.Table{
		nil,
		{Name: "t", Columns: []*ir.Column{nil, {Name: "c"}}},
	}}
	if err := (Engine{}).PreflightColumnTypes(schema); err != nil {
		t.Fatalf("nil tables/columns/types must be skipped, not refused: %v", err)
	}
	if err := (Engine{}).PreflightColumnTypes(nil); err == nil {
		t.Error("a nil SCHEMA is a caller bug and must be refused loudly")
	}
}
