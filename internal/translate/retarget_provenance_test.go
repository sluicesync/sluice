// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package translate

import (
	"reflect"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// Bug 232 — the retarget records what it REPLACED.
//
// `sluice restore` of a PG-sourced backup into MySQL runs the manifest
// schema through [RetargetForEngine] before handing it to the row
// writer. The rewrite `ir.Array{Element: ir.JSON{}}` → `ir.JSON{}` threw
// the element family away, and the MySQL writer's per-element leaf
// policy — which resolves that family from Type OR SourceColumnType —
// then refused every json[] / jsonb[] / bytea[] value with
// SLUICE-E-VALUE-UNREPRESENTABLE and restored 0 rows, on chains taken by
// ANY release. The element was in the manifest the whole time.
//
// These pin the provenance itself. The value-level consequence — that a
// retargeted column now renders each element family exactly as the
// migrate lane does — is pinned in internal/engines/mysql
// (TestArrayLeafForJSON_RetargetedRestoreColumnMatchesTheMigrateLane),
// and end to end against real servers by
// TestRestore_PGToMySQL_ArrayElementFamiliesMatchMigrate.

// TestRetargetForEngine_RecordsThePreRewriteSourceType walks every
// rewrite the PG→MySQL rule can make and asserts the pre-rewrite type is
// parked in SourceColumnType — the field the MySQL writer's
// arrayElementType / columnIsArrayLike already read.
//
// It is a per-RULE roster, not one representative: the defect was
// specific to the one rule whose output drops information its input
// carried, and only an enumeration says which others do.
func TestRetargetForEngine_RecordsThePreRewriteSourceType(t *testing.T) {
	rewrites := []struct {
		name string
		src  ir.Type
		want ir.Type
	}{
		{"uuid", ir.UUID{}, ir.Char{Length: 36}},
		{"inet", ir.Inet{}, ir.Varchar{Length: 45}},
		{"cidr", ir.Cidr{}, ir.Varchar{Length: 45}},
		{"macaddr", ir.Macaddr{}, ir.Varchar{Length: 30}},
		{"wide varchar", ir.Varchar{Length: 70000}, ir.Text{Size: ir.TextMedium}},
		// The Bug 232 rule: the only one whose input is COMPOSITE.
		{"json[]", ir.Array{Element: ir.JSON{}}, ir.JSON{Binary: true}},
		{"bytea[]", ir.Array{Element: ir.Blob{}}, ir.JSON{Binary: true}},
		{"text[]", ir.Array{Element: ir.Text{Size: ir.TextLong}}, ir.JSON{Binary: true}},
		{"hstore", ir.ExtensionType{Extension: "hstore"}, ir.JSON{Binary: true}},
		{"citext", ir.ExtensionType{Extension: "citext"}, ir.Varchar{Length: 255, Collation: "utf8mb4_0900_ai_ci"}},
	}
	for _, tc := range rewrites {
		t.Run(tc.name, func(t *testing.T) {
			out := RetargetForEngine(&ir.Schema{Tables: []*ir.Table{{
				Name:    "t",
				Columns: []*ir.Column{{Name: "c", Type: tc.src}},
			}}}, "postgres", "mysql")
			col := out.Tables[0].Columns[0]
			if !reflect.DeepEqual(col.Type, tc.want) {
				t.Fatalf("Type = %#v; want %#v (this roster row no longer describes the rule table)",
					col.Type, tc.want)
			}
			if !reflect.DeepEqual(col.SourceColumnType, tc.src) {
				t.Errorf("SourceColumnType = %#v; want the pre-rewrite %#v — a target writer that needs "+
					"the source shape (MySQL's array element family) can no longer reach it", col.SourceColumnType, tc.src)
			}
		})
	}
}

// TestRetargetForEngine_LeavesUnrewrittenColumnsAlone: a column no rule
// touches keeps a nil SourceColumnType. That nil is load-bearing — MySQL's
// columnIsNativelyBinary reads it as "no override fired" — so the retarget
// must not start setting it on columns it did not rewrite.
func TestRetargetForEngine_LeavesUnrewrittenColumnsAlone(t *testing.T) {
	out := RetargetForEngine(&ir.Schema{Tables: []*ir.Table{{
		Name: "t",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 32}},
			{Name: "blob", Type: ir.Blob{}},
			{Name: "narrow", Type: ir.Varchar{Length: 64}},
		},
	}}}, "postgres", "mysql")
	for _, col := range out.Tables[0].Columns {
		if col.SourceColumnType != nil {
			t.Errorf("column %q was not rewritten but carries SourceColumnType %#v", col.Name, col.SourceColumnType)
		}
	}
}

// TestRetargetForEngine_DoesNotClobberAnOverridesSourceType pins the
// precedence: `--type-override` runs FIRST ([ApplyMappings]) and records
// the operator's true source type. The retarget's own pre-rewrite type is
// one step nearer, so recording it would replace a truth with a
// half-truth — and would flip columnIsNativelyBinary for a text column an
// operator overrode to a binary type.
func TestRetargetForEngine_DoesNotClobberAnOverridesSourceType(t *testing.T) {
	out := RetargetForEngine(&ir.Schema{Tables: []*ir.Table{{
		Name: "t",
		Columns: []*ir.Column{{
			Name:             "c",
			Type:             ir.UUID{},
			SourceColumnType: ir.Text{Size: ir.TextLong}, // what --type-override=c=uuid replaced
		}},
	}}}, "postgres", "mysql")
	col := out.Tables[0].Columns[0]
	if _, isText := col.SourceColumnType.(ir.Text); !isText {
		t.Errorf("SourceColumnType = %#v; want the OVERRIDE's ir.Text — the retarget overwrote the "+
			"operator's real source type with its own", col.SourceColumnType)
	}
}

// retargetProbes is the input roster the premise checks below grade —
// one entry per arm of [retargetPGtoMySQL] plus the pass-through and
// binary-input shapes the premise is about.
//
// It is hand-written because it is the STATEMENT of coverage;
// TestRetargetRuleArmsAreAllProbed is what stops it from silently
// falling behind the rule table.
var retargetProbes = []ir.Type{
	ir.UUID{},
	ir.Inet{},
	ir.Cidr{},
	ir.Macaddr{},
	ir.Varchar{Length: 70000},
	ir.Varchar{Length: 64},
	ir.JSON{Binary: false},
	ir.JSON{Binary: true},
	ir.Bit{Length: 8, Varying: true},
	ir.Bit{Length: 8},
	ir.Array{Element: ir.JSON{}},
	ir.Array{Element: ir.Blob{}},
	ir.Array{Element: ir.Text{Size: ir.TextLong}},
	ir.ExtensionType{Extension: "hstore"},
	ir.ExtensionType{Extension: "citext"},
	ir.Blob{},
	ir.Binary{Length: 16},
	ir.Varbinary{Length: 255},
	ir.Integer{Width: 64},
	ir.Text{Size: ir.TextLong},
	// The DOMAIN wrappers the shape-compare rule flattens. The bytea
	// one is the cell that made the old "never produces a binary type"
	// claim false, so it must stay in the roster.
	ir.Domain{Name: "d_blob", BaseType: ir.Blob{}},
	ir.Domain{Name: "d_json", BaseType: ir.JSON{}},
	ir.Domain{Name: "d_jarr", BaseType: ir.Array{Element: ir.JSON{}}},
	ir.Domain{Name: "d_text", BaseType: ir.Text{Size: ir.TextLong}},
	ir.Domain{Name: "d_nested", BaseType: ir.Domain{Name: "d_inner", BaseType: ir.Blob{}}},
	ir.Domain{Name: "d_malformed"},
}

// TestRetargetRules_BinaryOutputOnlyFromABinarySource is the PREMISE
// check for the one consumer a newly non-nil SourceColumnType could
// flip. It REPLACES TestRetargetRules_NeverProduceABinaryType (roadmap
// item 153), which asserted the strictly stronger claim that no rule
// produces a binary type at all — true through v0.116.1, and made false
// by [RetargetForShapeCompare]'s DOMAIN unwrap, which rewrites
// `Domain{d AS Blob}` to `Blob`.
//
// # What this guarantees
//
// That the retarget never MANUFACTURES binariness: whenever a rule
// outputs Binary / Varbinary / Blob, the input's own storage type
// ([ir.UnwrapDomain]) was already one of those. That is exactly what
// MySQL's columnIsNativelyBinary needs — it reads a nil
// SourceColumnType as "natively binary" and is only asked about a
// column whose post-rewrite Type is binary, so the provenance this
// records may only ever CONFIRM the answer the un-retargeted column
// would have given, never invert it.
//
// # What it no longer guarantees
//
// That the retarget's OUTPUT is never binary. It now can be. Any future
// consumer resting on "a retargeted column is never binary-typed" has
// lost its evidence here and must re-derive it.
//
// # Scope, stated so the name cannot be read as broader
//
// It grades the PG→MySQL rule table — the only one [retargetRuleFor]
// has — through BOTH entry points, since they differ precisely in the
// DOMAIN handling that made the old claim false. It says nothing about
// whether columnIsNativelyBinary itself still unwraps domains; that
// binding is TestColumnIsNativelyBinary_SurvivesTheRetargetsProvenance
// in internal/engines/mysql, which drives this pass for real.
func TestRetargetRules_BinaryOutputOnlyFromABinarySource(t *testing.T) {
	isBinary := func(t ir.Type) bool {
		switch t.(type) {
		case ir.Binary, ir.Varbinary, ir.Blob:
			return true
		}
		return false
	}
	for _, entry := range []struct {
		lane string
		rule retargetRule
	}{
		{"RetargetForEngine", retargetPGtoMySQL},
		{"RetargetForShapeCompare", storageShapeRule(retargetPGtoMySQL)},
	} {
		rewritten, binaryOut := 0, 0
		for _, in := range retargetProbes {
			out := entry.rule(in)
			if out == nil {
				continue
			}
			rewritten++
			if !isBinary(out) {
				continue
			}
			binaryOut++
			if !isBinary(ir.UnwrapDomain(in)) {
				t.Errorf("%s: rule(%s) = %s — a BINARY output from a source whose storage type (%s) is "+
					"NOT binary. MySQL's columnIsNativelyBinary reads the SourceColumnType the retarget "+
					"records; a manufactured binary type makes it answer 'an override imposed this', and "+
					"the writer then stores a PostgreSQL `\\x…` bytea rendering VERBATIM instead of "+
					"decoding it. Either drop the rule or teach columnIsNativelyBinary about it",
					entry.lane, in, out, ir.UnwrapDomain(in))
			}
		}
		// Anti-vacuity: the probe list must actually exercise the rule
		// table. 8 was the floor when the roster held 11 rewritable
		// probes; it holds more now.
		if rewritten < 8 {
			t.Fatalf("%s: only %d of %d probes were rewritten; the probe list has drifted from the rule "+
				"table and this gate is grading almost nothing", entry.lane, rewritten, len(retargetProbes))
		}
		// The shape-compare lane is the one that CAN emit a binary type,
		// and it is the whole reason this check was re-derived. If it
		// stops producing one, the check above is vacuous on the only
		// lane it was written for and the roster has lost its bytea
		// DOMAIN cell.
		if entry.lane == "RetargetForShapeCompare" && binaryOut == 0 {
			t.Errorf("%s produced no binary output at all; the `Domain{d AS Blob}` probe is what makes "+
				"this premise non-vacuous and the roster no longer reaches it", entry.lane)
		}
	}
}

// TestRetargetForShapeCompare_FlattensADomainToItsRetargetedStorage
// grades the item-153 rewrite itself: the storage a MySQL catalog reads
// back for a domain-typed column, per family — the rule table's own
// output for the base type when one applies, and the bare base type
// when none does.
func TestRetargetForShapeCompare_FlattensADomainToItsRetargetedStorage(t *testing.T) {
	for _, tc := range []struct {
		name string
		src  ir.Type
		want ir.Type
	}{
		// A base type the rule table rewrites: both hops must happen.
		{"domain over json", ir.Domain{Name: "d", BaseType: ir.JSON{}}, ir.JSON{Binary: true}},
		{"domain over jsonb", ir.Domain{Name: "d", BaseType: ir.JSON{Binary: true}}, ir.JSON{Binary: true}},
		{"domain over json[]", ir.Domain{Name: "d", BaseType: ir.Array{Element: ir.JSON{}}}, ir.JSON{Binary: true}},
		{"domain over hstore", ir.Domain{Name: "d", BaseType: ir.ExtensionType{Extension: "hstore"}}, ir.JSON{Binary: true}},
		{"domain over uuid", ir.Domain{Name: "d", BaseType: ir.UUID{}}, ir.Char{Length: 36}},
		{"domain over wide varchar", ir.Domain{Name: "d", BaseType: ir.Varchar{Length: 20000}}, ir.Text{Size: ir.TextMedium}},
		{"domain over varbit", ir.Domain{Name: "d", BaseType: ir.Bit{Length: 8, Varying: true}}, ir.Bit{Length: 8}},
		// A base type no rule rewrites: the bare base type.
		{"domain over text", ir.Domain{Name: "d", BaseType: ir.Text{Size: ir.TextLong}}, ir.Text{Size: ir.TextLong}},
		{"domain over narrow varchar", ir.Domain{Name: "d", BaseType: ir.Varchar{Length: 64}}, ir.Varchar{Length: 64}},
		{"domain over bit", ir.Domain{Name: "d", BaseType: ir.Bit{Length: 8}}, ir.Bit{Length: 8}},
		{"domain over bytea", ir.Domain{Name: "d", BaseType: ir.Blob{}}, ir.Blob{}},
		// Nested, and the CHECKs the wrapper carries must not survive
		// into the expected side — a MySQL catalog has no domain to
		// read them back from.
		{
			"domain over domain",
			ir.Domain{Name: "outer", BaseType: ir.Domain{Name: "inner", BaseType: ir.JSON{}}},
			ir.JSON{Binary: true},
		},
		{
			"domain with checks",
			ir.Domain{Name: "d", BaseType: ir.Text{Size: ir.TextLong}, Checks: []ir.DomainCheck{{Name: "c", Body: "VALUE <> ''"}}},
			ir.Text{Size: ir.TextLong},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out := RetargetForShapeCompare(&ir.Schema{Tables: []*ir.Table{{
				Name:    "t",
				Columns: []*ir.Column{{Name: "c", Type: tc.src}},
			}}}, "postgres", "mysql")
			got := out.Tables[0].Columns[0]
			if !reflect.DeepEqual(got.Type, tc.want) {
				t.Errorf("Type = %s (%#v); want %s — this is the shape the pre-create gate compares against "+
					"MySQL's catalog read-back", got.Type, got.Type, tc.want)
			}
			if !reflect.DeepEqual(got.SourceColumnType, tc.src) {
				t.Errorf("SourceColumnType = %#v; want the pre-rewrite %#v — the MySQL writer's array "+
					"element family is reachable only through this field once the wrapper is gone (Bug 232 "+
					"through domains)", got.SourceColumnType, tc.src)
			}
		})
	}
}

// TestRetargetForShapeCompare_LeavesAMalformedDomainAlone: a DOMAIN with
// a nil BaseType is malformed, and the DDL emitter refuses it BY NAME
// (`DOMAIN %q has nil BaseType`). Rewriting it here would turn that
// named refusal into an anonymous shape mismatch.
func TestRetargetForShapeCompare_LeavesAMalformedDomainAlone(t *testing.T) {
	src := ir.Domain{Name: "broken"}
	out := RetargetForShapeCompare(&ir.Schema{Tables: []*ir.Table{{
		Name:    "t",
		Columns: []*ir.Column{{Name: "c", Type: src}},
	}}}, "postgres", "mysql")
	got := out.Tables[0].Columns[0]
	if !reflect.DeepEqual(got.Type, src) {
		t.Errorf("Type = %#v; want the malformed domain unchanged", got.Type)
	}
	if got.SourceColumnType != nil {
		t.Errorf("SourceColumnType = %#v; want nil — nothing was rewritten", got.SourceColumnType)
	}
}

// TestRetargetForShapeCompare_SameFamilyPairKeepsTheDomain: the unwrap
// is a claim about what the TARGET holds, so it must not fire for a pair
// whose target really does hold domains. A PG→PG diff comparing
// `Domain{d AS text}` against a bare `Text` would be reporting drift
// backwards — the target's domain would look like the mismatch.
func TestRetargetForShapeCompare_SameFamilyPairKeepsTheDomain(t *testing.T) {
	src := ir.Domain{Name: "d", BaseType: ir.Text{Size: ir.TextLong}}
	for _, pair := range [][2]string{{"postgres", "postgres"}, {"postgres", "postgres-trigger"}, {"postgres", "sqlite"}} {
		out := RetargetForShapeCompare(&ir.Schema{Tables: []*ir.Table{{
			Name:    "t",
			Columns: []*ir.Column{{Name: "c", Type: src}},
		}}}, pair[0], pair[1])
		if got := out.Tables[0].Columns[0].Type; !reflect.DeepEqual(got, src) {
			t.Errorf("%s→%s: Type = %#v; want the DOMAIN kept — only a target with no DOMAIN concept "+
				"(a MySQL-family one, via the rule table) may be compared against its base type",
				pair[0], pair[1], got)
		}
	}
}
