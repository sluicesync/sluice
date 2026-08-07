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

// TestRetargetRules_NeverProduceABinaryType is the PREMISE check for the
// one consumer a newly non-nil SourceColumnType could have flipped.
//
// MySQL's columnIsNativelyBinary treats a nil SourceColumnType as
// "natively binary", and it is only ever asked about a column whose
// (post-rewrite) Type is Binary / Varbinary / Blob. Recording the
// retarget's pre-rewrite type is therefore invisible to it — PROVIDED no
// rule here ever outputs a binary type. That is a fact about this rule
// table, and the safety argument rests on it, so it is asserted rather
// than believed (the premise-naming step in CLAUDE.md).
//
// Scope, stated so the name cannot be read as broader: it grades the
// PG→MySQL rule, the only rule table [retargetRuleFor] has. A new rule
// for another engine pair must be added to the probe list below.
func TestRetargetRules_NeverProduceABinaryType(t *testing.T) {
	probes := []ir.Type{
		ir.UUID{},
		ir.Inet{},
		ir.Cidr{},
		ir.Macaddr{},
		ir.Varchar{Length: 70000},
		ir.Varchar{Length: 64},
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
		ir.JSON{Binary: true},
	}
	rewritten := 0
	for _, in := range probes {
		out := retargetPGtoMySQL(in)
		if out == nil {
			continue
		}
		rewritten++
		switch out.(type) {
		case ir.Binary, ir.Varbinary, ir.Blob:
			t.Errorf("retargetPGtoMySQL(%#v) = %#v, a BINARY type. MySQL's columnIsNativelyBinary reads a "+
				"non-nil SourceColumnType on a binary column as 'an override made this binary', and the "+
				"retarget now sets that field — so this rule would make the writer hex-decode a source "+
				"string it should have stored verbatim. Either drop the rule or teach "+
				"columnIsNativelyBinary about retarget provenance", in, out)
		}
	}
	// Anti-vacuity: the probe list must actually exercise the rule table.
	if rewritten < 8 {
		t.Fatalf("only %d of %d probes were rewritten; the probe list has drifted from the rule table and "+
			"this gate is grading almost nothing", rewritten, len(probes))
	}
}
