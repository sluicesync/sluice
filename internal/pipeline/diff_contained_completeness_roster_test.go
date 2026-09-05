// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// The CONTAINED diff-struct render rosters (audit backlog G-6 follow-on,
// filed at the container gate's own definition and built 2026-08-22).
//
// # What this gate is for
//
// [TestDiffSurfaceRosterEveryTableDiffFieldIsRenderedAndCounted] proves
// every [irdiff.TableDiff]/[irdiff.SchemaDiff] field has a render
// surface and a summary counter — the SECTION exists. It says nothing
// about whether every attribute WITHIN a nested entry renders: a
// ForeignKeyDiff whose ActualOnDelete was dropped from the render would
// pass it, because the ForeignKeysMismatched probe fires on a different
// attribute. That is Bug 227's shape one level down — the comparison
// sees a delta the operator is never shown.
//
// So this walks the nested per-object diff structs by reflection and
// requires every field to be either PROBED — populated on an
// otherwise-empty diff, with the TEXT renderer required to print that
// field's own content — or exempted with a written reason.
//
// # What it reaches, stated rather than implied
//
// It reaches the fields of [irdiff.ColumnDiff], [irdiff.IndexDiff],
// [irdiff.ForeignKeyDiff], [irdiff.PolicyDiff] and
// [irdiff.SequenceDiff], against the TEXT renderer. It does NOT walk
// [irdiff.CheckDiff], [irdiff.ExcludeDiff] or [irdiff.ViewDiff] (three
// small structs whose every field is already exercised verbatim by the
// container roster's own probes: their render lines print name +
// expected + actual in one statement). Named residuals, not covered
// surface.
//
// # The COUNT half, and why it is per-entry here
//
// Each probe also asserts the container's summary counter fires
// (ColumnDiff rides ColumnsMismatched, and so on). There are no
// per-ATTRIBUTE counters by design: the JSON body embeds the whole
// SchemaDiff, so a CI consumer reading counts sees the ENTRY and reads
// the attributes from the body — the hand-written surface that can
// omit an attribute is the text renderer, which is what every probe
// binds.
//
// # Dialect note
//
// Probes default to the ANSI/PG renderer. The two charset fields render
// ONLY under the MySQL dialect (PG has no per-column charset — see
// renderCharsetCollationMismatch), so those probes pin the MySQL form
// explicitly; every other probe's surface is dialect-shared comment
// text or exists in both dialect arms.

import (
	"reflect"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	irdiff "sluicesync.dev/sluice/internal/ir/diff"
)

// containedDiffProbe populates one nested diff struct with the probed
// field (and any gate-fields its rendering requires), and names the
// substrings the text render must contain — each carrying the probed
// field's own VALUE, so a probe cannot be satisfied by a sibling
// attribute's line.
type containedDiffProbe struct {
	populate  func(td *irdiff.TableDiff, d *irdiff.SchemaDiff)
	wantTexts []string
	dialect   ir.DDLDialect // zero value = ANSI, the default renderer
}

// ---- irdiff.ColumnDiff ----

var columnDiffFieldProbes = map[string]containedDiffProbe{
	"ExpectedType": {
		populate: func(td *irdiff.TableDiff, _ *irdiff.SchemaDiff) {
			td.ColumnsMismatched = []irdiff.ColumnDiff{{Name: "amount", ExpectedType: "NUMERIC(20,8)", ActualType: "NUMERIC(10,2)"}}
		},
		wantTexts: []string{"TYPE NUMERIC(20,8)"},
	},
	"ActualType": {
		populate: func(td *irdiff.TableDiff, _ *irdiff.SchemaDiff) {
			td.ColumnsMismatched = []irdiff.ColumnDiff{{Name: "amount", ExpectedType: "NUMERIC(20,8)", ActualType: "NUMERIC(10,2)"}}
		},
		wantTexts: []string{"-- on target: NUMERIC(10,2)"},
	},
	"ExpectedNullable": {
		populate: func(td *irdiff.TableDiff, _ *irdiff.SchemaDiff) {
			exp, act := false, true
			td.ColumnsMismatched = []irdiff.ColumnDiff{{Name: "amount", ExpectedNullable: &exp, ActualNullable: &act}}
		},
		wantTexts: []string{"SET NOT NULL", "expected: false"},
	},
	"ActualNullable": {
		populate: func(td *irdiff.TableDiff, _ *irdiff.SchemaDiff) {
			exp, act := true, false
			td.ColumnsMismatched = []irdiff.ColumnDiff{{Name: "amount", ExpectedNullable: &exp, ActualNullable: &act}}
		},
		wantTexts: []string{"nullable on target: false"},
	},
	"ExpectedDefault": {
		populate: func(td *irdiff.TableDiff, _ *irdiff.SchemaDiff) {
			td.ColumnsMismatched = []irdiff.ColumnDiff{{Name: "amount", ExpectedDefault: "'42'", ActualDefault: "<none>"}}
		},
		wantTexts: []string{"SET DEFAULT '42'"},
	},
	"ActualDefault": {
		populate: func(td *irdiff.TableDiff, _ *irdiff.SchemaDiff) {
			td.ColumnsMismatched = []irdiff.ColumnDiff{{Name: "amount", ExpectedDefault: "<none>", ActualDefault: "'42'"}}
		},
		wantTexts: []string{"DROP DEFAULT", "-- on target: '42'"},
	},
	"DefaultLowConfidence": {
		populate: func(td *irdiff.TableDiff, _ *irdiff.SchemaDiff) {
			td.ColumnsMismatched = []irdiff.ColumnDiff{{
				Name: "amount", ExpectedDefault: "now()", ActualDefault: "current_date(3)", DefaultLowConfidence: true,
			}}
		},
		wantTexts: []string{"may differ across engines"},
	},
	"ExpectedGeneratedExpr": {
		populate: func(td *irdiff.TableDiff, _ *irdiff.SchemaDiff) {
			td.ColumnsMismatched = []irdiff.ColumnDiff{{Name: "amount", ExpectedGeneratedExpr: "price * 11", ActualGeneratedExpr: "price"}}
		},
		wantTexts: []string{`expected="price * 11"`},
	},
	"ActualGeneratedExpr": {
		populate: func(td *irdiff.TableDiff, _ *irdiff.SchemaDiff) {
			td.ColumnsMismatched = []irdiff.ColumnDiff{{Name: "amount", ExpectedGeneratedExpr: "price", ActualGeneratedExpr: "price * 2"}}
		},
		wantTexts: []string{`target="price * 2"`},
	},
	"ExpectedCharset": {
		populate: func(td *irdiff.TableDiff, _ *irdiff.SchemaDiff) {
			td.ColumnsMismatched = []irdiff.ColumnDiff{{Name: "amount", ExpectedCharset: "utf8mb4", ActualCharset: "latin1"}}
		},
		wantTexts: []string{"CHARACTER SET utf8mb4"},
		dialect:   ir.DDLDialectMySQL,
	},
	"ActualCharset": {
		populate: func(td *irdiff.TableDiff, _ *irdiff.SchemaDiff) {
			td.ColumnsMismatched = []irdiff.ColumnDiff{{Name: "amount", ExpectedCharset: "utf8mb4", ActualCharset: "latin1"}}
		},
		wantTexts: []string{"-- on target: latin1"},
		dialect:   ir.DDLDialectMySQL,
	},
	"ExpectedCollation": {
		populate: func(td *irdiff.TableDiff, _ *irdiff.SchemaDiff) {
			td.ColumnsMismatched = []irdiff.ColumnDiff{{Name: "amount", ExpectedCollation: "en_US.utf8", ActualCollation: "C"}}
		},
		wantTexts: []string{`COLLATE "en_US.utf8"`},
	},
	"ActualCollation": {
		populate: func(td *irdiff.TableDiff, _ *irdiff.SchemaDiff) {
			td.ColumnsMismatched = []irdiff.ColumnDiff{{Name: "amount", ExpectedCollation: "en_US.utf8", ActualCollation: "C"}}
		},
		wantTexts: []string{"-- on target: C"},
	},
}

// ---- irdiff.IndexDiff ----

var indexDiffFieldProbes = map[string]containedDiffProbe{
	"ExpectedColumns": {
		populate: func(td *irdiff.TableDiff, _ *irdiff.SchemaDiff) {
			td.IndexesMismatched = []irdiff.IndexDiff{{Name: "uq_email", ExpectedColumns: "(email(10), id)", ActualColumns: "(email)"}}
		},
		wantTexts: []string{"source (email(10), id)"},
	},
	"ActualColumns": {
		populate: func(td *irdiff.TableDiff, _ *irdiff.SchemaDiff) {
			td.IndexesMismatched = []irdiff.IndexDiff{{Name: "uq_email", ExpectedColumns: "(email(10), id)", ActualColumns: "(email)"}}
		},
		wantTexts: []string{"target (email)"},
	},
	"UniqueMismatched": {
		populate: func(td *irdiff.TableDiff, _ *irdiff.SchemaDiff) {
			td.IndexesMismatched = []irdiff.IndexDiff{{Name: "uq_email", UniqueMismatched: true}}
		},
		wantTexts: []string{"unique:"},
	},
	"ExpectedUnique": {
		populate: func(td *irdiff.TableDiff, _ *irdiff.SchemaDiff) {
			td.IndexesMismatched = []irdiff.IndexDiff{{Name: "uq_email", UniqueMismatched: true, ExpectedUnique: true}}
		},
		wantTexts: []string{"unique:    source true"},
	},
	"ActualUnique": {
		populate: func(td *irdiff.TableDiff, _ *irdiff.SchemaDiff) {
			td.IndexesMismatched = []irdiff.IndexDiff{{Name: "uq_email", UniqueMismatched: true, ActualUnique: true}}
		},
		wantTexts: []string{"target true"},
	},
	"ExpectedPredicate": {
		populate: func(td *irdiff.TableDiff, _ *irdiff.SchemaDiff) {
			td.IndexesMismatched = []irdiff.IndexDiff{{Name: "uq_email", ExpectedPredicate: "deleted_at IS NULL"}}
		},
		wantTexts: []string{`source "deleted_at IS NULL"`},
	},
	"ActualPredicate": {
		populate: func(td *irdiff.TableDiff, _ *irdiff.SchemaDiff) {
			td.IndexesMismatched = []irdiff.IndexDiff{{Name: "uq_email", ActualPredicate: "qty > 0"}}
		},
		wantTexts: []string{`target "qty > 0"`},
	},
}

// ---- irdiff.ForeignKeyDiff ----

func fkProbe(fd irdiff.ForeignKeyDiff, wantTexts ...string) containedDiffProbe {
	fd.Name = "fk_orders_user"
	return containedDiffProbe{
		populate: func(td *irdiff.TableDiff, _ *irdiff.SchemaDiff) {
			td.ForeignKeysMismatched = []irdiff.ForeignKeyDiff{fd}
		},
		wantTexts: wantTexts,
	}
}

var foreignKeyDiffFieldProbes = map[string]containedDiffProbe{
	"ExpectedReference": fkProbe(
		irdiff.ForeignKeyDiff{ExpectedReference: "(user_id) -> accounts(id)", ActualReference: "(user_id) -> users(id)"},
		"source (user_id) -> accounts(id)",
	),
	"ActualReference": fkProbe(
		irdiff.ForeignKeyDiff{ExpectedReference: "(user_id) -> accounts(id)", ActualReference: "(user_id) -> users(id)"},
		"target (user_id) -> users(id)",
	),
	"ExpectedOnDelete": fkProbe(
		irdiff.ForeignKeyDiff{ExpectedOnDelete: "RESTRICT", ActualOnDelete: "CASCADE"},
		"on delete:    source RESTRICT",
	),
	"ActualOnDelete": fkProbe(
		// The pair span, not a bare "target CASCADE": the consequence prose
		// ("the target CASCADEs where…") contains that substring, and a
		// mutation run proved a bare match let a dropped value line pass.
		irdiff.ForeignKeyDiff{ExpectedOnDelete: "RESTRICT", ActualOnDelete: "CASCADE"},
		"source RESTRICT   target CASCADE", "SILENTLY DESTROYS",
	),
	"ExpectedOnUpdate": fkProbe(
		irdiff.ForeignKeyDiff{ExpectedOnUpdate: "SET NULL", ActualOnUpdate: "NO ACTION"},
		"on update:    source SET NULL",
	),
	"ActualOnUpdate": fkProbe(
		irdiff.ForeignKeyDiff{ExpectedOnUpdate: "SET NULL", ActualOnUpdate: "NO ACTION"},
		"target NO ACTION",
	),
	"ExpectedMatch": fkProbe(
		irdiff.ForeignKeyDiff{ExpectedMatch: "FULL", ActualMatch: "SIMPLE"},
		"match:        source FULL", "PARTIALLY-NULL COMPOSITE KEYS",
	),
	"ActualMatch": fkProbe(
		irdiff.ForeignKeyDiff{ExpectedMatch: "FULL", ActualMatch: "SIMPLE"},
		"target SIMPLE",
	),
	// UPR-1. Each of the three gets its own probe for the same reason the
	// deferrability trio does: all are bools, so one probe covering the flag
	// would leave the two value fields indistinguishable from "not rendered".
	"ValidityMismatched": fkProbe(
		irdiff.ForeignKeyDiff{ValidityMismatched: true},
		"validated:",
	),
	"ExpectedNotValid": fkProbe(
		irdiff.ForeignKeyDiff{ValidityMismatched: true, ExpectedNotValid: true},
		"source false", "REJECTS ROWS THE SOURCE HOLDS TODAY",
	),
	"ActualNotValid": fkProbe(
		irdiff.ForeignKeyDiff{ValidityMismatched: true, ActualNotValid: true},
		"target false", "NEVER CHECKED against it",
	),
	"DeferrabilityMismatched": fkProbe(
		irdiff.ForeignKeyDiff{DeferrabilityMismatched: true},
		"deferrable:",
	),
	"ExpectedDeferrable": fkProbe(
		irdiff.ForeignKeyDiff{DeferrabilityMismatched: true, ExpectedDeferrable: true},
		"source true (initially deferred false)", "REJECTS A TRANSACTION ORDERING",
	),
	"ExpectedInitiallyDeferred": fkProbe(
		irdiff.ForeignKeyDiff{DeferrabilityMismatched: true, ExpectedDeferrable: true, ExpectedInitiallyDeferred: true},
		"source true (initially deferred true)",
	),
	"ActualDeferrable": fkProbe(
		irdiff.ForeignKeyDiff{DeferrabilityMismatched: true, ActualDeferrable: true},
		"target true (initially deferred false)",
	),
	"ActualInitiallyDeferred": fkProbe(
		irdiff.ForeignKeyDiff{DeferrabilityMismatched: true, ActualDeferrable: true, ActualInitiallyDeferred: true},
		"target true (initially deferred true)",
	),
}

// ---- irdiff.PolicyDiff ----

func policyProbe(pd irdiff.PolicyDiff, wantTexts ...string) containedDiffProbe {
	pd.Name = "tenant_isolation"
	return containedDiffProbe{
		populate: func(td *irdiff.TableDiff, _ *irdiff.SchemaDiff) {
			td.PoliciesMismatched = []irdiff.PolicyDiff{pd}
		},
		wantTexts: wantTexts,
	}
}

var policyDiffFieldProbes = map[string]containedDiffProbe{
	"ExpectedCommand": policyProbe(
		irdiff.PolicyDiff{ExpectedCommand: "ALL", ActualCommand: "SELECT"},
		"command:    source ALL",
	),
	"ActualCommand": policyProbe(
		irdiff.PolicyDiff{ExpectedCommand: "ALL", ActualCommand: "SELECT"},
		"target SELECT", "run UNFILTERED",
	),
	"PermissiveMismatched": policyProbe(
		irdiff.PolicyDiff{PermissiveMismatched: true},
		"permissive:",
	),
	"ExpectedPermissive": policyProbe(
		irdiff.PolicyDiff{PermissiveMismatched: true, ExpectedPermissive: true},
		"permissive: source true",
	),
	"ActualPermissive": policyProbe(
		irdiff.PolicyDiff{PermissiveMismatched: true, ActualPermissive: true},
		"permissive: source false   target true", "ADMITS ROWS THE SOURCE HIDES",
	),
	"ExpectedRoles": policyProbe(
		irdiff.PolicyDiff{ExpectedRoles: "app, reader", ActualRoles: "app"},
		"roles:      source app, reader",
	),
	"ActualRoles": policyProbe(
		irdiff.PolicyDiff{ExpectedRoles: "app, reader", ActualRoles: "app"},
		"target app",
	),
	"ExpectedUsing": policyProbe(
		irdiff.PolicyDiff{ExpectedUsing: "tenant_id = 1"},
		`using:      source "tenant_id = 1"`, "RETURNS EVERY ROW",
	),
	"ActualUsing": policyProbe(
		irdiff.PolicyDiff{ActualUsing: "true"},
		`target "true"`,
	),
	"ExpectedCheck": policyProbe(
		irdiff.PolicyDiff{ExpectedCheck: "tenant_id = 2"},
		`with check: source "tenant_id = 2"`, "ACCEPTS WRITES",
	),
	"ActualCheck": policyProbe(
		irdiff.PolicyDiff{ActualCheck: "tenant_id = 3"},
		`target "tenant_id = 3"`,
	),
}

// ---- irdiff.SequenceDiff ----

func seqProbe(sd irdiff.SequenceDiff, wantTexts ...string) containedDiffProbe {
	sd.Name = "order_number_seq"
	return containedDiffProbe{
		populate: func(_ *irdiff.TableDiff, d *irdiff.SchemaDiff) {
			d.SequencesMismatched = []irdiff.SequenceDiff{sd}
		},
		wantTexts: wantTexts,
	}
}

var sequenceDiffFieldProbes = map[string]containedDiffProbe{
	"ExpectedDataType": seqProbe(
		irdiff.SequenceDiff{ExpectedDataType: "bigint", ActualDataType: "integer"},
		"data type", "source bigint",
	),
	"ActualDataType": seqProbe(
		irdiff.SequenceDiff{ExpectedDataType: "bigint", ActualDataType: "integer"},
		"target integer",
	),
	"ExpectedStart": seqProbe(
		irdiff.SequenceDiff{ExpectedStart: "1000", ActualStart: "2000"},
		"start:", "source 1000",
	),
	"ActualStart": seqProbe(
		irdiff.SequenceDiff{ExpectedStart: "1000", ActualStart: "2000"},
		"target 2000",
	),
	"ExpectedIncrement": seqProbe(
		irdiff.SequenceDiff{ExpectedIncrement: "5", ActualIncrement: "9"},
		"increment:", "source 5",
	),
	"ActualIncrement": seqProbe(
		irdiff.SequenceDiff{ExpectedIncrement: "5", ActualIncrement: "9"},
		"target 9",
	),
	"ExpectedMinValue": seqProbe(
		irdiff.SequenceDiff{ExpectedMinValue: "1", ActualMinValue: "3"},
		"min value:", "source 1",
	),
	"ActualMinValue": seqProbe(
		irdiff.SequenceDiff{ExpectedMinValue: "1", ActualMinValue: "3"},
		"target 3",
	),
	"ExpectedMaxValue": seqProbe(
		irdiff.SequenceDiff{ExpectedMaxValue: "4096", ActualMaxValue: "8192"},
		"max value:", "source 4096",
	),
	"ActualMaxValue": seqProbe(
		irdiff.SequenceDiff{ExpectedMaxValue: "4096", ActualMaxValue: "8192"},
		"target 8192",
	),
	"ExpectedCache": seqProbe(
		irdiff.SequenceDiff{ExpectedCache: "1", ActualCache: "50"},
		"cache:", "source 1",
	),
	"ActualCache": seqProbe(
		irdiff.SequenceDiff{ExpectedCache: "1", ActualCache: "50"},
		"target 50",
	),
	"CycleMismatched": seqProbe(
		irdiff.SequenceDiff{CycleMismatched: true},
		"cycle:",
	),
	"ExpectedCycle": seqProbe(
		irdiff.SequenceDiff{CycleMismatched: true, ExpectedCycle: true},
		"cycle:", "source true",
	),
	"ActualCycle": seqProbe(
		irdiff.SequenceDiff{CycleMismatched: true, ActualCycle: true},
		"target true", "WRAPS AND RE-ISSUES VALUES",
	),
	"ExpectedOwnedBy": seqProbe(
		irdiff.SequenceDiff{ExpectedOwnedBy: "orders.id", ActualOwnedBy: "<none>"},
		"owned by", "source orders.id",
	),
	"ActualOwnedBy": seqProbe(
		irdiff.SequenceDiff{ExpectedOwnedBy: "orders.id", ActualOwnedBy: "<none>"},
		"target <none>",
	),
}

// nameIdentityExempt is the shared exemption for each nested struct's
// Name field, mirroring the container roster's TableDiff.Name precedent:
// the name is the ENTRY IDENTITY, rendered into every suggestion line
// and section header — and the harness asserts exactly that on every
// probe below, so it is covered behaviourally rather than by a probe of
// its own.
const nameIdentityExempt = "the entry identity, not a delta: it renders in every line of its own section, which the harness " +
	"asserts on EVERY probe (assertContainedDiffRosterCovers requires the probe entry's Name in the render output)"

// containedDiffRosters enumerates what this gate reaches. probeName is
// the fixed entry Name each probe uses, asserted present in every
// probe's render — the behavioural cover for the Name exemption.
var containedDiffRosters = []struct {
	typ       reflect.Type
	probes    map[string]containedDiffProbe
	exempt    map[string]string
	floor     int
	probeName string
	count     func(DiffJSONCounts) int
}{
	{
		reflect.TypeOf(irdiff.ColumnDiff{}), columnDiffFieldProbes,
		map[string]string{"Name": nameIdentityExempt},
		13, "amount",
		func(c DiffJSONCounts) int { return c.ColumnsMismatched },
	},
	{
		reflect.TypeOf(irdiff.IndexDiff{}), indexDiffFieldProbes,
		map[string]string{"Name": nameIdentityExempt},
		7, "uq_email",
		func(c DiffJSONCounts) int { return c.IndexesMismatched },
	},
	{
		reflect.TypeOf(irdiff.ForeignKeyDiff{}), foreignKeyDiffFieldProbes,
		map[string]string{"Name": nameIdentityExempt},
		13, "fk_orders_user",
		func(c DiffJSONCounts) int { return c.ForeignKeysMismatched },
	},
	{
		reflect.TypeOf(irdiff.PolicyDiff{}), policyDiffFieldProbes,
		map[string]string{"Name": nameIdentityExempt},
		11, "tenant_isolation",
		func(c DiffJSONCounts) int { return c.PoliciesMismatched },
	},
	{
		reflect.TypeOf(irdiff.SequenceDiff{}), sequenceDiffFieldProbes,
		map[string]string{"Name": nameIdentityExempt},
		17, "order_number_seq",
		func(c DiffJSONCounts) int { return c.SequencesMismatched },
	},
}

// TestContainedDiffSurfaceRosterEveryNestedFieldIsRendered is the G-6
// follow-on gate, render half. See the file comment for exactly which
// structs and surfaces it reaches.
func TestContainedDiffSurfaceRosterEveryNestedFieldIsRendered(t *testing.T) {
	for _, r := range containedDiffRosters {
		t.Run("irdiff."+r.typ.Name(), func(t *testing.T) {
			assertContainedDiffRosterCovers(t, r.typ, r.probes, r.exempt, r.floor, r.probeName, r.count)
		})
	}
}

func assertContainedDiffRosterCovers(
	t *testing.T,
	typ reflect.Type,
	probes map[string]containedDiffProbe,
	exempt map[string]string,
	floor int,
	probeName string,
	count func(DiffJSONCounts) int,
) {
	t.Helper()

	fields := make(map[string]bool, typ.NumField())
	for i := range typ.NumField() {
		f := typ.Field(i)
		if !f.IsExported() {
			continue
		}
		fields[f.Name] = true
	}
	if len(fields) == 0 {
		t.Fatalf("reflection found NO exported fields on %s — the walker has stopped matching the type", typ)
	}

	for name := range fields {
		_, probed := probes[name]
		reason, exempted := exempt[name]
		switch {
		case probed && exempted:
			t.Errorf("%s.%s is both probed and exempted — pick one", typ.Name(), name)
		case !probed && !exempted:
			t.Errorf("%s.%s is neither RENDERED by a probe nor exempted.\n"+
				"  A nested diff attribute with no render surface is a delta the comparison saw and the\n"+
				"  operator is never shown — Bug 227's shape one level down.", typ.Name(), name)
		case exempted && strings.TrimSpace(reason) == "":
			t.Errorf("%s.%s is exempted with an empty reason", typ.Name(), name)
		}
	}
	for name := range probes {
		if !fields[name] {
			t.Errorf("probe roster names %s.%s, which is not a field of the type (renamed or removed?)", typ.Name(), name)
		}
	}
	for name := range exempt {
		if !fields[name] {
			t.Errorf("exemption roster names %s.%s, which is not a field of the type (renamed or removed?)", typ.Name(), name)
		}
	}
	if len(probes) < floor {
		t.Errorf("only %d probed field(s) on %s; floor is %d — either the walker broke or fields were mass-exempted",
			len(probes), typ.Name(), floor)
	}

	for name, p := range probes {
		t.Run(name, func(t *testing.T) {
			td := irdiff.TableDiff{Name: "orders"}
			var d irdiff.SchemaDiff
			p.populate(&td, &d)
			if len(d.TablesMismatched) == 0 && !reflect.DeepEqual(td, irdiff.TableDiff{Name: "orders"}) {
				d.TablesMismatched = []irdiff.TableDiff{td}
			}
			if !d.HasChanges() {
				t.Fatalf("probe for %s.%s produced a diff with no changes — the renderer short-circuits on that", typ.Name(), name)
			}

			var sb strings.Builder
			if err := renderDiffText(&sb, diffBundle{
				srcEngine:  "postgres",
				tgtEngine:  "postgres",
				tgtDialect: p.dialect,
				diff:       d,
				expected:   &ir.Schema{},
				actual:     &ir.Schema{},
			}); err != nil {
				t.Fatalf("renderDiffText: %v", err)
			}
			got := sb.String()

			if len(p.wantTexts) == 0 {
				t.Fatalf("probe for %s.%s names no wantTexts — it would pass on any output", typ.Name(), name)
			}
			for _, want := range p.wantTexts {
				if !strings.Contains(got, want) {
					t.Errorf("text render of %s.%s does not carry the field's own content.\n  want substring: %q\n  got:\n%s",
						typ.Name(), name, want, got)
				}
			}
			// The Name exemption's behavioural cover: the entry identity must
			// appear in the render of every probe.
			if !strings.Contains(got, probeName) {
				t.Errorf("render of %s.%s does not name the entry %q — the Name identity exemption would be false", typ.Name(), name, probeName)
			}
			// The count half: the container counter this struct rides must
			// fire (per-entry granularity is deliberate; see the file doc).
			if n := count(summarise(d)); n == 0 {
				t.Errorf("summarise() counted ZERO for the container of %s.%s — a CI consumer sees no drift while the command exits 1",
					typ.Name(), name)
			}
		})
	}
}
