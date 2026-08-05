// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Render pins for the sections added in the C-7 / B-10 arc.
//
// The roster gate next door proves each TableDiff/SchemaDiff FIELD
// reaches the renderer at all. These pin the SHAPES within those fields —
// the v0.112.0 regression cycle confirmed all five foreign-key drift
// shapes rendered an empty section, so one shape passing is not evidence
// about the other four.

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	irdiff "sluicesync.dev/sluice/internal/ir/diff"
)

// renderOneTable renders a diff carrying exactly td and returns the text.
func renderOneTable(t *testing.T, td irdiff.TableDiff, expected *ir.Schema) string {
	t.Helper()
	if expected == nil {
		expected = &ir.Schema{}
	}
	var sb strings.Builder
	if err := renderDiffText(&sb, diffBundle{
		srcEngine:  "postgres",
		tgtEngine:  "postgres",
		tgtDialect: ir.DDLDialectANSI,
		diff:       irdiff.SchemaDiff{TablesMismatched: []irdiff.TableDiff{td}},
		expected:   expected,
		actual:     &ir.Schema{},
	}); err != nil {
		t.Fatalf("renderDiffText: %v", err)
	}
	return sb.String()
}

// TestForeignKeyDriftShapesAllRender walks every FK drift shape the
// comparison can produce. Each must put a CONSEQUENCE sentence in the
// output, not just an attribute pair.
func TestForeignKeyDriftShapesAllRender(t *testing.T) {
	for name, tc := range map[string]struct {
		td   irdiff.TableDiff
		want []string
	}{
		"missing": {
			td:   irdiff.TableDiff{Name: "orders", ForeignKeysMissing: []string{"fk_orders_user"}},
			want: []string{"fk_orders_user", "ACCEPTS ORPHAN ROWS THE SOURCE REJECTS"},
		},
		"extra": {
			td:   irdiff.TableDiff{Name: "orders", ForeignKeysExtra: []string{"fk_stale"}},
			want: []string{"DROP CONSTRAINT", "fk_stale", "not in source schema"},
		},
		"reference repointed": {
			td: irdiff.TableDiff{Name: "orders", ForeignKeysMismatched: []irdiff.ForeignKeyDiff{{
				Name:              "fk_orders_user",
				ExpectedReference: "(user_id) -> accounts(id)",
				ActualReference:   "(user_id) -> legacy_accounts(id)",
			}}},
			want: []string{"accounts(id)", "legacy_accounts(id)", "DIFFERENT parent"},
		},
		"on delete weakened to cascade": {
			td: irdiff.TableDiff{Name: "orders", ForeignKeysMismatched: []irdiff.ForeignKeyDiff{{
				Name: "fk_orders_user", ExpectedOnDelete: "RESTRICT", ActualOnDelete: "CASCADE",
			}}},
			want: []string{"RESTRICT", "CASCADE", "SILENTLY DESTROYS child rows"},
		},
		"on update diverged": {
			td: irdiff.TableDiff{Name: "orders", ForeignKeysMismatched: []irdiff.ForeignKeyDiff{{
				Name: "fk_orders_user", ExpectedOnUpdate: "CASCADE", ActualOnUpdate: "NO ACTION",
			}}},
			want: []string{"on update", "REFUSES a parent-row updated"},
		},
		"match weakened": {
			td: irdiff.TableDiff{Name: "orders", ForeignKeysMismatched: []irdiff.ForeignKeyDiff{{
				Name: "fk_orders_user", ExpectedMatch: "FULL", ActualMatch: "SIMPLE",
			}}},
			want: []string{"ACCEPTS PARTIALLY-NULL COMPOSITE KEYS THE SOURCE REJECTS"},
		},
		"deferrability lost": {
			td: irdiff.TableDiff{Name: "orders", ForeignKeysMismatched: []irdiff.ForeignKeyDiff{{
				Name: "fk_orders_user", DeferrabilityMismatched: true,
				ExpectedDeferrable: true, ExpectedInitiallyDeferred: true,
			}}},
			want: []string{"REJECTS A TRANSACTION ORDERING THE SOURCE ACCEPTS"},
		},
		"unnamed coverage note": {
			td:   irdiff.TableDiff{Name: "orders", ForeignKeysUnnamed: 3, ColumnsExtra: []string{"x"}},
			want: []string{"3 foreign key(s)", "carry no constraint name and were NOT compared"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			got := renderOneTable(t, tc.td, nil)
			for _, want := range tc.want {
				if !strings.Contains(got, want) {
					t.Errorf("render missing %q.\ngot:\n%s", want, got)
				}
			}
		})
	}
}

// TestMissingForeignKeySuggestionCarriesEveryEnforcedAttribute: an
// ADD CONSTRAINT suggestion that dropped MATCH FULL, a referential action
// or the deferrability would hand the operator a WEAKER constraint than
// the source's — a paste-ready way to reintroduce the exact drift the
// diff just reported.
func TestMissingForeignKeySuggestionCarriesEveryEnforcedAttribute(t *testing.T) {
	expected := &ir.Schema{Tables: []*ir.Table{{
		Name: "orders",
		ForeignKeys: []*ir.ForeignKey{{
			Name:              "fk_orders_user",
			Columns:           []string{"user_id", "tenant_id"},
			ReferencedTable:   "accounts",
			ReferencedColumns: []string{"id", "tenant_id"},
			OnDelete:          ir.FKActionCascade,
			OnUpdate:          ir.FKActionRestrict,
			Match:             ir.FKMatchFull,
			Deferrable:        true,
			InitiallyDeferred: true,
		}},
	}}}
	got := renderOneTable(t, irdiff.TableDiff{
		Name: "orders", ForeignKeysMissing: []string{"fk_orders_user"},
	}, expected)

	for _, want := range []string{
		`FOREIGN KEY ("user_id", "tenant_id")`,
		`REFERENCES "accounts" ("id", "tenant_id")`,
		"MATCH FULL",
		"ON DELETE CASCADE",
		"ON UPDATE RESTRICT",
		"DEFERRABLE INITIALLY DEFERRED",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("ADD CONSTRAINT suggestion missing %q — it would land a WEAKER constraint than the source's.\ngot:\n%s", want, got)
		}
	}
}

// TestRowLevelSecurityRenderShapes: the RLS section's three arms.
func TestRowLevelSecurityRenderShapes(t *testing.T) {
	for name, tc := range map[string]struct {
		td   irdiff.TableDiff
		want []string
	}{
		"disabled on target": {
			td: irdiff.TableDiff{
				Name: "orders", RLSMismatched: true,
				ExpectedRLSEnabled: true, ExpectedRLSForced: true,
			},
			want: []string{
				"EVERY ROW IS VISIBLE TO EVERY ROLE",
				`ALTER TABLE "orders" ENABLE ROW LEVEL SECURITY;`,
				`ALTER TABLE "orders" FORCE ROW LEVEL SECURITY;`,
			},
		},
		"force lost on target": {
			td: irdiff.TableDiff{
				Name: "orders", RLSMismatched: true,
				ExpectedRLSEnabled: true, ActualRLSEnabled: true, ExpectedRLSForced: true,
			},
			want: []string{"OWNER bypasses", `ALTER TABLE "orders" FORCE ROW LEVEL SECURITY;`},
		},
		"target stricter than source": {
			td: irdiff.TableDiff{
				Name: "orders", RLSMismatched: true, ActualRLSEnabled: true,
			},
			want: []string{"the target enforces row-level security the source does not"},
		},
		"policy write filter lost": {
			td: irdiff.TableDiff{Name: "orders", PoliciesMismatched: []irdiff.PolicyDiff{{
				Name: "tenant_isolation", ExpectedCheck: "tenant_id = 1", ActualCheck: "",
			}}},
			want: []string{"ACCEPTS WRITES", "another tenant"},
		},
		"policy widened to permissive": {
			td: irdiff.TableDiff{Name: "orders", PoliciesMismatched: []irdiff.PolicyDiff{{
				Name: "p", PermissiveMismatched: true, ActualPermissive: true,
			}}},
			want: []string{"ADMITS ROWS THE SOURCE HIDES"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			got := renderOneTable(t, tc.td, nil)
			for _, want := range tc.want {
				if !strings.Contains(got, want) {
					t.Errorf("render missing %q.\ngot:\n%s", want, got)
				}
			}
		})
	}
}

// TestMissingSequenceSuggestionRendersEveryOption: a CREATE SEQUENCE that
// dropped INCREMENT or MINVALUE would recreate the sequence at PG's
// defaults, which is the original silent-loss item restated as a
// copy-paste suggestion.
func TestMissingSequenceSuggestionRendersEveryOption(t *testing.T) {
	expected := &ir.Schema{
		Tables: []*ir.Table{{Name: "orders"}},
		Sequences: []*ir.Sequence{{
			Name: "order_number_seq", DataType: "bigint",
			Start: 1000, Increment: 5, MinValue: 1, MaxValue: 999999, Cache: 10,
			OwnedByTable: "orders", OwnedByColumn: "number",
		}},
	}
	var sb strings.Builder
	if err := renderDiffText(&sb, diffBundle{
		srcEngine: "postgres", tgtEngine: "postgres", tgtDialect: ir.DDLDialectANSI,
		diff:     irdiff.SchemaDiff{SequencesMissing: []string{"order_number_seq"}},
		expected: expected, actual: &ir.Schema{},
	}); err != nil {
		t.Fatalf("renderDiffText: %v", err)
	}
	got := sb.String()
	for _, want := range []string{
		"AS bigint", "INCREMENT BY 5", "MINVALUE 1", "MAXVALUE 999999",
		"START WITH 1000", "CACHE 10", "NO CYCLE",
		`ALTER SEQUENCE "order_number_seq" OWNED BY "orders"."number";`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("CREATE SEQUENCE suggestion missing %q — it would recreate the sequence at PG defaults.\ngot:\n%s", want, got)
		}
	}
}

// TestNoDriftRendersNoConstraintSections is the must-not-break control
// the v0.112.0 regression cycle checked on both binaries: an in-sync pair
// stays quiet, so none of the new sections leaks a false positive into a
// CI gate.
func TestNoDriftRendersNoConstraintSections(t *testing.T) {
	var sb strings.Builder
	if err := renderDiffText(&sb, diffBundle{
		srcEngine: "postgres", tgtEngine: "postgres", tgtDialect: ir.DDLDialectANSI,
		diff:     irdiff.SchemaDiff{},
		expected: &ir.Schema{}, actual: &ir.Schema{},
	}); err != nil {
		t.Fatalf("renderDiffText: %v", err)
	}
	got := sb.String()
	if !strings.Contains(got, "No drift detected") {
		t.Fatalf("an empty diff did not report in-sync:\n%s", got)
	}
	for _, forbidden := range []string{
		"foreign key", "EXCLUDE", "row-level security", "policy", "sequence", "COVERAGE",
	} {
		if strings.Contains(got, forbidden) {
			t.Errorf("an in-sync diff mentioned %q:\n%s", forbidden, got)
		}
	}
	if c := summarise(irdiff.SchemaDiff{}); c != (DiffJSONCounts{}) {
		t.Errorf("summarise(empty) = %+v; want all zeros", c)
	}
}
