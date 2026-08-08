// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package diff

// Behavioural pins for the schema-diff blind spots closed in this arc:
// standalone sequences and row-level security (audit 2026-08-04 B-10),
// plus the referential-action rendering defect the FK comparison shipped
// with in v0.112.0 (C-7 / Bug 227 symptom 3).

import (
	"strings"
	"testing"
	"unicode"

	"sluicesync.dev/sluice/internal/ir"
)

// tableWith returns a one-table schema, for the RLS/policy pins.
func tableWith(mutate func(t *ir.Table)) *ir.Schema {
	tbl := &ir.Table{
		Name:    "orders",
		Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
	}
	if mutate != nil {
		mutate(tbl)
	}
	return &ir.Schema{Tables: []*ir.Table{tbl}}
}

// ---- foreign-key referential actions (C-7 symptom 3) ----

// TestForeignKeyActionsRenderAsKeywordsNotCodePoints pins the whole
// FKAction and FKMatch families, not one representative.
//
// [ir.FKAction] and [ir.FKMatch] are uint8-backed with a String() method.
// `string(exp.OnDelete)` is therefore LEGAL Go that yields the raw code
// point — "\x02" for CASCADE — and `go vet` says nothing about it
// PRECISELY BECAUSE the types implement Stringer (stringintconv does not
// fire). That form shipped in v0.112.0 and put control bytes in the JSON
// diff's referential-action fields. Neither the compiler nor vet can
// catch a regression here, so this test is the only thing that does.
func TestForeignKeyActionsRenderAsKeywordsNotCodePoints(t *testing.T) {
	// Every FKAction value, not one representative: the defect is a
	// per-value code-point leak, so a single-value pin proves nothing
	// about the other four.
	actions := []struct {
		val  ir.FKAction
		want string
	}{
		{ir.FKActionNoAction, "NO ACTION"},
		{ir.FKActionRestrict, "RESTRICT"},
		{ir.FKActionCascade, "CASCADE"},
		{ir.FKActionSetNull, "SET NULL"},
		{ir.FKActionSetDefault, "SET DEFAULT"},
	}
	for _, onDelete := range actions {
		for _, onUpdate := range actions {
			if onDelete.val == ir.FKActionNoAction && onUpdate.val == ir.FKActionNoAction {
				continue // no drift to report against the baseline below
			}
			exp := tableWith(func(tb *ir.Table) {
				tb.ForeignKeys = []*ir.ForeignKey{{
					Name: "fk", Columns: []string{"id"},
					ReferencedTable: "users", ReferencedColumns: []string{"id"},
					OnDelete: onDelete.val, OnUpdate: onUpdate.val,
				}}
			})
			act := tableWith(func(tb *ir.Table) {
				tb.ForeignKeys = []*ir.ForeignKey{{
					Name: "fk", Columns: []string{"id"},
					ReferencedTable: "users", ReferencedColumns: []string{"id"},
				}}
			})
			d := Schemas(exp, act, Options{})
			if len(d.TablesMismatched) != 1 || len(d.TablesMismatched[0].ForeignKeysMismatched) != 1 {
				t.Fatalf("on_delete=%v on_update=%v: want one FK mismatch, got %+v", onDelete.val, onUpdate.val, d)
			}
			fd := d.TablesMismatched[0].ForeignKeysMismatched[0]
			if onDelete.val != ir.FKActionNoAction {
				assertKeyword(t, "expected_on_delete", fd.ExpectedOnDelete, onDelete.want)
				assertKeyword(t, "actual_on_delete", fd.ActualOnDelete, "NO ACTION")
			}
			if onUpdate.val != ir.FKActionNoAction {
				assertKeyword(t, "expected_on_update", fd.ExpectedOnUpdate, onUpdate.want)
				assertKeyword(t, "actual_on_update", fd.ActualOnUpdate, "NO ACTION")
			}
		}
	}

	// FKMatch is the same shape and the same trap, so it gets the same
	// treatment rather than being assumed covered by the FKAction pin.
	for _, m := range []struct {
		val  ir.FKMatch
		want string
	}{
		{ir.FKMatchFull, "FULL"},
		{ir.FKMatchPartial, "PARTIAL"},
	} {
		exp := tableWith(func(tb *ir.Table) {
			tb.ForeignKeys = []*ir.ForeignKey{{
				Name: "fk", Columns: []string{"id"},
				ReferencedTable: "users", ReferencedColumns: []string{"id"}, Match: m.val,
			}}
		})
		act := tableWith(func(tb *ir.Table) {
			tb.ForeignKeys = []*ir.ForeignKey{{
				Name: "fk", Columns: []string{"id"},
				ReferencedTable: "users", ReferencedColumns: []string{"id"},
			}}
		})
		d := Schemas(exp, act, Options{})
		fd := d.TablesMismatched[0].ForeignKeysMismatched[0]
		assertKeyword(t, "expected_match", fd.ExpectedMatch, m.want)
		assertKeyword(t, "actual_match", fd.ActualMatch, "SIMPLE")
	}
}

// assertKeyword requires the exact keyword AND rejects any control
// character. The equality check catches today's known defect; the
// control-character scan catches the CLASS — any future numeric-backed
// enum rendered by conversion instead of String().
func assertKeyword(t *testing.T, field, got, want string) {
	t.Helper()
	if got != want {
		t.Errorf("%s = %q; want %q — a uint8-backed FK enum was rendered with string(...) instead of .String()", field, got, want)
	}
	for _, r := range got {
		if unicode.IsControl(r) {
			t.Errorf("%s = %q contains a control character: the raw code point leaked through a string(...) conversion", field, got)
			return
		}
	}
}

// TestForeignKeysUnnamedIsReportedNotSilentlySkipped pins the coverage
// counter at the unit level. It is unreachable on a real MySQL or PG rig
// — both engines auto-name FK constraints — so there is deliberately no
// integration fixture for it.
func TestForeignKeysUnnamedIsReportedNotSilentlySkipped(t *testing.T) {
	unnamed := func() *ir.Schema {
		return tableWith(func(tb *ir.Table) {
			tb.ForeignKeys = []*ir.ForeignKey{
				{Columns: []string{"id"}, ReferencedTable: "users", ReferencedColumns: []string{"id"}},
				{Name: "fk_named", Columns: []string{"id"}, ReferencedTable: "t", ReferencedColumns: []string{"id"}},
			}
		})
	}
	// Both sides carry one unnamed FK and are otherwise IDENTICAL. That is
	// the case the per-table counter could never surface: with no other
	// drift the TableDiff fails hasChanges and is dropped, taking the
	// coverage figure with it. The schema-level total survives.
	d := Schemas(unnamed(), unnamed(), Options{})
	if len(d.TablesMismatched) != 0 {
		t.Fatalf("an unnamed FK is not drift; it must not put the table in TablesMismatched: %+v", d.TablesMismatched)
	}
	if d.HasChanges() {
		t.Error("HasChanges() = true on an unnamed-FK-only pair — that would exit 1 forever on a healthy schema")
	}
	if got := d.ForeignKeysUnnamed; got != 2 {
		t.Errorf("SchemaDiff.ForeignKeysUnnamed = %d; want 2 (one per side) — the skip must be visible even with no other drift", got)
	}

	// And when the table DOES drift, the per-table breakdown still carries
	// it so the operator can see which table the skip was on.
	drifted := tableWith(func(tb *ir.Table) {
		tb.Columns = append(tb.Columns, &ir.Column{Name: "extra", Type: ir.Integer{Width: 32}})
		tb.ForeignKeys = []*ir.ForeignKey{
			{Columns: []string{"id"}, ReferencedTable: "users", ReferencedColumns: []string{"id"}},
		}
	})
	d = Schemas(unnamed(), drifted, Options{})
	if len(d.TablesMismatched) != 1 || d.TablesMismatched[0].ForeignKeysUnnamed == 0 {
		t.Errorf("per-table ForeignKeysUnnamed lost on a drifting table: %+v", d.TablesMismatched)
	}
}

// ---- row-level security (B-10) ----

// TestRLSDisabledOnTargetIsReported is the headline B-10 case: before
// this, the target below reported "in sync" and exit 0 while every tenant
// read every tenant's rows.
func TestRLSDisabledOnTargetIsReported(t *testing.T) {
	exp := tableWith(func(tb *ir.Table) { tb.RLSEnabled = true; tb.RLSForced = true })
	act := tableWith(nil) // target ran DISABLE ROW LEVEL SECURITY

	d := Schemas(exp, act, Options{})
	if !d.HasChanges() {
		t.Fatal("a target with row-level security switched off reported IN SYNC")
	}
	td := d.TablesMismatched[0]
	if !td.RLSMismatched {
		t.Fatal("RLSMismatched = false on a table whose RLS was disabled")
	}
	if !td.ExpectedRLSEnabled || td.ActualRLSEnabled {
		t.Errorf("enabled pair = expected %v actual %v; want true/false", td.ExpectedRLSEnabled, td.ActualRLSEnabled)
	}
	if !td.ExpectedRLSForced || td.ActualRLSForced {
		t.Errorf("forced pair = expected %v actual %v; want true/false", td.ExpectedRLSForced, td.ActualRLSForced)
	}
	if got := d.Summary(); !strings.Contains(got, "row-level-security mismatch") {
		t.Errorf("Summary() = %q; want it to name the RLS mismatch", got)
	}
}

// TestRLSForceOnlyDriftIsReported: FORCE alone decides whether the table
// OWNER bypasses every policy, so ENABLE matching on both sides is not
// enough to call the pair in sync.
func TestRLSForceOnlyDriftIsReported(t *testing.T) {
	exp := tableWith(func(tb *ir.Table) { tb.RLSEnabled = true; tb.RLSForced = true })
	act := tableWith(func(tb *ir.Table) { tb.RLSEnabled = true })
	d := Schemas(exp, act, Options{})
	if !d.HasChanges() || !d.TablesMismatched[0].RLSMismatched {
		t.Fatalf("FORCE-only drift not reported: %+v", d)
	}
}

// TestRLSFlagsMatchingIsInSync is a must-not-break control: an identical
// pair must stay rc=0. A diff that cried drift on every healthy run would
// be suppressed, and a suppressed check hides the real thing.
func TestRLSFlagsMatchingIsInSync(t *testing.T) {
	for _, tc := range []struct{ enabled, forced bool }{
		{false, false}, {true, false}, {true, true},
	} {
		mk := func() *ir.Schema {
			return tableWith(func(tb *ir.Table) { tb.RLSEnabled = tc.enabled; tb.RLSForced = tc.forced })
		}
		if d := Schemas(mk(), mk(), Options{}); d.HasChanges() {
			t.Errorf("identical RLS (enabled=%v forced=%v) reported drift: %s", tc.enabled, tc.forced, d.Summary())
		}
	}
}

// TestPolicyDriftShapes walks every attribute of [ir.Policy] — the
// comparison dispatches on the whole struct, so one representative
// attribute would say nothing about the others.
func TestPolicyDriftShapes(t *testing.T) {
	base := func() *ir.Policy {
		return &ir.Policy{
			Name: "tenant_isolation", Command: "ALL", Permissive: true,
			Roles: []string{"app"}, Using: "tenant_id = 1", Check: "tenant_id = 1",
		}
	}
	for name, tc := range map[string]struct {
		mutate func(p *ir.Policy)
		assert func(t *testing.T, pd PolicyDiff)
	}{
		"command": {
			mutate: func(p *ir.Policy) { p.Command = "SELECT" },
			assert: func(t *testing.T, pd PolicyDiff) {
				if pd.ExpectedCommand != "ALL" || pd.ActualCommand != "SELECT" {
					t.Errorf("command pair = %q/%q", pd.ExpectedCommand, pd.ActualCommand)
				}
			},
		},
		"permissive": {
			mutate: func(p *ir.Policy) { p.Permissive = false },
			assert: func(t *testing.T, pd PolicyDiff) {
				if !pd.PermissiveMismatched || !pd.ExpectedPermissive || pd.ActualPermissive {
					t.Errorf("permissive = %+v", pd)
				}
			},
		},
		"roles": {
			mutate: func(p *ir.Policy) { p.Roles = []string{"public"} },
			assert: func(t *testing.T, pd PolicyDiff) {
				if pd.ExpectedRoles != "app" || pd.ActualRoles != "public" {
					t.Errorf("roles pair = %q/%q", pd.ExpectedRoles, pd.ActualRoles)
				}
			},
		},
		"using": {
			mutate: func(p *ir.Policy) { p.Using = "" },
			assert: func(t *testing.T, pd PolicyDiff) {
				if pd.ExpectedUsing != "tenant_id = 1" || pd.ActualUsing != "" {
					t.Errorf("using pair = %q/%q", pd.ExpectedUsing, pd.ActualUsing)
				}
			},
		},
		"check": {
			mutate: func(p *ir.Policy) { p.Check = "" },
			assert: func(t *testing.T, pd PolicyDiff) {
				if pd.ExpectedCheck != "tenant_id = 1" || pd.ActualCheck != "" {
					t.Errorf("check pair = %q/%q", pd.ExpectedCheck, pd.ActualCheck)
				}
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			exp := tableWith(func(tb *ir.Table) { tb.RLSEnabled = true; tb.Policies = []*ir.Policy{base()} })
			p := base()
			tc.mutate(p)
			act := tableWith(func(tb *ir.Table) { tb.RLSEnabled = true; tb.Policies = []*ir.Policy{p} })

			d := Schemas(exp, act, Options{})
			if !d.HasChanges() {
				t.Fatalf("policy %s drift not reported", name)
			}
			mm := d.TablesMismatched[0].PoliciesMismatched
			if len(mm) != 1 {
				t.Fatalf("PoliciesMismatched = %+v; want one entry", mm)
			}
			tc.assert(t, mm[0])
		})
	}
}

// TestPolicyRoleOrderIsNotDrift: PG does not guarantee pg_policies.roles
// ordering across catalog reads, so a reorder must not read as drift.
// Control for the roles comparison above.
func TestPolicyRoleOrderIsNotDrift(t *testing.T) {
	mk := func(roles ...string) *ir.Schema {
		return tableWith(func(tb *ir.Table) {
			tb.RLSEnabled = true
			tb.Policies = []*ir.Policy{{Name: "p", Command: "ALL", Permissive: true, Roles: roles}}
		})
	}
	if d := Schemas(mk("app", "readonly"), mk("readonly", "app"), Options{}); d.HasChanges() {
		t.Errorf("a reordered role list reported drift: %s", d.Summary())
	}
}

// TestPolicyMissingAndExtra pins the set semantics, including that
// IgnoreExtras scopes the extras and NOT the missing ones.
func TestPolicyMissingAndExtra(t *testing.T) {
	pol := func(name string) *ir.Policy {
		return &ir.Policy{Name: name, Command: "ALL", Permissive: true, Roles: []string{"app"}}
	}
	exp := tableWith(func(tb *ir.Table) { tb.RLSEnabled = true; tb.Policies = []*ir.Policy{pol("a")} })
	act := tableWith(func(tb *ir.Table) { tb.RLSEnabled = true; tb.Policies = []*ir.Policy{pol("b")} })

	d := Schemas(exp, act, Options{})
	td := d.TablesMismatched[0]
	if len(td.PoliciesMissing) != 1 || td.PoliciesMissing[0] != "a" {
		t.Errorf("PoliciesMissing = %v; want [a]", td.PoliciesMissing)
	}
	if len(td.PoliciesExtra) != 1 || td.PoliciesExtra[0] != "b" {
		t.Errorf("PoliciesExtra = %v; want [b]", td.PoliciesExtra)
	}

	d = Schemas(exp, act, Options{IgnoreExtras: true})
	td = d.TablesMismatched[0]
	if len(td.PoliciesExtra) != 0 {
		t.Errorf("IgnoreExtras left PoliciesExtra = %v", td.PoliciesExtra)
	}
	if len(td.PoliciesMissing) != 1 {
		t.Errorf("IgnoreExtras suppressed PoliciesMissing = %v; it must not", td.PoliciesMissing)
	}
}

// TestRLSFlagsAreNotScopedByIgnoreExtras: a flag is not an "extra on
// target" object. An operator who asked to ignore other applications'
// tables has not asked to stop hearing that this one stopped enforcing.
func TestRLSFlagsAreNotScopedByIgnoreExtras(t *testing.T) {
	exp := tableWith(func(tb *ir.Table) { tb.RLSEnabled = true })
	act := tableWith(nil)
	if d := Schemas(exp, act, Options{IgnoreExtras: true}); !d.HasChanges() {
		t.Fatal("--ignore-extras suppressed an RLS-disabled report")
	}
}

// ---- standalone sequences (B-10) ----

func seq(mutate func(s *ir.Sequence)) *ir.Schema {
	s := &ir.Sequence{
		Name: "order_number_seq", DataType: "bigint",
		Start: 1000, Increment: 5, MinValue: 1, MaxValue: 1 << 40, Cache: 1,
	}
	if mutate != nil {
		mutate(s)
	}
	return &ir.Schema{
		Tables:    []*ir.Table{{Name: "orders", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}}},
		Sequences: []*ir.Sequence{s},
	}
}

// TestSequenceDriftShapes walks every COMPARED attribute of
// [ir.Sequence]. The comparison dispatches on the whole struct, so one
// representative attribute would say nothing about the rest — and the
// original silent-loss item was precisely a re-optioned sequence.
func TestSequenceDriftShapes(t *testing.T) {
	for name, tc := range map[string]struct {
		mutate func(s *ir.Sequence)
		assert func(t *testing.T, sd SequenceDiff)
	}{
		"data type": {
			mutate: func(s *ir.Sequence) { s.DataType = "integer" },
			assert: func(t *testing.T, sd SequenceDiff) {
				if sd.ExpectedDataType != "bigint" || sd.ActualDataType != "integer" {
					t.Errorf("data-type pair = %q/%q", sd.ExpectedDataType, sd.ActualDataType)
				}
			},
		},
		"start": {
			mutate: func(s *ir.Sequence) { s.Start = 1 },
			assert: func(t *testing.T, sd SequenceDiff) {
				if sd.ExpectedStart != "1000" || sd.ActualStart != "1" {
					t.Errorf("start pair = %q/%q", sd.ExpectedStart, sd.ActualStart)
				}
			},
		},
		"increment": {
			mutate: func(s *ir.Sequence) { s.Increment = 1 },
			assert: func(t *testing.T, sd SequenceDiff) {
				if sd.ExpectedIncrement != "5" || sd.ActualIncrement != "1" {
					t.Errorf("increment pair = %q/%q", sd.ExpectedIncrement, sd.ActualIncrement)
				}
			},
		},
		"min value": {
			mutate: func(s *ir.Sequence) { s.MinValue = -100 },
			assert: func(t *testing.T, sd SequenceDiff) {
				if sd.ExpectedMinValue != "1" || sd.ActualMinValue != "-100" {
					t.Errorf("min pair = %q/%q", sd.ExpectedMinValue, sd.ActualMinValue)
				}
			},
		},
		"max value": {
			mutate: func(s *ir.Sequence) { s.MaxValue = 100 },
			assert: func(t *testing.T, sd SequenceDiff) {
				if sd.ActualMaxValue != "100" {
					t.Errorf("max pair = %q/%q", sd.ExpectedMaxValue, sd.ActualMaxValue)
				}
			},
		},
		"cache": {
			mutate: func(s *ir.Sequence) { s.Cache = 50 },
			assert: func(t *testing.T, sd SequenceDiff) {
				if sd.ExpectedCache != "1" || sd.ActualCache != "50" {
					t.Errorf("cache pair = %q/%q", sd.ExpectedCache, sd.ActualCache)
				}
			},
		},
		"cycle": {
			mutate: func(s *ir.Sequence) { s.Cycle = true },
			assert: func(t *testing.T, sd SequenceDiff) {
				if !sd.CycleMismatched || sd.ExpectedCycle || !sd.ActualCycle {
					t.Errorf("cycle = %+v", sd)
				}
			},
		},
		"owned by": {
			mutate: func(s *ir.Sequence) { s.OwnedByTable = "orders"; s.OwnedByColumn = "id" },
			assert: func(t *testing.T, sd SequenceDiff) {
				if sd.ExpectedOwnedBy != "<none>" || sd.ActualOwnedBy != "orders.id" {
					t.Errorf("owned-by pair = %q/%q", sd.ExpectedOwnedBy, sd.ActualOwnedBy)
				}
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			d := Schemas(seq(nil), seq(tc.mutate), Options{})
			if !d.HasChanges() {
				t.Fatalf("sequence %s drift not reported", name)
			}
			if len(d.SequencesMismatched) != 1 {
				t.Fatalf("SequencesMismatched = %+v; want one entry", d.SequencesMismatched)
			}
			tc.assert(t, d.SequencesMismatched[0])
		})
	}
}

// TestSequenceMissingAndExtra pins the set semantics and the IgnoreExtras
// scoping.
func TestSequenceMissingAndExtra(t *testing.T) {
	exp := seq(func(s *ir.Sequence) { s.Name = "a" })
	act := seq(func(s *ir.Sequence) { s.Name = "b" })

	d := Schemas(exp, act, Options{})
	if len(d.SequencesMissing) != 1 || d.SequencesMissing[0] != "a" {
		t.Errorf("SequencesMissing = %v; want [a]", d.SequencesMissing)
	}
	if len(d.SequencesExtra) != 1 || d.SequencesExtra[0] != "b" {
		t.Errorf("SequencesExtra = %v; want [b]", d.SequencesExtra)
	}

	d = Schemas(exp, act, Options{IgnoreExtras: true})
	if len(d.SequencesExtra) != 0 {
		t.Errorf("IgnoreExtras left SequencesExtra = %v", d.SequencesExtra)
	}
	if len(d.SequencesMissing) != 1 {
		t.Errorf("IgnoreExtras suppressed SequencesMissing = %v; it must not", d.SequencesMissing)
	}
}

// TestSequenceDefaultDataTypeSpellingIsNotDrift: [ir.Sequence] documents
// "" as meaning bigint, so a reader that spells the default out must not
// report drift against one that leaves it blank. Must-not-break control.
func TestSequenceDefaultDataTypeSpellingIsNotDrift(t *testing.T) {
	exp := seq(func(s *ir.Sequence) { s.DataType = "" })
	act := seq(func(s *ir.Sequence) { s.DataType = "bigint" })
	if d := Schemas(exp, act, Options{}); d.HasChanges() {
		t.Errorf("'' vs 'bigint' data type reported drift: %s", d.Summary())
	}
}

// TestSequencePositionIsDeliberatelyNotCompared pins the documented
// coverage gap so it stays a DECISION rather than becoming an accident
// somebody re-derives later. See [SequenceDiff] for the reasoning: the
// position moves under write load, so comparing it would report drift on
// every run of a healthy pair.
//
// If this test ever fails, the comparison grew position awareness — that
// may be correct, but the SequenceDiff doc comment claiming otherwise
// must change in the same commit.
func TestSequencePositionIsDeliberatelyNotCompared(t *testing.T) {
	exp := seq(func(s *ir.Sequence) { s.LastValue = 500000; s.LastValueIsCalled = true; s.LastValueValid = true })
	act := seq(func(s *ir.Sequence) { s.LastValue = 1; s.LastValueIsCalled = false; s.LastValueValid = true })
	if d := Schemas(exp, act, Options{}); d.HasChanges() {
		t.Errorf("sequence POSITION was compared (%s); SequenceDiff documents that it is not — update the doc or the test", d.Summary())
	}
}

// TestIdenticalSequencesAreInSync is the must-not-break control.
func TestIdenticalSequencesAreInSync(t *testing.T) {
	if d := Schemas(seq(nil), seq(nil), Options{}); d.HasChanges() {
		t.Errorf("identical sequences reported drift: %s", d.Summary())
	}
}

// ---- the target-cannot-hold-RLS suppression (Bug 234's deferred list) ----

// TestTargetCannotHoldRowLevelSecuritySuppressesTheWholeComparison is the
// phantom-drift half. A Postgres source's RLS flags and policies reach a
// MySQL target as a one-time WARN and nothing else — MySQL has no
// row-level security, its SchemaWriter drops them, and its SchemaReader
// never populates them — so the expected side asserts something the actual
// side structurally cannot carry, on a target `sluice migrate` itself
// created.
func TestTargetCannotHoldRowLevelSecuritySuppressesTheWholeComparison(t *testing.T) {
	exp := tableWith(func(tb *ir.Table) {
		tb.RLSEnabled = true
		tb.RLSForced = true
		tb.Policies = []*ir.Policy{
			{Name: "tenant_isolation", Command: "ALL", Using: "(tenant_id = current_tenant())"},
			{Name: "admin_all", Command: "SELECT", Using: "true"},
		}
	})
	act := tableWith(nil) // what a MySQL catalog reads back, always

	if d := Schemas(exp, act, Options{}); !d.HasChanges() {
		t.Fatal("the un-suppressed comparison reported no drift; this test's premise is that it DOES, " +
			"and without that premise the suppression below grades nothing")
	}

	d := Schemas(exp, act, Options{TargetCannotHoldRowLevelSecurity: true})
	if d.HasChanges() {
		t.Fatalf("a target with no row-level security still reported drift: %s\n%+v",
			d.Summary(), d.TablesMismatched)
	}
}

// TestTargetCannotHoldRowLevelSecurityDoesNotReachAPostgresTarget is the
// over-suppression half, and it is the one that matters: the option must be
// reachable ONLY when the target genuinely has no RLS. A Postgres target
// that silently stopped enforcing its policies is the exact silent-loss the
// B-10 comparison was added for, so every shape it catches must still be
// caught with the option off.
func TestTargetCannotHoldRowLevelSecurityDoesNotReachAPostgresTarget(t *testing.T) {
	for _, tc := range []struct {
		name     string
		expected func(*ir.Table)
		actual   func(*ir.Table)
	}{
		{
			"RLS disabled on target",
			func(tb *ir.Table) { tb.RLSEnabled = true; tb.RLSForced = true },
			nil,
		},
		{
			"FORCE dropped on target",
			func(tb *ir.Table) { tb.RLSEnabled = true; tb.RLSForced = true },
			func(tb *ir.Table) { tb.RLSEnabled = true },
		},
		{
			"policy dropped on target",
			func(tb *ir.Table) {
				tb.RLSEnabled = true
				tb.Policies = []*ir.Policy{{Name: "tenant_isolation", Command: "ALL", Using: "(t = c())"}}
			},
			func(tb *ir.Table) { tb.RLSEnabled = true },
		},
		{
			"policy predicate widened on target",
			func(tb *ir.Table) {
				tb.RLSEnabled = true
				tb.Policies = []*ir.Policy{{Name: "tenant_isolation", Command: "ALL", Using: "(t = c())"}}
			},
			func(tb *ir.Table) {
				tb.RLSEnabled = true
				tb.Policies = []*ir.Policy{{Name: "tenant_isolation", Command: "ALL", Using: "true"}}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := Schemas(tableWith(tc.expected), tableWith(tc.actual), Options{})
			if !d.HasChanges() {
				t.Fatalf("%s reported IN SYNC with the suppression OFF — the option's default must leave "+
					"every real RLS drift reportable", tc.name)
			}
		})
	}
}
