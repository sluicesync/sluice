// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package diff

import (
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestDiffChecks_ValidityUPR1C pins that the drift lane can see a CHECK
// constraint whose EXPRESSION matches but whose enforcement does not.
//
// WHY. v0.141.4 warns, at migrate time, that it is recreating a source
// NOT VALID CHECK as validating — Postgres rejects NOT VALID inline in
// CREATE TABLE, so sluice cannot yet carry it (UPR-1b). That warning is a
// one-shot log line. `sluice diff` is the independent evidence surface an
// operator uses afterwards to confirm the target matches the source, and it
// compared checks on Name and Expr only — so it reported the schemas as
// MATCHING for exactly the divergence the migrate had just warned about.
//
// Two things disagreeing about the same fact, with the durable one saying
// "clean", is the no-independent-expected-value shape CLAUDE.md names. The
// warning was right and the diff was wrong.
func TestDiffChecks_ValidityUPR1C(t *testing.T) {
	schemaWith := func(notValid bool) *ir.Schema {
		return &ir.Schema{Tables: []*ir.Table{{
			Name: "t",
			CheckConstraints: []*ir.CheckConstraint{{
				Name:     "ck",
				Expr:     "(qty >= 0)",
				NotValid: notValid,
			}},
		}}}
	}

	t.Run("identical expression, different validity, is a mismatch", func(t *testing.T) {
		d := Schemas(schemaWith(true), schemaWith(false), Options{})
		if len(d.TablesMismatched) == 0 {
			t.Fatal("diff reported the schemas as MATCHING while the source leaves the constraint " +
				"unvalidated and the target enforces it — the target rejects rows the source holds")
		}
		td := d.TablesMismatched[0]
		if len(td.ChecksMismatched) != 1 {
			t.Fatalf("ChecksMismatched = %d; want 1", len(td.ChecksMismatched))
		}
		ck := td.ChecksMismatched[0]
		if !ck.ValidityMismatched {
			t.Error("ValidityMismatched not set, so nothing downstream can tell this apart from an " +
				"expression difference")
		}
		if !ck.ExpectedNotValid || ck.ActualNotValid {
			t.Errorf("expected/actual validity not carried: %+v", ck)
		}
		// The expression matched, so the Expr fields stay empty — which is
		// what the renderer keys on to avoid suggesting `CHECK ()`.
		if ck.ExpectedExpr != "" || ck.ActualExpr != "" {
			t.Errorf("expression fields populated for an expression-identical pair: %+v", ck)
		}
	})

	t.Run("identical validity is still silent", func(t *testing.T) {
		if d := Schemas(schemaWith(true), schemaWith(true), Options{}); len(d.TablesMismatched) != 0 {
			t.Error("two identical schemas reported as mismatched — a diff that cries wolf on a " +
				"clean pair is worse than one that stays quiet")
		}
		if d := Schemas(schemaWith(false), schemaWith(false), Options{}); len(d.TablesMismatched) != 0 {
			t.Error("two identical validated schemas reported as mismatched")
		}
	})
}
