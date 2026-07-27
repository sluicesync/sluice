// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Emitter pins for audit 2026-07-26 SL-7 — foreign-key MATCH and DEFERRABLE
// are constraint-STRENGTH attributes and must reach the target.
//
// Both were read, dropped, and never emitted:
//   - MATCH FULL on the source rejects `INSERT (1, NULL)` into a composite FK;
//     the plain MATCH SIMPLE constraint sluice landed accepts it, so the
//     target admitted rows the source forbids, silently, forever.
//   - DEFERRABLE INITIALLY DEFERRED lets `BEGIN; INSERT child; INSERT parent;
//     COMMIT;` succeed; against the immediate constraint sluice landed, that
//     transaction aborts — a workload that worked before cutover breaks after.
package postgres

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

func TestEmitAddForeignKey_CarriesStrengthAttributes(t *testing.T) {
	base := func() *ir.ForeignKey {
		return &ir.ForeignKey{
			Name:              "child_fk",
			Columns:           []string{"pa", "pb"},
			ReferencedTable:   "parent",
			ReferencedColumns: []string{"a", "b"},
		}
	}

	cases := []struct {
		name       string
		mutate     func(*ir.ForeignKey)
		wantSubstr []string
		absent     []string
	}{
		{
			name:   "plain FK is byte-identical to before (no attribute clauses)",
			mutate: func(*ir.ForeignKey) {},
			absent: []string{"MATCH", "DEFERRABLE"},
		},
		{
			name:       "MATCH FULL",
			mutate:     func(fk *ir.ForeignKey) { fk.Match = ir.FKMatchFull },
			wantSubstr: []string{"MATCH FULL"},
			absent:     []string{"DEFERRABLE"},
		},
		{
			name:       "DEFERRABLE INITIALLY DEFERRED",
			mutate:     func(fk *ir.ForeignKey) { fk.Deferrable = true; fk.InitiallyDeferred = true },
			wantSubstr: []string{"DEFERRABLE", "INITIALLY DEFERRED"},
			absent:     []string{"MATCH"},
		},
		{
			name:       "DEFERRABLE INITIALLY IMMEDIATE",
			mutate:     func(fk *ir.ForeignKey) { fk.Deferrable = true },
			wantSubstr: []string{"DEFERRABLE"},
			absent:     []string{"INITIALLY DEFERRED"},
		},
		{
			// Both axes at once — they are independent, and an emitter that
			// handled them with an if/else would pass every single-axis case.
			name: "both axes together",
			mutate: func(fk *ir.ForeignKey) {
				fk.Match = ir.FKMatchFull
				fk.Deferrable = true
				fk.InitiallyDeferred = true
			},
			wantSubstr: []string{"MATCH FULL", "DEFERRABLE", "INITIALLY DEFERRED"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fk := base()
			tc.mutate(fk)
			got, err := emitAddForeignKey("public", "child", fk)
			if err != nil {
				t.Fatalf("emitAddForeignKey: %v", err)
			}
			for _, want := range tc.wantSubstr {
				if !strings.Contains(got, want) {
					t.Errorf("emitted DDL omits %q — the target lands a constraint of different STRENGTH than "+
						"the source, silently (audit SL-7).\n  got: %s", want, got)
				}
			}
			for _, no := range tc.absent {
				if strings.Contains(got, no) {
					t.Errorf("emitted DDL contains %q for a constraint that does not carry it: %s", no, got)
				}
			}
		})
	}
}

// TestFKMatchFromCode pins the pg_constraint.confmatchtype mapping. A wrong
// mapping here would be invisible: it produces valid DDL for the wrong
// constraint.
func TestFKMatchFromCode(t *testing.T) {
	cases := map[string]ir.FKMatch{
		"f": ir.FKMatchFull,
		"p": ir.FKMatchPartial,
		"s": ir.FKMatchSimple,
		"":  ir.FKMatchSimple, // absent → the column default
		"?": ir.FKMatchSimple, // unknown future code → the safe default
	}
	for code, want := range cases {
		if got := fkMatchFromCode(code); got != want {
			t.Errorf("fkMatchFromCode(%q) = %v, want %v", code, got, want)
		}
	}
}
