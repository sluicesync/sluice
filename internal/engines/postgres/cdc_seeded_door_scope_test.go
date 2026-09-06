// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"

	"sluicesync.dev/sluice/internal/ir"
)

// TestGradeRelationSchemaRace_SeededDoorIgnoresScopePredicate pins the fix
// for the HIGH the UPR-2 pre-tag value-fidelity review found (2026-09-05).
//
// UPR-2 put `relationInScope` in front of BOTH doors in
// [CDCReader.gradeRelationSchemaRace]. The tier-2 loud refusal
// (checkSchemaRace) genuinely wanted that gate — a DDL on a relation the
// stream emits nothing for must not end the stream, which is what UPR-2 was
// for. The SLM-1c seeded door did not, and gating it was actively wrong:
//
//   - It is ONE-SHOT. It fires only while the OID has no process-local
//     prior, and the caller caches the entry immediately after, so a single
//     skipped call disarms it permanently for the process.
//   - The predicate the pipeline supplies is EMPTY exactly when it fires.
//     `wireCDCScopePredicate` closes over `s.liveFilterRef`, which is nil
//     until `phaseStartApplySidecars` — and that runs after StreamChanges
//     opens the reader. On a warm resume of a stream carrying a live-added
//     table (`schema add-table`), the first RelationMessage for that table
//     is graded while the predicate still answers false for it.
//   - The seed KNOWS that table: on warm resume the seed is built from the
//     target witness, and a live-added table exists on the target.
//
// So the composed effect was: a stopped-stream `timestamptz → timestamp`
// swap on a live-added table forwarded silently, at exit 0, with every
// liveness signal green — the exact class SLM-1c shipped to close.
//
// The gate is not needed for the safe direction either, which is why the
// fix is an ordering change rather than a new condition: checkSeededSchemaRace
// already returns nil for a relation the seed does not know, and the seed is
// the scope. Both halves are asserted below so neither can regress into the
// other.
func TestGradeRelationSchemaRace_SeededDoorIgnoresScopePredicate(t *testing.T) {
	naivePrior := func(table string) *ir.Table {
		return &ir.Table{
			Name:    table,
			Columns: []*ir.Column{{Name: "paid_at", Type: ir.DateTime{PrecisionUnspecified: true}}},
		}
	}
	zonedWire := func(table string) *relationCacheEntry {
		return seedRel(t, "public", table, typedCol(t, "paid_at", pgtype.TimestamptzOID, -1))
	}

	t.Run("a seeded swap refuses even while the scope predicate says no", func(t *testing.T) {
		// The live-add shape: the operator filter does not carry `payments`,
		// and the live-added set has not been loaded yet, so the pipeline's
		// closure answers false for it. This is a warm resume, so the seed
		// (target witness) does carry it.
		r := &CDCReader{
			schema:       "public",
			scopeAllowed: func(_, _ string) bool { return false },
		}
		r.SetSchemaSeed([]*ir.Table{naivePrior("payments")})

		err := r.gradeRelationSchemaRace(map[uint32]*relationCacheEntry{}, 16400, zonedWire("payments"))
		if err == nil {
			t.Fatal("the seeded zone-swap door was skipped because the scope predicate answered false — " +
				"this door is ONE-SHOT, so skipping it once disarms it permanently and the swap forwards at exit 0. " +
				"It must not be gated on relationInScope (the seed is already the scope).")
		}
		for _, want := range []string{"while the stream was stopped", "public.payments", `column "paid_at"`, "timestamp and timestamptz", "TimeZone", "drained model"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("refusal missing %q; got: %v", want, err)
			}
		}
	})

	t.Run("a relation the seed does not know still primes, predicate or not", func(t *testing.T) {
		// The safe direction, asserted so the fix cannot be read as
		// "ungating re-opens UPR-2". An unrelated relation is absent from
		// the seed, so the seeded door is silent on its own account.
		for _, tc := range []struct {
			name    string
			allowed func(string, string) bool
		}{
			{"predicate excludes it", func(_, _ string) bool { return false }},
			{"predicate allows it", func(_, _ string) bool { return true }},
		} {
			t.Run(tc.name, func(t *testing.T) {
				r := &CDCReader{schema: "public", scopeAllowed: tc.allowed}
				r.SetSchemaSeed([]*ir.Table{naivePrior("payments")})
				if err := r.gradeRelationSchemaRace(map[uint32]*relationCacheEntry{}, 16401, zonedWire("unrelated")); err != nil {
					t.Fatalf("a relation absent from the seed refused: %v", err)
				}
			})
		}
	})

	t.Run("the tier-2 door stays scope-gated (UPR-2 is not reverted)", func(t *testing.T) {
		// A cached prior hands the comparison to checkSchemaRace, which is
		// the door UPR-2 scoped. An out-of-scope relation must still pass,
		// or the false-halt UPR-2 fixed comes back.
		prior := seedRel(t, "public", "ignored", typedCol(t, "n", pgtype.Int4OID, -1))
		curr := seedRel(t, "public", "ignored", typedCol(t, "n", pgtype.TextOID, -1))
		relations := map[uint32]*relationCacheEntry{16402: prior}

		out := &CDCReader{schema: "public", scopeAllowed: func(_, _ string) bool { return false }}
		if err := out.gradeRelationSchemaRace(relations, 16402, curr); err != nil {
			t.Fatalf("an out-of-scope ALTER ended the stream — UPR-2 regressed: %v", err)
		}
		// Anti-vacuity: the same boundary IN scope must refuse, or the
		// subtest above would pass against a door that never fires.
		in := &CDCReader{schema: "public", scopeAllowed: func(_, _ string) bool { return true }}
		if err := in.gradeRelationSchemaRace(relations, 16402, curr); err == nil {
			t.Fatal("the in-scope ALTER passed too — checkSchemaRace is not firing at all, so the out-of-scope assertion above proves nothing")
		}
	})
}
