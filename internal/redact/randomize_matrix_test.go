// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Redact family matrix for the randomize:* seed-lookup path (audit
// 2026-08-26 R-1/R-2). The Bug-74 discipline: PKValuesFromRow dispatches
// per PK-column NAME against per-row map keys, so the pin exercises
// EVERY randomize strategy (the family) × every lookup shape — exact
// spelling, case-diverged spelling, missing key, ambiguous case-variant
// keys, NULL in a PK-adjacent column — not one representative strategy.
// Pre-fix, a case-diverged PK silently seeded every row from nil, giving
// the whole table ONE identical "randomized" value at exit 0.

package redact

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// randomizeRoster returns one configured instance of EVERY randomize:*
// strategy. The roster mirrors the parser's form list in
// cmd/sluice/redact_flag.go (int, email, us-phone, uuid, ssn, pan,
// ca-sin, uk-nin, iban, dict); a new randomize form must be added here
// for the matrix to keep covering the family.
//
// Anti-vacuity floor: the count is pinned and every entry must actually
// be seed-requiring (needsSeed), so a rename that silently drops a
// strategy out of the seeded family fails here instead of vanishing.
func randomizeRoster(t *testing.T) []Strategy {
	t.Helper()
	roster := []Strategy{
		RandomizeInt{Min: 10, Max: 99},
		RandomizeEmail{},
		RandomizeUSPhone{},
		RandomizeUUID{},
		RandomizeSSN{},
		RandomizePAN{},
		RandomizeCASIN{},
		RandomizeUKNIN{},
		RandomizeIBAN{},
		RandomizeDict{DictName: "names", Entries: []string{"ada", "bo", "cy"}},
	}
	const wantForms = 10 // the parser's randomize form list
	if len(roster) != wantForms {
		t.Fatalf("randomize roster has %d strategies; want %d (keep in lockstep with parseRandomizeStrategy's form list)", len(roster), wantForms)
	}
	for _, s := range roster {
		if !needsSeed(s) {
			t.Fatalf("roster strategy %s is not seed-requiring — it does not belong in the seeded-lookup matrix", s.Name())
		}
	}
	return roster
}

// matrixPayload picks an input value the strategy accepts (RandomizeInt
// refuses non-integer inputs; everything else discards the input).
func matrixPayload(s Strategy) any {
	if strings.HasPrefix(s.Name(), "randomize:int") {
		return int64(4242)
	}
	return "secret"
}

// TestRandomizeSeedLookup_FamilyMatrix drives every randomize strategy
// through Registry.ApplyRow — the seed-lookup site the CDC apply path
// uses — across the PK lookup shapes.
func TestRandomizeSeedLookup_FamilyMatrix(t *testing.T) {
	for _, s := range randomizeRoster(t) {
		t.Run(s.Name(), func(t *testing.T) {
			payload := matrixPayload(s)
			apply := func(pkColumns []string, row ir.Row) error {
				reg := New()
				reg.Set("", "users", "payload", s)
				return reg.ApplyRow("", "users", pkColumns, row, "stream-1")
			}

			t.Run("exact PK spelling redacts", func(t *testing.T) {
				row := ir.Row{"id": int64(7), "payload": payload}
				if err := apply([]string{"id"}, row); err != nil {
					t.Fatalf("ApplyRow: %v", err)
				}
				if row["payload"] == payload {
					t.Fatalf("payload untouched (%v); want redacted", row["payload"])
				}
			})

			t.Run("case-diverged PK spelling derives the exact-spelling seed", func(t *testing.T) {
				// The R-1 CDC shape: pkColumns carries the target catalog's
				// spelling, the row the source's. Same PK value → same seed
				// → same redacted output as the exact-spelling row.
				exact := ir.Row{"id": int64(7), "payload": payload}
				diverged := ir.Row{"ID": int64(7), "payload": payload}
				if err := apply([]string{"id"}, exact); err != nil {
					t.Fatalf("exact-spelling ApplyRow: %v", err)
				}
				if err := apply([]string{"id"}, diverged); err != nil {
					t.Fatalf("case-diverged ApplyRow: %v (pre-R-1 this succeeded by seeding from nil; post-fix it must succeed via the fold fallback)", err)
				}
				if exact["payload"] != diverged["payload"] {
					t.Fatalf("case-diverged row redacted to %v; want the exact-spelling value %v (same PK value must derive the same seed)", diverged["payload"], exact["payload"])
				}
			})

			t.Run("missing PK key refuses loudly", func(t *testing.T) {
				row := ir.Row{"payload": payload}
				err := apply([]string{"id"}, row)
				if err == nil {
					t.Fatal("ApplyRow succeeded with the PK column absent from the row; want the R-1 loud refusal (silently seeding from nil gives every row one identical value)")
				}
				if !errors.Is(err, ErrPKColumnMissing) {
					t.Fatalf("error is not ErrPKColumnMissing: %v", err)
				}
				if !strings.Contains(err.Error(), `"id"`) || !strings.Contains(err.Error(), "payload") {
					t.Fatalf("refusal must name the missing column and the row's actual keys; got: %v", err)
				}
				if row["payload"] != payload {
					t.Fatalf("payload was modified (%v) despite the refusal", row["payload"])
				}
			})

			t.Run("ambiguous case-variant PK keys refuse", func(t *testing.T) {
				// Exact miss + two fold-matches: a replay-stable seed cannot
				// pick one arbitrarily (map iteration order is random).
				row := ir.Row{"id": int64(7), "ID": int64(7), "payload": payload}
				err := apply([]string{"Id"}, row)
				if err == nil {
					t.Fatal("ApplyRow succeeded with two case-variant PK keys and no exact match; want a refusal")
				}
				if !errors.Is(err, ErrPKColumnMissing) {
					t.Fatalf("error is not ErrPKColumnMissing: %v", err)
				}
			})

			t.Run("NULL in a PK-adjacent column is fine", func(t *testing.T) {
				row := ir.Row{"id": int64(7), "payload": payload, "note": nil}
				if err := apply([]string{"id"}, row); err != nil {
					t.Fatalf("ApplyRow: %v", err)
				}
				if row["payload"] == payload {
					t.Fatalf("payload untouched (%v); want redacted", row["payload"])
				}
				if row["note"] != nil {
					t.Fatalf("unruled NULL column was modified: %v", row["note"])
				}
			})
		})
	}
}

// TestRandomizeInt_OverflowingSpans pins the R-2 fix (audit 2026-08-26):
// ranges whose inclusive WIDTH exceeds MaxInt64 must actually vary per
// row instead of collapsing to the constant Min. Pre-fix,
// `randomize:int:0,MaxInt64` wrapped the int64 span negative and the
// span<=0 branch returned Min for EVERY row — inside BIGINT, so the
// Bug-105 preflight could not catch it.
func TestRandomizeInt_OverflowingSpans(t *testing.T) {
	col := &ir.Column{Name: "n", Type: ir.Integer{Width: 64}}

	drawMany := func(t *testing.T, r RandomizeInt) []int64 {
		t.Helper()
		outs := make([]int64, 0, 8)
		for i := range 8 {
			v, err := r.Redact(col, int64(i), seed32(fmt.Sprintf("row-%d", i)))
			if err != nil {
				t.Fatalf("Redact(seed row-%d): %v", i, err)
			}
			outs = append(outs, v.(int64))
		}
		return outs
	}

	allEqual := func(vals []int64) bool {
		for _, v := range vals {
			if v != vals[0] {
				return false
			}
		}
		return true
	}

	t.Run("0..MaxInt64 varies per row and stays in range", func(t *testing.T) {
		r := RandomizeInt{Min: 0, Max: math.MaxInt64}
		outs := drawMany(t, r)
		for _, v := range outs {
			if v < 0 {
				t.Fatalf("value %d below Min 0", v)
			}
		}
		if allEqual(outs) {
			t.Fatalf("all 8 rows redacted to the constant %d; want per-row variation (the R-2 collapse)", outs[0])
		}
	})

	t.Run("full int64 domain MinInt64..MaxInt64 varies per row", func(t *testing.T) {
		r := RandomizeInt{Min: math.MinInt64, Max: math.MaxInt64}
		if outs := drawMany(t, r); allEqual(outs) {
			t.Fatalf("all 8 rows redacted to the constant %d; want per-row variation", outs[0])
		}
	})

	t.Run("negative-anchored overflowing span stays in range", func(t *testing.T) {
		r := RandomizeInt{Min: -1, Max: math.MaxInt64} // width 2^63 + 1
		for _, v := range drawMany(t, r) {
			if v < -1 {
				t.Fatalf("value %d below Min -1", v)
			}
		}
	})

	t.Run("Min == Max still returns Min", func(t *testing.T) {
		r := RandomizeInt{Min: 5, Max: 5}
		for _, v := range drawMany(t, r) {
			if v != 5 {
				t.Fatalf("degenerate range returned %d; want 5", v)
			}
		}
	})

	t.Run("normal range stays in range and varies", func(t *testing.T) {
		r := RandomizeInt{Min: 10, Max: 20}
		outs := drawMany(t, r)
		for _, v := range outs {
			if v < 10 || v > 20 {
				t.Fatalf("value %d outside [10, 20]", v)
			}
		}
		if allEqual(outs) {
			t.Fatalf("all 8 rows redacted to the constant %d; want per-row variation", outs[0])
		}
	})
}
