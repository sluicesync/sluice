// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package redact

import (
	"bytes"
	"regexp"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// PII Phase 2.c first wave — replay-stable randomize strategies.
//
// The Strategy interface contract for randomize:* is:
//
//   - same seed → same output (replay stability)
//   - different seeds → different outputs (per-row uniqueness)
//   - nil seed → operator-actionable refusal
//
// Each generator's tests cover that triad plus shape-correctness
// for the form (int range, email regex, phone regex, UUIDv4 bit
// pattern).

// seed32 builds a 32-byte seed from a string for deterministic
// tests; pad/truncate to 32 bytes. Real production seeds come from
// [DeriveRowSeed] (SHA-256 of the row's identity), so any 32-byte
// input shape is realistic.
func seed32(s string) []byte {
	out := make([]byte, 32)
	copy(out, s)
	return out
}

// TestRandomizeInt covers RandomizeInt's three contract points
// (deterministic, in-range, refusal-on-nil-seed) plus the
// type-strictness check on non-integer inputs.
func TestRandomizeInt(t *testing.T) {
	col := &ir.Column{Name: "age", Type: ir.Integer{Width: 64}}

	t.Run("Name reports bounds", func(t *testing.T) {
		r := RandomizeInt{Min: 18, Max: 90}
		if got := r.Name(); got != "randomize:int:18,90" {
			t.Errorf("Name = %q; want %q", got, "randomize:int:18,90")
		}
	})

	t.Run("deterministic: same seed → same output", func(t *testing.T) {
		r := RandomizeInt{Min: 0, Max: 100}
		seed := seed32("seed-A")
		a, err := r.Redact(col, int64(42), seed)
		if err != nil {
			t.Fatalf("first call: %v", err)
		}
		b, err := r.Redact(col, int64(42), seed)
		if err != nil {
			t.Fatalf("second call: %v", err)
		}
		if a != b {
			t.Errorf("same seed produced different outputs: %v vs %v", a, b)
		}
	})

	t.Run("different seeds → different outputs (high probability)", func(t *testing.T) {
		r := RandomizeInt{Min: 0, Max: 1_000_000_000}
		a, _ := r.Redact(col, int64(0), seed32("seed-A"))
		b, _ := r.Redact(col, int64(0), seed32("seed-B"))
		if a == b {
			t.Errorf("different seeds produced same output: %v", a)
		}
	})

	t.Run("output is in [Min, Max] inclusive", func(t *testing.T) {
		r := RandomizeInt{Min: 18, Max: 90}
		// Test 100 different seeds; every output must be in bounds.
		for i := 0; i < 100; i++ {
			s := seed32("seed-" + string(rune('a'+i%26)) + string(rune('0'+i/26)))
			out, err := r.Redact(col, int64(0), s)
			if err != nil {
				t.Fatalf("seed %d: %v", i, err)
			}
			v, ok := out.(int64)
			if !ok {
				t.Fatalf("seed %d: output is %T, want int64", i, out)
			}
			if v < 18 || v > 90 {
				t.Errorf("seed %d: output %d outside [18, 90]", i, v)
			}
		}
	})

	t.Run("Min == Max returns Min", func(t *testing.T) {
		r := RandomizeInt{Min: 42, Max: 42}
		out, err := r.Redact(col, int64(0), seed32("seed"))
		if err != nil {
			t.Fatalf("%v", err)
		}
		if out != int64(42) {
			t.Errorf("Min==Max: got %v; want 42", out)
		}
	})

	t.Run("nil seed refuses with operator-actionable error", func(t *testing.T) {
		r := RandomizeInt{Min: 0, Max: 100}
		_, err := r.Redact(col, int64(0), nil)
		if err == nil {
			t.Fatal("expected error for nil seed; got nil")
		}
		for _, want := range []string{"randomize:int:0,100", "primary key", "age"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("err %q should contain %q", err.Error(), want)
			}
		}
	})

	t.Run("non-integer input refuses", func(t *testing.T) {
		r := RandomizeInt{Min: 0, Max: 100}
		_, err := r.Redact(col, "hello", seed32("seed"))
		if err == nil {
			t.Fatal("expected error for string input on int generator")
		}
		if !strings.Contains(err.Error(), "string") {
			t.Errorf("err %q should mention the input type", err.Error())
		}
	})

	t.Run("nil input is fine (type-check skipped)", func(t *testing.T) {
		r := RandomizeInt{Min: 0, Max: 100}
		_, err := r.Redact(col, nil, seed32("seed"))
		if err != nil {
			t.Errorf("nil input should be accepted; got %v", err)
		}
	})

	t.Run("Min > Max refuses defensively", func(t *testing.T) {
		r := RandomizeInt{Min: 90, Max: 18}
		_, err := r.Redact(col, int64(0), seed32("seed"))
		if err == nil {
			t.Fatal("expected error for Min > Max")
		}
	})
}

// TestRandomizeEmail covers shape correctness + determinism for
// RandomizeEmail. Output must match the documented regex.
func TestRandomizeEmail(t *testing.T) {
	col := &ir.Column{Name: "email", Type: ir.Varchar{Length: 255}}
	r := RandomizeEmail{}
	emailRe := regexp.MustCompile(`^[a-z]{6,12}@[a-z]{5,10}\.test$`)

	t.Run("Name is 'randomize:email'", func(t *testing.T) {
		if got := r.Name(); got != "randomize:email" {
			t.Errorf("Name = %q; want 'randomize:email'", got)
		}
	})

	t.Run("output matches local@domain.test shape", func(t *testing.T) {
		for i := 0; i < 50; i++ {
			s := seed32("seed-" + string(rune('a'+i)))
			out, err := r.Redact(col, "ignored@example.com", s)
			if err != nil {
				t.Fatalf("seed %d: %v", i, err)
			}
			str, ok := out.(string)
			if !ok {
				t.Fatalf("seed %d: output type %T, want string", i, out)
			}
			if !emailRe.MatchString(str) {
				t.Errorf("seed %d: %q does not match %s", i, str, emailRe)
			}
		}
	})

	t.Run("deterministic: same seed → same email", func(t *testing.T) {
		s := seed32("seed-deterministic")
		a, _ := r.Redact(col, nil, s)
		b, _ := r.Redact(col, nil, s)
		if a != b {
			t.Errorf("same seed produced different outputs: %v vs %v", a, b)
		}
	})

	t.Run("different seeds → different emails", func(t *testing.T) {
		a, _ := r.Redact(col, nil, seed32("A"))
		b, _ := r.Redact(col, nil, seed32("B"))
		if a == b {
			t.Errorf("different seeds produced same output: %v", a)
		}
	})

	t.Run("nil seed refuses", func(t *testing.T) {
		_, err := r.Redact(col, nil, nil)
		if err == nil {
			t.Fatal("expected error for nil seed")
		}
		if !strings.Contains(err.Error(), "randomize:email") {
			t.Errorf("err %q should mention the strategy", err.Error())
		}
	})
}

// TestRandomizeUSPhone covers shape + range correctness +
// determinism + 555-avoidance.
func TestRandomizeUSPhone(t *testing.T) {
	col := &ir.Column{Name: "phone", Type: ir.Varchar{Length: 14}}
	r := RandomizeUSPhone{}
	phoneRe := regexp.MustCompile(`^(\d{3})-(\d{3})-(\d{4})$`)

	t.Run("Name is 'randomize:us-phone'", func(t *testing.T) {
		if got := r.Name(); got != "randomize:us-phone" {
			t.Errorf("Name = %q; want 'randomize:us-phone'", got)
		}
	})

	t.Run("output matches XXX-XXX-XXXX shape", func(t *testing.T) {
		for i := 0; i < 200; i++ {
			s := seed32("seed-" + string(rune('A'+i%26)) + string(rune('A'+i/26)))
			out, err := r.Redact(col, nil, s)
			if err != nil {
				t.Fatalf("seed %d: %v", i, err)
			}
			str := out.(string)
			m := phoneRe.FindStringSubmatch(str)
			if m == nil {
				t.Fatalf("seed %d: %q does not match XXX-XXX-XXXX", i, str)
			}
			// Area code: 200-999, never 555.
			area := m[1]
			if area < "200" || area > "999" {
				t.Errorf("seed %d: area code %s out of [200, 999]", i, area)
			}
			if area == "555" {
				t.Errorf("seed %d: area code is 555 (reserved)", i)
			}
			// Exchange: 200-999.
			ex := m[2]
			if ex < "200" || ex > "999" {
				t.Errorf("seed %d: exchange %s out of [200, 999]", i, ex)
			}
		}
	})

	t.Run("deterministic", func(t *testing.T) {
		s := seed32("deterministic-seed")
		a, _ := r.Redact(col, nil, s)
		b, _ := r.Redact(col, nil, s)
		if a != b {
			t.Errorf("same seed produced different outputs: %v vs %v", a, b)
		}
	})

	t.Run("nil seed refuses", func(t *testing.T) {
		_, err := r.Redact(col, nil, nil)
		if err == nil {
			t.Fatal("expected error for nil seed")
		}
	})
}

// TestRandomizeUUID covers shape correctness + RFC 4122 v4 bit
// pattern + determinism.
func TestRandomizeUUID(t *testing.T) {
	col := &ir.Column{Name: "id", Type: ir.UUID{}}
	r := RandomizeUUID{}
	uuidRe := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

	t.Run("Name is 'randomize:uuid'", func(t *testing.T) {
		if got := r.Name(); got != "randomize:uuid" {
			t.Errorf("Name = %q; want 'randomize:uuid'", got)
		}
	})

	t.Run("output matches 8-4-4-4-12 hex with v4 + RFC-4122 variant bits", func(t *testing.T) {
		for i := 0; i < 100; i++ {
			s := seed32("seed-" + string(rune('a'+i%26)) + string(rune('a'+i/26)))
			out, err := r.Redact(col, nil, s)
			if err != nil {
				t.Fatalf("seed %d: %v", i, err)
			}
			str := out.(string)
			if !uuidRe.MatchString(str) {
				t.Fatalf("seed %d: %q does not match canonical UUID shape", i, str)
			}
			// Version 4: the 14th character (index 14) must be '4'.
			if str[14] != '4' {
				t.Errorf("seed %d: version digit at index 14 = %q; want '4'", i, str[14])
			}
			// Variant RFC-4122: the 19th character (index 19) must be
			// one of {8, 9, a, b}.
			variant := str[19]
			if variant != '8' && variant != '9' && variant != 'a' && variant != 'b' {
				t.Errorf("seed %d: variant digit at index 19 = %q; want one of 8/9/a/b", i, variant)
			}
		}
	})

	t.Run("deterministic", func(t *testing.T) {
		s := seed32("deterministic-seed")
		a, _ := r.Redact(col, nil, s)
		b, _ := r.Redact(col, nil, s)
		if a != b {
			t.Errorf("same seed produced different outputs: %v vs %v", a, b)
		}
	})

	t.Run("different seeds → different uuids", func(t *testing.T) {
		a, _ := r.Redact(col, nil, seed32("A"))
		b, _ := r.Redact(col, nil, seed32("B"))
		if a == b {
			t.Errorf("different seeds produced same UUID: %v", a)
		}
	})

	t.Run("nil seed refuses", func(t *testing.T) {
		_, err := r.Redact(col, nil, nil)
		if err == nil {
			t.Fatal("expected error for nil seed")
		}
	})
}

// TestDeriveRowSeed pins the seed-derivation contract: same inputs →
// same seed; different inputs → different seeds; length always 32.
func TestDeriveRowSeed(t *testing.T) {
	t.Run("same inputs → same seed", func(t *testing.T) {
		a := DeriveRowSeed("stream-1", "users", "email", []string{"id"}, []any{int64(42)})
		b := DeriveRowSeed("stream-1", "users", "email", []string{"id"}, []any{int64(42)})
		if !bytes.Equal(a, b) {
			t.Errorf("identical inputs produced different seeds")
		}
		if len(a) != 32 {
			t.Errorf("seed length = %d; want 32 (SHA-256)", len(a))
		}
	})

	t.Run("different streamID → different seed", func(t *testing.T) {
		a := DeriveRowSeed("stream-1", "users", "email", []string{"id"}, []any{int64(42)})
		b := DeriveRowSeed("stream-2", "users", "email", []string{"id"}, []any{int64(42)})
		if bytes.Equal(a, b) {
			t.Errorf("different streamID should produce different seed")
		}
	})

	t.Run("different table → different seed", func(t *testing.T) {
		a := DeriveRowSeed("s", "users", "email", []string{"id"}, []any{int64(1)})
		b := DeriveRowSeed("s", "accounts", "email", []string{"id"}, []any{int64(1)})
		if bytes.Equal(a, b) {
			t.Errorf("different table should produce different seed")
		}
	})

	t.Run("different column → different seed", func(t *testing.T) {
		a := DeriveRowSeed("s", "users", "email", []string{"id"}, []any{int64(1)})
		b := DeriveRowSeed("s", "users", "phone", []string{"id"}, []any{int64(1)})
		if bytes.Equal(a, b) {
			t.Errorf("different column should produce different seed")
		}
	})

	t.Run("different PK values → different seed", func(t *testing.T) {
		a := DeriveRowSeed("s", "users", "email", []string{"id"}, []any{int64(1)})
		b := DeriveRowSeed("s", "users", "email", []string{"id"}, []any{int64(2)})
		if bytes.Equal(a, b) {
			t.Errorf("different PK values should produce different seed")
		}
	})

	t.Run("composite PK", func(t *testing.T) {
		// Composite (tenant_id, id) is fully respected — different
		// tenant_id with same id produces a different seed.
		a := DeriveRowSeed("s", "users", "email",
			[]string{"tenant_id", "id"}, []any{int64(10), int64(1)})
		b := DeriveRowSeed("s", "users", "email",
			[]string{"tenant_id", "id"}, []any{int64(20), int64(1)})
		if bytes.Equal(a, b) {
			t.Errorf("composite PK: different tenant should yield different seed")
		}
	})

	t.Run("empty streamID is allowed (migrate path)", func(t *testing.T) {
		a := DeriveRowSeed("", "users", "email", []string{"id"}, []any{int64(1)})
		if len(a) != 32 {
			t.Errorf("empty streamID should still produce 32-byte seed")
		}
		// Stability with empty streamID:
		b := DeriveRowSeed("", "users", "email", []string{"id"}, []any{int64(1)})
		if !bytes.Equal(a, b) {
			t.Errorf("empty streamID seed should still be deterministic")
		}
	})
}

// TestApplyRow_RandomizeNoPK covers the refusal contract: when a
// randomize:* rule is registered AND pkColumns is empty, ApplyRow
// returns a clear operator-actionable error.
func TestApplyRow_RandomizeNoPK(t *testing.T) {
	r := New()
	r.Set("public", "users", "age", RandomizeInt{Min: 18, Max: 90})

	row := ir.Row{"id": int64(1), "age": int64(35)}
	err := r.ApplyRow("public", "users", nil, row, "stream-1")
	if err == nil {
		t.Fatal("expected refusal for randomize on no-PK table")
	}
	if !strings.Contains(err.Error(), "primary key") {
		t.Errorf("err %q should mention primary key", err.Error())
	}
	if !strings.Contains(err.Error(), "randomize:int:18,90") {
		t.Errorf("err %q should name the strategy", err.Error())
	}
}

// TestApplyRow_RandomizeWithPK covers the happy path: PK supplied,
// random value lands in the row, deterministic across runs.
func TestApplyRow_RandomizeWithPK(t *testing.T) {
	r := New()
	r.Set("public", "users", "age", RandomizeInt{Min: 18, Max: 90})

	row1 := ir.Row{"id": int64(1), "age": int64(35)}
	if err := r.ApplyRow("public", "users", []string{"id"}, row1, "stream-1"); err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	a := row1["age"].(int64)
	if a < 18 || a > 90 {
		t.Errorf("randomized age %d not in [18,90]", a)
	}

	// Second call with same PK → same value (replay stability).
	row2 := ir.Row{"id": int64(1), "age": int64(99)}
	if err := r.ApplyRow("public", "users", []string{"id"}, row2, "stream-1"); err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if row2["age"] != a {
		t.Errorf("replay stability broken: %v != %v", row2["age"], a)
	}

	// Different PK → different value (high probability with wide range).
	r2 := New()
	r2.Set("public", "users", "age", RandomizeInt{Min: 0, Max: 1_000_000_000})
	row3 := ir.Row{"id": int64(1), "age": int64(0)}
	row4 := ir.Row{"id": int64(2), "age": int64(0)}
	_ = r2.ApplyRow("public", "users", []string{"id"}, row3, "stream-1")
	_ = r2.ApplyRow("public", "users", []string{"id"}, row4, "stream-1")
	if row3["age"] == row4["age"] {
		t.Errorf("different PK values produced same random output (high-probability failure)")
	}
}

// TestApplyRow_StreamIDChangesSeed covers the cross-stream
// separation contract: same row redacted by different streams may
// produce different randomize:* values (the seed includes streamID).
func TestApplyRow_StreamIDChangesSeed(t *testing.T) {
	r := New()
	r.Set("public", "users", "age", RandomizeInt{Min: 0, Max: 1_000_000_000})

	rowA := ir.Row{"id": int64(1), "age": int64(0)}
	rowB := ir.Row{"id": int64(1), "age": int64(0)}
	_ = r.ApplyRow("public", "users", []string{"id"}, rowA, "stream-A")
	_ = r.ApplyRow("public", "users", []string{"id"}, rowB, "stream-B")
	if rowA["age"] == rowB["age"] {
		t.Errorf("different streamIDs should produce different randomized values")
	}
}
