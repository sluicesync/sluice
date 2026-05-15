// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package redact

import (
	"strings"
	"testing"

	"github.com/orware/sluice/internal/ir"
)

// tokTestKey is the shared HMAC secret for the tokenize:dict tests.
// PII Phase 4 (ADR-0041): tokenize:dict requires a keyset-sourced
// key — the hardcoded v0.61.0 constant was removed.
var tokTestKey = []byte("tokenize-dict-test-secret")

// TestRandomizeDict_Determinism pins the v0.61.0 PII Phase 3
// contract: same seed → same dictionary entry.
func TestRandomizeDict_Determinism(t *testing.T) {
	s := RandomizeDict{DictName: "first_names", Entries: []string{"Alice", "Bob", "Carol", "Dave", "Eve"}}
	seed := DeriveRowSeed("stream-1", "users", "first_name", []string{"id"}, []any{42})
	first, err := s.Redact(&ir.Column{Name: "first_name"}, "anything", seed)
	if err != nil {
		t.Fatalf("first call: %v", err)
	}
	second, err := s.Redact(&ir.Column{Name: "first_name"}, "anything-else", seed)
	if err != nil {
		t.Fatalf("second call: %v", err)
	}
	if first != second {
		t.Errorf("same seed: got %q then %q; want stable", first, second)
	}
	// Output should be one of the dict entries.
	got := first.(string)
	found := false
	for _, e := range s.Entries {
		if got == e {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("output %q not in dict %v", got, s.Entries)
	}
}

// TestRandomizeDict_PerSeedSeparation pins that different seeds
// produce different outputs (with high probability — over 5 entries
// the collision rate is 1/5 per pair, so trying 4 distinct seeds is
// enough to expect at least 2 distinct outputs).
func TestRandomizeDict_PerSeedSeparation(t *testing.T) {
	s := RandomizeDict{DictName: "names", Entries: []string{"Alice", "Bob", "Carol", "Dave", "Eve"}}
	seen := make(map[string]bool)
	for i := 0; i < 20; i++ {
		seed := DeriveRowSeed("stream-1", "users", "name", []string{"id"}, []any{i})
		got, err := s.Redact(&ir.Column{Name: "name"}, nil, seed)
		if err != nil {
			t.Fatalf("seed %d: %v", i, err)
		}
		seen[got.(string)] = true
	}
	// Across 20 seeds and a 5-entry dict, we should see most entries.
	// Allow some slack: require at least 3 distinct outputs (collision
	// rate would have to be pathologically high to miss this).
	if len(seen) < 3 {
		t.Errorf("only %d distinct outputs across 20 seeds (entries seen: %v); seed → index reduction may be broken", len(seen), seen)
	}
}

// TestRandomizeDict_NoSeedRefusal pins the no-PK refusal: nil seed
// returns an operator-actionable error naming the strategy.
func TestRandomizeDict_NoSeedRefusal(t *testing.T) {
	s := RandomizeDict{DictName: "names", Entries: []string{"Alice"}}
	_, err := s.Redact(&ir.Column{Name: "name"}, "val", nil)
	if err == nil {
		t.Fatal("nil seed: expected error; got nil")
	}
	if !strings.Contains(err.Error(), "primary key") {
		t.Errorf("nil seed error %q should mention 'primary key'", err.Error())
	}
}

// TestRandomizeDict_EmptyEntriesRefusal is a defense-in-depth: the
// loader refuses empty dicts at config-load time, but Redact must
// still refuse rather than panic on mod-by-zero.
func TestRandomizeDict_EmptyEntriesRefusal(t *testing.T) {
	s := RandomizeDict{DictName: "broken", Entries: nil}
	seed := DeriveRowSeed("s", "t", "c", []string{"id"}, []any{1})
	_, err := s.Redact(&ir.Column{Name: "c"}, "val", seed)
	if err == nil {
		t.Fatal("empty entries: expected error; got nil")
	}
	if !strings.Contains(err.Error(), "0 dictionary entries") {
		t.Errorf("empty entries error %q should mention '0 dictionary entries'", err.Error())
	}
}

// TestRandomizeDict_Name pins the Name() format used by the audit
// log line + preflight prefix-matching ("randomize:" prefix → no-PK
// refusal applies).
func TestRandomizeDict_Name(t *testing.T) {
	s := RandomizeDict{DictName: "first_names"}
	if got := s.Name(); got != "randomize:dict:first_names" {
		t.Errorf("Name() = %q; want randomize:dict:first_names", got)
	}
	if !strings.HasPrefix(s.Name(), "randomize:") {
		t.Errorf("Name() must start with 'randomize:' so the no-PK preflight matches it")
	}
}

// TestTokenizeDict_DeterminismPerInput pins the input-value-keyed
// contract: same input → same output across calls, irrespective of
// PK / seed.
func TestTokenizeDict_DeterminismPerInput(t *testing.T) {
	s := TokenizeDict{DictName: "first_names", Entries: []string{"Alice", "Bob", "Carol", "Dave", "Eve"}, StreamID: "s1", Key: tokTestKey}
	// Same input value through two different seeds (PK changed) →
	// same output. Seed is irrelevant for tokenize:dict.
	first, err := s.Redact(&ir.Column{Name: "n"}, "Alice", []byte("seed-a"))
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	second, err := s.Redact(&ir.Column{Name: "n"}, "Alice", []byte("totally-different-seed"))
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if first != second {
		t.Errorf("same input different seed: got %v then %v; want stable", first, second)
	}
	// Different input value → likely different output (with 5 entries
	// there's a 1/5 collision chance per pair). Try a few inputs.
	seen := map[string]bool{}
	for _, name := range []string{"Alice", "Bob", "Carol", "Dave", "Eve", "Frank", "Grace", "Heidi"} {
		got, err := s.Redact(&ir.Column{Name: "n"}, name, nil)
		if err != nil {
			t.Fatalf("input %q: %v", name, err)
		}
		seen[got.(string)] = true
	}
	if len(seen) < 3 {
		t.Errorf("only %d distinct outputs across 8 inputs (entries seen: %v)", len(seen), seen)
	}
}

// TestTokenizeDict_NilPassThrough pins that NULL input passes through
// as NULL (no tokenization of NULL).
func TestTokenizeDict_NilPassThrough(t *testing.T) {
	s := TokenizeDict{DictName: "n", Entries: []string{"x", "y"}, Key: tokTestKey}
	got, err := s.Redact(&ir.Column{Name: "c"}, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != nil {
		t.Errorf("got %v; want nil pass-through", got)
	}
}

// TestTokenizeDict_StreamIDAffectsOutput pins that the streamID is
// mixed into the HMAC: two strategies with same dict + different
// streamIDs produce (likely) different mappings for the same input.
// Test multiple inputs to make collision-only-by-chance unlikely.
func TestTokenizeDict_StreamIDAffectsOutput(t *testing.T) {
	entries := []string{"A", "B", "C", "D", "E", "F", "G", "H", "I", "J"}
	sA := TokenizeDict{DictName: "n", Entries: entries, StreamID: "stream-A", Key: tokTestKey}
	sB := TokenizeDict{DictName: "n", Entries: entries, StreamID: "stream-B", Key: tokTestKey}
	// At least one input must map to a different output across the
	// two streams; otherwise the streamID prefix isn't doing anything.
	differs := false
	inputs := []string{"alice", "bob", "carol", "dave", "eve", "frank", "grace", "heidi", "ivan", "judy"}
	for _, in := range inputs {
		a, _ := sA.Redact(&ir.Column{Name: "n"}, in, nil)
		b, _ := sB.Redact(&ir.Column{Name: "n"}, in, nil)
		if a != b {
			differs = true
			break
		}
	}
	if !differs {
		t.Errorf("streamID had no effect on output across %d inputs (HMAC may not be including streamID)", len(inputs))
	}
}

// TestTokenizeDict_EmptyStreamIDStillStable pins the migrate-path
// shape: when StreamID is "", the HMAC still computes deterministically.
func TestTokenizeDict_EmptyStreamIDStillStable(t *testing.T) {
	s := TokenizeDict{DictName: "n", Entries: []string{"x", "y", "z"}, StreamID: "", Key: tokTestKey}
	first, err := s.Redact(&ir.Column{Name: "c"}, "Alice", nil)
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	second, err := s.Redact(&ir.Column{Name: "c"}, "Alice", nil)
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if first != second {
		t.Errorf("empty streamID: got %v then %v; want stable", first, second)
	}
}

// TestTokenizeDict_DictNameAffectsOutput pins that two dicts with
// IDENTICAL entries but different names still produce (likely)
// different tokenizations for the same input — the dict name is
// part of the HMAC message.
func TestTokenizeDict_DictNameAffectsOutput(t *testing.T) {
	entries := []string{"A", "B", "C", "D", "E", "F", "G", "H", "I", "J"}
	sA := TokenizeDict{DictName: "dict-a", Entries: entries, StreamID: "s", Key: tokTestKey}
	sB := TokenizeDict{DictName: "dict-b", Entries: entries, StreamID: "s", Key: tokTestKey}
	differs := false
	for _, in := range []string{"alice", "bob", "carol", "dave", "eve", "frank", "grace", "heidi"} {
		a, _ := sA.Redact(&ir.Column{Name: "n"}, in, nil)
		b, _ := sB.Redact(&ir.Column{Name: "n"}, in, nil)
		if a != b {
			differs = true
			break
		}
	}
	if !differs {
		t.Errorf("dict name had no effect on output (HMAC may not be including dict name)")
	}
}

// TestTokenizeDict_NonStringInput pins that non-string inputs are
// stringified via fmt.Sprintf("%v", ...). Operators with integer /
// boolean / []byte columns shouldn't see a refusal.
func TestTokenizeDict_NonStringInput(t *testing.T) {
	s := TokenizeDict{DictName: "n", Entries: []string{"x", "y"}, StreamID: "s", Key: tokTestKey}
	// integer input
	intResult, err := s.Redact(&ir.Column{Name: "c"}, 42, nil)
	if err != nil {
		t.Fatalf("int: %v", err)
	}
	// re-tokenizing the same int produces the same output
	intResult2, err := s.Redact(&ir.Column{Name: "c"}, 42, nil)
	if err != nil {
		t.Fatalf("int repeat: %v", err)
	}
	if intResult != intResult2 {
		t.Errorf("int input not stable")
	}
	// []byte that stringifies the same way as a string should map to
	// the same output (both use the canonicalized form).
	bytesResult, err := s.Redact(&ir.Column{Name: "c"}, []byte("hello"), nil)
	if err != nil {
		t.Fatalf("bytes: %v", err)
	}
	stringResult, err := s.Redact(&ir.Column{Name: "c"}, "hello", nil)
	if err != nil {
		t.Fatalf("string: %v", err)
	}
	if bytesResult != stringResult {
		t.Errorf("[]byte and string of same content should hash identically: got %v vs %v", bytesResult, stringResult)
	}
}

// TestTokenizeDict_EmptyEntriesRefusal mirrors RandomizeDict's
// defense-in-depth: refuse rather than mod-by-zero.
func TestTokenizeDict_EmptyEntriesRefusal(t *testing.T) {
	s := TokenizeDict{DictName: "broken", Entries: nil, StreamID: "s"}
	_, err := s.Redact(&ir.Column{Name: "c"}, "val", nil)
	if err == nil {
		t.Fatal("empty entries: expected error; got nil")
	}
	if !strings.Contains(err.Error(), "0 dictionary entries") {
		t.Errorf("empty entries error %q should mention '0 dictionary entries'", err.Error())
	}
}

// TestTokenizeDict_NameNotRandomizePrefix pins that the Name() does
// NOT start with "randomize:" — this is what keeps the no-PK
// preflight from refusing tokenize:dict on PK-less tables (which is
// the whole point of tokenize:dict).
func TestTokenizeDict_NameNotRandomizePrefix(t *testing.T) {
	s := TokenizeDict{DictName: "n"}
	if got := s.Name(); got != "tokenize:dict:n" {
		t.Errorf("Name() = %q; want tokenize:dict:n", got)
	}
	if strings.HasPrefix(s.Name(), "randomize:") {
		t.Fatal("tokenize:dict Name() must NOT start with 'randomize:' or the no-PK preflight would refuse it")
	}
}

// TestSeedToIndex_Basic pins the seed → index reduction: same seed →
// same index; index always in [0, n).
func TestSeedToIndex_Basic(t *testing.T) {
	seed := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	for n := 1; n <= 50; n++ {
		idx := seedToIndex(seed, n)
		if idx < 0 || idx >= n {
			t.Errorf("n=%d: idx=%d out of [0,%d)", n, idx, n)
		}
	}
	// Different seeds typically produce different indices (over a
	// large n).
	a := seedToIndex([]byte{1, 2, 3, 4, 5, 6, 7, 8}, 1000)
	b := seedToIndex([]byte{255, 254, 253, 252, 251, 250, 249, 248}, 1000)
	if a == b {
		t.Errorf("two very different seeds produced same idx %d; reduction may be broken", a)
	}
	// Defensive: empty / short seed doesn't panic.
	if got := seedToIndex(nil, 5); got < 0 || got >= 5 {
		t.Errorf("nil seed: idx=%d out of [0,5)", got)
	}
}
