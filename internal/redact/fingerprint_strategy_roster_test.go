// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package redact

import (
	"reflect"
	"strings"
	"testing"
)

// fingerprintStatelessExempt names each Strategy whose emitted value is
// fully determined by what [Strategy.Name] already carries, with the
// reason. Fail-by-default: a strategy with state outside Name that does
// not implement [FingerprintMaterial] fails this test.
var fingerprintStatelessExempt = map[string]string{
	"Null": "no fields; the emitted value is always NULL",
	"Truncate": "Name carries N, which is the only field. `truncate:4` and `truncate:5` therefore " +
		"already fingerprint differently.",
	"Hash": "Name carries Algo. The Key IS distinguishing state and IS contributed — by " +
		"Hash.FingerprintMaterial, which returns \"\" for the keyless algorithms — so this entry " +
		"covers only the fields Name settles.",
	"RandomizeInt":     "Name carries Min and Max, its only fields",
	"RandomizePAN":     "Name carries Brand, its only field",
	"RandomizeIBAN":    "Name carries Country, its only field",
	"RandomizeEmail":   "no fields",
	"RandomizeUSPhone": "no fields",
	"RandomizeUUID":    "no fields",
	"RandomizeSSN":     "no fields",
	"RandomizeCASIN":   "no fields",
	"RandomizeUKNIN":   "no fields",
	"Static":           "Value is distinguishing and IS contributed by Static.FingerprintMaterial",
	"Mask":             "Form/M1/M2 are in Name; Char is contributed by Mask.FingerprintMaterial",
	"RandomizeDict":    "DictName is in Name; Entries are contributed by RandomizeDict.FingerprintMaterial",
	"TokenizeDict": "DictName is in Name; Entries, StreamID and Key are contributed by " +
		"TokenizeDict.FingerprintMaterial",
}

// TestFingerprintCoversEveryStrategysState is the ratchet the v0.145.0
// pre-tag value-fidelity review asked for.
//
// THE DEFECT IT PREVENTS RECURRING. The first fingerprint fix closed
// Static and keyed Hash and left TokenizeDict, RandomizeDict and
// Mask.Char — three strategies whose emitted value depends on state
// [Strategy.Name] does not carry, so two genuinely different policies
// fingerprinted identically and every door comparing them for equality
// accepted the mismatch. The commit that fixed the first two enumerated
// no siblings, which is this project's most expensive recurring shape.
//
// WHAT IT REACHES: every Strategy implementor constructed below, checked
// by REFLECTION for fields whose value could change the emitted
// surrogate. A strategy with such fields must either implement
// [FingerprintMaterial] or carry an exemption saying which part of
// [Strategy.Name] settles them.
//
// WHAT IT DOES NOT REACH, so the name is not read wider: it cannot tell
// which fields actually affect the output — that is a human judgement,
// recorded in the exemption text. It catches the case that matters,
// which is a NEW strategy (or a new field) arriving with neither.
func TestFingerprintCoversEveryStrategysState(t *testing.T) {
	// Every Strategy the parser can produce. Kept as constructed values
	// rather than a type list so a new strategy fails to compile here if
	// it does not satisfy the interface.
	all := []Strategy{
		Null{},
		Static{Value: "x"},
		Hash{Algo: "sha256"},
		Hash{Algo: "hmac-sha256", Key: []byte("k")},
		Truncate{N: 4},
		Mask{Form: MaskInner, M1: 1, M2: 1},
		Mask{Form: MaskInner, M1: 1, M2: 1, Char: "*"},
		RandomizeDict{DictName: "d", Entries: []string{"a"}},
		TokenizeDict{DictName: "d", Entries: []string{"a"}, StreamID: "s", Key: []byte("k")},
		// The fixed-format presets and the parameterised randomizers. They
		// belong here because this roster's universe must BE the Strategy
		// surface: the anti-vacuity floor caught the first cut covering
		// seven of twenty-four types, which is precisely the "grades a
		// fraction of the surface" failure the floor exists to prevent.
		MaskSSN{},
		MaskPAN{},
		MaskPANRelaxed{},
		MaskEmail{},
		MaskCASIN{},
		MaskUKNIN{},
		MaskIBAN{},
		MaskUUID{},
		RandomizeInt{Min: 1, Max: 9},
		RandomizeEmail{},
		RandomizeUSPhone{},
		RandomizeUUID{},
		RandomizeSSN{},
		RandomizePAN{},
		RandomizeCASIN{},
		RandomizeUKNIN{},
		RandomizeIBAN{},
	}

	seen := map[string]bool{}
	for _, s := range all {
		typ := reflect.TypeOf(s)
		for typ.Kind() == reflect.Pointer {
			typ = typ.Elem()
		}
		name := typ.Name()
		seen[name] = true

		_, contributes := s.(FingerprintMaterial)
		hasFields := typ.Kind() == reflect.Struct && typ.NumField() > 0
		if !hasFields || contributes {
			continue
		}
		if why, ok := fingerprintStatelessExempt[name]; ok {
			if strings.TrimSpace(why) == "" {
				t.Errorf("%s is exempt with an EMPTY reason; say which part of Name settles its fields", name)
			}
			continue
		}
		t.Errorf("strategy %s has fields but neither implements FingerprintMaterial nor carries an "+
			"exemption.\n\nIf any of those fields changes the emitted value, two different policies "+
			"fingerprint alike and every door comparing the fingerprint for equality — a resumed "+
			"`backup full`, `sync start --position-from-manifest` — silently accepts a mismatch. Add "+
			"FingerprintMaterial (a DIGEST, never the raw material), or an exemption saying why Name is "+
			"sufficient.", name)
	}

	// Anti-vacuity: the list above must still cover the real surface. The
	// floor is under the current count and far above zero.
	if len(seen) < 20 {
		t.Fatalf("only %d distinct strategy types exercised %v; this package defines 24, so the list has "+
			"drifted and this gate grades a fraction of the surface", len(seen), seen)
	}

	// The exemption map's other half: an entry naming a type this test no
	// longer constructs is stale, and a stale exemption is how a real
	// strategy later inherits a pass it was never granted.
	for name := range fingerprintStatelessExempt {
		if !seen[name] {
			t.Errorf("fingerprintStatelessExempt names %q, which this roster does not construct "+
				"(renamed? removed?)", name)
		}
	}
}

// TestFingerprintDistinguishesTheDictAndMaskGaps is the behavioural half:
// each of the three strategies the first fix missed must now separate
// policies that differ only in the state Name omits.
func TestFingerprintDistinguishesTheDictAndMaskGaps(t *testing.T) {
	fp := func(s Strategy) string {
		r := New()
		r.Set("public", "users", "name", s)
		return r.Fingerprint()
	}
	same := func(t *testing.T, label string, a, b Strategy) {
		t.Helper()
		if fp(a) == fp(b) {
			t.Errorf("%s: two policies that emit DIFFERENT surrogates fingerprint alike (%s) — every "+
				"equality door accepts the mismatch", label, fp(a))
		}
	}

	same(t, "tokenize:dict differing only in the HMAC key",
		TokenizeDict{DictName: "d", Entries: []string{"a", "b"}, Key: []byte("k1")},
		TokenizeDict{DictName: "d", Entries: []string{"a", "b"}, Key: []byte("k2")})

	same(t, "tokenize:dict differing only in the STREAM ID (the backup-vs-sync shape, no rotation involved)",
		TokenizeDict{DictName: "d", Entries: []string{"a", "b"}, Key: []byte("k"), StreamID: ""},
		TokenizeDict{DictName: "d", Entries: []string{"a", "b"}, Key: []byte("k"), StreamID: "prod"})

	same(t, "tokenize:dict differing only in dictionary ORDER (selection is by index)",
		TokenizeDict{DictName: "d", Entries: []string{"a", "b"}, Key: []byte("k")},
		TokenizeDict{DictName: "d", Entries: []string{"b", "a"}, Key: []byte("k")})

	same(t, "randomize:dict differing only in dictionary contents",
		RandomizeDict{DictName: "d", Entries: []string{"a", "b"}},
		RandomizeDict{DictName: "d", Entries: []string{"a", "c"}})

	same(t, "mask differing only in the mask character",
		Mask{Form: MaskInner, M1: 4, M2: 4, Char: "X"},
		Mask{Form: MaskInner, M1: 4, M2: 4, Char: "*"})

	// The floor: identical policies must still agree, or every legitimate
	// resume refuses — which is worse than the collision.
	stable := TokenizeDict{DictName: "d", Entries: []string{"a", "b"}, Key: []byte("k"), StreamID: "prod"}
	// Two separate calls, held in variables: staticcheck reads
	// fp(x) != fp(x) as identical expressions, which is exactly the
	// determinism this asserts.
	first, second := fp(stable), fp(stable)
	if first != second {
		t.Errorf("the same tokenize:dict policy fingerprinted differently across two calls: %s vs %s",
			first, second)
	}
	if a, b := fp(Mask{Form: MaskInner, M1: 4, M2: 4}), fp(Mask{Form: MaskInner, M1: 4, M2: 4}); a != b {
		t.Errorf("an unset mask Char is not stable: %s vs %s", a, b)
	}

	// And no secret reaches the material.
	secret := []byte("super-secret-keyset-value")
	if m := (TokenizeDict{DictName: "d", Key: secret}).FingerprintMaterial(); strings.Contains(m, string(secret)) {
		t.Errorf("the tokenize key appears VERBATIM in its fingerprint material: %q", m)
	}
}
