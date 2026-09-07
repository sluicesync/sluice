// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package redact

import (
	"strings"
	"testing"
)

// TestFingerprintDistinguishesSecretMaterial pins the fix for the
// collision the v0.144.0 pre-tag value-fidelity review measured.
//
// THE DEFECT. Fingerprint hashed [Strategy.Name] — deliberately, so no
// secret reaches the manifest. But Name is the ELIDED audit rendering:
// Static renders "static:<elided>" whatever its value, and Hash renders
// "hash:hmac-sha256" whatever its key. So two genuinely different
// policies fingerprinted identically, and every consumer that compares
// fingerprints for EQUALITY silently accepted a mismatch.
//
// WHY THAT MATTERS, per consumer, because the old doc argued the
// coarse granularity was correct and it is correct for none of them:
//
//   - refuseResumeUnderDifferentRedaction: resuming an interrupted
//     `backup full` under a rotated key produced ONE manifest whose
//     table A chunks were keyed differently from its table B chunks.
//     Each table is internally consistent and any downstream join on
//     the surrogate silently returns nothing.
//   - RefusePositionFromRedactedChain (v0.144.0): a sync resuming off
//     a redacted chain under a different key overwrites each restored
//     surrogate with a DIFFERENT surrogate for the same source value.
//
// The old doc said a rotated key "still redacts the same columns with
// the same strategy", which is true and is not the question the
// consumers ask. They ask whether these artifacts can be read TOGETHER.
//
// NO SECRET REACHES THE MANIFEST. The distinguishing material is
// SHA-256'd by the strategy before it enters the fingerprint input, so
// the manifest carries a digest of a digest. Pinned below.
func TestFingerprintDistinguishesSecretMaterial(t *testing.T) {
	fp := func(t *testing.T, s Strategy) string {
		t.Helper()
		r := New()
		r.Set("public", "users", "email", s)
		return r.Fingerprint()
	}

	t.Run("two static values no longer collide", func(t *testing.T) {
		a := fp(t, Static{Value: "REDACTED-A"})
		b := fp(t, Static{Value: "REDACTED-B"})
		if a == b {
			t.Errorf("static:\"REDACTED-A\" and static:\"REDACTED-B\" fingerprint alike (%s) — a resumed "+
				"backup full under a different replacement value would not be refused, leaving one "+
				"archive whose tables carry two different constants", a)
		}
	})

	t.Run("two HMAC keys no longer collide", func(t *testing.T) {
		a := fp(t, Hash{Algo: "hmac-sha256", Key: []byte("key-one")})
		b := fp(t, Hash{Algo: "hmac-sha256", Key: []byte("key-two")})
		if a == b {
			t.Errorf("two different HMAC keys fingerprint alike (%s) — a key rotation between an "+
				"interrupted backup full and its resume would not be refused, and the two halves of "+
				"the archive would carry surrogates that cannot be joined", a)
		}
	})

	t.Run("the same material still agrees with itself", func(t *testing.T) {
		// The floor. A fingerprint that varies run to run refuses every
		// legitimate resume, which is worse than the collision.
		if a, b := fp(t, Static{Value: "X"}), fp(t, Static{Value: "X"}); a != b {
			t.Errorf("the same static value fingerprinted differently across runs (%s vs %s) — every "+
				"resume and every --position-from-manifest would refuse", a, b)
		}
		k := []byte("stable-key")
		if a, b := fp(t, Hash{Algo: "hmac-sha256", Key: k}), fp(t, Hash{Algo: "hmac-sha256", Key: k}); a != b {
			t.Errorf("the same HMAC key fingerprinted differently across runs (%s vs %s)", a, b)
		}
	})

	t.Run("keyless strategies are unaffected", func(t *testing.T) {
		// sha256 carries no key, so it must not acquire per-run variance
		// from an empty Key slice being treated as material.
		a := fp(t, Hash{Algo: "sha256"})
		b := fp(t, Hash{Algo: "sha256", Key: []byte{}})
		if a != b {
			t.Errorf("hash:sha256 fingerprints differ by an empty vs nil Key (%s vs %s); a keyless "+
				"strategy has no material and must be stable", a, b)
		}
		if c := fp(t, Null{}); c == a {
			t.Errorf("null and hash:sha256 fingerprint alike (%s) — the strategy name is not reaching "+
				"the hash and this whole gate would be vacuous", c)
		}
	})

	t.Run("no secret reaches the fingerprint input", func(t *testing.T) {
		// The property the elision existed to protect, kept. The
		// contributor hands over a DIGEST, never the material, so a
		// grep of the fingerprint input cannot recover the value.
		secret := "hunter2-the-actual-password"
		mat := Static{Value: secret}.FingerprintMaterial()
		if mat == "" {
			t.Fatal("Static contributes no fingerprint material — the collision is back")
		}
		if strings.Contains(mat, secret) {
			t.Errorf("the static value appears VERBATIM in its fingerprint material (%q); it must be "+
				"hashed by the strategy before it enters the manifest's digest", mat)
		}
		key := []byte("super-secret-hmac-key")
		kmat := Hash{Algo: "hmac-sha256", Key: key}.FingerprintMaterial()
		if kmat == "" {
			t.Fatal("keyed Hash contributes no fingerprint material — the collision is back")
		}
		if strings.Contains(kmat, string(key)) {
			t.Errorf("the HMAC key appears VERBATIM in its fingerprint material (%q)", kmat)
		}
	})
}

// TestFingerprintCoversTheBareSchemaShape closes the pin gap the same
// review found: every existing fingerprint test used schema "public",
// and the bare `schema == ""` shape is the ONLY one a MySQL-source
// `backup full --redact users.email=...` produces (MySQL single-database
// mode leaves ir.Table.Schema empty — see Registry.Get's doc, Bug 58).
//
// So both of v0.144.0's new doors were pinned exclusively against a
// shape a MySQL source never emits. This is the "pin the class, not the
// representative" rule applied to the SCHEMA axis rather than the type
// axis.
func TestFingerprintCoversTheBareSchemaShape(t *testing.T) {
	bare := New()
	bare.Set("", "users", "email", Hash{Algo: "sha256"})

	qualified := New()
	qualified.Set("public", "users", "email", Hash{Algo: "sha256"})

	if bare.Fingerprint() == "" {
		t.Fatal("a bare-schema rule fingerprints to the empty string — Empty() or Rules() is dropping " +
			"the rule, and on a MySQL source that is EVERY rule, so the marker would record " +
			"\"unfingerprinted\" for every redacted MySQL backup")
	}
	if bare.Fingerprint() == qualified.Fingerprint() {
		t.Errorf("a bare-schema rule and a public-schema rule on the same table.column fingerprint "+
			"alike (%s) — the length-prefixed tokens are supposed to make the schema part of the "+
			"identity, and a MySQL chain would compare equal to a Postgres one", bare.Fingerprint())
	}

	// Stability on the bare shape specifically: the doors compare for
	// equality, so per-run variance here refuses every MySQL resume.
	again := New()
	again.Set("", "users", "email", Hash{Algo: "sha256"})
	if bare.Fingerprint() != again.Fingerprint() {
		t.Errorf("bare-schema fingerprint is not stable: %s vs %s", bare.Fingerprint(), again.Fingerprint())
	}
}
