// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package redact

import (
	"strings"
	"testing"
)

// TestRegistryFingerprint pins the properties the backup manifest's
// redaction marker depends on. The consumer compares two fingerprints
// for equality across process runs and machines to decide whether a
// chain may be extended or a resume may continue, so "same rules → same
// value" and "different rules → different value" are both load-bearing.
func TestRegistryFingerprint(t *testing.T) {
	build := func(fn func(r *Registry)) *Registry {
		r := New()
		fn(r)
		return r
	}

	t.Run("empty registries fingerprint to nothing", func(t *testing.T) {
		var nilReg *Registry
		if got := nilReg.Fingerprint(); got != "" {
			t.Errorf("nil registry fingerprint = %q; want empty", got)
		}
		if got := New().Fingerprint(); got != "" {
			t.Errorf("empty registry fingerprint = %q; want empty", got)
		}
	})

	t.Run("same rules fingerprint alike regardless of insertion order", func(t *testing.T) {
		a := build(func(r *Registry) {
			r.Set("public", "users", "email", Hash{Algo: "sha256"})
			r.Set("public", "users", "ssn", MaskSSN{})
		})
		b := build(func(r *Registry) {
			r.Set("public", "users", "ssn", MaskSSN{})
			r.Set("public", "users", "email", Hash{Algo: "sha256"})
		})
		if a.Fingerprint() != b.Fingerprint() {
			t.Errorf("insertion order changed the fingerprint: %q vs %q", a.Fingerprint(), b.Fingerprint())
		}
		if len(a.Fingerprint()) != 16 {
			t.Errorf("fingerprint = %q; want 16 hex chars", a.Fingerprint())
		}
	})

	t.Run("every axis of a rule moves it", func(t *testing.T) {
		base := build(func(r *Registry) { r.Set("public", "users", "email", Hash{Algo: "sha256"}) })
		for name, other := range map[string]*Registry{
			"different column":   build(func(r *Registry) { r.Set("public", "users", "name", Hash{Algo: "sha256"}) }),
			"different table":    build(func(r *Registry) { r.Set("public", "people", "email", Hash{Algo: "sha256"}) }),
			"different schema":   build(func(r *Registry) { r.Set("audit", "users", "email", Hash{Algo: "sha256"}) }),
			"different strategy": build(func(r *Registry) { r.Set("public", "users", "email", Null{}) }),
			"an extra rule": build(func(r *Registry) {
				r.Set("public", "users", "email", Hash{Algo: "sha256"})
				r.Set("public", "users", "ssn", MaskSSN{})
			}),
		} {
			if base.Fingerprint() == other.Fingerprint() {
				t.Errorf("%s fingerprints identically to the base policy — the marker cannot tell two policies apart", name)
			}
		}
	})

	// The reason this is safe to write into an artifact operators ship
	// off-site: its INPUT is the audit-log rendering, which elides the
	// static value and carries no key material, and the output is a hash
	// so no column name survives into the manifest either.
	t.Run("carries no secret", func(t *testing.T) {
		r := build(func(reg *Registry) {
			reg.Set("public", "users", "email", Hash{Algo: "hmac-sha256", Key: []byte("super-secret-key")})
			reg.Set("public", "users", "note", Static{Value: "PATIENT-NAME-REDACTED"})
		})
		fp := r.Fingerprint()
		for _, secret := range []string{"super-secret-key", "PATIENT-NAME-REDACTED", "email", "users"} {
			if strings.Contains(fp, secret) {
				t.Errorf("fingerprint %q leaks %q", fp, secret)
			}
		}
		// And the inputs it hashes are the elided names, not the values.
		for _, rule := range r.Rules() {
			if strings.Contains(rule.Strategy.Name(), "super-secret-key") ||
				strings.Contains(rule.Strategy.Name(), "PATIENT-NAME-REDACTED") {
				t.Errorf("Strategy.Name() itself carries a secret: %q — the fingerprint's safety rests on it not doing so",
					rule.Strategy.Name())
			}
		}
	})
}
