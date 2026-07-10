// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0152 (audit N-8/N-9) pins: the GCM chunk-AAD primitives, the
// BoundEnvelope CEK bindings on every implementing envelope family
// (passphrase / AWS KMS / GCP KMS — the Bug-74 discipline: each family
// rides a DIFFERENT wire mechanism, so one green family covers none of
// the others), and the Azure key-version pin/rebind (N-9).

package crypto

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
)

// TestDecryptChunk_AuthFailureWrapsSentinel pins that an AES-GCM auth-tag
// failure — the whole tamper/splice/wrong-key class — wraps the exported
// [ErrChunkAuthFailed] sentinel on BOTH the AAD and the legacy nil-AAD
// paths, so the restore/verify layer can [errors.Is] it and attach the
// coded refusal (SEC-1). A clean decrypt and a shape error (too-short
// ciphertext, wrong CEK length) do NOT wrap the sentinel — only the auth
// tag does.
func TestDecryptChunk_AuthFailureWrapsSentinel(t *testing.T) {
	cek, err := GenerateCEK()
	if err != nil {
		t.Fatalf("GenerateCEK: %v", err)
	}
	pt := []byte("chunk payload bytes")
	aad := []byte("sluice-chunk-aad/v1\nfile=chunks/a-0")
	bound, err := EncryptChunkWithAAD(pt, cek, aad)
	if err != nil {
		t.Fatalf("EncryptChunkWithAAD: %v", err)
	}
	unbound, err := EncryptChunk(pt, cek)
	if err != nil {
		t.Fatalf("EncryptChunk: %v", err)
	}

	// AAD mismatch (the SEC-1 chunk-swap shape) → sentinel.
	if _, err := DecryptChunkWithAAD(bound, cek, []byte("sluice-chunk-aad/v1\nfile=chunks/b-0")); !errors.Is(err, ErrChunkAuthFailed) {
		t.Errorf("AAD-mismatch error should wrap ErrChunkAuthFailed; got %v", err)
	}
	// Tampered ciphertext on the legacy nil-AAD path → sentinel.
	corrupt := append([]byte(nil), unbound...)
	corrupt[len(corrupt)-1] ^= 0xFF
	if _, err := DecryptChunk(corrupt, cek); !errors.Is(err, ErrChunkAuthFailed) {
		t.Errorf("tampered nil-AAD ciphertext should wrap ErrChunkAuthFailed; got %v", err)
	}
	// Clean decrypt → no sentinel.
	if _, err := DecryptChunkWithAAD(bound, cek, aad); errors.Is(err, ErrChunkAuthFailed) {
		t.Errorf("a clean decrypt must not wrap ErrChunkAuthFailed: %v", err)
	}
	// A shape error (too-short ciphertext) is NOT an auth failure.
	if _, err := DecryptChunkWithAAD([]byte("short"), cek, aad); err == nil || errors.Is(err, ErrChunkAuthFailed) {
		t.Errorf("a too-short ciphertext should error WITHOUT the auth sentinel; got %v", err)
	}
}

// TestUnwrapCEK_WrongKeyDisjointFromChunkAuth pins the F-1 robustness
// invariant: a CEK-unwrap failure (wrong passphrase / tampered wrap) wraps
// [ErrCEKUnwrapFailed] and NOT [ErrChunkAuthFailed], even though both run the
// same AES-GCM primitive. Keeping the two sentinels disjoint is what lets the
// restore chunk-auth coder fire only on a genuine chunk-body failure and never
// relabel a wrong passphrase as chunk tamper — a guarantee independent of
// caller routing discipline.
func TestUnwrapCEK_WrongKeyDisjointFromChunkAuth(t *testing.T) {
	p, err := DefaultArgon2idParams()
	if err != nil {
		t.Fatalf("DefaultArgon2idParams: %v", err)
	}
	p.Memory, p.Iterations, p.Parallelism = 1024, 1, 1
	right, err := NewPassphraseEnvelope("right-pass", p) // same salt (shared p), different KEK
	if err != nil {
		t.Fatalf("NewPassphraseEnvelope right: %v", err)
	}
	wrong, err := NewPassphraseEnvelope("wrong-pass", p)
	if err != nil {
		t.Fatalf("NewPassphraseEnvelope wrong: %v", err)
	}
	cek, err := GenerateCEK()
	if err != nil {
		t.Fatalf("GenerateCEK: %v", err)
	}
	const binding = "sluice-cek-binding/v1\ncreated_at=X"
	wrapped, err := right.WrapCEKBound(cek, binding)
	if err != nil {
		t.Fatalf("WrapCEKBound: %v", err)
	}

	// Both the bound and the legacy-unbound unwrap paths (UnwrapCEK delegates
	// to UnwrapCEKBound) must land in the CEK-unwrap class, not chunk-auth.
	for _, tc := range []struct {
		name string
		call func() ([]byte, error)
	}{
		{"bound", func() ([]byte, error) { return wrong.UnwrapCEKBound(wrapped, binding) }},
		{"delegated", func() ([]byte, error) { return wrong.UnwrapCEK(mustWrapUnbound(t, right, cek)) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.call()
			if err == nil {
				t.Fatal("wrong-key unwrap should fail")
			}
			if !errors.Is(err, ErrCEKUnwrapFailed) {
				t.Errorf("wrong-key unwrap should wrap ErrCEKUnwrapFailed; got %v", err)
			}
			if errors.Is(err, ErrChunkAuthFailed) {
				t.Errorf("wrong-key unwrap must NOT carry ErrChunkAuthFailed (restore would mislabel it as chunk tamper); got %v", err)
			}
		})
	}
}

func mustWrapUnbound(t *testing.T, e *PassphraseEnvelope, cek []byte) []byte {
	t.Helper()
	w, err := e.WrapCEKBound(cek, "") // "" → legacy unbound wrap (UnwrapCEK path)
	if err != nil {
		t.Fatalf("WrapCEKBound unbound: %v", err)
	}
	return w
}

// TestEncryptDecryptChunkWithAAD_Matrix pins the AAD acceptance
// matrix on the bulk-chunk primitive: only the identical
// (cek, aad) pair opens a chunk; every mismatch shape — wrong AAD,
// stripped AAD, retrofitted AAD — refuses.
func TestEncryptDecryptChunkWithAAD_Matrix(t *testing.T) {
	cek, err := GenerateCEK()
	if err != nil {
		t.Fatalf("GenerateCEK: %v", err)
	}
	plaintext := []byte("chunk payload bytes")
	aadA := []byte("sluice-chunk-aad/v1\ncreated_at=X\nfile=chunks/a-0")
	aadB := []byte("sluice-chunk-aad/v1\ncreated_at=X\nfile=chunks/b-0")

	bound, err := EncryptChunkWithAAD(plaintext, cek, aadA)
	if err != nil {
		t.Fatalf("EncryptChunkWithAAD: %v", err)
	}
	unbound, err := EncryptChunk(plaintext, cek)
	if err != nil {
		t.Fatalf("EncryptChunk: %v", err)
	}

	t.Run("same aad opens", func(t *testing.T) {
		got, err := DecryptChunkWithAAD(bound, cek, aadA)
		if err != nil {
			t.Fatalf("DecryptChunkWithAAD: %v", err)
		}
		if !bytes.Equal(got, plaintext) {
			t.Errorf("plaintext mismatch")
		}
	})
	t.Run("different aad refuses with the splice message", func(t *testing.T) {
		_, err := DecryptChunkWithAAD(bound, cek, aadB)
		if err == nil {
			t.Fatal("bound chunk opened under a DIFFERENT position binding; the splice class is back")
		}
		if !strings.Contains(err.Error(), "does not belong at this position") {
			t.Errorf("error %q should name the spliced-chunk hypothesis", err.Error())
		}
	})
	t.Run("stripped aad refuses", func(t *testing.T) {
		if _, err := DecryptChunk(bound, cek); err == nil {
			t.Fatal("bound chunk opened via the legacy nil-AAD path; the version-downgrade class is back")
		}
	})
	t.Run("retrofitted aad refuses", func(t *testing.T) {
		if _, err := DecryptChunkWithAAD(unbound, cek, aadA); err == nil {
			t.Fatal("UNBOUND (legacy) chunk opened under an AAD; bound and unbound ciphertext classes must not mix")
		}
	})
	t.Run("legacy nil-nil round-trip unchanged", func(t *testing.T) {
		got, err := DecryptChunk(unbound, cek)
		if err != nil {
			t.Fatalf("DecryptChunk: %v", err)
		}
		if !bytes.Equal(got, plaintext) {
			t.Errorf("plaintext mismatch on the legacy path")
		}
	})
}

// boundEnvelopeMatrix pins one [BoundEnvelope] implementation against
// the full accept/refuse matrix. Shared across the three implementing
// families so no family gets a weaker pin than its siblings.
func boundEnvelopeMatrix(t *testing.T, env BoundEnvelope) {
	t.Helper()
	cek, err := GenerateCEK()
	if err != nil {
		t.Fatalf("GenerateCEK: %v", err)
	}
	const bindingA = "sluice-cek-binding/v1\ncreated_at=A"
	const bindingB = "sluice-cek-binding/v1\ncreated_at=B"

	wrapped, err := env.WrapCEKBound(cek, bindingA)
	if err != nil {
		t.Fatalf("WrapCEKBound: %v", err)
	}
	got, err := env.UnwrapCEKBound(wrapped, bindingA)
	if err != nil {
		t.Fatalf("UnwrapCEKBound(same binding): %v", err)
	}
	if !bytes.Equal(got, cek) {
		t.Errorf("bound round-trip returned a different CEK")
	}
	if _, err := env.UnwrapCEKBound(wrapped, bindingB); err == nil {
		t.Error("bound wrap opened under a DIFFERENT binding; cross-backup CEK reuse is back (audit N-8)")
	}
	if _, err := env.UnwrapCEK(wrapped); err == nil {
		t.Error("bound wrap opened via the legacy unbound unwrap; the downgrade class is back")
	}
	legacy, err := env.WrapCEK(cek)
	if err != nil {
		t.Fatalf("WrapCEK: %v", err)
	}
	if _, err := env.UnwrapCEKBound(legacy, bindingA); err == nil {
		t.Error("LEGACY (unbound) wrap opened under a binding; bound and unbound wraps must not mix")
	}
	if got, err := env.UnwrapCEK(legacy); err != nil || !bytes.Equal(got, cek) {
		t.Errorf("legacy unbound round-trip broke: got=%v err=%v", got, err)
	}
}

func TestPassphraseEnvelope_BoundWrapMatrix(t *testing.T) {
	params, err := DefaultArgon2idParams()
	if err != nil {
		t.Fatalf("DefaultArgon2idParams: %v", err)
	}
	env, err := NewPassphraseEnvelope("binding-pass", params)
	if err != nil {
		t.Fatalf("NewPassphraseEnvelope: %v", err)
	}
	boundEnvelopeMatrix(t, env)
}

func TestKMSEnvelope_BoundWrapMatrix(t *testing.T) {
	const arn = "arn:aws:kms:us-east-1:123456789012:key/bind-test"
	fake := newFakeKMS(arn)
	env, err := NewKMSEnvelope(context.Background(), arn, WithKMSClient(fake), withSkipPreflight())
	if err != nil {
		t.Fatalf("NewKMSEnvelope: %v", err)
	}
	boundEnvelopeMatrix(t, env)
}

func TestGCPKMSEnvelope_BoundWrapMatrix(t *testing.T) {
	const resource = "projects/test/locations/us/keyRings/r/cryptoKeys/bind"
	fake := newFakeGCPKMS(resource)
	env, err := NewGCPKMSEnvelope(context.Background(), resource, WithGCPKMSClient(fake))
	if err != nil {
		t.Fatalf("NewGCPKMSEnvelope: %v", err)
	}
	boundEnvelopeMatrix(t, env)
}

// TestAzureKMSEnvelope_DoesNotImplementBoundEnvelope pins the
// DOCUMENTED limitation (ADR-0152): Azure Key Vault's WrapKey API has
// no context/AAD parameter, so the envelope must NOT satisfy
// [BoundEnvelope] — a fake implementation that silently discarded the
// binding would claim integrity the service cannot provide.
func TestAzureKMSEnvelope_DoesNotImplementBoundEnvelope(t *testing.T) {
	var env any = &AzureKMSEnvelope{}
	if _, ok := env.(BoundEnvelope); ok {
		t.Fatal("AzureKMSEnvelope implements BoundEnvelope; Azure KeyWrap has no context parameter — do not fake the binding (ADR-0152 documented limitation)")
	}
}

// TestAzureKMSEnvelope_VersionPinAndResolvedKEKRef pins the N-9
// mechanics: an unversioned operator key URL is resolved to the
// vault's current version at the GetKey preflight, every wrap/unwrap
// targets that pinned version explicitly, and ResolvedKEKRef reports
// the versioned URL for the manifest's KEKRef.
func TestAzureKMSEnvelope_VersionPinAndResolvedKEKRef(t *testing.T) {
	fake := newFakeAzureKMS("chain-key")
	fake.latestVersion = "v7"
	env, err := NewAzureKMSEnvelope(context.Background(),
		"https://stub.vault.azure.net/keys/chain-key",
		WithAzureKMSClient(fake))
	if err != nil {
		t.Fatalf("NewAzureKMSEnvelope: %v", err)
	}
	wantRef := "https://stub.vault.azure.net/keys/chain-key/v7"
	if got := env.ResolvedKEKRef(); got != wantRef {
		t.Fatalf("ResolvedKEKRef = %q; want the preflight-pinned %q", got, wantRef)
	}
	cek, _ := GenerateCEK()
	wrapped, err := env.WrapCEK(cek)
	if err != nil {
		t.Fatalf("WrapCEK: %v", err)
	}
	if fake.lastWrapVersion != "v7" {
		t.Errorf("wrap targeted version %q; want the pinned v7 (an unpinned wrap floats with rotation mid-run)", fake.lastWrapVersion)
	}
	// The key rotates. The pinned envelope must keep unwrapping the
	// old wrap against v7, not drift to the new latest.
	fake.latestVersion = "v8"
	got, err := env.UnwrapCEK(wrapped)
	if err != nil {
		t.Fatalf("UnwrapCEK after rotation: %v (the N-9 latest-version drift is back)", err)
	}
	if !bytes.Equal(got, cek) {
		t.Errorf("unwrap returned a different CEK")
	}
	if fake.lastUnwrapVersion != "v7" {
		t.Errorf("unwrap targeted version %q; want the pinned v7", fake.lastUnwrapVersion)
	}
}

// TestAzureKMSEnvelope_RebindChainKEKRef pins the restore-side N-9
// retarget: a fresh envelope (pinned to the CURRENT latest) rebinds to
// the manifest-recorded wrap-time version and unwraps a pre-rotation
// chain; the advisory cases (operator-explicit version, foreign
// vault/key, unversioned recorded ref) keep the current target.
func TestAzureKMSEnvelope_RebindChainKEKRef(t *testing.T) {
	newEnv := func(t *testing.T, fake *fakeAzureKMS, keyURL string) *AzureKMSEnvelope {
		t.Helper()
		env, err := NewAzureKMSEnvelope(context.Background(), keyURL, WithAzureKMSClient(fake))
		if err != nil {
			t.Fatalf("NewAzureKMSEnvelope: %v", err)
		}
		return env
	}

	t.Run("recorded versioned ref retargets a rotated key", func(t *testing.T) {
		// Chain wrapped at v3; the key has since rotated to v9. The
		// operator supplies the unversioned URL (the common case).
		fake := newFakeAzureKMS("rot-key")
		fake.latestVersion = "v3"
		writer := newEnv(t, fake, "https://stub.vault.azure.net/keys/rot-key")
		cek, _ := GenerateCEK()
		wrapped, err := writer.WrapCEK(cek)
		if err != nil {
			t.Fatalf("WrapCEK: %v", err)
		}
		recordedRef := writer.ResolvedKEKRef() // .../rot-key/v3

		fake.latestVersion = "v9" // auto-rotation happens
		reader := newEnv(t, fake, "https://stub.vault.azure.net/keys/rot-key")
		// Without the rebind, the reader is pinned at v9 and the unwrap
		// fails — the exact N-9 failure mode.
		if _, err := reader.UnwrapCEK(wrapped); err == nil {
			t.Fatal("unwrap against the post-rotation latest version succeeded; the fake is not reproducing the N-9 mechanism")
		}
		reader.RebindChainKEKRef(recordedRef)
		got, err := reader.UnwrapCEK(wrapped)
		if err != nil {
			t.Fatalf("UnwrapCEK after rebind: %v (restores of pre-rotation chains are broken — audit N-9)", err)
		}
		if !bytes.Equal(got, cek) {
			t.Errorf("unwrap returned a different CEK")
		}
		if fake.lastUnwrapVersion != "v3" {
			t.Errorf("unwrap targeted %q; want the recorded wrap-time v3", fake.lastUnwrapVersion)
		}
	})

	t.Run("operator-explicit version wins over the recorded ref", func(t *testing.T) {
		fake := newFakeAzureKMS("exp-key")
		env := newEnv(t, fake, "https://stub.vault.azure.net/keys/exp-key/v5")
		env.RebindChainKEKRef("https://stub.vault.azure.net/keys/exp-key/v2")
		if got := env.currentKeyVersion(); got != "v5" {
			t.Errorf("explicit operator version overridden to %q; the operator's deliberate retarget must win", got)
		}
	})

	t.Run("foreign vault/key recorded ref is advisory (kept target)", func(t *testing.T) {
		fake := newFakeAzureKMS("mine")
		fake.latestVersion = "v4"
		env := newEnv(t, fake, "https://stub.vault.azure.net/keys/mine")
		env.RebindChainKEKRef("https://other.vault.azure.net/keys/theirs/v1")
		if got := env.currentKeyVersion(); got != "v4" {
			t.Errorf("foreign recorded ref changed the target to %q; want the preflight-pinned v4 kept", got)
		}
	})

	t.Run("unversioned recorded ref (pre-v5 manifest) is a no-op", func(t *testing.T) {
		fake := newFakeAzureKMS("old")
		fake.latestVersion = "v2"
		env := newEnv(t, fake, "https://stub.vault.azure.net/keys/old")
		env.RebindChainKEKRef("https://stub.vault.azure.net/keys/old")
		if got := env.currentKeyVersion(); got != "v2" {
			t.Errorf("unversioned recorded ref changed the target to %q; want v2 kept (latest-version legacy behavior)", got)
		}
	})
}
