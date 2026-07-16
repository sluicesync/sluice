// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"strings"
	"testing"
)

// TestParseKMSURI pins the `kms://<provider>/<key-ref>` split (audit
// TEST-F3 T-1): only the FIRST `/` after the provider separates it from
// the key reference, because every provider's native key form carries
// its own `/` and `:` (an AWS ARN or alias/ path, a GCP resource path,
// an Azure key-identifier URL) and must reach the provider VERBATIM.
func TestParseKMSURI(t *testing.T) {
	valid := []struct {
		name, spec, provider, keyRef string
	}{
		{"aws ARN (colons + slash intact)", "kms://aws/arn:aws:kms:us-east-1:123456789012:key/abc-def", "aws", "arn:aws:kms:us-east-1:123456789012:key/abc-def"},
		{"aws alias (embedded slash intact)", "kms://aws/alias/backup-signing", "aws", "alias/backup-signing"},
		{"aws bare key id", "kms://aws/1234abcd-12ab-34cd-56ef-1234567890ab", "aws", "1234abcd-12ab-34cd-56ef-1234567890ab"},
		{"gcp versioned resource", "kms://gcp/projects/p/locations/global/keyRings/r/cryptoKeys/k/cryptoKeyVersions/1", "gcp", "projects/p/locations/global/keyRings/r/cryptoKeys/k/cryptoKeyVersions/1"},
		{"azure key-identifier URL", "kms://azure/https://vault.vault.azure.net/keys/sign/0123456789abcdef", "azure", "https://vault.vault.azure.net/keys/sign/0123456789abcdef"},
	}
	for _, tc := range valid {
		t.Run(tc.name, func(t *testing.T) {
			provider, keyRef, err := parseKMSURI(tc.spec)
			if err != nil {
				t.Fatalf("parseKMSURI(%q): %v", tc.spec, err)
			}
			if provider != tc.provider || keyRef != tc.keyRef {
				t.Errorf("parseKMSURI(%q) = (%q, %q); want (%q, %q)", tc.spec, provider, keyRef, tc.provider, tc.keyRef)
			}
		})
	}

	malformed := []struct{ name, spec string }{
		{"empty rest", "kms://"},
		{"provider only, no separator", "kms://aws"},
		{"provider only, trailing slash (empty key-ref)", "kms://aws/"},
		{"empty provider", "kms:///arn:aws:kms:us-east-1:1:key/x"},
	}
	for _, tc := range malformed {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := parseKMSURI(tc.spec)
			if err == nil || !strings.Contains(err.Error(), "malformed kms:// reference") {
				t.Errorf("parseKMSURI(%q) err = %v; want the malformed-reference refusal", tc.spec, err)
			}
		})
	}
}

// TestIsKMSURI pins the scheme dispatch: only a `kms://` prefix selects
// the KMS keystore; file paths and env:VAR specs stay on the local-key
// path.
func TestIsKMSURI(t *testing.T) {
	for spec, want := range map[string]bool{
		"kms://aws/alias/x":   true,
		"kms://":              true, // scheme selected; parseKMSURI then refuses it as malformed
		"/path/to/key.pem":    false,
		"env:SLUICE_SIGN_KEY": false,
		"":                    false,
	} {
		if got := isKMSURI(spec); got != want {
			t.Errorf("isKMSURI(%q) = %v; want %v", spec, got, want)
		}
	}
}

// TestBuildKMSSigner_UnknownProvider pins the provider allowlist (no
// live KMS involved — the refusal fires before any client construction).
func TestBuildKMSSigner_UnknownProvider(t *testing.T) {
	if _, err := buildKMSSigner("vault", "some-key", ""); err == nil ||
		!strings.Contains(err.Error(), "unknown kms:// signing provider") {
		t.Errorf("buildKMSSigner(vault) err = %v; want the unknown-provider refusal", err)
	}
	if _, err := fetchKMSPublicKey("vault", "some-key", ""); err == nil ||
		!strings.Contains(err.Error(), "unknown kms:// verify provider") {
		t.Errorf("fetchKMSPublicKey(vault) err = %v; want the unknown-provider refusal", err)
	}
}

// TestResolveWriteSigner_ExclusivityBeforeKMSBuild pins the audit
// TEST-F3 T-2 ordering: the --sign + --sign-key exclusivity refusal
// fires BEFORE any --sign-key resolution, so an invalid flag combo
// never reaches a cloud KMS client build / network attempt. The probe
// uses a kms:// spec with a provider buildKMSSigner would itself
// refuse — seeing the exclusivity error (not the unknown-provider one)
// proves the signer build was never entered.
func TestResolveWriteSigner_ExclusivityBeforeKMSBuild(t *testing.T) {
	t.Run("--sign + --sign-key kms:// refuses before the build", func(t *testing.T) {
		e := &EncryptionFlags{Sign: true, SignKey: "kms://bogus-provider/some-key"}
		_, err := e.resolveWriteSigner()
		if err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
			t.Fatalf("err = %v; want the --sign/--sign-key exclusivity refusal", err)
		}
		if strings.Contains(err.Error(), "unknown kms://") {
			t.Errorf("err = %v reached buildKMSSigner; the exclusivity check must run first", err)
		}
	})

	t.Run("--sign alone still selects the HMAC path (nil signer, nil error)", func(t *testing.T) {
		e := &EncryptionFlags{Sign: true}
		signer, err := e.resolveWriteSigner()
		if err != nil || signer != nil {
			t.Fatalf("signer, err = %v, %v; want nil, nil (HMAC-off-KEK is resolved later, off the envelope)", signer, err)
		}
	})

	t.Run("--sign-key alone still reaches the build", func(t *testing.T) {
		e := &EncryptionFlags{SignKey: "kms://bogus-provider/some-key"}
		_, err := e.resolveWriteSigner()
		if err == nil || !strings.Contains(err.Error(), "unknown kms:// signing provider") {
			t.Fatalf("err = %v; want the unknown-provider refusal from buildKMSSigner", err)
		}
	})
}

// TestBuildMaintenanceSigner_Exclusivity is the audit 2026-07-16 M3.2
// pin: the maintenance door (compact/prune re-sign) refuses the
// --sign + --sign-key combo exactly like resolveWriteSigner, and does
// so BEFORE any --sign-key resolution — same probe technique as the
// write-side pin (a kms:// spec whose provider would itself refuse:
// seeing the exclusivity error proves the build was never entered).
// The store is nil on purpose: the refusal must fire before any store
// read too.
func TestBuildMaintenanceSigner_Exclusivity(t *testing.T) {
	e := &EncryptionFlags{Sign: true, SignKey: "kms://bogus-provider/some-key"}
	_, err := e.buildMaintenanceSigner(context.Background(), nil)
	if err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("err = %v; want the --sign/--sign-key exclusivity refusal", err)
	}
	if strings.Contains(err.Error(), "unknown kms://") {
		t.Errorf("err = %v reached buildKMSSigner; the exclusivity check must run first", err)
	}

	// --sign-key alone keeps working through the maintenance door.
	solo := &EncryptionFlags{SignKey: "kms://bogus-provider/some-key"}
	_, err = solo.buildMaintenanceSigner(context.Background(), nil)
	if err == nil || !strings.Contains(err.Error(), "unknown kms:// signing provider") {
		t.Fatalf("err = %v; want the unknown-provider refusal from buildKMSSigner (proving --sign-key alone still resolves)", err)
	}
}
