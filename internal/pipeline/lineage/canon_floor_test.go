// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package lineage

import (
	"context"
	"strings"
	"testing"

	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// TestWarnPreSchemaCanonSignature_FiresOnlyBelowV5 pins the trigger in
// BOTH directions, which is the whole point: a warning that also fires
// on a current-release chain would be tuned out before the pre-v5 chain
// that needs it reached anyone, and one that stays silent on v4 is the
// defect (Bug 220) restated.
//
// The version axis is every canon the dual-version verifier still
// accepts, so a retired version added to the verifier without a
// coverage answer shows up here rather than in a forged restore.
func TestWarnPreSchemaCanonSignature_FiresOnlyBelowV5(t *testing.T) {
	cases := []struct {
		name         string
		canonVersion string
		wantWarn     bool
	}{
		{
			name:         "v4 — the Bug 220 shape: everything v0.107.0 and earlier signed",
			canonVersion: irbackup.ManifestCanonVersionV4,
			wantWarn:     true,
		},
		{
			name:         "v3 — Phase-2/3 chains, same missing schema coverage",
			canonVersion: irbackup.ManifestCanonVersionV3,
			wantWarn:     true,
		},
		{
			name:         "v2 — Phase-1 chains",
			canonVersion: irbackup.ManifestCanonVersionV2,
			wantWarn:     true,
		},
		{
			name:         "v5 — the current canon COVERS the schema; silence is required",
			canonVersion: irbackup.ManifestCanonVersion,
			wantWarn:     false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			recs := captureWarnings(t, func(ctx context.Context) {
				WarnPreSchemaCanonSignature(ctx, "manifests/incr-0001.json", c.canonVersion)
			})
			if !c.wantWarn {
				if len(recs) != 0 {
					t.Fatalf("warned on %s, which DOES cover the schema: %v", c.canonVersion, recs)
				}
				return
			}
			if len(recs) != 1 {
				t.Fatalf("got %d warnings for %s; want exactly 1: %v", len(recs), c.canonVersion, recs)
			}
			if got, _ := recs[0]["canon_version"].(string); got != c.canonVersion {
				t.Errorf("canon_version attr = %q; want %q", got, c.canonVersion)
			}
			if got, _ := recs[0]["artifact"].(string); got != "manifests/incr-0001.json" {
				t.Errorf("artifact attr = %q; want the manifest path", got)
			}
		})
	}
}

// TestWarnPreSchemaCanonSignature_NamesTheResidualAndTheRemedy pins the
// content. "Older canonicalization" alone reads as housekeeping; what an
// operator has to be able to act on is that the SCHEMA is unprotected on
// a chain their tooling just called valid, what that permits, and that
// the only way out is a fresh backup — a chain cannot be re-signed into
// coverage.
func TestWarnPreSchemaCanonSignature_NamesTheResidualAndTheRemedy(t *testing.T) {
	recs := captureWarnings(t, func(ctx context.Context) {
		WarnPreSchemaCanonSignature(ctx, "manifest.json", irbackup.ManifestCanonVersionV4)
	})
	if len(recs) != 1 {
		t.Fatalf("got %d warnings; want 1", len(recs))
	}
	msg, _ := recs[0]["msg"].(string)
	for _, want := range []string{
		irbackup.ManifestCanonVersionV4, // the version actually recorded
		"schema",                        // what is not covered
		"RLS",                           // what that permits, concretely
		"fresh full backup",             // the remedy
		irbackup.ManifestCanonVersion,   // the version that closes it
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("warning message does not mention %q:\n%s", want, msg)
		}
	}
}

// TestVerifyManifest_WarnsOnPreV5SignatureAndNotOnV5 is the WIRING
// gate, and it is the half that actually matters: the helper above can
// be perfect and the operator still learns nothing if nothing calls it.
//
// The warning is emitted from [VerifyManifest] — the one chokepoint all
// three verification entry points (single-manifest restore, chain
// restore, `backup verify`) funnel through — so pinning it here pins it
// for all of them, and for any entry point added later.
//
// Both directions, on REAL signatures built the way the shipping
// binaries built them: a v4 signature (every chain written by v0.107.0
// and earlier) verifies GREEN and warns; a v5 signature verifies GREEN
// and is silent.
func TestVerifyManifest_WarnsOnPreV5SignatureAndNotOnV5(t *testing.T) {
	ctx := context.Background()
	s := testSigner(t)

	t.Run("v4 signature warns", func(t *testing.T) {
		store := newMemStore()
		m := schemaBearingManifest()
		if err := WriteManifestAt(ctx, store, ManifestFileName, m); err != nil {
			t.Fatal(err)
		}
		writeV4ManifestSig(t, ctx, store, ManifestFileName, m, 0, s.Key)
		read, err := ReadManifestAt(ctx, store, ManifestFileName)
		if err != nil {
			t.Fatal(err)
		}
		var verifyErr error
		recs := captureWarnings(t, func(wctx context.Context) {
			verifyErr = VerifyManifest(wctx, store, ManifestFileName, read, 0, s)
		})
		if verifyErr != nil {
			t.Fatalf("a genuine v4 signature must still VERIFY (the design does not change): %v", verifyErr)
		}
		if len(recs) != 1 {
			t.Fatalf("got %d warnings verifying a v4 signature; want exactly 1 naming the schema residual: %v", len(recs), recs)
		}
		if msg, _ := recs[0]["msg"].(string); !strings.Contains(msg, irbackup.ManifestCanonVersionV4) {
			t.Errorf("warning does not name the recorded canon version:\n%s", msg)
		}
	})

	t.Run("v5 signature is silent", func(t *testing.T) {
		store := newMemStore()
		m := schemaBearingManifest()
		if err := WriteManifestAt(ctx, store, ManifestFileName, m); err != nil {
			t.Fatal(err)
		}
		if err := WriteManifestSig(ctx, store, ManifestFileName, m, 0, s); err != nil {
			t.Fatal(err)
		}
		read, err := ReadManifestAt(ctx, store, ManifestFileName)
		if err != nil {
			t.Fatal(err)
		}
		var verifyErr error
		recs := captureWarnings(t, func(wctx context.Context) {
			verifyErr = VerifyManifest(wctx, store, ManifestFileName, read, 0, s)
		})
		if verifyErr != nil {
			t.Fatalf("v5 signature failed to verify: %v", verifyErr)
		}
		if len(recs) != 0 {
			t.Fatalf("warned over a current-canon signature — the warning would be tuned out: %v", recs)
		}
	})
}

// TestManifestCanonCoversSchema_DerivesFromTheFeatureTable pins the
// predicate the warning keys on, including the shape that matters most:
// an UNKNOWN (newer) canon version must not be reported as covering the
// schema. This build cannot recompute those bytes at all, so claiming
// coverage would be a guess in the direction of reassurance.
func TestManifestCanonCoversSchema_DerivesFromTheFeatureTable(t *testing.T) {
	for _, c := range []struct {
		version string
		want    bool
	}{
		{irbackup.ManifestCanonVersionV2, false},
		{irbackup.ManifestCanonVersionV3, false},
		{irbackup.ManifestCanonVersionV4, false},
		{irbackup.ManifestCanonVersion, true},
		{"sluice-manifest-canon/v99", false},
		{"", false},
	} {
		if got := irbackup.ManifestCanonCoversSchema(c.version); got != c.want {
			t.Errorf("ManifestCanonCoversSchema(%q) = %v; want %v", c.version, got, c.want)
		}
	}
}
