// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"
)

// The goldens below were captured from the tree at v0.143.0 (commit
// 729c9c43), BEFORE [Manifest.Redaction] existed, by rendering
// fixedSignedManifest through the same three helpers. They are the whole
// backward-compatibility argument for the field made mechanical: a
// manifest that carries no redaction marker must serialise, identify and
// canonicalise to the same bytes it did before the field was added, or
// every chain already in the field becomes unreadable / unverifiable.
//
// A golden captured from POST-change values would prove only that this
// build agrees with itself, which is the audit-item-104 defect. These
// were taken from the older tree and pasted in.
const (
	goldenPreRedactionManifestDocSHA256 = "ac5ff59dcc312a06657a7e28be53f8c8ac69a1f9bccda2b6424c71d8c7d305c6"
	goldenPreRedactionCanonSHA256       = "0606b0485599e921cbff3c1808ba2f60cca2959c4ae02a67b0446167e5de589a"

	// fixedSignedManifest at FormatVersion 1..7 (pre-CDC-fold layout) and
	// at 8..9 (with the item-57 fold). Neither may move.
	goldenPreRedactionBackupIDLegacy = "5e9aef65e9d649c5"
	goldenPreRedactionBackupIDV8     = "42cf97c838eec575"
)

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// TestManifestWithoutRedactionIsByteIdenticalToPreFieldReleases is the
// format-compatibility gate for the redaction marker. `omitempty` on a
// pointer field is the CLAIM that an unredacted manifest is unchanged;
// this is the check, against values a build that had never heard of the
// field produced.
func TestManifestWithoutRedactionIsByteIdenticalToPreFieldReleases(t *testing.T) {
	m := fixedSignedManifest()
	if m.Redaction != nil {
		t.Fatal("fixture unexpectedly carries a redaction marker")
	}

	doc, err := MarshalManifest(m)
	if err != nil {
		t.Fatal(err)
	}
	if got := sha256Hex(doc); got != goldenPreRedactionManifestDocSHA256 {
		t.Errorf("manifest document bytes moved: sha256 %s, want %s (pre-field golden).\n%s",
			got, goldenPreRedactionManifestDocSHA256, doc)
	}
	// Stated separately from the hash so a failure says WHY: the `omitempty`
	// is what keeps the member out of the document entirely.
	if bytes.Contains(doc, []byte(`"redaction"`)) {
		t.Errorf("an unredacted manifest rendered a `redaction` member; omitempty is not doing what the field's doc claims:\n%s", doc)
	}

	cb, err := CanonicalManifestBytes(m, 2, SignatureSchemeHMACKEK)
	if err != nil {
		t.Fatal(err)
	}
	if got := sha256Hex(cb); got != goldenPreRedactionCanonSHA256 {
		t.Errorf("canonical signed bytes moved: sha256 %s, want %s (pre-field golden) — every signature already in the field would fail to verify",
			got, goldenPreRedactionCanonSHA256)
	}
}

// TestComputeBackupIDUnchangedBelowTheRedactionVersion pins the OTHER
// half of newer-reads-older: the id fold is gated on the manifest's own
// recorded version, so every manifest an older binary wrote recomputes to
// the id it recorded. A regression here fails `verifyBackupIDs` on every
// existing chain — a total, silent-until-restore loss of readability.
func TestComputeBackupIDUnchangedBelowTheRedactionVersion(t *testing.T) {
	for _, c := range []struct {
		fv   int
		want string
	}{
		{FormatVersionLegacy, goldenPreRedactionBackupIDLegacy},
		{FormatVersionSecurityMetadata, goldenPreRedactionBackupIDLegacy},
		{FormatVersionStandaloneSequences, goldenPreRedactionBackupIDLegacy},
		{FormatVersionEncryptedChunkBinding, goldenPreRedactionBackupIDLegacy},
		{FormatVersionSignedManifest, goldenPreRedactionBackupIDLegacy},
		{FormatVersionChunkTableBinding, goldenPreRedactionBackupIDLegacy},
		{FormatVersionCDCPositionBinding, goldenPreRedactionBackupIDV8},
		{FormatVersionInjectiveChunkAAD, goldenPreRedactionBackupIDV8},
	} {
		m := fixedSignedManifest()
		m.FormatVersion = c.fv
		if got := ComputeBackupID(m); got != c.want {
			t.Errorf("format version %d: BackupID = %s, want %s (pre-field golden)", c.fv, got, c.want)
		}
	}
}

// TestRedactionMarkerIsBoundToTheBackupID is the live half of the
// canonExempt claim for Manifest.Redaction: the exemption says the marker
// is bound transitively through ComputeBackupID, so deleting or editing
// it must move the id. If it did not, the exemption would be a shrug and
// the marker would be strippable with no consequence anywhere.
func TestRedactionMarkerIsBoundToTheBackupID(t *testing.T) {
	redacted := fixedSignedManifest()
	redacted.Redaction = &RedactionInfo{RuleCount: 2, Fingerprint: "aaaabbbbccccdddd"}
	StampRedaction(redacted)
	if redacted.FormatVersion != FormatVersionRedaction {
		t.Fatalf("StampRedaction left FormatVersion at %d; want %d", redacted.FormatVersion, FormatVersionRedaction)
	}
	base := ComputeBackupID(redacted)

	stripped := fixedSignedManifest()
	stripped.FormatVersion = FormatVersionRedaction // marker deleted, version left alone
	if got := ComputeBackupID(stripped); got == base {
		t.Error("deleting the redaction marker did not change the BackupID — the canonExempt reason for Manifest.Redaction is false")
	}

	edited := fixedSignedManifest()
	edited.Redaction = &RedactionInfo{RuleCount: 2, Fingerprint: "eeeeffff00001111"}
	StampRedaction(edited)
	if got := ComputeBackupID(edited); got == base {
		t.Error("editing the redaction fingerprint did not change the BackupID")
	}

	// And a redacted manifest at a version BELOW the fold keeps the legacy
	// id, so a mixed-version chain stays coherent (the v8 rule, restated).
	unstamped := fixedSignedManifest()
	unstamped.Redaction = &RedactionInfo{RuleCount: 2, Fingerprint: "aaaabbbbccccdddd"}
	unstamped.FormatVersion = FormatVersionInjectiveChunkAAD
	if got := ComputeBackupID(unstamped); got != goldenPreRedactionBackupIDV8 {
		t.Errorf("a pre-10 manifest folded the marker anyway: BackupID = %s, want %s", got, goldenPreRedactionBackupIDV8)
	}
}

// TestStampRedactionIsANoOpWithoutTheMarker pins the proportionality rule
// the whole Bug-116 ladder rests on: an unredacted backup keeps its
// feature-minimum version, so it still restores on older binaries.
func TestStampRedactionIsANoOpWithoutTheMarker(t *testing.T) {
	m := fixedSignedManifest()
	before := m.FormatVersion
	StampRedaction(m)
	if m.FormatVersion != before {
		t.Errorf("StampRedaction raised an unredacted manifest from %d to %d", before, m.FormatVersion)
	}
	StampRedaction(nil) // must not panic
}

// TestRedactionMarkerRoundTripsThroughJSON pins the read side: a document
// written by this build decodes with the marker intact (the guards read
// it), and a document from a release that predates the field decodes to
// nil rather than to some in-between state.
func TestRedactionMarkerRoundTripsThroughJSON(t *testing.T) {
	m := fixedSignedManifest()
	m.Redaction = &RedactionInfo{RuleCount: 3, Fingerprint: "0123456789abcdef"}
	StampRedaction(m)
	doc, err := MarshalManifest(m)
	if err != nil {
		t.Fatal(err)
	}
	back := &Manifest{}
	if err := json.Unmarshal(doc, back); err != nil {
		t.Fatal(err)
	}
	if back.Redaction == nil {
		t.Fatal("redaction marker lost across the JSON boundary")
	}
	if *back.Redaction != *m.Redaction {
		t.Errorf("redaction marker changed across the JSON boundary: %+v -> %+v", *m.Redaction, *back.Redaction)
	}
	if back.FormatVersion != FormatVersionRedaction {
		t.Errorf("recorded format version = %d; want %d", back.FormatVersion, FormatVersionRedaction)
	}

	// A pre-v0.144.0 document: no `redaction` member at all.
	old := &Manifest{}
	if err := json.Unmarshal([]byte(`{"format_version":9,"source_engine":"postgres","kind":"full"}`), old); err != nil {
		t.Fatal(err)
	}
	if old.Redaction != nil {
		t.Errorf("a manifest with no redaction member decoded to %+v; want nil", old.Redaction)
	}
}

// TestMinimumReaderVersionKnowsTheRedactionTier keeps the readability
// floor honest: a redacted backup raises the chain's minimum reader, and
// the write-side advice must be able to name it.
func TestMinimumReaderVersionKnowsTheRedactionTier(t *testing.T) {
	if got := MinimumReaderVersion(FormatVersionRedaction); got == "" {
		t.Error("MinimumReaderVersion has no entry for FormatVersionRedaction — the raise WARN would degrade to a bare 'upgrade sluice'")
	}
}
