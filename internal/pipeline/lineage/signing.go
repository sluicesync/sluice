// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package lineage

// Manifest / lineage signing glue (ADR-0154 Phase 1). This is where the
// three layers meet: the crypto primitive ([crypto.DeriveManifestHMACKey]
// + HMAC), the pure canonical serialization ([irbackup.CanonicalManifestBytes]),
// and the store I/O for the detached signature objects. The irbackup
// package stays pure (no crypto, no I/O); crypto stays manifest-agnostic;
// this package binds them.
//
// A signed chain carries one `<manifest>.sig` per manifest and one
// `lineage.json.sig` for the chain tip. The per-manifest signature
// authenticates that manifest (closing R2 change-list truncation and
// forgery, and authenticating the lineage stitch — parent pointer +
// sequence are under the MAC); the lineage signature authenticates the
// ENUMERATION of links (closing dropped-newest-link — the freshness (c)
// residual the per-manifest signatures alone can't see).

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"sluicesync.dev/sluice/internal/crypto"
	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// LineageCatalogCanonVersion versions the lineage-catalog canonical
// serialization the `lineage.json.sig` signature covers. Same on-disk
// contract discipline as [irbackup.ManifestCanonVersion].
const LineageCatalogCanonVersion = "sluice-lineage-canon/v1"

// LineageSigFileName is the detached signature object for lineage.json.
const LineageSigFileName = LineageCatalogFileName + irbackup.SignatureFileSuffix

// Sentinel errors for the verification policy. Callers map these to the
// operator-facing coded errors ([SignatureMissingError] /
// [SignatureInvalidError]) so the CLI exit boundary reports a stable
// SLUICE-E-BACKUP-SIGNATURE-* class.
var (
	// ErrSignatureMissing is returned when a signed (v6) artifact has no
	// detached signature object.
	ErrSignatureMissing = errors.New("detached signature is missing")

	// ErrSignatureInvalid is returned when a detached signature is
	// present but does not verify (tampered / rolled-back / wrong key /
	// scheme mismatch).
	ErrSignatureInvalid = errors.New("detached signature failed verification")
)

// Signer carries the derived HMAC signing key + its public fingerprint.
// Constructed once per run from the chain envelope; reused to sign /
// verify every manifest + the lineage catalog.
type Signer struct {
	Key   []byte
	KeyID string
}

// NewSigner derives the ADR-0154 Phase 1 signing key from env. ok is
// false (with a nil error) when env cannot key an HMAC off its KEK —
// env is nil, or it is a KMS envelope that does not implement
// [crypto.ManifestSigner] (Phase 1 signs only passphrase-encrypted
// chains; KMS Sign is Phase 3). A non-nil error is a real derivation
// failure. The write side turns ok==false into a loud refusal when
// signing was requested; the read side treats ok==false as "cannot
// verify" (warn-or-refuse per policy).
func NewSigner(env crypto.EnvelopeEncryption) (s *Signer, ok bool, err error) {
	ms, isSigner := env.(crypto.ManifestSigner)
	if !isSigner {
		return nil, false, nil
	}
	key, err := ms.ManifestSigningKey()
	if err != nil {
		return nil, false, fmt.Errorf("derive manifest signing key: %w", err)
	}
	return &Signer{Key: key, KeyID: crypto.ManifestSigKeyID(key)}, true, nil
}

// ManifestSigPath returns the detached-signature object path for a
// manifest path (`manifest.json` → `manifest.json.sig`).
func ManifestSigPath(manifestPath string) string {
	return manifestPath + irbackup.SignatureFileSuffix
}

// SignManifest builds the detached signature for m at chain position seq.
func (s *Signer) SignManifest(m *irbackup.Manifest, seq int) *irbackup.ManifestSignature {
	payload := irbackup.CanonicalManifestBytes(m, seq)
	mac := crypto.SignManifestHMAC(s.Key, payload)
	return &irbackup.ManifestSignature{
		CanonVersion: irbackup.ManifestCanonVersion,
		Scheme:       irbackup.SignatureSchemeHMACKEK,
		KeyID:        s.KeyID,
		Sequence:     seq,
		ChunkCount:   irbackup.ManifestChunkCount(m),
		MAC:          hex.EncodeToString(mac),
	}
}

// WriteManifestSig signs m at seq and writes the detached signature next
// to manifestPath.
func WriteManifestSig(ctx context.Context, store irbackup.Store, manifestPath string, m *irbackup.Manifest, seq int, s *Signer) error {
	sig := s.SignManifest(m, seq)
	body, err := irbackup.MarshalManifestSignature(sig)
	if err != nil {
		return err
	}
	return store.Put(ctx, ManifestSigPath(manifestPath), bytes.NewReader(body))
}

// ReadManifestSig reads the detached signature for manifestPath. ok is
// false (nil error) when the `.sig` object is absent.
func ReadManifestSig(ctx context.Context, store irbackup.Store, manifestPath string) (sig *irbackup.ManifestSignature, ok bool, err error) {
	sigPath := ManifestSigPath(manifestPath)
	exists, err := store.Exists(ctx, sigPath)
	if err != nil {
		return nil, false, fmt.Errorf("inspect %q: %w", sigPath, err)
	}
	if !exists {
		return nil, false, nil
	}
	rc, err := store.Get(ctx, sigPath)
	if err != nil {
		return nil, false, fmt.Errorf("get %q: %w", sigPath, err)
	}
	defer func() { _ = rc.Close() }()
	body, err := io.ReadAll(rc)
	if err != nil {
		return nil, false, fmt.Errorf("read %q: %w", sigPath, err)
	}
	sig, err = irbackup.UnmarshalManifestSignature(body)
	if err != nil {
		return nil, false, err
	}
	return sig, true, nil
}

// VerifyManifest reads and verifies the detached signature for m at the
// expected chain position seq. Returns nil on a valid signature,
// [ErrSignatureMissing] when absent, or [ErrSignatureInvalid] (wrapped
// with a naming context) on any mismatch — scheme, canon-version,
// sequence, chunk-count, or MAC. The sequence and chunk-count are also
// under the MAC; the explicit checks give a precise error before the
// MAC comparison.
func VerifyManifest(ctx context.Context, store irbackup.Store, manifestPath string, m *irbackup.Manifest, seq int, s *Signer) error {
	sig, ok, err := ReadManifestSig(ctx, store, manifestPath)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("manifest %q at sequence %d: %w", manifestPath, seq, ErrSignatureMissing)
	}
	if sig.CanonVersion != irbackup.ManifestCanonVersion {
		return fmt.Errorf("manifest %q: signature canon version %q != %q: %w",
			manifestPath, sig.CanonVersion, irbackup.ManifestCanonVersion, ErrSignatureInvalid)
	}
	if sig.Scheme != irbackup.SignatureSchemeHMACKEK {
		return fmt.Errorf("manifest %q: signature scheme %q is not %q (Phase 1 verifies HMAC-off-KEK only): %w",
			manifestPath, sig.Scheme, irbackup.SignatureSchemeHMACKEK, ErrSignatureInvalid)
	}
	if sig.Sequence != seq {
		return fmt.Errorf("manifest %q: signed sequence %d != expected chain position %d (rolled-back / reordered link?): %w",
			manifestPath, sig.Sequence, seq, ErrSignatureInvalid)
	}
	if got := irbackup.ManifestChunkCount(m); sig.ChunkCount != got {
		return fmt.Errorf("manifest %q: signed chunk count %d != actual %d (truncated change-list?): %w",
			manifestPath, sig.ChunkCount, got, ErrSignatureInvalid)
	}
	mac, err := hex.DecodeString(sig.MAC)
	if err != nil {
		return fmt.Errorf("manifest %q: decode signature mac: %w: %w", manifestPath, err, ErrSignatureInvalid)
	}
	payload := irbackup.CanonicalManifestBytes(m, seq)
	if !crypto.VerifyManifestHMAC(s.Key, payload, mac) {
		return fmt.Errorf("manifest %q (key_id recorded %q, verifying %q): %w",
			manifestPath, sig.KeyID, s.KeyID, ErrSignatureInvalid)
	}
	return nil
}

// CanonicalCatalogBytes is the deterministic serialization of the
// lineage catalog's structural record — the segment/incremental
// ENUMERATION and boundary positions — that `lineage.json.sig` covers.
// A dropped-newest-link (removing the tail incremental from both the
// store and lineage.json) shrinks this; the signature over it refuses.
// The Segments and Incrementals are already order-significant, so no
// sorting is needed (nor wanted — order is part of the structure).
func CanonicalCatalogBytes(cat *Catalog) []byte {
	var b strings.Builder
	b.WriteString(LineageCatalogCanonVersion)
	b.WriteByte('\n')
	b.WriteString("format_version=" + strconv.Itoa(cat.FormatVersion) + "\n")
	b.WriteString("source_engine=" + cat.SourceEngine + "\n")
	b.WriteString("restorable_from_segment=" + strconv.Itoa(cat.RestorableFromSegment) + "\n")
	b.WriteString("segment_count=" + strconv.Itoa(len(cat.Segments)) + "\n")
	for i := range cat.Segments {
		seg := &cat.Segments[i]
		b.WriteString("segment=" + strconv.Itoa(i) + "\n")
		b.WriteString("  id=" + seg.SegmentID + "\n")
		b.WriteString("  dir=" + seg.Dir + "\n")
		b.WriteString("  full=" + seg.FullManifestPath + "\n")
		b.WriteString("  start=" + posToken(seg.StartPosition) + "\n")
		b.WriteString("  end=" + posToken(seg.EndPosition) + "\n")
		b.WriteString("  incremental_count=" + strconv.Itoa(len(seg.Incrementals)) + "\n")
		for j, ip := range seg.Incrementals {
			b.WriteString("  incremental=" + strconv.Itoa(j) + ":" + ip + "\n")
		}
	}
	return []byte(b.String())
}

// SignLineage builds the detached signature for the lineage catalog. It
// reuses the [irbackup.ManifestSignature] envelope (Sequence carries the
// total manifest count across the lineage — the tip high-water; ChunkCount
// is unused and left 0).
func (s *Signer) SignLineage(cat *Catalog) *irbackup.ManifestSignature {
	payload := CanonicalCatalogBytes(cat)
	mac := crypto.SignManifestHMAC(s.Key, payload)
	return &irbackup.ManifestSignature{
		CanonVersion: LineageCatalogCanonVersion,
		Scheme:       irbackup.SignatureSchemeHMACKEK,
		KeyID:        s.KeyID,
		Sequence:     totalManifestCount(cat),
		MAC:          hex.EncodeToString(mac),
	}
}

// WriteLineageSig signs cat and writes lineage.json.sig.
func WriteLineageSig(ctx context.Context, store irbackup.Store, cat *Catalog, s *Signer) error {
	sig := s.SignLineage(cat)
	body, err := irbackup.MarshalManifestSignature(sig)
	if err != nil {
		return err
	}
	return store.Put(ctx, LineageSigFileName, bytes.NewReader(body))
}

// SignLineageCatalog loads the lineage catalog from store and writes its
// detached signature. Called after a manifest write has updated
// lineage.json, so the signed enumeration reflects the just-added link.
// Refuses on an absent catalog — a signed chain must have one.
func SignLineageCatalog(ctx context.Context, store irbackup.Store, s *Signer) error {
	cat, ok, err := LoadLineageCatalog(ctx, store)
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("lineage: cannot sign an absent catalog (lineage.json was not written)")
	}
	return WriteLineageSig(ctx, store, cat, s)
}

// VerifyLineage reads and verifies lineage.json.sig against cat. Same
// missing/invalid contract as [VerifyManifest].
func VerifyLineage(ctx context.Context, store irbackup.Store, cat *Catalog, s *Signer) error {
	exists, err := store.Exists(ctx, LineageSigFileName)
	if err != nil {
		return fmt.Errorf("inspect %q: %w", LineageSigFileName, err)
	}
	if !exists {
		return fmt.Errorf("lineage catalog %q: %w", LineageCatalogFileName, ErrSignatureMissing)
	}
	rc, err := store.Get(ctx, LineageSigFileName)
	if err != nil {
		return fmt.Errorf("get %q: %w", LineageSigFileName, err)
	}
	defer func() { _ = rc.Close() }()
	body, err := io.ReadAll(rc)
	if err != nil {
		return fmt.Errorf("read %q: %w", LineageSigFileName, err)
	}
	sig, err := irbackup.UnmarshalManifestSignature(body)
	if err != nil {
		return err
	}
	if sig.CanonVersion != LineageCatalogCanonVersion || sig.Scheme != irbackup.SignatureSchemeHMACKEK {
		return fmt.Errorf("lineage catalog: signature canon/scheme %q/%q unexpected: %w",
			sig.CanonVersion, sig.Scheme, ErrSignatureInvalid)
	}
	if got := totalManifestCount(cat); sig.Sequence != got {
		return fmt.Errorf("lineage catalog: signed link count %d != actual %d (dropped-newest-link?): %w",
			sig.Sequence, got, ErrSignatureInvalid)
	}
	mac, err := hex.DecodeString(sig.MAC)
	if err != nil {
		return fmt.Errorf("lineage catalog: decode signature mac: %w: %w", err, ErrSignatureInvalid)
	}
	if !crypto.VerifyManifestHMAC(s.Key, CanonicalCatalogBytes(cat), mac) {
		return fmt.Errorf("lineage catalog (key_id recorded %q, verifying %q): %w",
			sig.KeyID, s.KeyID, ErrSignatureInvalid)
	}
	return nil
}

// ChainIsSigned reports whether the chain in store is signed (ADR-0154):
// a signed chain always carries lineage.json.sig. Cheap presence probe
// used by compact/prune to gate the re-sign-or-refuse decision (Q4).
func ChainIsSigned(ctx context.Context, store irbackup.Store) (bool, error) {
	return store.Exists(ctx, LineageSigFileName)
}

// ResignLineage re-signs EVERY manifest in the (already-rewritten)
// lineage at its new flat position, plus the lineage catalog. Compact /
// prune call it after mutating a signed chain's structure: dropping or
// merging links renumbers positions, so the whole survivor set must be
// re-signed — not just the merged successor — for the sequence-gap-free
// check to hold at restore.
func ResignLineage(ctx context.Context, store irbackup.Store, s *Signer) error {
	recs, err := ListAllSegmentManifests(ctx, store)
	if err != nil {
		return fmt.Errorf("resign: list manifests: %w", err)
	}
	for i := range recs {
		rec := &recs[i]
		if err := WriteManifestSig(ctx, rec.Segment.Store(store), rec.Path, rec.Manifest, i, s); err != nil {
			return fmt.Errorf("resign manifest %q: %w", rec.Path, err)
		}
	}
	cat, err := ResolveLineage(ctx, store)
	if err != nil {
		return fmt.Errorf("resign: resolve lineage: %w", err)
	}
	return WriteLineageSig(ctx, store, cat, s)
}

// SignatureMissingError wraps a lineage.Err* verification failure in the
// operator-facing SLUICE-E-BACKUP-SIGNATURE-MISSING coded class.
func SignatureMissingError(err error) error {
	return sluicecode.Wrap(sluicecode.CodeBackupSignatureMissing,
		"restore from a copy whose .sig objects are intact, or re-run the maintenance step with the chain's --encrypt key material", err)
}

// SignatureInvalidError wraps a lineage.Err* verification failure in the
// operator-facing SLUICE-E-BACKUP-SIGNATURE-INVALID coded class.
func SignatureInvalidError(err error) error {
	return sluicecode.Wrap(sluicecode.CodeBackupSignatureInvalid,
		"restore from an untampered copy; the signature caught exactly the substitution/rollback/truncation it exists to catch", err)
}

// CodeForSignatureError maps a raw verification error to its coded form,
// or returns it unchanged when it is neither Err* sentinel.
func CodeForSignatureError(err error) error {
	switch {
	case errors.Is(err, ErrSignatureMissing):
		return SignatureMissingError(err)
	case errors.Is(err, ErrSignatureInvalid):
		return SignatureInvalidError(err)
	default:
		return err
	}
}

// ManifestCount returns the number of manifests across every segment
// (full + incrementals) in cat — the lineage's flat manifest count. The
// write side uses (count-1) as the newest link's signing sequence; the
// read side derives the same value from the walked chain length.
func ManifestCount(cat *Catalog) int { return totalManifestCount(cat) }

// totalManifestCount returns the number of manifests across every
// segment (full + incrementals) — the lineage's flat manifest count,
// which is the tip high-water the lineage signature pins.
func totalManifestCount(cat *Catalog) int {
	n := 0
	for i := range cat.Segments {
		n += 1 + len(cat.Segments[i].Incrementals)
	}
	return n
}

func posToken(p ir.Position) string { return p.Engine + "|" + p.Token }
