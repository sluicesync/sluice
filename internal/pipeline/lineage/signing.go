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
	stdcrypto "crypto"
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"sluicesync.dev/sluice/internal/crypto"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// LineageCatalogCanonVersion versions the lineage-catalog canonical
// serialization the `lineage.json.sig` signature covers. Same on-disk
// contract discipline as [irbackup.ManifestCanonVersion].
//
// v3 (ADR-0154 Phase 2): the signature scheme is folded into the
// canonical catalog bytes, mirroring the per-manifest scheme-binding.
// The verifier is DUAL-VERSION (v2/v3) so a Phase-1 (v0.99.208) lineage
// signature still verifies — see [CanonicalCatalogBytesForVersion].
const LineageCatalogCanonVersion = "sluice-lineage-canon/v3"

// LineageCatalogCanonVersionV2 is the Phase-1 lineage canonicalization
// (no scheme token), preserved verbatim as ON-DISK CONTRACT for the
// dual-version verifier. NEVER change its rendering.
const LineageCatalogCanonVersionV2 = "sluice-lineage-canon/v2"

// lineageCatalogHasScheme maps each SUPPORTED lineage canon version to
// whether it folds the scheme token in (v2 no, v3 yes). Absent == future.
var lineageCatalogHasScheme = map[string]bool{
	LineageCatalogCanonVersionV2: false,
	LineageCatalogCanonVersion:   true,
}

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

	// ErrSignatureUnsupportedVersion is returned when a detached signature
	// records a canonicalization version NEWER than this build supports
	// (it was written by a newer sluice). This is NOT a tamper signal —
	// the operator's fix is to upgrade sluice, not to distrust the backup —
	// so it maps to its own message, never SIGNATURE-INVALID.
	ErrSignatureUnsupportedVersion = errors.New("detached signature uses a newer canonicalization than this build supports; upgrade sluice")

	// ErrSignatureUnsupportedScheme is the scheme/algorithm sibling of
	// [ErrSignatureUnsupportedVersion]: a detached signature records a scheme
	// FAMILY (e.g. a future post-quantum scheme) or a `kms/<algorithm>` whose
	// algorithm this build cannot verify. Like the version case it is a
	// forward-incompatibility, NOT tamper — the fix is to upgrade sluice — so
	// it maps to SLUICE-E-BACKUP-SIGNATURE-UNSUPPORTED, never -INVALID. This
	// matters because the underlying primitives fail CLOSED (an unknown kms
	// algorithm collapses to a false MAC; an unknown family would be verified
	// with the wrong primitive), which is indistinguishable from tamper unless
	// the caller recognizes the scheme is simply unknown BEFORE verifying.
	ErrSignatureUnsupportedScheme = errors.New("detached signature uses a signature scheme or algorithm this build does not support; upgrade sluice")
)

// effectiveManifestScheme returns the scheme a manifest signature at the
// given canon version binds to. A v2 (Phase-1) signature predates the
// scheme token, so its scheme is IMPLICITLY [irbackup.SignatureSchemeHMACKEK]
// regardless of what its (possibly relabeled) Scheme field claims — this
// preserves the Phase-1 refuse-on-relabel behavior for v2 signatures.
// v3+ signatures bind the claimed scheme.
func effectiveManifestScheme(canonVersion, claimed string) string {
	if canonVersion == irbackup.ManifestCanonVersionV2 {
		return irbackup.SignatureSchemeHMACKEK
	}
	return claimed
}

// effectiveLineageScheme is [effectiveManifestScheme] for the lineage
// catalog signature (its own v2 constant).
func effectiveLineageScheme(canonVersion, claimed string) string {
	if canonVersion == LineageCatalogCanonVersionV2 {
		return irbackup.SignatureSchemeHMACKEK
	}
	return claimed
}

// Signer carries the material to sign and/or verify a manifest +
// lineage catalog under one of the ADR-0154 schemes. It dispatches on
// [Signer.Scheme]:
//
//   - [irbackup.SignatureSchemeHMACKEK] (Phase 1): symmetric HMAC keyed
//     off the chain KEK. [Signer.Key] holds the derived HMAC key; the
//     same material signs and verifies.
//   - [irbackup.SignatureSchemeEd25519] (Phase 2): asymmetric Ed25519.
//     edPub verifies; edPriv (nil for a verify-only signer) signs.
//   - [irbackup.SignatureSchemeKMS] (Phase 3): a cloud-KMS asymmetric
//     signature. kmsSigner is the sign seam (nil for a verify-only
//     signer); kmsPub + kmsAlgorithm drive the PURE local verify. The
//     on-disk scheme token is the COMPOSITE `kms/<algorithm>` (see
//     [Signer.schemeTag]).
//
// An empty Scheme is treated as HMAC-off-KEK for backward compatibility
// (pre-Phase-2 constructions that set only Key/KeyID).
type Signer struct {
	// Scheme selects the sign/verify primitive FAMILY. "" == HMAC-off-KEK.
	// For the kms family the algorithm lives separately in kmsAlgorithm and
	// is composed into the on-disk token by [Signer.schemeTag].
	Scheme string

	// KeyID is a stable, non-secret fingerprint of the signing key,
	// recorded in the detached signature.
	KeyID string

	// Key is the HMAC-SHA-256 key (HMAC-off-KEK scheme only).
	Key []byte

	// edPriv / edPub hold the Ed25519 keypair halves (ed25519 scheme).
	// edPriv is nil for a verify-only signer built from a public key.
	edPriv ed25519.PrivateKey
	edPub  ed25519.PublicKey

	// kms* hold the Phase-3 KMS material. kmsSigner is the sign seam (nil
	// for a verify-only signer); kmsAlgorithm is the sluice-canonical
	// algorithm bound into the composite scheme token; kmsPub is the
	// resolved public key the PURE verify runs against; kmsKeyRef is the
	// advisory versioned key ref recorded in the `.sig`.
	kmsSigner    crypto.KMSSigner
	kmsAlgorithm string
	kmsPub       stdcrypto.PublicKey
	kmsKeyRef    string
}

// schemeTag returns the on-disk scheme TOKEN for this signer. For the kms
// family it is the COMPOSITE `kms/<algorithm>` (e.g. `kms/ecdsa-p256`) —
// the algorithm is bound by being inside the token that is folded into the
// signed canonical bytes. An empty Scheme defaults to HMAC-off-KEK.
func (s *Signer) schemeTag() string {
	switch s.Scheme {
	case "":
		return irbackup.SignatureSchemeHMACKEK
	case irbackup.SignatureSchemeKMS:
		return irbackup.SignatureSchemeKMS + "/" + s.kmsAlgorithm
	default:
		return s.Scheme
	}
}

// canSign reports whether this signer holds material to PRODUCE a
// signature (not just verify one). An Ed25519 / KMS verify-only signer
// cannot.
func (s *Signer) canSign() bool {
	switch s.Scheme {
	case irbackup.SignatureSchemeEd25519:
		return len(s.edPriv) == ed25519.PrivateKeySize
	case irbackup.SignatureSchemeKMS:
		return s.kmsSigner != nil
	default:
		return len(s.Key) > 0
	}
}

// sign returns the MAC/signature over payload under this signer's scheme.
// Only the kms scheme performs ctx/IO (the KMS Sign call); HMAC/Ed25519
// ignore ctx and stay pure + in-process.
func (s *Signer) sign(ctx context.Context, payload []byte) ([]byte, error) {
	switch s.Scheme {
	case irbackup.SignatureSchemeEd25519:
		if len(s.edPriv) != ed25519.PrivateKeySize {
			return nil, errors.New("lineage: ed25519 signer is verify-only (no private key) and cannot sign")
		}
		return crypto.SignManifestEd25519(s.edPriv, payload), nil
	case irbackup.SignatureSchemeKMS:
		if s.kmsSigner == nil {
			return nil, errors.New("lineage: kms signer is verify-only (no KMS Sign client) and cannot sign")
		}
		return s.kmsSigner.Sign(ctx, payload)
	default:
		return crypto.SignManifestHMAC(s.Key, payload), nil
	}
}

// verify reports whether sig authenticates payload under this signer's
// scheme. PURE for every scheme (constant-time HMAC; ed25519.Verify; local
// stdlib ecdsa/rsa/ed25519 for kms — no KMS access needed to verify).
func (s *Signer) verify(payload, sig []byte) bool {
	switch s.Scheme {
	case irbackup.SignatureSchemeEd25519:
		return crypto.VerifyManifestEd25519(s.edPub, payload, sig)
	case irbackup.SignatureSchemeKMS:
		return crypto.VerifyManifestKMS(s.kmsPub, s.kmsAlgorithm, payload, sig)
	default:
		return crypto.VerifyManifestHMAC(s.Key, payload, sig)
	}
}

// NewSigner derives the ADR-0154 Phase 1 HMAC-off-KEK signing key from
// env. ok is false (with a nil error) when env cannot key an HMAC off its
// KEK — env is nil, or it is a KMS envelope that does not implement
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
	return &Signer{Scheme: irbackup.SignatureSchemeHMACKEK, Key: key, KeyID: crypto.ManifestSigKeyID(key)}, true, nil
}

// NewEd25519Signer builds a sign+verify Ed25519 signer from a private
// key (ADR-0154 Phase 2). The KeyID is derived from the PUBLIC half so it
// matches what a verifier holding only the public key computes.
func NewEd25519Signer(priv ed25519.PrivateKey) *Signer {
	pub, _ := priv.Public().(ed25519.PublicKey)
	return &Signer{
		Scheme: irbackup.SignatureSchemeEd25519,
		KeyID:  crypto.Ed25519KeyID(pub),
		edPriv: priv,
		edPub:  pub,
	}
}

// NewEd25519Verifier builds a verify-only Ed25519 signer from a public
// key — the key-separated verification Phase 2 unlocks (a CI/restore host
// holds only this, never a signing secret).
func NewEd25519Verifier(pub ed25519.PublicKey) *Signer {
	return &Signer{
		Scheme: irbackup.SignatureSchemeEd25519,
		KeyID:  crypto.Ed25519KeyID(pub),
		edPub:  pub,
	}
}

// NewKMSSigner builds a sign+verify KMS signer from a resolved
// [crypto.KMSSigner] adapter (ADR-0154 Phase 3). The signing key stays in
// the HSM; the adapter has already fetched the public key, so this signer
// can also verify locally (used by compact/prune re-sign, which signs but
// may self-check). The KeyID comes from the public key so a verifier
// holding only the exported public key computes the identical id.
func NewKMSSigner(k crypto.KMSSigner) *Signer {
	return &Signer{
		Scheme:       irbackup.SignatureSchemeKMS,
		KeyID:        k.KeyID(),
		kmsSigner:    k,
		kmsAlgorithm: k.Algorithm(),
		kmsPub:       k.PublicKey(),
		kmsKeyRef:    k.KeyRef(),
	}
}

// NewKMSVerifier builds a verify-only KMS signer from a resolved public
// key + the sluice-canonical algorithm (parsed from the chain's signed
// `kms/<algorithm>` scheme token). Verification is PURE and local — a DR
// host verifies a KMS-signed chain with an exported public key and no KMS
// access. pub is the OPERATOR's trusted key (an exported PEM, or the public
// half of the `kms://` key they name), never a key a manifest references.
func NewKMSVerifier(pub stdcrypto.PublicKey, algorithm string) *Signer {
	// Best-effort key-id for the reported "recorded vs verifying" message;
	// an unmarshalable key leaves it empty and the MAC check still governs.
	keyID, _ := crypto.KMSManifestKeyID(pub)
	return &Signer{
		Scheme:       irbackup.SignatureSchemeKMS,
		KeyID:        keyID,
		kmsAlgorithm: algorithm,
		kmsPub:       pub,
	}
}

// ManifestSigPath returns the detached-signature object path for a
// manifest path (`manifest.json` → `manifest.json.sig`).
func ManifestSigPath(manifestPath string) string {
	return manifestPath + irbackup.SignatureFileSuffix
}

// SignManifest builds the detached signature for m at chain position seq.
// ctx is used only by the kms scheme (the KMS Sign call); HMAC/Ed25519
// ignore it.
func (s *Signer) SignManifest(ctx context.Context, m *irbackup.Manifest, seq int) (*irbackup.ManifestSignature, error) {
	payload, err := irbackup.CanonicalManifestBytes(m, seq, s.schemeTag())
	if err != nil {
		return nil, err
	}
	mac, err := s.sign(ctx, payload)
	if err != nil {
		return nil, err
	}
	return &irbackup.ManifestSignature{
		CanonVersion: irbackup.ManifestCanonVersion,
		Scheme:       s.schemeTag(),
		KeyID:        s.KeyID,
		Algorithm:    s.kmsAlgorithm, // "" (omitted) for non-kms schemes
		KeyRef:       s.kmsKeyRef,    // "" (omitted) for non-kms schemes
		Sequence:     seq,
		ChunkCount:   irbackup.ManifestChunkCount(m),
		MAC:          hex.EncodeToString(mac),
	}, nil
}

// WriteManifestSig signs m at seq and writes the detached signature next
// to manifestPath.
func WriteManifestSig(ctx context.Context, store irbackup.Store, manifestPath string, m *irbackup.Manifest, seq int, s *Signer) error {
	sig, err := s.SignManifest(ctx, m, seq)
	if err != nil {
		return err
	}
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
	// DUAL-VERSION: recompute at the signature's OWN recorded canon version
	// so a Phase-1 v2 signature still verifies on a Phase-2 binary. The
	// effective scheme of a v2 signature is implicitly HMAC-off-KEK (v2
	// predates the scheme token), which keeps the Phase-1 refuse-on-relabel
	// behavior for v2.
	wantScheme := effectiveManifestScheme(sig.CanonVersion, sig.Scheme)
	// Scheme-binding: the effective scheme MUST match the verifier's. A
	// mismatch is a relabel-tamper signal (an HMAC `.sig` relabeled
	// ed25519, or vice versa) — refuse. For v3 the scheme is ALSO folded
	// into the canonical bytes below, so even if this check were bypassed
	// the MAC would fail; the check gives a precise error first.
	if wantScheme != s.schemeTag() {
		return fmt.Errorf("manifest %q: signature scheme %q != verifier scheme %q (relabel-tamper?): %w",
			manifestPath, wantScheme, s.schemeTag(), ErrSignatureInvalid)
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
	payload, err := irbackup.CanonicalManifestBytesForVersion(m, seq, sig.CanonVersion, wantScheme)
	if errors.Is(err, irbackup.ErrUnsupportedCanonVersion) {
		return fmt.Errorf("manifest %q: %w", manifestPath, ErrSignatureUnsupportedVersion)
	}
	if err != nil {
		return fmt.Errorf("manifest %q: recompute canonical bytes: %w", manifestPath, err)
	}
	if !s.verify(payload, mac) {
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
//
// scheme is folded in (as in [irbackup.CanonicalManifestBytes]) so the
// lineage signature is scheme-bound identically to the per-manifest ones.
// The writer always emits [LineageCatalogCanonVersion]; the DUAL-VERSION
// verifier recomputes at the signature's own version via
// [CanonicalCatalogBytesForVersion].
func CanonicalCatalogBytes(cat *Catalog, scheme string) []byte {
	// The writer path only ever emits the current version, which is always
	// supported, so the error is impossible here.
	b, _ := CanonicalCatalogBytesForVersion(cat, LineageCatalogCanonVersion, scheme)
	return b
}

// CanonicalCatalogBytesForVersion renders the lineage-catalog canonical
// bytes at a SPECIFIC canon version (dual-version verify): v2 (Phase 1)
// omits the scheme token, byte-matching what v0.99.208 signed; v3 folds
// it in. Everything after the version+scheme prefix is identical across
// versions. Returns [irbackup.ErrUnsupportedCanonVersion] for a newer
// version this build cannot recompute.
func CanonicalCatalogBytesForVersion(cat *Catalog, canonVersion, scheme string) ([]byte, error) {
	withScheme, ok := lineageCatalogHasScheme[canonVersion]
	if !ok {
		return nil, fmt.Errorf("%w (lineage canon version %q)", irbackup.ErrUnsupportedCanonVersion, canonVersion)
	}
	var b strings.Builder
	lpTok(&b, canonVersion)
	if withScheme {
		lpTok(&b, "scheme")
		lpTok(&b, scheme)
	}
	lpTok(&b, "format_version")
	lpTok(&b, strconv.Itoa(cat.FormatVersion))
	lpTok(&b, "source_engine")
	lpTok(&b, cat.SourceEngine)
	lpTok(&b, "restorable_from_segment")
	lpTok(&b, strconv.Itoa(cat.RestorableFromSegment))
	lpTok(&b, "segment_count")
	lpTok(&b, strconv.Itoa(len(cat.Segments)))
	for i := range cat.Segments {
		seg := &cat.Segments[i]
		lpTok(&b, "segment")
		lpTok(&b, strconv.Itoa(i))
		lpTok(&b, seg.SegmentID)
		lpTok(&b, seg.Dir)
		lpTok(&b, seg.FullManifestPath)
		lpTok(&b, seg.StartPosition.Engine)
		lpTok(&b, seg.StartPosition.Token)
		lpTok(&b, seg.EndPosition.Engine)
		lpTok(&b, seg.EndPosition.Token)
		lpTok(&b, strconv.Itoa(len(seg.Incrementals)))
		for j, ip := range seg.Incrementals {
			lpTok(&b, strconv.Itoa(j))
			lpTok(&b, ip)
		}
	}
	return []byte(b.String()), nil
}

// lpTok appends one length-prefixed token (`<len>:<bytes>\n`) — the same
// injective framing as [irbackup] uses, so no path/token byte can forge a
// structural boundary.
func lpTok(b *strings.Builder, s string) {
	b.WriteString(strconv.Itoa(len(s)))
	b.WriteByte(':')
	b.WriteString(s)
	b.WriteByte('\n')
}

// SignLineage builds the detached signature for the lineage catalog. It
// reuses the [irbackup.ManifestSignature] envelope (Sequence carries the
// total manifest count across the lineage — the tip high-water; ChunkCount
// is unused and left 0).
func (s *Signer) SignLineage(ctx context.Context, cat *Catalog) (*irbackup.ManifestSignature, error) {
	payload := CanonicalCatalogBytes(cat, s.schemeTag())
	mac, err := s.sign(ctx, payload)
	if err != nil {
		return nil, err
	}
	return &irbackup.ManifestSignature{
		CanonVersion: LineageCatalogCanonVersion,
		Scheme:       s.schemeTag(),
		KeyID:        s.KeyID,
		Algorithm:    s.kmsAlgorithm, // "" (omitted) for non-kms schemes
		KeyRef:       s.kmsKeyRef,    // "" (omitted) for non-kms schemes
		Sequence:     totalManifestCount(cat),
		MAC:          hex.EncodeToString(mac),
	}, nil
}

// WriteLineageSig signs cat and writes lineage.json.sig.
func WriteLineageSig(ctx context.Context, store irbackup.Store, cat *Catalog, s *Signer) error {
	sig, err := s.SignLineage(ctx, cat)
	if err != nil {
		return err
	}
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
	sig, present, err := readLineageSig(ctx, store)
	if err != nil {
		return err
	}
	if !present {
		return fmt.Errorf("lineage catalog %q: %w", LineageCatalogFileName, ErrSignatureMissing)
	}
	// DUAL-VERSION (as in [VerifyManifest]): recompute at the signature's
	// own recorded version; a v2 (Phase-1) lineage signature verifies on a
	// Phase-2 binary. A v2 signature's scheme is implicitly HMAC-off-KEK.
	wantScheme := effectiveLineageScheme(sig.CanonVersion, sig.Scheme)
	if wantScheme != s.schemeTag() {
		return fmt.Errorf("lineage catalog: signature scheme %q != verifier scheme %q (relabel-tamper?): %w",
			wantScheme, s.schemeTag(), ErrSignatureInvalid)
	}
	if got := totalManifestCount(cat); sig.Sequence != got {
		return fmt.Errorf("lineage catalog: signed link count %d != actual %d (dropped-newest-link?): %w",
			sig.Sequence, got, ErrSignatureInvalid)
	}
	mac, err := hex.DecodeString(sig.MAC)
	if err != nil {
		return fmt.Errorf("lineage catalog: decode signature mac: %w: %w", err, ErrSignatureInvalid)
	}
	payload, err := CanonicalCatalogBytesForVersion(cat, sig.CanonVersion, wantScheme)
	if errors.Is(err, irbackup.ErrUnsupportedCanonVersion) {
		return fmt.Errorf("lineage catalog: %w", ErrSignatureUnsupportedVersion)
	}
	if err != nil {
		return fmt.Errorf("lineage catalog: recompute canonical bytes: %w", err)
	}
	if !s.verify(payload, mac) {
		return fmt.Errorf("lineage catalog (key_id recorded %q, verifying %q): %w",
			sig.KeyID, s.KeyID, ErrSignatureInvalid)
	}
	return nil
}

// ChainSignatureScheme probes the scheme a signed chain claims, so the
// read side can select the matching verification material (an HMAC-off-KEK
// chain verifies with the envelope KEK; an Ed25519 chain with a
// `--verify-key`). It reads the claimed scheme from `lineage.json.sig`
// (present on every well-formed signed chain), falling back to the root
// manifest's `.sig`. ok is false when no signature object is present.
//
// The claimed scheme is attacker-controllable (it is not itself under an
// outer signature until verification runs), but selecting the verifier
// from it is SAFE: if the operator lacks material for the claimed scheme,
// the read side takes the unverifiable warn/refuse path; if they have it,
// a relabel fails the MAC (the scheme is folded into the signed bytes).
//
// A v2 (Phase-1) signature is reported as HMAC-off-KEK regardless of its
// (possibly relabeled) Scheme field — v2 predates the scheme token, so
// its scheme is definitionally HMAC — which keeps a relabeled v2 chain on
// the strict HMAC-verify path (matching Phase 1's refuse-on-relabel).
func ChainSignatureScheme(ctx context.Context, store irbackup.Store) (scheme string, ok bool, err error) {
	if sig, present, lerr := readLineageSig(ctx, store); lerr != nil {
		return "", false, lerr
	} else if present {
		return effectiveLineageScheme(sig.CanonVersion, sig.Scheme), true, nil
	}
	sig, present, err := ReadManifestSig(ctx, store, ManifestFileName)
	if err != nil {
		return "", false, err
	}
	if !present {
		return "", false, nil
	}
	return effectiveManifestScheme(sig.CanonVersion, sig.Scheme), true, nil
}

// readLineageSig reads and decodes lineage.json.sig. present is false
// (nil error) when the object is absent.
func readLineageSig(ctx context.Context, store irbackup.Store) (sig *irbackup.ManifestSignature, present bool, err error) {
	exists, err := store.Exists(ctx, LineageSigFileName)
	if err != nil {
		return nil, false, fmt.Errorf("inspect %q: %w", LineageSigFileName, err)
	}
	if !exists {
		return nil, false, nil
	}
	rc, err := store.Get(ctx, LineageSigFileName)
	if err != nil {
		return nil, false, fmt.Errorf("get %q: %w", LineageSigFileName, err)
	}
	defer func() { _ = rc.Close() }()
	body, err := io.ReadAll(rc)
	if err != nil {
		return nil, false, fmt.Errorf("read %q: %w", LineageSigFileName, err)
	}
	sig, err = irbackup.UnmarshalManifestSignature(body)
	if err != nil {
		return nil, false, err
	}
	return sig, true, nil
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
	if !s.canSign() {
		return errors.New("resign: signer holds no signing key (verify-only) — a compact/prune of a signed chain needs the signing key material (--sign-key for Ed25519, or the chain --encrypt passphrase for HMAC-off-KEK)")
	}
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

// SignatureUnsupportedError wraps a forward-incompatible verification
// failure ([ErrSignatureUnsupportedVersion] / [ErrSignatureUnsupportedScheme])
// in the operator-facing SLUICE-E-BACKUP-SIGNATURE-UNSUPPORTED coded class.
// It is fail-closed but NOT a tamper accusation: a newer sluice wrote the
// signature, so the fix is to upgrade, not to distrust the backup.
func SignatureUnsupportedError(err error) error {
	return sluicecode.Wrap(sluicecode.CodeBackupSignatureUnsupported,
		"upgrade sluice to a build that supports this backup's signature scheme/canonicalization; the backup is not necessarily tampered", err)
}

// CodeForSignatureError maps a raw verification error to its coded form,
// or returns it unchanged when it is neither Err* sentinel. The
// UNSUPPORTED cases are checked before INVALID: a forward-incompatible
// signature is an upgrade prompt, never a tamper accusation.
func CodeForSignatureError(err error) error {
	switch {
	case errors.Is(err, ErrSignatureMissing):
		return SignatureMissingError(err)
	case errors.Is(err, ErrSignatureUnsupportedVersion), errors.Is(err, ErrSignatureUnsupportedScheme):
		return SignatureUnsupportedError(err)
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
