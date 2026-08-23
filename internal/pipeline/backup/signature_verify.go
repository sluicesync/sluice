// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

// Restore/verify-side signature checking (ADR-0154 Phase 1). The
// verification policy (ratified ADR-0154 §4):
//
//   - Pre-v6 manifests carry no signature — restore normally, forever
//     (the FormatVersion gate means "predates signing", not "untrusted").
//   - A v6 manifest verified with a KEK-holding envelope (the encrypted
//     restore ALWAYS has one, so it can always verify) refuses loudly on
//     a missing/invalid/rolled-back signature — the strict default that
//     needs no extra flag.
//   - A v6 manifest with NO verification key (a KMS-signed forgery, or
//     `backup verify` without --encrypt) is WARNed present-but-unverified
//     and proceeds — a disaster restore must not fail for a signature it
//     cannot check — UNLESS the operator set strict-always (RequireSignature).
//
// The freshness anchors (ADR-0154 §2.2 option c) fall out of the
// per-link checks: each link's signed sequence must equal its position
// in the walked chain (a dropped/reordered middle link shifts positions
// and fails), each link's signed chunk-count must equal its actual chunk
// list length (a truncated change-list fails), and the lineage catalog's
// signed link enumeration closes dropped-newest-link.

import (
	"context"
	stdcrypto "crypto"
	"crypto/ed25519"
	"errors"
	"fmt"
	"log/slog"

	"sluicesync.dev/sluice/internal/crypto"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

// verifyMaterial carries the ADR-0154 verify-key sources — the encryption
// envelope (HMAC-off-KEK, Phase 1) and an asymmetric PUBLIC key (Ed25519 /
// ECDSA / RSA, from `--verify-key`, Phase 2/3). The verifier for a given
// chain is chosen from the chain's CLAIMED scheme FAMILY against whichever
// of these is present: an HMAC-off-KEK chain needs the envelope; an
// Ed25519 or KMS chain needs the public key (the KEK does NOT verify an
// asymmetric signature — the schemes are cryptographically independent).
// For the kms family, verifyPub is the OPERATOR's trusted key (an exported
// PEM, or the public half of the `kms://` key they named) — never a key a
// manifest references, so a rewritten manifest KeyRef cannot redirect
// trust.
type verifyMaterial struct {
	env       crypto.EnvelopeEncryption
	verifyPub stdcrypto.PublicKey
}

// signerForScheme builds the [lineage.Signer] that verifies scheme's
// signatures from the available material. It dispatches on the scheme
// FAMILY ([irbackup.SchemeFamily]) so a composite kms token
// (`kms/ecdsa-p256`) routes to the KMS verifier with the algorithm parsed
// from the token. ok is false when no material for that scheme is supplied
// (the caller then takes the unverifiable warn/refuse path — NEVER a
// "different scheme so skip" path, so an Ed25519/KMS chain presented with
// only a KEK does not silently pass). A non-nil error is a real
// key-derivation failure.
//
// Selecting the verifier from the claimed scheme is safe: a relabel to a
// scheme/algorithm the operator CAN verify still fails, because the scheme
// token (including the kms algorithm) is folded into the signed canonical
// bytes and each per-artifact verify re-checks sig.Scheme against the
// verifier's scheme AND runs the scheme-specific primitive.
func (m verifyMaterial) signerForScheme(scheme string) (s *lineage.Signer, ok bool, err error) {
	switch irbackup.SchemeFamily(scheme) {
	case irbackup.SignatureSchemeEd25519:
		edPub, isEd := m.verifyPub.(ed25519.PublicKey)
		if !isEd {
			return nil, false, nil
		}
		return lineage.NewEd25519Verifier(edPub), true, nil
	case irbackup.SignatureSchemeKMS:
		// A composite `kms/<algorithm>` whose algorithm this build cannot
		// verify was written by a newer sluice. Fail closed as UPGRADE, not
		// tamper: a bare NewKMSVerifier would collapse the unknown algorithm
		// to a false MAC = SIGNATURE-INVALID (an alarming, wrong signal).
		if !crypto.IsSupportedKMSAlgorithm(irbackup.SchemeAlgorithm(scheme)) {
			return nil, false, fmt.Errorf("kms scheme %q: %w", scheme, lineage.ErrSignatureUnsupportedScheme)
		}
		if m.verifyPub == nil {
			return nil, false, nil
		}
		return lineage.NewKMSVerifier(m.verifyPub, irbackup.SchemeAlgorithm(scheme)), true, nil
	case irbackup.SignatureSchemeHMACKEK:
		return hmacVerifier(m.env)
	case "":
		// Empty scheme = no signature object present (e.g. --require-signature
		// on an unsigned / fully-stripped chain): prefer an explicit verify
		// key, else the envelope, so a subsequent VerifyManifest reports the
		// precise MISSING error rather than skipping. This is NOT an unknown
		// scheme — there is simply no scheme to probe.
		if edPub, isEd := m.verifyPub.(ed25519.PublicKey); isEd {
			return lineage.NewEd25519Verifier(edPub), true, nil
		}
		return hmacVerifier(m.env)
	default:
		// A non-empty scheme FAMILY this build does not recognize (e.g. a
		// future post-quantum scheme) — written by a newer sluice. Fail closed
		// as UPGRADE, never tamper: verifying it with a known primitive would
		// yield SIGNATURE-INVALID, wrongly implying the backup is compromised.
		return nil, false, fmt.Errorf("scheme %q: %w", scheme, lineage.ErrSignatureUnsupportedScheme)
	}
}

// annotateSchemaFingerprintSkew adds the release-skew possibility to a
// SIGNATURE-INVALID error whose real cause may be that the manifest was
// written by a release with a different IR schema field set.
//
// Why this is needed at all. [irbackup.CanonicalManifestBytes] folds the
// manifest's RECORDED schema_hash verbatim (no recompute), so the signed
// bytes are release-stable — except for `deltaTableFingerprint`, which
// RECOMPUTES [irbackup.ComputeSchemaHash] over each SchemaDelta's
// before/after table at verify time. A signed manifest carrying a
// SchemaDelta therefore fails its MAC across a fingerprint epoch, and a
// MAC failure has no structure to read: it says "tampered" and means
// nothing of the kind. On the chain-RESTORE and BROKER paths
// [verifySchemaHashes] runs first and preempts this with the honest
// two-cause refusal; `backup verify` and `export-as-parquet` have no such
// preflight, which is where this note earns its keep.
//
// The signal is DATA, not a guess: the manifest's own recorded
// schema_hash failing to reproduce under this binary is exactly the
// release-skew fingerprint. Absence of the note is NOT evidence of
// tampering, and the wording says so. Same discipline as the
// unsupported-scheme branches above — never report a compatibility gap
// as a compromise.
func annotateSchemaFingerprintSkew(err error, m *irbackup.Manifest) error {
	if err == nil || m == nil || m.SchemaHash == "" || !errors.Is(err, lineage.ErrSignatureInvalid) {
		return err
	}
	got, hErr := irbackup.ComputeSchemaHash(m.Schema)
	if hErr != nil || got == m.SchemaHash {
		return err
	}
	return fmt.Errorf("%w (this manifest's recorded schema fingerprint also fails to reproduce under this binary — recorded %s, recomputed %s — which is what a manifest written by a release with a different IR schema field set looks like; a signed manifest carrying a schema delta cannot verify across that boundary, and the chain may be intact. See docs/operator/error-codes.md, SLUICE-E-BACKUP-MANIFEST-INVALID)",
		err, m.SchemaHash, got)
}

// hmacVerifier derives the Phase 1 HMAC-off-KEK verifier from env. ok is
// false when env is nil or cannot key an HMAC off its KEK (KMS — Phase 3).
func hmacVerifier(env crypto.EnvelopeEncryption) (s *lineage.Signer, ok bool, err error) {
	if env == nil {
		return nil, false, nil
	}
	return lineage.NewSigner(env)
}

// chainVerifier probes the chain's claimed signature scheme and returns
// the matching verifier from mat. ok is false when no material for the
// claimed scheme is supplied.
func chainVerifier(ctx context.Context, store irbackup.Store, mat verifyMaterial) (s *lineage.Signer, claimedScheme string, ok bool, err error) {
	scheme, _, serr := lineage.ChainSignatureScheme(ctx, store)
	if serr != nil {
		return nil, "", false, serr
	}
	signer, ok, err := mat.signerForScheme(scheme)
	return signer, scheme, ok, err
}

// manifestSigPresent reports whether the detached `.sig` object for
// manifestPath exists in store.
func manifestSigPresent(ctx context.Context, store irbackup.Store, manifestPath string) (bool, error) {
	return store.Exists(ctx, lineage.ManifestSigPath(manifestPath))
}

// chainHasSignatureArtifacts reports whether ANY ADR-0154 signature
// object is present across the lineage — the lineage.json.sig or any
// per-manifest `.sig`. This is the ROBUST signedness signal: it is
// derived from the PRESENCE of signature files, NEVER from the
// MAC-covered `FormatVersion` field. So a v6→v5 FormatVersion downgrade
// with the signatures left in place still forces verification (and then
// fails the MAC, because format_version is inside the signed canonical
// bytes). Only stripping the version stamp AND every signature object
// evades this — the honestly-documented external-anchor residual
// (ADR-0154 option b, out of Phase 1), which --require-signature closes.
func chainHasSignatureArtifacts(ctx context.Context, rootStore irbackup.Store, links []lineage.SegmentRecord) (bool, error) {
	if ok, err := lineage.ChainIsSigned(ctx, rootStore); err != nil {
		return false, err
	} else if ok {
		return true, nil
	}
	for i := range links {
		ok, err := manifestSigPresent(ctx, links[i].Segment.Store(rootStore), links[i].Path)
		if err != nil {
			return false, err
		}
		if ok {
			return true, nil
		}
	}
	return false, nil
}

// verifyManifestSignaturePolicy applies the ADR-0154 §4 policy to a
// SINGLE manifest at chain position seq, read from segStore at
// manifestPath. Verification is forced by the PRESENCE of a signature
// object (never by the tamperable FormatVersion) or by requireStrict; a
// genuinely unsigned backup with no signature files is a no-op.
func verifyManifestSignaturePolicy(
	ctx context.Context,
	segStore irbackup.Store,
	manifestPath string,
	manifest *irbackup.Manifest,
	seq int,
	mat verifyMaterial,
	requireStrict bool,
) error {
	sigPresent, err := manifestSigPresent(ctx, segStore, manifestPath)
	if err != nil {
		return err
	}
	lineageSigPresent, err := lineage.ChainIsSigned(ctx, segStore)
	if err != nil {
		return err
	}
	if !requireStrict && !sigPresent && !lineageSigPresent {
		return nil // genuinely unsigned (or fully-stripped residual — option b)
	}
	signer, claimedScheme, ok, err := chainVerifier(ctx, segStore, mat)
	if err != nil {
		return lineage.CodeForSignatureError(err) // UNSUPPORTED scheme → upgrade, not tamper
	}
	if !ok {
		return unverifiableSignedArtifact(ctx, manifestPath, claimedScheme, mat, requireStrict)
	}
	if err := lineage.VerifyManifest(ctx, segStore, manifestPath, manifest, seq, signer); err != nil {
		return lineage.CodeForSignatureError(annotateSchemaFingerprintSkew(err, manifest))
	}
	slog.InfoContext(ctx, "restore: manifest signature verified (ADR-0154)",
		slog.String("manifest", manifestPath),
		slog.Int("sequence", seq),
		slog.String("key_id", signer.KeyID))
	return nil
}

// verifyChainSignatures verifies every link's signature at its walked
// position, then the lineage catalog's signed enumeration. Verification
// is forced by the PRESENCE of signature objects (never the FormatVersion
// field — see [chainHasSignatureArtifacts]) or by requireStrict; a
// genuinely unsigned chain with no signature files is a no-op. Any link
// whose signature is absent inside a signed chain fails the per-link
// check (missing signature) — a mixed/partial-strip chain is a tamper
// signal.
func verifyChainSignatures(
	ctx context.Context,
	rootStore irbackup.Store,
	links []lineage.SegmentRecord,
	mat verifyMaterial,
	requireStrict bool,
) error {
	hasArtifacts, err := chainHasSignatureArtifacts(ctx, rootStore, links)
	if err != nil {
		return fmt.Errorf("chain restore: probe signature artifacts: %w", err)
	}
	if !requireStrict && !hasArtifacts {
		return nil
	}
	signer, claimedScheme, ok, err := chainVerifier(ctx, rootStore, mat)
	if err != nil {
		return lineage.CodeForSignatureError(err) // UNSUPPORTED scheme → upgrade, not tamper
	}
	if !ok {
		return unverifiableSignedArtifact(ctx, "chain", claimedScheme, mat, requireStrict)
	}
	for i := range links {
		link := &links[i]
		segStore := link.Segment.Store(rootStore)
		if err := lineage.VerifyManifest(ctx, segStore, link.Path, link.Manifest, i, signer); err != nil {
			return lineage.CodeForSignatureError(annotateSchemaFingerprintSkew(err, link.Manifest))
		}
	}
	// Lineage catalog enumeration — closes dropped-newest-link (the
	// per-link sequence checks alone cannot see a dropped tail).
	cat, err := lineage.ResolveLineage(ctx, rootStore)
	if err != nil {
		return fmt.Errorf("chain restore: resolve lineage for signature check: %w", err)
	}
	if err := lineage.VerifyLineage(ctx, rootStore, cat, signer); err != nil {
		return lineage.CodeForSignatureError(err)
	}
	slog.InfoContext(ctx, "chain restore: all manifest + lineage signatures verified (ADR-0154)",
		slog.Int("links", len(links)),
		slog.String("key_id", signer.KeyID))
	return nil
}

// verifyBackupSignatures is the `backup verify` reporting form: it logs
// each manifest's signature status (signed/valid, signed/invalid,
// unsigned) and the lineage status, returning the count of FAILURES to
// fold into the verify tally. An invalid signature is always a failure;
// an unverifiable signed chain (no key) is a failure only under strict.
// Reports rather than aborts so a run surfaces EVERY bad artifact.
func verifyBackupSignatures(ctx context.Context, store irbackup.Store, records []lineage.SegmentRecord, opts VerifyOptions) int {
	// Signedness is decided by the PRESENCE of signature objects, never
	// the tamperable FormatVersion field.
	hasArtifacts, err := chainHasSignatureArtifacts(ctx, store, records)
	if err != nil {
		slog.ErrorContext(ctx, "backup verify: cannot probe signature artifacts", slog.String("error", err.Error()))
		return 1
	}
	if !opts.RequireSignature && !hasArtifacts {
		slog.InfoContext(ctx, "backup verify: chain is unsigned (pre-ADR-0154 / no signature objects); no signatures to check")
		return 0
	}
	signer, claimedScheme, ok, err := chainVerifier(ctx, store, verifyMaterial{env: opts.Envelope, verifyPub: opts.VerifyKey})
	if err != nil {
		if errors.Is(err, lineage.ErrSignatureUnsupportedScheme) {
			// Forward-incompatibility, not tamper: a newer sluice wrote this
			// signature scheme. Report as an upgrade prompt (still a failure —
			// this build cannot confirm the signature).
			slog.ErrorContext(ctx, "backup verify: signature scheme unsupported by this build — upgrade sluice to verify (not a tamper signal)",
				slog.String("error", err.Error()))
			return 1
		}
		slog.ErrorContext(ctx, "backup verify: cannot derive verify key", slog.String("error", err.Error()))
		return 1
	}
	if !ok {
		mat := verifyMaterial{env: opts.Envelope, verifyPub: opts.VerifyKey}
		haveMaterial := mat.env != nil || mat.verifyPub != nil
		if opts.RequireSignature {
			if haveMaterial && claimedScheme != "" {
				slog.ErrorContext(ctx,
					"backup verify: the chain claims a signature scheme the supplied key material cannot verify, "+
						"and --require-signature is set",
					slog.String("claimed_scheme", claimedScheme),
					slog.String("supplied_material", describeVerifyMaterial(mat)))
				return 1
			}
			slog.ErrorContext(ctx, "backup verify: signed chain but no matching verification key supplied and --require-signature set")
			return 1
		}
		if haveMaterial && claimedScheme != "" {
			// The audit-Sec2 case. The operator DID supply key material; the
			// old message told them to go and supply it. Editing the recorded
			// scheme to one they hold no key for is how a signed chain is
			// moved from "verified" to "unverified-but-accepted" with every
			// .sig left in place, so say that rather than implying a
			// forgotten flag.
			slog.WarnContext(ctx,
				"backup verify: the chain claims a signature scheme the supplied key material CANNOT verify, so "+
					"its signatures were NOT checked — this is not a missing-key warning, you already supplied "+
					"material. Either the chain is signed with a different key type than you passed, or its "+
					"recorded scheme was edited, which downgrades a signed chain to an unverified one with every "+
					"signature file still present. Re-run with --require-signature to fail instead of passing.",
				slog.String("claimed_scheme", claimedScheme),
				slog.String("supplied_material", describeVerifyMaterial(mat)))
			return 0
		}
		slog.WarnContext(ctx, "backup verify: chain is signed but no matching verification key supplied — signatures are present-but-unverified. Re-run with the chain's --encrypt passphrase (HMAC-off-KEK) or --verify-key (Ed25519) to verify.")
		return 0
	}
	failed := 0
	for i := range records {
		rec := &records[i]
		segStore := rec.Segment.Store(store)
		if err := lineage.VerifyManifest(ctx, segStore, rec.Path, rec.Manifest, i, signer); err != nil {
			failed++
			// `backup verify` has no schema-hash preflight to preempt this
			// (unlike restore/broker), so the release-skew possibility is
			// annotated onto the report itself.
			slog.ErrorContext(ctx, "backup verify: signature INVALID",
				slog.String("manifest", rec.Path), slog.Int("sequence", i),
				slog.String("error", annotateSchemaFingerprintSkew(err, rec.Manifest).Error()))
			continue
		}
		slog.InfoContext(ctx, "backup verify: signature valid",
			slog.String("manifest", rec.Path), slog.Int("sequence", i))
	}
	// Lineage catalog enumeration.
	if cat, err := lineage.ResolveLineage(ctx, store); err != nil {
		failed++
		slog.ErrorContext(ctx, "backup verify: cannot resolve lineage for signature check", slog.String("error", err.Error()))
	} else if err := lineage.VerifyLineage(ctx, store, cat, signer); err != nil {
		failed++
		slog.ErrorContext(ctx, "backup verify: lineage signature INVALID", slog.String("error", err.Error()))
	} else {
		slog.InfoContext(ctx, "backup verify: lineage signature valid")
	}
	return failed
}

// refuseUnsignableMaintenance implements the ADR-0154 Q4 refuse-or-resign
// gate shared by compact + prune: a signed chain being restructured
// without a signing key is refused loudly (never emit an unsigned
// successor to a signed chain). op names the maintenance verb for the
// error. A no-op on a dry-run or an unsigned chain.
func refuseUnsignableMaintenance(op string, signed, dryRun bool, signer *lineage.Signer) error {
	if signed && !dryRun && signer == nil {
		return lineage.SignatureMissingError(fmt.Errorf(
			"%s: chain is signed (ADR-0154) but no signing key was supplied — re-run with the chain's --encrypt key material so the restructured chain can be re-signed; refusing to leave a signed chain with stale/absent signatures", op,
		))
	}
	return nil
}

// resignIfSigned re-signs the whole (already-restructured) lineage when
// the chain is signed and a signer is available. A no-op otherwise.
func resignIfSigned(ctx context.Context, store irbackup.Store, signed bool, signer *lineage.Signer) error {
	if !signed || signer == nil {
		return nil
	}
	return lineage.ResignLineage(ctx, store, signer)
}

// healStaleLineageSignatures is the NO-OP-path companion to [resignIfSigned]
// (batch C, 2026-08-23 — the resignIfSigned crash window). Compact and prune
// both commit their catalog restructure FIRST and re-sign SECOND, so a crash
// between the two leaves a restructured chain under stale signatures — which
// then surfaces at verify as SIGNATURE-INVALID ("signed link count N !=
// actual M"), whose remedy text accuses tamper, while the MISSING-signature
// remedy says "re-run the maintenance step with the chain's key material".
// Before this helper existed that remedy was FALSE for the crash case: the
// re-run found nothing left to restructure and returned through a no-op door
// (compact's fewer-than-2-segments / no-merge-groups returns, prune's r0
// returns) BEFORE its resignIfSigned call, leaving the stale signatures in
// place forever. This helper runs at exactly those doors: verify the lineage
// signature against the CURRENT catalog and, only when it does not verify,
// re-sign the whole survivor set — loudly, via WARN, so the heal is never
// silent.
//
// Deliberate boundaries, each with its reason:
//
//   - dryRun or a nil signer → do nothing (a dry-run must not write, and
//     without key material we can neither verify nor sign — the keyless
//     no-op run keeps today's exit-0 behavior; the operator following the
//     published remedy supplies the key on the re-run, which is when the
//     heal fires).
//   - An unsigned chain → do nothing (nothing to heal).
//   - A VERIFYING signature → do nothing (routine no-op maintenance on a
//     healthy signed chain must not churn .sig objects).
//   - Re-sign authority is key possession, exactly as on the restructure
//     path: a compact/prune that DOES restructure already re-signs whatever
//     the store contains without pre-verifying, so healing on the no-op path
//     grants no authority the maintenance verb did not already have. The
//     WARN names the key id so a heal that re-keyed a chain is visible.
//   - The heal re-signs; it does NOT sweep. A crash that also skipped the
//     post-resign orphan sweep leaves uncatalogued files on disk — a
//     disk-leak-only concern tracked separately (the compact orphan-GC
//     backlog entry, which needs the concurrent-writer design call).
func healStaleLineageSignatures(ctx context.Context, store irbackup.Store, cat *lineage.Catalog, op string, dryRun bool, signer *lineage.Signer) error {
	if dryRun || signer == nil {
		return nil
	}
	signed, err := lineage.ChainIsSigned(ctx, store)
	if err != nil {
		return fmt.Errorf("%s: probe signed chain: %w", op, err)
	}
	if !signed {
		return nil
	}
	verr := lineage.VerifyLineage(ctx, store, cat, signer)
	if verr == nil {
		return nil
	}
	slog.WarnContext(
		ctx, op+": the lineage signature does not verify on a no-op maintenance run — re-signing the chain in place. This is the expected recovery for a maintenance run that crashed between its catalog commit and its re-sign; if this chain should NOT have needed healing, treat the signature failure below as suspect before trusting the restored signatures",
		slog.String("verify_failure", verr.Error()),
		slog.String("signing_key_id", signer.KeyID),
	)
	if err := lineage.ResignLineage(ctx, store, signer); err != nil {
		return fmt.Errorf("%s: re-sign stale-signed chain: %w", op, err)
	}
	return nil
}

// unverifiableSignedArtifact reports a signed artifact that cannot be
// verified, distinguishing the two reasons it can happen — which the single
// message it replaces did not (audit 2026-08-01 Sec2).
//
// The two cases are operationally different and only one of them is ordinary:
//
//   - NO verification material was supplied at all. The operator did not ask
//     to verify anything; telling them to pass key material is correct advice.
//   - Material WAS supplied, and it cannot verify the scheme the chain
//     CLAIMS. The old message told this operator to "pass the chain's
//     --encrypt key material" — which they had already done — so the one
//     signal worth acting on was worded as though they had forgotten a step.
//
// The second case is what a SCHEME RELABEL looks like. The scheme token is
// folded into the signed canonical bytes, so an attacker cannot relabel a
// chain and still have it verify — but they do not need it to verify. Editing
// the scheme to one the operator holds no key for moves the chain from
// "verified" to "unverifiable", and unverifiable is a WARN that proceeds. One
// field edit, every .sig left in place, exit 0.
//
// This does not refuse by default, because sluice cannot tell a relabel from
// an operator who legitimately holds only the encryption KEK for a chain
// signed with an asymmetric key — both present as "material supplied, wrong
// family". What it can do is stop giving advice the operator has already
// followed, name the claimed scheme against what was actually supplied, and
// say plainly that a relabel produces exactly this. --require-signature turns
// it into a refusal, and the message says so.
func unverifiableSignedArtifact(
	ctx context.Context,
	what string,
	claimedScheme string,
	mat verifyMaterial,
	requireStrict bool,
) error {
	haveMaterial := mat.env != nil || mat.verifyPub != nil

	if requireStrict {
		if haveMaterial && claimedScheme != "" {
			return lineage.SignatureMissingError(fmt.Errorf(
				"%s requires a verified signature (--require-signature is set): the backup claims signature "+
					"scheme %q, and the key material supplied (%s) cannot verify that scheme. Either supply a "+
					"key for %q, or treat this as tampering — editing the recorded scheme to one you hold no "+
					"key for is how a signed chain is downgraded to an unverified one",
				what, claimedScheme, describeVerifyMaterial(mat), claimedScheme,
			))
		}
		return lineage.SignatureMissingError(fmt.Errorf(
			"%s requires a verified signature (--require-signature is set) but no verification key is available",
			what,
		))
	}

	if haveMaterial && claimedScheme != "" {
		slog.WarnContext(ctx,
			"restore: the backup claims a signature scheme the supplied key material CANNOT verify, so its "+
				"signature was not checked; proceeding. You already supplied key material — this is not a "+
				"missing-key warning. Either the chain is signed with a different key type than you passed, "+
				"or its recorded scheme was edited, which is how a signed chain is downgraded to an "+
				"unverified one with every signature file left in place. Re-run with --require-signature to "+
				"refuse instead of proceeding",
			slog.String("what", what),
			slog.String("claimed_scheme", claimedScheme),
			slog.String("supplied_material", describeVerifyMaterial(mat)))
		return nil
	}

	slog.WarnContext(ctx,
		"restore: backup asserts a signature but no verification key is available to check it; proceeding (pass the chain's --encrypt key material to verify, or --require-signature to refuse)",
		slog.String("what", what))
	return nil
}

// describeVerifyMaterial names what the operator actually supplied, so the
// unverifiable message can contrast it with the claimed scheme instead of
// assuming nothing was supplied.
func describeVerifyMaterial(mat verifyMaterial) string {
	switch {
	case mat.env != nil && mat.verifyPub != nil:
		return "an encryption KEK and a public verify key"
	case mat.env != nil:
		return "an encryption KEK only (verifies hmac-kek signatures)"
	case mat.verifyPub != nil:
		return "a public verify key only (verifies asymmetric signatures)"
	default:
		return "none"
	}
}
