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
	"fmt"
	"log/slog"

	"sluicesync.dev/sluice/internal/crypto"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

// signatureVerifier derives the ADR-0154 verify key from env, or reports
// that verification is impossible with this envelope. ok is false when
// env is nil or cannot key an HMAC off its KEK (KMS — Phase 3). A non-nil
// error is a real derivation failure.
func signatureVerifier(env crypto.EnvelopeEncryption) (s *lineage.Signer, ok bool, err error) {
	if env == nil {
		return nil, false, nil
	}
	return lineage.NewSigner(env)
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
	env crypto.EnvelopeEncryption,
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
	signer, ok, err := signatureVerifier(env)
	if err != nil {
		return err
	}
	if !ok {
		return unverifiableSignedManifest(ctx, manifestPath, requireStrict)
	}
	if err := lineage.VerifyManifest(ctx, segStore, manifestPath, manifest, seq, signer); err != nil {
		return lineage.CodeForSignatureError(err)
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
	env crypto.EnvelopeEncryption,
	requireStrict bool,
) error {
	hasArtifacts, err := chainHasSignatureArtifacts(ctx, rootStore, links)
	if err != nil {
		return fmt.Errorf("chain restore: probe signature artifacts: %w", err)
	}
	if !requireStrict && !hasArtifacts {
		return nil
	}
	signer, ok, err := signatureVerifier(env)
	if err != nil {
		return err
	}
	if !ok {
		return unverifiableSignedManifest(ctx, "chain", requireStrict)
	}
	for i := range links {
		link := &links[i]
		segStore := link.Segment.Store(rootStore)
		if err := lineage.VerifyManifest(ctx, segStore, link.Path, link.Manifest, i, signer); err != nil {
			return lineage.CodeForSignatureError(err)
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
	signer, ok, err := signatureVerifier(opts.Envelope)
	if err != nil {
		slog.ErrorContext(ctx, "backup verify: cannot derive verify key", slog.String("error", err.Error()))
		return 1
	}
	if !ok {
		if opts.RequireSignature {
			slog.ErrorContext(ctx, "backup verify: signed chain but no verification key supplied and --require-signature set")
			return 1
		}
		slog.WarnContext(ctx, "backup verify: chain is signed but no verification key supplied — signatures are present-but-unverified. Re-run with --encrypt + the chain's passphrase to verify.")
		return 0
	}
	failed := 0
	for i := range records {
		rec := &records[i]
		segStore := rec.Segment.Store(store)
		if err := lineage.VerifyManifest(ctx, segStore, rec.Path, rec.Manifest, i, signer); err != nil {
			failed++
			slog.ErrorContext(ctx, "backup verify: signature INVALID",
				slog.String("manifest", rec.Path), slog.Int("sequence", i), slog.String("error", err.Error()))
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

// unverifiableSignedManifest is the WARN-or-refuse branch for a v6
// manifest the caller cannot check (no KEK-holding verify key). The DR
// default is warn-and-proceed; RequireSignature makes it a refusal.
func unverifiableSignedManifest(ctx context.Context, what string, requireStrict bool) error {
	if requireStrict {
		return lineage.SignatureMissingError(fmt.Errorf(
			"%s asserts a signature (FormatVersion %d) but no verification key is available and --require-signature is set",
			what, irbackup.FormatVersionSignedManifest,
		))
	}
	slog.WarnContext(ctx,
		"restore: backup asserts a signature but no verification key is available to check it; proceeding (pass the chain's --encrypt key material to verify, or --require-signature to refuse)",
		slog.String("what", what))
	return nil
}
