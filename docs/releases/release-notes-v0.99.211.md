# sluice v0.99.211

**Signing robustness follow-up (ADR-0154 Phase 3a) — when this build meets a backup signature it doesn't recognize (a future scheme family, or a KMS algorithm it can't verify), it now says "upgrade sluice" (`SLUICE-E-BACKUP-SIGNATURE-UNSUPPORTED`) instead of the alarming "your backup is tampered" (`-INVALID`). It's still fail-closed — an unverifiable signature is refused — but a forward-incompatibility is no longer mistaken for an attack. Purely a correctness/UX improvement on an error edge; no signing, verification, or restore behavior changes for any recognized backup, and there is no silent-loss fix to re-verify.**

## Changed

- **An unrecognized signature scheme or KMS algorithm fails as "upgrade sluice", not "tamper".** The "a newer sluice wrote this, so upgrade rather than distrust the backup" treatment that already covered a newer canonicalization version now also covers an unrecognized signature scheme FAMILY (e.g. a hypothetical future post-quantum scheme) and a `kms/<algorithm>` whose algorithm this build cannot verify. Previously both fell through to a known verification primitive and failed the MAC as `SLUICE-E-BACKUP-SIGNATURE-INVALID` — the "your backup is compromised" signal — when the real situation is that a newer sluice wrote a signature format this build does not understand. The verifier now recognizes the unknown scheme up front (`crypto.IsSupportedKMSAlgorithm` gates the kms algorithm; an unknown family hits an explicit arm) and refuses with the new coded class `SLUICE-E-BACKUP-SIGNATURE-UNSUPPORTED` (fail-closed refusal, exit 3, "upgrade sluice" remedy) instead of `-INVALID`. It remains fail-closed — this build will not restore or verify a signature it cannot check — but it no longer accuses a merely-newer backup of being tampered. The empty-scheme probe path (`--require-signature` on an unsigned or fully-stripped chain) is split into its own branch so it keeps reporting the precise `SLUICE-E-BACKUP-SIGNATURE-MISSING`. Pinned by `TestVerifyChainSignatures_UnknownSchemeUpgrade` (unknown family + unknown kms algorithm) and `TestIsSupportedKMSAlgorithm`.

## Fixed

- **Downgrade-oracle hardening: a v3→v2 canon-version relabel can never verify (test-only pin).** The dual-version verifier recomputes each signature at its OWN recorded canonicalization version. A store-write adversary relabeling a canon-v3 signature — whose MAC covers bytes that INCLUDE the scheme token — down to canon v2, which recomputes WITHOUT the scheme token, produces different bytes and fails. This was already true by construction (the twin of the existing v2→ed25519 relabel refusal); v0.99.211 adds the regression pin (`TestVerifyManifest_V3RelabelToV2Refused`, hmac + ed25519 arms) so the dual-version path can never silently become a downgrade oracle. No behavior change.

## Compatibility

**No breaking changes; no opt-in required; every recognized backup verifies and restores exactly as on v0.99.210.** The only behavior change is on an error edge: a signature this build cannot recognize now surfaces as `SLUICE-E-BACKUP-SIGNATURE-UNSUPPORTED` (fail-closed, "upgrade sluice") rather than `SLUICE-E-BACKUP-SIGNATURE-INVALID`. Because no released sluice has ever written an unrecognized scheme, this only affects a *future* forward-incompatibility (a newer sluice's backup read by an older one) — it makes that case read correctly. All Phase-1 (HMAC), Phase-2 (Ed25519), and Phase-3a (KMS) signed chains, and all plaintext / pre-v6 backups, verify and restore byte-identically. One new coded error, `SLUICE-E-BACKUP-SIGNATURE-UNSUPPORTED` (documented in `docs/operator/error-codes.md`).

## Who needs this — action required

- **Everyone: nothing to do.** This is a correctness/UX refinement on a signature-verification error path. No signing, verification, restore, migrate, or sync behavior changes for any backup sluice can recognize; no data is affected; there is no silent-loss fix to re-verify. It simply ensures that if a future sluice ever writes a signature format this build doesn't understand, the refusal reads as "upgrade sluice" rather than "your backup is tampered."

---

**Install:** brew install sluicesync/tap/sluice · go install sluicesync.dev/sluice/cmd/sluice@v0.99.211 · **Container:** ghcr.io/sluicesync/sluice:0.99.211
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
