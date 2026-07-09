# sluice v0.99.213

**Diagnostic fix (LOW, no behavior change) — a GCP KMS `--sign-key`/`--verify-key` reference that names a bare crypto-key instead of a specific `.../cryptoKeyVersions/N` now reports that precise error even on a host without GCP credentials, instead of masking it behind "default credentials not found". Loud either way; no signing, verification, or restore behavior changes. Found by the v0.99.212 post-release regression cycle.**

## Fixed

- **GCP KMS versioned-resource refusal fires before the credential lookup (Bug 181).** GCP Cloud KMS signs with a specific key version, so `--sign-key kms://gcp/<resource>` (and the matching `--verify-key`) requires a versioned `.../cryptoKeys/KEY/cryptoKeyVersions/N` resource and refuses a bare crypto-key. That check is purely syntactic, but it was ordered *after* the KMS client was constructed — and client construction performs the Application Default Credentials lookup. So on a host with no GCP credentials configured, the credential lookup failed first and the operator got an opaque "could not find default credentials" instead of the precise, actionable "must be a versioned CryptoKeyVersion resource (.../cryptoKeyVersions/N)". The check is now hoisted into a named `requireGCPVersionedResource` helper, called at the top of both `NewGCPKMSSigner` and `FetchGCPKMSPublicKey` before any client build or network access, with a defense-in-depth call retained at the point of use — matching the sibling parse-time refusals (unknown provider, malformed `kms://` reference) that already fire pre-network. This is loud either way; it only improves *which* loud message you get when a malformed GCP reference and absent credentials coexist. Pinned by a test that exercises the live credential path (no injected client — the pre-existing versioned-refusal test injected a fake, skipping the credential lookup, which is why it could not catch the ordering).

## Compatibility

**No behavior change.** No signing, verification, restore, migrate, or sync path changes. This affects only the *diagnostic message* an operator sees when they pass a malformed GCP `kms://` reference on a host without GCP credentials — the operation is refused loudly in both the old and new code, only the error text differs. Every signed backup (HMAC / Ed25519 / AWS / GCP / Azure KMS) and every plaintext / pre-v6 backup verifies and restores exactly as on v0.99.212.

## Who needs this — action required

- **Nobody needs to act.** This is a CLI-preflight diagnostic refinement. If you use GCP KMS signing, a malformed key reference now gives you a clearer error on an unconfigured host; everything else is identical to v0.99.212.

---

**Install:** brew install sluicesync/tap/sluice · go install sluicesync.dev/sluice/cmd/sluice@v0.99.213 · **Container:** ghcr.io/sluicesync/sluice:0.99.213
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
