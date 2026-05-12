# ADR-0037: Key Management for Logical Backups (Phase 6)

**Status:** Accepted.
**Date:** 2026-05-11 (post-shipping summary; design captured retrospectively).
**Scope:** Sub-phases 6.1–6.3 of the logical-backup encryption track. Sub-phase 6.4 documentation lands alongside this ADR.

## Context

Phase 1–5 of the logical-backup track shipped chunk + manifest + chain-restore + chain handoff with **plaintext chunks on disk / object storage**. Operators relying on bucket-level SSE or filesystem-level FDE got an at-rest protection layer they didn't manage, but compliance postures requiring **customer-managed key material**, BYOK, audit trails, or HSM-rooted keys had no native sluice surface. Phase 6 closes that gap with client-side envelope encryption, sub-phased across four key-source providers:

- **Passphrase mode** (Phase 6.1, v0.22.0) — operator-managed; no cloud dependency.
- **AWS KMS** (Phase 6.2, v0.23.0) — IAM-rooted key, CloudTrail audit.
- **GCP Cloud KMS** (Phase 6.3, v0.34.0) — GCP IAM-rooted, Cloud Logging audit.
- **Azure Key Vault** (Phase 6.3, v0.34.0) — Azure RBAC-rooted, Key Vault Logs audit.

This ADR captures the design decisions that span all four providers, the per-provider trade-offs, and the rationale for the choices that aren't obvious from reading the code.

## Decision summary

1. **Standard envelope encryption with AES-256-GCM** as the bulk cipher. Per-chain CEK is default; per-chunk CEK is opt-in.
2. **One [`EnvelopeEncryption`](../../internal/crypto/envelope.go) interface** abstracts CEK wrap/unwrap. Each provider implements it; the chunk writer/reader code is provider-agnostic.
3. **Construction-time preflight on every provider.** Backup paths surface auth / not-found / access-denied errors before streaming begins, not mid-chunk.
4. **Per-chain CEK as default.** Single KEK derive (or single KMS Decrypt) per restore; subsequent chunks decrypt at AES-GCM speed. Per-chunk CEK is opt-in defense-in-depth.
5. **Loud-failure on key shape mismatch.** Mixed-mode chains (passphrase root + KMS incremental, or one KMS provider's root + another's incremental) refuse at chain-walk time. Encryption is per-chain, not per-chunk.
6. **Manifest carries `KEKMode` + `KEKRef`** so restore can surface "supply the right key" errors that name the chain's mode and reference operator-actionably.
7. **Cross-cloud restore is intentionally not supported.** A chain wrapped under an AWS KMS key can only be restored with that AWS KMS key; cross-cloud portability would require either re-wrapping the chain (out of scope) or operator-managed passphrase mode (already the answer for cross-cloud workflows).

## Cryptographic shape

- **Bulk cipher**: AES-256-GCM with 12-byte random nonce per chunk + 16-byte authentication tag. NIST SP 800-38D's recommended shape. Birthday-bound at ~2^48 chunks per CEK — beyond any realistic chain.
- **CEK length**: 32 bytes (AES-256 key). Generated via `crypto/rand` at chain start.
- **KEK derivation**:
  - **Passphrase mode**: Argon2id with NIST-recommended defaults (memory=64 MiB, iterations=3, parallelism=4, key length=32 bytes). Per-chain salt recorded in the manifest; old chains stay decryptable when defaults rotate forward.
  - **KMS modes**: KEK is the cloud-provider-managed root key. Sluice never sees the KEK material; it routes the CEK plaintext through the provider's Wrap/Unwrap RPC and stores the opaque wrapped bytes in the manifest.
- **Wrapped CEK on disk**: provider-opaque. For passphrase mode it's `[nonce (12B) | ciphertext (32B) | authtag (16B)]` = 60 bytes. For AWS / GCP / Azure it's whatever the provider's wrap operation returns (typically a few hundred bytes carrying the provider's own framing + key version + integrity check).

## Why per-chain CEK as default

The default `--encrypt-mode=per-chain` wraps one CEK at the chain root and reuses it across every chunk in the chain. Each chunk gets its own random AES-GCM nonce; the CEK rotation point is chain-level, not chunk-level.

Trade-offs vs `--encrypt-mode=per-chunk`:

| Dimension | per-chain (default) | per-chunk (opt-in) |
|---|---|---|
| KEK ops on backup | 1 wrap | N wraps (one per chunk) |
| KEK ops on restore | 1 unwrap | N unwraps |
| Restore latency overhead | One-time KMS roundtrip / Argon2id derive | N-times KMS roundtrip / Argon2id derive |
| Blast radius if CEK leaks | Whole chain readable | Single chunk readable |
| KMS request charges | ~$0.0000008 per restore | ~$0.0008 per 1000-chunk restore |

For passphrase mode the per-chunk cost is dominant — Argon2id at default params is ~30ms per derive; a 1000-chunk chain pays ~30s of Argon2id on restore. For KMS modes the per-chunk cost is API-call latency (~50-200ms per call) which serializes restore at chain-walk time.

**Per-chunk mode is opt-in only.** Operators with concrete defense-in-depth requirements (per-tenant chunk isolation, regulatory requirements about CEK rotation cadence) can pick it; for everyone else, per-chain is the right default.

## Why one `EnvelopeEncryption` interface

The interface lives at [`internal/crypto/envelope.go`](../../internal/crypto/envelope.go):

```go
type EnvelopeEncryption interface {
    WrapCEK(cek []byte) ([]byte, error)
    UnwrapCEK(wrapped []byte) ([]byte, error)
    Mode() string
}
```

Three methods. `Mode()` returns the `KEKMode` tag for the manifest. `WrapCEK` / `UnwrapCEK` are provider-opaque on both sides — the chunk writer/reader code doesn't care whether it's talking to Argon2id-derived AES-GCM, AWS KMS, GCP Cloud KMS, or Azure Key Vault.

Benefits:

- **Chunk writer/reader paths are stable across phases.** Phase 6.1 → 6.2 → 6.3 didn't touch `internal/pipeline/backup_chunk.go`'s encryption gate. Adding Phase 6.4 (new provider, e.g. HashiCorp Vault) needs only a new file in `internal/crypto/`.
- **Per-provider preflight stays at the provider.** Each provider's `New*Envelope` constructor pre-flights its own auth + key existence (AWS `DescribeKey`, GCP `GetCryptoKey`, Azure `GetKey`). The orchestrator's `pipeline.BackupEncryption.Envelope` field just holds whichever provider the operator picked.
- **Tests are provider-narrow.** Stub `EnvelopeEncryption` for orchestrator tests; stub each provider's narrow API interface (`KMSAPI`, `GCPKMSAPI`, `AzureKMSAPI`) for provider-specific tests.

## Construction-time preflight

Every provider's constructor pre-flights the key reference:

- **AWS**: `kms.DescribeKey(KeyId: keyARN)` — verifies access + key exists.
- **GCP**: `kmsClient.GetCryptoKey(Name: cryptoKeyForResource(keyResource))` — same.
- **Azure**: `azkeys.Client.GetKey(name, version, nil)` — same.
- **Passphrase**: eager Argon2id derive at `NewPassphraseEnvelope` time — surfaces a typo passphrase before the chain writer or reader has done any work.

**Rationale:** a backup that's already streamed half its rows shouldn't fail at the first chunk flush because the operator's IAM role lacks `kms:Encrypt`. The preflight cost is one cheap API call (~free at cloud KMS pricing tiers, ~tens of ms for Argon2id) and the right place to fail.

The preflight is gated behind an internal `skipPreflight` option that tests use to stub the client without asserting a Get/Describe call. **Operators shouldn't bypass preflight in production.**

## Per-provider trade-offs

(Captured in detail in [`docs/operator/encryption.md`](../operator/encryption.md) § "Choosing between providers". Summary table here for ADR completeness.)

| Dimension | Passphrase | AWS KMS | GCP Cloud KMS | Azure Key Vault |
|---|---|---|---|---|
| Trust anchor | Operator-managed | AWS IAM | GCP IAM | Azure RBAC |
| Audit trail | None (file access only) | CloudTrail | Cloud Logging | Key Vault Logs |
| Auth chain | Passphrase string (env / file) | AWS SDK default chain | Application Default Credentials | DefaultAzureCredential |
| HSM option | n/a | CloudHSM-backed key spec | HSM key spec | Managed HSM tier |
| Key reference shape | n/a (passphrase + per-chain salt) | ARN / alias / bare key ID | Resource name | Vault key identifier URL |
| Per-restore unwrap cost | ~30ms Argon2id | ~50-200ms KMS Decrypt | ~50-200ms Cloud KMS Decrypt | ~100-300ms WrapKey/UnwrapKey roundtrip |
| Mutual exclusion | All four families pairwise mutually exclusive at flag-parse time |

## Per-provider minimum-privilege IAM

The operator guide carries gcloud / aws / az command examples. Roles in summary:

- **AWS**: `kms:Encrypt`, `kms:Decrypt`, `kms:DescribeKey` on the specific key resource. **No `kms:GenerateDataKey`** — sluice generates the CEK locally via `crypto/rand` and calls `Encrypt` to wrap it (operators auditing IAM grants will notice the missing action).
- **GCP**: `roles/cloudkms.cryptoKeyEncrypter` (backup) + `roles/cloudkms.cryptoKeyDecrypter` (restore) + `roles/cloudkms.viewer` (preflight). Separation of duties: split between backup and restore principals.
- **Azure**: `Key Vault Crypto User` covers WrapKey / UnwrapKey / GetKey. (Azure doesn't have a built-in role with only-Decrypt; Crypto User is the minimum-privilege role that covers both wrap and unwrap.)

## Cross-cloud restore is intentionally not supported

A chain wrapped under an AWS KMS key cannot be restored from a runner that only has GCP credentials (or vice versa). The wrapped CEK in the manifest references the provider's key shape; the matching provider must be available to unwrap it.

**Operators who need cross-cloud portability:** use passphrase mode. The passphrase travels with the operator (or with the secrets-management system), not with the cloud identity.

A future ADR could revisit this if a real operator surfaces a "I have backups wrapped in AWS KMS but need to restore from a GCP runner" workflow. The technical shape would be: dual-provider restore where sluice pulls the wrapped CEK metadata, calls a sidecar tool (operator-supplied) to unwrap, and passes the plaintext CEK back to sluice. Out of scope until demand surfaces.

## What this ADR does NOT cover

- **Key rotation runbook.** Captured in [`docs/operator/encryption.md`](../operator/encryption.md) per provider; not duplicated here.
- **CLI flag UX details.** Captured in `cmd/sluice/backup.go` flag help strings.
- **Cipher choice deliberation.** AES-256-GCM was chosen in Phase 6.1; revisiting (e.g. ChaCha20-Poly1305 for non-AESNI hardware) would warrant its own ADR.
- **Manifest format-version bump path.** Phase 6 added fields under a single format-version bump in v0.22.0; future cipher / KDF additions are additive and don't require a bump.

## Open follow-ups (Phase 6.4+)

- **Re-encryption-at-rest** for chains that need to migrate from passphrase to KMS (or between KMS providers). Out of scope for v1; operators rotate forward by starting a fresh chain.
- **`--decrypt-verify` mode** for `sluice backup verify`. Currently verify is sha256-only (no key needed). A deep-verify mode that decrypts + re-hashes plaintext would catch ciphertext-corruption that survives sha256 (extremely rare; sha256 covers everything). Demand-driven follow-on.
- **HashiCorp Vault Transit** as a fifth provider for operators outside the three hyperscalers. Same `EnvelopeEncryption` shape; ~250 LOC.
- **Per-tenant CEK scoping** for multi-tenant backups. Currently per-chain; a per-tenant-within-chain mode would let operators isolate blast radius on a shared chain. Demand-driven.

## See also

- [`docs/dev/design-logical-backups-phase-6.md`](design-logical-backups-phase-6.md) — full Phase 6 design + threat model
- [`docs/operator/encryption.md`](../operator/encryption.md) — operator-facing setup guide (passphrase + 3 KMS providers)
- [`internal/crypto/envelope.go`](../../internal/crypto/envelope.go) — `EnvelopeEncryption` interface + passphrase implementation
- [`internal/crypto/aws_kms.go`](../../internal/crypto/aws_kms.go) — AWS KMS implementation (Phase 6.2)
- [`internal/crypto/gcp_kms.go`](../../internal/crypto/gcp_kms.go) — GCP Cloud KMS implementation (Phase 6.3)
- [`internal/crypto/azure_kms.go`](../../internal/crypto/azure_kms.go) — Azure Key Vault implementation (Phase 6.3)
- CHANGELOG entries: [0.22.0] (Phase 6.1), [0.23.0] (Phase 6.2), [0.34.0] (Phase 6.3)
