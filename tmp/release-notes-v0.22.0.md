# sluice v0.22.0

Logical backups Phase 6.1 lands. **Client-side passphrase-mode encryption** for backup chunks. The headline operator outcome: **chunks land in cloud storage as ciphertext, not plaintext.** Even an attacker with full read access to the bucket can't recover the underlying database rows. Closes the v0.16.0 / v0.17.2 release-notes-disclosed gap that sluice currently writes plaintext chunks; unlocks compliance-driven adoption (HIPAA, PCI-DSS, SOC 2 Type II, GDPR with customer-controlled keys) + air-gapped DR workflows where bucket-SSE doesn't follow the bytes. Implementation supplement: `docs/dev/design-logical-backups-phase-6.md`.

## Features

- **`sluice backup full --encrypt --encryption-passphrase-env=SLUICE_PASS ...`** — and the same flag family on `backup incremental`, `backup stream run`, `restore`, `sync from-backup run`. Operator passes a passphrase; sluice derives a Key Encryption Key via Argon2id (NIST-recommended starting params: 64 MiB memory, 3 iterations, 4 parallelism), generates a 256-bit Content Encryption Key, AES-256-GCM-encrypts every chunk under the CEK, wraps the CEK with the KEK, records the wrapped CEK + Argon2id params on the chain manifest. Restore re-derives the KEK from the operator's passphrase + the recorded salt, unwraps the CEK, decrypts every chunk on the fly. Three passphrase sources (mutually exclusive): `--encryption-passphrase=<value>` (NOT recommended for production), `--encryption-passphrase-env=<VAR>`, `--encryption-passphrase-file=<path>`.

- **Per-chain CEK as the default; per-chunk opt-in.** Per-chain wraps a single CEK at chain root; every chunk reuses the same CEK with its own random 12-byte nonce. Argon2id (the expensive op) runs **once per restore**, not once per chunk — a 1000-chunk chain decrypts at AES-GCM speed (GB/s) after a single ~tens-of-ms Argon2id derive. Per-chunk mode (`--encrypt-mode=per-chunk`) wraps a fresh CEK per chunk for defense-in-depth at the cost of per-chunk Argon2id derives.

- **Loud failure on key mismatch.** Encrypted chain restored without `--encrypt` → refusal at chain-walk time naming `algorithm` / `kek_mode` / `kek_ref` so operators know exactly what they need. Wrong passphrase → AES-GCM auth-tag-mismatch error before any data lands on the target. No partial data lands on the target on either failure mode.

- **`backup verify` runs without the passphrase.** SHA-256 verification covers ciphertext bytes (post-encryption), so cron-probe verification of archived encrypted backups doesn't need the passphrase distributed to the verification host. Operators wanting a deeper "decrypt + re-hash plaintext" check can wait for `--decrypt-verify` (planned, not in v1).

- **Mixed-mode chain refusal.** A chain's encryption shape is uniform — all encrypted or all plaintext. Attempting to extend a plaintext chain with encrypted incrementals (or vice versa) is rejected at chain-restore time. Prevents a foot-gun where a `backup stream run --encrypt` got toggled between rollovers.

- **`internal/crypto/envelope.go` — pluggable `EnvelopeEncryption` interface.** Phase 6.1 ships `PassphraseEnvelope`; Phase 6.2 (AWS KMS) and Phase 6.3 (GCP Cloud KMS / Azure Key Vault) plug in behind the same interface so the chunk writer/reader paths don't change when those land.

- **`docs/operator/encryption.md`** — operator-facing guide. Best practices for passphrase storage (1Password, AWS Secrets Manager, GCP Secret Manager, env-injection patterns), the "lose the passphrase = lose the data" warning, recovery posture (lost passphrase / wrong passphrase / forgot --encrypt / mixed-mode chain rejection), passphrase rotation workflow.

## Use cases this unlocks

| Scenario | Before v0.22.0 | With v0.22.0 |
|---|---|---|
| **HIPAA / PCI / SOC 2 audit on backup at-rest** | Operator relies on bucket-SSE + filesystem-FDE; some auditors require sluice-managed encryption with customer-controlled keys. | `--encrypt --encryption-passphrase-env=` produces ciphertext chunks. Operator owns the passphrase. |
| **Air-gapped / SneakerNet DR** | Backup bytes on a USB drive in transit are plaintext; bucket-SSE doesn't follow. | Encryption travels with the bytes. Drive theft → still ciphertext. |
| **Untrusted-storage replication** | Operator must trust the cloud provider's encryption story. | Even a malicious-insider read of the bucket recovers nothing without the operator's passphrase. |
| **Ransomware recovery from a remote read-replica bucket** | Ransomware that exfiltrates the read-replica bucket can read every row. | Ransomware sees ciphertext; row data stays safe (assuming the passphrase isn't on the same compromised host). |

## Compatibility

- **No CLI breaking changes.** `--encrypt` is opt-in; existing backup / restore / sync from-backup invocations without it continue to write / read plaintext chunks identically.
- **Pre-v0.22.0 chains restore unchanged.** Plaintext chains stay plaintext on restore; the manifest's `chain_encryption` field is absent (omitempty); the chunk reader takes the existing plaintext path.
- **Manifest schema additive.** New encryption fields use `omitempty` so older sluice readers (v0.21.x and earlier) ignore them gracefully on a plaintext manifest. **An older sluice cannot restore a v0.22.0 encrypted chain** (no decryption code path); the manifest is human-readable enough that operators can recognize the `chain_encryption` field's presence and upgrade.
- **`backup verify` continues to work without keys.** Existing cron probes that hash chunks need no changes; encrypted chunks hash ciphertext, which matches what's recorded in the manifest.
- **No new heavy dependencies.** AES-GCM uses stdlib `crypto/aes` + `crypto/cipher`; Argon2id uses `golang.org/x/crypto/argon2` (already an indirect dependency in v0.21.x; now promoted to direct).

## Operator notes

- **Passphrase storage is operator-critical.** Lose the passphrase, lose the data. **Store the passphrase in a password manager / secrets vault BEFORE creating the first encrypted chain.** Document which passphrase decrypts which chain alongside the backup retention policy. The `docs/operator/encryption.md` guide covers integrations with 1Password CLI, AWS Secrets Manager, and env-injection patterns.

- **Use `--encryption-passphrase-env` or `--encryption-passphrase-file` in production.** The inline `--encryption-passphrase=<value>` form puts the passphrase in shell history; only use it for one-off testing. The env / file forms keep the passphrase out of `ps` output too.

- **`per-chain` is the right default.** Argon2id at default params is ~tens of milliseconds; running it once per restore (per-chain mode) is invisible overhead. Per-chunk mode runs Argon2id per chunk, which on a 1000-chunk chain costs ~30s of CPU during restore — only worthwhile if the threat model genuinely demands chunk-isolated CEKs (e.g. defense-in-depth against per-chunk leaks of an in-memory CEK during a single restore process).

- **`--encrypt` on a `backup stream run` is uniform across the stream's lifetime.** The stream's first rollover stamps the chain's encryption shape; subsequent rollovers must match. If you forget to set `--encrypt` on the first rollover, the chain is plaintext for its lifetime; restart with `--encrypt` and a fresh destination to rotate to encrypted.

- **`backup verify` against an encrypted chain succeeds without the passphrase.** Cron-probe verification doesn't need the key distributed to the probe host. The check is sha256-only; a future `--decrypt-verify` flag (Phase 6.2+) will add a deeper "decrypt + re-hash plaintext" mode.

- **Argon2id default params are conservative for 2026 hardware.** Operators concerned about brute-force can raise via the recorded params (future `--argon2-memory=128M` flag); chains record the actual params used so older chains stay decryptable when defaults rotate forward. The Phase 6 design doc covers the threat model in detail.

## What's next

- **Phase 6.2 — AWS KMS.** `--kms-key-arn=arn:aws:kms:...` flag; AES-256-GCM bulk cipher unchanged; AWS KMS replaces Argon2id for KEK derivation. Per-chain CEK cache reduces KMS Decrypt calls to one per restore. Targets the BYOK + audit-trail use cases that passphrase mode can't fully address.
- **Phase 6.3 — GCP Cloud KMS + Azure Key Vault.** Same shape as 6.2; per-cloud SDK wrappers behind the `EnvelopeEncryption` interface.
- **`--decrypt-verify` for `backup verify`.** Deeper verification mode that decrypts every chunk + re-hashes plaintext, in addition to the sha256-of-ciphertext check.
- **Passphrase rotation tooling.** v0.22.0's rotation workflow is "fresh full + new chain"; future tooling could re-encrypt existing chunks under a new passphrase without retaking the source backup.
