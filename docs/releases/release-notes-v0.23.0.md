# sluice v0.23.0

Logical backups Phase 6.2 lands. **AWS KMS-backed envelope encryption** for backup chunks. Operators who already manage encryption keys via AWS KMS — the common compliance posture for HIPAA / PCI / SOC 2 shops, and the common BYOK posture for multi-tenant SaaS — can hand sluice a key ARN and skip the passphrase plumbing entirely. The manifest's per-chain Content Encryption Key is wrapped via `kms.Encrypt`; restore unwraps via `kms.Decrypt` once at the start and caches the CEK in-memory for the rest of the chain. Phase 6.1 (passphrase mode) keeps working unchanged; the two modes are mutually exclusive per backup but pluggable behind the same `EnvelopeEncryption` interface. Implementation supplement: `docs/dev/design-logical-backups-phase-6.md`. Operator guide: `docs/operator/encryption.md` ("AWS KMS setup" section).

## Features

- **`sluice backup full --encrypt --kms-key-arn=arn:aws:kms:us-east-1:1:key/abc ...`** — and the same flag family on `backup incremental`, `backup stream run`, `restore`, `sync from-backup run`. Operator passes a KMS key ARN (or alias ARN, or alias/<name>); sluice loads the default AWS config (env vars / IAM role / profile / SSO), pre-flights the key with a `DescribeKey` call (auth/region/key-not-found errors surface at construction time, not mid-backup), then wraps every chain's CEK via `kms.Encrypt`. Restore mirrors the path: build the envelope, unwrap once, decrypt every chunk with the cached CEK.

- **`--kms-region` for explicit region override.** Defaults to `AWS_REGION` env var or the SDK's default region resolution. Useful for cross-region restore (decrypt a us-east-1-wrapped chain from a runner in eu-west-1 with the role's region pinned explicitly).

- **Per-chain CEK caching.** A 100-chunk chain restore makes ≤1 KMS Decrypt call regardless of chunk count. KMS API charges stay flat against chain length — a 10000-chunk monthly backup chain costs the same KMS request count as a 10-chunk daily one. Verified via stubbed-client call counter; pinned in the unit tests.

- **Operator-actionable KMS error translation.** `AccessDeniedException` surfaces as "AWS IAM principal lacks kms:Encrypt/Decrypt/DescribeKey on the key (verify key policy + role policy grants the action)"; `NotFoundException` as "key not found (verify the ARN/alias)"; `KMSInvalidStateException` and `DisabledException` as state-specific recovery hints; `IncorrectKeyException` as "ciphertext was wrapped under a different key" (the wrong-key-on-restore path). Generic SDK errors fall through with the key ARN preserved for support correlation against KMS request logs.

- **Mutually exclusive with passphrase mode.** Backup paths refuse `--kms-key-arn` together with any of `--encryption-passphrase{,-env,-file}` so operators can't accidentally double-key a chain. The chain's recorded `KEKMode` (passphrase-argon2id vs aws-kms) drives restore-side validation; a wrong-mode envelope refuses with a clear "envelope mode does not match chain's kek_mode" message before any unwrap is attempted.

- **`docs/operator/encryption.md` "AWS KMS setup" section.** IAM policy template (least-privilege grant of kms:Encrypt + kms:Decrypt + kms:DescribeKey on a specific key ARN), key creation walkthrough (CLI + console), key alias usage, key-rotation handling (KMS rotates the root key transparently; wrapped CEKs reference the key ID — old chains stay decryptable through KMS's version chain).

## Use cases this unlocks

| Scenario | Before v0.23.0 | With v0.23.0 |
|---|---|---|
| **AWS-resident workload, KMS-managed keys** | Passphrase-mode encryption requires a separate secrets-management story (1Password / AWS Secrets Manager). Two key-management surfaces to audit. | `--kms-key-arn` reuses the existing KMS key; one audit surface. IAM controls who can decrypt. |
| **BYOK / multi-tenant SaaS** | Customer-managed-keys story requires distributing per-customer passphrases to the backup runner. | Per-customer KMS keys + per-customer IAM roles; sluice never holds the underlying key material. |
| **Compliance audit trail** | Passphrase access is opaque (whoever has the file has access; no log trail). | KMS CloudTrail logs every Encrypt/Decrypt call with the principal — auditors can see exactly who decrypted which chain when. |
| **Key rotation** | Passphrase rotation is a fresh-chain workflow. | KMS automatic key rotation transparently rotates the root key; old chains stay decryptable; new chains use the current version. |

## Compatibility

- **No format changes.** Manifest schema, control-table schema, change-chunk format, CLI surface — all unchanged. v0.22.x chains taken under passphrase mode restore unchanged under v0.23.0. KMS-mode chains are taken with v0.23.0+ binaries; pre-v0.23.0 binaries refuse them at preflight (the chain root's `KEKMode = "aws-kms"` doesn't match a `PassphraseEnvelope.Mode()`, surfaces as a clear refusal naming the kek_mode).

- **No CLI breaking changes.** All existing `sluice` subcommands keep their flag surfaces verbatim. The new `--kms-key-arn` and `--kms-region` flags are additive.

- **AWS SDK pulled in via `github.com/aws/aws-sdk-go-v2/service/kms`.** Already an indirect dependency for the S3 backup target; v0.23.0 promotes it to direct. Build size change is negligible (KMS service module is ~200 KB compiled).

- **Phase 6.1 passphrase mode unchanged.** All v0.22.x integration tests still pass; the passphrase + KMS modes share the `EnvelopeEncryption` seam without coupling.

## Operator notes

- **IAM policy grants the operator role kms:Encrypt + kms:Decrypt + kms:DescribeKey on the specific key ARN.** Least-privilege; do NOT grant `kms:*` on `*`. The encryption.md guide ships a copy-paste-ready policy template. `kms:DescribeKey` is needed for the construction-time preflight (catches auth issues before the backup starts streaming rows).

- **Use a key alias for production deployments.** `--kms-key-arn=alias/sluice-backup-prod` resolves to whichever key the alias currently points at; rotating the underlying key is a single `aws kms update-alias` call, no sluice restart needed. The chain's manifest records the alias literally; KMS resolves it on every restore.

- **KMS request charges are bounded by chain count, not chunk count.** Per-chain CEK caching means each restore makes one KMS Decrypt call; each backup makes one Encrypt at chain start. A monthly backup-stream that produces 720 hourly rollovers (one per hour) makes 720 Encrypt calls + 1 Decrypt per restore = ~$0.0022 in KMS charges/month. Negligible against any practical backup workload.

- **Cross-region restore: pass `--kms-region` to pin the region.** The default AWS config chain picks the region from the runner's environment; if the runner's region differs from the key's region, the SDK errors with "could not resolve region". `--kms-region=us-east-1` pins explicitly.

- **Passphrase mode is unchanged.** Existing operators using `--encryption-passphrase{,-env,-file}` need no migration; chains stay decryptable with the same flags. Switching to KMS mode is a fresh-chain workflow (take a new full with `--kms-key-arn`; old passphrase chains stay accessible with the original flags).

## Test coverage

- Unit tests for the KMS envelope (round-trip, wrong-key, missing-key, access-denied, disabled, invalid-state, generic SDK error fall-through, length validation, per-chain caching pattern).
- Pipeline-level integration tests (manifest stamping, restore preflight, mode-mismatch refusal, per-chain caching across the chunk-CEK resolver).
- CLI flag tests (KMS-vs-passphrase mutual exclusion, KMS-without-encrypt sanity check, encrypt-without-any-key shape).
- A `kmsverify` build-tag harness skeleton ships in `internal/pipeline/backup_kms_localstack_integration_test.go` for operators who want to verify against localstack KMS locally; the main `integration` build tag stays focused on real-database scenarios so CI throughput doesn't regress on the localstack pull/boot cost.

## What's next

- **Phase 6.3 — GCP Cloud KMS + Azure Key Vault.** Same shape as 6.2; per-cloud SDK wrappers behind the `EnvelopeEncryption` interface. `--kms-key-resource=projects/.../cryptoKeys/...` and `--azure-key-vault-id=https://....vault.azure.net/keys/...`.
- **Key-management ADR.** Document the design rationale + threat-model recap + recovery posture across all three cloud providers.
- **`--decrypt-verify` for `backup verify`.** Deeper verification mode that decrypts every chunk + re-hashes plaintext, in addition to the sha256-of-ciphertext check.

## Who needs this

- AWS-resident operators with HIPAA / PCI / SOC 2 / GDPR-with-customer-controlled-keys audit obligations.
- Multi-tenant SaaS shops with per-customer BYOK requirements.
- Operators preferring KMS's IAM-rooted access control + CloudTrail audit log over passphrase-as-secret.
- Anyone running sluice on AWS who wants encryption without managing a separate passphrase secrets store.
