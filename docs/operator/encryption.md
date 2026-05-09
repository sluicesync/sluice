# Backup encryption

`sluice` v0.22.0 introduced client-side envelope encryption for logical backup chunks (passphrase mode); v0.23.0 adds AWS KMS-backed encryption alongside it. Once enabled, chunks land on storage as AES-256-GCM ciphertext; only an operator with the right key material (passphrase or KMS-rooted IAM access) can recover the underlying rows. This guide covers the operator-facing shape: when to enable it, which mode fits which workload, and the recovery posture.

## What encryption protects against

- **Storage-provider compromise** (cloud breach, subpoena, malicious insider with bucket read access). Ciphertext is unreadable without the operator's passphrase.
- **Bucket misconfiguration leakage** (accidentally-public bucket, lost-credential exposure).
- **Air-gapped / SneakerNet exposure** (backups on physical media in transit). Encryption travels with the bytes; bucket-SSE doesn't.
- **Compliance posture** (HIPAA / PCI / SOC 2 / GDPR-with-customer-controlled-keys auditors who require demonstrable customer-controlled-key encryption).

## What encryption does NOT protect against

See `docs/dev/design-logical-backups-phase-6.md` for the full threat model. Briefly:

- In-flight network traffic between sluice and source/target databases (use TLS at the connection layer).
- Compromise of the host running sluice while a backup is in flight.
- Loss of the operator's passphrase. **Lose the passphrase → permanently lose the data.** Treat the passphrase like a database root password: store it in a password manager / secrets vault BEFORE creating the first encrypted chain.

## Enabling encryption

Add `--encrypt` plus a passphrase source to `sluice backup full` / `backup incremental` / `backup stream run` / `restore` / `sync from-backup run`.

Three passphrase sources (mutually exclusive — pick one):

1. `--encryption-passphrase=<value>` (NOT recommended for production — passphrase appears in shell history)
2. `--encryption-passphrase-env=<VAR>` (read from environment variable)
3. `--encryption-passphrase-file=<path>` (read from file; trailing newline trimmed)

### Example: backup full with env-var passphrase

```bash
export SLUICE_BACKUP_PASS="$(op read 'op://Production/sluice-backup/passphrase')"
sluice backup full \
    --source-driver=postgres \
    --source="$DATABASE_URL" \
    --target=s3://my-bucket/backups/2026-05-09/ \
    --encrypt \
    --encryption-passphrase-env=SLUICE_BACKUP_PASS
```

### Example: restore with file-based passphrase

```bash
# 1Password CLI dumps to a temp file; read-once and discard.
PASSFILE=$(mktemp)
trap "rm -f $PASSFILE" EXIT
op read 'op://Production/sluice-backup/passphrase' > "$PASSFILE"

sluice restore \
    --target-driver=postgres \
    --target="$TARGET_DSN" \
    --from=s3://my-bucket/backups/2026-05-09/ \
    --encrypt \
    --encryption-passphrase-file="$PASSFILE"
```

### Example: AWS Secrets Manager

```bash
SLUICE_BACKUP_PASS="$(aws secretsmanager get-secret-value \
    --secret-id sluice/backup-passphrase \
    --query SecretString --output text)"
export SLUICE_BACKUP_PASS

sluice backup stream run \
    --source-driver=postgres \
    --source="$DATABASE_URL" \
    --target=s3://my-bucket/streams/prod/ \
    --encrypt \
    --encryption-passphrase-env=SLUICE_BACKUP_PASS
```

## Encryption modes

`--encrypt-mode` controls the per-chain vs per-chunk shape:

- `per-chain` (default) — single CEK wrapped at the chain root; every chunk uses the same CEK with its own random nonce. Argon2id KEK derivation runs **once per restore** (~tens of ms with default params); subsequent chunks decrypt at AES-GCM speed (GB/s). Recommended for almost all workloads.
- `per-chunk` — fresh CEK per chunk; each chunk's manifest entry carries its own wrapped CEK. Defense-in-depth: a chunk-level compromise can't decrypt other chunks. Costs one Argon2id derive per chunk on restore; a 1000-chunk chain pays ~30s of Argon2id at default params (~30ms × 1000). Opt in only when the threat model genuinely demands it.

## Argon2id parameters

The Phase 6.1 default is the NIST-recommended Argon2id starting point:

- Memory: 64 MiB
- Iterations: 3
- Parallelism: 4
- Key length: 32 bytes (AES-256)

These are recorded in the manifest's `ChainEncryption.Argon2id` field, so restore-side derivation matches the backup's. Future revisions may rotate the defaults forward; older chains stay decryptable because the manifest records the actual params used.

## Backup verification without the passphrase

`sluice backup verify` runs sha256-only integrity checks against ciphertext bytes. **No passphrase is needed.** Run it from a cron probe to confirm archived backups stay intact without distributing the encryption key to the verification host.

```bash
sluice backup verify --from=s3://my-bucket/backups/2026-05-09/
# Verifies every chunk's SHA-256 against the manifest.
```

A future enhancement (`--decrypt-verify`) will offer a deeper "decrypt + re-hash plaintext" check; v0.22.0 ships sha256-only.

## Recovery posture

### Lost the passphrase

**The chain is unrecoverable.** AES-256-GCM has no backdoor; Argon2id is computationally infeasible to brute-force at default params for a non-trivial passphrase. Treat lost-passphrase as equivalent to deleted-data.

**Mitigation.** Store the passphrase in a password manager (1Password, Bitwarden, AWS Secrets Manager, GCP Secret Manager, Azure Key Vault) BEFORE creating the first encrypted chain. Document the recovery procedure (which passphrase decrypts which chain) alongside the backup retention policy.

### Wrong passphrase on restore

`sluice restore --encrypt --encryption-passphrase-env=...` with a wrong passphrase fails fast at chain-walk time with an auth-tag-mismatch error — no partial data lands on the target. Operator can re-attempt with the correct key.

### Forgot `--encrypt` on restore

`sluice restore` against an encrypted chain without `--encrypt` refuses with a clear error naming the chain's KEKMode and KEKRef. The error message tells the operator exactly what they need to supply.

### Mixed-mode chain rejection

A chain's encryption shape is **uniform across the chain** — all encrypted or all plaintext. Attempting to extend a plaintext chain with encrypted incrementals (or vice versa) is refused at chain-restore time. This prevents foot-guns where a stream's `--encrypt` got toggled between rollovers.

## Passphrase rotation

Phase 6.1 passphrase rotation is a **fresh-chain workflow**:

1. Take a fresh full backup with the new passphrase, into a new destination directory.
2. Verify the new chain restores cleanly into a test target.
3. Update your backup tooling to point at the new destination + new passphrase.
4. Retain the old chain (decryptable with the old passphrase) for as long as your retention policy demands.

Re-encrypting existing chunks under a new passphrase is out of scope for Phase 6.1; operator can rotate forward by starting a new chain.

(KMS-mode key rotation rotates transparently via cloud-provider mechanisms — see "Key rotation" below.)

## AWS KMS setup (Phase 6.2)

v0.23.0 adds AWS KMS-backed envelope encryption alongside passphrase mode. The trade-offs:

| Dimension | Passphrase mode (Phase 6.1) | AWS KMS mode (Phase 6.2) |
|---|---|---|
| Key material | Operator-controlled passphrase | KMS-managed key (IAM-rooted access) |
| Audit trail | None — whoever has the file has access | KMS CloudTrail logs every Encrypt/Decrypt call with the principal |
| Rotation | Fresh-chain workflow | KMS rotates the root key transparently; old chains stay decryptable |
| Lost key | Permanent data loss | Permanent data loss (operator must not delete the KMS key while chains exist) |
| Best for | Air-gapped / SneakerNet / non-AWS deployments | AWS-resident workloads, multi-tenant BYOK, compliance audit trails |

### Enabling KMS mode

```bash
sluice backup full \
    --source-driver=postgres \
    --source="$DATABASE_URL" \
    --target=s3://my-bucket/backups/2026-05-09/ \
    --encrypt \
    --kms-key-arn=arn:aws:kms:us-east-1:123456789012:key/abcd1234-...
```

Restore mirrors the path:

```bash
sluice restore \
    --target-driver=postgres \
    --target="$TARGET_DSN" \
    --from=s3://my-bucket/backups/2026-05-09/ \
    --encrypt \
    --kms-key-arn=arn:aws:kms:us-east-1:123456789012:key/abcd1234-...
```

The `--kms-key-arn` and `--encryption-passphrase{,-env,-file}` flag families are **mutually exclusive** — pick one mode per chain. Cross-region restore (e.g. backup in us-east-1, restore from a runner in eu-west-1) needs `--kms-region=us-east-1` to pin the region explicitly; the default AWS config chain picks the runner's region.

### Acceptable key reference shapes

`--kms-key-arn` accepts any of the standard AWS KMS key references:

- **Full ARN** — `arn:aws:kms:us-east-1:123456789012:key/abcd1234-ef56-7890-...`
- **Alias ARN** — `arn:aws:kms:us-east-1:123456789012:alias/sluice-backup-prod`
- **Bare key ID** — `abcd1234-ef56-7890-...` (resolved via the SDK's configured region)
- **Alias name** — `alias/sluice-backup-prod` (same)

**Use an alias for production.** Rotating the underlying key is a single `aws kms update-alias` call without sluice restart; the chain's manifest records the alias literally and KMS resolves it on every restore.

### Creating a KMS key

```bash
# 1. Create the key.
aws kms create-key \
    --description "sluice backup encryption key" \
    --key-usage ENCRYPT_DECRYPT \
    --key-spec SYMMETRIC_DEFAULT \
    --query 'KeyMetadata.Arn' --output text
# arn:aws:kms:us-east-1:123456789012:key/abcd1234-...

# 2. Add a friendly alias.
aws kms create-alias \
    --alias-name alias/sluice-backup-prod \
    --target-key-id abcd1234-...

# 3. Enable automatic annual rotation (optional; recommended).
aws kms enable-key-rotation --key-id abcd1234-...
```

Console equivalent: AWS Console → KMS → Customer managed keys → Create key → Symmetric → ENCRYPT_DECRYPT → assign administrator/usage roles → Save.

### IAM policy template

Grant the role sluice runs as the minimum required actions on the specific key. Do NOT grant `kms:*` on `*`.

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "SluiceKMSEnvelope",
            "Effect": "Allow",
            "Action": [
                "kms:Encrypt",
                "kms:Decrypt",
                "kms:DescribeKey"
            ],
            "Resource": "arn:aws:kms:us-east-1:123456789012:key/abcd1234-..."
        }
    ]
}
```

`kms:DescribeKey` is needed for sluice's construction-time preflight (catches auth issues before the backup starts streaming rows). `kms:Encrypt` covers the backup path; `kms:Decrypt` covers restore. No `kms:GenerateDataKey` because sluice generates the CEK locally via `crypto/rand` and calls `Encrypt` to wrap it (this matters for operators auditing IAM grants — sluice doesn't use the GenerateDataKey API).

For BYOK / cross-account scenarios, the **key policy** on the key itself must also grant the principal access — IAM and key policies are AND-gated. See AWS's [Key policies](https://docs.aws.amazon.com/kms/latest/developerguide/key-policies.html) docs.

### Key rotation

KMS handles key rotation transparently. Two rotation modes:

1. **Automatic annual rotation** (`aws kms enable-key-rotation`). KMS rotates the key material yearly; wrapped CEKs reference the key ID and KMS resolves to whichever version was active when the wrap happened. Old chains stay decryptable indefinitely; new chains use the current version. **Recommended for most operators.**

2. **Manual rotation** (create a new key, update the alias, retire the old key). Useful for major rotations (compromise response, compliance milestone). **Do NOT delete the old key while chains wrapped under it still need to be restored.** KMS keys have a mandatory pending-deletion window (7-30 days); use it to verify no active chains reference the old key version before scheduling deletion.

Sluice doesn't expose a key-version selector — it always asks KMS to use whichever version the key alias / ID currently resolves to. KMS picks the right version from the wrapped CEK's metadata on Decrypt.

### KMS request charges

Per-chain CEK caching keeps charges flat against chain length:

- **Per backup**: 1 KMS Encrypt call (chain CEK wrap) + 1 DescribeKey call (preflight). ~$0.0000004 + ~$0.0000004 = ~$0.0000008.
- **Per restore**: 1 KMS Decrypt call (chain CEK unwrap) + 1 DescribeKey call (preflight). ~$0.0000008.
- **A 720-rollover monthly backup-stream**: 720 Encrypt + 720 DescribeKey calls = ~$0.000576/month in KMS charges.

Negligible against any practical backup workload. Per-chunk mode (`--encrypt-mode=per-chunk`) makes one Encrypt call per chunk, which on a 1000-chunk chain is ~$0.0004 — still negligible, but disables the per-chain caching benefit on restore (1000 Decrypts instead of 1).

### Wrong-key / missing-key recovery

Restoring an encrypted chain without `--kms-key-arn` (or with the wrong key ARN) refuses with operator-actionable errors:

- **Missing key**: "encrypted chain (kek_mode=\"aws-kms\" kek_ref=\"arn:aws:kms:...\") requires --encrypt + a KMS reference; no key was supplied". Operator-actionable: pass `--kms-key-arn` matching the chain's recorded ARN.
- **Wrong key**: "kms decrypt rejected: ciphertext was wrapped under a different key (chain manifest's KEKRef does not match the supplied --kms-key-arn)". Operator-actionable: verify the ARN matches what the manifest's `chain_encryption.kek_ref` records.
- **Access denied**: "kms decrypt denied: AWS IAM principal lacks kms:Decrypt on key ... (verify key policy + role policy grants the action)". Operator-actionable: extend the IAM grant.
- **Key disabled / pending deletion**: "kms decrypt rejected: key ... is in an invalid state (verify the key is enabled and not pending deletion)". Operator-actionable: re-enable the key in the KMS console.

### What NOT to do with KMS mode

- **Don't delete a KMS key while chains wrapped under it still need to be restored.** KMS deletion is irreversible after the pending-deletion window expires; the data becomes unrecoverable.
- **Don't grant `kms:*` on `*`.** Use the least-privilege IAM template above.
- **Don't share KMS keys across unrelated chains.** Per-customer / per-environment keys are the right pattern; one compromised key shouldn't expose every other tenant's backups.

## See also

- `docs/dev/design-logical-backups-phase-6.md` — full design + threat model
- `docs/dev/design-logical-backups.md` — original logical-backup proto-ADR
- `docs/postgres-source-prep.md` — TLS at the database-connection layer
