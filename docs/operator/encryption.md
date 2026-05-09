# Backup encryption (Phase 6.1 — passphrase mode)

`sluice` v0.22.0 introduces client-side envelope encryption for logical backup chunks. Once enabled, chunks land on storage as AES-256-GCM ciphertext; only an operator with the right passphrase can recover the underlying rows. This guide covers the operator-facing shape: when to enable it, how to manage passphrases safely, and the recovery posture.

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

(KMS-mode key rotation, coming in Phase 6.2/6.3, will rotate transparently via cloud-provider mechanisms — wrapped CEKs reference the key alias; KMS handles the version-chain.)

## See also

- `docs/dev/design-logical-backups-phase-6.md` — full design + threat model
- `docs/dev/design-logical-backups.md` — original logical-backup proto-ADR
- `docs/postgres-source-prep.md` — TLS at the database-connection layer
