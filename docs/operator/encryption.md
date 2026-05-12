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

## GCP Cloud KMS setup (Phase 6.3)

v0.34.0 adds GCP Cloud KMS-backed envelope encryption with the same shape as AWS KMS. Same `EnvelopeEncryption` interface; only the per-cloud RPCs and IAM model differ.

### Enabling GCP KMS mode

```bash
sluice backup full \
    --source-driver=postgres \
    --source="$DATABASE_URL" \
    --target=gs://my-bucket/backups/2026-05-09/ \
    --encrypt \
    --gcp-kms-key-resource=projects/my-project/locations/us/keyRings/sluice/cryptoKeys/backup-prod
```

Restore mirrors the path with the same `--gcp-kms-key-resource` flag. The flag is mutually exclusive with `--encryption-passphrase{,-env,-file}`, `--kms-key-arn`, and `--azure-key-vault-id`.

### Acceptable key reference shapes

`--gcp-kms-key-resource` accepts either of Cloud KMS's canonical resource forms:

- **Crypto-key resource** — `projects/PROJECT/locations/LOCATION/keyRings/RING/cryptoKeys/KEY` (recommended; KMS picks the primary version on wrap, recovers the version from ciphertext metadata on unwrap)
- **Versioned crypto-key resource** — `projects/.../cryptoKeys/KEY/cryptoKeyVersions/3` (pins a specific version; useful when re-wrapping a chain to migrate off a deprecated version)

### Creating a Cloud KMS key

```bash
# 1. Create the key ring (one per project / per environment).
gcloud kms keyrings create sluice --location=us --project=my-project

# 2. Create the crypto-key for envelope encryption.
gcloud kms keys create backup-prod \
    --location=us \
    --keyring=sluice \
    --purpose=encryption \
    --project=my-project

# 3. (Optional) Enable automatic rotation.
gcloud kms keys update backup-prod \
    --location=us \
    --keyring=sluice \
    --rotation-period=90d \
    --next-rotation-time=$(date -d '+90 days' --iso-8601=seconds) \
    --project=my-project
```

### IAM policy template

Grant the service account sluice runs as the minimum-privilege roles on the specific crypto-key. Do NOT grant `roles/cloudkms.admin`.

```bash
# Encrypter — needed by sluice backup full / incremental / stream run.
gcloud kms keys add-iam-policy-binding backup-prod \
    --location=us --keyring=sluice --project=my-project \
    --member=serviceAccount:sluice@my-project.iam.gserviceaccount.com \
    --role=roles/cloudkms.cryptoKeyEncrypter

# Decrypter — needed by sluice restore.
gcloud kms keys add-iam-policy-binding backup-prod \
    --location=us --keyring=sluice --project=my-project \
    --member=serviceAccount:sluice@my-project.iam.gserviceaccount.com \
    --role=roles/cloudkms.cryptoKeyDecrypter

# Viewer — needed for sluice's preflight GetCryptoKey call.
gcloud kms keys add-iam-policy-binding backup-prod \
    --location=us --keyring=sluice --project=my-project \
    --member=serviceAccount:sluice@my-project.iam.gserviceaccount.com \
    --role=roles/cloudkms.viewer
```

For separation of duties, run backup and restore under different service accounts and grant only Encrypter to the backup principal, only Decrypter to the restore principal.

### Authentication

The Cloud KMS client uses Application Default Credentials. Three common shapes:

1. **`gcloud auth application-default login`** — operator-laptop usage; credentials cached at `~/.config/gcloud/application_default_credentials.json`.
2. **`GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json`** — service-account JSON key file; common for CI / Cloud Run / cron.
3. **Workload Identity / metadata server** — on GKE / Cloud Run / Compute Engine, no explicit credential file needed; the platform provides credentials automatically.

Sluice surfaces "no valid credentials available" with a `gcloud auth application-default login` hint if ADC is unconfigured.

### Key rotation

Cloud KMS handles rotation transparently. Two modes:

1. **Automatic rotation** (set via `--rotation-period`). KMS rotates the key material on the configured cadence; wrapped CEKs reference the resource name and KMS resolves to whichever version was primary at wrap time. Old chains stay decryptable indefinitely; new chains use the current version.
2. **Manual rotation** (`gcloud kms keys versions create`). Useful for compromise response. **Do NOT destroy old key versions while chains wrapped under them need to be restored.** Cloud KMS versions have a 24-hour minimum destroy delay; use it to verify no active chains reference the old version.

### KMS request charges

Cloud KMS pricing (~$0.03 per 10,000 calls) makes per-chain costs negligible:

- **Per backup**: 1 Encrypt + 1 GetCryptoKey = ~$0.000006.
- **Per restore**: 1 Decrypt + 1 GetCryptoKey = ~$0.000006.
- **A 720-rollover monthly backup-stream**: ~720 Encrypt + 720 GetCryptoKey calls = ~$0.004/month.

### Wrong-key / missing-key recovery

Cloud KMS surfaces gRPC status codes that sluice translates to operator-actionable errors:

- **NotFound**: "gcp kms decrypt failed: key X not found (verify the resource name is correct + the service account has access)". Recovery: check the resource name and IAM grants.
- **PermissionDenied**: "gcp kms decrypt denied: service account lacks the required IAM role on key X (grant roles/cloudkms.cryptoKeyDecrypter)". Recovery: extend the IAM grant.
- **FailedPrecondition**: "gcp kms decrypt rejected: key X is in an invalid state (verify the key is enabled and the primary version is not disabled)". Recovery: re-enable the key version.
- **Unauthenticated**: "gcp kms decrypt denied: no valid credentials available (ensure GOOGLE_APPLICATION_CREDENTIALS is set or run `gcloud auth application-default login`)". Recovery: configure ADC.

### What NOT to do with GCP KMS mode

- **Don't destroy old crypto-key versions while chains wrapped under them still need restoring.** Cloud KMS destruction is irreversible after the destroy delay; data becomes unrecoverable.
- **Don't grant `roles/cloudkms.admin` to the runtime service account.** Use the three minimum-privilege roles above (Encrypter / Decrypter / Viewer).
- **Don't share crypto-keys across unrelated environments.** Per-environment crypto-keys (or even per-tenant) limit blast radius.

## Azure Key Vault setup (Phase 6.3)

v0.34.0 also adds Azure Key Vault-backed envelope encryption. Same operator surface; key identifier is a URL rather than an ARN / resource path.

### Enabling Azure Key Vault mode

```bash
sluice backup full \
    --source-driver=postgres \
    --source="$DATABASE_URL" \
    --target=azblob://my-container/backups/2026-05-09/ \
    --encrypt \
    --azure-key-vault-id=https://my-vault.vault.azure.net/keys/backup-prod
```

Restore mirrors the path. Like the other KMS flags, `--azure-key-vault-id` is mutually exclusive with the other three key-source families.

### Acceptable key reference shapes

`--azure-key-vault-id` accepts either of Key Vault's standard key identifier URL forms:

- **Latest version** — `https://VAULT.vault.azure.net/keys/KEY` (Key Vault uses the current version on wrap; on unwrap the version is recovered from the wrapped blob's metadata)
- **Versioned** — `https://VAULT.vault.azure.net/keys/KEY/abc123def456` (pins a specific version)
- **Managed HSM** — `https://VAULT.managedhsm.azure.net/keys/KEY[/VERSION]` (FIPS 140-3 Level 3 HSM tier; same URL structure)

### Creating a Key Vault key

```bash
# 1. Create the Key Vault (one per environment).
az keyvault create \
    --name sluice-prod \
    --resource-group my-rg \
    --location eastus \
    --enable-rbac-authorization

# 2. Create the key for envelope encryption.
#    RSA 4096-bit, used for RSA-OAEP-256 wrap (sluice's default).
az keyvault key create \
    --vault-name sluice-prod \
    --name backup-prod \
    --kty RSA \
    --size 4096 \
    --ops wrapKey unwrapKey

# 3. (Optional) Configure rotation policy.
az keyvault key rotation-policy update \
    --vault-name sluice-prod \
    --name backup-prod \
    --value '{"lifetimeActions": [{"action": {"type": "Rotate"}, "trigger": {"timeAfterCreate": "P90D"}}]}'
```

For HSM-backed AES keys (Managed HSM only):

```bash
az keyvault key create \
    --hsm-name my-managed-hsm \
    --name backup-prod \
    --kty oct-HSM \
    --size 256 \
    --ops wrapKey unwrapKey
```

Pass `--azure-wrap-algorithm=A256KW` to sluice when using an AES-typed key (the default RSA-OAEP-256 won't apply).

### Role assignment template

Azure Key Vault has two access models: legacy access policies and modern RBAC. **Use RBAC** (`--enable-rbac-authorization` above). Minimum-privilege role assignment for a service principal:

```bash
# Crypto User — covers WrapKey / UnwrapKey / GetKey.
az role assignment create \
    --role "Key Vault Crypto User" \
    --assignee <SERVICE_PRINCIPAL_OBJECT_ID> \
    --scope /subscriptions/<SUB>/resourceGroups/my-rg/providers/Microsoft.KeyVault/vaults/sluice-prod
```

For separation of duties, assign "Key Vault Crypto User" to the backup principal and "Key Vault Crypto Service Encryption User" (read-only on the key) to a separate restore principal. (Azure doesn't have a built-in role with only-Decrypt; "Crypto User" is the closest minimum-privilege role that covers both wrap and unwrap.)

### Authentication

Sluice uses `DefaultAzureCredential` which probes a chain of credential sources:

1. **Environment variables** — `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET` (service principal) or `AZURE_USERNAME` / `AZURE_PASSWORD` (user).
2. **Workload Identity** — on AKS pods configured with workload-identity federation, no explicit credential needed.
3. **Managed Identity** — on Azure VMs / App Service / Container Instances, the platform's IMDS endpoint provides credentials automatically.
4. **Azure CLI cached login** — `az login` for laptop usage; works for the operator interactively.

Sluice surfaces "no valid credentials" with an `az login` / managed-identity hint when authentication fails.

### Key rotation

Key Vault rotates transparently with `az keyvault key rotation-policy`. Same shape as the other clouds: wrapped CEKs reference the key name (not the version explicitly); on unwrap, Key Vault recovers the version from the wrapped blob's metadata. Old chains stay decryptable.

Manual rotation: `az keyvault key rotate` creates a new version; Key Vault preserves old versions until explicitly purged (soft-delete protected for the configured retention period). **Don't purge old versions while chains need restoring.**

### Wrap algorithm choice

The `--azure-wrap-algorithm` flag controls which Key Vault algorithm is used:

| Algorithm | Key type | When to use |
|---|---|---|
| `RSA-OAEP-256` (default) | RSA (vault or HSM) | The standard for software-protected RSA keys; works for HSM-backed RSA too. |
| `RSA-OAEP` | RSA (vault or HSM) | Legacy; only use if a compliance baseline requires it. |
| `A256KW` | AES-256 (Managed HSM only) | Required for HSM-backed AES keys; sluice's default doesn't apply. |

If you pass the wrong algorithm for the key type, Key Vault rejects with `BadParameter`; sluice translates this to "verify the wrap algorithm matches the key type."

### Wrong-key / missing-key recovery

Key Vault surfaces error codes that sluice translates to operator-actionable errors:

- **KeyNotFound** (404): "azure kms decrypt failed: key X not found (verify the key identifier URL + the role assignment grants 'Key Vault Crypto User' or equivalent)". Recovery: verify URL + role.
- **Forbidden** (403): "azure kms decrypt denied: principal lacks the required role on key X (grant 'Key Vault Crypto User')". Recovery: extend the role assignment.
- **BadParameter** (400): "azure kms decrypt rejected: bad parameter for key X (verify the wrap algorithm matches the key type — RSA keys default to RSA-OAEP-256; AES keys need --azure-wrap-algorithm=A256KW)". Recovery: align the algorithm.
- **KeyDisabled**: "azure kms decrypt rejected: key X is disabled (re-enable via `az keyvault key set-attributes --enabled true`)". Recovery: re-enable the key.
- **401 status fallback**: "azure kms decrypt denied: no valid credentials (run `az login` or set AZURE_CLIENT_ID/AZURE_TENANT_ID/AZURE_CLIENT_SECRET)". Recovery: configure credential chain.

### What NOT to do with Azure Key Vault mode

- **Don't purge old key versions while chains wrapped under them need restoring.** Soft-delete keeps purged keys recoverable for the retention period, but operators have been known to bypass it.
- **Don't disable a key while operations against it might be in-flight.** Disabled keys break in-progress backup / restore with `KeyDisabled`.
- **Don't use access policies (legacy) on a new vault.** RBAC is the supported model; access policies are being deprecated.
- **Don't share vaults across unrelated environments.** Per-environment vaults isolate blast radius and align with Azure's billing / quota boundaries.

## Choosing between providers

| Dimension | Passphrase | AWS KMS | GCP Cloud KMS | Azure Key Vault |
|---|---|---|---|---|
| **Trust anchor** | Operator-managed | AWS IAM | GCP IAM | Azure RBAC |
| **Cloud lock-in** | None | AWS-resident | GCP-resident | Azure-resident |
| **Best for** | Air-gapped, SneakerNet, multi-cloud | AWS-resident workloads | GCP-resident workloads | Azure-resident workloads |
| **Audit trail** | None (file access only) | CloudTrail | Cloud Logging | Key Vault Logs |
| **Compliance certs** | None inherent | FIPS 140-2 L3 (with HSM tier) | FIPS 140-2 L3 (with HSM tier) | FIPS 140-2 L3 / 140-3 L3 (HSM) |
| **HSM option** | n/a | CloudHSM-backed key spec | HSM key spec | Managed HSM (`managedhsm.azure.net`) |
| **Cross-cloud restore** | Yes (operator carries passphrase) | No (key tied to AWS account) | No (tied to GCP project) | No (tied to Azure tenant) |

Most operators pick one cloud and stay there; passphrase mode is the right answer when sluice is run from outside any of the three (CI runners with no cloud identity, air-gapped recovery hosts, SneakerNet workflows).

## See also

- `docs/dev/design-logical-backups-phase-6.md` — full design + threat model
- `docs/dev/adr-0037-key-management.md` — design rationale, threat-model recap, per-provider trade-offs
- `docs/dev/design-logical-backups.md` — original logical-backup proto-ADR
- `docs/postgres-source-prep.md` — TLS at the database-connection layer
