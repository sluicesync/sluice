# Logical Backups Phase 6 — Implementation Design

Supplement to [`design-logical-backups.md`](design-logical-backups.md), [`design-logical-backups-phase-2.md`](design-logical-backups-phase-2.md) (which corrected the v0.16.0 release notes' claim that encryption was shipped — it wasn't), [`design-logical-backups-phase-3.md`](design-logical-backups-phase-3.md), [`design-logical-backups-phase-4.md`](design-logical-backups-phase-4.md), [`design-logical-backups-phase-4-5.md`](design-logical-backups-phase-4-5.md), and [`design-logical-backups-phase-5.md`](design-logical-backups-phase-5.md). This file covers Phase 6: **client-side at-rest encryption**, including KMS-backed key management.

The headline operator outcome: **chunks land in cloud storage as ciphertext, not plaintext.** Even an attacker with full read access to the bucket can't recover the underlying database rows. Closes the v0.16.0 + v0.17.2 release-notes-disclosed gap that sluice currently writes plaintext chunks; unlocks compliance-driven adoption (HIPAA, PCI-DSS, SOC 2 Type II, GDPR with customer-controlled keys) + air-gapped DR workflows where bucket-SSE doesn't follow the bytes.

## What's already in Phase 1-5 that this builds on

- **`BackupStore` interface** (Phase 1+) reads/writes raw byte streams; encryption sits at the layer above (chunk writer / chunk reader).
- **Manifest format** (Phase 1+) carries per-chunk `sha256` integrity. Phase 6 adds optional `encryption` metadata fields per chunk; the existing sha256 covers ciphertext bytes (so `backup verify` doesn't need decryption to check integrity).
- **Per-chunk independence** (Phase 1+ chunk shape) means encryption can be per-chunk without orchestrator-level rework. Each chunk gets its own Content Encryption Key (CEK).
- **Phase 3.1's incremental-chunk codec + Phase 4's stream rollover machinery** wrap the same chunk-write path; encryption hooks in once at the chunk-writer layer and inherits everywhere.

## Threat model + non-goals

### What Phase 6 protects against

1. **Storage-provider compromise**: cloud provider breach, subpoena, malicious insider with bucket read access. Phase 6's ciphertext is unreadable without operator-controlled keys.
2. **Bucket misconfiguration leakage**: accidentally-public bucket, lost-credential exposure. Ciphertext is still ciphertext.
3. **Air-gapped / SneakerNet exposure**: backups on physical media in transit. Encryption travels with the bytes; bucket-SSE doesn't.
4. **Compliance posture**: HIPAA / PCI / SOC 2 auditors who require demonstrable customer-controlled-key encryption. KMS-backed mode satisfies the "even AWS can't decrypt" audit cleanly.

### What Phase 6 does NOT protect against

1. **In-flight network compromise** between sluice and source/target databases. That's TLS at the database connection layer (sluice already supports `sslmode=verify-full` per `docs/postgres-source-prep.md`).
2. **Compromise of the host running sluice**. If an attacker has shell access on the sluice host while a backup is running, they can read the plaintext from sluice's memory — encryption is end-to-end from sluice to storage, not "secure against root on the same machine."
3. **Compromise of the operator's KMS credentials**. If your AWS access keys are stolen, the attacker can call KMS Decrypt on your wrapped CEKs. KMS access control + IAM policies + key rotation are the operator's responsibility.
4. **Compromise of sluice's source/target database credentials**. If the attacker has the DB password, they can read the source directly — sluice's ciphertext at rest doesn't matter at that point.
5. **Pre-Phase-6 backups**. Existing v0.21.x and earlier chains stay plaintext; Phase 6 is opt-in via `--encrypt` flag, and re-encryption-at-rest of old chunks is out of scope. Operators wanting encrypted DR should take a fresh full + start a new chain post-upgrade.
6. **Encrypted manifests.** The manifest carries the wrapped CEK + the sha256 of ciphertext per chunk; the manifest itself stays plaintext (so `backup verify` doesn't need decryption + so the chain structure is auditable without keys). Operators wanting "encrypt everything including manifests" have a future-phase option but it's not in 6.x scope.

## Design — envelope encryption with per-chain CEK (default) or per-chunk CEK (opt-in)

Standard envelope-encryption pattern, adapted to sluice's chunk model:

### Write path (per chunk)

1. **CEK lookup** (per-chain default): if this chain doesn't yet have a CEK, generate one via `crypto/rand.Read` (32 bytes for AES-256-GCM) and wrap with the KEK. Cache the wrapped CEK on the chain header. Per-chunk mode (opt-in via `--encrypt-mode=per-chunk`) generates a fresh CEK + wrap per chunk.
2. **Generate nonce**: `crypto/rand.Read` 12 bytes (96-bit nonce, NIST-recommended for AES-GCM). Per-chunk random nonce; never reuse.
3. **Encrypt chunk payload**: `AES-256-GCM(plaintext, CEK, nonce)` → ciphertext + 16-byte auth tag.
4. **Wrap CEK with KEK** (Key Encryption Key — operator-controlled):
   - **Passphrase mode** (Phase 6.1): KEK derived via `Argon2id(passphrase, salt)`. Salt is per-chain, stored in the chain's full manifest's `ChainEncryption.Argon2id` field.
   - **KMS mode** (Phase 6.2+): `KMS.Encrypt(CEK, kms_key_arn)` → wrapped CEK as opaque bytes (~200 bytes for AWS).
5. **Compose chunk file**: `[nonce (12B) | ciphertext | auth tag (16B)]` — concatenated, written to `BackupStore.Put`.
6. **Compute sha256 of the full chunk file** (post-encryption) for the manifest. This means `backup verify` doesn't need decryption.
7. **Update manifest's `ChunkInfo`** with new `Encryption` field:
   ```go
   type ChunkEncryption struct {
       Algorithm  string // "AES-256-GCM"
       NonceLen   int    // 12 (constant for v1; future-proof)
       AuthTagLen int    // 16 (constant for v1; future-proof)
       // CEK reference: empty string = use chain-level CEK from ChainEncryption;
       // non-empty = per-chunk CEK is base64-wrapped here
       WrappedCEK []byte
   }
   ```
8. **Per-chain encryption header** in the full manifest:
   ```go
   type ChainEncryption struct {
       Algorithm  string         // "AES-256-GCM"
       Mode       string         // "per-chain" | "per-chunk"
       KEKMode    string         // "passphrase-argon2id" | "aws-kms" | "gcp-kms" | "azure-keyvault"
       KEKRef     string         // for KMS modes: the key ARN/resource name; for passphrase: empty
       WrappedCEK []byte         // per-chain CEK wrap (when Mode == "per-chain")
       Argon2id   *Argon2idParams // when KEKMode == passphrase: salt + cost params
   }
   type Argon2idParams struct {
       Salt        []byte // base64 in JSON; 16 bytes
       Memory      uint32 // KiB; default 64 MiB = 65536
       Iterations  uint32 // default 3
       Parallelism uint8  // default 4
       KeyLen      uint32 // 32 (AES-256-GCM)
   }
   ```

### Read path (per chunk, on restore)

1. **Read chunk file** from BackupStore.
2. **Verify sha256** against manifest (existing path).
3. **Look up encryption metadata** in manifest:
   - If `ChunkEncryption` field absent AND `ChainEncryption` field absent: this is a pre-Phase-6 plaintext chunk. Decompress + parse as today.
   - If present: decrypt path.
4. **Unwrap CEK** (with caching during restore):
   - **Per-chain mode**: unwrap once at restore start; reuse for all chunks.
   - **Per-chunk mode**: unwrap per chunk (more KMS calls).
   - **Passphrase mode**: re-derive KEK via `Argon2id(passphrase, ChainEncryption.Argon2id.Salt)`; AES-decrypt the wrapped CEK with the KEK.
   - **KMS mode**: `KMS.Decrypt(WrappedCEK, KEKRef)` via the cloud-provider SDK.
5. **Parse chunk file**: split `[nonce (12B) | ciphertext | auth tag (16B)]`.
6. **Decrypt + verify**: `AES-256-GCM.Open(ciphertext, CEK, nonce)` — auth tag failure raises a clear error (chunk corrupted or tampered).
7. **Decompress + parse** as today (plaintext flow).

### Why per-chain CEK as default

Naive implementation calls `KMS.Decrypt` per chunk. For a 1000-chunk chain, that's 1000 KMS API calls × $0.03/10K = $0.003 per restore + N × ~50ms latency. Per-chain CEK reduces this to a single KMS Decrypt per restore (cached for the restore process's lifetime).

The chunk-level-compromise threat model is weak — most attackers either get bucket read access (and see all chunks) or don't. Per-chunk CEKs buy little against that. Operators wanting defense-in-depth opt in via `--encrypt-mode=per-chunk`.

### Key rotation

- **KMS mode**: KMS root keys rotate via cloud-provider mechanisms (AWS KMS automatic rotation, GCP KMS scheduled rotation, Azure Key Vault auto-rotation). Wrapped CEKs reference the key alias; KMS handles the version-chain lookup transparently. Old backups stay decryptable; new backups use the current key version.
- **Passphrase mode**: passphrase rotation requires re-encrypting every wrapped CEK (or every chunk if per-chunk CEKs). Out of scope for v1; operators rotate passphrase by taking a fresh full + starting a new chain.

## CLI surface

Three new flags on `sluice backup full` / `backup incremental` / `backup stream run`:

- `--encrypt`: opt-in flag. Default off (preserves existing behavior). When set, requires either `--encryption-passphrase*` or `--kms-key-arn`/`--kms-key-resource`/`--azure-key-vault-id` (whichever cloud).
- `--encryption-passphrase=<value>` OR `--encryption-passphrase-env=<VAR>` OR `--encryption-passphrase-file=<path>`: passphrase mode (Phase 6.1). Three input methods so operators can choose what fits their secrets-management story. **Document the env / file methods as preferred** — passphrase on the command line shows up in shell history.
- `--kms-key-arn=<arn>` (Phase 6.2 — AWS) / `--kms-key-resource=<resource>` (Phase 6.3 — GCP) / `--azure-key-vault-id=<id>` (Phase 6.3 — Azure): KMS mode.
- `--encrypt-mode=per-chain|per-chunk` (default `per-chain`): see "Why per-chain CEK as default" above.

`sluice restore` / `sync from-backup`: same flag families. **The operator must provide the right key**; restore detects encrypted chunks via manifest metadata and refuses with a clear error if the key is missing. Refusal message names the algorithm + KEKMode + KEKRef so the operator knows what's needed.

`sluice backup verify` runs without keys (sha256 verification doesn't need decryption); operators wanting deep verification (decrypt + re-hash plaintext) opt in via `--decrypt-verify` (future enhancement; not v1 scope).

## Sub-phasing

| Sub-phase | Scope | LOC est. |
|---|---|---|
| **6.1 — Passphrase mode (no cloud dependency)** | New `internal/crypto/envelope.go` with the AES-256-GCM + Argon2id machinery; manifest schema additions (`ChunkEncryption`, `ChainEncryption`, `Argon2idParams`); chunk writer + reader paths gated on `--encrypt`; CLI flag wiring; unit + integration tests covering encryption round-trip, wrong-passphrase refusal, missing-key refusal, plaintext-backward-compat. | 300-400 |
| **6.2 — AWS KMS** | `internal/crypto/aws_kms.go` wrapping `aws-sdk-go-v2/service/kms`; `--kms-key-arn` flag; per-chain CEK caching during restore; integration test against a localstack KMS container OR real AWS KMS via opt-in `kmsverify` build tag. | 250-350 |
| **6.3 — GCP Cloud KMS + Azure Key Vault** | Per-cloud SDK wrappers; `--kms-key-resource` / `--azure-key-vault-id` flags; integration tests via cloud emulators where available; same CEK-caching pattern. | 150-200 each |
| **6.4 — BYOK / imported-key documentation + ADR** | `docs/operator/encryption.md` operator guide (passphrase storage best practices, KMS key setup, key rotation runbook); `docs/dev/adr-XXXX-key-management.md` design rationale + threat-model recap; example KMS IAM policies. | 100-150 |
| **CI integration** | Local-FS encryption round-trip on every CI run (no KMS dependency); cloud-KMS variants gated behind `kmsverify` build tag (only run with explicit creds). | 200-300 |
| **Total Phase 6** | | ~1100-1700 |

## Acceptance criteria

A clean Phase 6 must:

1. **Passphrase round-trip**: `sluice backup full --encrypt --encryption-passphrase-env=SLUICE_PASS ...` produces a chain whose chunks are AES-256-GCM ciphertext (verified by inspecting bytes — no plaintext "INSERT" or table-name patterns visible). `sluice restore --encryption-passphrase-env=SLUICE_PASS ...` recovers the original data byte-for-byte.
2. **Wrong-passphrase refusal**: restore with the wrong passphrase fails with a clear error naming the auth-tag mismatch; no partial data lands on target.
3. **Missing-key refusal**: restore of an encrypted chain without `--encryption-passphrase` fails at chain-walk time with operator-actionable error citing the chain's KEKMode + KEKRef.
4. **AWS KMS round-trip**: same shape with `--kms-key-arn=arn:aws:kms:...`; verified against localstack KMS in CI + (optional) real AWS KMS via opt-in build tag.
5. **CEK caching reduces KMS calls**: a 100-chunk chain restore makes ≤ 1 KMS Decrypt call (per-chain CEK shared across chunks). Verified via test instrumenting the KMS SDK call count.
6. **Plaintext backward-compat**: pre-Phase-6 chains restore unchanged; manifest absent the `Encryption` field is treated as plaintext.
7. **Mixed-mode chain refusal**: a chain whose full is encrypted but an incremental isn't (or vice versa) is refused at chain-walk time with a clear error. Encryption is per-chain, not per-chunk; chains are atomic.
8. **`backup verify` doesn't need keys**: `sluice backup verify --from=<encrypted-chain-url>` succeeds without `--encryption-passphrase` (sha256 verification only).
9. **Stream + broker work with encryption**: `backup stream run --encrypt ...` produces encrypted incrementals; `sync from-backup run --encryption-passphrase-env=... ...` decrypts on the fly.
10. **Bug 41+42 UUID story unaffected**: encryption layer doesn't interact with value translation; cross-engine UUID restore continues to work post-Phase-6.

## Tenet check

- **IR-first.** Encryption sits below the chunk-writer/chunk-reader boundary; the IR is unchanged. Manifest gains optional fields; existing chains restore unchanged.
- **Contain Postgres complexity.** Encryption is engine-agnostic — same machinery for PG, MySQL, future engines.
- **Validate end-to-end.** Acceptance criteria 1 + 4 are the load-bearing round-trip integration tests.
- **Loud failure beats silent corruption.** Auth-tag mismatch on AES-GCM decrypt surfaces a clear error; missing-key refusal is explicit; mixed-mode chain refusal is loud.
- **Clean, elegant code.** New `internal/crypto/` package isolates the cryptographic primitives; chunk writer/reader gain ~one function call each (encrypt/decrypt branch on `Encryption` presence).

## Open design questions — resolved decisions

### Q1: Per-chunk CEK vs per-chain CEK?

**Decision: per-chain CEK as v1 default; `--encrypt-mode=per-chunk` opt-in.** Per-chain reduces KMS calls from O(N chunks) to O(1) per chain; the chunk-level-compromise threat model is weak because most attackers either get bucket read access (and see all chunks) or don't. Per-chunk for operators who want defense-in-depth.

### Q2: Plaintext-via-bucket-SSE vs Phase 6 client-side?

**Decision: keep both as supported options.** v0.21.x's existing "operator manages bucket-SSE" workflow continues to work (without `--encrypt`). Phase 6 adds `--encrypt` as opt-in client-side encryption. The two layer cleanly: an operator can use `--encrypt` AND have bucket-SSE enabled; the bytes are encrypted twice (once by sluice, once by S3), which is fine (slightly more compute on read but no functional issue).

### Q3: Manifest encryption?

**Decision: not v1.** The manifest contains chunk paths, sha256s, and the wrapped CEK — none of which leak the underlying data. Encrypting it would prevent operator-side chain inspection (`sluice backup verify`, debugging chain structure) without keys. Future-phase opt-in if operators ask for it.

### Q4: Argon2id parameters for passphrase mode?

**Decision: Argon2id with NIST-recommended starting params** (memory=64 MB, iterations=3, parallelism=4). Stored in `ChainEncryption.Argon2id` so future chains can use stronger params without breaking old chains' decryption. Operators concerned about brute-force can raise via flag (`--argon2-memory=128M`); chain header records the actual params used.

### Q5: Key-rotation UX?

**Decision: KMS handles it transparently** (rotate the KMS root key; wrapped CEKs reference the key alias; cloud SDK resolves to current version). **Passphrase rotation is a fresh-chain workflow** for v1 (start a new chain with the new passphrase; old chain stays decryptable with old passphrase). Re-encrypting existing chains is out of scope.

## Risks + mitigations

- **Risk**: passphrase loss = permanent data loss. Mitigated by: documentation prominently calling this out; recommending operators store passphrases in a password manager / secrets vault BEFORE creating the first encrypted chain. Optional: future phase could add "recovery shares" via Shamir's Secret Sharing for operators wanting M-of-N reconstruction.
- **Risk**: KMS API outage = restore unavailable. Mitigated by: per-chain CEK caching means a single KMS Decrypt at restore start covers the whole chain; even with API throttling, restore completes once the initial Decrypt succeeds. Operators wanting offline-restore-capability can dump the unwrapped CEK to a secure vault (future phase).
- **Risk**: AES-GCM nonce reuse = catastrophic key compromise. Mitigated by: per-chunk random nonces (12 bytes from `crypto/rand`); birthday-bound at ~2^48 chunks per CEK is well beyond any realistic chain size. Per-chain CEK + per-chunk random nonce keeps the math safe.
- **Risk**: Argon2id params too weak for future hardware. Mitigated by: chain header records the actual params; operators can rotate forward by taking a new full + new chain with stronger params. v1 defaults are NIST-current; reassess in 2-3 years.

## See also

- [`design-logical-backups.md`](design-logical-backups.md) — original proto-ADR (Phase 6 was always on the roadmap)
- [`design-logical-backups-phase-2.md`](design-logical-backups-phase-2.md) — corrected the v0.16.0 release notes' incorrect encryption claim; v0.16.1 amended the live release body
- ADR-0016 (cross-engine type-policy) — the pattern Phase 6's manifest extensions follow (additive fields, forward-compat)
- AWS KMS docs: https://docs.aws.amazon.com/kms/latest/developerguide/concepts.html
- GCP Cloud KMS docs: https://cloud.google.com/kms/docs/concepts
- Azure Key Vault docs: https://learn.microsoft.com/en-us/azure/key-vault/general/about-keys-secrets-certificates
- NIST SP 800-38D (AES-GCM specification): https://csrc.nist.gov/publications/detail/sp/800-38d/final
