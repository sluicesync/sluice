# Security policy

## Reporting a vulnerability

If you believe you've found a security vulnerability in sluice, please report it privately rather than opening a public GitHub issue.

**Preferred channel:** [GitHub Security Advisories](https://github.com/sluicesync/sluice/security/advisories/new). Click "Report a vulnerability" to start a private disclosure thread visible only to maintainers.

If GitHub Security Advisories is unavailable for any reason, you can email **security@sluicesync.com** with the subject line `[sluice security]`. Encrypted reports via the maintainer's public key are welcome but not required.

Please include, at a minimum:

- A description of the vulnerability and the impact you believe it has.
- Steps to reproduce, including the sluice version, engine versions (MySQL/Postgres), and any relevant configuration.
- Any proof-of-concept code or sample data, with sensitive data redacted.

## What to expect

- Acknowledgment within 72 hours of your report.
- An initial assessment within one week, including whether we accept the report as a vulnerability and a rough severity classification.
- If the report is accepted, regular updates as a fix is developed. If it's not accepted (e.g. the behavior is intentional), a clear explanation.
- A coordinated disclosure timeline. We aim for fixes within 30 days for high-severity issues; lower-severity issues may take longer. We'll always communicate the timeline before publishing.

We will credit you in the release notes and the published advisory unless you prefer to remain anonymous.

## Scope

Sluice's threat model assumes a trusted operator: the user running `sluice migrate` or `sluice sync` is granting the tool the privileges of their database credentials, and the source/target databases are within their control. Issues we treat as in-scope include:

- **Credential handling.** DSNs are passed via flags or environment variables. Anything that causes them to leak (logs, error messages, on-disk artifacts) is in scope.
- **Source-data tampering.** Anything that lets a malicious source produce output that compromises the target beyond the expected schema-and-data copy (e.g. SQL injection through a maliciously crafted column name surviving DDL emission).
- **Misuse of replication slots / binlog access.** Sluice asks for elevated privileges; bugs that misuse them are in scope.
- **Memory or filesystem leaks** that expose data across migrations or beyond the lifetime of a single `sluice` invocation.

### The source-trust boundary (catalog expressions execute on the target)

Sluice's schema translation preserves certain SQL expressions from the **source catalog verbatim** in the DDL it executes against the **target**: `CHECK` constraint bodies, column `DEFAULT` expressions, and (where an engine pair supports passthrough) generated-column and index expressions. This is inherent to faithful schema migration — the same property `pg_dump | psql` has (cf. the CVE-2018-1058 trust class) — and it means:

> **A compromised or hostile *source database* can execute SQL on the *target* with sluice's credentials.**

The threat model above already assumes the source and target are within the operator's control; this subsection makes the consequence explicit. If you migrate from a database you do not fully trust (a vendor handoff, a seized/forensic copy, a multi-tenant snapshot), treat its catalog as untrusted *code*, not just untrusted data:

- Run `sluice migrate --dry-run` (or `schema preview`) first and review the emitted DDL — every verbatim expression is visible there.
- Point the first run at a scratch target with throwaway credentials.
- Grant the target DSN the least privilege that works (no superuser; a role scoped to the target schema).

What stays in scope as a *vulnerability*: anything that lets a malicious source execute SQL **beyond** the catalog expressions visible in the dry-run DDL (e.g. injection through identifier quoting, or expression content escaping its DDL position). The verbatim emission of catalog expressions itself is intended behavior under the trusted-source model.

Out of scope:

- Denial of service against the source or target arising from the user's own configuration choices (e.g. running a migration without a maintenance window).
- Issues in dependencies that are not exploitable through sluice's API surface — please report those upstream.
- Behaviour that requires the operator to already have privileged access they shouldn't have (privilege escalation against the database itself is the database's concern).
- SQL contained in a *trusted-by-the-operator* source catalog executing on the target (see "The source-trust boundary" above) — that is the documented contract; review the dry-run DDL when the source isn't fully trusted.

## Release artifact provenance

Every release is built by [`.github/workflows/release.yml`](.github/workflows/release.yml) on GitHub-hosted runners, and its artifacts carry [GitHub artifact attestations](https://docs.github.com/actions/security-for-github-actions/using-artifact-attestations) — Sigstore-backed SLSA build provenance naming the workflow, the repository and the tag that produced them. Verify before you install:

```bash
# any release asset — an archive, a .deb/.rpm/.apk, or checksums.txt
gh attestation verify sluice_<version>_Linux_x86_64.tar.gz --repo sluicesync/sluice

# the container image, by tag or by digest
gh attestation verify oci://ghcr.io/sluicesync/sluice:<version> --repo sluicesync/sluice
```

**What is covered, and what is not.** This is written as a roster rather than a sentence because the sentence is what went wrong. Release assets have been attested since v0.110.1; the container image was **not** a subject until the first release after v0.116.1, while the workflow claimed provenance for "everything a consumer downloads" — so the tarball verified green and `gh attestation verify oci://ghcr.io/sluicesync/sluice:0.116.1` returned `HTTP 404` for the artifact most likely to be running in production. A narrowed security claim is worse than an absent one, because it is what stops the next person from checking. `TestEveryPublishedReleaseArtifactIsAttestedOrExempt` (`internal/docsync`) now fails the build when these two lists, the release workflow and `.goreleaser.yaml` disagree.

Attested:

<!-- attested-release-artifacts: GHCR image multi-arch index, GHCR image per-arch manifests, archives (.tar.gz/.zip), checksums.txt, native packages (.deb/.rpm/.apk) -->

The image is attested at three digests, not one: the multi-arch OCI index that `:<version>` and `:latest` resolve to (the digest a `docker pull` reaches, and the one `gh attestation verify oci://` looks up), plus each per-architecture manifest, because `:<version>-amd64` and `:<version>-arm64` are published, pullable tags whose digests an attestation on the index does not cover.

Not attested, each for a reason:

<!-- unattested-release-artifacts: GitHub source archives, homebrew formula, scoop manifest, winget manifest -->

- **Homebrew formula and Scoop manifest** are commits in sibling repositories rather than build outputs. Each is a download URL plus the SHA-256 of an archive that *is* attested, so verifying what they install routes through the archive; the control on the commit itself is the scope of the publishing token.
- **The WinGet manifest** is generated but never published by CI (`skip_upload`); WinGet submissions are made by hand.
- **GitHub's auto-generated "Source code (zip/tar.gz)"** is produced by GitHub from the tag, never passes through the build, and therefore has nothing for the workflow to hash. Its integrity control is the commit SHA the tag names — verify it with `git`, not with `gh attestation`.

Attestation is deliberately **not** one of the release publish gates: a release whose attestation step failed still ships working, checksummed binaries, and gating publication on it would trade a real property for a newer one. If `gh attestation verify` fails for an artifact you downloaded, that is worth reporting through the channel at the top of this file.

## Supported versions

While the project is in `0.x`, only the latest minor release line is supported for security fixes. Once `1.0` ships, we'll publish a longer support window in this document.

## Defensive practices

If you're operating sluice in a sensitive environment, a few hardening notes:

- Run with the least-privileged DB credentials that work for your migration. The CLI honours read-only DSNs for source where the operation allows it.
- Avoid placing DSNs in shell history or repository-tracked files. The `SLUICE_SOURCE` / `SLUICE_TARGET` environment variables are loaded from your environment; combine them with a secret manager.
- The `--config` YAML may contain sensitive overrides; treat it like a secrets file.
- Migrating from a source you don't fully trust? Review the dry-run DDL first — see "The source-trust boundary" under Scope.
- Local backup stores (`backup` / `backup stream` without a cloud URL) contain full row data; sluice writes them owner-only (0600/0700) since v0.99.31, but `--encrypt` is the real control on shared or backed-up filesystems.
- What `--encrypt` does and does not protect against a **store-level adversary** (someone who can rewrite the backup directory / bucket): chunks written at backup format version 5+ (ADR-0152) are AES-GCM-bound to their backup identity and position, so substituting, splicing, reordering, or cross-backup-replaying encrypted chunks fails loudly at restore; on AWS/GCP KMS the chain key's wrap is additionally bound via EncryptionContext/AAD (Azure Key Vault's wrap API has no such parameter — its chunks rely on the chunk-level binding). The manifest is **unsigned by default**: without a signature, presenting a complete older (manifest + chunks) pair — a whole-backup rollback — or truncating the tail of a change window remains possible.
- **On an UNSIGNED chain the manifest's SCHEMA is forgeable, on every chain shape.** Say it directly rather than let a reader infer protection from the presence of a fingerprint: the manifest records a `schema_hash`, but it is a *keyless public hash* over the schema stored beside it, so a store-level adversary edits the schema and recomputes the hash. That is corruption detection, never tamper-proofing, and no amount of hashing can make it tamper-proofing — integrity without a secret is not achievable. It matters because restore drives every `CREATE TABLE` / `CREATE INDEX` / `CREATE POLICY` off that schema: an edit can bring the restored database up with row-level security off, CHECK/foreign-key/unique constraints absent, columns widened or nullable — none of which needs an injected statement to be a real loss. `--sign` is the control (see below); the same residual applies to chunk contents on a pre-format-5 chain. Independently of signing, the DDL positions that take a bare, unquotable identifier (index access method, operator class, sequence data type, RLS policy command, MySQL charset/collation) are validated at emit time and refused with `SLUICE-E-SCHEMA-IDENTIFIER-INVALID`, so a hostile value there is a refused DDL rather than executed SQL regardless of how it arrived.
- **Manifest signing (ADR-0154, since v0.99.208–212) closes the rollback and tail-truncation gaps when enabled — and, since ADR-0183 (canon v5), authenticates the SCHEMA ITSELF.** Before v5 the signature folded the recorded `schema_hash` string and zero bytes of the schema, so schema authenticity depended on a fingerprint recompute that the single-manifest restore path does not run: on a signed bare full nothing authenticated the schema. v5 folds the manifest's raw schema bytes, so a signed backup of any shape refuses a schema edit at signature verification. Signatures written by older releases keep verifying at their own recorded canon version. **That backward compatibility is also a boundary, and it is worth stating outright: a chain signed before canon v5 — anything written by v0.107.0 or earlier — keeps the pre-v5 exposure forever.** A signature cannot be strengthened after the fact, no re-seal migration exists, and so a v4-signed manifest whose schema was edited on the store still reports `signature valid` under `--require-signature` and still restores with CHECK/UNIQUE/foreign-key constraints stripped and RLS flipped off. sluice now emits a WARN at every signature verification whose recorded canon version predates v5, naming what is not covered; the only remedy is a fresh full backup taken with a v5-writing release, which is what makes newly-taken chains schema-authentic. Pass `--sign` with a key (`--sign-key`) to sign backups; the signature covers the manifest's canonical bytes, the chain lineage, and per-link enumeration, so a rollback, a dropped tail, or a swapped signed manifest fails verification. Keys can be a local HMAC-off-KEK / Ed25519 keypair (`sluice backup keygen`) or a cloud KMS key (`--sign-key kms://{aws,gcp,azure}/<ref>`, verify with `--verify-key`); pass `--require-signature` on restore to refuse any chain that is not signed (without it, an unsigned chain restores with a warning, not a refusal). Signing is opt-in — a chain taken without `--sign` still carries only the format-5 chunk-level confidentiality/position binding above, so pin/verify manifests out of band (object-store versioning + immutability policies) if you do not sign. Backups written before format version 5 carry confidentiality only.
