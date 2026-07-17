# sluice v0.99.264

The Google Cloud SQL validation batch — plus, landing the same day: the object-store CAS matrix fully real-cloud-verified and the KMS signing surface validated on both remaining clouds.

## Added

- **Google Cloud SQL for MySQL is detected in-band on `sync`/backup runs.** Cloud SQL has no hostname to pattern-match (connections are bare IPs or a localhost proxy), so for IP-shaped source hosts sluice fingerprints via `@@version`'s `-google` suffix and verifies `binlog_expire_logs_seconds` — which on Cloud SQL is honest (1-day enforced floor, no out-of-band reaper: the first probed platform where the variable tells the truth), so correctly configured sources stay silent. A CDC position lost to the point-in-time-recovery toggle now explains the binlog-numbering reset (a disable/enable round-trip destroys binlogs and restarts numbering at 000001) and names auto-resnapshot as the correct recovery.
- **docs/managed-services.md gains live-validated Cloud SQL MySQL + PostgreSQL sections** and a three-provider managed-MySQL retention comparison (DO ~13–16 min, API-only truth / RDS ~5–11 min, SQL-visible truth / Cloud SQL honest floor); the stale claim that Cloud SQL requires the cloud-sql-proxy is corrected — direct public-IP connections validated end to end.
- **ADR-0160's backend matrix is fully real-cloud-verified** — S3, GCS, and Azure Blob all confirmed running true create-only CAS through sluice's own store path with the coded chain-conflict refusal (each provider's distinct loser shape — 412, uniform 412s, 409 `BlobAlreadyExists` — correctly classified). **Roadmap item 59 is closed**: the `kms://` signer validated live on Google Cloud KMS (in-HSM ECDSA, CRC32C wire handshake) and Azure Key Vault (all four key families including P-521 and RSA-PSS); the emulator OAuth blocker that stalled the item since v0.99.228 did not exist against the real service.

## Changed

- The Postgres replication-capability refusal names Google Cloud SQL as a platform where `ALTER ROLE … WITH REPLICATION` works verbatim as the default user (validated live).

## Fixed

- **`backup verify` no longer counts signature failures against the chunk denominator** ("2 of 1 chunk(s) failed verification" on a wrong-key verify) — signature failures get their own summary line; exit codes unchanged.

## Compatibility

- **No breaking changes.** One behavior note: CDC-anchoring runs whose MySQL source DSN is an IP literal or localhost pay one short-lived (15s-bounded) fingerprint probe at start; named hosts are unaffected.

## Who needs this

Anyone running sluice against Google Cloud SQL (both engines now live-validated with honest platform guidance), anyone verifying signed backups (the corrected summary), and anyone relying on the backup chain guard or KMS signing against real cloud providers — the verification story for both is now observed, not derived, on every supported backend.

## Install

- Binaries: https://github.com/sluicesync/sluice/releases/tag/v0.99.264
- Homebrew: `brew install sluicesync/tap/sluice`
- Scoop: `scoop bucket add sluicesync https://github.com/sluicesync/scoop-bucket; scoop install sluice`
- winget: `winget install sluicesync.sluice`
- Docker: `docker pull ghcr.io/sluicesync/sluice:v0.99.264`
