# sluice v0.99.269

A robustness fix for the backup-chain concurrent-writer guard's S3 conditional-PUT loser detection — the create-only CAS write the `SLUICE-E-BACKUP-CHAIN-CONFLICT` refusal rides on.

## Fixed

- **The chain concurrent-writer guard now detects the conditional-PUT loser from the authoritative S3 API error code (`PreconditionFailed`), not gocloud's derived error class.** gocloud v0.46.0's s3blob error classifier carries a substring hack — `strings.Contains(err.Error(), "301")`, meant to catch an invalid-bucket 301 redirect — that matches the literal `301` anywhere in the error string, including the random hex RequestID/HostID that S3 stamps on every response. When that substring landed by chance (~2% of requests, whenever the RequestID happened to contain `301`), a genuine `412 PreconditionFailed` conditional-PUT loser was misclassified as `NoSuchBucket` → not-found, so the losing writer surfaced a confusing "not found" error instead of the coded `SLUICE-E-BACKUP-CHAIN-CONFLICT`. sluice now reads the smithy API error code directly — authoritative and immune to the flaky derived class — and falls back to gocloud's error class only for the fileblob/memblob drivers that carry no API error. This was surfaced as a v0.99.268 tag-CI flake on the MinIO CAS integration test (RequestID `18C30130E2747EAB` contains `301`); the same misclassification was ~2% latent in production for a real chain-conflict loser.

## Compatibility

- **Purely additive robustness.** No behavior change for the ~98% of conditional-PUT losers gocloud already classified correctly — the fix only recovers the ~2% it misclassified. No CLI, config, or on-disk format change; existing backups and chains are unaffected.

## Who needs this

Anyone running `sluice backup` chains with the concurrent-writer guard against S3 or an S3-compatible store (AWS S3, MinIO, R2, B2, Wasabi, Tigris). The guard already refused a genuine concurrent-writer conflict correctly ~98% of the time; this closes the ~2% window where the refusal surfaced as a misleading "not found" instead of the coded conflict.

## Install

- Binaries: https://github.com/sluicesync/sluice/releases/tag/v0.99.269
- Homebrew: `brew install sluicesync/tap/sluice`
- Scoop: `scoop bucket add sluicesync https://github.com/sluicesync/scoop-bucket; scoop install sluice`
- winget: `winget install sluicesync.sluice`
- Docker: `docker pull ghcr.io/sluicesync/sluice:v0.99.269`
