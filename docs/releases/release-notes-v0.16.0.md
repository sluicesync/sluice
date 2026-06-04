# sluice v0.16.0

Logical backups Phase 2 — cloud backends. Phase 1 (v0.15.0) shipped the manifest format, chunk shape, gzipped chunks, per-chunk SHA-256 integrity, and verification against a local filesystem. v0.16.0 takes the same `BackupStore` contract to S3, S3-compatible providers, GCS, and Azure Blob, and adds a resumable backup writer so a partially-completed job picks up where it left off rather than restarting.

> **Note on encryption:** Sluice-managed client-side encryption is **not yet shipped**. Chunks land unencrypted at rest. Operators relying on at-rest encryption should use bucket-level SSE on the cloud side or filesystem-level encryption (LUKS / BitLocker / FileVault) on the local-FS side. Sluice-managed AES-256-GCM remains a Phase 6 (KMS) deliverable per the proto-ADR.

> **Known issues, see v0.16.1 for fixes:** Bug 33 (`s3://bucket/prefix` URL silently drops the prefix; chunks land at bucket root, blocking many-backups-per-bucket); Bug 34 (resumable writer is per-table-only and emits no log line on resume detection — design intent was per-chunk skip via `BackupStore.Exists` + SHA-256). Round-trip and cross-engine restore work cleanly; these are operational papercuts, not data-correctness bugs.

## Features

- **Cloud backends for backup/restore/verify** via `gocloud.dev/blob`. `--target=s3://bucket/prefix/` for backup; `--from=s3://...` for restore + verify. URL schemes: `s3://` (AWS + S3-compatible), `gs://` (GCS, ADC creds), `azblob://` (Azure, managed-identity), `file:///` (parity with `--output-dir`/`--from-dir`). Per-chunk SHA-256 integrity carries through from Phase 1 unchanged; chunks are gzipped (`*.jsonl.gz`).

- **S3-compatible provider support** via three flags: `--backup-endpoint URL` overrides the S3 endpoint (MinIO, Cloudflare R2, Backblaze B2, Wasabi, Tigris, DigitalOcean Spaces), `--backup-region REGION` overrides the region string (some providers like Archil use codes like `aws-us-east-1`), and `--backup-path-style` forces path-style addressing (required by Archil and many MinIO setups). Combining any of these with a non-`s3://` URL scheme is rejected with a clear error.

- **Resumable backup writer.** A backup process killed mid-job (process crash, host restart, network blip, operator Ctrl-C) resumes on re-run rather than restarting from scratch. Two mechanisms work together: per-chunk skip via the new `BackupStore.Exists` method + the manifest's recorded SHA-256 (skip if present and checksum-matches), and per-table progress checkpoints (manifest is updated atomically after each table completes). Re-run against the same `--output-dir` / `--target` and the orchestrator detects the partial state and resumes from the next un-completed table. Use `--force-overwrite` to discard the partial backup and start fresh.

- **Archil integration** as a side benefit of the S3-compatible flag work. Their S3 API is read-only (write path is POSIX mount via Phase 1's `LocalStore`), but the cross-environment restore-from-Archil flow is now zero-extra-code. See `docs/dev/design/logical-backups-phase-2.md` for the full integration shape including the per-disk credential setup checklist.

## Compatibility

- **No breaking IR / CLI changes** for existing backup-from / restore-from local-FS users — the `--output-dir` / `--from-dir` shape is unchanged. The new `--target` / `--from` URL flags are additive (one of each pair is required; they're mutually exclusive). The pre-existing local-FS path goes through the same orchestrator as the new cloud path; behavior on local-FS is identical to v0.15.x.

- **`BackupStore` interface gained an `Exists(ctx, path) (bool, error)` method.** Both shipping engines (`LocalStore`, `BlobStore`) implement it. Out-of-tree implementations of `ir.BackupStore` need to add the method.

- **`gocloud.dev/blob` is a new dependency.** Binary size delta: ~38.5 MB → ~84.2 MB on linux/windows amd64 (the four cloud-driver side-effect imports each pull their respective cloud SDK). In line with the pessimistic estimate from the design doc. If footprint becomes a real concern, a future change can add build-tag gates per cloud or pivot back to native SDKs per backend; the `BackupStore` abstraction is the same either way.

## Who needs this

- **Anyone who wants their sluice backups in S3 / GCS / Azure** (or any S3-compatible provider — MinIO, R2, B2, Wasabi, Tigris, Spaces, Archil-read).
- **Operators running long backups against large databases** — the resumable writer means a multi-hour backup that gets interrupted at hour 5 doesn't redo hours 0-5 on the next run.
- **Anyone building toward Phase 3 (incremental backups + backup-chain → CDC handoff for zero-rebuild disaster recovery).** Phase 2 is the load-bearing storage layer for that.

## What's next

Phase 3 — **incremental backups + backup-chain → CDC handoff** — is the next chunk on the backup track. The handoff piece is the user-visible payoff: restoring a backup chain into a fresh target leaves it CDC-resumable from the chain's terminal position, so the source DB is never re-bulked when sync falls behind irrecoverably. Design captured in the Phase 2 supplement; implementation queued.
