# sluice v0.96.3

## v0.96.3 — Bug 117 ingestion-path closure: incremental/stream now refuses a rotated passphrase loudly at start

v0.94.1 made passphrase-rotation-mid-chain loud on `sluice backup verify`. v0.96.3 closes the symmetric ingestion-path hole: `sluice backup incremental` and `sluice sync` now refuse loudly at start when the operator's envelope can't unwrap any of the parent chain's existing per-chunk WrappedCEKs — instead of silently extending the chain under the rotated envelope and only failing later at restore.

### Fixed

- **Bug 117 ingestion-path closure (symmetric to v0.94.1's verify-path closure).** Pre-fix `IncrementalBackup.alignEncryption` and `BackupStream.alignEncryption` returned `nil` for the per-chunk-mode chain CEK without probing any of the parent's existing chunk WrappedCEKs against the operator's envelope. An operator rotating their passphrase between two incrementals (or between a full and its first incremental) in per-chunk mode silently accepted the rotation at incremental START, wrote new chunks under the rotated envelope, and the loud failure only surfaced later at restore-time crossing the rotation boundary. v0.96.3 adds a probe to both `alignEncryption` paths: after the existing mode-mismatch check, when the chain is per-chunk mode, find the first probe-able chunk in the parent manifest (`Tables[].Chunks` for full-manifest shape; `ChangeChunks` for incremental-manifest shape) via the new `firstPerChunkProbe` helper, then call `probeChunkDecrypt` (the same helper `VerifyBackupWith` uses) against the operator's envelope. On unwrap failure the call returns the documented `"passphrase rotated mid-chain?"` error wrapped with the `incremental:` / `stream:` prefix — refusing the incremental/stream start before any new chunks land. When the parent carries no probe-able chunks (e.g. an empty prior incremental window), the probe falls through silently so no regression is introduced on the brand-new-chain edge. Pinned by `TestIncrementalAlignEncryption_PerChunkDecryptProbe_Bug117_Ingestion` (4 sub-pins: per-chunk correct, per-chunk rotated → loud refuse, per-chain correct, per-chain rotated → existing chain-CEK probe fires first) + `TestFirstPerChunkProbe` (7 sub-pins on the helper covering nil/empty/full/incremental/per-chain-mode/plaintext/precedence-order).

### Compatibility

- Pure additive probe at incremental/stream start. No format change, no manifest version bump, no behavior change on the brand-new-chain edge.
- Same-shape error string as v0.94.1's verify-path probe (`"passphrase rotated mid-chain?"`) — operator tooling already keying on that signal continues to work.
- Per-chain-mode chains are unchanged (the existing chain-CEK probe already covered them).

### Who needs this

- **Operators running `sluice backup incremental` or `sluice sync` in per-chunk encryption mode** (`--encrypt-mode=per-chunk`) and rotating their passphrase between runs. Before v0.96.3 the rotation was silently accepted at incremental start and the loud failure only fired at restore. With v0.96.3 the loud failure fires at incremental start before any new chunks under the rotated envelope land.
- **CI/CD pipelines** with key rotation policies that operate on a stream of incrementals; the new probe stops the pipeline at the exact run where the rotation was applied, not weeks later when the disaster-recovery restore is attempted.

### Open backlog after this release

**Zero numbered bugs.** Per CLAUDE.md tenet "first real migration that silently corrupts data ends the project's credibility permanently" — v0.96.3 completes the Bug 117 closure across both verify (v0.94.1) and ingestion (v0.96.3) paths so the silent-loss class is closed bidirectionally.

Optional v0.96.x successor stretch tracked in `sluice-testing/NEXT-CYCLE.md`: inline MySQL 8.0.16+ table-level CHECK for PG DOMAINs (regex DOMAIN → `REGEXP_LIKE`, range DOMAIN verbatim). Queued for v0.97.x if operator chooses to revisit.
