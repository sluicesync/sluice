# sluice v0.17.2

Logical backups Phase 3.3 — **CDC handoff from a backup chain**, the third sub-phase that closes the v0.17.0 known-limitation list. Take a v0.17.2 full, restore the chain into a fresh target, and `sluice sync start --position-from-manifest=<chain-url>` resumes CDC from the chain's tail without re-bulking from source. PG operators get three pre-flight checks before the slot opens; `--strict-preflight` flips the soft warnings to refusals.

> **Important caveat — during-backup write window.** v0.17.2's `EndPosition` is captured *at end-of-backup* (current source CDC position when the manifest flips to `complete`), not at snapshot start. Sluice's full-backup row sweep reads each table outside a single shared snapshot transaction, so writes that arrive *during* the backup window may or may not be captured by the row sweep depending on which tables had already been read. A subsequent incremental from `EndPosition` then resumes CDC from end-of-backup forward, leaving the in-window writes potentially missed. **The recommended operational shape is to pair backups with a continuously-running `sluice sync start` CDC stream** — the live stream captures every write as it happens, the backup chain provides the cold-bootstrap path, and the CDC stream's normal idempotent apply fills any chain gap on restore. Operators relying on backups *alone* (no live CDC) for DR should take backups during quiet windows, or wait for a future release that wires the full-backup row sweep into the existing snapshot infrastructure (planned follow-up; tracked alongside Phase 4).

## Features

- **Full-backup `EndPosition` recording (Phase 3.3.A).** `sluice backup full` now captures the source's CDC position at end-of-backup automatically and writes it onto the manifest's `EndPosition` field. PG records `pg_current_wal_lsn()` paired with the configured slot name; MySQL records `@@global.gtid_executed` (or `(file, position)` when GTID mode is off). Closes the v0.17.0 "parent has no EndPosition; chain will start from CDC's current position" warning — chains rooted in v0.17.2+ fulls now get clean chain math for free. Engines opt in via the new `ir.BackupPositionCapturer` optional interface; engines without CDC support skip silently. New `sluice backup full --slot-name` flag labels the recorded position on PG so a Phase 3 incremental opens CDC against the same slot.

- **`sluice sync start --position-from-manifest=<chain-url>` (Phase 3.3.B).** New CLI flag that reads the chain's terminal manifest's `EndPosition` and uses it as the resume position, bypassing the per-target `sluice_cdc_state` row. Use after `sluice restore --from=<chain-url>` to resume CDC from the chain's tail without re-bulking from source. Mutually exclusive with `--reset-target-data` (different recovery shapes). The slot-missing fall-through (ADR-0022) is suppressed when chain handoff is requested — silently re-bulking would defeat the chain's purpose. Accepts the same URL schemes as `sluice backup` (`s3://` / `gs://` / `azblob://` / `file:///`); companion `--backup-endpoint` / `--backup-region` / `--backup-path-style` flags for S3-compatible providers.

- **PG soft-warning pre-flights (Phase 3.3.C) for `--position-from-manifest`.** Three checks run before CDC opens:

   1. **`wal_keep_size` sufficiency.** Soft warning when configured below PG's 64 MB default — the threshold is at the default, so only setups that explicitly dialed it down trigger. Surfaces a pointer to `docs/postgres-source-prep.md` for tuning guidance.
   2. **Patroni / HA-managed source detection.** Soft warning about the idle-slot failover trap — slots not actively consumed don't replicate to standbys and are silently lost on failover. Three signals checked in order: Patroni-set GUCs in `pg_settings`, `pg_stat_replication.application_name` LIKE 'patroni%' (gracefully degrades on permission denied), role names `patroni` / `replicator`.
   3. **Slot existence + health.** Fatal refusal for missing slots or slots with `wal_status='lost'` / `'unreserved'`. Always a refusal regardless of `--strict-preflight` — the slot can't deliver what's needed.

   MySQL has no preflight surface — its CDC reader's existing `verifyPositionResumable` already covers binlog purge with `ir.ErrPositionInvalid`.

- **`sluice sync start --strict-preflight` (Phase 3.3.D).** New flag that promotes Phase 3.3.C soft warnings to hard refusals before CDC starts. Default off: warnings log via slog and the run proceeds. Use in CI gates, scripted runbooks, or post-incident audits where you want a strict "fail loudly on any preflight signal" posture.

- **`pipeline.LoadChainTerminalPosition(ctx, store)` exported helper.** Reads every manifest in a backup store, validates the chain shape, and returns the terminal manifest's `EndPosition`. Used by the streamer's `--position-from-manifest` path; exposed for downstream tooling that wants to inspect a chain's tail position.

## Compatibility

- **No IR schema breaking changes.** All Phase 3.3 additions are forward-compatible: `Manifest.EndPosition` is an existing field (added in v0.17.0; just unused on fulls until now), `ir.BackupPositionCapturer` and `ir.PositionFromManifestPreflight` are optional engine interfaces. `ir.Manifest.FormatVersion` stays at 1.

- **`ir.PositionFromManifestPreflight` and `ir.PreflightReport` live in the `ir` package** so engine packages can implement the interface without forming an import cycle through pipeline's integration tests. The `pipeline` package keeps type aliases so existing call sites compile unchanged.

- **No CLI flag removals.** `--position-from-manifest`, `--strict-preflight`, and the S3-compat tuning flags are additive.

- **Out-of-tree engines should add `ir.BackupPositionCapturer` to their `SchemaReader`** if they want their full backups to record `EndPosition`. Engines without it skip the capture (the manifest's `EndPosition` stays empty, matching the v0.16.x shape — chains rooted in such manifests still surface the v0.17.0 "parent has no EndPosition" warning, which is the right degraded behaviour).

- **Multi-incremental chains written by v0.17.0 remain unrecoverable** (Bug 35; see v0.17.1). v0.17.2 doesn't change that; operators with broken chains should take a fresh full + restart the chain on v0.17.1+.

## Phase 3 known limitations (closed)

The v0.17.0 release notes flagged three Phase 3.3 follow-ups; **all three are addressed in v0.17.2:**

- ✅ Full backups record `EndPosition` automatically (Phase 3.3.A).
- ✅ `sluice sync start --position-from-manifest` is implemented (Phase 3.3.B).
- ✅ PG `wal_keep_size` soft-warning + Patroni-detection pre-flights are implemented (Phase 3.3.C/D).

## Who needs this

- **Anyone building toward zero-rebuild disaster recovery.** v0.17.0/.1 gave you chain restore; v0.17.2 closes the loop by making the post-restore CDC handoff a single command. `restore --from=<chain-url>` then `sync start --position-from-manifest=<same-url>`.

- **Operators of HA-managed PG clusters (Patroni, PlanetScale Postgres).** The idle-slot failover trap is a known production failure mode (operator-confirmed, 2026-05-07); v0.17.2 surfaces it as an in-CLI soft warning the moment you point `--position-from-manifest` at a Patroni-managed source. See `docs/postgres-source-prep.md` for the four-tier mitigation hierarchy.

- **Operators with low `wal_keep_size` on chatty sources.** v0.17.2's preflight catches the "your wal_keep_size doesn't cover the chain's typical incremental cadence" mismatch before it becomes a wedged slot. Use `--strict-preflight` if you want CI to fail-fast on the warning.

## Operator notes

For PG sources, the `--position-from-manifest` preflight runs three checks against the source. The slot-existence check is always a refusal; the `wal_keep_size` and Patroni-detection checks are warnings by default. The `--strict-preflight` flag escalates warnings to refusals.

If your workflow is "restore a chain + immediately resume sync", the PG path requires the slot named in the chain's terminal `EndPosition` to exist on the source with a `restart_lsn` covering that LSN. The simplest pattern: keep a long-lived sluice CDC stream running alongside the periodic `sluice backup incremental` calls — the slot's `restart_lsn` advances with the consumer, and the chain's terminal LSN always sits comfortably inside the slot's WAL window. v0.17.2's preflight catches the failure modes; the docs in `docs/postgres-source-prep.md` cover the slot-lifecycle setup.

For MySQL sources, no preflight is needed — the CDC reader's existing `verifyPositionResumable` check fires before streaming starts and surfaces a clear "binlog file is no longer available" / "source has purged GTIDs not present in resume set" error wrapped with `ir.ErrPositionInvalid`. The streamer surfaces these as run-aborting errors; the recovery path is "take a fresh full backup + restart the chain."
