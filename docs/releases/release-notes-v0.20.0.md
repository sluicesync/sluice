# sluice v0.20.0

Logical backups Phase 4.5 lands. `sluice sync from-backup` is the consumer-side companion to v0.19.0's `sluice backup stream` — a long-running broker that polls a backup chain and replays incrementals into a target. The headline operator outcome: **decouple source and target via the backup chain as the message log.** Source-side `backup stream` writes incrementals to S3/GCS/Azure/local-FS; target-side `sync from-backup` polls the same destination and replays into its own database. Log-based ETL without direct source-target connectivity. Implementation supplement: `docs/dev/design-logical-backups-phase-4-5.md`.

## Features

- **`sluice sync from-backup run --backup-target=<url> --target-driver --target --stream-id=ID`.** Long-running broker that drives a `for { tick(); replay(); commit(); }` loop at the configured `--poll-interval` cadence (default `30s`). Each tick lists manifests at the chain root, filters to incrementals NOT yet applied (via the persisted `last_applied_backup_id` in `sluice_cdc_state`), and replays each in chain order — schema deltas first, then change chunks through the engine's batched `ChangeApplier.ApplyBatch`. Reuses the same applier code path that `sync start` uses; ADR-0010's idempotent-apply guarantee makes broker crashes safe to re-replay.

- **`sluice sync from-backup stop --backup-target=<url>`.** Companion stop command. Writes `stop_requested_at` to the chain destination's `manifests/broker_state.json`; the running broker observes the request on its next tick poll and exits cleanly. Cross-machine: an operator on machine B can stop a broker on machine A without process access — both sides agree on the chain destination. Mirrors the `backup stream stop` pattern from v0.19.0.

- **Cold-start safeguards.** First-start refusal when `sluice_cdc_state` has no row for the supplied `--stream-id`, with operator-actionable guidance naming the two override flags:
  - `--reset-target-data` — broker runs `pipeline.ChainRestore` internally first (full + every incremental up to current), then transitions to live polling.
  - `--at-chain-id=<BACKUP-ID>` — operator asserts the target is currently at chain ID `<BACKUP-ID>` (after manual `sluice restore`); broker writes a fresh `sluice_cdc_state` row and transitions to live polling.

  Same friction tier as `migrate --force-cold-start` (Bug 9 recovery). Prevents silent target overwrites.

- **Schema evolution within a broker stream.** Each incremental's `SchemaDelta` (AddTable, DropTable, AlterTable with column adds) replays on the target before that incremental's change chunks. Reuses `ir.SchemaDeltaApplier.AlterAddColumn` from Phase 3.2; both PG and MySQL implementations are idempotent so broker re-replays are safe. Rename-shaped deltas (single drop + single add of same table within a window) are flagged ambiguous → broker stops with a "force fresh full + new chain" recovery message.

- **In-process stop signal**, mirroring the v0.19.1 stream-stop fix. `RequestSyncFromBackupStop` (called from `sync from-backup stop` or downstream tooling) closes a process-local channel for instantaneous same-process observation, bypassing file I/O entirely. Cross-process operators take the file-poll path; both paths land at the same eager-exit code path.

- **Heartbeat read-modify-write.** `broker_state.json` heartbeats use the same v0.19.1 R-M-W shape: read the current state file before writing, copy any concurrent `stop_requested_at` forward, then write. A heartbeat write that lands after an operator's `RequestSyncFromBackupStop` no longer clobbers the stop request.

- **`pipeline.SyncFromBackup` exported** for downstream tooling. Same shape as the existing `Migrator` / `Streamer` orchestrators; construct the value, set fields, call `Run(ctx)`. Concurrent calls on the same value not supported (single-shot per Run).

- **`ir.PositionWriter` optional applier surface.** Engines that implement it (Postgres + MySQL as of this release) let the broker record cold-start positions and schema-delta-only-incremental positions without an accompanying data write. Wraps the same `writePositionTx` helper the apply path uses, so row shape + idempotency are identical.

## Use cases this unlocks

| Scenario | Before v0.20.0 | With v0.20.0 |
|---|---|---|
| **No direct connectivity** between source and target (compliance, firewall, VPN) | Operator must establish source→target connectivity OR run sluice on a host that bridges both networks. | Source-side sluice writes to S3; target-side reads from same S3. No network bridge needed. |
| **Multi-region replication without VPN/peering** | Operator-managed VPN, CrossLink, or accept latency of a sluice instance in a third bridging region. | Source in `us-east-1`, target in `eu-west-1`; backup bucket is rendezvous. |
| **One source → many targets fan-out** (prod analytics + dev refresh + staging clone) | Run N sluice instances against the source; N times the slot pressure. | One source-side stream + N broker instances reading the same chain. **Source slot pressure unchanged.** |
| **Sync-itself disaster recovery** (target down for hours) | Source-side sluice CDC stream blocks if target connectivity is lost. | Source-side stream continues writing to S3 regardless of target state. Target catches up when it returns. |
| **Air-gapped target replication** | Manual SneakerNet bulk-loads. | Backup bucket → SneakerNet → restore at target site → broker continues from disk-mounted chain. |

## Compatibility

- **No CLI breaking changes.** Existing `sluice migrate` / `sync start` / `sync status` / `sync stop` / `sync health` / `backup *` / `restore` / `verify` / `slot *` / `schema *` flag surfaces are unchanged.
- **No format changes.** Manifest schema, change-chunk format, and `sluice_cdc_state` schema are unchanged. Pre-v0.20.0 backup chains restore + verify identically.
- **No new dependencies.** The broker is built on the existing chain-walker (Phase 3.2), batched applier (ADR-0017), and stop-signal patterns (ADR-0025, v0.19.1).
- **Brokers are read-only consumers of the chain.** They never modify manifests; `backup verify` against a destination consumed by N brokers is clean.
- **Same-engine only in v1.** Cross-engine `sync from-backup` (PG-source-chain → MySQL target) is deferred to Phase 5 alongside cross-engine chain restore. The broker refuses cross-engine chains the same way `chain restore` does.

## Operator notes

- **Poll interval tuning.** Default `30s` is reasonable for staging, dev refresh, and analytics replication. Production critical-path replication can drop to `5s` for tighter source→target latency at the cost of more `List(manifests/)` calls against blob storage. Quiet chains amortise either way; the broker only does work when new manifests appear.

- **Cold-start ergonomics.** First time you point a broker at a fresh target, you'll get a refusal naming the override flags. Two operator workflows:
  - **Greenfield target**: `--reset-target-data --yes` — broker runs the full + chain restore internally, then transitions to live polling.
  - **Already-restored target**: `sluice restore --from=<url> --target=<dsn>` first, note the chain's tail BackupID from the verify output, then `sync from-backup --at-chain-id=<BACKUP-ID>` — broker writes a fresh `sluice_cdc_state` row at that ID and transitions to live polling.

- **Restart resume semantics.** Kill the broker (Ctrl-C, SIGTERM, OOM, host kill) and the next start picks up at the persisted `last_applied_backup_id` from `sluice_cdc_state`. ADR-0010's idempotent-apply guarantee means re-applying the partially-replayed last incremental is safe — the same `ON CONFLICT (PK) DO UPDATE` semantics as live CDC.

- **`broker_state.json` is informational.** Like `stream_state.json`, the file is for liveness signalling + cross-machine stop. Losing it (operator deletes, object-store eventual-consistency lag) doesn't break anything; the chain is the source of truth and `sluice_cdc_state` carries the broker's resume floor.

- **Stream + broker against the same destination.** Both files (`stream_state.json` + `broker_state.json`) coexist in `manifests/` without colliding. Producer (stream) and consumer (broker) lifecycles are independent; stopping one does not affect the other. The broker's `RequestSyncFromBackupStop` and the stream's `RequestStreamStop` use separate in-process registries so cross-signaling is impossible.

- **Fan-out (1 stream → N brokers)** is the load-bearing value-prop. The source has exactly 1 replication slot regardless of how many brokers read the chain. Add brokers without source-side coordination; remove them the same way. Each broker writes its own row in its target's `sluice_cdc_state` so per-target progress is tracked independently.

## What's next

- **Phase 5 — cross-engine chain restore + cross-engine sync from-backup.** Phase 4.5 is same-engine only; cross-engine waits for the SELECT-grammar translator + `RetargetForEngine` extensions. PG-source chain → MySQL target is the load-bearing direction.
- **Phase 6 — KMS-backed client-side encryption** of backup chunks. Independent of sync from-backup; once it lands, encrypted-chain consumption works transparently.
- **Phase 7+ — operationally-mature features.** Multi-source aggregation (one target consuming N source chains), selective-table replay (`--include-table` on the broker), time-shifted replay (`--lag-window=DURATION` for "always 1 hour behind source").
