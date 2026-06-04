# Logical Backups Phase 4.5 — Implementation Design

Supplement to [`logical-backups.md`](logical-backups.md), [`logical-backups-phase-2.md`](logical-backups-phase-2.md), [`logical-backups-phase-3.md`](logical-backups-phase-3.md), and [`logical-backups-phase-4.md`](logical-backups-phase-4.md). This file covers Phase 4.5: **backup-as-broker** — `sluice sync from-backup` — the watcher that consumes a continuously-written backup chain and replays incrementals into a target.

The headline operator outcome: **decouple source and target via the backup chain as the message log.** Source-side `sluice backup stream` writes incrementals to S3/GCS/Azure/local-FS; target-side `sluice sync from-backup` polls the same destination and replays incrementals into its own database, producing log-based ETL without direct source-target connectivity.

## What's already in Phase 3 + 3.3 + 4 that this builds on

- **Phase 3.1 + 3.2**: incremental backup writer + chain-aware restore (chain-walker, schema-delta replay, idempotent applier per ADR-0010).
- **Phase 3.3**: `sluice sync start --position-from-manifest=<chain-url>` reads chain's terminal position; full's `EndPosition` recording at snapshot start.
- **Phase 4**: `sluice backup stream` long-running incremental writer; rolling manifests under `manifests/incr-<unix-millis>-<seq>.json`; `stream_state.json` with `last_rollover_at` heartbeat.
- **v0.18.0**: snapshot-anchored EndPosition closes the during-backup write window — backup-only DR is byte-perfect.
- **v0.19.1 (Bug 37)**: cooperative stop signaling via heartbeat R-M-W + in-process channel — reliable on contended systems.

## Scope (Phase 4.5)

**In scope:**

- `sluice sync from-backup --backup-dir=<url> --target-driver=... --target=... --stream-id=ID` long-running watcher process
- Polls the chain root for new manifests at a configurable cadence (`--poll-interval=DURATION`, default `30s`)
- On each new incremental: walks the chain delta (incrementals not yet replayed), applies schema deltas via `ir.SchemaDeltaApplier`, replays change chunks via the existing `ChangeApplier.ApplyBatch` path
- Per-target replay-state tracking via the existing `sluice_cdc_state` control table (`stream_id` keys the row; replays a chain's `BackupID` rather than a CDC LSN/GTID)
- Cooperative stop via `sluice sync from-backup stop --target=<url>` (mirrors Phase 4's stop-signal pattern, including the v0.19.1 heartbeat-R-M-W + in-process channel close)
- Catch-up mode: on first start against a non-empty target, refuses unless `--reset-target-data` is passed OR the operator has manually restored the chain's full + the target is at the chain's current state (operator asserts via `--at-chain-id=<ID>` flag)
- Schema evolution: replay schema deltas from manifests in chain order (already exists in `chain_restore.go`; reuse)

**Shipped in Phase 5 (cross-engine chain restore — the partner feature):**

- Cross-engine `sync from-backup` (PG-source-chain → MySQL target) shipped alongside the SELECT-grammar translator + `RetargetForEngine` extensions.

**Shipped in Phase 6 (passphrase + AWS + GCP + Azure KMS):**

- Encrypted-chain consumption — the broker reads the same chunks that decrypt-on-restore handles, via the `EnvelopeEncryption` interface across all four key-management modes.

**Deferred to Phase 7+ (operationally-mature features):**

- Multi-source aggregation via the broker pattern (one target consuming N source chains)
- Selective-table replay (`--include-table` / `--exclude-table` on the broker; chain-walker would skip change events for unselected tables)
- Time-shifted replay (`--lag-window=DURATION` for "always 1 hour behind source")

## Use cases this unlocks (operator-facing)

| Scenario | Today (without Phase 4.5) | With Phase 4.5 |
|---|---|---|
| **No direct connectivity** between source and target (compliance, firewall, VPN) | Operator must establish source→target connectivity OR run sluice on a host that bridges both networks. | Source-side sluice writes to S3; target-side reads from same S3. No network bridge needed. |
| **Multi-region replication without VPN/peering** | Operator-managed VPN, CrossLink, or accept the latency of a sluice instance in a third region with bridging connectivity. | Source in `us-east-1`, target in `eu-west-1`; backup bucket is rendezvous. |
| **One source → many targets fan-out** (prod analytics + dev refresh + staging clone) | Run N sluice instances against the source; N times the slot pressure on the source. | One source-side stream + N broker instances reading the same chain. **Source slot pressure unchanged.** |
| **Time-shifted sync** (staging always 1h behind prod) | Manual scheduled bulk-loads with stale snapshots. | Phase 7+: `--lag-window=1h` on the broker; chain provides natural delay buffer. |
| **Sync-itself disaster recovery** (target down for hours; source must keep moving) | Source-side sluice CDC stream blocks if target connectivity is lost. | Source-side stream continues writing to S3 regardless of target state. Target catches up when it returns. |
| **Air-gapped target replication** | Manual SneakerNet bulk-loads. | Backup bucket → SneakerNet → restore at target site → broker continues from disk-mounted chain. |

## Sub-phasing

| Sub-phase | Scope | LOC est. |
|---|---|---|
| **4.5.1 — Broker pump + chain-watcher** | New `internal/pipeline/broker.go` with `SyncFromBackup` orchestrator. Chain-walker poll loop (every `--poll-interval`); detect new incremental manifests via `BackupID` linkage to the last-applied state. | 400-500 |
| **4.5.2 — Replay state tracking** | Reuse `sluice_cdc_state` table with new column `last_applied_backup_id` (or encode in the existing position field as a tagged-union). On each successful incremental apply, commit the BackupID + position alongside the data writes (same idempotent-apply pattern as live CDC). | 150-200 |
| **4.5.3 — Cooperative stop + state file** | Mirror Phase 4's `stream_state.json` pattern at the BROKER side: `manifests/broker_state.json` with `pid`, `host`, `last_apply_at`, `stop_requested_at`. Reuse v0.19.1's heartbeat R-M-W helpers + in-process channel registry. | 100-150 |
| **4.5.4 — Catch-up + first-start safeguards** | First-start refusal when target has rows from a different chain; `--reset-target-data` confirmation. `--at-chain-id=<ID>` operator assertion for resumption from external state. | 100-150 |
| **CI integration** | Two-process integration test: spin up `backup stream` writing to MinIO; `sync from-backup` reading from same MinIO; drive source writes; assert target-side replay catches up. Same-engine PG + MySQL. | 200-300 |
| **Total Phase 4.5** | | ~950-1300 |

## CLI surface

| Command | Phase 4.5 work |
|---|---|
| `sluice backup stream` | Unchanged. |
| `sluice backup incremental` | Unchanged. |
| `sluice sync start` | Unchanged. |
| `sluice sync from-backup --backup-dir=<url> --target-driver --target --stream-id=ID` | NEW. Long-running broker. |
| `sluice sync from-backup stop --target=<url>` | NEW. Companion stop command (mirrors `backup stream stop`). |
| `sluice sync status` | EXTENDED. Now includes `from-backup` streams alongside `sync start` streams in the status output. |

## Replay state on the target

The broker writes to the same `sluice_cdc_state` table that `sync start` uses, just with a different position-shape:

```
stream_id        = "<operator-supplied>"
position_engine  = "backup-broker"                    -- new sentinel
position_token   = '{"chain_url":"s3://...","last_applied_backup_id":"<id>"}'
last_applied_at  = <ts of last successful incremental commit>
```

Restoring the chain's full + replaying incrementals can then be coordinated:
- **Cold start**: broker refuses unless `--reset-target-data` (mirror migrate's pattern). With `--reset-target-data`, the broker first runs `sluice restore --from=<chain-url>` internally to land the full + all incrementals up to current, then transitions to live polling.
- **Warm resume**: `last_applied_backup_id` in the position tells the broker which incremental was last applied; it walks forward from there.
- **Operator assertion**: `--at-chain-id=<ID>` lets the operator skip the cold-start refusal when they've manually loaded the target via `sluice restore` and are confident it's at chain ID X.

## Schema evolution within a broker stream

Already handled by `chain_restore.go`'s `ApplyChain` path:
- Each incremental's manifest carries an optional `SchemaDelta` (`AddTable`, `DropTable`, `AlterTable` with `ir.SchemaDeltaApplier.AlterAddColumn`).
- Broker applies schema deltas first, then change chunks. Same shape as Phase 3.2 chain restore.
- `Rename`-shaped deltas (single drop + single add of same table within a window) flagged ambiguous → broker stops with a "force fresh full + new chain" recovery message.

## Acceptance criteria

A clean Phase 4.5 must:

1. **End-to-end two-process flow on PG.** `backup stream` writing to local-FS or MinIO; `sync from-backup` reading from same destination; drive INSERTs on source; observe target catch up within `2 × --poll-interval`.
2. **End-to-end two-process flow on MySQL.** Same shape, GTID-based positions.
3. **Schema evolution within a broker stream.** Drive `ALTER TABLE ADD COLUMN` on source; broker applies the delta + subsequent rows referencing the new column.
4. **Cooperative stop.** `sync from-backup stop --target=<url>` triggers graceful drain within `2 × --poll-interval` (mirrors v0.19.1's `backup stream stop` behavior).
5. **Restart resumes.** Kill broker mid-stream; restart picks up at `last_applied_backup_id`; drives no duplicate applies (idempotent-apply per ADR-0010).
6. **Cold-start refusal.** Run broker against a non-empty target without `--reset-target-data` → refuse with operator-actionable message.
7. **Cold-start with `--reset-target-data`.** Same setup → broker drops + recreates target tables, restores chain, transitions to live polling.
8. **Warm-start with `--at-chain-id=<ID>`.** Operator-asserted resumption from external state.
9. **Source slot pressure unchanged with N brokers.** Run 1 source-side stream + 3 brokers reading the same chain. Confirm source has 1 slot (the stream's), regardless of broker count. **The fan-out value-prop.**
10. **Backup chain integrity preserved.** Brokers are read-only consumers; they never modify manifests. Verify post-run that `backup verify` against the destination is clean.

CI: long-running tests use small `--poll-interval=2s` + bounded source writes for ~30-60s scenarios. The fan-out test (criterion 9) is the most ambitious — may require a dedicated integration test that observes `pg_replication_slots` state on the source.

## Tenet check

- **IR-first.** Broker consumes `ir.Change` events from the chain; replays via existing `ChangeApplier`. No new translation surface (cross-engine waits for Phase 5).
- **Contain Postgres complexity.** Broker doesn't open slots on the target; it writes via the existing applier path that already handles target-side coordination.
- **Validate end-to-end.** Acceptance criteria 1-2 are the load-bearing two-process integration tests.
- **Loud failure beats silent corruption.** Cold-start refusal (criterion 6) prevents silent target overwrites; ambiguous schema deltas (Rename shape) stop with recovery message.
- **Clean, elegant code.** Broker = small loop reusing chain_restore.go's apply path; new orchestrator file; reuses Phase 4's heartbeat R-M-W + stop-channel registry.

## Open question — broker liveness across operator restarts

When the broker process exits cleanly (not crashed), the next start picks up at `last_applied_backup_id` from `sluice_cdc_state`. Clean.

When the broker process crashes mid-incremental (panic, OOM, host kill), the partial-replay state is in `sluice_cdc_state` — incremental N is partially applied if the crash happens between row writes and the position commit. ADR-0010's idempotent-apply guarantee means re-applying the same batch on restart is safe.

But: **what if `sluice_cdc_state` itself is wiped** (operator drops the row, target DB is recreated)? The broker's `last_applied_backup_id` is lost. On restart, broker can't tell which incremental to start from. Two operator-actionable responses:

1. **`--at-chain-id=<ID>`** asserts "I know the target is at chain state X; resume from there." Operator's responsibility.
2. **`--reset-target-data`** asserts "I want a fresh start; restore the chain's full + replay all incrementals to current."

The broker refuses without one of these flags when `sluice_cdc_state` has no row for the supplied `--stream-id` AND the target has rows in any sluice-managed table. Same friction tier as `migrate`'s cold-start (Bug 9 / `--force-cold-start`).

## See also

- [`logical-backups.md`](logical-backups.md) — original proto-ADR; Phase 4.5 mentioned as a future direction
- [`logical-backups-phase-2.md`](logical-backups-phase-2.md) — "Backup-as-broker pattern (Phase 4.5+ direction)" section sketches the use cases this design now formalizes
- [`logical-backups-phase-3.md`](logical-backups-phase-3.md) — chain-walker + chain-restore the broker reuses
- [`logical-backups-phase-4.md`](logical-backups-phase-4.md) — `backup stream` + `stream_state.json` patterns the broker mirrors at the consumer side
- ADR-0007 (per-target control table) — the conceptual cousin of `broker_state.json`
- ADR-0010 (idempotent CDC apply) — the load-bearing assumption for safe broker re-replay on crash
- ADR-0025 (graceful-drain `sync stop`) — the conceptual cousin of `sync from-backup stop`
