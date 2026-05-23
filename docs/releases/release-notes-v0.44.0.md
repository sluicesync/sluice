# sluice v0.44.0 — PlanetScale backup chain-resume (closes #16)

**Closes GitHub issue #16.** Before v0.44.0, `sluice backup full` against a PlanetScale/Vitess source captured the source position via the binlog-shape encoder:

```json
{"Engine":"mysql","Token":"{\"mode\":\"gtid\",\"gtid_set\":\"fb0fb32e-...:1-886171\"}"}
```

But the continuous-sync VStream reader that `sluice backup stream run` and `sluice backup incremental` use only understands the VStream `[]shardGtid` shape:

```json
{"Engine":"planetscale","Token":"[{\"keyspace\":\"sync-source\",\"shard\":\"-\",\"gtid\":\"...\"}]"}
```

Operators got an immediate unmarshal failure on any chain-resume attempt:

```
sluice: error: stream: start cdc stream:
  mysql: vstream position: unmarshal:
  json: cannot unmarshal object into Go value of type []mysql.shardGtid
```

Backup chains were completely unusable on PlanetScale sources. The architectural fix routes PlanetScale's `OpenBackupSnapshot` through the same VStream COPY-mode machinery the live-sync `coldStart` path already uses — same gRPC stream, same per-shard vgtid capture, same `vstreamSnapshotRows` table-by-table reader.

## Fixed

### Backup snapshot routes through VStream COPY for PlanetScale

- **`internal/engines/mysql/backup_snapshot_vstream.go` (new)** — `openBackupSnapshotVStream(ctx, dsn)` delegates to the existing `openVStreamSnapshotStream` (the live-sync coldStart path), constructs an `ir.BackupSnapshot` from the snapshot stream's `Position` / `Rows` / `CloseFn`, and ignores the `Changes` field (backup doesn't consume CDC, just records the position so a downstream incremental can resume from there). The gRPC stream stays open until `BackupSnapshot.Close` fires.

- **`internal/engines/mysql/backup_snapshot.go::OpenBackupSnapshot`** — flavor branch added: `if e.Flavor == FlavorPlanetScale { return e.openBackupSnapshotVStream(ctx, dsn) }`. The pre-v0.44.0 pinned-conn + `START TRANSACTION WITH CONSISTENT SNAPSHOT` + `@@global.gtid_executed` path stays for vanilla MySQL only.

### Why the pre-v0.44.0 path failed two ways

1. **Wrong position encoding.** `@@global.gtid_executed` is a binlog concept; the VStream reader can't decode it. Chain-resume always failed.
2. **Snapshot semantics that don't generalise.** vtgate routes `START TRANSACTION WITH CONSISTENT SNAPSHOT` to a single tablet. For an unsharded keyspace this happens to give a per-tablet snapshot. For a sharded keyspace the snapshot covers only one shard's view — other shards are read from their own unsynchronised local clocks, breaking cross-shard consistency.

VStream COPY mode is sharded-aware by construction per Vitess RFC #6277 — the same RFC that informed the v0.43.0 dedup fix. The terminal vgtid encodes per-shard positions, and incrementals resume from this vgtid picking up each shard's CDC from its own captured position.

## Migration / Compatibility

- **Drop-in upgrade from v0.43.x for vanilla MySQL, PG, and all non-PS engines** — `OpenBackupSnapshot` for `FlavorVanilla` and the PG `OpenBackupSnapshot` are unchanged.

- **PS-MySQL backups: chain-resume requires a fresh `sluice backup full`.** Existing backups produced by v0.43.0 or earlier against PS-MySQL sources have wrong-shape EndPositions in their manifests. Those manifests cannot be chained from on v0.44.0 (the position decoder will still reject the binlog shape — this is intentional). Re-take a `sluice backup full` on v0.44.0; that backup's manifest will carry the VStream-shape position and chain cleanly to incrementals.

- **Behaviour change worth flagging**: PS-MySQL backups now spin up a brief gRPC VStream connection to vtgate (in addition to the existing MySQL-protocol connection). Operators with strict outbound-network policies on the backup-running host should ensure the vtgate gRPC port (typically 15991 or per PlanetScale's `vstream_url`) is reachable.

- **Memory profile**: PS-MySQL backups now buffer all rows for all tables in memory before the orchestrator drains table-by-table (matches the live-sync coldStart trade-off — sluice's v1 simple-mode workloads fit comfortably; very large sharded keyspaces would need a streaming variant in a future revision).

## Who needs this release

- **Anyone running `sluice backup full` against PlanetScale-MySQL and trying to chain incrementals or stream-run rollovers off it**: **upgrade immediately**. Pre-v0.44.0 the chain was always broken. Take a fresh full backup on v0.44.0 to seed a working chain.
- **PS-MySQL backup operators who only ever take full backups (no chain)**: still benefits — the v0.44.0 snapshot path uses Vitess's documented multi-shard consistency contract, whereas the pre-v0.44.0 pinned-conn snapshot was single-tablet only.
- **Vanilla MySQL backup operators**: drop-in; no behaviour change.
- **PG backup operators**: drop-in; this release doesn't touch the PG path.

## Verification surface

- **`internal/engines/mysql/backup_snapshot_vstream_test.go` (new)** — two routing unit tests:
  - `TestEngine_OpenBackupSnapshot_FlavorBranchRoutes` confirms `FlavorPlanetScale` routes to the VStream-COPY path (error message contains "vstream" when dial fails)
  - `TestEngine_OpenBackupSnapshot_VanillaDoesNotUseVStream` confirms `FlavorVanilla` does NOT route to the VStream path (guards against future routing regressions)
- **End-to-end validation deferred to operator re-test against real PlanetScale.** The unit-test layer pins the routing decision; real-world re-test pins the position-shape contract. A `psverify`-tagged backup-chain-resume test will land in a follow-on release once the wild validation confirms the fix.

## Issue tracker after v0.44.0

| # | State | Resolution |
|---|---|---|
| 12 | ✅ Closed | v0.40.0 — CDC generated-column filter |
| 13 | ✅ Closed | v0.42.0 — bounded retry on transient applier errors (ADR-0038) |
| 14 | ✅ Closed | v0.43.0 — VStream COPY-phase dedup |
| 15 | ✅ Closed | v0.41.0 — pre-CDC anchor write |
| 16 | ✅ Closed | v0.44.0 — **PlanetScale backup chain-resume via VStream COPY** |

Five production-reported bugs closed in 24 hours via six sequential releases. Every fix shipped with a real root-cause, not a workaround.
