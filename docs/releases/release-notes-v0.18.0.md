# sluice v0.18.0

Closes the v0.17.2-documented "during-backup write window" gap. Full backups now wire the row sweep into a snapshot-anchored consistent view and capture `EndPosition` at snapshot **start**, so the chain's next link's CDC stream from `EndPosition` forward picks up every write after the snapshot. **Backup-only DR is now byte-perfect under heavy write load** — no continuous `sluice sync start` pairing required for correctness (still recommended for "the chain's tail is fresher than the most recent incremental window" use case).

## Features

- **`ir.BackupSnapshotOpener` optional engine surface.** Returns an `ir.BackupSnapshot` bundle: snapshot-anchor `Position` + a snapshot-pinned `RowReader` + a cleanup closure. Engines that implement it get cross-table snapshot consistency plus a snapshot-anchored `EndPosition` for free.

- **Postgres `OpenBackupSnapshot`** creates a temporary `EXPORT_SNAPSHOT`-shape replication slot named `sluice_backup_anchor_<unix-nanos>` to anchor the snapshot LSN, opens a `*sql.Conn` that imports the snapshot via `SET TRANSACTION SNAPSHOT '<name>'`, and returns a `RowReader` bound to the conn. Reuses `createLogicalReplicationSlot`'s PG-version-adaptive helper (FAILOVER on PG 17+). The temporary anchor slot is dropped on close — distinct from the operator's chain-handoff slot, which is recorded on `EndPosition` alongside the LSN.

- **MySQL `OpenBackupSnapshot`** pins a single `*sql.Conn`, runs `SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ` + `START TRANSACTION WITH CONSISTENT SNAPSHOT`, and captures `@@global.gtid_executed` (or `(file, pos)` in non-GTID mode) inside the same transaction so the recorded position refers to the snapshot's logical clock. All table reads run on this one connection sequentially. **Trade-off vs PG**: MySQL's REPEATABLE READ snapshot is per-session and not shareable across connections (ADR-0019), so multi-conn parallel reads aren't available on this path. MySQL operators running backups under high read parallelism configurations should expect single-conn throughput on the row sweep; mitigation is to run backups during slightly slower windows or accept the consistent-view trade.

- **Graceful fallback when snapshot open fails.** PG environments without `wal_level=logical` can't create the temporary anchor slot; v0.18.0's orchestrator catches the error, logs a clear `WARN` naming the operational implication (chains rooted in this full will have a during-backup window gap), and falls back to the v0.17.x `OpenRowReader` + post-sweep `BackupPositionCapturer` path. One-shot backups on `wal_level=replica` PG keep working; only chain-correctness requires the snapshot path.

## Compatibility

- **No CLI breaking changes.** All `sluice backup` / `restore` / `sync start` flags are unchanged.

- **Wire shape of `Manifest.EndPosition` is unchanged**; only its capture-timing semantics shift. v0.18.0+ fulls record the snapshot-anchor LSN/GTID (captured AT snapshot start); v0.17.0–v0.17.3 fulls record the post-sweep position (captured at end-of-backup). The chain-walker treats both identically — the field is a CDC resume cursor — so existing chains restore unchanged. **Old chains and new chains coexist in a chain history without operator action.**

- **Pre-v0.18.0 chains restore unchanged** via the v0.17.x semantics. Only fulls written by v0.18.0+ get snapshot-anchored `EndPosition`.

- **Operators running PG with `wal_keep_size` tuned for chain cadence don't need to revisit settings.** The snapshot anchor is short-lived (dropped at end of full) and doesn't change the chain's WAL footprint at the chain-handoff slot.

- **`ir.BackupSnapshotOpener` is optional**; out-of-tree engines that don't implement it fall through to the v0.17.x path with a clear WARN.

## Closed

- **v0.17.2's documented "during-backup write window" caveat.** The v0.17.2 release notes surfaced this as a known limitation with the workaround "pair backups with continuous `sluice sync start`." v0.18.0 closes it — backup-only DR is correct even under heavy write load on the source. The mitigation pattern is still recommended for the "fresher than the most recent incremental window" use case, but is no longer load-bearing for chain correctness. v0.17.2's live release body has been amended to surface the closed-in-v0.18.0 status.

## Who needs this

- **Anyone building toward zero-rebuild disaster recovery on backups alone** (no continuous CDC stream alongside). v0.17.2 required the continuous-CDC pattern for byte-perfect correctness; v0.18.0 makes the backup-only path correct on its own.

- **Operators of write-busy sources** who couldn't take backups during quiet windows. v0.17.2's caveat made backups under heavy write load potentially miss in-window writes; v0.18.0 captures them via the snapshot's consistent view.

## Operator notes

- **PG**: `wal_level=logical` is required for the snapshot path (same as for incrementals). Operators on `wal_level=replica` will see a clear WARN at backup time naming the gap and fall back to the v0.17.x shape — one-shot backups still work, but chain-correctness requires the snapshot path. Set `wal_level=logical` and restart if you want the gap closed.

- **MySQL**: row sweep is single-conn per session-snapshot constraint. Throughput-sensitive backups previously running with multiple workers via `OpenRowReader` will lose that parallelism on the snapshot path. Mitigation: run backups during slower windows, or accept the trade for byte-perfect chains.

- **Anchor-slot lifetime**: the temporary `sluice_backup_anchor_<unix-nanos>` slot exists only for the duration of the full backup; it's dropped on close. It does NOT survive a sluice-process crash mid-backup — operators who notice an orphan anchor slot post-crash should drop it via `sluice slot drop --target-driver=postgres --target=$SRC --slot-name=sluice_backup_anchor_<unix-nanos>` or the equivalent psql `pg_drop_replication_slot()` call.

## What's next

Phase 4 — **continuous-incremental long-running stream** (`sluice backup stream`) — is the next chunk on the backup track. Builds on the Phase 3 + 3.3 storage / restore / handoff plumbing to add a long-running process that produces rolling incrementals at a configured cadence. Manifest update under concurrent writers + operator UX for the long-running mode are the load-bearing design pieces.
