# Logical Backups Phase 3 — Implementation Design

Supplement to [`design-logical-backups.md`](design-logical-backups.md) (the original proto-ADR) and [`design-logical-backups-phase-2.md`](design-logical-backups-phase-2.md) (cloud backends + resumable writer). This file covers Phase 3: **incremental backups + backup-chain → CDC handoff**, the load-bearing piece that closes the resync-avoidance story for irrecoverable position loss.

The headline operator outcome: when CDC position is gone (PG slot dropped past `wal_keep_size`, MySQL binlog purged), restoring the backup chain leaves the target ready for `sync start` to resume CDC from the chain's terminal position **without re-bulking from source**.

## Scope

**In scope (Phase 3):**

- `sluice backup incremental --since <backup-id>` writes serialised `ir.Change` events to rolling chunk files
- `sluice restore --from=<chain-url>` walks `[full, incr, incr, …]` in order
- `sluice sync start --position-from-manifest` reads the chain's terminal position and resumes CDC from there
- Schema evolution within a chain (option (b): per-incremental schema fingerprint + replay of schema deltas at restore time)
- Operator-facing soft warnings when PG `wal_keep_size` looks insufficient for the chain's cadence
- Pointer to the idle-slot failover trap doc (`docs/postgres-source-prep.md`) when `--position-from-manifest` is used against PG

**Deferred to Phase 4 (continuous-incremental, separate chunk after Phase 3 verifies clean):**

- `sluice backup stream` long-running process producing rolling incrementals
- Manifest update under concurrent writers
- Operator UX for the long-running mode

**Deferred to Phase 4.5 (backup-as-broker, after Phase 4 stabilizes):**

- `sluice sync from-backup` watcher that polls the chain and replays incrementals into a target
- Decoupled source / target sync via backup as the message log

**Deferred to Phase 6 (KMS encryption):**

- Client-side AES-256-GCM remains unimplemented through Phase 3 (and v0.16.0's docs were corrected on this point in v0.16.1)

## Implementation note: deviation from "snapshot-anchored EndPosition" (v0.17.2)

v0.17.2 implemented Phase 3.3.A with a lighter shape than the design originally proposed. The original design said: "use the existing snapshot infrastructure to capture `consistent_point` at slot creation" — i.e. open an `EXPORT_SNAPSHOT`-style transaction during the full backup, take the snapshot LSN as `EndPosition`, and have the row sweep read from inside that transaction.

The actual v0.17.2 implementation:
- Adds `ir.BackupPositionCapturer` as a `SchemaReader`-attached optional interface.
- Captures `pg_current_wal_lsn()` (PG) or `@@global.gtid_executed` (MySQL) at end-of-backup, after all row chunks have been written.
- The row sweep continues to use plain `OpenRowReader` (no shared snapshot transaction across tables).

**What this trades:**

- **Lighter implementation** — no coupling between full-backup orchestrator and CDC slot lifecycle. Operators don't need backup-time slot creation; they manage slots independently via `sluice sync start`.
- **Aligns with "Contain Postgres complexity"** — slot lifecycle stays explicit at the operator's hand, not implicit in backup runs.
- **Loud-failure path preserved** — the 3.3.C preflight catches missing/lost slots before CDC opens for chain handoff.

**The cost — the "during-backup write window" gap:**

- Writes that land on tables already read by the row sweep, before `EndPosition` capture, are NOT in the row chunks AND are not covered by the first incremental's `--since=<full>.EndPosition` window (they're before that point).
- Cross-table consistency at full-backup time is also not guaranteed (also a pre-existing characteristic of `OpenRowReader`-based backups, not new in v0.17.2).
- The chain → handoff path is therefore not byte-perfect for sources under heavy write load during the backup window itself.

**The intended operational mitigation:**

- Pair backups with a continuously-running `sluice sync start` CDC stream. The live stream captures every write as it happens; the backup chain provides cold-bootstrap; the CDC stream's idempotent apply fills any chain gap on restore.
- For backup-alone DR (no live CDC), take backups during quiet windows.
- A future release (likely paired with Phase 4) will wire the full-backup row sweep into the existing snapshot infrastructure (`EXPORT_SNAPSHOT` + `SET TRANSACTION SNAPSHOT` for PG; `WITH CONSISTENT SNAPSHOT` for MySQL), closing the gap with snapshot-anchored `EndPosition`.

**Why ship v0.17.2 with this gap:**

- The gap is pre-existing in v0.17.0/v0.17.1 — those releases had a *larger* gap (incremental started at "current LSN at incremental-start time" — well after backup completion). v0.17.2 narrows the gap to just the during-backup window.
- The continuous-CDC pattern is the recommended DR shape anyway; operators using sluice for ongoing replication get the gap closed for free.
- The snapshot-anchored fix is non-trivial integration work; deferring it to a focused chunk avoids piling complexity onto Phase 3.3's already-substantial scope.

## Sub-phasing

Per the user's preference for "ship 3.1 + 3.2 → test → 3.3 → test" sequencing — keeps blast radius small and lets restore-correctness issues surface before they pile up under handoff complexity.

| Sub-phase | Scope | LOC est. | Subagent slot |
|---|---|---|---|
| **3.1 — Incremental writer** | New `sluice backup incremental --since <backup-id>` subcommand. Reuses the existing CDC pump from `internal/engines/{mysql,postgres}/cdc_reader.go`. New `internal/pipeline/incremental.go` orchestrator that wires the CDC reader's output channel into a chunk writer that emits serialised `ir.Change` events. Manifest gains a `Kind: "full"|"incremental"`, `ParentBackupID`, `StartLSN`/`EndLSN` (PG) or `StartGTID`/`EndGTID` (MySQL). | 800–1000 | First subagent (combined with 3.2). |
| **3.2 — Chain-aware restore** | `sluice restore --from=<chain-url>` walks the chain. New `internal/pipeline/chain_restore.go` that lists manifests at the URL, sorts by `Kind` + `ParentBackupID` linkage, applies the full first then each incremental in order. Reuses the existing applier path (already idempotent per ADR-0010). | 300–500 | First subagent (combined with 3.1). |
| **3.3 — CDC handoff** | New `sluice sync start --position-from-manifest <chain-url>` flag. Reads the chain's terminal manifest, extracts terminal_LSN / terminal_GTID, and starts CDC from there. **MySQL**: `SET GLOBAL gtid_purged = '<set>'` on target, then start; clean. **PG**: pre-flight checks + soft warnings (see §"PG operator UX" below). | 200–400 | Second subagent (after 3.1 + 3.2 test cycle). |
| **CI integration** | Backup-chain restore → CDC handoff round-trip on both engines via testcontainers. | 300–500 | Bundled with each sub-phase's subagent as appropriate. |
| **Total** | | ~1600–2400 | Two subagents. |

## Schema evolution within a chain (Open Question #1, resolved)

Recommendation: **option (b) — per-incremental schema fingerprint + restore-side replay of schema deltas.**

### How it works

- Every full and incremental manifest carries `SchemaHash string` (deterministic hash of the IR's `Schema`).
- When the CDC pump observes a DDL event (PG: RELATION-message change with new column types; MySQL: binlog DDL row), the incremental manifest currently being written records a `SchemaDelta` entry: `{kind: "alter_table", table: "...", before: <IR>, after: <IR>}`.
- Restore walks the chain in order; before applying each incremental's row events, applies the schema deltas in that incremental's manifest first.
- If the chain hits an unsupportable shape (column dropped + new column with same name in the same chain, ambiguous type changes), restore refuses with a clear message naming the offending incremental + suggesting "force fresh full + new chain."

### Why (b) over (a) or (c)

- **(a) refuse + force fresh full** is operationally heavy — every schema change becomes a re-bulk, which defeats the point of incremental backups for low-volatility schemas.
- **(c) full schema snapshot in every incremental** is wasteful (most incrementals carry no schema change) and complicates restore because the diff against the previous manifest's schema is the source of truth, not the snapshot.
- **(b)** is medium complexity, matches the operator's mental model ("I made an ALTER, the next incremental captured it, restore applies it"), and degrades gracefully — falls back to (a)'s "force fresh full" only when the chain encounters something genuinely unrepresentable.

### Out of scope for v1

- Multi-table-source backups for sharded sources (Vitess) — see Open Question #2 in the original proto-ADR. Probably per-shard chain.
- Cross-engine schema delta translation. Phase 3 chains are same-engine restore only; cross-engine restore from a chain is a Phase 5+ topic.

## CDC handoff — engine-specific operator UX

Reiterating from the Phase 2 supplement's "Backup-chain → CDC handoff" section, with concrete CLI shape now that we're implementing it.

### MySQL (clean path)

```
sluice restore --from=s3://backups/chain-2026-05-07 \
    --target-driver=mysql --target=$DEST
sluice sync start --position-from-manifest=s3://backups/chain-2026-05-07 \
    --source-driver=mysql --source=$SRC --target=$DEST --stream-id=catchup
```

Implementation: `--position-from-manifest` reads `terminal_GTID` from the latest manifest in the chain. Sluice runs `SET GLOBAL gtid_purged = '<set>'` on the target before starting the binlog reader. Source streams everything not in the GTID set. **Pre-flight check**: query source's `binlog_expire_logs_seconds`; if the chain's terminal_GTID is older than `now - binlog_expire_logs_seconds`, surface a soft warning: `WARN binlog may not cover [terminal_GTID, current_GTID]; consider taking a fresh full backup or shortening the chain interval`.

### Postgres (operator-attention path)

```
sluice restore --from=s3://backups/chain-2026-05-07 \
    --target-driver=postgres --target=$DEST
sluice sync start --position-from-manifest=s3://backups/chain-2026-05-07 \
    --source-driver=postgres --source=$SRC --target=$DEST --stream-id=catchup
```

Implementation: `--position-from-manifest` reads `terminal_LSN` from the latest manifest in the chain. Sluice attempts to create a fresh slot on the source at that LSN. PG's catch: `pg_create_logical_replication_slot()` creates at the **current** server LSN, not arbitrary historical. So:

1. **If the slot the chain was using still exists and is healthy** (not lost / not invalidated), reuse it. This is the happy path when sluice was running continuously alongside the backup chain — the slot's `restart_lsn` already covers the chain's terminal.
2. **If the slot is gone but `wal_keep_size` covers `[terminal_LSN, current_LSN]`**, create a fresh slot. PG advances it to the current LSN, but the WAL we need is still on disk so the consumer's start position can be the chain's terminal — sluice will read everything from terminal_LSN onward, dropping into the slot's "live" window once it catches up.
3. **If `wal_keep_size` does NOT cover the gap**, the WAL needed to bridge is gone. **Refuse with a loud error** naming the gap size in bytes + the operator-actionable next step (take a fresh full backup OR adjust `wal_keep_size`).

### PG soft warnings (the user-input piece)

Per the user's input on Open Question #4 (`wal_keep_size` UX), Phase 3 implements **soft warnings** rather than hard refusals — operators have legitimate reasons to override our threshold (their backup cadence + WAL volume math may be self-consistent without our guess).

`sluice sync start --position-from-manifest` runs these pre-flight checks against PG sources, regardless of whether the slot exists or not:

1. **`wal_keep_size` (PG 13+) / `max_slot_wal_keep_size`** sufficiency. Query `SHOW wal_keep_size`; estimate average WAL volume per minute from `pg_stat_wal` over the chain's incremental cadence; if `wal_keep_size < (incremental_interval × wal_volume_per_minute × 2)` (the "2" is a safety margin), `WARN wal_keep_size of <X> may not cover the worst-case CDC catch-up window from your backup chain (estimated <Y> WAL/incremental). Consider increasing.`
2. **The idle-slot failover trap** (the user's 2026-05-07 finding). If the source is Patroni-managed (detect via `pg_settings.name LIKE '%patroni%'` or `application_name`), surface: `WARN this PG cluster is HA-managed (Patroni). The slot you're starting CDC from is subject to the idle-slot failover trap (see docs/postgres-source-prep.md). Ensure the slot is being actively consumed; for low-traffic sources, consider a heartbeat-write strategy.`
3. **Slot existence + health.** `SELECT wal_status FROM pg_replication_slots WHERE slot_name = ?`. If `wal_status = 'lost'` or row is missing, refuse with the slot-recovery flow that already exists for `sync start --resume`.

The first two are warnings (proceed unless `--strict-preflight` is supplied); the third is a refusal because the slot can't deliver what we need.

## Acceptance criteria

A clean Phase 3 must:

1. **Take an incremental backup against a chain.** `sluice backup incremental --since=<full-backup-url>` writes a new incremental manifest + chunks. Manifest carries `Kind: "incremental"`, `ParentBackupID`, `StartLSN`/`StartGTID` (= parent's terminal), `EndLSN`/`EndGTID` (= position at end of write window), `SchemaHash`, optional `SchemaDelta`.
2. **Chain restore round-trip.** `sluice restore --from=<chain-url>` into a fresh target produces the same data state as the live source at the chain's terminal position. Verified via `sluice verify --depth sample` post-restore on both PG → PG and MySQL → MySQL.
3. **Cross-engine chain restore (basic).** PG full + incrementals → MySQL target via existing `RetargetForEngine`. Schema deltas in the chain translate where possible; loud-failure on unsupportable shapes (e.g. PG-native types in a delta).
4. **Schema evolution survives a chain.** Take full → ALTER TABLE on source → take incremental → restore the chain. Target reflects the post-ALTER schema + data.
5. **CDC handoff (MySQL).** Restore chain → `sync start --position-from-manifest` → CDC catches up from the chain's terminal_GTID. Source binlog must still cover the gap (no test-time forced fail; just verify the happy path).
6. **CDC handoff (PG, slot exists).** Restore chain → `sync start --position-from-manifest` → CDC reuses the existing slot (whose `restart_lsn` covers the chain's terminal_LSN) and catches up.
7. **CDC handoff (PG, slot gone, wal_keep_size sufficient).** Drop slot before handoff → `sync start --position-from-manifest` → sluice creates a fresh slot, reads from terminal_LSN, catches up. Soft warning emitted about the slot-creation; no refusal.
8. **CDC handoff (PG, wal_keep_size insufficient).** Drop slot, write enough WAL to push past `wal_keep_size`, then `sync start --position-from-manifest` → loud refusal naming the gap; operator must take a fresh full backup or adjust `wal_keep_size`.
9. **Soft warnings fire correctly.** Set `wal_keep_size` to something tiny on a chatty source → `--position-from-manifest` warns about the cadence math; passes (warning, not refusal). `--strict-preflight` flips it to a refusal.
10. **Idle-slot trap warning fires on Patroni-managed sources.** Mocked or detected via `application_name`.

CI: testcontainer-based round-trip for criteria 1, 2, 5, 6, 7. The wal_keep_size cases (8, 9) are unit-tested via mocked engine surfaces; PG testcontainer doesn't easily reproduce the "WAL is gone" scenario without elaborate setup.

## CLI surface

| Command | Phase 3 work |
|---|---|
| `sluice backup full` | Unchanged. Already in v0.16.x. |
| `sluice backup incremental --since=<backup-id\|url>` | NEW. Writes incremental manifest + chunks; references parent. |
| `sluice backup verify --from=<url>` | EXTENDED. Now walks chain (full + incrementals) and verifies all chunks. |
| `sluice restore --from=<url>` | EXTENDED. Detects chain shape vs single-backup; walks chain in order. |
| `sluice sync start --position-from-manifest=<url>` | NEW flag on existing command. Reads chain's terminal position. Mutually exclusive with `--resume` (different position sources). |
| `sluice sync start --strict-preflight` | NEW flag on existing command. Promotes Phase 3 soft warnings to refusals. |

## Sluice-side mitigation for PG idle-slot trap (orthogonal, smaller chunk)

Worth filing as a separate bug-fix-grade follow-up after Phase 3:

> **Bug 35 candidate**: sluice's PG CDC reader sends `pg_send_standby_status_update` keepalives every 10s, but does NOT send any WAL-write activity from the consumer side. On Patroni-managed PG, this means the slot's `confirmed_flush_lsn` only advances when the *source* writes — not when sluice keeps the connection alive. For low-traffic sources, sluice could optionally inject `pg_logical_emit_message` heartbeat writes (gated behind `--heartbeat-interval`) so the slot stays advanced regardless of source write volume. Tier 2 mitigation in `docs/postgres-source-prep.md` becomes built-in.

Out of Phase 3 scope; capture for the post-Phase-3 backlog.

## See also

- [`design-logical-backups.md`](design-logical-backups.md) — original proto-ADR
- [`design-logical-backups-phase-2.md`](design-logical-backups-phase-2.md) — cloud backends + resumable writer (v0.16.x)
- [`../postgres-source-prep.md`](../postgres-source-prep.md) — operator setup including the idle-slot failover trap (load-bearing for Phase 3 PG handoff)
- ADR-0010 (idempotent CDC apply) — the load-bearing assumption for chain replay
- ADR-0022 (slot-missing fall-through) — the existing recovery path that Phase 3 supplements but does not replace
