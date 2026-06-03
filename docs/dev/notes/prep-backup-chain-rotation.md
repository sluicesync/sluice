# Prep: backup-chain rotate-at (GitHub #20 / roadmap 14b)

When to pick this up: after v0.50.0 ships 14c (prune) + 14d (compact). 14b is the larger chunk; this doc captures the architecture so the implementation pass doesn't have to re-derive it.

## What 14b is

A new `--retain-rotate-at=DUR` flag on `sluice backup stream run`. When the active chain reaches the configured age, the stream:

1. Commits a final incremental at the current CDC position.
2. Opens a fresh `backup full` to a sibling directory.
3. Tombstones the previous chain (per `chain.json` schema from 14a).
4. Continues streaming, with subsequent incrementals chaining off the new full.

Operator gets bounded chain length + bounded restore time **without writing cron**. Pairs with the operator-controlled prune (14c) and compact (14d) chunks: rotate-at gives a natural break-point for prune, and bounded chain length means compact has fewer-per-chain incrementals to process.

## The load-bearing correctness concern

The new full's snapshot anchor MUST be `>=` the previous stream's last-committed incremental position. If the new full's snapshot anchor is `<` the previous incremental's end-position, the chain handoff has a gap window:

- Old chain: incremental N committed at position P_N
- New chain rotation: opens new snapshot at position P_S, where P_S < P_N
- Writes to source between P_S and P_N are captured in incremental N (old chain) but **not** in the new chain — restoring from the new chain alone produces a target that's missing those writes.

The handoff must guarantee: `new_full.snapshot_anchor >= P_N` for the LAST committed incremental of the old chain.

## Architectural options

### Option A — Inline rotation (recommended for v0.51.0)

The same `BackupStream.Run` goroutine that owns the streaming CDC pump also drives the rotation:

1. Hit `--retain-rotate-at` threshold.
2. Wait for next TxCommit boundary, then drain the in-flight rollover and commit final incremental at position `P_N`.
3. **Same goroutine** opens the source's snapshot stream — the source's CDC reader knows the current position from the same gRPC handle (MySQL VStream) or the same logical-slot read connection (PG). The snapshot anchor returned by `OpenBackupSnapshot` against the current source state is, by construction, `>= P_N` — the source can't go backwards in position-monotonic terms.
4. Bulk-copy source rows for the new full via the same connection / shard scope.
5. Write the new full's manifest under a sibling directory (e.g. `<root>/rotated-<unix-ms>/`).
6. Mark the old chain's `chain.json` entries with `tombstoned: true` and add a pointer field (`succeeded_by: <new_chain_root_id>`) so restore tooling can chain across rotations if desired.
7. Resume CDC from the new full's snapshot anchor (`P_S >= P_N`).
8. Continue streaming.

**Why inline**: the position-monotonic invariant is intrinsic. A separate process restarting against the source between step 2 and step 3 has no position guarantee — replication slot advancement, time-shifts, etc. can introduce gaps. Same-process handoff sidesteps the problem.

### Option B — Separate rotation process

A new `sluice backup rotate --from-dir CHAIN --to-dir <new-root>` command operator runs against an already-stopped stream. Same logic as inline but no guarantee the source position hasn't advanced past `P_N` in ways the operator can't observe.

**Status**: not recommended for v0.51.0. Mention in release notes as a possible future variant; defer until an operator surfaces a need (e.g. cross-machine rotation).

## State machine (Option A)

```
[STREAMING] --new rollover-->  [STREAMING]            (loop)
[STREAMING] --rotation tick--> [ROTATING:DRAIN]
[ROTATING:DRAIN] --drain done--> [ROTATING:SNAPSHOT]
[ROTATING:SNAPSHOT] --snapshot done--> [ROTATING:BULKCOPY]
[ROTATING:BULKCOPY] --copy done--> [ROTATING:COMMIT]
[ROTATING:COMMIT] --tombstone old chain--> [STREAMING]
[any] --ctx cancel--> exit cleanly
```

Each transition writes a marker to a `rotation-state.json` in the new chain's root, so an external `kill -9` mid-rotation leaves a recoverable state. The next `BackupStream.Run` reading the destination sees the marker and:

- `DRAIN` → resume drain
- `SNAPSHOT` → retry snapshot open (the old chain's terminal position is still recoverable from old chain's `chain.json`)
- `BULKCOPY` → resume per-table bulk-copy (table-level checkpoint already exists from v0.16.1)
- `COMMIT` → tombstone old chain + resume

## chain.json schema additions (forward-compat from v0.47.0)

The v0.47.0 catalog has a `tombstoned bool` placeholder field. 14b uses it for the first time AND adds three more fields:

```jsonc
{
  "format_version": 2,  // bumped from 1; v0.47.0 readers refuse loudly
  "rotated_at": "2026-06-01T12:00:00Z",  // when this chain was tombstoned
  "succeeded_by": "<new_chain_root_backup_id>",  // chain.json's chain_root_backup_id of the next chain
  "rotation_reason": "rotate-at-threshold | operator-driven | <other>",
  "entries": [
    {
      // existing v0.47.0 fields ...
      "tombstoned": true  // v0.51.0 finally sets this to true
    }
  ]
}
```

**Backwards-compat (v0.47.0 → v0.51.0)**:
- v0.47.0 readers refuse `format_version: 2` with "upgrade sluice" hint — load-bearing per the v0.47.0 catalog code's [forward-version refusal](https://github.com/sluicesync/sluice/blob/main/internal/pipeline/chain_catalog.go).
- v0.50.0 readers (after 14c/14d ship) need to handle `format_version: 2` gracefully even though 14b hasn't shipped yet. **Action**: bump the reader's max-supported version to 2 in v0.50.0; the new fields will be ignored by v0.50.0 but won't trigger the refusal.

This deferred-bump is required to avoid forcing operators to upgrade in lockstep when 14b lands — v0.50.0 + chain-rotated-to-v0.51.0 mixed deployments need to be safe.

## CLI surface

```
sluice backup stream run \
  --output-dir /backups/stream/ \
  --retain-rotate-at 24h \
  ... existing flags ...
```

Plus optional `--rotate-at-chain-length N` (rotate after N incrementals regardless of time — useful for high-volume streams).

The new full lands in `--output-dir/rotated-<unix-ms>/` (sibling to the previous chain). The original `--output-dir` is preserved as the parent; the rotation sibling is in a deterministic subdirectory the operator can locate without reading chain.json.

Alternative considered + rejected: in-place rotation (overwrite the original chain). Rejected because (a) it breaks the operator's ability to restore from the old chain after rotation, (b) it makes the snapshot/CDC handoff invariant harder to verify (the old chain's terminal position is gone).

## What 14b does NOT do

- Doesn't delete the old chain. Tombstoning marks it as "no longer extending"; operator combines 14b + 14c (prune) for the bounded-size goal: rotate-at every 24h → keep last N chains via prune.
- Doesn't run the rotation in parallel with streaming. Streaming pauses for the rotation duration. Time bound: snapshot + bulk-copy of the full data set. For multi-TB sources this is significant; future revision could split into background-bulk-copy + position-handoff at end, but v0.51.0 keeps it simple.
- Doesn't compact the old chain. That's 14d's job; operator chains the commands or schedules them.

## Integration points (code surface estimate)

New file: `internal/pipeline/stream_rotation.go` (~250-350 LOC):
- `rotationState` type (FSM)
- `(*BackupStream).performRotation(ctx) (*ir.Manifest, error)` — runs the full state machine
- `rotation_state.json` read/write helpers
- Tombstone-write helper that updates the old chain's `chain.json`

Modified files:
- `internal/pipeline/stream.go` — rotation tick check in the rollover loop (~30 LOC)
- `internal/pipeline/chain_catalog.go` — bump `chainCatalogFormatVersion` to 2 with the new fields (additive); reader stays backwards-compat
- `internal/pipeline/restore.go` + `chain_restore.go` — honor `succeeded_by` pointer for cross-rotation chain walks (~50 LOC)
- `cmd/sluice/backup.go` — `--retain-rotate-at` + `--rotate-at-chain-length` flags

Testing:
- Unit tests on FSM state transitions
- Integration test: artificial rotation trigger, verify new full's snapshot anchor `>=` old chain's terminal incremental position
- Integration test: restore from rotated chain (read both old and new via `succeeded_by`)
- Crash-mid-rotation recovery (kill at each FSM state, restart, verify resume)

**Size estimate: ~600-800 LOC + tests.** Larger than 14c+14d combined; warrants its own release (v0.51.0) for review.

## Open questions worth flagging before code

1. **Snapshot encryption inheritance on rotation**: the new full's encryption envelope — same chain CEK as the old chain, or fresh-derived? Recommendation: fresh-derived per the new chain (different chain root = different chain CEK). The old chain's CEK stays with the tombstoned chain for restoring from it.

2. **PG `wal_keep_size` / slot consumption during rotation**: the old chain's slot can advance to `P_N` only after the final incremental commits. The new full's snapshot opens a new slot. During rotation there are briefly TWO slots open on the source PG. Worth documenting in `docs/postgres-source-prep.md` so operators don't hit `wal_keep_size` exhaustion.

3. **Backup-broker integration** (Phase 4.5 `sluice sync from-backup`): a rotated chain's broker state needs to advance to the new chain's full when rotation completes, so downstream sync streams continue from the new full's snapshot anchor. Recommendation: out of scope for v0.51.0; brokers stick with the old chain's incrementals until rotated chain's first incremental commits. Document the lag.

## Pre-implementation checklist

Before writing code for v0.51.0:

- [ ] Read this prep doc.
- [ ] Confirm chain.json format_version 2 is acceptable (no chains in operator production today; safe to bump).
- [ ] Decide on the `--retain-rotate-at` threshold default. Suggest 24h as the operator-friendly default; document that "0 = disabled" preserves pre-v0.51.0 unbounded behaviour.
- [ ] Run a local benchmarking rig's continuous workload at a tight `--retain-rotate-at 5m` to exercise the FSM on every state transition + crash-recovery cases.

## Pointers

- 14a chain.json: `docs/dev/notes/prep-chain-catalog.md` + `internal/pipeline/chain_catalog.go`
- Backup-snapshot machinery: `internal/engines/{mysql,postgres}/backup_snapshot*.go`
- Stream rollover loop: `internal/pipeline/stream.go::BackupStream.Run`
- GitHub #20 issue body: roadmap item 14
