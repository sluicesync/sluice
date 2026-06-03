# sluice v0.51.0 — rotation-EXIT thresholds (GitHub #20 chunk 14b phase 1)

**Third chunk of roadmap item 14** (backup-chain retention/compaction, following v0.47.0's chain.json catalog and v0.50.0's `sluice backup prune`). Operators can now bound chain length AND chain age via two new flags that exit `sluice backup stream run` cleanly when either threshold trips; chain.json gets a rotation marker so subsequent tooling can detect the closed-state.

## Scope decision: rotation-EXIT (phase 1) vs in-process rotation (phase 2)

Phase 1 ships the rotation-EXIT signal pattern. Operators wrap `sluice backup stream run` in a cron / systemd / supervisord loop that detects the clean exit and restarts with a fresh `--output-dir` for the next chain.

Phase 2 (planned v0.52.0+) will implement in-process rotation that doesn't require the operator wrapper. The position-monotonic correctness analysis (per `docs/dev/notes/prep-backup-chain-rotation.md`) shows the rotation-EXIT pattern has the same final-state restore correctness as inline rotation: between exit and new run start, source events get absorbed into the new full's snapshot data; no events get silently dropped from the chain's restorable range.

## Added

- **`--exit-after-age=DUR`** on `sluice backup stream run`. After this duration of chain age (computed as `now - chain.json's CreatedAt`), commit the current rollover and exit cleanly. The chain catalog's `RotatedAt` + `RotationReason="retain-rotate-at"` fields get written. Zero disables.

- **`--exit-after-chain-length=N`** on `sluice backup stream run`. After N incrementals committed, exit cleanly. Same chain.json marking with `RotationReason="rotate-at-chain-length"`. Either threshold firing wins; length checked first (cheaper, no I/O). Zero disables.

- **`ChainCatalog` schema extensions**: `RotatedAt`, `SucceededBy`, `RotationReason` fields. Additive + `omitempty`; no format-version bump. `SucceededBy` is reserved for v0.52.0+ inline rotation; v0.51.0 phase 1 leaves it empty.

- **`prefixedStore` wrapper** (`internal/pipeline/prefixed_store.go`). Internal scaffolding for v0.52.0+ inline rotation. Wraps an `ir.BackupStore` with a transparent path prefix. Three unit tests pin the round-trip + edge cases.

## Operator workflow

```bash
# Rotation cadence: 24h or 500 incrementals, whichever first.
TS=$(date -u +%Y%m%dT%H%M%S)
DIR="/backups/chain-$TS"
mkdir -p "$DIR"

# Wrap in systemd / cron / supervisord with Restart=on-success.
exec sluice backup stream run \
    --output-dir="$DIR" \
    --since=<previous-final-incremental-id> \
    --exit-after-age=24h \
    --exit-after-chain-length=500 \
    ...
```

After v0.52.0+ phase 2 lands, the workflow collapses to a single long-running `sluice backup stream run --output-dir=/backups/ --retain-rotate-at=24h` command with no wrapper.

## Migration / Compatibility

- **Drop-in upgrade from v0.50.x.** Both new flags are opt-in.
- **No chain.json schema bump.** The three new fields are additive and `omitempty`-encoded. v0.47.0+ readers continue working unchanged.
- The `prefixedStore` wrapper is internal; no operator-facing API.

## Who needs this release

- **Anyone running `sluice backup stream run` against an unboundedly-growing chain**: upgrade and pair the new flags with a wrapper script. Bounded chain length + bounded restore time without writing application-level rotation logic.

- **Anyone preparing for v0.52.0+ in-process rotation**: v0.51.0 chains marked with `RotatedAt` will be readable by v0.52.0+ readers gaining auto-stitching via `SucceededBy`.

- **Anyone whose chains don't grow problematically**: drop-in, no behavior change.

## Verification surface

- 9 new unit tests in `internal/pipeline/stream_rotation_test.go` covering: both thresholds (fires + not-yet), length-preferred-over-age tie-breaker, none-configured no-op, catalog-absent conservative fall-back, marker-write success + no-op.
- 3 new unit tests in `internal/pipeline/prefixed_store_test.go` for the wrapper's transparency invariants.
- End-to-end validation deferred to operator re-test via the local validation rig — `--exit-after-chain-length=2` is the quickest reproduction.

## What's NOT in v0.51.0

- **In-process rotation** (the same goroutine opens a new snapshot stream + bulk-copies into a new sibling chain root + resumes streaming).
- **`SucceededBy` auto-stitching** across rotations.
- **`--retain-rotate-at=DUR` flag**: reserved name for v0.52.0+ phase 2.

Both deferred to v0.52.0+ per the prep doc at `docs/dev/notes/prep-backup-chain-rotation.md`.

## Issue tracker after v0.51.0

| # | State | Resolution |
|---|---|---|
| 12–17, 19, 21, 22, 25, 26 | ✅ Closed | v0.40.0–v0.49.0 |
| 18 | 🟡 Open (in progress) | Phase 1+2 shipped v0.45.0; Phase 3 (AIMD) pending operator telemetry |
| 20 | 🟡 Open (in progress) | 14a (v0.47.0) + 14c (v0.50.0) + **14b phase 1 (v0.51.0)** shipped; 14b phase 2 (v0.52.0+) + 14d (after 14b) queued |
| 23 | 🟡 Open (Phase A shipped) | v0.48.0 heartbeat + pprof; Phase B pending operator goroutine dump |
| 24 | 🟡 Open (planned) | PII redaction; roadmap entry pending |
