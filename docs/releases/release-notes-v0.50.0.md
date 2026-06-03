# sluice v0.50.0 — `sluice backup prune` (GitHub #20 chunk 14c)

**Second chunk of roadmap item 14** (the backup-chain retention/compaction track, following v0.47.0's `chain.json` catalog keystone). Operators of long-running `backup stream run` chains now have a first-class primitive to bound disk usage and restore time without writing cron.

## Added

- **`sluice backup prune --from-dir DIR [--keep-incrementals N | --keep-duration DUR] [--dry-run]`** — drop the oldest incrementals from an existing chain to bound disk + restore time. The full at the chain root is always preserved. Mutually exclusive flags; one required.

- **First-kept manifest re-stitch**. When the drop set includes the parent of the first surviving incremental, that incremental's manifest gets rewritten in place: `ParentBackupID` re-anchors to the chain-root full, `StartPosition` re-anchors to the full's `EndPosition`. Chain restore's parent-link walk + `StartPosition` validation pass on the re-stitched chain. **Event windows in the DROPPED incrementals are LOST from the chain's restorable range** — operator opts into this via the explicit prune command; `PruneResult.EarliestRestorableBackupID` records the new earliest restorable point.

- **`docs/dev/notes/prep-backup-chain-rotation.md` (new)** — design prep for chunk 14b (`--retain-rotate-at`, planned v0.52.0+). Captures the snapshot/CDC overlap correctness concern (new full's snapshot anchor must be ≥ previous chain's last-committed incremental position), the inline-rotation FSM, and the `chain.json` schema additions planned for that release. Captured pre-implementation per CLAUDE.md's design-first feedback for non-trivial chunks.

## Migration / Compatibility

- **Drop-in upgrade from v0.49.x.** Pure additive feature; no behaviour change for operators who never invoke `sluice backup prune`.
- **`chain.json` schema unchanged** at this release. v0.50.0 readers continue to load v1 catalogs; v0.50.0 writers continue to produce v1 catalogs. The schema bump (v2) will land alongside 14b's rotation work.
- **Concurrent stream protection**: `sluice backup prune` does NOT lock the chain. Recommended workflow: pause / stop the stream → prune → restart.

## Who needs this release

- **Anyone running `sluice backup stream run` against a local-FS chain that has accumulated meaningful disk usage** (per GitHub #20's evidence — chains past ~10k incrementals start hitting `find` / `ls` slowdowns): drop-in benefit. `sluice backup prune --keep-duration=30d` (or similar) caps chain growth without manual `rm` orchestration.
- **Anyone preparing for the v0.52.0+ rotate-at workflow**: prep doc captures the design; the catalog format bump lands alongside 14b. No action needed in v0.50.0.
- **Anyone whose chains aren't growing problematically**: drop-in, no behaviour change.

## Example usage

```bash
# Inspect what would be dropped (dry-run)
sluice backup prune --from-dir /backups/stream/ --keep-incrementals 100 --dry-run

# Actually prune to keep the most recent 100 incrementals
sluice backup prune --from-dir /backups/stream/ --keep-incrementals 100

# Or keep anything from the last 7 days
sluice backup prune --from-dir /backups/stream/ --keep-duration 168h
```

## Verification surface

- 8 new unit tests in `internal/pipeline/chain_prune_test.go` covering: basic keep-N-most-recent, keep-duration with `Now` injection, no-op cases, dry-run mode, mutual-exclusion + at-least-one validation gates, catalog-absent refusal, and structural-break refusal for hand-corrupted chains.
- **End-to-end validation deferred to operator re-test** — the validation rig at `sluice-validation/` has the 4-table chain that can be pruned + restored to verify the re-stitch + restore-side behavior.

## What's NOT in v0.50.0 (scope decisions)

- **Chunk 14d (compact)**: deferred to v0.51.0. Initial scope estimate underestimated the complexity (chunk encoding, encryption envelope handling, SHA tracking across merged chunks, schema-delta merging). Cleaner as its own release.
- **Chunk 14b (rotate-at)**: deferred to v0.52.0+. Prep doc landed in this release; the implementation needs the snapshot/CDC overlap design to be reviewable before code.

## Issue tracker after v0.50.0

| # | State | Resolution |
|---|---|---|
| 12–17, 19, 21, 22, 25, 26 | ✅ Closed | v0.40.0–v0.49.0 |
| 18 | 🟡 Open (in progress) | Phase 1+2 shipped v0.45.0; Phase 3 (AIMD) pending operator telemetry |
| 20 | 🟡 Open (in progress) | 14a (v0.47.0) + **14c (v0.50.0)** shipped; 14d (v0.51.0) + 14b (v0.52.0+) queued |
| 23 | 🟡 Open (Phase A shipped) | v0.48.0 heartbeat + pprof; Phase B pending operator goroutine dump |
| 24 | 🟡 Open (planned) | PII redaction; roadmap entry pending |
