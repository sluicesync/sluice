# sluice v0.88.0 — rotated backup chains are compactable (Bug 95)

**Headline:** `sluice backup compact` can now merge across rotation boundaries. Previously, a backup chain rotated under continuous writes (`backup stream run --retain-rotate-at*`) was uncompactable — every rotation left a small position gap that the compactor correctly refused to merge across, so the "rotate a long chain, then compact the churn" workflow was unreachable on a live source. v0.88.0 (ADR-0067) makes rotated chains **born-contiguous** so both the naive and smart compactors merge them and the merged chain restores cleanly. Drop-in from v0.87.0 — no config, schema, IR, or lineage-format changes.

## Fixed

- **Rotated backup chains are now compactable (Bug 95, ADR-0067).** The rotation handoff used to drop the `(P_N, S]` window of changes between the prior segment's terminal position `P_N` and the new segment's snapshot anchor `S` — those changes lived only in the new segment's full snapshot, which naive compaction discards, so the §14d contiguity pre-flight refused every merge across a rotation boundary (loud, no data loss). Because update-heavy churn implies continuous writes, *every* boundary gapped, making the compaction value-prop unreachable. The fix keeps the `(P_N, S]` overlap in the new segment's incrementals (replaying CDC from `P_N` instead of `S`), so the lineage is contiguous and compactable — for all table shapes (PK and no-PK) and encrypted or not, since the fix is upstream of the compactor. A new additive `lineage.json` field `incremental_coverage_start` records where each segment's incrementals begin; the contiguity checks key off it while `start_position` keeps its full-anchor meaning. The kept overlap re-applies idempotently on restore (the same snapshot→CDC handoff dedup sluice already proves for the initial full→stream transition). A genuine position gap (a pre-0067, imported, or crash-truncated lineage) is still refused loudly.

## Internal

- **Crash-recovery-honest coverage marker.** `incremental_coverage_start` is derived from the actual first incremental when it commits, not recorded at rotation time — so it stays correct across a crash that resumes at `S` instead of `P_N` (the post-crash segment reads as a normal, non-overlap segment that restores cleanly). Verified by the ADR-0046 crash-injection matrix.
- **Compaction parent-link re-stitch.** Merging real rotated chains (newly reachable in this release) drops the intermediate segments' full snapshots, so each former-segment-first incremental is re-chained to the previous link. Cascade-free, since the backup-ID derivation excludes the parent reference. Without it, a merged real chain would have failed its own restore-walk.
- **Smart-compaction test threshold corrected** to reflect v1's per-incremental collapse granularity (cross-incremental collapse is a separate, out-of-scope follow-on).

## Compatibility

- **Minor version bump (v0.88.0)** — additive, drop-in from v0.87.0. The new `incremental_coverage_start` lineage field is optional (absent → resolves to `start_position`); no lineage-format-version bump, no config / schema / IR changes. Existing one-segment and never-compacted chains restore byte-identically.
- **One behavior change:** rotated chains now retain a small `(P_N, S]` event overlap per rotation boundary, re-applied idempotently on restore (so the restored result is unchanged). This is what makes the chain compactable; `backup compact` collapses the redundancy.

## Who needs this

- **Operators running long-lived `backup stream run` with rotation** (`--retain-rotate-at` / `--retain-rotate-at-chain-length`) who want to periodically `backup compact` the chain to shrink restore time / segment count. Before v0.88.0 those chains refused to compact on a live source; now they merge and restore cleanly.
- **Everyone else:** no action needed — one-segment and never-rotated backups are unchanged.
