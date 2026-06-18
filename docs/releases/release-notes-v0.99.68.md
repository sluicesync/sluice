# sluice v0.99.68

**HIGH latent-race fix — the cold-start→CDC handoff now joins a COPY-completion barrier before reading the CDC-resume `Position`, closing a data race (and a potential silent-gap window) on the VStream auto-shard / concurrent-COPY paths (ADR-0095 / ADR-0099).** Drop-in: nothing changes in your invocation, no flag or token-shape change, and the handoff now deterministically resumes from the correct stitched position. Cold-start copies themselves were always correct — this hardens the snapshot→CDC seam. Real-world exposure was very low (the handoff almost always lost the race so `Position` was already populated, and the reliably-triggering case is a concurrent-resume path not in any shipped release), but it was a genuine latent race and is now closed.

## Fixed

- **Cold-start→CDC handoff joins a COPY-completion barrier before reading `Position` (HIGH, correctness / latent silent-gap).** The VStream auto-shard (ADR-0095, v0.99.63) and cross-table concurrent COPY (ADR-0099, v0.99.67) paths close each table's row channel on a *per-table* completion signal, so the orchestrator's bulk-copy drain could return BEFORE the producer goroutine finished stitching and writing the stitched-minimum CDC-resume `Position`. The cold-start→CDC handoff then read `stream.Position` racing that write — two consequences: a data race on the `Position` field (caught by a CI `-race` run), and a window where the handoff could observe an empty/stale `Position`, pick the wrong CDC start position, and leave a potential gap at the snapshot→CDC seam. The fix adds an optional engine-supplied COPY-completion barrier (`ir.SnapshotStream.WaitCopyCompleteFn`, wired on the MySQL VStream snapshot to a `copyDone` channel the COPY pump closes only AFTER `finishCopy`/`finishCopyAutoShard` writes `Position` under its mutex); the handoff joins that barrier after bulk-copy drains and BEFORE it reads `Position`, establishing the happens-before edge — write `Position` under `mu` → close `copyDone` → handoff waits → handoff reads `Position`. The barrier is nil-safe (non-VStream paths leave it unset → no-op, since their row-channel close already orders the read), idempotent (an already-closed channel returns immediately), and ctx-cancellable (a shutdown mid-wait unwedges). The single-stream (non-auto-shard) VStream path was never affected — its row-channel close already orders the `Position` write. Honesty note: this was always loud-or-correct in practice (a wrong `Position` surfaces as a re-stream/overlap or a gap, not exit-0 with silently-wrong data in the normal case), and every regression cycle and live large-scale run observed clean handoffs; the reliably-triggering path is the concurrent-resume case that isn't in any shipped release. Pinned by `TestVStreamSnapshot_AutoShard_WaitCopyCompleteOrdersPosition` (asserts the barrier hook is wired — guarding the zero-value-nil-hook trap — and that `Position` is non-empty and equals the stitched per-shard minimum once the barrier returns), and by the CI `-race` Integration (vstream) job, which proves the race itself is gone and is the merge gate (CGO is off locally).
  - **Who's affected:** anyone running `sluice sync` from a Vitess/PlanetScale source over a multi-table keyspace on the auto-shard (v0.99.63+) or concurrent-COPY (v0.99.67) cold-copy path. The cold-start→CDC handoff now deterministically resumes from the correct stitched position; cold-start copies themselves were correct, so there is nothing to re-copy.

## Compatibility

- **No breaking changes; drop-in upgrade.** No flag, default, or behavior change to a successful sync; the persisted handoff/position token shape is unchanged (no new field). The barrier is an internal happens-before edge, invisible above the `SnapshotStream` boundary.
- **Scope: the `sluice sync` / VStream (Vitess/PlanetScale-source) cold-start→CDC handoff on the multi-table auto-shard (ADR-0095) and concurrent-COPY (ADR-0099) paths.** The single-stream VStream path was unaffected (its row-channel close already orders the `Position` write). The vanilla MySQL binlog, PostgreSQL cold-copy, and `sluice migrate` paths are untouched — they drain rows in a way that already orders the `Position` read, so the hook is left nil and the handoff is a no-op there.
- **Affected releases:** the auto-shard variant since **v0.99.63** (ADR-0095) and the concurrent-COPY variant since **v0.99.67** (ADR-0099). The single-stream path in those and all earlier releases is not affected.

## Who needs this — action required

- **Anyone running `sluice sync` from a Vitess/PlanetScale source over a multi-table keyspace on v0.99.63–v0.99.67 (auto-shard or concurrent COPY):** upgrade to v0.99.68. No re-verification or re-copy of completed syncs is required. In practice the handoff lost the race so `Position` was populated, and every regression cycle plus every live large-scale run observed clean handoffs; the reliably-triggering case is a concurrent-resume path that shipped in no release. If you want belt-and-suspenders assurance on a sync that cold-started on v0.99.63–v0.99.67 and is in doubt, the at-least-once seam means a forced re-stream from the recorded position is idempotent (ADR-0010 upsert / Bug-125 idempotent copy) — but no proactive action is needed.
- **Cold-start copies themselves needed no fix and need no re-check.** This change hardens the handoff *seam* (the read of the CDC-resume position), not the row copy; the bulk copy was always correct and gapless.

---

## Install

```
brew install sluicesync/tap/sluice
go install sluicesync.dev/sluice/cmd/sluice@v0.99.68
# Linux/macOS/Windows binaries + checksums on the release page.
docker pull ghcr.io/sluicesync/sluice:v0.99.68
```

**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
