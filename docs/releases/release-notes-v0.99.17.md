# sluice v0.99.17

**Crash-orphaned backup catalog is now self-healing — a rotation crash can no longer leave a backup un-restorable.** A streaming backup that crashed (or was cancelled) at the wrong moment during a segment rotation could lose one incremental from `lineage.json` even though the incremental was durably written to object storage — and a later `restore` would then **refuse the whole segment** as `branching/mis-stitched lineage`, despite the on-disk chain being complete. No data was ever lost (loud refusal, never silent), but the backup was unrestorable until repaired. sluice now reconciles the catalog against the on-disk chain on resume and repairs the gap automatically. Drop-in from v0.99.16.

## Fixed

- **Rotation-crash "branching/mis-stitched lineage" restore refusal.** Each incremental's manifest is written durably *before* its `lineage.json` entry is appended (the append is best-effort, so a transient catalog-write hiccup never fails the live stream). A crash or cancel in that window — classically right after a rotation opens a new segment and writes its first `(P_N, S]` overlap incremental — left that incremental **on disk but missing from the segment's catalog list**. On resume the stream re-stitched off the on-disk tail correctly (data intact), but the catalog kept the head gap, so its first recorded incremental parented off the orphan instead of the segment's full, and `restore`'s strict per-link chain walk refused the segment.

  sluice now **reconciles the open segment's catalog against the on-disk chain on resume** (before resolving the parent), re-cataloguing any orphaned incremental in true chain order — by parent links, not the filename sort, which can tie on same-millisecond rollovers — and re-deriving the segment's coverage-start and end positions. The repair is **conservative and idempotent**: a no-op when the catalog already matches disk, and it **refuses to guess** when the on-disk manifests aren't a single clean linear chain off the full (a branch, a parentless incremental, or an unreachable manifest), leaving those for `restore`'s strict validation to surface rather than masking real corruption.

  Surfaced by the ADR-0046 crash-injection matrix under the race detector and verified by passing that matrix at 4× stress (~20 crash attempts) with the fix. Deterministically pinned (heal-head-orphan, no-op-when-consistent, refuse-on-branch).

## Compatibility

- No breaking API or CLI changes. Drop-in from v0.99.16. The heal runs only on stream resume and only when the open segment's catalog disagrees with the on-disk chain; an already-consistent backup is untouched. Existing backups that were left in the mis-stitched state by an earlier crash are repaired automatically the next time the stream resumes (and restore then succeeds).

## Who needs this

- **Anyone running continuous backups (`sluice backup` streaming with rotation) that may experience a process crash, OOM, or hard stop mid-rotation.** Without this fix, an unlucky crash could leave a backup that `restore` refuses; with it, the catalog self-heals on the next resume. Migrations and one-shot backups are unaffected.

---

**Install:** `brew install sluicesync/tap/sluice`  ·  `go install sluicesync.dev/sluice/cmd/sluice@v0.99.17`  ·  **Container:** `ghcr.io/sluicesync/sluice:0.99.17`
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
