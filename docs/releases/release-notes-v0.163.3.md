# sluice v0.163.3

**HIGH fix, silent — a PlanetScale/Vitess cold start stopped during its copy is now re-copied, never resumed, because a resumed copy kept rows the source had deleted (VSTREAM-COPY-RESUME-RECREATED-TABLE, present since v0.99.5).** A resumed VStream copy copies the already-finished tables again by upsert, which removes nothing, so any row deleted on the source while the stream was stopped stayed on the target, and so did every old row of a table dropped and re-created under its name. Measured on Vitess 24 before this release: 6,701 rows on the target against 701 on the source after a re-create. The re-run after a stop now refuses, and `--restart-from-scratch` clears the in-scope target tables and copies them again. Also: three unsafe paths around that refusal are closed, and the build moves to Go 1.26.10 for a reachable standard-library advisory. No flag change and no backup format change.

## Fixed

- **A stopped PlanetScale/Vitess copy is re-copied instead of resumed (VSTREAM-COPY-RESUME-RECREATED-TABLE, HIGH, silent; `VSTREAM-COPY-RESUME-WITHDRAWN`).** On a resume, Vitess (vttablet's uvstreamer) copies again from the start every table that has no copy cursor, the tables the stopped run had already finished included, and drops their changes from the stopped window; the table holding the cursor gets changes only for rows at or below it. sluice's copy writes by upsert, so every row the source deleted while the stream was stopped survived. On a multi-shard keyspace a table can be finished on one shard and mid-copy on another, which the target cannot tell apart, so no repair on the target side is sound in general; the resume is withdrawn. The cold start now saves no position until its copy finishes. A first cold start stopped during its copy leaves no position, so its re-run finds the copied rows and refuses (`SLUICE-E-COLDSTART-TARGET-NOT-EMPTY`). A re-copy of an existing stream stopped the same way keeps its old position under the in-progress mark, and its re-run refuses `COLD-START-INCOMPLETE`. A mid-copy position saved by an earlier release is refused under `VSTREAM-COPY-RESUME-WITHDRAWN`. The remedy for all three is the same command with `--restart-from-scratch`. Measured on a local Vitess 24 cluster with a Postgres target, on one shard and two: every stopped first cold start left no saved position, every plain re-run refused with `SLUICE-E-COLDSTART-TARGET-NOT-EMPTY`, and every `--restart-from-scratch` re-copy matched the source exactly, including after rows were deleted, and after both tables were dropped and re-created, while it was stopped (700 = 700 where the resume had left 6,701 against 701). A stream that has reached CDC resumes as before. Pinned by `TestStreamer_InterruptedColdStart_RefusesTheCursor`, `TestStreamer_InterruptedColdStart_RestartFromScratchStillRuns`, `TestColdStartOpenSnapshot_WiresNoCopyCheckpoint` and the vttestserver cells of `TestStreamer_VStreamCopyResume` and `TestStreamer_VStreamCopyResume_TwoShards`.
- **The populated-target refusal no longer recommends a lossy override.** `SLUICE-E-COLDSTART-TARGET-NOT-EMPTY` is now the expected re-run result after a stopped PlanetScale/Vitess copy, and its hint recommended `--force-cold-start` with the warning that it "will collide on PRIMARY KEY". On these sources it does not collide: it copies by upsert over the rows already there and keeps every row the source has deleted, silently. The hint and the error-code row now name `--restart-from-scratch` and say what `--force-cold-start` does on each kind of source. Found by this release's pre-tag review.
- **`--restart-from-scratch` with `--schema-already-applied` refuses instead of keeping deleted rows (`RESTART-FROM-SCRATCH-TARGET-NOT-CLEARED`, silent before).** `--schema-already-applied` skips the clear `--restart-from-scratch` promises, because the clear drops the tables you pre-created, and on a PlanetScale/Vitess source the re-copy then kept every row the source had deleted. That combination now refuses while an in-scope target table holds rows; empty the tables yourself and re-run the same command. Pinned by `TestColdStartGate_RestartFromScratchSchemaAlreadyApplied_RefusesAPopulatedTarget` and a vttestserver cell. Found by this release's pre-tag review.
- **The cold-start in-progress mark is re-checked on every warm resume, and a stream with a saved position refuses when the mark cannot be written (silent before, under narrow conditions).** With no mid-copy checkpoint, only the `COLD-START-INCOMPLETE` mark keeps a re-copy that stopped mid-copy from being warm-resumed from the position it was replacing, onto a partly re-copied target. The mark was checked once per process, so a re-copy begun later in the same process (the automatic re-snapshot after an invalid position) could fall back to that warm resume; and where the mark could not be written to a stream that had a position but no record (an older control table this role cannot alter), the cold start only warned and carried on. Both now refuse. Pinned by `TestScopeDriftDoor_ColdStartInProgressAfterTheDoorRan` and `TestColdStartEstablishedRecord`. Found by this release's pre-tag review.

## Measured, no change needed

- **Rows deleted on the source during an uninterrupted VStream cold copy are removed on the target.** This release's pre-tag review raised it from the code: the copy-phase decoder reads only a row's after image, and a delete carries only a before image. Measured on the default path, one shard: 100 already-copied rows deleted and one primary key moved mid-copy, and the target matched the source exactly (299,901 = 299,901). Kept as the vttestserver cell `ChangesDuringCopy/default-path`. The two-shard and single-stream paths were not measured.

## Corrected

- **v0.163.2's notes said a stopped VStream copy would leave "an earlier cursor, from which the resume re-copies". On a Postgres target no such cursor was ever saved.** The copy's saved position could only advance past rows the target writer reported as durably written, and only the MySQL row writer's serial path reported them, so against a Postgres target, or with the parallel or concurrent copy, the one position ever saved was the first, which carried no cursor (sluice-testing Bug 325). And where a cursor was saved, resuming from it kept deleted rows, which is this release's fix. Both stopped-copy paths now end in a refusal and a `--restart-from-scratch` re-copy.

## Not closed, stated

- **The copy's in-place reconnect after a transient mid-copy fault resumes the same way, inside one process (VSTREAM-COPY-RECONNECT-STALE-ROWS, open, silent, not yet measured).** A row deleted on the source during the reconnect's few-second window, in a table that copy had already finished or above the cursor, could stay on the target. It is next in the queue: it will be measured, and if it reproduces the reconnect will be removed so a fault falls to a full, clearing re-copy.
- **Not built:** an end-to-end cell that plants a real mid-copy position from an earlier release and checks the refusal; the refusal's routing is pinned by a unit test, and the predicate that recognises such a position by the engine's own position tests.
- **Not run for this release: `d1verify` and `psverify`.** `scripts/prerelease-triggers.sh` raised its unscheduled-pin advisory because the delta touches `internal/pipeline/`; neither suite exercises a stopped VStream copy. Every VStream measurement above was on a local Vitess 24 cluster, not on PlanetScale itself.

## Security

<!-- notes-claims-exempt: GO-2026-6809 -->
- **Built with Go 1.26.10, for GO-2026-6809 in the standard library's `crypto/x509`, which `govulncheck` reports as reachable from sluice's code.** No sluice code changed for it. It is fixed only in Go 1.26.10, so treat every earlier sluice release, each built with an earlier Go, as affected.

## Compatibility

- **No flag change and no backup format change** (`BackupFormatVersion` stays 15).
- **A PlanetScale/Vitess stream stopped mid-copy on an earlier release needs `--restart-from-scratch` to continue.**
- **A transient vtgate drop during a PlanetScale/Vitess re-copy of an existing stream now stops the stream** (`COLD-START-INCOMPLETE`) rather than resuming the copy: re-run with `--restart-from-scratch`. A retriable drop inside either kind of copy is first retried in place by the copy's own reconnect, within its retry budget, as before.

## Who needs this

- **Anyone who stopped a PlanetScale or Vitess `sync start` cold start during its copy and let a re-run continue it, on any release before this one.** If the source deleted rows, or a table was dropped and re-created, while it was stopped, the target may still hold them. Compare each table against the source, or re-copy with `--restart-from-scratch`.
- **Anyone with a PlanetScale/Vitess stream currently stopped mid-copy:** re-run with `--restart-from-scratch` after upgrading. Do not use `--force-cold-start` for it.
- **Everyone else:** upgrade for the Go 1.26.10 rebuild; nothing to do.

---

**Install:** `brew install sluicesync/tap/sluice` · `go install sluicesync.dev/sluice/cmd/sluice@v0.163.3` · **Container:** `ghcr.io/sluicesync/sluice:0.163.3`

**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
