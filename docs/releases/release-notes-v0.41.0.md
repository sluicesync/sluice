# sluice v0.41.0 — cold-start persists the CDC anchor before the first batch

**Closes GitHub issue #15.** Before v0.41.0, the target's `sluice_cdc_state` row only existed after the first CDC batch committed successfully. Any failure between "bulk-copy complete; entering CDC mode" and that first batch wedged the operator:

- Warm-resume couldn't recover (no persisted position).
- Cold-start refused (target tables held the freshly-bulk-copied data).
- Only escapes: `--reset-target-data --yes` (re-bulk-copy everything) or `--force-cold-start` (collides on PK).

This was the operationally-load-bearing follow-on consequence of GitHub issue #13 (Vitess tx-killer exits the stream): even after #13's retry policy ships, a long-tail failure mode would still leave operators wedged. Fixing #15 makes #13's eventual fix recoverable, and any future transient mid-first-batch self-recoverable on restart.

## Fixed

- **`internal/pipeline/streamer.go::coldStart`** — after the "bulk-copy complete; entering CDC mode" log line and before `StreamChanges`, the streamer now calls `applier.WritePosition(ctx, streamID, stream.Position)` with the snapshot's anchor. CDC from this position is gapless (snapshot anchor = CDC start position per ADR-0007), so a restart reading this row warm-resumes correctly and replays the same change stream the failed run would have processed. Idempotent: the first `applier.commitBatch` overwrites the same row with a monotonically-newer position.

- **Mode-aware "cold-start refused" hint (`internal/pipeline/preflight.go`).** The refusal previously assumed every hit came from `sluice migrate` and recommended `--resume on sluice migrate` even when the operator was running `sluice sync start`. The function now takes a `preflightMode` and emits a tailored hint:
  - **Migrate mode** (unchanged): "previous cold-start killed mid-bulk-copy → drop tables or `--resume`".
  - **Sync mode** (new): names GitHub #15 as a candidate cause, recommends `--reset-target-data --yes` as the primary recovery, notes that the slot-drop step applies to PG sources only.

## Migration / Compatibility

- **Drop-in upgrade from v0.40.0.** No CLI changes, no IR changes, no engine-interface changes.
- **Operators currently in a v0.40.0 wedge state**: upgrade to v0.41.0, run once with `--reset-target-data --yes` to recover; future cold-starts will not re-wedge.
- **First-write contract.** The pre-CDC `WritePosition` is gated on the applier implementing `ir.PositionWriter`. Both shipping engines (MySQL, Postgres) do; an engine that doesn't logs a WARN and falls through to pre-fix behaviour (no regression, just no protection).

## Who needs this release

- **Anyone running `sluice sync start` against PlanetScale-MySQL / Vitess targets**: **upgrade**. The #15 wedge required `--reset-target-data` recovery on every transient; v0.41.0 makes restarts clean warm-resumes.
- **PG-source streams**: drop-in. The cold-start anchor write makes restart-during-first-batch recoverable; pre-fix this was rare (PG applier transients are less frequent than Vitess tx-killer events) but the fix is symmetric.
- **Operators currently wedged on v0.40.0**: upgrade + one `--reset-target-data --yes` recovery; future runs are protected.

## Verification surface

- New unit test `TestPreflightColdStart_SyncModeHint` asserts the sync-mode hint names #15, recommends `--reset-target-data`, and does NOT point at `sluice migrate --resume` (which would mislead operators in sync flows).
- Existing preflight tests updated for the new mode arg; their hint assertions still pass unchanged.
- **`TestStreamer_ResetTargetData_RecoversFromSlotMissing`** updated to use a new `waitForPersistedPositionChanged` helper. The v0.41.0 fix shrinks the "row absent" window during reset+cold-start to roughly the bulk-copy duration (milliseconds), making the brittle "wait for row to go missing" assertion miss the transient. "Position changed from a known prior value" is the strictly stronger signal. This is the integration-test bug that was caught only after the v0.40.1 tag was cut and the CI Integration job failed; v0.41.0 supersedes the v0.40.1 historical tag with the test fix bundled.

## Companion design — ADR-0038 (issue #13 retry policy)

GitHub issue #13 (Vitess tx-killer / PG serialisation transients exit the stream) remains open; the design lives in [ADR-0038](docs/adr/adr-0038-applier-retry-on-transient-errors.md). With v0.41.0 closing #15, a v0.40.0+ operator hit by an issue #13 transient can now restart and warm-resume to retry the failed batch, rather than re-bulk-copying the whole dataset. The retry policy in ADR-0038 is the next surface to ship — implementation in v0.42.0.
