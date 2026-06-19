# sluice v0.99.78

**Housekeeping release: removes the experimental, ineffective `--apply-pipeline-depth` flag (superseded by `--apply-concurrency` in v0.99.77) and de-flakes a `-race` test.** No change to how any sync moves data; the only user-visible change is that one experimental flag is gone.

## Removed

**`--apply-pipeline-depth` (ADR-0104 Phase 1) is removed — use `--apply-concurrency` instead.** Phase 1 was an in-order pipelined-*commit* design that, measured on the live cross-region PlanetScale-MySQL link, overlapped only the commit round-trip and not the per-batch data execs, so it produced no throughput improvement (apply stayed at roughly the serial single-stream rate with 7 of 8 backends idle). Its successor, `--apply-concurrency` (added in v0.99.77 and live-validated at ~4×), is the lever that actually closes the cross-region apply-deficit wedge — it fans the change stream across W in-order key-hash lanes that commit concurrently. With the successor shipped and proven, the dead Phase-1 implementation (the `mysqlPipeline` subsystem, ~1,100 lines including tests) and the `--apply-pipeline-depth` flag are removed wholesale rather than left as confusing, ineffective scaffolding. This is a pure removal that reverts the batched-apply seam to its pre-ADR-0104 serial shape — serial apply behavior is byte-unchanged, and the `--apply-concurrency` path is untouched. Because `--apply-pipeline-depth` never delivered a throughput improvement, removing it loses no working capability; anyone who set it should switch to `--apply-concurrency=W`.

## Fixed

**De-flaked an internal `-race` watchdog test (test-only; no runtime change).** The end-to-end test for the per-shard VStream stall WARN had a harness race that could intermittently time out under the race detector. The production watchdog is unaffected — it pre-seeds every known shard's clock at start, so the fix simply removes a redundant, racy step from the test. No behavior change.

## Compatibility

No data, schema, or sync-behavior changes. The only surface change is the removal of the experimental `--apply-pipeline-depth` flag; a `sync start` invocation that still passes it will now error on the unknown flag — switch to `--apply-concurrency=W` (for the MySQL target) to get concurrent CDC apply. Everything else, including `--apply-concurrency` and all serial apply behavior, is unchanged from v0.99.77.

## Who needs this

Nobody urgently — this is housekeeping. Upgrade at your convenience to drop the dead flag and pick up the CI-stability fix. If you were experimenting with `--apply-pipeline-depth`, move to `--apply-concurrency=W` (start with 4), which is the lever that actually lifts cross-region CDC-apply throughput.

## Install

```
go install sluicesync.dev/sluice/cmd/sluice@v0.99.78
```

Container image:

```
docker pull ghcr.io/sluicesync/sluice:0.99.78
```
