# sluice v0.99.81

**A small, safe follow-up to the v0.99.80 concurrent-apply GA: an over-large `--apply-batch-size` now self-corrects after the first successful commit, and the live cross-region validation produced a finding worth stating plainly — a frozen resume position under sustained lock contention is exactly-once working as designed, not a stall.**

## Changed

**Lane-local committable-size read cap (follow-up to the v0.99.80 known-limitation).** Each concurrent-apply lane (`--apply-concurrency > 1`) now caps its next read at twice the size that last committed. When `--apply-batch-size` is set far above what the target's transaction-killer timeout actually allows, a lane no longer keeps re-reading oversized batches and walking the ceiling down one multiplicative-decrease per transaction-killer timeout — after its first durable commit it immediately converges to a committable size. The cap only ever lowers the *next* read after a commit, and never below what just succeeded, so a sanely-configured run (the default) reads and commits byte-for-byte as before. This directly addresses the v0.99.80 "split churn" note for the *sizing*-driven case.

## Clarified (no behavior change)

**A frozen resume position under sustained lock contention is correct exactly-once behavior.** The live cross-region 2-shard Vitess→PlanetScale-MySQL validation deliberately drove the target into a transaction-killer storm and surfaced a case worth documenting clearly. When the target is contended enough that one change is *repeatedly* aborted — lock contention, where the abort fires even at batch-size 1, so it is not a sizing problem — the persisted resume position holds steady **while other lanes keep committing data ahead of it**. We confirmed forward progress two independent ways during the storm: the per-lane AIMD controllers climbed (additive-increase only fires on successful commits), and the target row count kept growing (~63 rows/s) even with the position frozen.

That is the checkpoint frontier refusing to advance its contiguous prefix past the one not-yet-durable change — exactly as exactly-once requires. Advancing past it would skip it on warm-resume (silent loss); holding is correct. The committed-ahead rows are idempotent (UPSERT), so they re-apply harmlessly once the stuck change finally lands and the frontier catches up. No data loss, no crash, fully warm-resumable.

So there are two distinct cases under a transaction-killer-heavy target: the **sizing**-driven one (over-large ceiling → safe-but-slow split churn), which the lane-local cap above now self-corrects; and the **contention**-driven one (a row the target keeps lock-killing), which is correct by design and outside what any batch-sizing strategy can change.

## Compatibility

Fully backward-compatible. The cap affects only the `--apply-concurrency > 1` path and is happy-path-neutral. No data, schema, default-behavior, or position-contract changes. Postgres targets are unaffected.

## Who needs this

Operators running `sluice sync --apply-concurrency=W` against a **cross-region PlanetScale-MySQL target** who set `--apply-batch-size` well above the default: the over-large ceiling now self-corrects after the first commit instead of churning. Everyone else is unaffected — the default ceiling already commits cleanly. If you ever see the resume position sit still on a heavily-contended target while data is still landing, this release's clarification explains why that is correct (and safe to leave running — it catches up).

## Install

```
go install sluicesync.dev/sluice/cmd/sluice@v0.99.81
```

Container image:

```
docker pull ghcr.io/sluicesync/sluice:0.99.81
```
