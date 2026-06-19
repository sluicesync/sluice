# sluice v0.99.77

**Fixes a HIGH multi-shard resume bug — a Vitess/PlanetScale sync with more than one shard could re-snapshot on every restart and never warm-resume into CDC — and adds an opt-in, default-off preview of concurrent CDC apply that measured ~4× apply throughput live.** The resume fix is on by default and is the reason to upgrade; the concurrency knob is opt-in and marked preview.

## Fixed

**Warm-resume of a multi-shard Vitess/PlanetScale source no longer falsely refuses when the schema-history holds two same-schema anchors at incomparable per-shard positions (HIGH; live large-scale finding).** The ADR-0049 schema-history resolver picks the schema in effect at a CDC position from retained boundary anchors, ordered by the per-shard GTID *partial* order. On a multi-shard source, every cold-start / auto-resnapshot re-emits a table's current boundary at fresh per-shard GTIDs (the reader's true-delta cache is per-instance, so a new reader re-snapshots each table on first touch), so the *same* schema accumulates several anchors whose multi-shard positions are mutually incomparable — each ahead on a different shard. The resolver treated any two incomparable candidates as a fatal ambiguity and returned `ErrPositionInvalid`, which the orchestrator turns into an ADR-0093 auto-resnapshot — a wasteful full re-snapshot (idempotent re-upsert, no drop and no data loss, but an availability/throughput hit) on *every* restart, so the stream could never reach steady CDC. The fix compares the two incomparable anchors' decode contracts with the same schema-signature equality the CDC reader uses to decide a real schema change: if they are identical (the multi-shard / re-snapshot duplicate), resolve cleanly; only genuinely different schemas at incomparable anchors still refuse loudly. Validated live — the fixed binary warm-resumed the exact failing 2-shard state straight into CDC with no re-snapshot. Value-fidelity-reviewed to confirm the signature captures every decode-affecting type parameter (so the relaxation can never merge two genuinely-different decodes), and pinned per type-family through the real codec round-trip plus a ≥3-anchor incomparable chain.

## Added

**`--apply-concurrency W` (preview): concurrent key-hashed CDC apply for the MySQL target — ~4× apply throughput measured live, default-off.** On a cross-region multi-shard Vitess/PlanetScale → PlanetScale-MySQL `sync`, steady-state CDC apply is round-trip-bound on a single stream and can fall below the source write rate, which causes the per-shard `MinimizeSkew` wedge (one shard's apply backlog makes vtgate hold its peer). With `--apply-concurrency=W` the merged change stream is fanned across W in-order apply lanes by primary-key hash — every change for a given key lands on the same lane and is applied in source order there, so a dependent INSERT→UPDATE→DELETE on a row can never reorder — and the lanes commit concurrently on a dedicated W-backend pool. The resume position advances only to a source boundary durable across all lanes, a deliberate exactly-once-preserving relaxation (the persisted position can lag the durable data but never lead it; keyed tables stay exactly-once via idempotent re-apply on resume, keyless tables keep their existing at-least-once guarantee). Measured live on a 2-shard Vitess→PlanetScale-MySQL link: serial ~52 rows/s vs `--apply-concurrency=4` ~206 rows/s (≈4.0×), with the concurrent rate exceeding the source rate so the behind shard's backlog drained instead of growing — the apply-bound wedge closes.

This knob is **marked preview** because of one known v1 limitation: the lanes use static batch sizing (no per-lane adaptive controller yet), so a PlanetScale tx-killer abort stops the stream and warm-resumes (correct and non-lossy — the resume fix above is what makes that safe) rather than shrinking the batch in place like the serial path. Per-lane adaptive sizing + tx-killer convergence is the tracked follow-up. It supersedes the experimental `--apply-pipeline-depth` (whose commit-only overlap was measured ineffective on the same link, and which now remains only as a serial fallback).

## Compatibility

Fully backward-compatible. The resume fix changes only when a multi-shard warm-resume succeeds instead of needlessly cold-starting — no flags, no data or schema changes, and unsharded sources are unaffected. `--apply-concurrency` defaults to `0` (serial), byte-identical to prior behavior; it engages only when an operator sets `W>1`, and only the MySQL target implements it (Postgres targets ignore it and keep their existing apply path). No migration or config change is required to upgrade.

## Who needs this

Anyone running `sluice sync` against a **multi-shard Vitess or PlanetScale source** should upgrade for the resume fix — before it, every restart of such a sync re-snapshotted the full dataset and the stream never settled into CDC. Operators on a **cross-region PlanetScale-MySQL target** whose CDC apply lags the source (the per-shard wedge) can additionally opt into `--apply-concurrency=W` (start with 4) for a roughly W× apply-throughput lift, bearing the preview caveat above in mind.

## Install

```
go install sluicesync.dev/sluice/cmd/sluice@v0.99.77
```

Container image:

```
docker pull ghcr.io/sluicesync/sluice:0.99.77
```
