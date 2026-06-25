# sluice v0.99.123

**New: the `sync from-backup` broker now replays incrementals through the concurrent key-hash apply lanes (`--apply-concurrency W`) instead of a single serial stream — a large incremental into a high-latency / cross-region target no longer effectively stalls. Drop-in: fast by default (auto:4), `--apply-concurrency 1` is the serial opt-out, exactly-once preserved.**

## Features

**Broker chain-replay fans an incremental across W in-order PK-hash lanes (ADR-0104 MySQL / ADR-0105 Postgres).** The live-CDC path (`sync start`) already fans its merged change stream across W concurrent in-order PK-hash lanes, but the `sync from-backup` broker's incremental-replay path opened its applier and then never wired the lane count — so every incremental replayed through the single-stream ADR-0092 pipelined applier, RTT-bound on a cross-region link. The Track-C large-scale program caught it live: replaying an ~8 M-change incremental from an object-storage backup chain into a small cross-region PlanetScale-Postgres target ground to ~1–2 changes/s — exactly the wedge `--apply-concurrency` was built to lift, but on the broker-replay path instead of the live one. The broker now plumbs the lane count onto the applier through the same `applyApplyConcurrency` helper the streamer uses, right after it opens the applier, so an incremental's changes fan across W concurrent lanes. The new `--apply-concurrency` flag on `sync from-backup run` follows the ADR-0106 fast-by-default contract: `0`/unset → an adaptive default of 4 (a fixed conservative ceiling — the broker opens a single applier up front and does not run a connection-budget probe; per-lane backpressure handles a tight target), `1` → the explicit serial opt-out, `W>1` → honored verbatim. Pinned by unit tests on the resolver (zero-value-safe fast default; explicit `1` stays serial) and the plumb (the shared helper engages the applier's lane setter for `W>1`, no-op for serial).

## Compatibility

No breaking changes; drop-in upgrade. The only new surface is the `--apply-concurrency W` flag on `sync from-backup run`, which is opt-in-fast-by-default: with no flag the broker resolves to an adaptive default of 4 lanes, `--apply-concurrency 1` restores the prior single-serial-stream behaviour exactly, and `W>1` is honored verbatim. Exactly-once is preserved exactly as on the live path — every change in an incremental carries the same broker chain-position token, so the lanes persist the identical resume position the serial path did, the frontier still checkpoints only at a fully-durable boundary, and the broker's idempotent re-replay-from-parent crash recovery is unchanged. Applies to both engine targets (MySQL via ADR-0104, Postgres via ADR-0105). No change to value handling, the live `sync start` path, or any other command.

## Who needs this — action required

Operators running `sync from-backup` as a continuous broker that replays large incrementals into a high-latency / cross-region target: this is the upgrade that lifts the RTT-bound stall. No action required beyond upgrading — the concurrent path is on by default (auto:4) and produces byte-identical results to the serial path (exactly-once preserved), so there is nothing to re-verify or re-run from prior releases; this is a throughput change, not a correctness fix, and no earlier release silently lost or mis-applied data on this path. If you deliberately want the old single-serial-stream behaviour (a single-connection target, or to bound concurrency yourself), pass `--apply-concurrency 1`. Everyone else can leave it at the default. Operators of the live `sync start` path are unaffected — that path already had concurrent apply.

---

**Install:** brew install sluicesync/tap/sluice · go install sluicesync.dev/sluice/cmd/sluice@v0.99.123 · **Container:** ghcr.io/sluicesync/sluice:0.99.123
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
