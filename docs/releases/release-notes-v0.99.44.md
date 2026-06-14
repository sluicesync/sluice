# sluice v0.99.44

**Reliable-by-default CDC throughput + VStream throttle resilience.** Three changes from the first real PlanetScale long-haul soak: (1) `sluice sync` now adapts its apply batch size by default — **>10× out-of-box CDC throughput** — with a guard that keeps keyless tables safe; (2) a transient source-side Vitess throttle (or failover) is now **ridden out in-process** instead of crash-looping; (3) self-hosted `--source-driver=vitess` can finally **warm-resume**. Drop-in from v0.99.43 — no format or breaking changes; `migrate`, Postgres paths, and cross-engine behavior are untouched.

## Changed

- **`sluice sync --apply-batch-size` now defaults to `auto` (ADR-0089) — >10× CDC apply throughput out of the box.** The ADR-0052 AIMD batch-size controller (adapts to a p95-latency target, floor 1) has shipped since v0.72.0, but the conservative default `--apply-batch-size=1` made its cap equal its floor, leaving it **dormant for every default user** — sluice shipped an adaptive throughput controller that did nothing unless you knew to pass a flag. The first real PlanetScale soak measured the cost: single-row apply drained a backlog at **~240 rows/s vs ~6,500 at `auto` (>10×)**, and the slow drain badly compounded throttle recovery. The default is now `auto` (engine ceiling 1000 mysql/postgres, 100 planetscale; the controller adapts within `[1, ceiling]` and backs off under pressure). **Safety guard:** a table with **no PRIMARY KEY and no usable unique index** (a non-idempotent plain INSERT on replay — Bug 125 class 3) is never batched — each such change commits alone, so a crash-replay's duplicate blast radius stays at exactly 1 (identical to `--apply-batch-size=1`), while PK/unique tables batch and adapt; a one-time WARN names any such table. Restore the old behavior with `--apply-batch-size=1` (conservative single-row) or `--no-auto-tune` (static cap).

## Fixed

- **VStream throttle/large-transaction stalls no longer crash-loop a continuous sync (Bug 141 / ADR-0090).** When a Vitess source's tablet throttler engages (e.g. a co-tenant migration, or a heavy write burst lagging the replica), vtgate withholds change events and — near its own 10-minute tolerance — even heartbeats, so sluice's liveness/progress watchdog fired and **misdiagnosed the transient throttle as a failover hang.** The error was terminal, so the `sync` process exited; under a supervisor (systemd, k8s) it restarted, warm-resumed to the same throttled position, re-stalled, and exited again — a tight, **non-converging crash-loop**. The watchdog timeouts are now `ir.RetriableError`, so the existing ADR-0038 exponential-backoff retry reconnects from the last position **in-process** and rides out the throttle until it clears (the correct recovery for a real failover too). A genuinely non-healing wedge (primary-only cluster with no serving replica) still fails loud after the bounded retry budget — just not in a tight loop. The headline finding of the soak, reproduced and root-caused on a self-hosted Vitess-24 cluster.
- **Self-hosted `--source-driver=vitess` can now warm-resume (Bug 142).** The `vitess` flavor's engine name (`"vitess"`) wasn't in the position-decode accept set, so a resumed CDC position stamped `Engine="vitess"` was rejected — **every restart of a self-hosted Vitess continuous sync crash-looped** with `wrong engine "vitess"` and could never warm-resume. Unconditional, not throttle-gated. PlanetScale was unaffected (flavor name `"planetscale"`). The decoder now accepts `"vitess"`.

## Compatibility

- **Drop-in from v0.99.43 — no on-disk/format or breaking changes.** Existing backups restore unchanged; `migrate`, Postgres sources/targets, and cross-engine translation are untouched.
- **Behavior change (intended):** `sluice sync` now batches CDC applies by default (`--apply-batch-size=auto`). For nearly all OLTP workloads this is a large throughput win at no correctness cost (ADR-0010 idempotency; keyless tables auto-clamp to single-row). Pin the old behavior with `--apply-batch-size=1` or `--no-auto-tune`.
- No new required flags. The watchdog and warm-resume fixes need no configuration.

## Who needs this

- **Anyone running `sluice sync` (CDC) — especially against Vitess/PlanetScale.** You get an order-of-magnitude faster steady-state and backlog catch-up out of the box, and a transient source throttle now self-recovers instead of wedging the sync in a restart loop.
- **Self-hosted Vitess (`--source-driver=vitess`) users — upgrade required for continuous sync:** before this release a vitess-flavor sync could not survive a restart at all.

---

**Install:** `brew install sluicesync/tap/sluice`  ·  `go install sluicesync.dev/sluice/cmd/sluice@v0.99.44`  ·  **Container:** `ghcr.io/sluicesync/sluice:0.99.44`
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
