# sluice v0.99.76

**Completes the per-shard VStream stall WARN (v0.99.74): it now catches a shard that is wedged from the very first CDC event — previously the one case it was blind to.** Observability-only; drop-in upgrade, no config changes, no behavior change to healthy syncs.

## Fixed

**Per-shard stall WARN was blind to a shard frozen from the start of the CDC tail.** The per-shard progress watchdog introduced in v0.99.74 only began tracking a shard once it delivered its first *advancing* VGTID. A shard that froze at the snapshot→CDC handoff — and therefore never delivered an advancing event — was never entered into the watchdog's state, so its stall was never even evaluated and no WARN fired. That was exactly the silent case observed live on a multi-shard Vitess→PlanetScale-MySQL sync: one shard frozen from the handoff while the other advanced, with no per-shard signal. The fix threads the resolved shard layout into the watchdog and pre-seeds every known shard's clock at startup, so a shard that never advances goes stale after the window and is reported (when a peer is fresh — the asymmetric-wedge signature). Seeding at start time keeps a full window of warm-up grace, and with the existing serving-proven gate and "a peer must be fresh" requirement a freshly-started stream still cannot warn during warm-up. Shards that appear later (e.g. after a reshard) are still tracked lazily on first advance. This is observability-only — it never alters the stream's resilience, it just no longer misses the from-start-frozen case.

## Compatibility

Fully backward-compatible. No new flags, no config changes, and no change to the data a sync moves or the schema it creates. The change only affects when an internal advisory WARN is emitted. Existing healthy syncs behave exactly as on v0.99.75.

## Who needs this

Operators running `sluice sync` against a **multi-shard Vitess or PlanetScale source** who rely on the per-shard stall WARN for visibility into a wedged shard — it now fires even when the wedged shard was frozen from the first CDC event. (The *recovery* for the dominant delivery-side wedge — a cross-region apply-throughput improvement — is tracked separately; this release is the detector-completeness fix.)

## Install

```
go install sluicesync.dev/sluice/cmd/sluice@v0.99.76
```

Container image:

```
docker pull ghcr.io/sluicesync/sluice:0.99.76
```
