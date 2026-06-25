# sluice v0.99.112

**Target-telemetry rolling history (item 35) and threshold alerts (item 36) now cover the cold-copy phase, not just CDC apply (roadmap item 39).** The window where they matter most — a long initial cold-copy, under heavy write load, when storage auto-grows happen — was previously uncovered.

## Changed

Both telemetry consumers — the rolling-history recorder and the threshold alerter — were started in the apply-phase sidecar wiring, so during the cold-copy phase they recorded no metrics history and fired no threshold alerts. That's exactly the phase a storage-approaching-capacity or CPU-saturation alert is most useful (it's when the target fills toward a grow), and the phase whose resource trend you'd most want recorded.

They are now started **once per attempt, right after the applier opens** — which lives for the whole attempt and is idle during cold-copy, so reusing it is safe — spanning cold-copy and CDC apply with a single start (no double-record, no double-fire). A run-scoped context cancels both cleanly at attempt end, so there's no cross-attempt goroutine leak on a warm-resume loop.

Net effect: an item-36 alert can fire *during* the cold-copy that triggers a storage grow (pairs naturally with v0.99.111's PG cold-copy grow ride-through), and the item-35 `sluice_target_metrics_history` table captures the cold-copy resource trend, not just the steady-state CDC trend.

## Compatibility

No configuration changes and a total no-op for any sync that hasn't configured PlanetScale telemetry (`--planet-scale-org`). For a telemetry-configured sync, the only change is that the recorder/alerter start earlier (covering cold-copy); the per-sample dedupe and per-rule edge-trigger/cooldown semantics are unchanged, and the single start point means no double-record or double-alert across the cold-copy→apply transition. Advisory/observability only — no effect on the apply path or the exactly-once contract.

## Who needs this

Anyone running `sluice sync` / `migrate` into a **PlanetScale** target with metrics telemetry configured and a dataset large enough for a multi-minute cold-copy — you now get target-health alerts and recorded history throughout the cold-copy, not only once CDC begins. Automatic.

## Install

```
go install sluicesync.dev/sluice/cmd/sluice@v0.99.112
```

Container image:

```
docker pull ghcr.io/sluicesync/sluice:0.99.112
```
