# sluice v0.99.105

**The ADR-0110 grow-gate now hands a proactively-tripped pause back to the proven reactive cycling instead of parking the cold-copy lanes for the whole storage-grow window — the v14 live calibration finding.** A small but load-bearing correction to the storage-grow resilience arc: a proactive pause is now a brief anticipatory quiesce, not a multi-minute hold.

## Fixed

**A proactively-tripped grow-gate now quiet-cycles its lanes back to work instead of parking them for the whole max-hold.** ADR-0110's first cut made the two trip sources reopen asymmetrically: a *signal-driven* pause (the first classified grow-transient) reopened as soon as a backoff cycle passed quietly, but a *proactive* (PlanetScale-telemetry-tripped) pause held until either storage headroom `recovered()` or the 20-minute max-hold backstop expired.

The v0.99.104 v14 PS-320 live validation proved that asymmetry backfires. The proactive trip fired correctly at storage util = 0.859, but the `planetscale_vttablet_volume_available_bytes` / `_capacity_bytes` gauges swing wildly and transiently vanish across the reparent — the 62 GB volume read 85.9% one moment and a 1.66 TB volume at ~0% the next, with the series absent in between — so `recovered()` could not confirm the grow had actually finished exactly when it mattered. The gate therefore rode a flat, zero-progress 20-minute max-hold on every reparent: strictly worse than the reactive cycling it was meant to improve on, which makes incremental progress each ~30 s window.

The reopen path is now unified. **Every** pause — proactive or signal-driven — quiet-cycles open as soon as a backoff cycle elapses with no re-trip: the lanes resume and probe, and if the target is still in its grow window the next transient simply re-trips a fresh, longer-backed-off window (the exponential backoff still grows across windows). `recovered()` is retained purely as an *accelerator* that reopens earlier when the telemetry signal is trustworthy, and the max-hold remains the backstop for a genuinely-dead target. A proactive trip is thus a brief anticipatory pause that hands off to the proven reactive cycling — never a hold for the entire grow.

This is pinned by a new test that holds `recovered()` false with a far-off max-hold and a no-re-trip cycle, asserting the gate still reopens promptly via the quiet cycle, plus an updated recovery test that continuously hammers re-trips (suppressing the quiet-cycle path) to isolate `recovered()` as the genuine early-reopen route.

**Corrected the storage-headroom WARN hint, which wrongly told operators "sluice does not pause."** Since ADR-0110 the cold-copy phase *does* coordinate a lane quiesce across a storage-grow window, and steady-state CDC apply rides the resize via the bounded retries. The hint text was left over from the pre-ADR-0110 WARN-only sidecar; it now states the behaviour accurately: "during a cold-copy the grow-gate quiesces the copy lanes for this window and resumes when headroom recovers; during steady-state CDC apply the stream rides the resize transparently via the bounded retries — apply correctness is unaffected either way." Text-only; no behaviour change.

## Compatibility

No configuration changes and no behaviour change for an untroubled migration. The grow-gate is still constructed inert and only engages on a classified grow-transient (signal-driven) or a configured-PlanetScale-telemetry crossing (proactive); this release changes only *when a proactive pause reopens*, bringing it in line with the already-proven signal-driven reopen. No resume-format, wire, or result-state changes; the exactly-once contract is unchanged.

## Who needs this

Anyone running a `sync` cold-start into a **non-Metal PlanetScale** target with PlanetScale metrics configured (`--planet-scale-org` + the metrics token), where a storage auto-grow triggers a reparent during the cold-copy. The proactive pause now resumes copying within a backoff cycle of the grow window quieting, instead of idling for up to 20 minutes per reparent. Without telemetry configured, the signal-driven path was already correct and is unchanged. Automatic; no action required.

## Install

```
go install sluicesync.dev/sluice/cmd/sluice@v0.99.105
```

Container image:

```
docker pull ghcr.io/sluicesync/sluice:0.99.105
```
