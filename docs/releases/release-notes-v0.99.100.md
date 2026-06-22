# sluice v0.99.100

**Coordinated cold-copy grow-window pause — the proactive deepening of the v0.99.92–v0.99.99 reactive storage-grow arc.** v0.99.92–v0.99.99 made a large cold-copy *ride through* a non-Metal PlanetScale storage auto-grow by making every transient face retriable and widening the budget. This release makes that ride *calm* instead of *frantic*: during the multi-minute grow/reparent window, all cold-copy lanes now quiesce together rather than each independently hammering the struggling target.

## What we learned (the diagnostic behind this release)

A live diagnostic on the Track-D PS-320 rig (four runs) pinned the real cause of the stall. Three runs on a *growing* 12 GB volume all froze at the **same** byte point — ~86% of the volume, i.e. exactly the auto-grow trigger threshold — and exhausted their retry budget. A fourth run on a volume that had **already** grown to 62 GB rode straight through that point, copying the big `documents` MEDIUMTEXT table clean past its full row count with **zero** transient faces. Two hypotheses were ruled out by ground truth: concurrency is not the cause (a 1-lane run stalled identically to a 16-lane run), and the data is not pathological (`documents` copies fine on a pre-grown volume — it only looked like the culprit because it is the big table being hammered when the threshold trips). The cause is precisely being mid-write into the volume during its grow/reparent serving-transition window. The resize itself is fast; the serving-transition it triggers is where writes are rejected.

The reactive arc already rides that window — but inefficiently: during the stall, all ~16 cold-copy lanes (W tables × D fan-out) independently hammer-retry the target, which prolongs the grow and recovery and breeds the secondary lock-wait-timeouts (a *consequence* of the hammering, not an independent fault). This release treats the cause.

## Added

**Coordinated cold-copy grow-window pause (ADR-0110, roadmap item 37).** One engine-neutral coordinated-pause primitive (`ir.GrowGate`) shared across every cold-copy write lane in a run, tripped from two sources driving the same mechanism:

- **Signal-driven (the universal floor, no external dependency).** The first classified grow-transient on any write lane or source-read attempt trips the shared gate; all sibling lanes quiesce together for a coordinated, exponentially-backed-off window (the same 100 ms→30 s shape as the per-lane retry envelope), then resume and probe. This works for **any** target with storage-auto-grow / transient-reparent behaviour — non-PlanetScale included — because the trigger is the classified transient itself, not a PlanetScale-specific metric.
- **Telemetry-driven (a precision enhancement, PlanetScale-only when configured).** When PlanetScale metrics are wired (the v0.99.95 Item-32 provider), the storage-headroom sidecar trips the *same* gate **proactively** — before the lanes start hitting transients, as storage heads toward the grow boundary — and releases it when headroom recovers. This avoids burning retry attempts and avoids the source-read backpressure cascade entirely. It is advisory: a no-metrics run still rides through via the signal path, just less efficiently.

The gate is constructed unconditionally for a cold-copy run and is **inert until tripped** — there is no CLI flag and no `EnableX`-defaulting-true config bool (the zero value is the safe default). It is purely advisory about *when* a flush attempts, never about *what* it does: it never swallows a terminal error, advances a position, or marks a table complete. The per-lane reparent-retry / source-read-resume budgets remain the authoritative loud-on-exhaustion floor, and the gate has its own max-hold so a genuinely-dead target still surfaces rather than parking forever.

## Compatibility

No configuration changes and no behaviour change for an untroubled migration. The only difference during a target storage-grow window is a calmer, coordinated quiesce instead of every lane independently hammering the target. No resume-format, wire, or result-state changes; the correctness contract is unchanged from v0.99.99.

## Who needs this

Anyone running a large cold-copy into a **non-Metal PlanetScale MySQL** target across storage auto-grow steps — the copy now rides each grow window with a coordinated pause rather than a thundering-herd retry, completing the grow faster and with fewer secondary errors. Anyone migrating into any other target with similar storage-auto-grow / transient-reparent behaviour gets the same signal-driven coordination automatically. Automatic; no action required.

## Install

```
go install sluicesync.dev/sluice/cmd/sluice@v0.99.100
```

Container image:

```
docker pull ghcr.io/sluicesync/sluice:0.99.100
```
