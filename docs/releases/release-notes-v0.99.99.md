# sluice v0.99.99

**The cold-copy retry budget now spans a prolonged multi-step storage-grow stall.** With every storage-auto-grow transient face made retriable across v0.99.92–v0.99.98, the remaining gap was simply patience: a long PlanetScale validation copy rode ~23 minutes of retries cleanly, but a single big-table grow step stalled longer than the per-batch ~4-minute retry budget and failed loudly mid-grow. This release widens that budget.

## Changed

**Cold-copy reparent / disk-full retry budget raised (12 → 24 attempts; ~4 min → ~15–20 min envelope).** The per-batch budget for both the target-write reparent-retry (`flushWithReparentRetry`) and the source-read reconnect-retry is raised from 12 to 24 attempts. The backoff shape is unchanged (100 ms→30 s exponential), so the envelope grows to roughly 12 minutes of backoff plus each attempt's own stall-until-error — about a 15–20 minute window — comfortably spanning a multi-minute big-table grow step (12→39→62→214 GB on a non-Metal PlanetScale volume). It remains **bounded and loud on exhaustion**: a genuinely-wedged target, or an undersized fixed-storage target that will never grow, still surfaces a clear terminal error — just after a longer, grow-appropriate wait rather than ~4 minutes. The bounds are package-baked constants (no config field, no zero-value trap).

This is the targeted budget fix for the case the v0.99.98 validation surfaced (the copy rode the whole grow's worth of transients and only fell over on one big-table step exceeding the old 4-minute budget). The deeper lever — a **proactive coordinated pause-on-stall** (and the Item-32-telemetry-driven throttle), which shortens the stall itself by not hammering a struggling target with all write lanes during a grow — is a tracked follow-up; this release rides the stall out patiently, that one would avoid prolonging it.

## Compatibility

No configuration changes and no behaviour change for an untroubled migration. The only difference is that a transient target stall during cold-copy is now tolerated for ~15–20 minutes (up from ~4) before a bounded, loud terminal failure. No resume-format, wire, or result-state changes.

## Who needs this

Anyone running a large cold-copy into a **non-Metal PlanetScale MySQL** target across multiple storage auto-grow steps — the copy now rides a prolonged big-table grow step instead of exhausting its retry budget mid-grow. Automatic; no action required.

## Install

```
go install sluicesync.dev/sluice/cmd/sluice@v0.99.99
```

Container image:

```
docker pull ghcr.io/sluicesync/sluice:0.99.99
```
