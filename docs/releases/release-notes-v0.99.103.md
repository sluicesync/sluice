# sluice v0.99.103

**The ADR-0110 grow-gate finally engages in the cold-copy path PlanetScale MySQL→MySQL `sync` actually uses, and the cold-copy retry now rides a storage grow on a wall-clock budget instead of an attempt count.** Three successive live validations on a fresh non-Metal PS-320 target surfaced two distinct gaps the earlier releases missed; this release closes both.

## Fixed

**The coordinated grow-gate is now wired into the native-concurrent `sync` cold-copy path — v0.99.100–v0.99.102 wired functions that path never calls.** The native-MySQL concurrent cold-copy (the path every continuous PlanetScale MySQL→MySQL migration and the Track-D validation rig takes) runs `coldStartRunCopy → runBulkCopyWithOpts → runConcurrentTableCopy`, not the `runBulkCopyPhases` path the v0.99.100/102 wiring touched. So the gate was inert (or never even constructed) for that path, and three successive live PS-320 runs tripped it zero times. The gate is now constructed and attached in `coldStartRunCopy` (and the multi-database twin in `streamer_multidb.go`), where the Streamer's PlanetScale telemetry is in scope for the proactive trip + storage-recovery probe. `runConcurrentTableCopy` reuses one writer across all W×D fan-out workers, so attaching the gate to that writer engages the coordination for the entire fan-out — the lanes now quiesce together through a grow window instead of independently hammering the target.

**The cold-copy retry bound is now wall-clock based (~30 min), not a fixed 24-attempt count.** The grow-gate's fast probe cycles (reopen → one attempt → re-trip) consume retry *attempts* far faster than wall-clock time, so the prior 24-attempt cap could exhaust on a *single* batch mid-grow — a fresh PS-320 cold-copy died on the `documents`/`bool_tiny` batch during the initial 12→39 GB grow, before coordination could help. Both the target-write reparent-retry (`flushWithReparentRetry`) and the source-read reconnect-retry (`copyTableWithSourceReadRetry`) now terminate on an elapsed-time deadline (~30 min, sized to span a multi-step 12→39→62→214 GB auto-grow) rather than an attempt count; the count remains only as a high runaway backstop. A genuinely-wedged or undersized target still fails loudly once the window passes — bounded, never infinite.

This is the robust "don't get stuck on a storage threshold and have to restart from scratch" guarantee that the whole v0.99.92–v0.99.103 arc has been building toward: every transient grow face is retriable (v0.99.92–v0.99.101), a single batch rides the grow on a wall-clock budget (this release), and the lanes coordinate their pause so the grow completes faster (the gate, now actually engaged). Pinned by wall-clock-bound tests on both retry paths plus the existing convergence / exhaustion / ctx-cancel pins.

## Compatibility

No configuration changes and no behaviour change for an untroubled migration — the gate stays inert until a classified grow-transient (or telemetry signal) trips it, and the wall-clock retry only changes *when* a persistent transient gives up (after ~30 min instead of ~24 attempts). A nil gate / non-PlanetScale target is a byte-for-byte no-op. No resume-format, wire, or result-state changes.

## Who needs this

Anyone running a continuous `sync` cold-start into a **non-Metal PlanetScale MySQL** target across storage auto-grow steps — this is the release where the grow-window resilience actually engages end-to-end on that path: the copy rides each grow on a wall-clock budget and the lanes coordinate their pause. Automatic; no action required.

## Install

```
go install sluicesync.dev/sluice/cmd/sluice@v0.99.103
```

Container image:

```
docker pull ghcr.io/sluicesync/sluice:0.99.103
```
