# sluice v0.99.102

**The ADR-0110 coordinated grow-window pause is now actually engaged for `sync` migrations — v0.99.100 wired it only for `migrate`.** The v0.99.101 live validation (a fresh PS-320 `sync` cold-copy) surfaced this immediately: the grow-gate tripped zero times while the cold-copy writers logged 74 real grow-window retries. This release closes the wiring gap.

## Fixed

**The coordinated grow-gate is now attached to the cold-copy writer in the `sync` cold-start path, not only `migrate`.** ADR-0110 (v0.99.100) introduced one shared grow-gate so that during a PlanetScale storage-grow window all cold-copy lanes quiesce together instead of each independently hammer-retrying the struggling target. But v0.99.100 attached that gate to the writer only in the migrate keyset-chunked path (`openOneChunkConn`). The `sync` cold-start path — including the native-concurrent W×D cold-copy that every continuous PlanetScale CDC migration uses (and the Track-D validation rig) — opens a single top-level writer that the fan-out reuses across all D workers, and that writer never had the gate attached. So in a `sync` run the gate was inert on the write path (the source-read retry did get it), and the lanes rode a storage grow by independently retrying — exactly the thundering-herd behaviour the gate is meant to replace.

The live v0.99.101 run made it obvious: a fresh PS-320 `sync` cold-copy tripped the grow-gate **zero** times while its writers logged **74** real grow-window retries. The fix wires the gate centrally in `runBulkCopyPhases` (`applyGrowGate(rw, parallel.growGate)`), so every cold-copy path — sync parallel, native-concurrent, and migrate-nonchunked — engages the coordination; the migrate chunked path keeps its existing per-chunk wiring. Pinned by a `runBulkCopyPhases` wiring test (the top-level writer receives the run's gate) plus a nil-gate no-op pin.

This is the wiring counterpart to v0.99.101's classifier fix: v0.99.101 made the read-only grow face *retriable*; v0.99.102 makes the *coordination* actually engage during that retry in the path where it matters.

## Compatibility

No configuration changes and no behaviour change for an untroubled migration — the gate stays inert until a classified grow-transient (or a telemetry signal) trips it, and a nil gate / non-PlanetScale target is a byte-for-byte no-op. No resume-format, wire, or result-state changes. The correctness contract is unchanged; this only makes the existing coordination actually run in the `sync` path.

## Who needs this

Anyone running a continuous `sync` migration (cold-start → CDC) into a **non-Metal PlanetScale MySQL** target across storage auto-grow steps — the cold-copy lanes now coordinate their pause through a grow window instead of independently hammering the target, completing the grow faster and with fewer secondary errors. `migrate`-mode users already had the coordination since v0.99.100. Automatic; no action required.

## Install

```
go install sluicesync.dev/sluice/cmd/sluice@v0.99.102
```

Container image:

```
docker pull ghcr.io/sluicesync/sluice:0.99.102
```
