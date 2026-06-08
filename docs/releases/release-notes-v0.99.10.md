# sluice v0.99.10

**A Vitess/PlanetScale stream-resilience release.** A stalled stream now fails loud instead of hanging silently, cold-start CDC errors are no longer swallowed, an unproductive reconnect loop after a tablet death can no longer churn forever, and a new `--restart-from-scratch` flag forces a clean cold-start without dropping the target. Drop-in upgrade from v0.99.9 — no breaking API or CLI changes; every new behaviour is opt-in and the defaults are unchanged.

The hardening was surfaced and validated by a new fault-injection (**"chaos"**) test suite run against a **full Vitess 24 cluster** — killed tablets, primary failovers (`PlannedReparentShard` + `EmergencyReparentShard`), vtgate restarts, and a rolling version upgrade — each asserting the load-bearing invariant: after a fault, the stream delivers every row **exactly once, or fails loudly — never a silent partial**.

## Added

- **`--restart-from-scratch` (sync) forces a fresh cold-start, ignoring the persisted position, without dropping the target.** It sits between `--force-cold-start` (which only skips the cold-start preflight and still warm-resumes from the persisted position, including a mid-COPY cursor) and `--reset-target-data` (which drops the target tables): it re-runs the full cold-start COPY from the beginning while leaving the target data in place (the idempotent COPY writer absorbs the re-copy). Mutually exclusive with `--reset-target-data` and `--position-from-manifest`; applies to `sync` only. Use it to recover a sync whose persisted position is suspect without a destructive target rebuild.
- **`vstream_progress_timeout` / `vstream_copy_progress_timeout` DSN parameters** tune the new mid-stream liveness watchdog (see below). Defaults: 45s (CDC tail), 10m (cold-start COPY, which tolerates vreplication's multi-minute slow start).

## Fixed

- **A wedged Vitess/PlanetScale stream now fails loud instead of hanging silently.** v0.99.7 added a *first-event* liveness watchdog (the silent primary-only stall). This generalizes it to a **continuous two-phase watchdog**: phase 1 is the absolute first-event deadline (unchanged); phase 2 re-arms on every event and fires a loud, actionable error if the stream goes totally silent mid-flight — no data, no heartbeat, `Err() == nil`. That is the failure mode a hard `EmergencyReparentShard` can leave behind, where the gRPC `Recv` goes dead-silent; without the watchdog a post-failover dead stream looked identical to an idle-but-healthy one.
- **Cold-start CDC-pump errors are no longer silently swallowed.** The cold-start CDC reader wrapper (`vstreamSnapshotChanges`) had no `Err()`, so the pipeline's optional-error probe read back `nil` and a genuine loud failure on the cold-start path — the watchdog's, and a post-failover "row event without preceding FIELD event" decode error — was dropped (a silent-partial hazard). It now delegates `Err()` to the underlying snapshot stream, so cold-start CDC failures surface loudly.
- **An unproductive reconnect loop after a tablet death no longer churns forever.** The in-place COPY reconnect budget reset on *any* successful `Recv`, so a loop of reconnect → non-progress events (a heartbeat or a stale VGTID when the cursor is unresumable post-reparent) → error → repeat never exhausted its budget and never failed. The reset is now gated on actual COPY *progress* (a row buffered), so an unproductive loop burns `reconnectMax` and surfaces a loud `failCopy` the pipeline's retry can act on. A *productive* reconnect — e.g. the COPY resuming across an `EmergencyReparentShard` onto a surviving replica — still resets and continues; the chaos suite validates that exact path end-to-end with **zero loss**.

## Compatibility

- No breaking API or CLI changes. Drop-in from v0.99.9. All defaults preserve existing behaviour; the watchdog windows are opt-in DSN knobs.
- **Operational note** (unchanged, now documented): a PlanetScale-branch **target** needs an `admin`-role password for sluice's control-table DDL (`readwriter` is denied — `Error 1105 … DDL command denied … [planetscale-writer]`); the **source** needs only read access.

## Who needs this

- **Anyone running a PlanetScale/Vitess CDC sync through real infrastructure events** — tablet failovers, vtgate rollouts, version upgrades. Before this release a post-failover dead stream or an unproductive reconnect loop could hang silently; now it either recovers with zero loss or fails loudly so your orchestration can retry.

---

**Install:** `brew install sluicesync/tap/sluice`  ·  `go install sluicesync.dev/sluice/cmd/sluice@v0.99.10`  ·  **Container:** `ghcr.io/sluicesync/sluice:0.99.10`
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
