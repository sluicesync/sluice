# sluice v0.99.57

**PlanetScale/Vitess (VStream) resume from a purged GTID position now
reliably auto-recovers (Bug 146).** v0.99.51 shipped a *reactive* recovery
for this, but it couldn't actually fire on Vitess 24 — this release adds the
**proactive pre-flight** that closes the gap, so a resume position that has
fallen behind the source's binlog retention triggers a clean cold-start
re-snapshot instead of a silent restart loop.

## Fixed

- **Purged-GTID VStream resume (Bug 146, ADR-0093 amendment).** v0.99.51's
  reactive approach classified vtgate's "purged required binary logs" error
  as a cold-start trigger — but a local Vitess-24 cluster reproduction proved
  vtgate does **not** emit that error on a purged resume: it accepts the
  stale (behind) position, the source tablet drops the binlog dump (errno
  2013, `CRServerLost`), and vtgate keeps the stream open emitting only
  heartbeats. The stream therefore idled into a retriable liveness timeout
  and looped on the same purged position, never cold-starting. sluice now runs
  a **proactive pre-flight** on the VStream open path — `GTID_SUBSET(@@global.
  gtid_purged, <resume>)`, the same primitive the self-hosted binlog reader
  uses — and returns the invalid-position signal (→ cold-start re-snapshot)
  before opening the stream when the resume position is unreachable.
  - The check is **routed at the same tablet type the stream binds to**:
    `gtid_purged` is tablet-type-routed by vtgate, and a replica can purge
    independently of the primary, so reading the default (primary) value
    could miss a replica-tailing stream's gap. (This was the load-bearing
    finding.)
  - It strips the Vitess GTID flavor prefix (`MySQL56/…`) before the query,
    and **degrades gracefully** — if the probe connection/query can't run
    (e.g. a transient routing blip), it proceeds rather than forcing a
    spurious re-snapshot; only a definitive "unreachable" refuses.
  - The reactive classifier from v0.99.51 is **retained as defence-in-depth**
    for any source that does surface the error.

## Compatibility / notes

- No flag change. `--no-auto-resnapshot` still converts the recovery into a
  loud, actionable terminal error instead of an automatic re-snapshot
  (ADR-0093).
- Self-hosted MySQL (binlog) already had this via its own pre-flight; this
  brings the PlanetScale/Vitess (VStream) path to parity.

## Who needs this

- Anyone running continuous sync from a **PlanetScale / Vitess source** whose
  resume position can fall behind the platform's binlog-retention window
  (routine on PlanetScale's ~multi-day retention, e.g. after a pause or a
  co-tenant migration). Previously such a resume restart-looped; now it
  cold-starts cleanly.

## Install

```
# Linux/macOS/Windows binaries + checksums on the release page.
docker pull ghcr.io/sluicesync/sluice:v0.99.57
```
