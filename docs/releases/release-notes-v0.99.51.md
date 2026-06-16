# sluice v0.99.51

**A PlanetScale / Vitess (VStream) sync that resumes from a purged GTID position
now auto-recovers instead of restart-looping (ADR-0093).** If a sluice resume
position falls behind the source's binlog-retention window (`gtid_purged`
advances past it — routine on PlanetScale), the stream used to exit and
restart-loop on the same dead position. It now self-heals with a one-shot
cold-start re-snapshot, the same way the self-hosted MySQL binlog path already
does.

## Fixed

- **VStream purged-GTID resume → auto cold-start re-snapshot (ADR-0093).** The
  self-hosted binlog source recovers a purged resume position via a pre-flight
  `gtid_purged ⊆ resume` check → cold-start (ADR-0022). VStream has no clean
  pre-flight (vtgate is a proxy; there is no single authoritative `gtid_purged`),
  so the condition only surfaces *reactively* when vtgate rejects the position on
  the stream — and that error was classified only as retriable/terminal, never as
  an invalid position, so the stream restart-looped. Now sluice:
  - classifies the vtgate "purged required binary logs" error as an invalid
    position (ahead of the retriable check — retrying a purged position never
    succeeds);
  - routes it to a **one-shot, non-destructive cold-start re-snapshot** (the
    idempotent copy absorbs the overlap; the target is not dropped);
  - **bounds** the recovery — a second consecutive invalid position immediately
    after a fresh re-snapshot fails loudly (the source is purging faster than a
    snapshot can complete, which auto-retry cannot fix).

  This was always a loud failure, never silent data loss; it now self-heals.

## Added

- **`--no-auto-resnapshot`** (`sync start`) — opt out of the automatic
  re-snapshot. A purged/invalid resume position then surfaces as a loud,
  actionable terminal error naming the recovery commands (`--restart-from-scratch`
  / `--reset-target-data`) instead of auto re-snapshotting. For operators who
  would rather decide a (potentially expensive) full re-snapshot of very large
  tables deliberately. Gates both the binlog pre-flight fall-through and the new
  VStream reactive recovery, so the two paths stay consistent. Default (unset) =
  auto-recover, parity with the binlog path.

## Compatibility / notes

- No change to steady-state behavior or on-disk/wire formats. The new behavior
  only triggers on a resume from a position the source has purged.
- Default-on (auto re-snapshot) matches the existing binlog-source behavior, so
  nothing regresses for self-hosted MySQL; the VStream/PlanetScale source simply
  reaches parity.

## Who needs this

- Anyone running **continuous sync from a PlanetScale / self-hosted Vitess
  source** whose resume position can age past the source's binlog retention
  (e.g. a sync paused/down longer than the retention window). It now recovers
  automatically instead of needing a manual re-snapshot.

## Install

```
# Linux/macOS/Windows binaries + checksums on the release page.
docker pull ghcr.io/sluicesync/sluice:v0.99.51
```
