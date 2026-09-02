# sluice v0.137.5

**Closes a silent data-loss hole in MySQL GTID-mode resume, and corrects v0.137.2's claim that GTID mode was never exposed.** If you run MySQL sync or a backup chain with `gtid_mode=ON` and your source instance can be replaced, reset, or rebuilt — a fresh primary after a failover that did not carry the old lineage, a `RESET MASTER`, a restore without `--set-gtid-purged` — read the Fixed section.

## Fixed

**A GTID-mode position is now bound to the source's lineage, not just to whatever the source has not purged.**

v0.137.2 stamped the source's `@@server_uuid` onto file/pos positions so a resume against a replaced instance refuses, and said GTID mode did not need that because "GTID UUIDs are themselves instance-bound and were always checked". That was false. The GTID resume arm ran one check — `GTID_SUBSET(@@global.gtid_purged, resume)`, "has the source thrown away anything I still need?" — and a fresh or reset instance has purged nothing: its `gtid_purged` is empty, and the empty set is a subset of every set. So a position captured on instance A and resumed against unrelated instance B was accepted, and MySQL, asked to start after a set it had never seen, streamed B's *entire history*.

Observed on two real MySQL 8.0 instances with `gtid_mode=ON` (audit 2026-09-01, SLM-2): `backup incremental` against the replacement recorded fourteen of the other instance's changes as the chain's delta at exit 0, with a manifest `end_position` that was the union of two lineages — a chain that restores to a state matching neither instance, every signature and count internally consistent.

The fix is the other direction: `GTID_SUBSET(resume, @@global.gtid_executed)` — the source must have *executed* everything the position claims to have consumed. A promoted replica passes (its executed set contains what it replicated). A restore with `--set-gtid-purged=ON` passes (`gtid_purged` seeds `gtid_executed`). A fresh instance, a `RESET MASTER`, and a replica promoted without transactions the old primary had — transactions sluice has already applied to the target — refuse with the position-invalid error and route to the same cold-start fall-through the file/pos arm's identity mismatch does. The refusal names both sets.

Pinned on real servers in all three directions, because any one alone can pass for the wrong reason: the foreign instance refuses; the same instance accepts; and a third instance whose `gtid_executed` was seeded with the position's set — the shape of a promoted replica or a `--set-gtid-purged=ON` restore — accepts even though its `@@server_uuid` differs. The mutation run that bypasses the new check fails exactly the first direction.

**MariaDB was measured, not assumed.** The MariaDB arm has no `GTID_SUBSET` and relies on the server refusing a position it cannot serve. It does: resuming instance A's position on fresh instance B, MariaDB 11.4 returns error 1236 "connecting slave requested to start from GTID 0-1-3, which is not in the master's binlog". But sluice's classifier recognised only the *other* 1236 wording, the one for a position that was once held and since purged, so this refusal was loud and terminal: the stream exited instead of taking the cold-start fall-through the arm's own comment promises. It is now classified as position-invalid and takes that route. No silent loss on MariaDB in any version.

## Who was exposed

MySQL sources in GTID mode (`gtid_mode=ON`), on every release that resumed a GTID position, when the source instance was replaced, reset, or rebuilt without carrying the old lineage — through `sync` warm resume, `backup incremental`, chain replay, and the backup→CDC handoff, all of which share the one resume check. **Not exposed:** file/pos mode (v0.137.2's `@@server_uuid` stamp); MariaDB (refused by the server, now routed rather than terminal); Postgres (its own identity pin, ADR-0051). PlanetScale and Vitess ride VStream positions on a different arm and were **not measured** in this release; that is stated so nobody assumes it.

## Compatibility

Drop-in from v0.137.4. No schema, format, or flag change. One new refusal shape: a GTID-mode resume whose set is not contained in the source's `gtid_executed` now refuses where it previously streamed. The refusal takes the existing cold-start fall-through (a re-snapshot by default; a hard stop under `--no-auto-resnapshot`), the same as a file/pos identity mismatch.

## Install

```
# Homebrew
brew upgrade sluice

# Scoop (Windows)
scoop update sluice

# Go
go install sluicesync.dev/sluice/cmd/sluice@v0.137.5
```

Container images: `ghcr.io/sluicesync/sluice:0.137.5` (multi-arch; the image tag carries no `v` prefix).

**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
