# sluice v0.137.2

> **Correction (2026-09-02):** the "Not affected" line below — "GTID mode, where GTID UUIDs are themselves instance-bound and were always checked" — was false. The GTID resume arm checked only that nothing the position needs had been *purged*, and a fresh or reset instance has an empty `gtid_purged`, which is a subset of everything. A GTID-mode position captured on one instance and resumed against an unrelated one was accepted, and `backup incremental` recorded the wrong instance's entire history as the chain delta at exit 0 (audit 2026-09-01, SLM-2). Fixed in v0.138.0, which adds the lineage check the sentence assumed existed. MariaDB, PlanetScale/Vitess and Postgres remain as stated.

**A silent data-loss hole in the MySQL backup→CDC handoff is closed.** If you resume MySQL sync from a backup manifest and your source runs `gtid_mode=OFF` — MySQL 8's default — a resume pointed at a *replaced or rebuilt* server could start streaming from a byte offset in an unrelated binlog and skip changes at exit 0. Upgrade, and take one fresh full backup to move existing chains onto the stronger guard. Drop-in from v0.137.1.

## Fixed

**A backup-captured binlog position now records which server it came from.**

MySQL binlog file names and offsets are *instance-local*. A replaced or rebuilt server starts a fresh, unrelated lineage that reuses the same names — `mysql-bin.000001` and so on — which is not an edge case: two stock MySQL 8 containers do it by default.

sluice already knew this. `binlogPos.ServerUUID` exists precisely as the loud-failure floor for the "node replaced / restored from backup" class, and the resume path refuses when a position's recorded `server_uuid` does not match the source's. The three CDC-side capturers stamped it.

**The two backup capturers did not.** The guard's check is a no-op when the recorded identity is empty, so it silently skipped — and the one door named in its own rationale, *restored from backup*, was the door it never reached. A `backup incremental`, or a `sync start --position-from-manifest`, pointed at a different instance was **accepted**: streaming began at a byte offset in a binlog that had nothing to do with the backup.

Whether that surfaced loudly depended on byte alignment. Land mid-event and you get a parse error; land on an event boundary and the run is silent. Reproduced end to end on two independent MySQL 8.0.46 instances whose binlogs both carried `mysql-bin.000001`: **three source rows never reached the target**, while rows written after the resume applied normally — a healthy-looking stream with a quietly skipped window, exit 0.

Both capturers now stamp the source's `@@server_uuid`. A cross-instance resume refuses loudly and cold-starts instead. An unreadable `@@server_uuid` degrades to no stamp with a WARN rather than failing the backup; the cost is that that one cursor keeps the previous filename-only protection.

## Who was exposed

**Affected:** MySQL sources in file/pos mode — `gtid_mode=OFF`, which is MySQL 8's default — resuming through the backup→CDC handoff, *after* the source instance was replaced, rebuilt, or swapped. It takes an instance-replacement event to bite; a stable source was never at risk.

**Not affected:** GTID mode, where GTID UUIDs are themselves instance-bound and were always checked. MariaDB, which is always in GTID mode. PlanetScale and Vitess, which ride VStream positions on a different arm. And every Postgres source — a standby or replaced source is refused by its own preflight.

## What to do

Upgrade. New backups stamp the identity automatically.

**Backups taken before this release carry no identity**, and the resume path deliberately still accepts them rather than forcing a full re-copy on positions that are almost certainly fine. Those chains keep exactly the filename-only protection they have always had — no worse than before, no better. **Taking one fresh full backup is what moves a chain onto the stronger guard**, and it is worth doing if your source could ever be replaced under you: managed-service failovers, node replacements, and restore-from-backup events are the shapes that matter.

## Internal

`TestFilePosPositionsCarryServerUUID_ASTRoster` derives its universe from the AST — every `binlogPos` literal in the package — rather than a hand-written list, so a future capture door cannot be added without either stamping the identity or failing the gate by `file:line`. Anti-vacuity floor at today's 5 sites across 4 files. The cross-instance refusal has its own two-instance integration pin, with the acceptance and the refusal graded by separate assertions so neither can pass on the other's evidence.

## Compatibility

Drop-in from v0.137.1 — no schema, format, or flag change. Positions captured by earlier versions continue to resume exactly as before.

## Install

```
# Homebrew
brew upgrade sluice

# Scoop (Windows)
scoop update sluice

# Go
go install sluicesync.dev/sluice/cmd/sluice@v0.137.2
```

Container images: `ghcr.io/sluicesync/sluice:0.137.2` (multi-arch; the image tag carries no `v` prefix).

**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
