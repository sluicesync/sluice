# sluice v0.138.0

**Closes a silent data-loss hole in MySQL GTID-mode resume, and corrects v0.137.2's claim that GTID mode was never exposed.** If you run MySQL sync or a backup chain with `gtid_mode=ON` and your source instance can be replaced, reset, or rebuilt — a fresh primary after a failover that did not carry the old lineage, a `RESET MASTER`, a restore without `--set-gtid-purged` — read the Fixed section.

## Fixed

**A GTID-mode position is now bound to the source's lineage, not just to whatever the source has not purged.**

v0.137.2 stamped the source's `@@server_uuid` onto file/pos positions so a resume against a replaced instance refuses, and said GTID mode did not need that because "GTID UUIDs are themselves instance-bound and were always checked". That was false. The GTID resume arm ran one check — `GTID_SUBSET(@@global.gtid_purged, resume)`, "has the source thrown away anything I still need?" — and a fresh or reset instance has purged nothing: its `gtid_purged` is empty, and the empty set is a subset of every set. So a position captured on instance A and resumed against unrelated instance B was accepted, and MySQL, asked to start after a set it had never seen, streamed B's *entire history*.

Observed on two real MySQL 8.0 instances with `gtid_mode=ON` (audit 2026-09-01, SLM-2): `backup incremental` against the replacement recorded fourteen of the other instance's changes as the chain's delta at exit 0, with a manifest `end_position` that was the union of two lineages — a chain that restores to a state matching neither instance, every signature and count internally consistent.

The fix is the other direction: `GTID_SUBSET(resume, @@global.gtid_executed)` — the source must have *executed* everything the position claims to have consumed. A promoted replica passes (its executed set contains what it replicated). A restore with `--set-gtid-purged=ON` passes (`gtid_purged` seeds `gtid_executed`). A fresh instance, a `RESET MASTER`, and a replica promoted without transactions the old primary had — transactions sluice has already applied to the target — refuse with the position-invalid error and route to the same cold-start fall-through the file/pos arm's identity mismatch does. The refusal names both sets.

Pinned on real servers in all three directions, because any one alone can pass for the wrong reason: the foreign instance refuses; the same instance accepts; and a third instance whose `gtid_executed` was seeded with the position's set — the shape of a promoted replica or a `--set-gtid-purged=ON` restore — accepts even though its `@@server_uuid` differs. The mutation run that bypasses the new check fails exactly the first direction.

**MariaDB was measured, not assumed — and it was exposed too, in two shapes the server does not refuse.** MariaDB GTIDs are `(domain, server_id, sequence)` with no instance identity, and the MariaDB arm relied on the server refusing a position it cannot serve. It refuses only some: resuming instance A's `0-1-3` on a fresh instance with a *different server_id* returns error 1236 ("not in the master's binlog"), which sluice had classified as terminal rather than as position-invalid, so that refusal now takes the cold-start fall-through. But a foreign instance with a *different `gtid_domain_id`*, and a *rebuilt* instance with the same server_id whose own history happens to read `0-1-3`, are both ACCEPTED, and MariaDB streams their entire history as the position's continuation. Measured on real MariaDB 11.4 through the exact wire path sluice uses.

The MariaDB closure is a lineage anchor: every MariaDB position now records the binlog `(file, offset)` its GTID set was captured at, and on resume sluice asks the source for `BINLOG_GTID_POS(file, offset)`. On the same lineage that returns exactly the recorded set; on a rebuilt or foreign instance it returns NULL or a different set, and sluice refuses with the position-invalid error. As a second door, every domain in the resume set must appear in the source's `@@gtid_binlog_state`. A MariaDB position persisted before this release carries no anchor: it resumes as before, with the `UNVERIFIED-INSTANCE-IDENTITY` WARN the file/pos arm already uses for the same situation, and one fresh full backup or cold start replaces it.

## Who was exposed

MySQL sources in GTID mode (`gtid_mode=ON`), on every release that resumed a GTID position, when the source instance was replaced, reset, or rebuilt without carrying the old lineage — through `sync` warm resume, `backup incremental`, chain replay, and the backup→CDC handoff, all of which share the one resume check. **Also exposed:** MariaDB sources, in the two shapes described above, and PlanetScale/Vitess sources in one shape, on every release that resumed such a position. **Not exposed:** MySQL file/pos mode (v0.137.2's `@@server_uuid` stamp); Postgres (its own identity pin, ADR-0051).

**PlanetScale and Vitess were measured on a real Vitess cluster, at the operator's request.** vttablet itself refuses a resume position from a different lineage — `GTIDSet Mismatch`, the same containment predicate the MySQL arm gained — so there is no whole-history replay on the sync path. The problem was that the refusal did not reliably reach sluice. With `backup incremental` against a different cluster, vtgate marked the refusing tablet ignorable and blocked waiting for another; sluice's window deadline expired into a clean close, the chain committed a zero-change link with an *empty* end position, and the next link started from the source's current position on the unrelated cluster: silent loss at exit 0. With `sync start` the refusal was terminal with no cold-start route when vtgate had three or more candidate tablets, and a 255-second stall blamed on a missing replica when it had fewer. Three closures: the VStream pre-flight asks each shard `GTID_SUBSET(resume, @@global.gtid_executed)` before opening and refuses at the door; the reactive `GTIDSet Mismatch` is classified as position-invalid and takes the cold-start route; and a chain extension whose parent link recorded no end position — a quiet window, or a DDL-only one, which records an empty end position by design — now resumes from the nearest ancestor that recorded one, which is where that link itself started, instead of falling into the legacy branch meant for v0.16-era fulls and starting from the source's *current* position. That last shape turned out to be engine-independent: any quiet window followed by writes and another incremental silently skipped those writes, on every engine. It is the arm that made the Vitess cell silent rather than merely slow, and `backup stream` had the same branch and gets the same rule. A primary replaced by a tablet restored from the same lineage still resumes, measured: an emergency reparent to a fresh-UUID tablet, with rows converging after.

One more thing the anchor had to get right, found by the same review before the tag: a start-of-stream anchor carried forever would have been purged by routine binlog retention on the *same* lineage, turning every retention window into a spurious re-snapshot. So the anchor follows the stream — it moves to each new binlog file's first boundary at every rotation the stream sees — and when a resume finds its anchor's file gone, sluice distinguishes a purge on the same lineage (a retained file numbered above the anchor whose own start state covers the anchor's set) from a different instance (no such file), measured on real MariaDB. The residual, stated: a rebuilt instance whose binlog is byte-identical up to the anchor reproduces it and passes; with the anchor following live traffic, that requires reproducing that traffic byte for byte.

## Compatibility

Drop-in from v0.137.4. No flag change. MariaDB positions gain three optional fields (the lineage anchor); older binaries ignore them, and positions written by older binaries resume with the WARN described above. New refusal shapes: a MySQL GTID resume whose set is not contained in the source's `gtid_executed`, and a MariaDB resume whose anchor the source cannot reproduce or whose domain the source has never written, now refuse where they previously streamed. Each takes the existing cold-start fall-through (a re-snapshot by default; a hard stop under `--no-auto-resnapshot`), the same as a file/pos identity mismatch. The MySQL refusal distinguishes a source that is merely *behind* the position (every source UUID present, sequence numbers lower — a lagging replica behind a load-balanced or failover endpoint) from a different lineage, and says which; and lineage is checked before retention, so a foreign source that has also purged binlogs is diagnosed as foreign rather than with a point-in-time-recovery hint that does not apply.

## Install

```
# Homebrew
brew upgrade sluice

# Scoop (Windows)
scoop update sluice

# Go
go install sluicesync.dev/sluice/cmd/sluice@v0.138.0
```

Container images: `ghcr.io/sluicesync/sluice:0.138.0` (multi-arch; the image tag carries no `v` prefix).

**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
