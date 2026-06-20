# sluice v0.99.86

**The MySQL source-unresponsive diagnosis now gives you the exact, safe binlog-purge command — not just "consider PURGE BINARY LOGS".** A small, high-leverage follow-up to v0.99.85.

## Added

**Exact, safe binlog-purge remediation in the warm-resume timeout diagnosis.** When a resume-position verify times out and the v0.99.85 liveness probe attributes it to binlog/disk pressure on the source, sluice now derives the precise remediation from the resume position it already holds and includes it in the (still retriable, still never `ErrPositionInvalid`) error:

- **file/pos mode** → the exact command `PURGE BINARY LOGS TO '<resume-file>'`. MySQL's `PURGE … TO` deletes only the binary logs *older* than the named file and keeps that file — which is precisely the file the stream resumes from — so running it frees space without losing this stream's resume point. The operator gets a copy-paste command instead of having to reason out a safe boundary on a wedged source.
- **GTID mode** → there is no single resume file, so sluice states the constraint instead: purge so that no GTID in this stream's resume set is removed (e.g. `PURGE BINARY LOGS BEFORE` a timestamp older than the resume).

Every hint carries an explicit shared-infrastructure caveat: sluice knows only *its own* resume needs, so other replicas or point-in-time-recovery backups may still need the older logs — the operator must confirm before purging.

**sluice surfaces the command; it never runs it — by design.** Purging source binary logs is destructive, affects infrastructure sluice cannot see (other replicas, PITR backups), and would require granting sluice's source credential an elevated privilege (`BINLOG_ADMIN`, or the deprecated `SUPER`) that it deliberately does not need — the CDC user holds only `REPLICATION SLAVE`, `REPLICATION CLIENT`, and `SELECT`. Auto-executing a destructive infra operation on the strength of sluice's partial view would violate the project's "surface complexity explicitly, never silently auto-handle" stance — the same report-don't-auto-apply posture sluice takes for forwarded index DDL (ADR-0103). So the operator gets the exact command and runs it deliberately.

This was validated against the live large-scale source that motivated v0.99.85 (a source that had accumulated 2,585 binlog files on a 100%-full disk): the diagnosis now points straight at the exact `PURGE BINARY LOGS TO` target for the resume position.

## Compatibility

No interface, flag, or default-behavior changes. This only enriches the text of the verify-timeout diagnosis (which itself fires only on a timeout); it changes no retry, position, or apply behavior, and runs nothing against the source. No effect on Postgres sources or the steady-state path.

## Who needs this

Operators running `sluice sync` against MySQL/Vitess/PlanetScale sources. If a resume stalls because the source's binlog disk is under pressure, sluice now tells you the exact, safe command to free space — preserving your resume point — rather than leaving you to derive it by hand.

## Install

```
go install sluicesync.dev/sluice/cmd/sluice@v0.99.86
```

Container image:

```
docker pull ghcr.io/sluicesync/sluice:0.99.86
```
