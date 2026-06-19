# sluice v0.99.75

**Fixes a HIGH multi-shard warm-resume bug: a Vitess/PlanetScale source whose sync was interrupted no longer gets falsely told its position was "purged" and forced into a needless full re-copy. Plus: the cross-engine schema-narrowing heads-up notices that `migrate` always showed now also appear on `sync` cold-start.** Drop-in upgrade; no config changes, no behavior change to healthy syncs.

## Fixed

**Multi-shard Vitess/PlanetScale warm-resume was falsely rejected as "position purged" (HIGH).** When a `sync` against a sharded Vitess/PlanetScale keyspace was restarted, the purged-GTID preflight (the ADR-0093 check that re-snapshots when the source has aged binlogs past the saved position) could reject a perfectly valid resume position and force a full cold-start re-copy — wasting hours and, without `--force-cold-start`, surfacing as a loud refusal. The root cause, found by three-phase investigation against a real 2-shard source: the check read `@@global.gtid_purged` through a keyspace-level vtgate target, and vtgate answers that variable from an *arbitrary shard per query* — so the per-shard subset check compared one shard's purged set (a different server-UUID) against another shard's resume position, which is never a subset, yielding a false "purged" verdict on essentially every multi-shard resume. This is the real cause behind repeated re-copies (and several handoff failures previously attributed to binlog-retention expiry) seen on large-scale Vitess→PlanetScale runs. The fix targets each shard's `@@global.gtid_purged` read at that shard specifically (`keyspace:shard@<tablettype>`), so the comparison is always same-shard/same-UUID and correct. The degrade-don't-refuse behavior is preserved — a transient probe failure proceeds rather than forcing a spurious re-snapshot. Unsharded keyspaces were never affected. Validated live: a 2-shard keyspace now warm-resumes CDC from its persisted position with zero false rejection and zero re-copy.

## Added

**Cross-engine narrowing notices now appear on `sync` cold-start too, not just `migrate`.** The advisory WARN heads-ups for sluice's deliberate cross-engine type narrowings — MySQL `bigint unsigned`→PostgreSQL `bigint`, unconstrained PostgreSQL `numeric`→MySQL `DECIMAL(65,30)`, and wide PostgreSQL `varchar(N)`→MySQL TEXT — were previously emitted only by `migrate` and `schema preview`. A continuous-sync operator therefore got no up-front signal and could hit a mid-copy abort on the first out-of-range value. They now fire on the `sync` cold-start path as well (single- and multi-database), before any data moves. They stay advisory (the common case keeps flowing), and a same-engine sync — where these mappings are lossless — emits none of them, so there's no false warning on the faithful path.

## Compatibility

Fully backward-compatible. No new flags, no config changes, and no change to the data any sync moves or the schema it creates. The warm-resume fix only changes how an internal preflight targets its probe query; the notices addition is log output only. Existing healthy syncs behave exactly as on v0.99.74.

## Who needs this

Anyone running `sluice sync` against a **multi-shard Vitess or PlanetScale source** should upgrade — without it, an interrupted sync's warm-resume can be falsely rejected and re-copy the whole dataset. The notices change benefits anyone doing cross-engine MySQL↔PostgreSQL `sync` cold-starts who wants the same up-front fidelity heads-up `migrate` already gave.

## Install

```
go install sluicesync.dev/sluice/cmd/sluice@v0.99.75
```

Container image:

```
docker pull ghcr.io/sluicesync/sluice:0.99.75
```
