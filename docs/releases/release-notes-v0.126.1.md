# sluice v0.126.1

**The regression-cycle patch.** v0.126.0's own verification cycle filed three defects — all loud, none silent — and this patch closes all three, including the one that falsified the release's headline claim on its primary door: `migrate` into a sharded Vitess/PlanetScale keyspace still materialized sluice's state tables on every shard before the new refusal could fire. Also fixed: `backup incremental` refusing any PostgreSQL chain whose own scope carries a `tsvector`-family column, and a MySQL `round(x)` CHECK on a DOUBLE column dying at PostgreSQL CREATE TABLE. **Drop-in upgrade from v0.126.0.**

## Fixed

**`migrate` now reaches the sharded-keyspace refusal before its state bootstrap touches the target (Bug 250).** v0.126.0's sharded-target door guarded the schema-writer open — but `migrate` opens its migration-state store first, and that open materialized `sluice_migrate_state` and its progress tables on every shard before dying on the raced errors the release claimed closed. The same refusal (`SLUICE-E-SCHEMA-TARGET-KEYSPACE-SHARDED`) now guards the state-store open, so a migrate at a sharded keyspace refuses before anything lands. The live 2-shard pin now drives a real cross-engine migrate end to end and asserts zero `sluice_%` tables materialize; the v0.126.0 release notes carry a dated correction stating what the shipped door did and didn't protect.

**`backup incremental` no longer refuses a chain whose own scope carries a verbatim-family column (Bug 251 — the other half of the Bug 110 class).** A PostgreSQL chain over a table with a `tsvector`/`tsrange`/`xml`/`money`-family column takes its full fine, and then every incremental died at its own schema read with the engine's "unsupported data_type" refusal — that read never enabled the verbatim capture the full's read has. It does now, capture-only; the restore-time engine gates stay authoritative, and a verbatim column arriving mid-chain via a schema delta is refused per target — at chain-restore preflight toward MySQL-family targets, and by the SQLite emitter's own loud refusal toward SQLite/D1 (that asymmetry is stated in code and pinned). *(Correction, 2026-08-14, from this release's own verification cycle: the SQLite emitter arm is in practice unreachable — every drivable chain-restore-to-SQLite configuration is refused earlier, before any data lands, by the chain applier gate. Reality is strictly safer than this note originally stated; the emitter refusal stands as defense in depth.)*

**A MySQL CHECK using `round(x)` on a DOUBLE column now lands on PostgreSQL instead of dying at CREATE TABLE (Bug 252).** MySQL's catalog materializes `round(x)` as `round(x, 0)`, and PostgreSQL's two-argument `round` exists only for `numeric` — so on a DOUBLE/FLOAT column the passed-through CHECK failed with 42883 while `schema preview` rendered the doomed DDL without a word. The expression translator now strips the materialized zero scale (semantics-exact — it restores the author's own spelling; a non-zero scale is meaning and passes through), and `schema diff`'s canonicalizer learned PostgreSQL's two-word cast spellings (`::double precision`), which had turned the DOUBLE arm's read-back into phantom drift. The DOUBLE member of the class is now pinned beside the DECIMAL representative in the migrate-then-diff corpus.

## Compatibility

No flag, error code, or on-disk format changed. Two refusals now fire earlier or narrower on already-failing configurations (a sharded-keyspace migrate refuses cleanly at preflight instead of dying dirty; an incremental over a verbatim-bearing chain proceeds); one previously-failing migration shape (DOUBLE-column `round(x)` CHECKs toward PostgreSQL) now succeeds. **Drop-in upgrade from v0.126.0.** Nothing in this release requires re-verifying past runs — all three defects were loud.

## Who needs this — action required

**Anyone who tried `migrate` into a sharded Vitess/PlanetScale keyspace on v0.126.0**: upgrade, and drop any per-shard `sluice_migrate_state`/`sluice_migrate_table_progress` debris the failed run left behind. **Anyone whose `backup incremental` refuses with "unsupported data_type" on a tsvector/tsrange/xml/money column**: upgrade — the chain itself is fine; incrementals resume where they left off. **Anyone migrating MySQL CHECKs on DOUBLE/FLOAT columns toward PostgreSQL**: upgrade; `round(x)` constraints now land (a `round(x, 2)` non-zero scale on a DOUBLE column still refuses loudly at CREATE — that overload gap is filed). No one needs to re-verify previously migrated data.

## Install

```
# Homebrew
brew upgrade sluice

# Scoop (Windows)
scoop update sluice

# Go
go install sluicesync.dev/sluice/cmd/sluice@v0.126.1
```

Container images: `ghcr.io/sluicesync/sluice:0.126.1` (multi-arch; the image tag carries no `v` prefix).
