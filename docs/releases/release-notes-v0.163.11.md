# sluice v0.163.11

<!-- notes-claims-exempt: CROSS-SHARD-VINDEX-NOT-IN-PK -->
<!-- The refusal for Bugs 337/338 is built on a separate branch and ships in its own release; named here so operators can follow it. -->

**Fix, loud: `--inject-shard-column` from a PlanetScale or Vitess source stopped at the first change (`SHAPE-A-KEYLESS-BOUNDARY`).** Every per-shard stream that stamps a shard discriminator into a consolidated target (Shape A) exited with "InjectShardColumn: table … has no primary key" at its first CDC change, with or without `--no-coordinate-live-ddl`. Nothing was written wrongly; the stream just could not run. Affected: VStream sources (PlanetScale, Vitess), v0.156.10 (by the code, when the check landed) through v0.163.10 (measured). Drop-in: no flag, state or format change.

## Fixed

- **A VStream schema boundary no longer refuses Shape A for having no primary key (`SHAPE-A-KEYLESS-BOUNDARY`, loud, v0.156.10 through v0.163.10).** v0.156.10 (GC-44) began checking each table's first schema boundary against the target. For a Shape A stream that check translates the boundary through `InjectShardColumn`, which folds the discriminator into the primary key and refuses a table without one. A VStream schema boundary carries the table's columns and no key, so every such boundary refused. Measured on the v0.163.10 binary: one stream per shard, each with `--inject-shard-column`, every stream exited rc 1 at its first change. Now, on a VStream source only (`keylessBoundaries`), the boundary's columns are compared with the discriminator column appended (`translate.AppendShardColumn`) and no key fold. On a binlog or Postgres source, whose boundaries carry the key, a boundary without one still refuses. Pinned by `TestFirstBoundaryWitness_ShapeAKeylessBoundary` (MySQL and Postgres targets, keyed lane still refusing) and `TestAppendShardColumn`.
- **The discriminator-collision refusal is kept on that path.** The first cut of this fix appended the discriminator by hand and so dropped `InjectShardColumn`'s other refusal: a source table that already has a column with the discriminator's name. A keyless boundary carrying its own `src_shard` would then have matched, and the applier's shard stamp would have overwritten that source column's value on every row. The pre-tag review caught it; both functions now share one collision check, pinned on the keyless path for MySQL and Postgres targets.

## Not closed, stated

- **A primary key dropped mid-stream on a PlanetScale or Vitess table whose only key is its primary key is not re-checked (`VSTREAM-PK-ONLY-MIDSTREAM-KEY-DROP`, filed).** The VStream boundary carries no key, and the mid-stream unique-key re-check covers only tables with a unique key besides the primary key. A key dropped while the stream is stopped is caught at the next start (`UNIQUE-KEY-STOPPED-WINDOW-DOOR`). Single-stream VStream had the same exposure before this release.
- **Bugs 337/338 (open, silent): `--allow-cross-shard-merge` loses rows an application moves between shards when a table's primary vindex is not inside its primary key.** A row whose vindex column changes keeps its id and moves shard, and the merged stream can apply the new shard's insert before the old shard's delete. Measured loss on MySQL targets; any target is exposed by the code. A refusal for this shape (`CROSS-SHARD-VINDEX-NOT-IN-PK`) ships in its own upcoming release. Until then, the safe topology for such tables is one stream per shard (`vstream_shards=<shard>` in the source DSN), each with its own `--inject-shard-column` value — the topology this release makes runnable. Measured exact on MySQL and Postgres targets, 701,042 of 701,042 rows with forward, insert-first and tail moves during and after the copy, on the branch carrying this fix's first cut (the collision refusal added since does not touch that path).
- **Bugs 333/336 (open): a drained `schema add-table` is exact for inserts, updates and deletes made during the add; primary-key changes on the added table during the add are not covered.** A fix is in review and ships in its own release.
- **Test coverage on this release.** The integration tests run in the per-PR CI shards and the dispatched Extended suites. The `psverify` (PlanetScale) and `d1verify` suites were not run; the change touches only the VStream Shape A boundary check, measured on vttestserver.

## Compatibility

- **Drop-in.** No flag, control-table, backup-format or `BackupFormatVersion` change. Binlog and Postgres sources behave as in v0.163.10.

## Who needs this

- **Anyone running `--inject-shard-column` from a PlanetScale or Vitess source**, or planning per-shard streams for the Bugs 337/338 shape above: upgrade, then start the streams as usual.
- **Everyone else:** nothing to do.

---

**Install:** `brew install sluicesync/tap/sluice` · `go install sluicesync.dev/sluice/cmd/sluice@v0.163.11` · **Container:** `ghcr.io/sluicesync/sluice:0.163.11`

**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
