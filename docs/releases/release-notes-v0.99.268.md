# sluice v0.99.268

MariaDB joins the engine matrix as a first-class bulk source and target (roadmap item 73 Phase 1, ADR-0168) — operator-requested, scoped by a live probe against MariaDB 11.4 and 10.11.

## Added

- **The `mariadb` flavor: bulk migrate source and target, plus backup/restore/verify** (floor MariaDB 10.11 LTS). `restore` into MariaDB already worked; this closes the source and target gaps a MySQL-8-only reader/writer left, all live-tested against 11.4 and 10.11 with the full MySQL-8 regression suite unchanged:
  - The schema reader's `information_schema` queries flavor-gate the MySQL-8-only `srs_id` and `statistics.expression` columns MariaDB doesn't have.
  - The migrate-state store and change applier use MariaDB's `VALUES()` upsert spelling instead of MySQL 8.0.20+'s row-alias form.
  - A **defaults shim** (shipped atomically with the query fix, because it would otherwise be a silent-corruption vector) normalizes MariaDB's distinct `COLUMN_DEFAULT` conventions — quoted literals, the literal string `'NULL'` for defaultless-nullable columns, `current_timestamp()` with empty `extra` — to a byte-identical IR read, so a MariaDB schema and its MySQL-8 equivalent produce the same IR.
  - The `utf8mb4_0900_*` vs `utf8mb4_uca1400_*` collation split between the LTS lines is remapped in the emitter with a WARN.
- **CDC from a MariaDB source is refused loudly** with the coded `SLUICE-E-CDC-MARIADB-UNSUPPORTED` — MariaDB's domain-based GTID positions (`0-100-38`) are Phase 3 (the vendored replication library already carries the support; the flavor's capabilities stay honest per phase). Geometry is deliberately excluded from the supported types for now — MariaDB has no `srs_id` catalog column to round-trip an SRID and spells the attribute `REF_SYSTEM_ID`, so carrying it would silently drop the SRID (Phase 2, with native uuid/inet and the sequence/system-versioned-table census). A source/target declared plain `mysql` that fingerprints as MariaDB WARNs, recommending `--source-driver`/`--target-driver mariadb`.

## Compatibility

- **Purely additive.** One new flavor name, one new coded refusal; the MySQL-8 path is byte-identical (every flavor gate carries the MySQL-8 behavior as its zero value). No change to any existing command or engine.

## Who needs this

Anyone with a MariaDB database to migrate onto MySQL, Postgres, PlanetScale, or another target — or onto MariaDB from any supported source. Bulk copy and backup/restore are supported now; continuous CDC sync is Phase 3.

## Install

- Binaries: https://github.com/sluicesync/sluice/releases/tag/v0.99.268
- Homebrew: `brew install sluicesync/tap/sluice`
- Scoop: `scoop bucket add sluicesync https://github.com/sluicesync/scoop-bucket; scoop install sluice`
- winget: `winget install sluicesync.sluice`
- Docker: `docker pull ghcr.io/sluicesync/sluice:v0.99.268`
