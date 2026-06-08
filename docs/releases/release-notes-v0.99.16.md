# sluice v0.99.16

**Multi-database MySQL migration and continuous sync.** A single `sluice` run can now connect to a MySQL server and migrate — and continuously sync — many databases at once, each landing in its own same-named target namespace, analogous to how a Postgres source carries multiple schemas. Drop-in upgrade from v0.99.15 — purely additive; without the new flags, every existing single-database run is byte-identical.

## Added

- **`migrate` across multiple MySQL databases in one run.** New flags `--include-database <glob>` / `--exclude-database <glob>` (mutually exclusive, repeatable) and `--all-databases`. When any is set, the source DSN is a *server* connection (its database component is optional), sluice enumerates the server's databases, and each selected database is migrated to a **same-named target namespace** — a Postgres **schema** (MySQL→Postgres) or an auto-created target **database** (MySQL→MySQL, via `CREATE DATABASE IF NOT EXISTS`).

  System databases (`information_schema`, `performance_schema`, `mysql`, `sys`) are always excluded. Cross-database foreign keys are preserved when both databases are in scope (applied in a final pass once every database's tables exist); a foreign key pointing at a database *outside* the selected set is **refused loudly** — sluice can't guarantee the referent exists on the target, so it never silently flattens the reference.

- **`sync start` across multiple MySQL databases — cold-start, CDC, and resume.** The same `--include-database` / `--exclude-database` / `--all-databases` flags on `sync start` give continuous multi-database replication:

  - **One consistent snapshot across all databases.** The cold start captures a single `START TRANSACTION WITH CONSISTENT SNAPSHOT` on one pinned connection and one binlog position, so the snapshot→CDC handoff is a single gapless cut across every selected database.
  - **One CDC stream, routed per change.** Steady-state CDC rides the **server-wide MySQL binlog as one stream** and routes each change to its source database's target namespace — not N streams to coordinate.
  - **Warm resume.** A stopped stream resumes from the one persisted server-wide position without re-copying.

  Works MySQL→MySQL and MySQL→Postgres.

## Fixed

- **Silent-loss boundary gap in the binlog snapshot→CDC handoff (single- *and* multi-database).** The binlog snapshot opener captured the row view (`START TRANSACTION WITH CONSISTENT SNAPSHOT`) and the CDC start position (`SHOW BINARY LOG STATUS`) as two separate statements. A transaction committing in the window between them landed in **neither** the snapshot **nor** the CDC tail — a silently lost row. The capture is now wrapped in `FLUSH TABLES WITH READ LOCK` … `UNLOCK TABLES` (the mydumper/Debezium consistent-snapshot pattern), so the snapshot view and the binlog position name the exact same logical cut. `FLUSH TABLES WITH READ LOCK` needs the `RELOAD` privilege; absent it, sluice warns and falls back to the prior lock-free capture rather than failing. The fix lands in the shared snapshot opener, so it closes the gap for both the new multi-database cold start and the pre-existing single-database binlog snapshot path.

## Compatibility

- No breaking API or CLI changes. Drop-in from v0.99.15. Multi-database mode engages only when a `--*-database` / `--all-databases` flag is set; without them, single-database `migrate` and `sync start` are byte-identical (same snapshot, same position, same apply path). This is MySQL-source fan-out — PlanetScale/VStream multi-keyspace and the reverse Postgres-source→MySQL-multi-database direction are tracked follow-ons. A MySQL→MySQL multi-database target DSN must name a "home" database for the sync control table (it errors clearly if absent); per-source user data still routes to its own database.

## Who needs this

- **Anyone consolidating or replicating a MySQL server with many databases** (the classic "connect as `root`, copy everything" case). One `sluice migrate --all-databases` or `sync start --all-databases` moves/keeps-in-sync the whole server, each database in its own target namespace — instead of one process per database.
- **MySQL→Postgres movers** get each MySQL database as its own PG schema automatically, no per-database `--target-schema` juggling.

---

**Install:** `brew install sluicesync/tap/sluice`  ·  `go install sluicesync.dev/sluice/cmd/sluice@v0.99.16`  ·  **Container:** `ghcr.io/sluicesync/sluice:0.99.16`
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
