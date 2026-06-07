# ADR-0074: Multi-database MySQL migration and continuous sync

## Status

**Proposed (2026-06-07) — DRAFT for review.** Operator-requested: connect to a
MySQL server (e.g. as `root`, with access to many databases) and migrate **and
sync** all — or a selected subset — of its databases to a target in one run,
analogous to how a Postgres source can carry multiple schemas. Supersedes the
"Multi-source aggregation — MySQL native parity" roadmap item's `--rename-table`
framing for the *fan-out* (one server → N databases) case; the *fan-in* (N
sources → one namespace) case stays as described in [ADR-0031](adr-0031-multi-source-aggregation-target-schema.md).

## Context

Today a MySQL source is **hard-wired to a single database**. `parseDSN` requires
a database in the DSN (`internal/engines/mysql/connect.go`), the schema reader
filters `information_schema` by that one database name (`schema_reader.go`,
`WHERE table_schema = ?` on every query), and the Migrator/Streamer read **one**
`ir.Schema` exactly once. There is no `--database` / `--all-databases` selector;
the source database is chosen purely by the DSN's path component.

Two facts make multi-database support mostly *additive* rather than a rewrite:

1. **The IR is already multi-namespace-capable.** `ir.Table` and `ir.View` each
   carry a `Schema` field. Postgres sources populate it per-table; MySQL sources
   leave it empty under `ir.SchemaScopeFlat`. Nothing in the IR assumes a single
   namespace.
2. **The MySQL-database ↔ PG-schema equivalence is already settled** by
   [ADR-0031](adr-0031-multi-source-aggregation-target-schema.md): a MySQL
   database is the namespacing primitive that corresponds 1:1 to a PG schema.
   PG already namespaces target tables via `--target-schema`; the writer emits
   `Schema`-qualified DDL when set.

So the conceptual model is decided. What's missing is the plumbing: **enumerate
and select databases, loop per database, populate `Table.Schema`, route each to a
same-named target namespace, and apply CDC across all of them.**

The operator's two cases frame the routing:

- **Regular self-hosted / managed MySQL connected with broad privileges** — many
  databases on one server; the operator wants all of them (or a glob subset)
  copied and synced. This is the primary case for this ADR.
- **PlanetScale** — generally one database per branch (one keyspace), so the
  fan-out case mostly does not arise there; the per-table rename/mapping approach
  (ADR-0031) covers PlanetScale's single-database shape.

## The load-bearing insight: the MySQL binlog is already server-wide

The hard part of "multi-database **with CDC**" is *not* N streams. **MySQL's binlog
is server-wide** — a single binlog/replication connection already carries the
changes for every database, with each event tagged by its source database. So
multi-database binlog CDC is **one stream, one position (the server GTID/binlog
coordinate), and per-event apply-routing** to the correct target namespace — not N
independent streams to coordinate. This is *simpler* than the single-database case
conceptually generalised, and it is exactly the operator's "regular MySQL as root"
scenario.

PlanetScale/Vitess **VStream is the exception**: it is scoped to a *keyspace*
(≈ database), so multi-keyspace CDC genuinely needs N VStream connections with N
position tokens. That asymmetry drives the phasing below — vanilla-MySQL binlog
multi-database CDC is tractable now; VStream multi-keyspace is a distinct,
later sub-phase gated on real PlanetScale-multi-keyspace demand.

## Decision

Add a **multi-database mode** to the MySQL engine, driven by database-scope flags
that mirror the existing table-filter design, with the orchestrator iterating
databases for the snapshot and routing a single server-wide binlog stream for CDC.

### 1. CLI surface

- `--include-database GLOB` / `--exclude-database GLOB` — repeatable, glob-aware,
  mutually exclusive (mirrors `--include-table` / `--exclude-table`).
- `--all-databases` — convenience for "every non-system database".
- When any database-scope flag is set, the **source DSN's database becomes
  optional** (it is a *server* connection). With none set, behaviour is exactly
  as today (single database from the DSN) — fully back-compat.
- System databases (`information_schema`, `performance_schema`, `mysql`, `sys`)
  are **always** excluded, even from `--all-databases`.
- `--map-database SRC=DST` (repeatable, optional) renames on the target; the
  default is **same name**. Deferred to a 1.x follow-on unless a reviewer wants
  it in the first cut.

### 2. Source reader scoping — enumerate, then re-open per database (snapshot)

- New **optional** engine surface `ir.DatabaseLister`: `ListDatabases(ctx)
  ([]string, error)`. MySQL implements it via `information_schema.schemata` minus
  the system set. The orchestrator calls it to resolve `--all-databases` /
  globs into a concrete list.
- For the **snapshot** the orchestrator **re-opens a single-database reader per
  selected database** (a DSN clone with `DBName` set). This reuses 100% of the
  existing single-database reader/writer logic — each reader stays single-scope,
  as today — and avoids inventing a multi-schema `ReadSchema` variant. (A
  `SetDatabase` mutator was considered and rejected: re-open is simpler, gives a
  clean per-database snapshot boundary, and sidesteps connection-state reuse
  bugs. The small cost is N connect handshakes, negligible against a bulk copy.)

### 3. IR namespace population + the flat-scope carve-out

- In multi-database mode the MySQL reader **populates `Table.Schema` /
  `View.Schema` with the source database name**. In single-database mode it stays
  empty (back-compat; existing tests unaffected).
- **Foreign keys:** MySQL permits cross-database InnoDB FKs. Today the reader
  *deliberately drops* the referenced database to honour the "`ReferencedSchema`
  is empty for flat-scope engines" contract. In multi-database mode that carve-out
  is lifted: `ForeignKey.ReferencedSchema` carries the referenced table's
  database. A FK that references a database **outside the selected set** is
  **refused loudly** at pre-flight (sluice cannot guarantee the referent exists on
  the target) with the `--include-database` / `--exclude-table` remedy named — the
  loud-failure tenet, never a silently-broken reference.
- This makes the MySQL engine behave as `SchemaScopeNamespaced` **for a
  multi-database run**. The `Capabilities.SchemaScope` flag continues to describe
  the *default* (single-database, flat) shape; multi-database is an
  orchestrator-level composition, not a capability change. The carve-out is named,
  tested (pin the class: same-DB FK, cross-in-scope-DB FK, cross-out-of-scope-DB
  FK refusal), and commented per the "warts get a name + a test" tenet.

### 4. Target routing — same-named namespaces

- **MySQL → Postgres:** each source database → a target **PG schema of the same
  name**. The PG writer already emits `Table.Schema`-qualified DDL and
  `CREATE SCHEMA IF NOT EXISTS`; the orchestrator drives it per `Table.Schema`
  instead of a single `--target-schema` override.
- **MySQL → MySQL:** each source database → a target **database of the same
  name**. The MySQL writer gains per-database routing — `CREATE DATABASE IF NOT
  EXISTS` + database-qualified DDL/DML (or a per-target-database writer handle).
  This is the main new writer surface.
- The **target DSN names the server** (plus, for PG, the containing database);
  source database names supply the per-namespace routing under it. `--map-database`
  overrides the name. Same-name routing means **no table-name collisions** across
  databases (each lands in its own namespace) — the collision hazard ADR-0031
  flagged for the fan-in case does not apply to fan-out.

### 5. CDC — one server-wide binlog stream, per-event apply-routing (vanilla MySQL)

- The **snapshot → CDC handoff** captures one server-wide binlog position under a
  single consistent snapshot spanning all selected databases (see Consistency
  below), exactly as the single-database path does — just with the snapshot
  transaction covering N databases.
- The **binlog CDC reader stays a single stream** (the binlog is server-wide). It
  filters to the selected database set and tags each change with its source
  database (already present in the binlog event's schema field).
- The **applier routes each change to the right target namespace** by its source
  database → target namespace (PG schema / MySQL database) mapping. This is the
  new apply-path surface: today the applier targets one namespace; it gains a
  per-change namespace dispatch keyed on the change's source database.
- **Position state** stays a single `sluice_cdc_state` row per `--stream-id` (one
  server-wide binlog coordinate). No multi-row position bookkeeping for the binlog
  path.

### 6. VStream (PlanetScale/Vitess) multi-keyspace — deferred sub-phase

Because VStream is keyspace-scoped, multi-keyspace sync needs N VStream
connections + N position tokens + a multi-row position model. This is a distinct
design, gated on real PlanetScale-multi-keyspace demand, and is **not** in the
first implementation. Single-keyspace PlanetScale (the common shape) is unaffected.

## Consistency model

A multi-database snapshot must be a single consistent cut so the binlog handoff is
correct. sluice uses **one `START TRANSACTION WITH CONSISTENT SNAPSHOT`** (the
existing REPEATABLE-READ snapshot mechanism) spanning the reads of **all** selected
databases, and captures the binlog position at that boundary. N databases share one
snapshot and one handoff position. This is documented as the consistency guarantee;
per-database independent snapshots are explicitly *not* the model (they would race
the binlog handoff).

## Phasing

- **1a — Multi-database `migrate` (snapshot).** `--include-database` /
  `--exclude-database` / `--all-databases`, `ir.DatabaseLister`, per-database
  re-open loop, `Table.Schema` population, the FK flat-scope carve-out + refusal,
  target routing (PG schemas / MySQL databases), the single spanning snapshot.
- **1b — Multi-database binlog CDC (`sync start`).** Server-wide binlog reader
  scoped to the selected set + per-change namespace apply-routing + the single
  spanning snapshot→CDC handoff. (Operator chose CDC in Phase 1; binlog's
  server-wide nature makes this tractable alongside 1a.)
- **1c — VStream multi-keyspace CDC.** Deferred; N-stream design, demand-gated.

## Consequences / risks

- **The flat-scope FK carve-out is the sharpest IR subtlety** — it must be
  class-pinned (same-DB / cross-in-scope-DB / cross-out-of-scope-DB-refusal) and
  cannot silently drop a reference.
- **New MySQL writer surface** (per-database `CREATE DATABASE` + qualified DDL/DML)
  and **new applier surface** (per-change namespace routing). Both are additive
  and gated behind multi-database mode; single-database emits byte-identical SQL.
- **Snapshot atomicity across N databases** rides the existing consistent-snapshot
  transaction; very large multi-database snapshots hold that transaction longer
  (documented; the bulk-copy memory bounds and resume machinery already apply).
- **Cross-engine symmetry note:** this ADR is MySQL-source fan-out. The reverse
  (a multi-schema PG source → multi-database MySQL target) is a natural follow-on
  that reuses the same target-routing + applier-routing surfaces; the PG reader
  would populate `Table.Schema` per schema (it already can) and the orchestrator
  would iterate schemas. Tracked as a follow-on, not built here.

## Alternatives considered

- **Status quo — separate sluice processes per database (ADR-0031).** Works, but
  is operationally heavy, gives no single consistent run, and no single CLI
  invocation. The operator explicitly wants the multi-database run.
- **N independent CDC streams for the binlog path.** Rejected — the binlog is
  server-wide, so one stream + apply-routing is both simpler and the natural model.
  N-stream is retained only for the VStream/keyspace case (1c).
- **A multi-schema `ReadSchema` returning `[]*ir.Schema`.** Rejected for the
  snapshot — re-opening a single-database reader per database reuses all existing
  logic with no interface churn.

## Open questions for review

1. **`--map-database` in the first cut, or a 1.x follow-on?** (Default is
   same-name; renaming is the only thing it adds.)
2. **Out-of-scope cross-database FK:** refuse loudly (this ADR's choice) vs. drop
   to a flat reference with a WARN. The loud-refuse is the tenet-aligned default.
3. **MySQL → MySQL target:** auto-`CREATE DATABASE IF NOT EXISTS` per source
   database (this ADR's choice) vs. require the operator to pre-create them. Auto
   is more convenient but is sluice owning a bit more DDL surface.
