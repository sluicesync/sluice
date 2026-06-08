# ADR-0075: Postgres-source multi-schema migration and continuous sync

## Status

**Accepted (2026-06-07); shipped v0.99.24 (Phase 2a migrate + Phase 2b continuous sync/CDC).** The symmetric reverse of
[ADR-0074](adr-0074-multi-database-mysql-migration-and-sync.md): connect to a
Postgres source that carries **many schemas** and migrate **and sync** all — or
a selected subset — of them to a target in one run, each schema landing in its
own same-named target namespace (a PG schema, or an auto-created MySQL
database). This realises the "connect to a Postgres database and copy all
schemas over" case the operator described when requesting ADR-0074, and
**supersedes ADR-0074's deferred "cross-engine symmetry note"** (§Consequences)
and its Phase-1c reverse-direction follow-on.

Operator decisions taken at scoping (2026-06-07), baked in below as **decided**,
not open:

1. **Phase 2 includes CDC** — migrate (snapshot) *and* continuous multi-schema
   sync ship together, not migrate-first.
2. **Schema-named flags** — `--include-schema` / `--exclude-schema` /
   `--all-schemas`, mapping to the same internal `DatabaseFilter` as ADR-0074's
   `--*-database` flags (the "MySQL database ≈ PG schema" equivalence holds
   internally; the schema-named surface is the clearer UX for PG operators).

## Context

ADR-0074 fanned a **MySQL server** (one connection, many *databases*) out to N
target namespaces. The reverse — a **Postgres database** (one connection, many
*schemas*) fanned out to N target namespaces — is the same shape with the
namespacing primitive swapped: **a PG schema corresponds 1:1 to a MySQL
database** (ADR-0031). The whole orchestration layer ADR-0074 built is already
**engine-neutral** — `internal/pipeline/migrate_multidb.go` and
`streamer_multidb.go` drive everything through the `ir.DatabaseLister` /
`ir.DatabaseDSNDeriver` / `ir.MultiDatabaseScoper` / `ir.CDCDatabaseScoper` /
`ir.MultiDatabaseRouter` / `ir.MultiDatabaseSnapshotOpener` /
`ir.ServerCDCReaderOpener` interfaces, never engine-specific imports. Today
**MySQL implements all of them; Postgres implements only the write-side
`MultiDatabaseRouter`** (`internal/engines/postgres/change_applier.go`). So this
ADR is overwhelmingly *additive on the Postgres engine*, reusing the
orchestrator wholesale.

Two facts make it tractable:

1. **The IR is already multi-namespace and PG already populates it.** Unlike
   MySQL (which leaves `Table.Schema` empty under `SchemaScopeFlat`), the PG
   schema reader **already stamps `Table.Schema` / `View.Schema`** with the
   bound schema, and the PG CDC reader **already decodes the schema** of every
   change (`rel.Schema` from the pgoutput `RelationMessage.Namespace`). There is
   no flat-scope carve-out to lift — PG is `SchemaScopeNamespaced` already.
2. **The reverse-direction follow-on was anticipated by ADR-0074** (the
   "Cross-engine symmetry note" and the rejected `[]*ir.Schema` `ReadSchema`):
   reuse the target-routing + applier-routing surfaces, iterate schemas, and
   re-open a single-schema reader per schema rather than inventing a multi-schema
   `ReadSchema`.

## The load-bearing insight: a Postgres logical slot is already database-wide

ADR-0074's hard part dissolved because **the MySQL binlog is server-wide** (one
stream, route per event). The exact symmetric fact holds for Postgres CDC:

**A Postgres logical replication slot is *database-wide*.** A single slot +
publication decodes the WAL for **every schema in the connected database**
through **one** stream, and each change already arrives tagged with its schema
(the pgoutput `RelationMessage` carries `Namespace`). sluice's PG CDC reader
already builds this — it decodes `rel.Schema` for every relation
(`cdc_reader.go` `buildRelationCacheEntry`) and stamps `ir.Change.Schema =
rel.Schema` — but today it **drops** any change whose schema ≠ the single bound
schema:

```go
// internal/engines/postgres/cdc_reader.go (emitInsert / emitUpdate / emitDelete)
if rel.Schema != r.schema {
    return nil // out-of-scope schema; drop
}
```

So multi-schema PG CDC is **one slot, one LSN position, per-event apply-routing**
— *not* N slots to coordinate. It is the direct analog of ADR-0074's server-wide
binlog, and it is *more* natural than the MySQL case (a PG slot's creation gives
an exported, consistent snapshot spanning the whole database — no `FLUSH TABLES
WITH READ LOCK` dance is needed; see Consistency below). The three drop-sites
become a `CDCDatabaseScoper` predicate check, and the existing PG
`MultiDatabaseRouter` routes each change to its target namespace.

**VStream (PlanetScale/Vitess) stays the only N-stream exception** (ADR-0074 §6 /
Phase 1c), unchanged and still deferred.

## Decision

Add a **multi-schema mode** to the Postgres engine — schema-scope flags mirroring
ADR-0074's database-scope flags — by implementing the engine-neutral interfaces
the orchestrator already calls. No orchestrator rewrite; PG joins the same
machinery MySQL already drives.

### 1. CLI surface

- `--include-schema GLOB` / `--exclude-schema GLOB` — repeatable, glob-aware,
  mutually exclusive (mirror `--include-database` / `--exclude-database`).
- `--all-schemas` — convenience for "every non-system schema".
- The flags populate the **same `DatabaseFilter`** struct ADR-0074 uses;
  `--*-schema` and `--*-database` are internal synonyms. Supplying both forms in
  one invocation is a loud error (pick one vocabulary). On a **Postgres source**
  the `--*-schema` spelling is canonical; on a **MySQL source** `--*-database` is.
- When any schema-scope flag is set, the source DSN's schema / `search_path`
  becomes a **default-only** hint (the connection is to a *database*, and sluice
  fans out its schemas). With none set, behaviour is exactly as today (single
  schema from the DSN / `--target-schema`) — fully back-compat.
- System schemas — `pg_catalog`, `information_schema`, `pg_toast`, and the
  `pg_temp*` / `pg_toast_temp*` session-temp namespaces — are **always**
  excluded, even from `--all-schemas`.
- `--map-schema SRC=DST` (rename on the target) is a **follow-on**, mirroring
  ADR-0074's `--map-database` deferral; the first cut routes **same-name**.

### 2. Source reader scoping — enumerate, then re-open per schema (snapshot)

- Postgres implements **`ir.DatabaseLister`**: `ListDatabases(ctx, dsn)` returns
  the user schemas (`pg_namespace` / `information_schema.schemata` minus the
  system set above). "Database" in the interface name is the engine-neutral term
  for "namespace to fan out"; for PG it returns schemas.
- Postgres implements **`ir.DatabaseDSNDeriver`**: `WithDatabase(dsn, schema)`
  returns a DSN bound to that schema (sets the reader's target schema /
  `search_path`); `EnsureDatabase(ctx, dsn, schema)` is `CREATE SCHEMA IF NOT
  EXISTS` (used only on a **PG target**).
- For the **snapshot** the orchestrator **re-opens a single-schema reader per
  selected schema** (exactly as ADR-0074 re-opens a single-database reader per
  database), reusing the existing single-schema reader/writer logic. No
  multi-schema `ReadSchema` variant (consistent with ADR-0074's rejection).

### 3. IR namespace population — no carve-out needed

- The PG reader **already** populates `Table.Schema` / `View.Schema`; in
  multi-schema mode it is driven per selected schema via
  **`ir.MultiDatabaseScoper`** (`SetMultiDatabaseScope(schema, inScope)`), which
  stamps the schema name and supplies the `inScope` predicate for the FK
  carve-out. Unlike MySQL there is **no flat-scope carve-out to lift** — PG is
  namespaced already, so this is a thin addition.
- **Foreign keys:** PG FKs already carry `ReferencedSchema`. A FK that references
  a schema **outside the selected set** is **refused loudly** at pre-flight (the
  loud-failure tenet), naming the `--include-schema` remedy — identical policy to
  ADR-0074's cross-database FK refusal. Class-pin: same-schema FK /
  cross-in-scope-schema FK / cross-out-of-scope-schema FK refusal.

### 4. Target routing — same-named namespaces

- **Postgres → Postgres:** each source schema → a target **PG schema of the same
  name** (`CREATE SCHEMA IF NOT EXISTS` + schema-qualified DDL — already exists;
  the orchestrator drives it per `Table.Schema` instead of one `--target-schema`).
- **Postgres → MySQL:** each source schema → a target **MySQL database of the
  same name**. This reuses ADR-0074 Phase 1b's MySQL multi-database **writer +
  applier** routing surface verbatim (`CREATE DATABASE IF NOT EXISTS` +
  database-qualified DDL/DML); the MySQL side already has it.
- The **target DSN names the server** (plus, for PG, the containing database);
  source schema names supply the per-namespace routing. Same-name routing ⇒ no
  cross-schema table-name collisions.

### 5. CDC — one database-wide slot, per-event apply-routing

- The **snapshot → CDC handoff** uses the slot's **exported consistent snapshot**
  (see Consistency): one slot, one consistent LSN spanning **all** selected
  schemas. No multi-slot coordination.
- The **PG CDC reader stays a single stream.** The three `rel.Schema != r.schema`
  drop-sites become a **`ir.CDCDatabaseScoper`** check
  (`SetCDCDatabaseScope(inScope)`): a change is emitted iff its schema is in
  scope. `ir.Change.Schema` is **already** populated with `rel.Schema` — no new
  decode work.
- The **applier routes each change to its target namespace** via the **already
  implemented** PG `ir.MultiDatabaseRouter` (`change_applier.go`) — PG→PG routes
  to the same-named schema; PG→MySQL routes through the MySQL multi-database
  applier to the same-named database.
- **Publication scope:** `CREATE PUBLICATION ... FOR ALL TABLES` (works on all
  supported PG versions; the slot is database-wide regardless, and the reader's
  `inScope` filter does the selection). PG15+ `FOR TABLES IN SCHEMA <list>` to
  trim WAL volume is a possible later optimisation, not the first cut — the
  reader-side filter is the correctness boundary either way.
- **Position state** stays a single `sluice_cdc_state` row per `--stream-id` (one
  database-wide LSN). No per-schema position bookkeeping.

### 6. VStream multi-keyspace — still deferred (ADR-0074 Phase 1c)

Unchanged: keyspace-scoped VStream multi-keyspace remains the N-stream design,
demand-gated. This ADR is the **Postgres-source** reverse direction only.

## Consistency model

Postgres makes the spanning snapshot **free and exact**: creating the logical
replication slot returns an **exported snapshot** at a single consistent LSN, and
that snapshot spans the **entire database** (all schemas) by construction. sluice
copies every selected schema from that one exported snapshot and starts CDC from
the slot's LSN — one consistent cut, one handoff position, across all schemas.
This is *cleaner* than the MySQL binlog handoff (no `FLUSH TABLES WITH READ
LOCK`; the slot's snapshot already is the boundary). Per-schema independent
snapshots are explicitly **not** the model.

## Phasing

- **2a — Multi-schema `migrate` (snapshot).** PG `ir.DatabaseLister` (list
  schemas), `ir.DatabaseDSNDeriver` (`WithDatabase` / `EnsureDatabase` =
  `CREATE SCHEMA IF NOT EXISTS`), `ir.MultiDatabaseScoper` on the PG schema
  reader; `--include-schema` / `--exclude-schema` / `--all-schemas`; reuse
  `migrate_multidb.go`; target routing (PG→PG schemas, PG→MySQL databases) + the
  cross-schema FK refusal. Per-schema re-open snapshot (acceptable for a one-time
  migrate, exactly as ADR-0074 Phase 1a).
- **2b — Multi-schema CDC (`sync start`).** `ir.CDCDatabaseScoper` (flip the
  three drop-sites), `ir.MultiDatabaseSnapshotOpener` (exported-snapshot spanning
  copy + slot LSN handoff) + `ir.ServerCDCReaderOpener` (bare database-wide CDC
  reader for warm-resume); per-change routing via the existing PG
  `MultiDatabaseRouter`; one slot, one LSN position. Cold-start, steady-state,
  and warm-resume — mirroring ADR-0074 Phase 1b.1/1b.2/1b.3.
- **VStream multi-keyspace** stays ADR-0074 Phase 1c (deferred).

## Consequences / risks

- **Concurrency / `-race`-before-tag.** 2b touches the CDC reader's per-event
  path + warm-resume FSM; it is a concurrency chunk — the `-race` integration
  gate runs before any tag (per CLAUDE.md).
- **Pin the class, not the representative.** The FK carve-out and the
  schema-routing apply path are family-dispatched (per-schema); the pins must
  cover same-schema / cross-in-scope / cross-out-of-scope-refusal, and the CDC
  routing must be pinned for ≥2 schemas with interleaved changes (route each
  change to the right namespace; never cross-contaminate).
- **Minimal new engine surface.** PG gains `DatabaseLister` +
  `DatabaseDSNDeriver` + `MultiDatabaseScoper` + `CDCDatabaseScoper` +
  `MultiDatabaseSnapshotOpener` + `ServerCDCReaderOpener`; the write-side
  `MultiDatabaseRouter` already exists. The orchestrator and all CLI plumbing are
  unchanged (the `--*-schema` aliases reuse `DatabaseFilter`). Single-schema runs
  emit byte-identical SQL and take byte-identical CDC paths (gated on
  multi-schema mode).
- **`pg_temp` / partition / inheritance edges.** Schema enumeration must exclude
  session-temp namespaces and must not mistake a partition child's schema; pin
  the exclusion set + a non-system-lookalike battery (a false exclusion silently
  drops a user schema — the inverse of the MySQL `_vt_*` lesson).
- **Publication WAL volume.** `FOR ALL TABLES` decodes the whole database even
  when few schemas are selected; documented, with `FOR TABLES IN SCHEMA` (PG15+)
  as the later trim. Correctness never depends on it (reader-side `inScope`).

## Alternatives considered

- **N replication slots, one per schema.** Rejected — a PG slot is already
  database-wide; one slot + per-event routing is both simpler and the natural
  model (the exact symmetry of ADR-0074's single binlog stream). N-slot is
  retained only conceptually for cross-*database* PG fan-out, which is out of
  scope (a PG slot cannot span databases — that genuinely would need N
  connections, and is not requested).
- **A multi-schema `ReadSchema` returning `[]*ir.Schema`.** Rejected for the
  snapshot — re-opening a single-schema reader per schema reuses all existing
  logic with no interface churn (consistent with ADR-0074).
- **Reusing `--*-database` for PG sources verbatim (no `--*-schema`).** Rejected
  on UX — a PG "database" is a connection boundary, not the thing being fanned
  out; the schema-named aliases are clearer. They share the internal filter so
  there is no duplicated logic.

## Resolved decisions (operator review, 2026-06-07)

All three were resolved with the proposed defaults:

1. **An unsafe PG→MySQL schema-name fold is refused loudly.** PG schema names are
   case-sensitive; MySQL database names fold per `lower_case_table_names`. If two
   selected schemas would fold to the same MySQL database (or a name is otherwise
   unsafe), sluice fails at pre-flight naming both schemas — it never silently
   merges two schemas into one database. Class-pin the fold detection.
2. **`--map-schema` is a follow-on, not in the first cut.** The first cut routes
   each source schema to a **same-named** target namespace; the optional rename
   map is added later if demand surfaces (mirrors ADR-0074's `--map-database`).
3. **Target namespaces are auto-created** — `CREATE SCHEMA IF NOT EXISTS` (PG
   target) / `CREATE DATABASE IF NOT EXISTS` (MySQL target) per selected schema.
   sluice owns the small DDL surface (identical to ADR-0074's auto-create
   decision).

## Implementation notes — Phase 2a (migrate; shipped 2026-06-08)

Phase 2a (multi-schema `migrate`/snapshot) landed reusing the ADR-0074
orchestrator unchanged. New surface: PG `ir.DatabaseLister` (schema enumeration
+ system-schema exclusion with the non-lookalike battery), `ir.DatabaseDSNDeriver`
(`WithDatabase` = bind the `schema` DSN param, `EnsureDatabase` = `CREATE SCHEMA
IF NOT EXISTS`), `ir.MultiDatabaseScoper` on the PG schema reader (stamp + the FK
carve-out predicate); MySQL `FoldNamespace` + the new optional `ir.NamespaceFolder`
target surface for the unsafe-fold refusal; `preflightNamespaceFoldCollisions` in
the orchestrator; and the `--include-schema` / `--exclude-schema` / `--all-schemas`
CLI synonyms (same internal `DatabaseFilter`, both-forms-error). Class-pinned:
schema enumeration + non-lookalike battery, FK same/cross-in/cross-out-refusal,
fold-collision, `--dry-run` no-writes, single-schema back-compat, PG→PG and
PG→MySQL end to end.

One divergence worth recording (mirrors ADR-0074's notes):

- **PG now implements `ir.DatabaseDSNDeriver`, which unifies the multi-namespace
  target routing.** Before Phase 2a, `migrate_multidb.go` keyed target routing on
  `targetCanDeriveDB` and only MySQL satisfied it; a **PG target** took the `else`
  branch and routed via `--target-schema`. Now PG satisfies it too, so **every
  shipped target** (MySQL→MySQL, MySQL→PG, PG→PG, PG→MySQL) takes the deriver
  branch — auto-create the same-named namespace (`CREATE DATABASE` / `CREATE
  SCHEMA`) + `WithDatabase` re-point — and the `--target-schema` `else` branch is
  now an unreachable fallback for a hypothetical namespaced target without the
  surface, kept for engine-neutrality. This changed the **pre-existing MySQL→PG
  multi-database path** (ADR-0074) from `--target-schema` routing to
  `WithDatabase`(`schema=` param)+`EnsureDatabase`(`CREATE SCHEMA`); the two are
  equivalent (the PG writer auto-creates + emits schema-qualified DDL either way)
  and the existing MySQL→PG multi-database integration test confirms no behavior
  change.

## Implementation notes — Phase 2b (CDC)

Phase 2b (multi-schema `sync start`) landed reusing the ADR-0074 orchestrator
(`streamer_multidb.go`) **entirely unchanged** — the engine-neutral cold-start /
warm-resume / route-per-change machinery MySQL already drives now drives PG once
PG implements the interfaces. New PG engine surface only:

- **`ir.CDCDatabaseScoper`** on the PG `CDCReader`. The load-bearing insight held:
  a PG logical slot is **database-wide**, so the one stream already carries every
  schema's changes (each tagged `rel.Schema`). The drop-site flip was **four**
  sites, not three — the ADR text said "three" but the truncate path is a fourth
  emit-side drop: `emitInsert` / `emitUpdate` / `emitDelete` / `emitTruncate` all
  had `rel.Schema != r.schema`, replaced by a single `schemaInScope(rel.Schema)`
  predicate (the symmetric analog of MySQL's `databaseInScope`). A nil scope
  reduces EXACTLY to `rel.Schema == r.schema` — byte-identical single-schema
  back-compat. The **separate** ADR-0049 schema-history gate in
  `maybeSnapshotSchema` (also `rel.Schema != r.schema`) was deliberately **left
  untouched** — it gates which relations get schema-history version writes, not
  which changes get applied, and is orthogonal to basic multi-schema CDC apply.
- **`ir.MultiDatabaseSnapshotOpener`** — `OpenMultiDatabaseSnapshotStream`. The PG
  slot makes this the easy case: `CREATE_REPLICATION_SLOT … EXPORT_SNAPSHOT`
  returns one exported snapshot at one consistent LSN that already spans every
  schema in the database (no MySQL-style `FLUSH TABLES WITH READ LOCK` dance — the
  slot's snapshot already IS the boundary). Implemented as the existing slot-based
  `OpenSnapshotStreamWithSlot` body refactored into a shared `openSnapshotStreamShared`
  with two spanning-mode toggles: (1) the publication is forced **FOR ALL TABLES**
  via a new `ensureAllTablesPublication` (the slot is DB-wide; the reader-side
  `inScope` filter is the selection boundary), and (2) the returned `RowReader` sets
  a new `qualifyBySchema` flag so its one pinned exported-snapshot connection reads
  `schema."table"` across N schemas (the PG reader already schema-qualifies — the
  flag only changes *which* schema qualifies; only `ReadRows`/`buildSelect` needed
  it, since the snapshot reader's `CountRows` short-circuits on `closer==nil`).
- **`ir.ServerCDCReaderOpener`** — `OpenServerCDCReader` is simply `OpenCDCReader`
  (a PG slot is already database-wide, so the warm-resume bare reader is the
  ordinary reader scoped via `SetCDCDatabaseScope` rather than its DSN-bound
  schema). One slot, one persisted LSN, no per-schema position bookkeeping.

The write-side `ir.MultiDatabaseRouter` (`change_applier.go`) was already
implemented (ADR-0074 Phase 1b, same surface — PG→PG schemas / PG→MySQL databases).
The `--include-schema` / `--exclude-schema` / `--all-schemas` flags' `sync start`
help text dropped the Phase-2b "refused loudly" caveat now that it ships.

Class-pinned: the four emit drop-sites (insert/update/delete/truncate each
emit an in-scope **non-bound** schema and drop an out-of-scope one), the unset-scope
single-schema back-compat drop, `buildSelect` qualify-by-schema, and end to end —
PG→PG and PG→MySQL cold-start + steady-state (insert/update/delete in both schemas,
no cross-schema bleed) + stop→restart warm-resume from the one persisted LSN
(zero loss/dup, exact source==target parity), single-schema back-compat, and the
scope pin (an out-of-scope schema's writes never reach the target — neither copied
nor routed).

No ADR divergence beyond the "three → four drop-sites" count correction above and
the (additive) `ensureAllTablesPublication` / `RowReader.qualifyBySchema` /
`openSnapshotStreamShared` helpers, all named + tested + commented per the
warts-get-a-name tenet. **Concurrency chunk:** the CDC reader's per-event path +
warm-resume FSM are touched, so the `-race` integration gate runs before any tag
(CLAUDE.md).
