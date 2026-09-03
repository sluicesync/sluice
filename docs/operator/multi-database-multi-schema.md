# Migrating many databases / schemas in one run

By default `sluice migrate` and `sluice sync start` move the **one**
database (MySQL) or schema (Postgres) named in the source DSN. The
multi-namespace flags let you move **all** of a server's databases, or
all of a Postgres source's schemas, in a single run — snapshot and CDC
both — fanning each source namespace out to a **same-named** target
namespace.

The unifying idea ([ADR-0031](../adr/adr-0031-multi-source-aggregation-target-schema.md))
is that *a MySQL database is the rough equivalent of a Postgres schema*.
So there is one internal filter with two spellings: use the
`--*-database` form on a MySQL source ([ADR-0074](../adr/adr-0074-multi-database-mysql-migration-and-sync.md))
and the `--*-schema` form on a Postgres source
([ADR-0075](../adr/adr-0075-postgres-source-multi-schema-migration-and-sync.md)).
They populate the same routing; mixing the two spellings in one
invocation is a loud error.

The flags (identical on `migrate` and `sync start`):

| Flag | Meaning |
|---|---|
| `--all-databases` / `--all-schemas` | every non-system namespace on the source |
| `--include-database` / `--include-schema` | only these (comma-separated, repeatable; glob patterns allowed, e.g. `app_*`) |
| `--exclude-database` / `--exclude-schema` | every non-system namespace except these |

Within a form, include / exclude / all are mutually exclusive. System
namespaces are always excluded: `information_schema`, `performance_schema`,
`mysql`, `sys` on MySQL; `pg_catalog`, `information_schema`, `pg_toast`,
`pg_temp*` on Postgres. When any namespace-scope flag is set, the source
DSN's database/schema is **optional** — sluice connects to the server (or
to the database, on PG) rather than a single namespace.

---

## Scenario 1 — Postgres source: every schema in one run

A Postgres database holding several application schemas (`sales`,
`billing`, `inventory`) → one Postgres target, each schema recreated
(auto-created if absent) under its own name:

```bash
sluice migrate \
    --source-driver postgres --source 'postgres://user:pw@src/appdb?sslmode=disable' \
    --target-driver postgres --target 'postgres://user:pw@dst/appdb?sslmode=disable' \
    --all-schemas
```

Lands: `sales.*`, `billing.*`, `inventory.*` on the target — same schema
names, same table names. Continuous sync is identical, just `sync start`:

```bash
sluice sync start \
    --source-driver postgres --source 'postgres://user:pw@src/appdb?sslmode=disable' \
    --target-driver postgres --target 'postgres://user:pw@dst/appdb?sslmode=disable' \
    --all-schemas --stream-id appdb-allschemas
```

Scope to a subset (glob allowed), or take everything except a couple:

```bash
# only the app_* schemas
sluice migrate ... --include-schema 'app_*,public'

# everything except the staging schemas
sluice migrate ... --exclude-schema 'scratch,tmp_load'
```

---

## Scenario 2 — MySQL server: every database → Postgres in one run

A MySQL server hosting one database per tenant/service → a single
Postgres target, **each MySQL database recreated as a same-named PG
schema** (auto-created):

```bash
sluice migrate \
    --source-driver mysql    --source 'root:pw@tcp(src:3306)/' \
    --target-driver postgres --target 'postgres://user:pw@dst/warehouse?sslmode=disable' \
    --all-databases
```

MySQL `shop` / `crm` / `analytics` → PG schemas `shop` / `crm` /
`analytics` under `warehouse`. Note the source DSN has no database after
the `/` — with `--all-databases` it is a server connection.

Scope with `--include-database` / `--exclude-database` (glob allowed):

```bash
sluice migrate ... --include-database 'tenant_*'
```

When the **target is also MySQL** (the `mysql` or `mariadb` flavor), each
source database is recreated as a target database via `CREATE DATABASE IF
NOT EXISTS` — same names, no manual pre-creation. On `vitess`/`planetscale`
targets the fan-out probes first and REFUSES only what is missing
(`SLUICE-E-SCHEMA-KEYSPACE-MISSING`): vtgate has no `CREATE DATABASE` —
keyspaces are provisioned through the platform — so pre-create them (the
fan-out proceeds once they exist), or use per-database explicit targets.

---

## Scenario 3 — Fan-IN: many sources → one target namespace

The reverse shape (Shape B in ADR-0031): several independent source
databases — e.g. per-microservice MySQL databases — consolidated into
**one** Postgres analytics schema. This is not a `--all-*` fan-*out*; it
is N separate runs, each pinned to the same target namespace with
`--target-schema`:

```bash
# service A → warehouse.analytics
sluice migrate \
    --source-driver mysql    --source 'root:pw@tcp(svc-a:3306)/orders' \
    --target-driver postgres --target 'postgres://user:pw@dst/warehouse?sslmode=disable' \
    --target-schema analytics

# service B → the SAME warehouse.analytics (run separately)
sluice migrate \
    --source-driver mysql    --source 'root:pw@tcp(svc-b:3306)/users' \
    --target-driver postgres --target 'postgres://user:pw@dst/warehouse?sslmode=disable' \
    --target-schema analytics
```

`--target-schema` is Postgres-target-only; it prefixes every emitted
`CREATE TABLE` / index / type and auto-creates the schema. The control
table `sluice_cdc_state` stays in the DSN's default schema. To avoid
table-name collisions across services landing in one schema, pair this
with `--inject-shard-column NAME=VALUE`
([ADR-0048](../adr/adr-0048-multi-source-aggregation-shape-a.md)), which adds a
per-source discriminator and a composite PK. (A MySQL target consolidates
via distinct target DSN databases instead — schemas and databases collapse
on MySQL.)

---

## The documented edges

- **`--slot-name` applies to a multi-schema Postgres run, on both its cold start and its warm resume (since v0.139.0).** A logical slot is database-wide, so a spanning multi-schema sync uses exactly one — and before v0.139.0 the fan-out ignored the flag and always used the default name, which meant two multi-schema streams against one database could not coexist and the slot recorded in the CDC state row was not the one you named. Name a slot per stream when you run more than one.
- **A multi-schema `sync start` refuses a table with no usable replica identity, before it publishes anything (since v0.139.0).** Postgres will not `UPDATE` or `DELETE` a published table that lacks one, so publishing first and checking later would break your own application's writes. The refusal carries `SLUICE-E-SOURCE-REPLICA-IDENTITY` and names the offending tables; the remedy is a primary key, a `REPLICA IDENTITY FULL`, or `--exclude-table`.
- **Cross-database / cross-schema foreign keys are refused loudly.** A
  fan-out collects FKs and validates referents are inside the selected
  set; an out-of-scope FK fails loudly at the deferred FK pass (after the
  copy), never silently dropped. This is the loud-failure tenet — see
  ADR-0074 §"Resolved decisions".
- **Postgres separate *databases* (not schemas) are one run per DSN.** A
  Postgres connection is scoped to a single database, so `--all-schemas`
  covers every schema *within* the connected database; moving N separate
  PG databases is N runs (one `--source` DSN each). This is inherent to
  how PG connections work, not a sluice limitation.
- **PlanetScale-MySQL is a single keyspace.** Its CDC (VStream) is
  keyspace-scoped, and multi-keyspace streaming is a deferred sub-phase
  (ADR-0074 §6) — single-keyspace PlanetScale (the common shape) is
  unaffected. Because a PS-MySQL branch exposes one database/keyspace, it
  is **not a multi-namespace target**: fanning several source databases
  into one PS-MySQL branch would collapse them into one namespace and
  collide. **PlanetScale-Postgres** behaves like regular Postgres and
  takes `--all-schemas` fine.
- **One vocabulary per run.** Supplying both a `--*-schema` and a
  `--*-database` form in one invocation is an error (they are synonyms);
  pick the one that matches your source engine.

---

## Renaming on the way across

Routing is **same-name** today: each source namespace lands in a
target namespace of the same name. A `--map-database SRC=DST` flag to
rename on the target is a planned 1.x follow-on (ADR-0074 §"Resolved
decisions" / §"Future"); until it lands, use a same-named target (or, for
the fan-IN consolidation shape, `--target-schema`).

## See also

- [ADR-0074](../adr/adr-0074-multi-database-mysql-migration-and-sync.md) — multi-database MySQL migrate + sync.
- [ADR-0075](../adr/adr-0075-postgres-source-multi-schema-migration-and-sync.md) — Postgres-source multi-schema migrate + sync.
- [ADR-0031](../adr/adr-0031-multi-source-aggregation-target-schema.md) — `--target-schema` namespacing and the database≈schema model.
- [ADR-0048](../adr/adr-0048-multi-source-aggregation-shape-a.md) — `--inject-shard-column` for collision-free fan-IN.
