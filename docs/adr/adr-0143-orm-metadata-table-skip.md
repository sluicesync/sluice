# ADR-0143: ORM/framework migration-bookkeeping table recognition + loud-skip-by-default

- Status: Accepted
- Date: 2026-06-29
- Deciders: sluice maintainers
- Relates: ADR-0022 (engine-default table exclusions / PlanetScale `_vt_*`), ADR-0128 (SQLite/D1 migrate source), the orchestrator `TableFilter` boundary

## Context

ORMs and migration frameworks keep a small bookkeeping table that records which schema migrations have been applied to a database: Flyway's `flyway_schema_history`, Prisma's `_prisma_migrations`, Rails' `schema_migrations`, Django's `django_migrations`, and ~two dozen more. When sluice copies a source that contains one of these tables, carrying it to the target is almost always wrong: the rows record migrations that ran against the **source** engine, so the ORM/framework on the target concludes those migrations already ran when in fact sluice built the target schema directly. The operator then can't cleanly re-baseline their ORM on the target — the history says everything is already applied.

This surfaced from the SQLite/D1 migrate-source work (ADR-0128): a D1 export routinely contains a framework's history table next to the application tables. But the problem is engine-universal — these tables exist in every source engine — so the fix belongs in the engine-neutral orchestrator, not in any reader.

## Decision

Recognize ORM/framework migration-bookkeeping tables in the pipeline package and **loud-skip them by default on the CLI for CROSS-engine migrations only**: skip recognized tables (announcing each loudly) when source and target are different engine families, because a migration-history table records migrations written for the source engine that never ran against the differently-built target — the carried history is invalid there. For a **same-engine** migration (same family — Postgres→Postgres, MySQL→PlanetScale-MySQL, …) the history is valid (sluice's replicated schema is exactly what those migrations would build on that engine), so the tables are **kept by default**. `--include-orm-tables` forces keep on any run; `--skip-orm-tables` forces skip (e.g. on a same-engine run); the two are mutually exclusive. Engine family is computed from the driver name (`mysql`/`planetscale`/`vitess`; `postgres`/`postgres-trigger`; `sqlite`/`d1`/`sqlite-trigger`/`d1-trigger`). Recognition is engine-neutral; only the skip-*default* is cross-engine-scoped.

### Recognition (two classes)

A rule table (`internal/pipeline/orm_tables.go`) maps each framework to a name predicate and, where needed, a column-shape guard. Matching is case-insensitive and engine-neutral — the shape guards test the dialect-neutral IR type families (`ir.Text`/`ir.Varchar`/`ir.Char`, `ir.Integer`), never an engine-specific SQL type string.

**Distinctive names (name-only, low false-positive — always recognized):** Drizzle (`__drizzle*` prefix), Prisma (`_prisma_migrations`), Knex (`knex_migrations`, `knex_migrations_lock`), Sequelize (`sequelizemeta`), Rails ActiveRecord (`ar_internal_metadata`), Flyway (`flyway_schema_history`), Liquibase (`databasechangelog`, `databasechangeloglock`), Django (`django_migrations`), Alembic (`alembic_version`), TypeORM (`typeorm_metadata`), Goose (`goose_db_version`), EF Core (`__efmigrationshistory`), Doctrine/Symfony (`doctrine_migration_versions`), Phinx/CakePHP (`phinxlog`), sqlx (`_sqlx_migrations`), Diesel (`__diesel_schema_migrations`), SeaORM (`seaql_migrations`), MikroORM (`mikro_orm_migrations`), node-pg-migrate (`pgmigrations`), Atlas (`atlas_schema_revisions`), Aerich (`aerich`), Fluent (`_fluent_migrations`).

**Generic names (name + COLUMN-SHAPE guard — these collide with plausible application tables):** A generic name is recognized ONLY when the name AND the shape match. If the name matches but the shape does not, the table is NOT skipped — it is application data — and the caller emits a name-collision warning. The three generic rules and their load-bearing shape guards:

- `schema_migrations` (Rails / Ecto / golang-migrate / dbmate): exactly ONE column named `version` of a text family. (Ported from pscale's `looksLikeRailsSchemaMigrations`.)
- `migrations` (Laravel / gormigrate): the column set is exactly `{id integer, migration text, batch integer}` — `migration`+`batch` required with their families, `id` is the optional surrogate, any other column disqualifies. Kept tight: Laravel's table is exactly those three.
- `migration` (Yii): the column set is exactly `{version text, apply_time integer}`.

The recognition surface is `recognizeORMTable(*ir.Table) (rule, bool)` plus `ormNameCollision(*ir.Table) (orm string, collided bool)` (name matched a generic rule but shape didn't).

### Skip mechanism + where it runs

A single engine-neutral helper, `applyORMTableSkip(ctx, schema, skip, filter)`, runs **after** the existing `applyTableFilter`/`applyViewFilter` (so explicit `--include-table`/`--exclude-table` are resolved first). For each surviving table: if it is recognized AND was not named explicitly in `--include-table` (an explicit include always wins — exact, case-insensitive literal; a glob that merely matches does not count as "named"), it is removed from `schema.Tables` and a loud per-table notice is emitted:

```
WARN migration: skipping ORM migration-history table — pass --include-orm-tables to keep it
     table=<name> orm=<framework> reason=<remediation>
```

A generic-name collision is NOT skipped; it emits a one-time warning naming the framework, so a real application table is never silently dropped.

The prune is wired at every post-read filter point so both one-shot and continuous paths skip consistently:

- `migrate_phases.go` (`phaseReadSourceSchema`) — the single-database migrate path, which the multi-database fan-out reuses per database.
- `migrate_multidb.go` — the deferred cross-database constraints pass (so `CreateConstraints` doesn't target tables the per-database run already skipped).
- `streamer_coldstart.go` — the sync cold-start, before the snapshot/publication scope is computed (so a continuous sync neither cold-copies nor — on publication-scoped sources — streams them).
- `streamer_dryrun.go` — the sync dry-run preview, so it honestly reflects a real run.
- `streamer_multidb.go` — the per-database sync fan-out.

### Field + flag (zero-value-safe)

`SkipORMTables bool` is added to `Migrator` and `Streamer`. **The zero value (false) = DO NOT skip** — the conservative default every programmatic / broker / test / fleet construction gets; they must never suddenly start dropping tables (the v0.99.51 zero-value trap, applied deliberately). Only the **CLI** `migrate` and `sync start` commands default skip on, flipped off by a new `--include-orm-tables` bool (`SkipORMTables: !IncludeORMTables`). There is no `--skip-orm-tables` flag — skip is already the CLI default; the flag is the opt-out.

## Correctness / safety

- **Identity default.** `SkipORMTables=false` ⇒ behaviour byte-identical to before this feature (the helper is a no-op).
- **No silent loss of application data.** The generic-name shape guards are load-bearing: a false skip of a real application table (even a loud one) is a bad surprise. A name match without a shape match is kept, with a loud collision warning — never dropped.
- **Explicit include wins.** A table named exactly via `--include-table` is never skipped — the operator named it.
- **Engine-neutral.** Lives entirely in the pipeline package; no engine imports. Works for any source.

## Consequences / scope notes

- **Continuous-sync CDC dispatch (vanilla MySQL binlog).** The cold-start prune removes the ORM tables from the schema, so publication-scoped CDC sources (Postgres `FOR TABLE` publications, PlanetScale VStream's table-scoped filter) never stream them. A vanilla-MySQL binlog stream is NOT table-scoped at the source — the dispatch filter is `s.Filter`, which doesn't carry the ORM exclusion — so a write to a skipped ORM table during steady-state would reach the applier for a table that isn't on the target and fail LOUDLY (a missing-table apply error), not silently. ORM bookkeeping tables are written only during migrations, so this is rare in practice; threading the concrete skipped-table set into the dispatch drop (precise — only the shape-validated tables actually skipped, never a generic-name app table) is the tracked follow-up if it bites. PG and PlanetScale sync are fully covered today.
- **`sync run` fleet.** The config-driven fleet (`internal/pipeline` Streamers built in `cmd/sluice/sync_run.go`) gets the zero-value `SkipORMTables=false` (no skip), consistent with "programmatic constructions don't skip." Exposing it as a per-sync config key is a future increment if demanded.

## Alternatives considered

- **Hard-skip (always, no flag).** Rejected — an operator who genuinely wants the history table (e.g. a like-for-like clone) must be able to keep it; loud-skip-with-opt-out preserves trust.
- **Name-only matching for the generic names.** Rejected — `migrations` / `migration` / `schema_migrations` collide with real application tables; a name-only skip would silently drop application data (the exact failure the shape guards exist to prevent).
- **Push recognition into the readers.** Rejected — it's a product-level decision identical across engines; it belongs at the orchestrator's `TableFilter` boundary alongside the existing include/exclude and engine-default exclusions (ADR-0022).
