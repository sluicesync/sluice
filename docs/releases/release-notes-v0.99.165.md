# sluice v0.99.165

**ORM/framework migration-bookkeeping tables (Flyway, Prisma, Rails, Django, Drizzle, … — 22 frameworks) are now recognized and, on a CROSS-engine migration, skipped by default with a loud notice — carrying a source engine's migration history to a differently-built target is invalid there. Same-engine migrations keep them by default (the history is valid). `--include-orm-tables` / `--skip-orm-tables` override. Generic-named tables are guarded by column shape so a real application table is never dropped. Second of the D1/SQLite migration-ergonomics improvements; recognition works for any source engine.**

## Added

**ORM migration-bookkeeping tables: loud-skip-by-default (ADR-0143).** When a source contains an ORM's migration-history table — `flyway_schema_history`, `_prisma_migrations`, `django_migrations`, `__drizzle_migrations`, … — those rows record migrations applied to the **source** engine. On a **cross-engine** migration (D1/SQLite→Postgres, Postgres→MySQL, …) those migrations never ran against the differently-built target, so the carried history is invalid/misleading. So `sluice migrate` and `sluice sync` now **recognize these tables and, on a cross-engine run, skip them by default**, announcing each skip:

```
WARN  migration: skipping ORM migration-history table — pass --include-orm-tables to keep it
        table=flyway_schema_history orm=Flyway reason="After import, baseline Flyway on Postgres; …"
```

`--include-orm-tables` keeps them all. This also resolves the most common cause of a stray non-portable default (these history tables are where `datetime('now')` defaults live) — combined with v0.99.164's default handling, a D1/SQLite import "just works."

**22 frameworks recognized:** Drizzle (`__drizzle*`), Prisma, Knex, Sequelize, Rails ActiveRecord, Flyway, Liquibase, Django, Alembic, TypeORM, Goose, EF Core, Doctrine/Symfony, Phinx/CakePHP, sqlx, Diesel, SeaORM, MikroORM, node-pg-migrate, Atlas, Aerich, Fluent.

**Generic names are shape-guarded (no false drops).** Three names collide with plausible application tables — `schema_migrations` (Rails/Ecto/golang-migrate/dbmate), `migrations` (Laravel/gormigrate), `migration` (Yii). These are recognized **only when the column shape also matches** (e.g. `schema_migrations` must be a single text `version` column; Laravel's `migrations` must be exactly `{id, migration, batch}`). A table whose name matches but whose columns don't is **kept as application data** with a loud name-collision warning — sluice never silently drops a table it isn't sure about (the loud-failure tenet). A table you explicitly `--include-table` is never ORM-skipped.

## Compatibility

Recognition is engine-neutral (the shape guards test the dialect-neutral IR type families, so any source engine is covered, not just SQLite/D1), but the skip-*default* is **cross-engine-scoped**: a same-engine migration keeps these tables (the history is valid), a cross-engine migration skips them. **Zero-value-safe:** the underlying `SkipORMTables` setting defaults to *off* for every programmatic/library/broker/fleet construction; only the `migrate` and `sync start` CLI commands enable it, and only on a cross-engine run. So nothing changes for embedded/SDK callers or same-engine migrations; cross-engine CLI runs gain the skip, and `--include-orm-tables` / `--skip-orm-tables` override on either path. After migrating, re-baseline your ORM on the target (the skip notice points this out per framework). The `-race` integration gate passed before tagging.

## Who needs this

Anyone importing a database built with an ORM/migration tool — especially Cloudflare D1 / SQLite apps (Drizzle, Prisma, Knex are common there) — into Postgres or MySQL. The migration-history tables that used to clutter (or break) the target are now skipped by default, with a clear path to keep them if you want.

---

**Install:** brew install sluicesync/tap/sluice · go install sluicesync.dev/sluice/cmd/sluice@v0.99.165 · **Container:** ghcr.io/sluicesync/sluice:0.99.165
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
