# sluice v0.89.0 — managed-Postgres sources migrate cleanly (Bug 96)

**Headline:** A default `sluice migrate` / `sync` from a managed-Postgres source (Heroku, RDS, Cloud SQL, Supabase) no longer fails at create-views. Those platforms pre-install `pg_stat_statements`, whose views live in `public`; sluice used to try to recreate them on the target and error with `function pg_stat_statements does not exist` — *after* the user tables had already copied. v0.89.0 excludes extension-owned relations from the schema read, mirroring `pg_dump`. Drop-in from v0.88.0 — no config, schema, IR, or lineage-format changes.

## Fixed

- **Exclude extension-owned relations from the Postgres schema read (Bug 96).** The `SchemaReader` now skips relations that are **extension members** (recorded in `pg_depend` with `deptype='e'` against a `pg_extension`) from both the table set and the view set, via a new `extensionMemberRelations()` helper. Covers the `pg_stat_statements` views that managed providers pre-install in `public`, plus PostGIS `spatial_ref_sys` / `geometry_columns` and any other extension-provided objects. This mirrors `pg_dump`'s extension-member exclusion and sluice's existing Vitess `_vt_*` (Bug 22) and bookkeeping-table (Bug 93) exclusions. Extension objects belong to the extension and are recreated by `CREATE EXTENSION` on the target (the operator's `--enable-pg-extension` decision), never silently copied as user data.

## Compatibility

- **Minor version bump (v0.89.0)** — additive, drop-in from v0.88.0. No config / schema / IR / lineage-format changes.
- **One behavior change:** a PG source's extension-owned tables/views are no longer migrated as user data. This fixes the previously-failing default `migrate` from every managed-PG provider; operators who relied on sluice copying an extension's relations should recreate the extension on the target via `--enable-pg-extension`.

## Who needs this

- **Anyone migrating from a managed Postgres** — Heroku, RDS, Cloud SQL, Supabase all ship `pg_stat_statements` in `public`, so before v0.89.0 the default `migrate --source-driver=postgres` aborted at create-views (after copying the data) unless you knew to pass `--skip-views` / `--exclude-view`. Now it just works. Validated against a real Heroku Postgres source (essential-0 + standard-0).
- **Everyone else:** no action needed — sources without extension-owned relations are unaffected.
