# sluice v0.160.4

**HIGH fix: a copied view could read the wrong tables on the target at exit 0, because its body kept the source's schema or database name (Bug 314).** MySQL writes the database name into every table reference in a stored view. Postgres writes the schema name whenever it is not on the reading session's search path. Copied verbatim, a view landed in the target schema or database but kept pointing at a schema or database named after the source. Where the target server also held something by that name, the view read it silently, returning another database's rows. Otherwise the view failed loudly.

## Fixed

- **A MySQL view migrated into a differently named database now reads the target's own tables (HIGH, silent; Bug 314; long-standing).** `information_schema.VIEWS` stores ``select ... from `app`.`t` ``, and sluice copied it verbatim. If the target server also held a database named after the source (including the source itself, when both live on one server), the target view returned the source's rows. The reader now removes the source database's own qualifier, leaving string literals and references to any other database untouched (`stripOwnSchemaQualifier`). This also covers multi-database mode, where `--map-database` renames the database. The renamed view used to read the original database.
- **A MySQL view that uses its own database's name as a table alias, derived-table name or CTE name is now refused by name, before any view is created (new refusal, `VIEW-QUALIFIER-AMBIGUOUS`).** In such a body, `` `app`.`id` `` may be the alias's column, and removing the qualifier could silently make a correlated subquery read another column. The run refuses before any data is copied. Rename the alias in the source view, or leave the view out with `--exclude-view` (the run then proceeds) and create it on the target yourself.
- **A Postgres view now resolves in the schema its tables were copied into (HIGH, silent; Bug 314).** Two causes, both fixed:
  - **The reader.** A view in a non-`public` source schema read back as `FROM app.t`, so `--target-schema`, a target `?schema=`, `--map-schema` and `sync start` all created a view that named `app` on the target. A Postgres → MySQL migration named a MySQL database `app` the same way; such a view is now refused (`VIEW-BODY-CROSS-ENGINE`, below). The reader now reads view bodies with the source schema as the search path, so the server writes the source's own names unqualified and every other schema's qualified (`catalogQueryUnderSearchPath`).
  - **The writer.** It bound an unqualified body under its session default. A `public` source migrated with `--target-schema app3` produced `app3.pv` reading the target's own `public.p`. Each `CREATE VIEW` and `CREATE MATERIALIZED VIEW` now runs with the target schema on the search path (`execViewDDL`). `public` follows it only for a view read from `public`, whose extension calls are unqualified. For any other source schema, a name the run did not create, such as a table left out by `--exclude-table`, now fails loudly. It no longer binds to an unrelated table in the target's `public`.
  - Pinned against decoys on real servers by the rows each view returns: `TestMigrate_MySQLToMySQL_ViewReadsTheTargetDatabase`, `TestMigrate_PostgresView_ReadsTheTargetSchema`.
- **A view crossing engine families that could never be created before stays refused, now by name and before any data is copied (`VIEW-BODY-CROSS-ENGINE`).** This covers a MySQL view going to Postgres or SQLite, and a Postgres view from a non-`public` schema going to MySQL or SQLite. Their source schema or database name used to make the CREATE fail. Without it, the text would parse on the other engine and compute differently: MySQL's `a / b` is `3.5000` where Postgres and SQLite give `3`. Leave such a view out with `--exclude-view` and create an equivalent on the target. A Postgres view from `public` is copied verbatim as before (`RefuseUnportableView`).
- The `VIEW-QUALIFIER-AMBIGUOUS` refusal is also raised before any data is copied, not at the end of the run.

## Compatibility

**Drop-in for anything that migrated correctly before, with three loud exceptions. Views that already failed now fail earlier and by name.** No flag, default or error-code change, and no backup format change (`BackupFormatVersion` stays 14; existing backups' schema fingerprints are unchanged). A same-engine view that previously failed with "relation does not exist" or `Error 1049` after the rows were copied now creates and reads the target's tables. The three exceptions:
- A MySQL view using its database's name as an alias is refused (`VIEW-QUALIFIER-AMBIGUOUS`, above).
- **Postgres multi-schema → MySQL** (`--include-schema`, `--all-schemas` or `--map-schema` fanning each schema out to its own MySQL database), and restoring such a backup into MySQL. A view outside `public` used to create there, because its `app.t` named the MySQL database sluice had just created. It is now refused with `VIEW-BODY-CROSS-ENGINE`, like every other cross-engine body that carried a qualifier. Its text was always in Postgres's dialect and could compute differently on MySQL (integer division, for one). Leave it out with `--exclude-view` and create an equivalent on the target.
- A Postgres view calling an extension installed outside `public`, such as a Supabase-style `extensions` schema, may now fail at the views phase, where before it was created. Its body now names that schema, or the target's default search path no longer applies. Leave it out with `--exclude-view` and create it on the target yourself.

**Still open, filed in the backlog:**
- **A view over a different schema that the same run also renames.** For example, `--include-schema app,public --map-schema app=app2` with a `public` view over `app.t`. The view keeps naming `app`.
- **Restoring a backup taken before this release** replays its recorded, qualified view bodies. Take a fresh full backup on v0.160.4 if you restore into a differently named database or schema.

**Not run for this release:** the dispatch-only `psverify` (live PlanetScale), `d1verify` (live Cloudflare D1) and `nekiverify` (live Neki) suites, which need live, billable infrastructure.

## Who needs this — action required

- **Anyone who migrated or synced views MySQL → MySQL into a database with a different name (including with `--map-database`), or Postgres → Postgres into a different schema (`--target-schema`, a target `?schema=`, `--map-schema`), or Postgres → MySQL from a non-`public` schema.** On the target, check what each view reads: in MySQL, `SELECT view_definition FROM information_schema.views WHERE table_schema = DATABASE()`; in Postgres, `pg_get_viewdef`. Recreate any view that names the source's schema or database, or re-run the migration on v0.160.4.
- **Everyone else:** upgrade.

---

**Install:** `brew install sluicesync/tap/sluice` · `go install sluicesync.dev/sluice/cmd/sluice@v0.160.4` · **Container:** `ghcr.io/sluicesync/sluice:0.160.4`

**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
