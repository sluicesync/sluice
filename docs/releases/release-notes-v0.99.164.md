# sluice v0.99.164

**Fixes a real D1→Postgres pain point: a SQLite-specific column DEFAULT (e.g. `DEFAULT (datetime('now'))`, common in Flyway/Goose history tables) no longer aborts the entire migration. Portable SQLite defaults now translate to Postgres; non-portable ones are dropped with a loud warning so the run completes. First of a few D1/SQLite migration-robustness improvements.**

## Fixed

**SQLite/D1 column DEFAULTs no longer fail the whole migration (real-user-reported).** Migrating a SQLite/D1 source whose table has a SQLite-specific default — e.g. `installed_on TEXT NOT NULL DEFAULT (datetime('now'))` — used to carry that expression verbatim to Postgres, where `CREATE TABLE` failed with `ERROR: function datetime(unknown) does not exist`. Because that happens in the create-tables phase, **the whole migration aborted and no data loaded for any table** — one throwaway migration-history table could cost you the entire run.

Now the Postgres writer recognizes the SQLite-source default dialect and:

1. **Translates the portable "current instant" set** to the Postgres keyword equivalents — `datetime('now')` / `CURRENT_TIMESTAMP` → `CURRENT_TIMESTAMP`, `date('now')` / `CURRENT_DATE` → `CURRENT_DATE`, `time('now')` / `CURRENT_TIME` → `CURRENT_TIME` (case-, whitespace-, and paren-tolerant, and it accepts SQLite's double-quoted `"now"` misfeature).
2. **Drops a non-portable default with a loud `WARN`** instead of failing. For anything outside that provably-portable set (`julianday(…)`, `strftime(…)`, `unixepoch(…)`, arbitrary expressions), the column is created with **no DEFAULT** and sluice logs a warning naming the table, column, and dropped expression. A DEFAULT is non-data metadata — it only affects future inserts, never the rows being migrated (which carry explicit values) — so a loud drop is strictly better than aborting the migration. (This keeps sluice's loud-failure discipline: never silent.)

The result: a stray SQLite default never costs you the migration. With the next release's ORM-table handling, the throwaway migration-history tables that carry these defaults will be skipped by default anyway.

## Compatibility

Scoped to the **SQLite-source → Postgres-target** path. The MySQL-source default translator, the bit-literal default path, and same-engine / untagged defaults are byte-for-byte unchanged. A genuine non-portable default is now dropped-and-warned rather than failing — if your application relies on a target default, set it explicitly post-migration (the warning names the column).

## Who needs this

Anyone migrating a Cloudflare D1 / SQLite source into Postgres whose schema includes SQLite-specific column defaults (Flyway, Goose, or hand-written `datetime('now')`/`date('now')` defaults). Other source engines and same-engine migrations are unaffected.

---

**Install:** brew install sluicesync/tap/sluice · go install sluicesync.dev/sluice/cmd/sluice@v0.99.164 · **Container:** ghcr.io/sluicesync/sluice:0.99.164
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
