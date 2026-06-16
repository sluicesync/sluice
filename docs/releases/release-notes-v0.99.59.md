# sluice v0.99.59

**A forwarded `DROP COLUMN` of a migrated ENUM column no longer leaves an
orphaned PostgreSQL enum type behind (Bug 150).** A small schema-change-
forwarding hygiene fix: when sluice drops a MySQL-source ENUM column on the
PostgreSQL side, it now also drops the dedicated per-column enum type it
synthesized for that column — but only that kind of type, never a shared or
named one.

## Fixed

- **Forwarded `DROP COLUMN` of a synthesized ENUM column drops its
  per-column enum type (Bug 150).** A MySQL `ENUM` has no type identity, so
  when sluice migrates one to PostgreSQL it creates a dedicated
  `"<table>_<col>_enum"` type for that column. A schema-change-forwarded
  `DROP COLUMN` removed the column but left that type as an orphan. It was
  harmless to existing data, but a later re-add of a same-named column with a
  *different* value set would collide with — or silently reuse — the stale
  type. `AlterDropColumn` now also issues
  `DROP TYPE IF EXISTS "<schema>"."<table>_<col>_enum"` for these synthesized
  types. A re-added column then gets a **fresh** type carrying its current
  values (pinned by an integration round-trip).
- **Shared/named PostgreSQL enum types are never auto-dropped.** The drop is
  gated on the synthesized shape (`TypeName == ""`). A preserved PG enum type
  (a named type, possibly shared across columns or tables) is left in place,
  so same-engine PG→PG enum sharing is unaffected.

## Compatibility / notes

- No flag or config change.
- Scope: **MySQL source → PostgreSQL target**, schema-change forwarding of a
  `DROP COLUMN` on an ENUM column. No effect on data rows or on same-engine
  PG→PG enum handling.

## Who needs this

- Anyone running continuous MySQL→PostgreSQL sync with schema-change
  forwarding enabled who drops (and possibly later re-adds) ENUM columns.

## Install

```
# Linux/macOS/Windows binaries + checksums on the release page.
docker pull ghcr.io/sluicesync/sluice:v0.99.59
```
