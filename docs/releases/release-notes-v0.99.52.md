# sluice v0.99.52

**MySQL `ENUM` columns now sync correctly over CDC — including schema changes to a PostgreSQL target (Bug 145).** Two issues, both fixed: enum *values* were replicated as their ordinal index instead of the label, and enum *schema changes* (add column / add value) didn't forward to a PG target.

## Fixed

- **MySQL ENUM CDC value fidelity.** The MySQL binlog delivers an `ENUM` cell as its **1-based ordinal index**, not the label, and sluice's CDC value decoder passed it straight through — so a replicated INSERT carried `"2"` instead of `"active"`. A PostgreSQL enum target **rejected** that (`SQLSTATE 22P02`); a MySQL target only seemed fine because it coerces the numeric string by index (fragile). The decoder now maps the index to its label via the column's value list; a label string (the snapshot/copy path and the VStream reader both deliver labels) passes through unchanged. This fixes **all** MySQL enum CDC, not only schema-change forwarding.
- **MySQL ENUM schema-change forwarding → PostgreSQL.** Two DDL-emission gaps:
  - `ADD COLUMN <enum>` referenced the named PG enum type without creating it first → `42704 type does not exist`. The forward path now creates the type (idempotent `CREATE TYPE`) before the `ADD COLUMN`.
  - A MySQL `MODIFY … ENUM(...)` that appends a value arrives as an alter-column-type and hit an internal "enum DDL requires column context" error. It now forwards as `ALTER TYPE … ADD VALUE IF NOT EXISTS`.

  Combined with the value fix, **MySQL→PG ENUM works end-to-end**: add an enum column, append a value, and rows land by label.

## Compatibility / notes

- No flag or config change. Both fixes are on by default.
- Forwarding an enum value **append** is exact. A value **rename or removal** on the source leaves the PG enum type a *superset* (PG's `ALTER TYPE` can't drop or rename a label) — no data loss (every value the source can still produce remains valid on the target), a documented edge.
- MySQL→MySQL enum sync was previously "working" only because MySQL coerces the index string by position; it is now correct (label-based) rather than coincidental.

## Who needs this

- Anyone running continuous sync from a **MySQL / PlanetScale source with `ENUM` columns**, especially to a **PostgreSQL** target (where the prior index value was rejected outright), and anyone whose source performs online enum schema changes (add column, add value).

## Install

```
# Linux/macOS/Windows binaries + checksums on the release page.
docker pull ghcr.io/sluicesync/sluice:v0.99.52
```
