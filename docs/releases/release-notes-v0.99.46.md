# sluice v0.99.46

**Completes the schema-change-forwarding story: a PostgreSQL-source `RENAME COLUMN` now forwards through the live CDC stream instead of halting it** — when sluice can *prove* it's a rename (not a drop+add) via the column's stable catalog identity. This is the final piece of the default-on schema-change forwarding that landed in v0.99.45 (ADR-0091); see Compatibility for the safety reasoning.

## Features

- **PostgreSQL-source `RENAME COLUMN` forwarding (ADR-0091, F7b).** Under the default `--schema-changes=forward`, a column rename on a PostgreSQL source now forwards to the target as `ALTER TABLE … RENAME COLUMN` (data preserved on the target) — but **only when it is provable.** A rename and a `DROP x + ADD y` of the same type are indistinguishable from the replication stream's row shape alone, so sluice proves the distinction with `pg_attribute.attnum`, the column's catalog identity that is **stable across a rename**: same attnum + different name = a real rename (forward, preserve data); a different attnum = a genuine drop+add (refuse loudly). The proof is definitive, so this can only ever forward a true rename or refuse — it can never mis-forward a drop+add and lose data. Works same-engine (PG → PG) and cross-engine (PG → MySQL, where the rename is applied on the MySQL target). A **MySQL source** has no equivalent stable column id, so a MySQL-source rename continues to refuse loudly (drain + rename manually) — unchanged.

## Compatibility

- **Extends the v0.99.45 behavior change to PostgreSQL renames.** v0.99.45 made `sluice sync` forward source DDL by default; renames were the one shape it still refused. On a PostgreSQL source, a `RENAME COLUMN` now forwards automatically. To keep the conservative halt-on-DDL behavior for *all* shapes, set `--schema-changes=refuse` (unchanged from v0.99.45). No on-disk/format change; `migrate` and the cold-start copy path are untouched.
- The rename proof relies on a column-identity catalog read on the PostgreSQL source at schema-change boundaries (rare events) — no steady-state cost.

## Who needs this

- Operators running a **live PostgreSQL-source sync** who rename columns as part of routine schema evolution — the stream now stays online through the rename instead of refusing and forcing a drain + manual DDL.
- Everyone else is unaffected: MySQL-source renames still refuse (safely), and no other shape's behavior changes from v0.99.45.

## Install

```
# Linux/macOS/Windows binaries + checksums on the release page.
# Container image:
docker pull ghcr.io/sluicesync/sluice:v0.99.46
```
