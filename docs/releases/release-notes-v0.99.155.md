# sluice v0.99.155

**Fixes Bug 170: migrating a SQLite `TEXT PRIMARY KEY` to MySQL failed at table creation with an opaque MySQL error (1170) that named neither the column nor a remedy. sluice now refuses early with an actionable message. No data loss in any version (the table creation failed cleanly); this is an error-clarity fix.**

## Fixed

**LOW/MEDIUM — a SQLite `TEXT PRIMARY KEY` migrated to MySQL failed at CREATE TABLE with the opaque errno 1170 (Bug 170).** A SQLite `TEXT`-affinity column used as a PRIMARY KEY maps to MySQL `LONGTEXT`, and MySQL/InnoDB cannot use a TEXT/BLOB column as a key without a prefix length, so `migrate`/`sync` from SQLite to MySQL hard-aborted at `CREATE TABLE` with `Error 1170 (42000) "BLOB/TEXT column used in key specification without a key length"`. There was zero data loss — the statement failed cleanly and nothing was written — but the raw 1170 gave no hint about which column was at fault or how to proceed. (The same schema migrates cleanly to Postgres, where a TEXT primary key is legal.)

**Fix.** The MySQL DDL emitter now detects, *before* issuing the statement, a PRIMARY KEY (or inline UNIQUE key) column that maps to a TEXT/BLOB-family type without a prefix length, and refuses with a message that names the table and column and gives the remedy: re-run with `--type-override <table>.<col>=VARCHAR(n)`, choosing `n` ≥ the longest key value (MySQL's maximum indexable length for `utf8mb4` is `VARCHAR(768)`). A key column that already carries a prefix length (which MySQL accepts), and a TEXT column outside any key, are unaffected.

**Why not auto-map.** sluice deliberately does *not* silently rewrite the column to a fixed `VARCHAR(n)`: picking a length on the operator's behalf risks truncating — and therefore colliding — a primary-key value, which is exactly the silent-corruption class sluice refuses to introduce. The operator chooses a safe length explicitly.

## How it was found

The PlanetScale target phase of the SQLite/D1 test program, migrating a corpus with `TEXT PRIMARY KEY` tables to PlanetScale-MySQL (worked around there with `--type-override`).

## Compatibility

Behavior-preserving: this only converts a downstream MySQL 1170 into an earlier, clearer refusal naming the column and remedy. Schemas that already migrated (VARCHAR keys, prefix-length keys, TEXT columns outside any key) are unaffected. Pinned by unit tests over the refusal cases (TEXT PK, charset-qualified TEXT PK, composite PK containing a TEXT column) and the allowed cases (prefix-length key, VARCHAR PK, TEXT column outside any key). MySQL-target only.

## Who needs this

Anyone migrating a SQLite (or other) schema with a `TEXT`/`BLOB` primary-key column to MySQL — you now get a one-line, column-named remedy instead of a bare `Error 1170` at table creation.

---

**Install:** brew install sluicesync/tap/sluice · go install sluicesync.dev/sluice/cmd/sluice@v0.99.155 · **Container:** ghcr.io/sluicesync/sluice:0.99.155
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
