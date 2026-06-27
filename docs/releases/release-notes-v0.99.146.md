# sluice v0.99.146

**SQLite is now a migration TARGET, not just a source — `sluice migrate --target-driver sqlite --target ./out.db` writes a SQLite database from Postgres, MySQL, or another SQLite, which (with `wrangler d1 import`) makes `X → SQLite → Cloudflare D1` a clean path. Plus a fix restoring `--type-override` for SQLite sources.**

## Features

**SQLite as a migration target (ADR-0134).** The SQLite engine gains `OpenSchemaWriter` + `OpenRowWriter` over the pure-Go `modernc.org/sqlite` driver (still no CGO), so any source migrates INTO a SQLite file. It is a target for `migrate` only — CDC/ChangeApplier stay not-implemented (`CDC = CDCNone`). The type map IR→SQLite is chosen so the existing SQLite reader reads each emitted declared type back to the same IR type, so `SQLite → X → SQLite` and `X → SQLite → read` round-trip: `Integer→INTEGER`, `Float→REAL`, `Boolean→BOOLEAN` (0/1), `Char/Varchar/Text→TEXT`, `Blob/Binary/Varbinary→BLOB`, `Decimal→DECIMAL(p,s)`/`NUMERIC`, `Date→DATE`, `Time→TIME`, `DateTime/Timestamp→DATETIME`, `JSON/UUID/Enum/Set→TEXT`.

**Faithful, refuse-not-coerce value encoding.** The writer is the inverse of the reader's storage-class decode: an `int64` (including > 2^53) is bound as INTEGER and round-trips exactly, a `bool` as 0/1, a timestamp as canonical ISO-8601 TEXT (a tz-aware source value is stored as its UTC instant — instant-faithful, display zone dropped, since SQLite is tz-naive), and any value that cannot be faithfully represented is refused loudly (naming table/column), never silently coerced. DECIMAL has a SQLite-specific limit: NUMERIC affinity stores a decimal text value losslessly only within ~15 significant digits, so a decimal that is neither an int64-range integer nor representable in 15 significant digits is refused loudly (escape hatch: `--type-override <col>=text` to carry it verbatim). Within the guard the stored decimal is numerically lossless, though its TEXT form may be normalized (scientific notation, dropped trailing zeros) — an explicitly-pinned contract of numeric (not byte-identical) fidelity.

**Constraints + FK integrity within SQLite's model.** SQLite cannot `ALTER TABLE ADD` a FOREIGN KEY or CHECK after creation, so `CreateTablesWithoutConstraints` emits the full inline DDL (PK/UNIQUE/CHECK/FK), the bulk-copy phase runs with `PRAGMA foreign_keys=OFF` for order-independence, and the constraints phase runs a final `PRAGMA foreign_key_check` that refuses loudly on any violation. Materialized views and unrepresentable types (geometry/inet/bit/interval/array/EXCLUDE/RLS) are refused loudly.

## Fixed

**`--type-override` for SQLite sources (Bug 161).** A SQLite-source migrate with `--type-override <col>=<type>` for any non-default target type — e.g. `=varchar(255)` to turn a SQLite TEXT primary key into a valid bounded MySQL `VARCHAR` PK — aborted with `sqlite: no decoder for IR type ir.Varchar`: the value decoder only handled the IR types SQLite's own affinity resolution produced, not the broader families `--type-override` rewrites a column to. The decoder now routes the string-affinity family (`Varchar`/`Char`/`JSON`/`UUID`) through the same faithful decode as `Text`, and the binary family (`Binary`/`Varbinary`) through the same decode as `Blob` — a SQLite TEXT/BLOB value carries into the overridden type, and any other storage class is still a loud mismatch (refuse-not-coerce preserved). This restores the documented escape hatch the value-fidelity policy relies on.

## Compatibility

Additive: a new `sqlite` target driver and a decoder fix; nothing else changes. The target engine shipped under a full IR-type × value-family unit matrix, an explicit decimal numeric-fidelity + loud-refusal suite, `SQLite→PG→SQLite` / `SQLite→MySQL→SQLite` / `PG→SQLite` / `MySQL→SQLite` / FK-integrity cross-engine integration tests, and an independent value-fidelity review. Known limit (Bug 161, separate from the `--type-override` fix): a SQLite TEXT/BLOB primary key migrated to MySQL still aborts at create-tables with MySQL's "BLOB/TEXT column used in key specification" error — use `--type-override <pk>=varchar(N)` (now working) to give it a bounded length; Postgres targets are unaffected.

## Who needs this

Anyone who wants to produce a SQLite database from a Postgres/MySQL source — including the `X → SQLite → Cloudflare D1` import path — and anyone who hit the `--type-override` "no decoder" error on a SQLite source.

---

**Install:** brew install sluicesync/tap/sluice · go install sluicesync.dev/sluice/cmd/sluice@v0.99.146 · **Container:** ghcr.io/sluicesync/sluice:0.99.146
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
