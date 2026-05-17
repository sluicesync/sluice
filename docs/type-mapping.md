# Type Mapping

This document defines the internal type model and the policies that govern translation between MySQL, PostgreSQL, and the IR.

> **Related:** [docs/value-types.md](value-types.md) defines the *runtime* contract — the Go value types stored in `ir.Row` for each IR `Type`. This document covers the static type model and the DDL ↔ IR translation; that one covers the data values flowing through the pipeline.

The fundamental decision is that translation is **not** a pairwise dialect→dialect operation. It is always source-dialect → IR → target-dialect. With four directions in scope, the pairwise approach would require twelve mapping tables; the IR approach requires four readers and four writers.

## Internal type model

The IR type system is a sealed hierarchy. Every column type in every supported dialect maps to exactly one IR type. New types are added by extending the hierarchy, never by introducing dialect-specific shapes.

The IR is organised into two tiers:

**Core IR types** are the types every relational engine has, in some form. The translator and pipeline assume any engine can read and write these. Core types are the lingua franca.

**Extension IR types** are types that only some engines support natively. They exist in the IR so engines that have them can express them faithfully, but every engine declares (via its `Capabilities`) which extension types it supports. Engines that don't support an extension type either decline the migration with a clear error or apply a documented degradation policy (e.g., `Array` → `JSON` on MySQL).

This split is the mechanism that lets the IR remain genuinely engine-neutral as more engines are added. The core stays small and universal. New engine-specific richness arrives as new extension types — never by amending the core.

```go
package ir

// Type is the sealed interface for all column types.
type Type interface {
    isType()  // unexported sentinel; only IR types can satisfy this
    String() string
    Tier() Tier  // Core or Extension
}

// =====================================================================
// CORE IR TYPES — every engine must read and write these.
// =====================================================================

// --- Numeric ---

type Integer struct {
    Width         int8 // 8, 16, 24 (mysql mediumint), 32, 64
    Unsigned      bool
    AutoIncrement bool
}

type Decimal struct {
    Precision int // total digits
    Scale     int // digits after the decimal point
}

type Float struct {
    Precision FloatPrecision // Single (32-bit) or Double (64-bit)
}

type Boolean struct{}

// --- String ---

type Char struct {
    Length    int
    Charset   string
    Collation string
}

type Varchar struct {
    Length    int
    Charset   string
    Collation string
}

type Text struct {
    Size      TextSize // Tiny, Regular, Medium, Long
    Charset   string
    Collation string
}

// --- Binary ---

type Binary    struct { Length int }
type Varbinary struct { Length int }
type Blob      struct { Size BlobSize } // Tiny, Regular, Medium, Long

// --- Temporal ---

type Date      struct{}
type Time      struct { Precision int }
type DateTime  struct { Precision int }                     // no timezone
type Timestamp struct { Precision int; WithTimeZone bool }

// --- Structured ---

type JSON struct { Binary bool } // Postgres JSON vs JSONB

// =====================================================================
// EXTENSION IR TYPES — engines opt in via Capabilities.SupportedTypes.
// =====================================================================

// --- Categorical ---

type Enum struct { Values []string }   // MySQL ENUM, Postgres CREATE TYPE ... AS ENUM
type Set  struct { Values []string }   // MySQL SET; degraded elsewhere

// --- Identity ---

type UUID struct{}                     // Postgres uuid; MySQL stores as CHAR(36) or BINARY(16)

// --- Composite ---

type Array struct { Element Type }     // Postgres native; degraded to JSON on MySQL

// --- Spatial ---

type Geometry struct { Subtype GeometrySubtype } // PostGIS / MySQL spatial

// --- Network (Postgres-native) ---

type Inet    struct{}
type Cidr    struct{}
type Macaddr struct{}
```

### Engine opt-in to extension types

Each engine's `Capabilities` value (see [architecture.md](architecture.md#engine-capabilities)) includes a `SupportedTypes` set listing the extension types it handles natively. Three things follow from this:

1. **Readers** may only emit extension types listed in their own `SupportedTypes`. A MySQL reader cannot emit `Inet{}` because MySQL doesn't have it; it would emit `Varchar{Length: 45}` instead.

2. **Writers** consult their own `SupportedTypes` and the source engine's capabilities to decide what to do with an extension type they don't natively support. The default behaviour is documented degradation (e.g., `Array` → `JSON`); the user may override with a stricter `error` mode that fails the migration rather than degrading silently.

3. **Adding a new engine** does not require touching core IR types. It requires declaring which extension types the new engine supports, and providing the reader/writer code for those.

The contract is intentionally one-directional: engines describe themselves to the orchestrator. The orchestrator never asks "are you MySQL?" — it asks "do you support arrays?"

Each leaf type implements `isType()` so the compiler enforces that only IR types satisfy the interface — no dialect-specific shapes can sneak in through interface satisfaction.

## MySQL ↔ IR

### Reader policies

| MySQL declaration             | IR type                                            | Notes |
|-------------------------------|----------------------------------------------------|-------|
| `TINYINT(1)`                  | `Boolean{}`                                        | Convention. Configurable; see overrides. |
| `TINYINT` (any other width)   | `Integer{Width: 8}`                                | |
| `SMALLINT`                    | `Integer{Width: 16}`                               | |
| `MEDIUMINT`                   | `Integer{Width: 24}`                               | Becomes `Integer{Width: 32}` when emitted to Postgres. |
| `INT` / `INTEGER`             | `Integer{Width: 32}`                               | |
| `BIGINT`                      | `Integer{Width: 64}`                               | |
| `... UNSIGNED`                | `Integer{..., Unsigned: true}`                     | Postgres has no unsigned; see emitter. |
| `DECIMAL(p,s)` / `NUMERIC`    | `Decimal{Precision, Scale}`                        | |
| `FLOAT`                       | `Float{Single}`                                    | |
| `DOUBLE` / `REAL`             | `Float{Double}`                                    | |
| `BIT(1)`                      | `Boolean{}`                                        | |
| `BIT(n)` for n > 1            | `Varbinary{Length: ceil(n/8)}`                     | |
| `CHAR(n)`                     | `Char{Length: n}`                                  | |
| `VARCHAR(n)`                  | `Varchar{Length: n}`                               | |
| `TINYTEXT`                    | `Text{Size: Tiny}`                                 | |
| `TEXT`                        | `Text{Size: Regular}`                              | |
| `MEDIUMTEXT`                  | `Text{Size: Medium}`                               | |
| `LONGTEXT`                    | `Text{Size: Long}`                                 | |
| `BINARY(n)` / `VARBINARY(n)`  | `Binary{Length: n}` / `Varbinary{Length: n}`       | |
| `TINYBLOB` … `LONGBLOB`       | `Blob{Size: ...}`                                  | |
| `DATE`                        | `Date{}`                                           | |
| `TIME(p)`                     | `Time{Precision: p}`                               | |
| `DATETIME(p)`                 | `DateTime{Precision: p}`                           | |
| `TIMESTAMP(p)`                | `Timestamp{Precision: p, WithTimeZone: true}`      | MySQL `TIMESTAMP` always stores UTC. |
| `YEAR`                        | `Integer{Width: 16}`                               | Lossy in name only; values preserved. |
| `ENUM('a','b',...)`           | `Enum{Values: [...]}`                              | |
| `SET('a','b',...)`            | `Set{Values: [...]}`                               | |
| `JSON`                        | `JSON{Binary: false}`                              | MySQL JSON is binary-stored but value-equivalent. |
| `GEOMETRY`, `POINT`, etc.     | `Geometry{Subtype: ...}`                           | |

### Writer policies

When the IR is emitted as MySQL DDL, the inverse mapping applies, with the following notes:

- `Integer{Width: 24}` is preserved as `MEDIUMINT`.
- `Boolean{}` is emitted as `TINYINT(1)`.
- `Array{}`, `Inet{}`, `Cidr{}`, `Macaddr{}` from a Postgres source have no native MySQL representation. Sluice auto-emits a sensible shape as of v0.7.0:
  - `Inet`/`Cidr` → `VARCHAR(45)` (max IPv6 + CIDR mask in canonical form is 43 chars; round up for headroom)
  - `Macaddr` → `VARCHAR(30)` (EUI-64 in canonical form is 23 chars)
  - `Array` → `JSON` (operators wanting unstructured storage can pick `longtext` instead)

  The `mappings:` YAML hook and `--type-override` CLI flag continue to override the auto-shape for operators who want different storage (e.g. binary representations, `LONGTEXT` for array serialisation).
- `Timestamp{WithTimeZone: false}` is emitted as `DATETIME`.

## PostgreSQL ↔ IR

### Reader policies

| Postgres declaration              | IR type                                       | Notes |
|-----------------------------------|-----------------------------------------------|-------|
| `boolean`                         | `Boolean{}`                                   | |
| `smallint`                        | `Integer{Width: 16}`                          | |
| `integer`                         | `Integer{Width: 32}`                          | |
| `bigint`                          | `Integer{Width: 64}`                          | |
| `serial`, `bigserial`             | `Integer{..., AutoIncrement: true}`           | Sequence is recorded but managed at emit time. |
| `numeric(p,s)` / `decimal(p,s)`   | `Decimal{Precision, Scale}`                   | |
| `real`                            | `Float{Single}`                               | |
| `double precision`                | `Float{Double}`                               | |
| `char(n)` / `character(n)`        | `Char{Length: n}`                             | |
| `varchar(n)` / `character varying`| `Varchar{Length: n}`                          | |
| `text`                            | `Text{Size: Long}`                            | Postgres `text` is unbounded; map to widest MySQL equivalent. |
| `bytea`                           | `Blob{Size: Long}`                            | |
| `date`                            | `Date{}`                                      | |
| `time(p)`                         | `Time{Precision: p}`                          | |
| `timestamp(p)`                    | `Timestamp{Precision: p, WithTimeZone: false}`| |
| `timestamp(p) with time zone`     | `Timestamp{Precision: p, WithTimeZone: true}` | |
| user-defined `enum` type          | `Enum{Values: [...]}`                         | |
| `json`                            | `JSON{Binary: false}`                         | |
| `jsonb`                           | `JSON{Binary: true}`                          | |
| `uuid`                            | `UUID{}`                                      | |
| `T[]`                             | `Array{Element: ...}`                         | Recursive on element. |
| `inet`, `cidr`, `macaddr`         | `Inet{}`, `Cidr{}`, `Macaddr{}`               | |
| `geometry`                        | `Geometry{Subtype: ...}`                      | PostGIS only. |

### Writer policies

- `Integer{Unsigned: true}` widens by one rank: 8→`smallint`, 16→`integer`, 24/32→`bigint`, **64→`bigint` (uniform)**. The widening is documented in a per-run report. See the [MySQL unsigned integers](#mysql-unsigned-integers) edge case below for the load-bearing `bigint unsigned` policy and the deliberate range narrowing it carries.
- `Enum{}` defaults to a Postgres `enum` type (`CREATE TYPE foo AS ENUM (...)`). Per-column override available to emit as `text` with a `CHECK` constraint instead, which is more flexible at the cost of speed.
- `Set{}` from a MySQL source is emitted as `text[]` plus a `CHECK` constraint enforcing membership. Storage is larger but semantics are preserved.
- `Boolean{}` is emitted as `boolean`.
- `JSON{Binary: false}` is emitted as `jsonb` by default, since `jsonb` is almost always the right choice on Postgres. Override available.
- `Geometry{}` requires the PostGIS extension; if PostGIS is not in the allowlist, the run errors with an explicit message.

## Edge cases that need explicit policies

These are the cases that historically turn type-mapping code into a regex zoo. Each one is named, has a default policy, and is overridable via config.

### MySQL zero-dates (`'0000-00-00'`)

MySQL accepts `'0000-00-00'` as a `DATE` value when `NO_ZERO_DATE` is not in `sql_mode`. PostgreSQL does not.

**Default policy:** detect during read, surface a count in the pre-migration report, replace with `NULL` if the column is nullable, otherwise replace with `'0001-01-01'` (a minimum sentinel) and log every replacement.

**Override:** `on_zero_date: error | null | sentinel | <literal>`.

### MySQL unsigned integers

PostgreSQL has no unsigned integer types.

**Default policy (`tinyint`/`smallint`/`mediumint`/`int` unsigned):** widen to the next signed integer rank — `tinyint unsigned`→`smallint`, `smallint unsigned`→`integer`, `mediumint unsigned`→`integer`, `int unsigned`→`bigint`. The original numeric range still fits losslessly. Widening is reported per-column. This mapping is consistent across PK / FK-child / standalone columns, so a foreign key matches its referenced primary key by construction.

**`bigint unsigned` policy (deliberate range narrowing — read this):** `bigint unsigned` maps **uniformly to PostgreSQL `bigint`** — for primary keys, foreign-key child columns, and standalone columns alike. PostgreSQL has no unsigned 64-bit type, so the upper half of the range — values in `(2^63-1, 2^64-1]` — is **not representable on the target**. This is an intentional, documented cross-engine policy, not a silent wart, for these reasons:

- **It is the only mapping that keeps FK types consistent.** PostgreSQL's `GENERATED ... AS IDENTITY` is valid only on `smallint`/`integer`/`bigint`, never `numeric`. A `bigint unsigned AUTO_INCREMENT PRIMARY KEY` therefore *must* emit `bigint ... IDENTITY`. If a plain `bigint unsigned` FK child column instead widened to `numeric(20,0)` (the pre-v0.68.2 policy), the FK column type would not match the IDENTITY PK type and `ALTER TABLE ... ADD FOREIGN KEY` would fail `SQLSTATE 42804` (datatype mismatch) — after the target was partially created. `id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY` + `*_id BIGINT UNSIGNED` foreign keys is the *default* schema shape of essentially every Rails / Laravel / Django / Sequelize / Prisma MySQL application, so the pre-fix policy broke the most common real-world migration with no `schema preview` warning. The uniform `bigint` mapping makes PK and FK types match by construction with zero schema-graph machinery (Bug 11 / v0.68.2).
- **The narrowed range is virtually never used.** Real `bigint unsigned` columns — especially autoincrement IDs — do not reach `2^63` (≈9.2 × 10¹⁸) in practice. This is the industry-standard pragmatic mapping (pgloader and peers do the same).
- **The narrowing is surfaced LOUDLY, never silently.** Both `sluice schema preview` and `sluice migrate` preflight emit a dedicated, operator-actionable **unsigned-bigint range-narrowing notice** that names every affected `table.column`, states the `2^63-1` ceiling, and gives the per-column override. It is an *advisory notice* (the migration proceeds — the universal ORM schema must still migrate), not a hard refusal, but it is visible at both surfaces (a section in `schema preview` output / JSON `unsigned_bigint_narrowings`, and a `WARN` log line at `migrate` preflight). The loud-failure tenet is satisfied by the loud notice, not by silently narrowing.

**Override:** for a column that genuinely stores values above `2^63-1`, supply `--type-override TABLE.COL=numeric` (or the per-column `mappings:` hook) to keep the full unsigned 64-bit range as `numeric(20,0)`. Note such a column then *cannot* also be an `IDENTITY`/`AUTO_INCREMENT` key — that combination is impossible in PostgreSQL and is precisely why the default cannot be `numeric` for autoincrement keys.

### MySQL ENUM and SET → Postgres

**ENUM default:** Postgres `enum` type. Faster, more space-efficient, but rigid (adding a value requires `ALTER TYPE`).

**ENUM override:** `text` plus a `CHECK` constraint listing valid values.

**SET default:** `text[]` plus a `CHECK` constraint that every element is in the allowed set.

**SET override:** `text` with a comma-delimited representation, preserving MySQL's storage shape but losing structured query support.

### Postgres ENUM → MySQL

MySQL `ENUM` is roughly equivalent in shape but is not a separate type — it is a column-level definition. The translator produces a column-level `ENUM(...)`. If the Postgres enum is used by multiple columns, each column gets its own MySQL `ENUM` declaration (no shared type).

### Postgres ARRAY → MySQL

MySQL has no array type.

**Default policy:** emit as `JSON`. Reads and writes serialise/deserialise transparently in the row pipeline.

**Override:** `array_strategy: json | concat`. The `concat` option is offered for simple scalar arrays where a comma-delimited string is preferable.

### JSON semantics

MySQL `JSON` and Postgres `jsonb` both validate and normalise on insert. MySQL `JSON` and Postgres `json` (no `b`) preserve original whitespace and key order.

**Default mapping:** MySQL `JSON` → `JSON{Binary: true}`, Postgres `jsonb` → `JSON{Binary: true}`, Postgres `json` → `JSON{Binary: false}`.

**Default emission:** `JSON{Binary: true}` → MySQL `JSON` / Postgres `jsonb`. `JSON{Binary: false}` → MySQL `JSON` (no distinction available) / Postgres `json`.

### Charsets and collations

Charset and collation are stored on `Char`, `Varchar`, and `Text`. The reader records the source values verbatim. The writer attempts to map to a target-side equivalent using a small lookup table (e.g. MySQL `utf8mb4_unicode_ci` → Postgres collation `und-x-icu`) and falls back to the database default when no equivalent exists, with a warning.

## Per-column overrides

The default policies cover the common case. The config file is the canonical place to override on a per-column basis. Overrides are typed against the IR, not against dialect-specific syntax.

```yaml
mappings:
  - table: orders
    column: status
    # Force this column to be emitted as text with a CHECK constraint
    # instead of a Postgres ENUM type.
    enum_strategy: text_check

  - table: legacy_users
    column: created_at
    # This column has known zero-dates; replace with NULL even though
    # the column is currently NOT NULL (we'll relax the constraint).
    on_zero_date: null
    nullable_override: true

  - table: events
    column: payload
    # Force jsonb regardless of source-side json/jsonb distinction.
    target_type: json
    target_type_options:
      binary: true
```

## Expression translation and extension gating

Type translation is one half of the cross-engine story; expression-body translation is the other. The translator catalog (see [docs/dev/translator-coverage.md](dev/translator-coverage.md)) rewrites MySQL expressions in `DEFAULT` / `GENERATED` / `CHECK` bodies into their PG equivalents. Most rewrites ship unconditionally, but a few depend on operator-enabled Postgres extensions:

- **`SHA1(x)` / `SHA2(x, n)` → `encode(digest(x, '<algo>'), 'hex')`** (v0.38.0) — requires `pgcrypto` on the target. Pass `--enable-pg-extension pgcrypto` and ensure `CREATE EXTENSION pgcrypto;` has been run; the rewrite then fires automatically. Without the flag, the calls fall through verbatim and PG's parse-time error signals the missing extension. `MD5(x)` ships unconditionally — PG core has `md5(text)`.

For MySQL → PG migrations, run `sluice schema preview` first — its translator-gap preflight scan (v0.39.0, see [ADR-0024](adr/adr-0024-schema-preview.md)) lists every expression pattern sluice does NOT auto-rewrite, with operator-actionable workaround hints.

## What the IR is not

The IR is not a wire format. It is not stable. It is not for users. It is an internal Go data model whose only job is to make the four-direction matrix tractable and the type-translation code testable. We will not export it, version it, or guarantee compatibility across releases.

If at some point external tooling wants to integrate, that's a separate decision about a stable schema export format — likely something like a JSON schema descriptor — rather than exposing the IR itself.
