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
| `TINYINT(1)`                  | `Boolean{}`                                        | Convention. A value outside `{0,1}` is collapsed to `true` by the boolean mapping; the reader WARNs loudly per column. Override with `--type-override col=smallint`/`int` to preserve the integer. |
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
| `TIME(p)`                     | `Time{Precision: p}`                               | MySQL TIME is a *duration* (−838:59:59…838:59:59); PG `time` holds only 00:00–24:00. See [MySQL TIME range](#mysql-time-outside-postgres-times-range-durations--bug-187). |
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
- `Time{WithTimeZone: true}` (a Postgres `timetz` / `time with time zone`) is emitted as MySQL `TIME` — MySQL has **no** timezone-aware time type, so the zone is dropped. This is the same documented cross-engine policy as `Timestamp{WithTimeZone: true}` → MySQL `TIMESTAMP` (zone-flattening, not a refusal): a tz-aware temporal still migrates, with the zone offset lost on the MySQL side. PG → PG round-trips `timetz` faithfully (the IR carries `Time.WithTimeZone`, the PG writer emits `TIME WITH TIME ZONE`, and a per-conn binary codec encodes the value — pgx ships none for `timetz`). Catalog Bug 71.
- `Decimal{Unconstrained: true}` (an unconstrained Postgres `numeric`) is emitted as `DECIMAL(65,30)` — MySQL's widest representable fixed-point form, since MySQL has no unbounded DECIMAL — plus a loud, operator-overridable widening advisory at `schema preview` and `migrate` preflight. Constrained `Decimal{Precision, Scale}` emits `DECIMAL(p,s)` unchanged. See the [unconstrained numeric](#unconstrained-postgres-numeric-no-precisionscale--owner-surface-design-call-v069x--bug-69) edge case.
- `Varchar{Length: N}` with `N` over MySQL's representable cap (utf8mb4's ~16383-char creatable limit / the 65535-byte InnoDB row budget) is down-mapped to the smallest MySQL **TEXT-family** type that still holds `N` characters (`TEXT` ≤ 65535 bytes, `MEDIUMTEXT` ≤ 16 MiB, else `LONGTEXT`), sized by the column's worst-case byte width (`N × 4`). This mirrors the unbounded PG `text` → `LONGTEXT` policy and is surfaced by a loud, operator-overridable advisory at `schema preview` and `migrate` preflight. Narrow varchars (≤ 16000 chars) are emitted as `VARCHAR(N)` unchanged. PG → PG round-trips `varchar(N)` unchanged (PG has no such limit). Catalog Bug 72.

## PostgreSQL ↔ IR

### Reader policies

| Postgres declaration              | IR type                                       | Notes |
|-----------------------------------|-----------------------------------------------|-------|
| `boolean`                         | `Boolean{}`                                   | |
| `smallint`                        | `Integer{Width: 16}`                          | |
| `integer`                         | `Integer{Width: 32}`                          | |
| `bigint`                          | `Integer{Width: 64}`                          | |
| `serial`, `bigserial`             | `Integer{..., AutoIncrement: true}`           | Only the factory-default, single-owner shape collapses; anything looser is carried as a standalone sequence. See [Sequences and serial columns](#sequences-and-serial-columns). |
| `numeric(p,s)` / `decimal(p,s)`   | `Decimal{Precision, Scale}`                   | |
| `numeric` / `decimal` (bare)      | `Decimal{Unconstrained: true}`                | Arbitrary precision; `numeric_precision`/`numeric_scale` are NULL. See [unconstrained numeric](#unconstrained-postgres-numeric-no-precisionscale--owner-surface-design-call-v069x--bug-69). |
| `real`                            | `Float{Single}`                               | |
| `double precision`                | `Float{Double}`                               | |
| `char(n)` / `character(n)`        | `Char{Length: n}`                             | |
| `varchar(n)` / `character varying`| `Varchar{Length: n}`                          | |
| `text`                            | `Text{Size: Long}`                            | Postgres `text` is unbounded; map to widest MySQL equivalent. |
| `bytea`                           | `Blob{Size: Long}`                            | |
| `date`                            | `Date{}`                                      | |
| `time(p)`                         | `Time{Precision: p}`                          | Explicit `p` includes 0 — `time(0)` is catalog-distinct from bare `time`. |
| `time` (bare, no precision)       | `Time{PrecisionUnspecified: true}`            | atttypmod -1; behaves as the engine default (6) but round-trips BARE. See [temporal precision](#temporal-fractional-second-precision-bare-vs-explicit--restore-parity-triage-3). |
| `time(p) with time zone` (`timetz`)| `Time{Precision: p, WithTimeZone: true}`     | PG→PG faithful; PG→MySQL drops the zone (no tz-aware MySQL time). Bug 71. Bare form → `PrecisionUnspecified`. |
| `timestamp(p)`                    | `DateTime{Precision: p}`                      | Bare form → `DateTime{PrecisionUnspecified: true}`. |
| `timestamp(p) with time zone`     | `Timestamp{Precision: p, WithTimeZone: true}` | Bare form → `Timestamp{WithTimeZone: true, PrecisionUnspecified: true}`. |
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
- `Decimal{Unconstrained: true}` is emitted as bare `NUMERIC` (no parentheses) — PostgreSQL's native arbitrary-precision form. Constrained `Decimal{Precision, Scale}` emits `NUMERIC(p,s)` unchanged. See the [unconstrained numeric](#unconstrained-postgres-numeric-no-precisionscale--owner-surface-design-call-v069x--bug-69) edge case.
- `Boolean{}` is emitted as `boolean`.
- Temporal types with `PrecisionUnspecified` emit the BARE form (`TIMESTAMP WITH TIME ZONE`, no parentheses); a known precision always emits explicitly, **including `(0)`** — the pre-fix `0 → bare` collapse silently widened an explicit `timestamptz(0)` (and a MySQL `DATETIME`, whose precision is genuinely 0) to the 6-behaving bare form. See [temporal precision](#temporal-fractional-second-precision-bare-vs-explicit--restore-parity-triage-3).
- A `ConstraintBacked` unique index (a source PG `CONSTRAINT x UNIQUE (...)`) re-emits as `ALTER TABLE ... ADD CONSTRAINT x UNIQUE (...)` in the constraints phase (unique constraints before FKs, which may reference them); a plain unique index stays `CREATE UNIQUE INDEX` in the index phase. See [UNIQUE constraints vs unique indexes](#unique-constraints-vs-unique-indexes--restore-parity-triage-4).
- `JSON{Binary: false}` is emitted as `jsonb` by default, since `jsonb` is almost always the right choice on Postgres. Override available.
- `Geometry{}` requires the PostGIS extension; if PostGIS is not in the allowlist, the run errors with an explicit message.

### Sequences and serial columns

PG sequences fall into three classes with three different policies (item 51, restore-parity oracle finding #1):

- **Identity columns** (`GENERATED ... AS IDENTITY`) round-trip as identity columns; their implicit backing sequences are never surfaced.
- **Classic `serial` / `bigserial` columns are deliberately modernized to identity columns on emit.** This applies only to the exact serial shape: a sequence OWNED BY the column, carrying factory-default options (`START 1 INCREMENT 1 MINVALUE 1 CACHE 1 NO CYCLE`, type-max `MAXVALUE`), referenced by no other column's default. The modernized column behaves identically for inserts (same numbers in the same order, and `SyncIdentitySequences` advances it past the copied rows), but the catalog *text* differs from a pg_dump restore's: pg_dump re-emits the serial trio (`CREATE SEQUENCE` + `OWNED BY` + `SET DEFAULT nextval(...)`) while a sluice target dumps an identity clause. That delta is a documented decision, not a gap — the dump-parity oracle allowlists it citing this section.
- **Standalone sequences** — explicitly created sequences, serials whose sequence was re-optioned, and sequences shared by multiple columns — are carried as first-class schema objects (`ir.Schema.Sequences`): the exact options (`AS type` / `START` / `INCREMENT` / `MINVALUE` / `MAXVALUE` / `CACHE` / `CYCLE`), the `OWNED BY` binding when present, and the current position (`last_value` / `is_called`, primed onto the target via `setval` so post-migration `nextval()` continues exactly where the source would). Referencing columns keep their `DEFAULT nextval('...')` verbatim. Cross-engine targets (MySQL family, SQLite/D1) **refuse loudly** when the schema carries standalone sequences — they have no sequence objects, `--exclude-table` is no escape (a sequence is schema-level), and the refusal names the sequence plus the remedy (drop the sequence + its nextval defaults on the source, or use a PG target). Backups whose schema carries standalone sequences are stamped backup format version 4 so pre-sequence binaries refuse-before-touch instead of silently restoring a sequence-less target (see `docs/backup-format-versioning.md`).

  **Position sync windows.** A standalone sequence's *position* re-syncs at discrete points, not continuously: CDC carries row changes, never catalog-level sequence advances, so under continuous sync the target's sequence position lags the source between the initial carry and `sluice cutover` — cutover re-reads the live source position and forward-only primes the target (with the same `--sequence-margin` headroom identity columns get; CYCLE sequences are reported as skipped and need manual verification). Chain restore likewise re-primes at the chain tail from the newest link's captured schema; incrementals refresh sequence positions at window *end*, so the residual exposure is only source advancement after the final link's end-of-window read — the chain's ordinary row RPO. Run `sluice cutover` (or a manual `setval`) before taking writes on a chain-restored target whose sequences advanced past the last captured link.

### Typed-literal default rendering

sluice re-emits literal column defaults as quoted literals, which PostgreSQL stores as explicitly typed casts — a source column declared `DEFAULT 0` lands in the target catalog as `DEFAULT '0'::bigint`. The two forms evaluate identically for every insert; only the catalog text (and therefore pg_dump output) differs. This is an operator-approved cosmetic divergence (2026-07-03), surfaced and allowlisted by the restore-parity oracle citing this section.

### Extension-passthrough types (`--enable-pg-extension`)

Postgres extension types — `hstore`, `citext`, `pgvector` (`vector`), `pg_trgm` (operator classes), PostGIS (`geometry`/`geography`) — are opt-in via `--enable-pg-extension EXT` (repeatable), per [ADR-0032](adr/adr-0032-pg-extension-passthrough.md). The flag is required because the target must actually have `CREATE EXTENSION <ext>` run; a pre-flight refuses cleanly if it doesn't.

- **`hstore`** (ADR-0032 Tier 1). With `--enable-pg-extension hstore`:
  - **PG → PG:** passes through verbatim — the column stays `hstore` and values round-trip in their text form (`"a"=>"1", "b"=>"2"`).
  - **PG → MySQL:** degraded to MySQL `JSON` — the `"k"=>"v"` text is rewritten to `{"k":"v"}` at value-write time (key/value pairs preserved; hstore has no ordering to lose).
  - **Without the flag:** an `hstore` column refuses **loudly at schema-read**, naming the column and the remedy (`pass --enable-pg-extension hstore to enable passthrough`) — it is *not* silently dropped, and it is *not* the older misleading "not a recognised enum" message (that wording is now reserved for a genuinely-unknown, non-extension user-defined type).
- `citext` follows the same Tier-1 shape; `pgvector`/`pg_trgm`/PostGIS add index-method awareness (Tier 2). See ADR-0032 for the per-extension catalog and the cross-engine policy.

## SQLite & Cloudflare D1 ↔ IR

SQLite (and Cloudflare D1, which is SQLite over HTTP) is the one engine whose *value* storage is not pinned by its *column* declaration: a column has a **type affinity** (one of INTEGER / TEXT / BLOB / REAL / NUMERIC) derived from its declared-type spelling, but each stored value carries its own storage class. sluice's mapping respects affinity on the schema side and refuses loudly on a per-row storage-class mismatch on the value side ([ADR-0128](adr/adr-0128-sqlite-d1-migrate-source.md)); the value-level wrinkle (the `(typeof, text/hex)` encoding that keeps big ints and BLOBs exact) is in [value-types.md](value-types.md#sqlite--cloudflare-d1-the-typeof-texthex-value-encoding).

### Reader policies (SQLite / D1 → IR)

A column's IR type is resolved from its **declared type** in a load-bearing order ([ADR-0129](adr/adr-0129-sqlite-date-bool-policy.md) first, affinity second):

1. **Declared temporal / bool spellings override affinity.** A column declared `DATE` → `ir.Date`; `DATETIME` / `TIMESTAMP` → `ir.Timestamp` (no tz; SQLite is tz-naive); `TIME` → `ir.Time`; `BOOL` / `BOOLEAN` → `ir.Boolean`. (`DATETIME` is checked before `DATE`/`TIME` because the string contains all three.) These spellings would otherwise fall to NUMERIC affinity and read as decimals. Temporal types carry `PrecisionUnspecified` — SQLite has no fractional-second precision concept, so targets pick their max-fidelity form (PG bare, MySQL `(6)`; see [temporal precision](#temporal-fractional-second-precision-bare-vs-explicit--restore-parity-triage-3)).
2. **Otherwise, affinity maps to IR:** INTEGER → `ir.Integer{Width:64}` (SQLite integers are 64-bit signed); TEXT → `ir.Text` (unbounded — declared `VARCHAR(n)` lengths are not enforced by SQLite, so no misleading bound is carried); BLOB (or no declared type) → `ir.Blob`; REAL → `ir.Float{Double}`; NUMERIC → `ir.Decimal{Unconstrained:true}`.

The **value** of a declared temporal column is ambiguous (SQLite has no native date type — dates live as ISO text, unix integers, or Julian-day reals by app convention), so the encoding is an explicit operator choice, `--sqlite-date-encoding` (`iso` default / `unixepoch` / `unixmillis` / `julian`). A value whose storage class doesn't match the chosen encoding is **refused loudly** naming the row — never a silently-wrong date. Booleans decode `0`/`1` and truthy text; any other value is refused. The per-column escape hatch is `--type-override <col>=text` (carry the column raw). A per-source override is the `sqlite_date_encoding` DSN query param.

Generated columns, CHECK constraints, and partial / expression indexes are read and **carried verbatim** tagged dialect `"sqlite"` ([ADR-0133](adr/adr-0133-sqlite-schema-feature-detection.md)): portable constructs work on the target, non-portable ones (e.g. `strftime`) are loud-rejected at target DDL rather than silently dropped or mistranslated.

### Writer policies (IR → SQLite)

SQLite as a migrate target ([ADR-0134](adr/adr-0134-sqlite-target-engine.md)) emits the declared type the reader reads back to the same IR type — the faithful inverse of the affinity + ADR-0129 rules:

| IR type | SQLite declared type | Affinity | Note |
|---|---|---|---|
| `Boolean` | `BOOLEAN` | NUMERIC | value stores 0/1 |
| `Integer` | `INTEGER` | INTEGER | 64-bit signed; width/sign not preserved |
| `Float` | `REAL` | REAL | 8-byte IEEE-754 |
| `Decimal` | `TEXT` | **TEXT** | **Bug 162 — byte-exact.** NUMERIC affinity would coerce `19.99` to the binary float `19.989999999999998` and silently corrupt money; TEXT stores the exact decimal string verbatim. Reads back as `ir.Text` (documented downgrade). |
| `Char` / `Varchar` / `Text` | `TEXT` | TEXT | length not enforced; `Char`/`Varchar` widen to `ir.Text` on round-trip |
| `Binary` / `Varbinary` / `Blob` | `BLOB` | BLOB | |
| `Date` / `Time` / `DateTime`,`Timestamp` | `DATE` / `TIME` / `DATETIME` | — | tz-aware values land instant-faithful as UTC ISO (display zone dropped — SQLite has no tz type) |
| `JSON` / `UUID` / `Enum` / `Set` | `TEXT` | TEXT | no native SQLite type; raw value preserved exactly |

Anything SQLite has no faithful storage for — geometry, `inet`/`cidr`/`macaddr`, `bit`, `interval`, `array`, `domain`, unknown extension types — is **refused loudly at emit time** naming the IR type, never coerced to a silently-wrong column (use `--type-override` to carry it as text/blob if a lossy carry is acceptable). Because SQLite can't `ALTER TABLE … ADD` a FK or CHECK after creation, all constraints are emitted inline in the `CREATE TABLE`; the constraint phase becomes a `PRAGMA foreign_key_check` verification.

> D1 is not a migrate target. To land data in D1, emit a SQLite `.db` (`--target-driver sqlite`) and then `wrangler d1 import`. See [docs/operator/sqlite-d1-import.md](operator/sqlite-d1-import.md).

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

### MySQL `TINYINT(1)` used as an integer (not a boolean)

`TINYINT(1)` is MySQL's conventional boolean, and sluice maps it to `Boolean{}` by default (the `(1)` is only a display width; the column physically stores the full signed 8-bit range, `-128…127`). Some schemas use `TINYINT(1)` as a genuine small integer — a status code, a small enum, a count — so it can hold values outside `{0,1}`.

**Default policy:** keep the documented `TINYINT(1)`→`Boolean` mapping (changing it would break the overwhelming majority of schemas that *do* mean boolean), but **detect and WARN loudly** when a value outside `{0,1}` is read. The boolean decode collapses every non-zero value to `true`, so `2`/`127`/`-1` lose their real value; sluice now emits a one-time-per-column `WARN` (naming the `table.column` and an example value) on every read path — the bulk-copy / snapshot reader, the binlog CDC reader, and the VStream CDC + cold-start path — instead of doing it silently.

**Override:** for a column that genuinely stores integers, supply `--type-override TABLE.COL=smallint` (or `=int`/`=integer`, or the per-column `mappings:` hook) to preserve the value end-to-end. The override rewrites the IR type the **reader** decodes with, so the cell is read as an integer (not collapsed to a bool) and carried faithfully to the target. `smallint` (16-bit) is the recommended floor: a `TINYINT(1)` value always fits, and — unlike a `tinyint` override — it cannot re-emit a MySQL `TINYINT(1)` target column that would re-trigger the boolean mapping on a round-trip.

### MySQL `TIME` outside Postgres `time`'s range (durations) — Bug 187

MySQL `TIME` is semantically a **duration**, spanning `-838:59:59` … `838:59:59` — negative values and values beyond 24 hours are legal and common in elapsed-time / offset columns. PostgreSQL `time` is a **time-of-day**: `00:00:00` – `24:00:00`, never negative. sluice maps MySQL `TIME(p)` → PG `time(p)` by default (correct for the overwhelmingly common time-of-day usage), so a stored duration outside PG `time`'s window has **no representation on the target**.

**Default policy (unchanged):** keep the `TIME` → `time` mapping. A value outside `00:00:00–24:00:00` **refuses loudly at copy time** — pgx declines to encode it, and the table fails with coded `SLUICE-E-BULKCOPY-TABLE-FAILED` naming the table (zero rows written to it, zero silent loss). Nothing is ever clamped or wrapped.

**The advisory (so the refusal isn't a mid-copy surprise):** `schema preview` emits a per-column hint on every MySQL-family → PG `TIME` column (the duration-range caveat plus the copy-pasteable override), and `migrate` / `sync` preflight WARNs with the same wording before any table is created or row moves — the same two-surface advisory wiring as the unsigned-bigint notice. The notice fires for every MySQL-family source (`mysql`, `mariadb`, `planetscale`, `vitess`, `mydumper`) and is suppressed per-column once the operator applies an override.

**Override (lossless):** for a column that stores durations, supply `--type-override TABLE.COL=interval` — PG `interval` holds the full MySQL `TIME` range, and the override is first-class on both `migrate` and continuous sync/CDC (the value is carried as its textual duration form, which PG's interval parser accepts). `--type-override TABLE.COL=text` carries it as raw text instead.

### Unconstrained Postgres `numeric` (no precision/scale) — owner-surface design call (v0.69.x / Bug #69)

A PostgreSQL column declared as bare `numeric` / `decimal` with **no precision or scale** is *arbitrary precision* — PostgreSQL stores whatever the value requires. This is an extremely common PG column shape. `information_schema` reports both `numeric_precision` and `numeric_scale` as **NULL** for such a column. The IR models this distinctly from a bounded `numeric(p,s)` via `ir.Decimal.Unconstrained`; collapsing the absent precision to `(0,0)` (the pre-fix behaviour) was catalog Bug 69 — `NUMERIC(0,0)` on a PG target (hard fail, `SQLSTATE 22023`) and `DECIMAL(0,0)` on a MySQL target (**silent decimal-precision data loss**: `3.14159` → `3`, exit 0, no warning at any level).

**PG-target policy (PG → PG):** emit bare `NUMERIC` (no parentheses) — PostgreSQL's native arbitrary-precision form. The value round-trips byte-identically; there is no narrowing and no advisory. Constrained `numeric(p,s)` is unchanged.

**MySQL-target policy (PG → MySQL — deliberate widening, read this):** MySQL has no unbounded `DECIMAL`. An unconstrained `numeric` maps **uniformly to `DECIMAL(65,30)`** — MySQL's documented maximum precision (65) and scale (30), the widest representable fixed-point form. This is an intentional, documented cross-engine policy, mirroring the `bigint unsigned` precedent above:

- **It preserves far more than the alternatives.** `DECIMAL(65,30)` keeps up to 35 integer digits and 30 fractional digits. The pre-fix `DECIMAL(0,0)` silently truncated every fractional value to an integer. A hard refusal would over-block: unconstrained `numeric` is ubiquitous in PG schemas (default for `NUMERIC` columns in countless ORMs and hand-written schemas), so refusing it would block the majority of real PG→MySQL migrations.
- **Any value exceeding 65/30 is astronomically rare and has no wider MySQL home.** There is no MySQL type that stores more — `DECIMAL(65,30)` is the ceiling.
- **The widening is surfaced LOUDLY, never silently.** Both `sluice schema preview` and `sluice migrate` preflight emit a dedicated, operator-actionable **unconstrained-numeric widening notice** that names every affected `table.column`, states the `DECIMAL(65,30)` ceiling, and gives the per-column override. It is an *advisory notice* (the migration proceeds), not a hard refusal, visible at both surfaces (a section in `schema preview` output / JSON `unconstrained_numeric_widenings`, and a `WARN` log line at `migrate` preflight). The loud-failure tenet is satisfied by the loud notice, not by silently narrowing. This is the same renderer/preflight wiring as the unsigned-bigint notice.

**`numeric[]` (array of unconstrained numeric):** on a PG target the array round-trips as `NUMERIC[]` (each element bare `NUMERIC`, lossless, no advisory). On a MySQL target the whole array column lands as `JSON` (the [Postgres ARRAY → MySQL](#postgres-array--mysql) policy) — the values are stored as JSON text with no decimal-precision loss, so the widening advisory does **not** fire for the array case (there is nothing to narrow).

**Override:** for a column that needs a specific precision/scale (to recover storage, or because values exceed 65/30), supply `--type-override TABLE.COL=decimal(N,M)` (or the per-column `mappings:` hook).

### Temporal fractional-second precision (bare vs explicit) — restore-parity TRIAGE #3

The temporal counterpart of the unconstrained-numeric case above. A PG column declared bare `time` / `timestamp` / `timestamptz` (no parenthesized precision, `pg_attribute.atttypmod = -1`) *behaves* as the engine default (6), and `information_schema.columns.datetime_precision` reports 6 for it — so reading that catalog view verbatim materialized `timestamp(6) with time zone` into every target, a catalog-visible divergence the restore-parity oracle flagged. The IR now models the bare form distinctly via `PrecisionUnspecified` on `ir.Time` / `ir.DateTime` / `ir.Timestamp`; the PG reader keys declaredness off `atttypmod` (the ground truth), and the same rule covers temporal ARRAY elements (`timestamptz(3)[]` carries its typmod to the element).

- **PG target:** unspecified emits bare; a known precision always emits explicitly, **including `(0)`** — the pre-fix `0 → bare` emit rule silently widened an explicit `timestamptz(0)` (whole-second rounding semantics) to the 6-behaving bare form. A MySQL-sourced `DATETIME`/`TIME`/`TIMESTAMP` (precision genuinely 0 — see the asymmetry note below) now lands as `timestamp(0)` etc., which is the faithful carry; values are unchanged (MySQL sources never produce fractional seconds on a precision-0 column).
- **MySQL target:** MySQL has no 6-behaving bare form (bare *is* `(0)` and would silently truncate fractional seconds), so unspecified **materializes as the explicit maximum, `(6)`** — same target DDL as before the fix, when the reader materialized 6. `CURRENT_TIMESTAMP` defaults are precision-matched to `(6)` accordingly. Behaviorally identical to the PG source; no advisory fires (nothing narrows).
- **SQLite/D1:** SQLite temporals are TEXT storage with no precision concept; the SQLite reader emits `PrecisionUnspecified` and the SQLite writer ignores precision entirely. SQLite → MySQL therefore now lands `DATETIME(6)` (previously bare `DATETIME`, which truncated fractional seconds in SQLite text values — a silent-loss fix that rides along).
- **Engine asymmetry (deliberate):** MySQL's reader *always* knows the precision — `information_schema` reports 0 for a bare `DATETIME`, and bare ≡ `(0)` in MySQL — so MySQL-sourced temporals never carry `PrecisionUnspecified`. Only engines with a genuine "no declared precision" catalog state (PG's typmod -1, SQLite's affinity typing) produce it.
- **Cross-version:** backups/manifests written by older binaries carry the materialized `Precision: 6`; they decode as an explicit `(6)` and restore byte-identically to the old behaviour. The wire flag is append-only (`temporal_precision_unspecified`), no backup-format bump.

### UNIQUE constraints vs unique indexes — restore-parity TRIAGE #4

PostgreSQL distinguishes a table-level `CONSTRAINT x UNIQUE (cols)` (a `pg_constraint` row owning a backing index) from a plain `CREATE UNIQUE INDEX`. They are near-equivalent for enforcement, but only the constraint form supports `INSERT ... ON CONFLICT ON CONSTRAINT x` and dumps as `ADD CONSTRAINT` — demoting one to a bare index (the pre-fix behaviour) broke that surface and the catalog diff. The IR carries the distinction via `ir.Index.ConstraintBacked`; the PG writer re-emits constraints in the constraints phase (all unique constraints before any FK, since FKs may reference them; `INCLUDE` payload columns carry) and plain unique indexes in the index phase. The Bug 125 keyless-table path is unchanged: a PK-less table's promoted COPY key is emitted inline as a `CONSTRAINT ... UNIQUE` at CREATE TABLE time and skipped by both later phases.

**Engine asymmetry (deliberate):** MySQL has no such distinction — a MySQL `UNIQUE KEY` *is* an index under the hood, so MySQL-sourced unique indexes never carry `ConstraintBacked`, and a PG-sourced unique CONSTRAINT lands on a MySQL (or SQLite) target as a unique index — the only shape those engines have. Round-tripping PG → MySQL → PG therefore lands the constraint as a plain unique index; only same-engine PG (and PG backups) preserve constraint identity.

### MySQL ENUM and SET → Postgres

**ENUM default:** Postgres `enum` type. Faster, more space-efficient, but rigid (adding a value requires `ALTER TYPE`).

**ENUM override:** `text` plus a `CHECK` constraint listing valid values.

**SET default:** `text[]` plus a `CHECK` constraint that every element is in the allowed set.

**SET override:** `text` with a comma-delimited representation, preserving MySQL's storage shape but losing structured query support.

### Postgres ENUM → MySQL

MySQL `ENUM` is roughly equivalent in shape but is not a separate type — it is a column-level definition. The translator produces a column-level `ENUM(...)`. If the Postgres enum is used by multiple columns, each column gets its own MySQL `ENUM` declaration (no shared type).

### Postgres ARRAY → MySQL

MySQL has no array type.

**Default policy:** emit as `JSON`. Reads and writes serialise/deserialise transparently in the row pipeline — a PG array value (`text[]`/`int[]`, incl. empty `{}`→`[]`, NULL whole-column→SQL NULL, NULL element→JSON `null`, nested→nested) is serialised to JSON text on all three MySQL write paths (LOAD DATA, batched INSERT, CDC apply) via the shared `prepareValue` chokepoint (v0.69.0 / Bug #18 — earlier releases crashed the LOAD DATA serializer on a non-empty array column).

**Override:** `array_strategy: json | concat`. The `concat` option is offered for simple scalar arrays where a comma-delimited string is preferable.

### JSON semantics

MySQL `JSON` and Postgres `jsonb` both validate and normalise on insert. MySQL `JSON` and Postgres `json` (no `b`) preserve original whitespace and key order.

**Default mapping:** MySQL `JSON` → `JSON{Binary: true}`, Postgres `jsonb` → `JSON{Binary: true}`, Postgres `json` → `JSON{Binary: false}`.

**Default emission:** `JSON{Binary: true}` → MySQL `JSON` / Postgres `jsonb`. `JSON{Binary: false}` → MySQL `JSON` (no distinction available) / Postgres `json`.

### Postgres large objects (`pg_largeobject` / `oid` / `lo`)

Postgres large objects are server-side blobs stored in the `pg_largeobject` catalog, referenced from user tables by an `oid` value (or the `lo` extension's domain over `oid`). Because the contents live outside every user table, **sluice does not copy them**: the referencing `oid`/`lo` column values copy faithfully as plain integers, but on the target they point at nothing. A migrate or sync from a source whose `pg_largeobject_metadata` is non-empty emits an advisory WARN (roadmap item 68c) — naming the in-scope `oid`/`lo`-typed columns when any exist, or a quieter one-liner when nothing in scope appears to reference the blobs. It is a WARN, never a refusal: an `oid` column is also a perfectly ordinary integer-ish column, and only the operator knows whether the referenced blobs matter. To carry the data: export the blobs separately (`lo_export`, or `pg_dump --blobs` restored with the native tools), or convert the columns to inline `bytea` on the source first (`ALTER TABLE t ALTER COLUMN c TYPE bytea USING lo_get(c)`) so sluice copies the bytes like any other column. The probe is advisory — if the census queries fail (managed-platform permission variance), the run proceeds without the WARN.

### Charsets and collations

Charset and collation are stored on `Char`, `Varchar`, and `Text`. The readers record the source values verbatim, each in its own convention: the MySQL reader records `character_set_name` + `collation_name` for every string column (so a MySQL-dialect collation always arrives charset-paired), the Postgres reader records `pg_collation.collname` only when the column carries an *explicit non-default* collation and never records a charset (PG has no per-column charset — `server_encoding` is database-wide), and the SQLite reader records neither. That charset-pairing is how the writers infer which dialect an untagged collation name belongs to (`translate.CollationDialect`).

**Same-engine: carried.** MySQL → MySQL re-emits `CHARACTER SET` / `COLLATE` on the column type. PG → PG re-emits an explicit column collation as `COLLATE "<name>"` (bare `pg_collation.collname`, quoted; mirrors pg_dump's rule of omitting the database default, so ordinary columns stay clause-free). Limitation: the PG reader does not carry the collation's namespace, so an operator-created collation outside the target's `search_path` fails loudly at CREATE TABLE (SQLSTATE 42704) rather than resolving — sluice does not carry `CREATE COLLATION` objects. Array element collations (`text[] COLLATE ...`) are not carried by the reader.

**Cross-engine: dropped with a WARN — no name translation.** Collation names are not portable (MySQL's `utf8mb4_0900_ai_ci` means nothing to PG; PG's `"C"` means nothing to MySQL) and sluice does not guess at semantic equivalents. The target column lands with the target's default collation, and the writer WARNs naming the table and the dropped column collations — once per table on the CREATE TABLE path (a MySQL source tags *every* string column, usually with the database default, so per-column WARNs would flood), once per column on the schema-forward ALTER paths. This applies in all cross-engine directions (MySQL ↔ PG, and either → SQLite, whose `BINARY`/`NOCASE` namespace shares no names with anyone). Note the semantic shift this can imply — e.g. MySQL's default collations are case-insensitive while PG's libc/ICU defaults are case-sensitive; operators who need source-identical comparison semantics must set up the target collation themselves.

**Cross-family within the MySQL dialect (MySQL ↔ MariaDB): remapped with a WARN.** The one place sluice does translate collation names is between the two MySQL-dialect server families (roadmap item 73 / ADR-0168), because here a close, same-shape equivalent exists and the alternative is failing every default-collation schema: a MySQL 8 source's `utf8mb4_0900_{ai_ci,as_ci,as_cs}` land on a MariaDB target as `utf8mb4_uca1400_*` (valid on both supported LTS lines, 10.11 and 11.4 — 11.4's own 0900 aliases resolve to the uca1400 set) and `utf8mb4_0900_bin` as `utf8mb4_nopad_bin`; a MariaDB source's uca1400 collations map the mirror direction onto a MySQL-family target. The swap is surfaced with the same WARN cadence as the drop policy (per table on CREATE, per column on ALTER), naming each `from → to` pair; the known deltas are UCA version weights (9.0.0 vs 14.0.0, edge-case characters only) and PAD semantics. Language-specific variants (`utf8mb4_de_pb_0900_ai_ci`, uca1400 language forms) are deliberately not mapped — they pass through verbatim and fail loudly on a target that lacks them rather than guess a language table.

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

- **`LOWER('lit')` / `UPPER('lit')` over a bare string literal** (v0.69.0 / Bug #20 — operator-visible design call). MySQL accepts `LOWER('ABC')` in a STORED generated column / CHECK; PG rejects it with `SQLSTATE 42P22` ("could not determine which collation") because an unadorned string literal has the `unknown` type and no collation. **Two cases, by position:**
  - **CHECK / DEFAULT** — sluice wraps the sole string-literal argument in an explicit `::text` cast (`lower('ABC'::text)`), faithful (byte-identical to MySQL's result) and accepted by PG. A column reference (already collatable) and a compound/already-cast argument pass through verbatim.
  - **GENERATED column** — every migrated generated column is STORED on PG, and a STORED generated column's expression must have a *determinable* collation that even the `::text` cast does not supply; a synthesised `COLLATE` would change Unicode case-folding semantics vs MySQL. The value is a constant, so sluice **loud-refuses at `schema preview` and `migrate` pre-flight** (before any DDL) naming the site and the `--expr-override TABLE.COL=<already-lowered-literal>` remedy — the loud-failure-tenet choice over a fragile/unfaithful cast or a raw mid-pipeline 42P22 + partial target.

- **Known pre-existing limitation — `CAST(x AS BINARY(n))` in a generated column → PG `42804`** (Bug #22, pre-existing on v0.68.x; *not* introduced by the v0.69.0 batch — the #16 fix merely stopped masking it behind a spurious refusal). MySQL's `BINARY(n)` cast target maps to PG `bytea` while the surrounding generated-column type resolves to a text/varchar form, and PG rejects the type mismatch. Loud failure (exit 1, no corruption). Tracked as a separate type-mapping item; the `--expr-override` / `--type-override` / `--exclude-table` escape hatches apply. Not a v0.69.0 release blocker.

For MySQL → PG migrations, run `sluice schema preview` first — its translator-gap preflight scan (v0.39.0, see [ADR-0024](adr/adr-0024-schema-preview.md)) lists every expression pattern sluice does NOT auto-rewrite, with operator-actionable workaround hints.

### Untranslatable-expression backstop (allowlist gate, v0.68.3 / Bug #14)

The translator's policy is "anything not matched falls through verbatim" (loud-failure tenet: a PG parse error beats a silent wrong translation). To make that loud failure happen **before any DDL is applied** rather than mid-`migrate` (which leaves a partial target) and to stop `schema preview` from being a false green, a post-translation **PG-validity gate** runs at both `schema preview` and `migrate` pre-flight:

- **It is an allowlist, not a denylist.** v0.68.1 shipped a *curated denylist* of known MySQL-only patterns; that is structurally insufficient — any MySQL-only construct outside the curated set still leaked verbatim. v0.68.3 flips it: every function-call identifier in a `DEFAULT`/`GENERATED`/`CHECK` body must be **provably PG-valid** — a MySQL function the translator provably rewrites, a PG core/built-in (or one of the exact forms the translator emits), or a function owned by an `--enable-pg-extension`-enabled extension. Anything else is a loud, operator-actionable refusal naming the site + the `--expr-override` remedy.
- **False-positive safety is the load-bearing design constraint.** A *missed* detection degrades to the pre-existing late-migrate parse error (no worse than status quo); a *false-positive* that refuses a valid schema is the real hazard. So only a bare unrecognized function-call identifier trips it — string literals, column refs, operators, the catalogued translations, arithmetic, SQL keyword-forms, and qualified `schema.fn(...)` calls never do. `--expr-override` (which retags the expression off the `mysql` dialect) suppresses the gate for that expression.
- The curated `ScanMySQLToPGGaps` layer is retained as a *construct-specific actionable-hint* layer on top of (not replaced by) the general gate.
- **Cast-target type specifiers are exempt (v0.69.0 / Bug #16 — operator-visible design call).** A parameterized type in CAST/`::` target position — `CAST(x AS DECIMAL(10,2))`, `CAST(x AS CHAR(20))`, `CAST(x AS BINARY(16))`, `x::numeric(20,0)` — is SQL *grammar*, not a function call, and PG accepts these natively (or the translator rewrites `CHAR(n)`→`VARCHAR(n)`). v0.68.3's gate misread the parenthesized type as an unknown call and spuriously refused valid schemas; the fix is **context-aware**: a recognised SQL type name is exempt **only** in cast-target position (immediately after `AS`, or after `::`). The same spelling used call-shaped elsewhere — notably MySQL's `CHAR(65)` *scalar* function, which has no PG form and the translator does not rewrite — is still refused (a blanket type-name allowlist would re-open that false-green). `UNSIGNED`/`SIGNED` cast targets remain refused (no PG spelling).

### Gen-col-references-gen-col is refused up front (v0.69.0 / Bug #9 — operator-visible policy)

MySQL permits a generated column's expression to reference *another* generated column in the same table; PostgreSQL forbids it (`SQLSTATE 42P17`). Left unchecked, PG rejects the `CREATE TABLE` mid-`migrate`, after other tables already migrated (a partial target). sluice now refuses this at **both `schema preview` and `migrate` pre-flight**, before any DDL, naming each site and the remedy: inline the referenced column's own generation expression via `--expr-override TABLE.COL=<inlined-expr>` (or `--exclude-table`). This is the loud-failure tenet applied to a UX-quality gap — no silent corruption either way, but the clean up-front refusal replaces PG's opaque mid-pipeline error.

## What the IR is not

The IR is not a wire format. It is not stable. It is not for users. It is an internal Go data model whose only job is to make the four-direction matrix tractable and the type-translation code testable. We will not export it, version it, or guarantee compatibility across releases.

If at some point external tooling wants to integrate, that's a separate decision about a stable schema export format — likely something like a JSON schema descriptor — rather than exposing the IR itself.
