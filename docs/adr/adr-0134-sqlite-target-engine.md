# ADR-0134: SQLite target engine (SchemaWriter + RowWriter)

## Status

**Accepted (prototype) (2026-06-27).** Roadmap item 49 follow-up (#4 of the SQLite
queue). The `sqlite` engine — until now a migrate SOURCE only (ADR-0128/0129/0130,
`OpenSchemaWriter`/`OpenRowWriter` returned `ErrNotImplemented`) — gains a write side:
`internal/engines/sqlite/schema_writer.go` + `row_writer.go` implement `ir.SchemaWriter`
and `ir.RowWriter`, so `sluice migrate --target-driver sqlite --target ./out.db` works
from ANY source (Postgres, MySQL, or another SQLite/D1). This enables the X→SQLite→D1
flow via `wrangler d1 import out.db`. CDC / `ChangeApplier` stay `ErrNotImplemented`
(SQLite has no target CDC in this prototype, `Capabilities.CDC = CDCNone`). The write
side is the faithful INVERSE of the existing reader: an emitted target round-trips back
through `resolveColumnType` + the ADR-0129 date/bool decode to the same IR.

## Context

The reader (ADR-0128) maps each SQLite column's declared-type AFFINITY to an IR type and
decodes each row by its actual storage class, refusing storage-class mismatches loudly.
The writer must be its mirror image: pick declared types that the reader reads BACK to
the same IR type, and encode each value so a re-read recovers it. Two SQLite realities
make this non-trivial, and both are load-bearing decisions below:

1. **SQLite is dynamically typed and applies COLUMN AFFINITY at INSERT time.** A value
   bound to a `NUMERIC`/`DECIMAL`-affinity column is silently coerced to INTEGER or REAL
   (text→number) — empirically confirmed: `"123.45"` → REAL `123.45`,
   `"12345678901234567890.12345"` → REAL `1.2345678901234567e+19` (15-sig-digit
   precision LOSS). There is NO affinity that both (a) preserves a decimal value as
   exact text AND (b) reads back as `ir.Decimal`. This is the value-fidelity crux.

2. **SQLite cannot `ALTER TABLE ADD` a FOREIGN KEY or CHECK constraint after table
   creation.** The pipeline's `CreateTablesWithoutConstraints → copy → CreateIndexes →
   CreateConstraints` model assumes constraints can be deferred and added last; on SQLite
   they CANNOT be added at all post-hoc. The phase contract has to be re-mapped without
   forking the orchestrator.

## Decision

### 1. Type mapping IR → SQLite (round-trip faithful through the reader)

Emit DECLARED types whose affinity (and the ADR-0129 declared-temporal/bool override)
read back to the SAME IR type via the reader's `resolveColumnType`:

| IR type | Emitted SQLite declared type | Reader reads back as | Notes |
|---|---|---|---|
| `Boolean` | `BOOLEAN` | `Boolean` (declared-bool match) | value stored 0/1 INTEGER |
| `Integer` (any width/sign) | `INTEGER` | `Integer{Width:64}` | SQLite ints are 64-bit signed; width/unsigned NOT preserved (SQLite has neither) |
| `Float` | `REAL` | `Float{double}` | |
| `Decimal` | `TEXT` (Bug 162 amendment) | `Text` | NUMERIC affinity silently coerces money to REAL — see the Bug 162 amendment below; TEXT stores the exact decimal string, reading back as `ir.Text` |
| `Char` / `Varchar` / `Text` | `TEXT` | `Text{long}` | SQLite does not enforce length; Char/Varchar widen to Text on a SQLite round-trip |
| `Binary` / `Varbinary` / `Blob` | `BLOB` | `Blob{long}` | Binary/Varbinary widen to Blob |
| `Date` | `DATE` | `Date` | value = ISO `YYYY-MM-DD` TEXT |
| `Time` | `TIME` | `Time` | value = ISO time-of-day TEXT |
| `DateTime` | `DATETIME` | `Timestamp` (no tz) | value = ISO `YYYY-MM-DD HH:MM:SS[.fff]` TEXT |
| `Timestamp` (tz-naive) | `DATETIME` | `Timestamp` (no tz) | UTC ISO TEXT |
| `Timestamp` (tz-aware) | `DATETIME` | `Timestamp` (no tz) | **see tz wart below** |
| `JSON` | `TEXT` | `Text{long}` | **see JSON divergence below**; value = raw JSON text |
| `UUID` | `TEXT` | `Text{long}` | value = canonical hyphenated string |
| `Enum` / `Set` | `TEXT` | `Text{long}` | enum value / `Set` joined with `,` as text |

Anything NOT faithfully representable is **refused LOUDLY at schema-write** (naming
table.column and the IR type), never silently coerced — mirroring the reader's
refuse-not-coerce tenet. Refused at emit: `ir.Geometry`, `ir.Inet`/`Cidr`/`Macaddr`,
`ir.Bit`/`Varbit`, `ir.Interval`, `ir.Array`, and any unknown/verbatim extension type —
SQLite has no faithful storage and these would otherwise land as silently-wrong text.

**tz-aware timestamp wart (named, documented — NOT silent).** SQLite is tz-naive; it has
no `timestamptz`. A tz-aware source `Timestamp{WithTimeZone:true}` arrives on the value
path as a `time.Time` ALREADY normalized to UTC (the value contract,
`docs/value-types.md`). The writer stores it as UTC ISO-8601 TEXT in a `DATETIME` column:
the INSTANT is preserved EXACTLY; what is dropped is the original display-zone metadata,
which SQLite fundamentally cannot hold. This is instant-faithful (no silent corruption of
the moment in time), and the loss is named here and tested. The reader reads it back as
tz-naive `Timestamp`. (A tz-aware `Time`/`timetz` has no date to anchor a UTC instant —
its value text is carried verbatim; a re-read under the default `iso` encoding may
loud-refuse a zone-suffixed time-of-day. timetz→SQLite is a documented edge, not a
shipping-path concern.)

**Decimal type vs value — SUPERSEDED by the Bug 162 amendment below.** The original design
emitted NUMERIC affinity to round-trip the `ir.Decimal` type; that silently corrupted money
to REAL. Decimals now emit TEXT affinity (exact value, type downgrades to `ir.Text`). See
the Bug 162 amendment.

### 2. Value encode (inverse of `value_decode.go`), refuse-not-coerce

Per the value contract (`docs/value-types.md`), each `ir.Row` value encodes as:

| Go value (IR type) | SQLite binding | Storage class |
|---|---|---|
| `bool` (`Boolean`) | int64 0/1 | INTEGER |
| `int64`/`uint64` (`Integer`) | int64 (uint64 > MaxInt64 refused loudly) | INTEGER |
| `float64` (`Float`) | float64 | REAL |
| `string` (`Char`/`Varchar`/`Text`/`UUID`/`Enum`/`JSON-as-text`) | string | TEXT |
| `[]byte` (`Blob`/`Binary`/`Varbinary`/`JSON`) | []byte / string | BLOB / TEXT |
| `string` (`Decimal`) | string, **guarded** | INTEGER or REAL (coerced) |
| `time.Time` (`Date`) | `YYYY-MM-DD` TEXT | TEXT |
| `time.Time` (`Timestamp`/`DateTime`) | UTC `YYYY-MM-DD HH:MM:SS[.fffffffff]` TEXT | TEXT |
| `string` (`Time`) | time-of-day TEXT verbatim | TEXT |
| `[]string` (`Set`) | joined `,` TEXT | TEXT |
| `nil` (any) | NULL | NULL |

The write side ALWAYS writes canonical ISO for temporal values (the `iso` read encoding,
so a re-read recovers them); `--sqlite-date-encoding` is a READ concern for arbitrary
existing DBs, not a write knob. A value that cannot be faithfully written is refused
loudly naming the row's table.column.

**★ AMENDMENT (2026-06-27, Bug 162 — the original decimal decision was a CRITICAL silent
corruption; corrected to TEXT affinity.)** The first cut declared decimals with NUMERIC/
`DECIMAL(p,s)` affinity (to round-trip the `ir.Decimal` *type*) and guarded the value with
a significant-digit count (`refuse-decimal-beyond-float64`). That was WRONG: SQLite's
NUMERIC affinity stores an ordinary money value like `19.99` as a binary **REAL** on disk
(ground-truthed: `typeof` = `real` for `19.99`, `5.10`, `98765432.1098`, `0.1`), and the
significant-digit guard misses it because float64-loss is about *dyadic representability*,
not digit count — `19.99` has 4 digits yet is not float64-exact. The `.db` is the entire
deliverable (X→SQLite→D1), so the on-disk artifact handed to D1 holds money as binary
floats, and a re-migrate of the REAL back into a constrained target `NUMERIC` can fail
loudly (`98765432.1098` scans back as `9.87654321098e+07`). The §4 value-fidelity review
missed this because it read `19.99` back through SQLite's own 15-digit text conversion
(which shows `19.99`) rather than asserting the on-disk storage class / a byte-exact
round-trip; the post-release regression cycle on the real binary caught it. This is the
Bug-74 lesson again: pin the on-disk/real-path behavior, not a representative read.

**Corrected decision — decimals are emitted with TEXT affinity (declared `TEXT`).** SQLite
stores text verbatim with no numeric coercion, so a decimal of ANY precision round-trips
**byte-exact** (`19.99` → `19.99`, `100.00` → `100.00`, `0.30` → `0.30`,
`12345678901234567890.1234567890` → exact). The value is bound as its exact string by
`encodeDecimal` (no guard, no refusal — TEXT preserves everything). The cost is a
**documented type downgrade**: the column reads back as `ir.Text` rather than
`ir.Decimal` — the same value-faithful trade as `JSON`/`UUID`→`TEXT`, and the correct one,
since a silent VALUE corruption is never acceptable to preserve a TYPE label (and SQLite/D1
are dynamically typed, so a decimal-as-text is a perfectly faithful decimal value). Pinned
by an explicit writer→DB→**schema-resolved-reader** round-trip test (`TestWriterDecimalTextExact`)
that asserts BYTE-exact equality through the real read path — the gap the original review left.

This refuse rule diverges from the seeded ADR phrasing "DECIMAL stored as TEXT to preserve precision" —
that is empirically IMPOSSIBLE under NUMERIC affinity (text decimals are coerced to
REAL/INTEGER on insert). The 15-digit bound is the safe `DBL_DIG` floor: a 16–17-digit
decimal that would happen to survive is refused too (loud, safe side), and the operator's
escape hatch is to carry the column as `TEXT` (it round-trips byte-exact as `ir.Text`).

### 3. The SQLite ALTER-TABLE limitation (the named structural wart)

SQLite cannot add a FOREIGN KEY or CHECK after `CREATE TABLE`, so the three-phase
SchemaWriter contract is re-mapped — NOT forked in the orchestrator:

- **`CreateTablesWithoutConstraints` emits the FULL table DDL inline** — columns, NOT
  NULL, DEFAULT, generated columns, PRIMARY KEY, UNIQUE, **CHECK, and FOREIGN KEY** —
  because SQLite can't add the constraint-y parts later. The method whose name says
  "WithoutConstraints" deliberately INCLUDES them for SQLite; this is documented in the
  code with the reason.
- **`CreateIndexes` creates secondary indexes** (`CREATE INDEX [UNIQUE] ... [WHERE pred]`
  works post-hoc; partial/expression indexes carry their predicate/expression verbatim).
- **`CreateConstraints` is a FK-integrity VERIFICATION pass, not an emit pass.** Inline
  FKs are already present, so there is nothing to add; instead it runs
  `PRAGMA foreign_key_check` and refuses LOUDLY (naming the violating table/rowid/parent)
  if the loaded data violates any FK — the loud-failure surface that replaces PG's
  validating `ADD CONSTRAINT`.

**FK-off bulk load.** SQLite enforces FKs per-connection via `PRAGMA foreign_keys`
(default OFF). To make the row-copy phase order-independent despite inline FKs, every
writer connection (schema + row) opens with `_pragma=foreign_keys(0)` so a child row can
land before its parent during the unordered copy. After the copy,
`CreateConstraints`'s `PRAGMA foreign_key_check` surfaces any genuine violation loudly on
a fresh check of the whole database file. (Setting it explicitly off also guards against
a future driver default flip — the reader's pragmas are set the same defensive way.)

### 4. `SyncIdentitySequences` is a no-op (verified)

Empirically confirmed: a plain `INTEGER PRIMARY KEY` (rowid alias) auto-continues from
`max(rowid)+1` on the next NULL insert (after explicit ids 5,10 the next is 11), and
`AUTOINCREMENT` updates `sqlite_sequence` on explicit-value insert (seq=10 → next 11). So
SQLite needs no post-copy sequence bump — like MySQL InnoDB. `SyncIdentitySequences`
returns nil (documented).

### 5. `CreateViews`

SQLite supports `CREATE VIEW IF NOT EXISTS` (idempotent for resume). The view body emits
VERBATIM (same verbatim philosophy as the schema-feature carry); a non-portable
cross-dialect body fails LOUDLY at `CREATE VIEW` time, never silently dropped. SQLite has
**no materialized views** → a `View.Materialized` entry is refused LOUDLY (it cannot be
represented).

### 6. Capabilities

`BulkLoad` flips from `BulkLoadNone` to `BulkLoadBatchedInsert` so the engine declares a
usable target load path (SQLite's fast path is a multi-row parameterised INSERT inside
one transaction — there is no COPY/LOAD DATA). `CDC = CDCNone` and `SchemaScope = Flat`
are unchanged. The remaining fields (no extension types, CHECK + generated columns
supported, JSON/Enum None) already describe SQLite honestly for both directions.

## Consequences

- `migrate` into a SQLite file works from PG, MySQL, and SQLite/D1 sources; the produced
  `.db` is a valid SQLite database ready for `wrangler d1 import`.
- Round-trip fidelity is proven by SQLite→PG→SQLite and SQLite→MySQL→SQLite value-identity
  tests (the writer is the faithful inverse of the reader) plus PG→SQLite / MySQL→SQLite
  value-contract tests read back through the existing SQLite reader.
- Lossy edges are LOUD, not silent: decimals beyond float64's exact range, uint64 > int64,
  and unrepresentable types (geometry/inet/bit/interval/materialized view) are refused at
  emit/write time naming the offending object; tz-aware timestamps are instant-faithful
  (UTC ISO) with the display-zone drop documented and tested.
- `Char`/`Varchar`→`Text`, `Binary`/`Varbinary`→`Blob`, `JSON`/`UUID`→`Text`, and
  `Timestamp`(tz-aware)→tz-naive `Timestamp` are documented TYPE widenings on a SQLite
  round-trip; VALUES are preserved.

## Alternatives considered

- **Store decimals in a TEXT-affinity column to preserve them byte-exact.** Rejected as
  the default: the reader then resolves the column to `ir.Text`, not `ir.Decimal`, so the
  type no longer round-trips. The loud refusal (with the `--type-override=text` escape)
  keeps the common case (≤15-sig-digit / money decimals) faithful AND typed, and makes
  the rare lossy case explicit instead of silent. (TEXT carry remains the operator's
  documented escape hatch.)
- **Emit `JSON` as a `JSON`-spelled declared type.** Rejected: the reader has no JSON
  resolution — a `JSON`-declared column resolves to NUMERIC affinity → `ir.Decimal`, which
  then REFUSES the JSON-object text on read-back. Emitting `TEXT` preserves the JSON value
  exactly and reads back cleanly as `ir.Text` (SQLite's honest "no native JSON type",
  `JSONSupport = None`). Documented divergence from the seeded ADR's `ir.JSON→JSON`.
- **Toggle `PRAGMA foreign_keys` ON during load and order the copy topologically.**
  Rejected: SQLite's default is already OFF and the pipeline's copy order is not
  topologically sorted; the FK-off load + final `foreign_key_check` is simpler, matches
  the deferred-constraint spirit, and still surfaces real violations loudly.
- **Fork the orchestrator to special-case SQLite's no-ALTER constraints.** Rejected per
  the engine-neutral tenet: the three SchemaWriter phases are re-mapped INSIDE the SQLite
  writer (full DDL inline / indexes / FK-verify) so `pipeline.Migrator` is reused
  unchanged.
