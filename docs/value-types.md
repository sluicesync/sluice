# Row Value Contract

This document defines the runtime types that flow through [`ir.Row`](../internal/ir/change.go) — the dialect-neutral representation of a single row of data. While [docs/type-mapping.md](type-mapping.md) describes how engine-specific *DDL* types translate to and from the IR's `Type` hierarchy, this document describes the Go *values* that live in `Row` for each IR type, end to end through the read → translate → write pipeline.

The contract is normative: every engine reader MUST produce values matching this table; every engine writer MUST accept values matching this table. The translator may rely on it.

## The `Row` type

```go
type Row map[string]any
```

Rows are keyed by column name. Values are stored as `any` because the type set is small but heterogeneous; a typed sum would add ceremony without practical benefit. The contract below removes the looseness `any` would otherwise imply.

## NULL handling

SQL `NULL` is represented by a Go `nil` value:

```go
row["nullable_col"] == nil   // true when the column was NULL in the source
```

This applies to every column type — there is no distinction between "Integer NULL" and "String NULL"; both are stored as a plain `nil` interface value. Engine readers must never store a typed nil pointer (`(*int64)(nil)`, `[]byte(nil)`, etc.) — only an untyped `nil`. Engine writers must treat any `nil` value as SQL `NULL` regardless of the column's IR type.

Nullability itself is a property of the IR `Column` (`Column.Nullable`), not of the value. A non-nullable column whose `Row` value is `nil` is an error the writer should reject; readers will not produce such values from well-formed source data.

## Core IR types

| IR type | Go value type in `Row` | Notes |
|---|---|---|
| `Boolean` | `bool` | |
| `Integer` (signed, any width) | `int64` | Width is preserved on `Column.Type`, not on the value. |
| `Integer` (unsigned ≤ `MaxInt64`) | `int64` | Numeric value is unambiguous; signedness lives on `Column.Type`. |
| `Integer` (unsigned, may exceed `MaxInt64`) | `uint64` | Engine readers should choose `uint64` for unsigned columns whose values may exceed `MaxInt64` (typically `BIGINT UNSIGNED`). |
| `Decimal` | `string` | Textual representation preserves precision losslessly. Avoid `float64` round-trips. |
| `Float` (single or double) | `float64` | Single-precision values are widened to `float64` — no information loss in this direction. |
| `Char`, `Varchar`, `Text` | `string` | Charset / collation are properties of the column, not the value. The bytes are interpreted in that charset to produce the Go string (engine readers handle the conversion). |
| `Binary`, `Varbinary`, `Blob` | `[]byte` | See [memory ownership](#memory-ownership-of-byte-slices) below. |
| `Bit` | `string` | Fixed-width `'0'`/`'1'` bit-string (e.g. `"00001010"` for a `BIT(8)` value of 10). MySQL `BIT` and PG `bit(n)`/`varbit` both land in this canonical form; its bytewise order reproduces the server's `ORDER BY`, which the keyset-chunk clip fallback (`migcore.fallbackClipOrderUnsafeColumn`) relies on. |
| `Date` | `time.Time` | Time portion is `00:00:00`, location is `UTC`. |
| `Time` | `string` | A textual representation such as `"08:30:00"` or `"08:30:00.123456"`. Go's `time.Duration` is *not* used because some SQL `TIME` values fall outside its valid range. |
| `DateTime` | `time.Time` | Location is `UTC` for transport. The semantics of "no time zone" are recorded on `Column.Type`, not on the value. |
| `Timestamp` | `time.Time` | Always `UTC` regardless of the source's session timezone. Engine readers handle the conversion. |
| `JSON` | `[]byte` | Raw JSON bytes. Whether the engine validated/normalised the bytes is recorded on `Column.Type` (`JSON{Binary: true}` for a parsed/normalised representation, `JSON{Binary: false}` for textual). |

**Float NaN ORDERS with Postgres's total order in the `--where` evaluator.** The client-side row-predicate comparator (`internal/rowpredicate`) orders a `float64` NaN the way PG's float comparison does — NaN greater than every non-NaN, NaN equal to NaN — so a NaN row selected by an ordering predicate on the snapshot leg keeps matching on the CDC leg (audit 2026-07-23 D0-6 / Q4). PG is the only supported source that can deliver a float NaN (MySQL/MariaDB cannot store one; SQLite stores NaN as NULL), so the total order is applied engine-neutrally. Relatedly, **temporal literals in `--where` follow the SOURCE engine's own coercion of a finer-than-column literal** — PG casts to the column's type (DATE truncates the time-of-day; fractional seconds round to µs by PG's double-mediated `rint(strtod·10⁶)` rule, reproduced bit-for-bit — nominally half-even but decided by where the binary double lands), MySQL promotes the DATE column and rounds half-up, MariaDB promotes and truncates — normalized at predicate compile via `ir.TemporalLiteralSemantics` and ground-truthed per engine by `internal/rowpredicate`'s temporal real-server matrix (observed 2026-07-23 on PG 16.14 / MySQL 8.0.46 / MariaDB 11.8.8).

**Float NaN payload bits are outside the contract.** A Postgres `float8` NaN applied through slot-CDC lands as Go's canonical quiet NaN (`7ff8000000000001`) where the source held PG's (`7ff8000000000000`) — text-identical, invisible to every SQL comparison and to `verify`, detectable only via `float8send` bytes; cold copy and trigger-CDC preserve the source's bits (observed live on Cloud SQL PG 16, 2026-07-16, provider-independent). `-0.0` DOES round-trip exactly through slot-CDC apply (the trigger engine's `-0`→`+0` wart is documented in ADR-0066). If float-bit forensics ever become a stated guarantee, this is the line to revisit.

## Extension IR types

| IR type | Go value type in `Row` | Notes |
|---|---|---|
| `Enum` | `string` | The enum value itself, not its ordinal. |
| `Set` | `[]string` | The currently-selected members, in declaration order. An empty set is a non-nil empty slice (`[]string{}`), distinct from `nil` which would mean SQL `NULL`. |
| `UUID` | `string` | Canonical hyphenated form (`"01234567-89ab-cdef-0123-456789abcdef"`). Lowercase. |
| `Array` | `[]any` | Each element follows the contract for its `Element` IR type. Multidimensional arrays nest. |
| `Geometry` | `[]byte` | Raw WKB (Well-Known Binary). The subtype is recorded on `Column.Type`. |
| `Inet` | `string` | Canonical textual form (`"192.168.1.1"`, `"2001:db8::1"`). |
| `Cidr` | `string` | Canonical textual form (`"192.168.1.0/24"`). |
| `Macaddr` | `string` | Lower-case, colon-separated. Six bytes when `Width` is 6 (`"08:00:2b:01:02:03"`), eight when it is 8 (`"08:00:2b:ff:fe:01:02:03"`). Postgres widens an EUI-48 literal to EUI-64 on input to a `macaddr8` column, so a value read back from such a column always carries eight. |

## Memory ownership of byte slices

Engine readers MUST return byte slices that the caller owns. In particular, the slice MUST NOT alias the database driver's internal scan buffer — those buffers are typically reused across rows, and aliasing would silently corrupt earlier rows once a later row is read.

Concretely: the MySQL reader copies bytes off the driver's `[]byte` before returning them. Other engine readers must do the same. The unit test [`TestDecodeBytesIsCopy`](../internal/engines/mysql/value_decode_test.go) enforces this property for the MySQL engine; equivalent tests should accompany every other engine reader.

## Time zone semantics

| IR type | Stored timezone in `time.Time` | Semantic interpretation |
|---|---|---|
| `Date` | `UTC` | A wall-clock date with no time portion. The `UTC` location is a transport convention, not a meaningful timezone. |
| `Time` (stored as string) | n/a | A wall-clock time of day, no timezone. |
| `DateTime` | `UTC` | A wall-clock date+time with no timezone. The `UTC` location is, again, a transport convention; the value's semantic timezone is "unspecified" and recorded on `Column.Type` (`Timestamp{WithTimeZone: false}`). |
| `Timestamp` | `UTC` | An instant in time. The instant is the same regardless of where the consumer reads it. |

Engine readers must not return `time.Time` values in a non-UTC location. Engine writers must accept any `time.Time` location (calling `.UTC()` is cheap and idempotent), but should write in the engine's expected form: a literal date for `Date`, a UTC datetime literal for `DateTime`, and an instant for `Timestamp`.

### Zero and partial dates (MySQL legacy data)

MySQL under a relaxed `sql_mode` can store dates with no valid calendar value: the all-zero `'0000-00-00'`, a zero month (`'2026-00-15'`), or a zero day (`'2026-06-00'`). These have **no faithful `time.Time` representation** — Go's `time.Date` would normalize a zero component into a neighbouring real date, silently corrupting the value (Vector A).

The MySQL reader therefore reads `Date`/`DateTime`/`Timestamp` columns as their raw text (via `CAST(... AS CHAR)`) so the decode layer sees the literal, and resolves zero/partial dates per the operator's `--zero-date` policy **before** a `time.Time` is ever constructed:

- `error` (default) — refuse loudly, naming the column. The IR never carries a guessed value.
- `null` — emit SQL `NULL` (refused loudly for a `NOT NULL` column).
- `epoch` — emit `1970-01-01` (`1970-01-01 00:00:01 UTC` for date+time types). The one-second offset past midnight is deliberate: MySQL's `TIMESTAMP` floor is `1970-01-01 00:00:01` UTC, so a midnight placeholder is unrepresentable on a MySQL `TIMESTAMP` target and a relaxed-`sql_mode` write would silently coerce it back to the `0000-00-00` zero sentinel. A single sentinel at the floor is representable by every temporal target (and the offset is meaningless on a synthetic placeholder for an invalid date), so the resolution stays target-agnostic in the source reader.

A genuinely out-of-range but **non-zero** date (month 13, Feb 30) is not a zero date; it stays a hard decode error regardless of `--zero-date`, so the flag can never silently rescue malformed data. See [migrating-legacy-mysql.md](operator/migrating-legacy-mysql.md) for the operator-facing flow and its interaction with the write-side `--mysql-sql-mode`.

## Per-engine reader normalisation requirements

Drivers vary in what they return for the same SQL value. Engine readers are responsible for normalising to the contract above. The MySQL reader, for example, must:

- Coerce `int64` → `bool` for `Boolean` columns whose source is `TINYINT(1)`. MySQL maps `BOOL`/`BOOLEAN` to `TINYINT(1)`, so sluice reads a `TINYINT(1)` column as a boolean — the right call for the overwhelmingly common case. But a `TINYINT(1)` is only a *display width*: the column physically stores the full signed 8-bit range, so a legacy column used as a real small integer can hold values outside `{0,1}` (2, 127, -1, …), and the boolean convention collapses every non-zero value to `true`, silently losing the integer. **The reader now REFUSES loudly** — a coded [`SLUICE-E-VALUE-TINYINT1-RANGE`](operator/error-codes.md) refusal — at the **first** such value on every read path (the bulk-copy / snapshot reader, the binlog CDC reader, the VStream CDC + cold-start path, and the mydumper flat-file source). The refusal fires *before* that row is written, so no collapsed value ever reaches the target; the rows already copied held only genuine `0`/`1` and are correct. **Earlier releases only WARNed here and carried the collapsed bool** — a field report hit that as silent data loss on a legacy column holding `0..6`, which is why it is now a hard refusal. The **universal remedy** is to change the source column's type away from `TINYINT(1)` (for example `ALTER TABLE … MODIFY <col> SMALLINT`) so it is no longer read as a boolean — this works on every source. For a **bulk migrate from a non-Vitess MySQL source** you can instead re-run with `--type-override <table>.<col>=smallint` (or `=int`), which re-types the column without touching the source (`smallint` is the safe floor — a `tinyint` override could re-emit a `TINYINT(1)` target that re-triggers the mapping on a round-trip). **The `--type-override` escape does not apply to a PlanetScale/Vitess (VStream) source:** the VStream decoder makes the boolean decision from the replication wire's own `column_type`, not from the IR schema, so an override there re-types the target column but the value is still collapsed at read time — on a VStream source, change the source column's type. A column that genuinely holds only `0`/`1` never triggers this on any path.
- Coerce `[]byte` → `bool` for `Boolean` columns whose source is `BIT(1)`.
- Coerce `[]byte` → `string` for `Decimal`, `Char`, `Varchar`, `Text`, `Time`, and `Enum` columns.
- Split a comma-separated `[]byte` into `[]string` for `Set` columns.
- Copy `[]byte` values off the driver's scan buffer.
- Pass `time.Time` values through (with `parseTime=true` set in the DSN).

The Postgres reader and the SQLite / Cloudflare D1 readers have their own driver quirks; each is documented at the engine package level and tested with a parallel set of unit tests. The SQLite/D1 value contract has one non-obvious wrinkle worth calling out here (next section).

### SQLite & Cloudflare D1: the `(typeof, text/hex)` value encoding

SQLite has no static per-column value type — every stored value carries its own storage class (`integer` / `real` / `text` / `blob` / `null`), independent of the column's *declared* type. Two problems follow, both of which would corrupt values if read naively:

- **Big integers.** Cloudflare D1's HTTP query API (and `wrangler d1 export`) serialise results through JSON, whose number type is an IEEE-754 float64 — so any integer above 2⁵³ is silently rounded ([ADR-0131](adr/adr-0131-d1-http-api-reader-deferred.md) corrected this empirically: even the export path rounds). A naive `SELECT col` is therefore lossy on large IDs.
- **BLOBs.** JSON can't represent raw bytes at all.

The `d1` reader and the `sqlite-trigger` / `d1-trigger` capture path defeat both by projecting, per column, a pair: `typeof(col)` plus `CASE WHEN typeof(col)='blob' THEN hex(col) ELSE CAST(col AS TEXT) END` (REALs use `format('%!.20g', col)` — SQLite's alternate-form-2 `!` flag, which lifts printf's 16-significant-digit cap to 26 so the 17 digits a binary-64 needs actually emit). The reader reconstructs the `ir.Row` value from that `(storage-class, text-or-hex)` pair through the engine's shared faithful decoder, so integers above 2⁵³ and binary BLOBs round-trip **exactly** ([ADR-0132](adr/adr-0132-d1-query-api-reader.md)). The same encoding is used by the trigger-CDC capture triggers (they must NOT use `json_object()`, which reintroduces the float64 rounding) so cold-start and CDC share one proven seam ([ADR-0135](adr/adr-0135-sqlite-trigger-cdc.md)). *(Corrected 2026-08-22, mechanism re-verified by experiment 2026-08-23: this originally read `format('%.17g', col)` "for shortest-exact round-trip" — true of C printf, but SQLite's printf is NOT C's printf: its `%g`/`%.Ng` conversion CAPS output at 16 significant digits (26 only with the `!` alternate-form-2 flag), and 16 digits don't round-trip every binary-64 — so `%.17g` silently clamped to 16 (`0.30000000000000004` renders as `"0.3"`; `%.16g`–`%.25g` all emit the same 16-digit render). This is longstanding printf behaviour, not a version change. Every REAL through the D1 reader, D1 staging, and both trigger-CDC lanes was silently altered — and a REAL at the float64 max rendered as an out-of-range 16-digit string that killed the stream loudly. Found by the SQLite/D1 adversarial corpus; the per-PR gate is `TestCapturedValueExpr_RealRenderRoundTripsExactly`, the CDC reader refuses at stream start if installed capture triggers still carry the old expression, and since v0.131.4 a render-fidelity probe verifies the connected engine's render round-trips at every stream open.)*

**Decimal read-back from a SQLite target.** When SQLite is the migrate *target*, an `ir.Decimal` is stored with TEXT affinity (the exact decimal string, byte-for-byte) rather than NUMERIC/REAL affinity, because SQLite's NUMERIC affinity silently coerces a value like `19.99` to the binary float `19.989999999999998` (Bug 162). On a later read that column comes back as `ir.Text`, not `ir.Decimal` — a documented type downgrade (the same value-faithful trade as JSON/UUID → TEXT), chosen because silent value loss is never acceptable. See [type-mapping.md](type-mapping.md#sqlite--cloudflare-d1--ir) and [ADR-0134](adr/adr-0134-sqlite-target-engine.md) §2.

## Per-engine writer expectations

Engine writers receive values matching the contract and convert them into a form the target driver accepts. The MySQL writer (when implemented) will, for example:

- Translate `bool` values to `1`/`0` for `TINYINT(1)`-backed `Boolean` columns.
- Pass `string`-typed `Decimal` values directly into prepared statements.
- Pass `time.Time` values directly (with `parseTime=true` and `loc=UTC` set on the connection, the driver round-trips them correctly).

A writer that receives a value not matching the contract (e.g. a `float64` for a `Decimal` column) MUST error rather than coerce silently — the value flowing through has a known canonical form, and a deviation indicates a bug upstream.

## Future considerations

These are deliberate non-goals for the current contract; they may be revisited as the project matures.

- **Arbitrary-precision Decimal type.** A typed `Decimal` value (rather than `string`) would catch parse errors at the read boundary instead of at the write boundary, but adds dependency surface and a serialisation question. The string form is sufficient until a real use case emerges.
- **Native `time.Duration` for `Time`.** Some SQL dialects allow `TIME` values outside `time.Duration`'s range; staying with `string` avoids the encoding problem.
- **Typed JSON.** `[]byte` preserves the source's exact encoding; promoting to a parsed `map[string]any` would lose that and is rarely what a migration tool wants.
- **A typed `Row` rather than `map[string]any`.** Possible eventually; the value contract above is the prerequisite either way.

## CDC-event volume — sizing `--rollover-max-changes`

The CDC reader and `backup stream` both count *change events*, not user-visible row changes. On **every** engine the two counts differ — transaction framing is an event — and operators sizing rollover bounds against expected INSERT counts can under-size the bound by a factor of 3-4×. The MySQL shape is documented first because its inflation is the larger of the two; the Postgres subsection below records that PG is not exempt.

### Per-INSERT shape

A single autocommit `INSERT ... VALUES (one row)` lands in the binlog as **3 events**:

1. `BEGIN` (`QueryEvent`)
2. `WRITE_ROWS_EVENTv2`
3. `XID` / `TxCommit`

A multi-row `INSERT ... VALUES (r1), (r2), ..., (rN)` collapses the row events into one — **2 + N events** total: `BEGIN` + N row events + `XID`. Same shape applies to `UPDATE` and `DELETE` (one row event per row touched, wrapped in BEGIN/XID).

### Spurious empty BEGIN/COMMIT pair

Many MySQL client sessions emit an **empty `BEGIN` / `COMMIT`** pair into the binlog ahead of the first DML in a connection — typically from the driver issuing a session-setup statement (`SET autocommit`, `SET time_zone`, etc.) inside an implicit transaction that gets logged but contains no row changes. The pair is a constant overhead per session, not per row. Operators should budget +2 events for the first DML of any new connection.

### Operator rule of thumb (MySQL)

When setting `--rollover-max-changes=N` on `sluice backup stream` against a MySQL source: **budget at least 4× your expected INSERT count**. The 4× covers the per-row 3-event shape plus headroom for the spurious empty pair and any other session-bookkeeping events (heartbeats, format-description, rotate). For workloads with predictable transaction shapes (e.g. bulk multi-row inserts) the bound can be tighter — the 2 + N shape means a 1000-row multi-row INSERT consumes ~1002 events, not 3000 — but the safe default for naive INSERT-counting is 4×.

### Postgres counts transaction framing too

**Postgres is not exempt from the framing arithmetic.** pgoutput's `Begin` and `Commit` messages are surfaced by sluice's PG CDC reader as `ir.TxBegin` / `ir.TxCommit` changes (`internal/engines/postgres/cdc_reader.go`), and both backup paths count every change they write to a chunk — framing included, with no per-kind filter (`internal/pipeline/incremental.go`, `internal/pipeline/stream.go`). So a one-row autocommit transaction consumes **3** against `--max-changes` / `--rollover-max-changes` on Postgres exactly as it does on MySQL, and an N-row transaction consumes 2 + N. **Budget at least 3× a naive INSERT count on PG.**

What differs between the engines is the *headroom*, not the shape. MySQL's 4× buys slack for binlog-specific extras — the spurious empty `BEGIN`/`COMMIT` pair per session, plus rotate / format-description bookkeeping. The PG reader emits only `TxBegin`, `TxCommit`, `Insert`, `Update`, `Delete`, `Truncate` (and one `SchemaSnapshot` at stream start), so there is no equivalent per-session inflation to budget for — but the 3-per-transaction floor is identical.

*(An earlier version of this section carved Postgres out entirely, on the claim that its transaction boundaries "arrive as separate `Begin`/`Commit` messages but sluice's CDC reader doesn't surface them as countable changes", and told PG operators to size `--rollover-max-changes` off INSERT-count with no multiplier. Both halves were wrong against the code: the reader does surface them, and the window does count them. Corrected 2026-07-28.)*

### Why this matters

Under-sized `--rollover-max-changes` causes incremental backup windows to close earlier than the operator expects, which leaves rows the operator believed would land in the *current* incremental in the *next* one. For a chain restore that's harmless (the chain replays in order), but for an operator scripting "drive 5 INSERTs then expect them in this incremental" the off-by-time-window can be confusing. The multipliers above (4× on MySQL, 3× on Postgres) eliminate the surprise. Neither cap can close a rollover before the window has advanced past its own start position, either — a resumed pump opens by replaying the transaction its parent ended on, and those replayed events are the parent's, not this window's (roadmap item 98).

## Cross-references

- [docs/type-mapping.md](type-mapping.md) — DDL types ↔ IR types
- [docs/architecture.md#engine-capabilities](architecture.md#engine-capabilities) — capability declaration shape
- [internal/ir/types.go](../internal/ir/types.go), [internal/ir/extension_types.go](../internal/ir/extension_types.go), [internal/ir/change.go](../internal/ir/change.go) — Go definitions for `Type`, `Row`, etc.
