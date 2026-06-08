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
| `Date` | `time.Time` | Time portion is `00:00:00`, location is `UTC`. |
| `Time` | `string` | A textual representation such as `"08:30:00"` or `"08:30:00.123456"`. Go's `time.Duration` is *not* used because some SQL `TIME` values fall outside its valid range. |
| `DateTime` | `time.Time` | Location is `UTC` for transport. The semantics of "no time zone" are recorded on `Column.Type`, not on the value. |
| `Timestamp` | `time.Time` | Always `UTC` regardless of the source's session timezone. Engine readers handle the conversion. |
| `JSON` | `[]byte` | Raw JSON bytes. Whether the engine validated/normalised the bytes is recorded on `Column.Type` (`JSON{Binary: true}` for a parsed/normalised representation, `JSON{Binary: false}` for textual). |

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
| `Macaddr` | `string` | Lower-case, colon-separated (`"08:00:2b:01:02:03"`). |

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

- Coerce `int64` → `bool` for `Boolean` columns whose source is `TINYINT(1)`. MySQL stores the full signed 8-bit range in a `TINYINT(1)` (it is only a display width), so a column used as a real small integer can hold values outside `{0,1}` (2, 127, -1, …). The boolean convention collapses every non-zero value to `true`, losing the integer. The reader **WARNs loudly, once per column** (naming the column and an example value) when it sees such a value, and points at the data-preserving remedy: `--type-override <table>.<col>=smallint` (or `=int`) rewrites the IR type the reader decodes with, so the cell is read as an integer and carried faithfully. `smallint` is the safe floor — a `tinyint` override could re-emit a MySQL `TINYINT(1)` target column that would re-trigger the boolean mapping on a round-trip. The WARN fires on every read path: the bulk-copy / snapshot reader, the binlog CDC reader, and the VStream CDC + cold-start path.
- Coerce `[]byte` → `bool` for `Boolean` columns whose source is `BIT(1)`.
- Coerce `[]byte` → `string` for `Decimal`, `Char`, `Varchar`, `Text`, `Time`, and `Enum` columns.
- Split a comma-separated `[]byte` into `[]string` for `Set` columns.
- Copy `[]byte` values off the driver's scan buffer.
- Pass `time.Time` values through (with `parseTime=true` set in the DSN).

Future engine readers (Postgres, SQLite, etc.) will have their own driver quirks; each is documented at the engine package level and tested with a parallel set of unit tests.

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

## MySQL binlog-event volume — sizing `--rollover-max-changes`

The CDC reader and `backup stream` both count *binlog events*, not user-visible row changes. On MySQL the two counts are not the same, and operators sizing rollover bounds against expected INSERT counts can under-size the bound by a factor of 3-4×.

### Per-INSERT shape

A single autocommit `INSERT ... VALUES (one row)` lands in the binlog as **3 events**:

1. `BEGIN` (`QueryEvent`)
2. `WRITE_ROWS_EVENTv2`
3. `XID` / `TxCommit`

A multi-row `INSERT ... VALUES (r1), (r2), ..., (rN)` collapses the row events into one — **2 + N events** total: `BEGIN` + N row events + `XID`. Same shape applies to `UPDATE` and `DELETE` (one row event per row touched, wrapped in BEGIN/XID).

### Spurious empty BEGIN/COMMIT pair

Many MySQL client sessions emit an **empty `BEGIN` / `COMMIT`** pair into the binlog ahead of the first DML in a connection — typically from the driver issuing a session-setup statement (`SET autocommit`, `SET time_zone`, etc.) inside an implicit transaction that gets logged but contains no row changes. The pair is a constant overhead per session, not per row. Operators should budget +2 events for the first DML of any new connection.

### Operator rule of thumb

When setting `--rollover-max-changes=N` on `sluice backup stream` against a MySQL source: **budget at least 4× your expected INSERT count**. The 4× covers the per-row 3-event shape plus headroom for the spurious empty pair and any other session-bookkeeping events (heartbeats, format-description, rotate). For workloads with predictable transaction shapes (e.g. bulk multi-row inserts) the bound can be tighter — the 2 + N shape means a 1000-row multi-row INSERT consumes ~1002 events, not 3000 — but the safe default for naive INSERT-counting is 4×.

This rule of thumb only applies to MySQL. PostgreSQL's `pgoutput` logical replication delivers **one event per row change** with no in-band BEGIN/COMMIT inflation in the consumer's view (transaction boundaries arrive as separate `Begin` / `Commit` messages but sluice's CDC reader doesn't surface them as countable changes), so PG operators sizing `--rollover-max-changes` can use INSERT-count directly without a multiplier.

### Why this matters

Under-sized `--rollover-max-changes` causes incremental backup windows to close earlier than the operator expects, which leaves rows the operator believed would land in the *current* incremental in the *next* one. For a chain restore that's harmless (the chain replays in order), but for an operator scripting "drive 5 INSERTs then expect them in this incremental" the off-by-time-window can be confusing. The 4× rule eliminates the surprise.

## Cross-references

- [docs/type-mapping.md](type-mapping.md) — DDL types ↔ IR types
- [docs/architecture.md#engine-capabilities](architecture.md#engine-capabilities) — capability declaration shape
- [internal/ir/types.go](../internal/ir/types.go), [internal/ir/extension_types.go](../internal/ir/extension_types.go), [internal/ir/change.go](../internal/ir/change.go) — Go definitions for `Type`, `Row`, etc.
