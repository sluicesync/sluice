# ADR-0129: SQLite date/bool interpretation policy

## Status

**Accepted (2026-06-26).** Roadmap item 49 follow-up; extends ADR-0128 (the SQLite
migrate-source prototype). Makes the SQLite source usable on real-world SQLite /
Cloudflare D1 databases by interpreting **declared** `DATE`/`DATETIME`/`TIMESTAMP`/
`TIME` and `BOOLEAN`/`BOOL` columns as the corresponding IR temporal/boolean types —
instead of SQLite's raw NUMERIC affinity (which made an ISO-text date a
TEXT-in-NUMERIC mismatch that the prototype refused).

## Context

ADR-0128 mapped SQLite columns by pure affinity. SQLite's affinity rules send
`DATE`/`DATETIME`/`BOOLEAN`/`STRING` to **NUMERIC** affinity → `ir.Decimal`. SQLite has
no native date/time/bool storage — apps store dates as ISO **TEXT** (`'2024-01-01'`,
SQLite's own `date()`/`datetime()` output), or as **INTEGER** unix epoch (seconds or
millis; the JS/D1 ecosystem's `Date.now()`), or as **REAL** Julian day (`julianday()`);
booleans as 0/1 INTEGER. Under the prototype, a column declared `DATE` holding ISO text
is a TEXT-in-NUMERIC mismatch → **loud refusal**. That is the safe direction, but it
hard-fails the very common `created_at DATE` / `is_active BOOLEAN` schema, so the
prototype bounces off most real databases. This ADR adds the interpretation policy that
makes it usable — without giving up the loud-failure guarantee.

The ambiguity that demands care: the IR temporal *type* is unambiguous from the
**declared** type (the operator declared `DATE`), but the value **encoding** (ISO text
vs unix int vs julian real) is app-specific, and guessing it wrong silently produces
wrong dates — a value-fidelity violation. So the type is inferred; the encoding is an
explicit, loud-on-mismatch policy.

## Decision

1. **Declared-type → IR temporal/bool, by default.** In the schema reader, a column
   whose declared type (case-insensitive, substring per SQLite's own matching) names a
   date/time/bool overrides the affinity-NUMERIC default:
   - contains `DATETIME` or `TIMESTAMP` → `ir.Timestamp` (no tz; SQLite is tz-naive)
   - else contains `DATE` → `ir.Date`
   - else contains `TIME` → `ir.Time`
   - `BOOL` / `BOOLEAN` → `ir.Boolean`
   This precedes the existing affinity rules for these spellings only; everything else
   is unchanged from ADR-0128. (`INTEGER`/`INT`-declared bool-ish columns are NOT
   guessed as bool — only the explicit `BOOL`/`BOOLEAN` declaration.)

2. **Value decode is governed by an explicit, global date encoding** —
   `--sqlite-date-encoding` (DSN param `sqlite_date_encoding`), one of:
   - `iso` (DEFAULT): temporal values are **TEXT** parsed against a fixed set of ISO
     layouts (`2006-01-02`, `2006-01-02 15:04:05[.999999]`, RFC3339). A temporal column
     whose value is a non-text storage class, or text that matches no layout, is
     **REFUSED LOUDLY** (naming table/column/rowid + the seen value), with a hint to set
     `--sqlite-date-encoding` or `--type-override <col>=text` to carry it raw.
   - `unixepoch` / `unixmillis`: temporal values are **INTEGER** (or REAL with a
     fractional part) read as unix seconds / milliseconds → `time.Time` (UTC). Non-
     numeric storage class refused.
   - `julian`: temporal values are **REAL**/INTEGER read as Julian day → `time.Time`.
     Non-numeric refused.
   The encoding is global because mixing encodings within one DB is rare; a per-column
   outlier is handled by `--type-override <col>=text` (carry raw) or is refused. A
   per-column encoding map is a deferred refinement.

3. **Boolean decode is unambiguous and needs no encoding flag.** A `BOOLEAN`-declared
   column: INTEGER `0`/`1` → `false`/`true`; TEXT `true|false|t|f|yes|no|1|0`
   (case-insensitive) → bool; **any other value (INTEGER 2, arbitrary text, REAL, BLOB)
   is REFUSED LOUDLY** — never coerced (a non-0/1 int in a bool column is a data
   problem the operator must see).

4. **Never silently guess the encoding.** The default `iso` assumes text and refuses
   non-text temporal values; an operator with unix-int dates opts in explicitly
   (`--sqlite-date-encoding=unixepoch`). This keeps the loud-failure guarantee: a wrong
   encoding produces a loud refusal (type/encoding mismatch) or correct values, never a
   silently-wrong date.

5. **The `--type-override` escape hatch (pipeline-level, already exists)** lets an
   operator force any column back to `text` (carry the raw SQLite value verbatim) when
   neither the inferred type nor any encoding fits — the documented fallback.

## Consequences

- Real SQLite / D1 databases (ISO-text dates, 0/1 bools — the common case) migrate
  cleanly into Postgres/MySQL with proper `timestamp`/`date`/`boolean` target columns,
  not numeric. Unix-int / julian date stores work with one flag. The engine becomes
  usable, and the SQLite source can ship as a real release (not just a prototype).
- The loud-failure guarantee is preserved: type is inferred from the unambiguous
  declared type; the ambiguous encoding is explicit and refuses on mismatch; bool
  refuses any non-boolean value. No silent date/bool coercion anywhere.
- Value-path change → ships under the family matrix (below) + a value-fidelity review.

## Value-fidelity requirement

Pin the class: {`ir.Date`, `ir.Timestamp`, `ir.Time`} × each encoding (`iso`,
`unixepoch`, `unixmillis`, `julian`) × each storage class (TEXT/INTEGER/REAL/BLOB/NULL)
→ faithful `time.Time` for the matching combination, NULL for NULL, **loud refusal** for
every mismatch (wrong storage class, unparseable text). And `ir.Boolean` × {INTEGER 0/1,
INTEGER other, TEXT truthy/falsy, TEXT other, REAL, BLOB, NULL} → faithful/NULL/refuse.
Round-trip the inferred types into BOTH Postgres and MySQL (a `DATE`/`DATETIME`/`BOOLEAN`
SQLite source column lands as the right target type with the right value). A
`value-fidelity-reviewer` pass is required before it lands (Bug-74 corollary).

## Alternatives considered

- **Keep affinity-pure (the prototype) and require `--type-override` for every date/bool
  column.** Rejected: unusable on real DBs (every `DATE` column needs a manual override).
- **Auto-detect the encoding by sniffing values** (text→iso, int→unixepoch). Rejected:
  silent guessing is exactly the value-fidelity violation — an int that's really a
  unix-seconds value vs a unix-millis value can't be told apart safely, and a wrong
  guess yields a plausible-but-wrong date. Explicit encoding + loud refusal is the tenet.
- **Per-column encoding map up front.** Deferred: the global flag + `--type-override`
  escape hatch covers the common and outlier cases; a per-column map can come on demand.
