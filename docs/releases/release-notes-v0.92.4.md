# sluice v0.92.4

# sluice v0.92.4 — Bug 97 wire-encoding REDO ([]byte → string for ir.VerbatimType)

**Headline:** v0.92.3 added explicit `$N::TYPE` SQL casts for `ir.VerbatimType` columns in the apply path, but the v0.92.3 verification cycle found the bug **STILL reproduced** for `money` and `pg_lsn`: pgx's `database/sql` adapter binds Go `[]byte` as PG `bytea` on the wire, so PG evaluates `bytea::money`, which goes through an implicit `bytea → text` cast that produces a `\x…` hex literal, and then `text → money` fails to parse that. `xml` / `tsvector` / `int4range` syntactically tolerated the bytea-hex form. **v0.92.4 closes the second wire-format layer.**

## Fixed

- **`fix(postgres): convert []byte to string for ir.VerbatimType columns in prepareApplierValue (Bug 97 wire-encoding REDO — v0.92.3's $N::TYPE SQL cast was correct but missed the bind side)`** — the v0.92.3 verification cycle reproduced the byte-identical wire shape pre-fix produced: `ERROR: invalid input syntax for type money: "\x2439392e3939"` (hex = ASCII `$99.99`) and `pg_lsn: "\x302f33303030303030"` (`0/3000000`). Root cause: two wire-format layers, v0.92.3 only addressed one. The SQL `$N::money` cast is correct, but pgx's `database/sql` adapter binds Go `[]byte` as PG `bytea` on the wire, so PG receives `bytea` and evaluates the cast `bytea::money`. The implicit `bytea → text` cast produces the canonical `\x…` hex form, then `text → money` fails. v0.92.4 changes `prepareApplierValue` to convert `[]byte` to `string` for `ir.VerbatimType` columns before binding. pgx then binds as text, PG receives text, and the cast `text::money` (and `text::pg_lsn` / `text::xml` / etc.) parses the actual canonical text form (`$99.99`, `0/3000000`, `<a/>`, …) cleanly. Pinned by `TestPrepareApplierValue_VerbatimTypeBytesBecomeString` (5 sub-pins covering money / pg_lsn / xml bytes → string conversion, money-string idempotency, and non-verbatim `[]byte` passthrough). Uniform across every verbatim family per Bug 74's family-dispatch lesson. **Self-critique note:** v0.92.3's `TestBuildSQL_VerbatimTypeCasts` unit pin only exercised SQL-string output, not the actual pgx wire encoding. A follow-up integration pin that exercises the bind path against a real PG would catch this class — that's worth doing regardless of which approach lands. Tracked separately.

## Compatibility

- **Patch bump (v0.92.4).** Drop-in from v0.92.3.
- **Behavior change:**
  - PG → PG sync apply correctly round-trips `money` / `pg_lsn` rows now. The other verbatim families (`xml` / `tsvector` / range / multirange / `pg_snapshot` / `txid_snapshot`) syntactically tolerated the pre-fix bytea-hex form and round-tripped before, but the uniform `[]byte → string` conversion v0.92.4 applies makes the wire encoding family-dispatch-honest per Bug 74.

## Who needs this

- **Anyone running PG → PG sync with `money` / `pg_lsn` columns** — the bug that v0.92.2 thought it closed, v0.92.3 thought it closed, and the v0.92.3 cycle proved still reproduced. **v0.92.4 is the actual closure. Upgrade.**
- **Everyone else** — no action needed; v0.92.3 was correct for the other verbatim families.

## Coming next

The CDC schema-race family (Bugs 112 + 119 + 120 — shared root: applier's relation cache doesn't react to relation-OID changes mid-stream → silent drops on RENAME / DROP COLUMN / DROP+CREATE-same-name) is queued for v0.93.0. It's concurrency-adjacent and needs the `-race` integration gate before tag cut. After v0.93.0, the v0.94+ arcs will work through the backup-family (Bugs 110 / 116 / 117 + 118 re-verify), the silent-correctness-loss PG types (Bugs 113 / 115), and operator-quality-of-life (Bugs 108 / 114) — the open backlog has a tractable path to zero.
