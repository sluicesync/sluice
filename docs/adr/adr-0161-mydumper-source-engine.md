# ADR-0161: mydumper flat-file source engine (bounded CREATE TABLE parse + faithful dump-value decode)

## Status

**Accepted (2026-07-14).** Roadmap item 55 Phase 1 (`docs/research/flat-file-sources.md`). Numbered 0161 because a sibling chunk in flight holds 0160.

## Context

`pscale database dump` and mydumper both emit the same regular, per-table flat-file layout: a `metadata` file (binlog position / GTID), one `<db>.<table>-schema.sql` per table containing exactly one CREATE TABLE, and `<db>.<table>.<NNNNN>.sql` data chunks holding only extended INSERT statements (~1 MB each), optionally gzip/zstd-compressed. Provider-only-gives-dumps migrations (PlanetScale's own export IS this format) and air-gapped moves start from such a directory, not a live DSN. The research doc concluded a native reader is sound for exactly this format — and only this format (plain mysqldump/pg_dump `.sql` remain refusals, Phase 3).

This ADR records the tenet-sensitive decisions behind `internal/engines/mydumper`, a source-only engine registered as `mydumper`:

```
sluice migrate --source-driver mydumper --source /path/to/dumpdir \
  --target-driver postgres --target '<pg-dsn>'
```

## §2 Directory contract and detection

`--source` is a directory. At open the layout is validated: `metadata` present (a `metadata.partial*` marker = torn dump = refusal), at least one `*-schema.sql`, every file attributable. Recognised auxiliaries (`-schema-create`, `-schema-view`, `-schema-triggers`, `-schema-post`, `-schema-sequence`, per-table `-metadata`/`-checksum`) are schema-only and skipped — views/triggers/routines with a WARN naming each; anything unrecognised refuses loudly naming the file. Exactly ONE database prefix per directory (multi-`-B` dumps refuse naming the databases); the `<db>.<table>` filename split is on the FIRST dot, inheriting mydumper's own ambiguity for dotted table names. Chunks sort by numeric chunk id (the zero-padded field widens past 99999).

## §3 The contained DDL-parse exception

The IR-first tenet forbids grammar over DDL strings — engine schema knowledge belongs in catalog readers. A flat file has no catalog, so this engine carries a DELIBERATELY BOUNDED exception, on the ADR-0133 precedent (SQLite's sqlite_master DDL text is likewise the only schema surface that engine has):

- **Scope: exactly one CREATE TABLE per schema file**, plus comments and SET statements. A second statement of any kind — ALTER, INSERT, DROP, a second CREATE, CREATE VIEW — is a loud refusal naming the file. Inside the CREATE TABLE, any unrecognised column attribute or body item is equally a refusal naming file + offset + token: the parser never skips what it does not understand (versioned `/*!` comments — partition clauses, physical options — are the one skip, matching the live reader's posture of not carrying partitioning).
- **The type mapping is NOT forked.** Parsed column metadata is folded into the live MySQL engine's own `mysql.ColumnMeta` and translated by `mysql.TranslateColumnType` (a thin exported shim over the information_schema translator, `internal/engines/mysql/flatfile_shim.go`). DEFAULT clauses reproduce the live reader's exact IR shapes (bit-literal BIT-vs-BOOLEAN dispatch, binary hex-literal `hexbytes` dialect, `mysql`-tagged expressions through `NormalizeExpressionText`). NOT ENFORCED CHECKs are dropped with a WARN (they do not constrain source rows; a target would enforce them).

Engine-to-engine reuse (mydumper → mysql package) follows the sqlite-trigger → sqlite precedent; the orchestrator still imports neither.

## §4 The escape-decoder fidelity contract

Data-chunk values decode through a faithful MySQL string-literal lexer and then the live engine's value decoder (`mysql.DecodeRowValue`), so dump values land in the `ir.Row` contract (docs/value-types.md) byte-identical to a live read:

- **Strings:** the shared `scanMySQLQuotedString` decoder — full backslash set (`\0 \b \t \n \r \Z \\ \' \"`), doubled-quote escapes, unknown-escape passthrough, and (fixed in this chunk, in the shared decoder) MySQL's `\%`/`\_` LIKE-pattern semantics where the backslash is KEPT. Double-quoted strings (mydumper ≥1.0 emits them) decode via delimiter transposition through the same decoder. `_binary`/`_utf8mb4`-style charset introducers are stripped when UTF-8-compatible and REFUSED otherwise.
- **Binary, both shapes:** `0x…` hex-blob (vanilla `--hex-blob`) and backslash-escaped strings (the mydumper ≥1.0 default and the pscale writer's only shape — binary fidelity there rides entirely on the escape decoder). `b'…'` bit literals and `X'…'` hex strings are also lexed.
- **Numbers:** raw decimal text parsed straight to int64 / uint64 (unsigned columns above MaxInt64 only) / float64 by the column type — never through an intermediate float (the D1 2^53 lesson). DECIMAL stays text.
- **NULL vs `'NULL'`:** the bare keyword is SQL NULL; the quoted string is data.
- **CONVERT wrappers:** mydumper ≥1.0 wraps JSON values in `CONVERT("…" USING UTF8MB4)` (ground-truthed against v1.0.3); the wrapper is unwrapped with the conversion charset held to the same UTF-8 allowlist as §5.
- **FLOAT display-rounding (named wart, WARNed):** mydumper's bare SELECT renders single-precision FLOAT through mysqld's ~6-significant-digit float→text formatter — 8388608 dumps as `8.38861e6`; the LOSS IS IN THE FILE, at dump time (the dump-format sibling of the VStream-COPY FLOAT class, ADR-0153 / `ir.LossyFloatCopyReader`). The reader cannot re-read exactly, so it WARNs once per table naming the FLOAT columns and pointing at the migrate-from-live remedy. DOUBLE is unaffected. Caught by this chunk's real-dump oracle (the live-path-equivalence compare), which is exactly the class of divergence it exists to catch.
- **BIT(1)-as-Boolean quoted bytes:** a quoted value on a Boolean column is a BIT(1)'s raw wire byte (`_binary "\0"`), decoded through the bytes branch — the text branch would misread `"\x00"` as true (also a real-dump oracle catch, pinned in the unit matrix).
- **Loud failure:** any literal-kind × type pairing outside the matrix (a hex literal on an INT column, a number on a BLOB, int64 overflow on a signed column) refuses naming the column — never a coercion.

**Pin matrix** (unit, `insert_lexer_test.go` + `row_reader_test.go`): every family — signed ints (incl. >2^53 and MinInt64), uint64 max, DECIMAL, FLOAT/DOUBLE, DATE/DATETIME/TIMESTAMP (incl. fractional), TIME, CHAR/VARCHAR/TEXT (every escape), BINARY/VARBINARY/BLOB (hex-blob AND escaped shapes), JSON, ENUM, SET (incl. empty), BIT (bit-literal/hex/escaped/number shapes), Boolean, Geometry — × {value, NULL, the `'NULL'` string} × tuple edges (separators inside strings, multi-tuple, column-list/bare, arity/garbage refusals) × {plain, .gz, .zst}. Integration (`mydumper_integration_test.go`): REAL mydumper-generated dumps (the mydumper/mydumper image) in default-escape, `--hex-blob`, gzip, and zstd legs, migrated into MySQL AND Postgres targets and compared row-by-row against the live-`mysql`-engine migration of the same source (live-path equivalence as the oracle), plus direct dump-reader-vs-live-reader ir.Row comparison.

## §5 Charset posture: UTF-8-compatible or refuse

Dump bytes are carried verbatim, assumed UTF-8. Three gates enforce it rather than transcode silently: (1) a data chunk's `SET NAMES` must be binary/utf8/utf8mb3/utf8mb4; (2) a value's charset introducer must be `_binary`/`_utf8*`/`_ascii`; (3) every string-family column's effective charset (explicit, else table default, else assumed utf8mb4) must be utf8mb4/utf8/utf8mb3/ascii/binary — a latin1 table refuses at ReadSchema naming table/column with the convert-or-migrate-live remedy. `SET TIME_ZONE` other than +00:00/UTC likewise refuses (temporal literals are interpreted as UTC).

## §6 Streaming and bounds

Chunks stream through a carry-based MySQL-aware statement splitter (the sqlite `dump.go` shape with MySQL string/identifier/comment states — the SQLite splitter itself would desynchronise on `\'`). Named wart: a single statement is capped at 256 MiB (`maxStatementBytes`) — mydumper/pscale target ~1 MiB statements; a carry past the cap means the file is not statement-structured and refuses rather than buffering unbounded. Schema files cap at 64 MiB.

## §7 Zero dates and --zero-date

A relaxed-sql_mode zero/partial date (`0000-00-00`, zero month/day) in a dump refuses loudly naming the value (the shared decoder's refuse default). The live engine's `--zero-date null|epoch` plumbing is NOT yet threaded through this engine — an operator hitting the refusal migrates from the live database with `--zero-date`, or repairs the source. Wiring the same `WithZeroDate` option here is a small follow-up if demand shows.

## §8 metadata position: recorded, not built

The dump's binlog position / GTID set (traditional `SHOW MASTER STATUS:` shape, ini `[master]`/`[source]` shape; mydumper ≥1.0's commented-out `# SOURCE_LOG_FILE` coordinates are deliberately ignored as non-authoritative) is parsed leniently and logged at INFO on open — the future dump→CDC-handoff hook. No CDC surface exists (`Capabilities.CDC = CDCNone`); a later phase could seed a `sync` position from it.

## §9 Capability shape and verify semantics

Source-only, the d1 registry shape: every write/CDC/snapshot `Open*` returns a wrapped `ErrNotImplemented`; `BulkLoad = BulkLoadNone`. The RowReader implements NONE of the batched/counter/bounds surfaces (file chunks have no PK addressing), so every table routes through the single-reader whole-table copy. `sluice verify --depth count` works — the SchemaReader implements `ir.Verifier.ExactRowCount` by chunk re-scan (tuple count, no value decode). Sample depth is NOT implemented: deterministic sampling needs cheap row addressing files don't have; the orchestrator's "sample mode not supported" refusal stands, and this is the documented limitation.

## Non-goals

- Plain mysqldump `.sql` single-file dumps (full-dialect stream; Phase 3 = signature-detect + loud refusal + scratch-server recipe).
- Anything pg_dump (`.sql`, `-Fc`, `-Fd`) — private/dialect formats per the research.
- Staging typed dumps through SQLite (DECIMAL/uint64/temporal degradation — the research's option-b analysis forbids it; SQLite staging is for schema-less input only).
- myloader-side writing, multi-database dump dirs, CDC handoff (recorded only, §8).
