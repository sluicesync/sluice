# ADR-0163: CSV/TSV/NDJSON flat-file sources (stage-into-SQLite) + foreign-dump refusal recipes

## Status

**Accepted (implemented, 2026-07-15).** Roadmap item 55 Phases 2 and 3 (`docs/research/flat-file-sources.md`). Phase 1 (the mydumper source engine) is ADR-0161.

## Context

Spreadsheet-to-database onboarding starts from a CSV, log pipelines emit NDJSON, and neither carries a schema. The research doc concluded these schema-less formats fit sluice's EXISTING machinery verbatim — the ADR-0130 materialize-into-a-temp-SQLite path plus the ADR-0144 validated rich-type inference — while full-dialect SQL dumps (plain mysqldump/pg_dump `.sql`, pg_dump `-Fc`) remain a parse trap that gets a loud, recipe-bearing refusal instead (pgloader, the most successful file loader, deliberately parses no SQL dumps either).

This ADR records the decisions behind `internal/engines/flatfile` (the `csv`, `tsv`, `ndjson` source drivers), the staged-reader shim in the sqlite engine, and the `internal/engines/internal/dumpsig` signature detector:

```
sluice migrate --source-driver csv --source ./users.csv --csv-header \
  --target-driver postgres --target '<pg-dsn>'
```

## §1 Stage-into-SQLite — for SCHEMA-LESS input only

Each `Open*` materializes the flat file into a fresh temp SQLite database — one table (named from the file's basename, sanitized: `users-2024.csv` → `users_2024`), every column declared `TEXT`, every value bound as its exact source text — and returns the sqlite engine's readers over it via a small staging shim (`sqlite.OpenStagedSchemaReader` / `OpenStagedRowReader`; the mydumper→mysql engine-reuse precedent). The staged readers inherit the whole validated sqlite surface for free: value decode, the `ir.InferredTypeValidator` behind `--infer-types`, the (new) `ir.Verifier` count surface, and the ADR-0130 tempPath ownership rules (removed on Close; within-table chunking disqualified exactly like a materialized dump — a per-chunk re-open would re-stage the whole file). *(Amended 2026-07-15, audit MED-P2: originally each reader open re-staged independently, so a migrate — whose orchestrator holds the schema reader open run-long — staged the file TWICE and held both copies for the run (~2× staging writes, ~2× peak temp). A configured engine now stages ONCE per source through a refcounted stage-once handle shared by the schema and row readers; the last reader's Close removes the copy, and a later open re-stages. `--stage-dir` / `SLUICE_STAGE_DIR` overrides the staging directory — the ADR-0145 tmpfs-/tmp hazard class — for flat-file staging, the D1 `--stage-local` replica, and the export-as-parquet scratch.)*

TEXT staging is the fidelity decision, not a shortcut: SQLite TEXT affinity stores text verbatim, so no affinity coercion can occur — `007.1500` stays `007.1500`, `9007199254740993` stays exact, `-0.000` stays `-0.000`. **Staging typed dumps through SQLite remains forbidden** (ADR-0161 non-goals — DECIMAL/uint64/temporal degradation); this path exists precisely because CSV/TSV/NDJSON have no types to degrade.

## §2 Auto-engaged `--infer-types`, and what stays TEXT

The `migrate` CLI auto-engages `--infer-types` (ADR-0144) for these drivers — the staged columns are all TEXT, so validated inference is what recovers rich target types. Promotions keep ADR-0144's exact contract: name-hinted candidates only, promoted only when EVERY non-NULL value validates, `--type-override` always wins. Named limitations, documented rather than papered over:

- **Numbers stay TEXT.** ADR-0144 has no numeric promotion (hints select boolean/temporal/json/uuid), so a digits column lands as TEXT — lossless, byte-exact, and explicitly conservative. The escape hatch is `--type-override table.col=bigint` (etc.). A future numeric-inference family is a separate ADR against ADR-0144, not a flag here.
- **Boolean hints don't fire.** ADR-0144's boolean candidate requires the *Integer* source family; staged columns are Text. `true`/`false` text lands as TEXT (NDJSON carries the raw `true`/`false` tokens).
- Programmatic `pipeline.Migrator` callers keep the zero-value default (no inference): everything lands TEXT — the safe posture (the v0.99.51 zero-value rule).

## §3 The CSV NULL contract (`--csv-null`) — declared, never sniffed

RFC 4180 has no NULL representation; NULL-vs-empty-string is producer convention and the #1 CSV silent-loss class. The contract:

- **A QUOTED field is always data.** `"NULL"` is the four-character string; `""` is the empty string. This is the same bare-keyword-vs-quoted-string line the mydumper decoder draws (ADR-0161 §4), and it is why this package carries its own strict RFC 4180 lexer — `encoding/csv` collapses `a,,b` and `a,"",b` into the same record, destroying exactly the distinction the contract needs.
- **`--csv-null=REPR`** declares the UNQUOTED field text that means SQL NULL. `--csv-null=''` adopts the PostgreSQL-COPY-CSV convention (unquoted empty = NULL); `--csv-null='\N'` / `--csv-null=NULL` declare that literal, and unquoted empty fields are then empty strings (declaring a representation resolves the ambiguity — under a declared convention, empty is data).
- **No flag → refuse-on-ambiguity.** An unquoted empty field with no declaration refuses loudly (`SLUICE-E-CSV-NULL-AMBIGUOUS`, naming record and column). A file with no empty unquoted fields needs no flag. The CLI carries the distinction on a `*string` kong flag (nil = undeclared vs `""` = declared-empty), pinned through the real parser (the Bug-180 lesson).

Header presence is equally declared, never sniffed: `--csv-header` or `--csv-no-header` (columns `col1..colN`), else `SLUICE-E-CSV-HEADER-UNDECLARED` — a wrong guess either eats the first data row or turns data into column names.

## §4 tsv is a registered flavor; the delimiter is a csv-driver flag

`tsv` is the same engine registered under its own name with the delimiter fixed to TAB (the MySQL-flavor pattern: same code, different declaration). `--csv-delimiter` (single ASCII char, or `\t`/`tab`) customises the `csv` driver; passing a non-tab delimiter with `--source-driver tsv` is a contradiction and refuses. Extension cross-checks catch the silent-wrong-dialect trap (a `.tsv` through the comma lexer stages one wide column) but yield to an EXPLICIT `--csv-delimiter` — extensions hint refusals, they never decide how bytes are parsed. Quoting semantics for tsv are RFC 4180-with-tab; backslash escape sequences (mysqldump `--tab` style `\t`-in-field) are NOT interpreted — that format is a different dialect, out of scope.

## §5 The strict lexer and encoding gates

The lexer refuses loudly, naming record and line: bare quote in an unquoted field, text after a closing quote, unterminated quoted field, lone CR outside quotes, ragged arity (every record must match the first record's width), a field above 64 MiB (the ADR-0161 §6 bounded-carry posture — past that the file is not delimiter-structured). CRLF and LF record ends both parse; CR/LF/delimiter/quote inside a QUOTED field are data. Fully-blank lines are skipped (the encoding/csv convention) — EXCEPT in a ONE-column file, where an empty line is exactly a one-empty-field record and flows through the NULL contract like any other field (declared `--csv-null=''` → a NULL row; a declared non-empty repr → an empty-string row; undeclared → the ambiguity refusal). Skipping it there would silently drop a row (the F1 review finding); a blank line before the first record (width not yet established) is still skipped.

Encoding is UTF-8 only: invalid UTF-8 refuses with a transcode hint; a NUL byte refuses explicitly (interleaved 0x00 is byte-wise "valid UTF-8" — the UTF-16-without-BOM tell utf8.Valid alone misses); a UTF-16/32 BOM is refused by signature; a UTF-8 BOM is stripped (lossless) with a WARN.

## §6 NDJSON value posture (the D1 2^53 lesson)

One JSON OBJECT per line. Values land by kind: **numbers as their RAW source text** — never through a float64 — so int64 > 2^53, beyond-int64 integers, and arbitrary-precision decimals land byte-exact; strings JSON-decoded (`\u` escapes included); `true`/`false` as those tokens' text; `null` as SQL NULL; nested objects/arrays as their raw JSON text verbatim (a `payload`-hinted column then promotes to jsonb, with jsonb's documented whitespace/key-order normalization). Columns are keys in first-seen order and the set may GROW mid-file (SQLite's O(1) ADD COLUMN backfills earlier rows NULL). Named collapse: an ABSENT key and an explicit `null` both land as SQL NULL — SQL has one nothing where JSON has two. Refusals: a non-object top level, TRAILING content after the object, a DUPLICATE key within one object (encoding/json's silent last-wins is a silent-loss shape), and a leading `[` (a single-array JSON document is not NDJSON — the refusal carries the `jq -c '.[]'` conversion). `--csv-*` flags refuse on this driver.

## §7 Phase 3: foreign-dump signatures → recipe-bearing refusals

`dumpsig` classifies a source's head bytes at open — on the flat-file drivers, the sqlite driver (extending the ADR-0130 sniff: previously a mysqldump `.sql` died mid-materialize on a confusing SQL error), and the mydumper driver's file-not-directory branch:

- Plain **mysqldump** (`-- MySQL dump`), plain **pg_dump** (`-- PostgreSQL database dump` at a line start in the head), and **PGDMP** (`pg_dump -Fc`) refuse with `SLUICE-E-SOURCE-FOREIGN-DUMP`, carrying the exact scratch-server-replay recipe (docker run a scratch server → native restore tool → `sluice migrate` from it). These are never parsed — the IR-first tenet (and `-Fc` is explicitly private).
- Cross-driver misuse refuses with `SLUICE-E-SOURCE-WRONG-DRIVER` naming the right driver or preparation step: a mydumper DIRECTORY to `csv`/`sqlite`, a CSV/TSV/NDJSON file to `mydumper`/`sqlite` (extension hint), a binary SQLite `.db` to `csv`, gzip/zstd (decompress first), UTF-16 (transcode first).

Detection is by content signature; extensions only refine wrong-driver hints. A refusal here also collects the demand signal for any future `--scratch <dsn>` replay orchestration (deferred, per the research doc).

## §8 Capability shape and verify semantics

Source-only, the d1/mydumper registry posture: `CDC = CDCNone`, `BulkLoad = BulkLoadNone`, every write/CDC/snapshot `Open*` returns a wrapped `ErrNotImplemented`. `sluice verify --depth count` works: the sqlite file `SchemaReader` now implements `ir.Verifier.ExactRowCount` (an authoritative `COUNT(*)` over the staged database — which also lights up count-depth verify for plain sqlite `.db`/dump endpoints). A verify run re-stages the file (once per run, via the stage-once handle) — the re-scan cost, same class as mydumper's chunk re-scan. Sample depth stays refused (no `SampleVerifier`; a staged keyless table has no deterministic sampling key).

## Non-goals

- Parsing plain mysqldump/pg_dump SQL or `-Fc`/`toc.dat` (Phase 3 refuses with recipes; scratch-server orchestration is demand-gated).
- Compressed flat files (`.csv.gz` refuses with "decompress first"; transparent decompression is a small follow-up if demand shows).
- mysqldump `--tab` backslash-escaped TSV dialect; single-document JSON arrays (the refusal names `jq -c`).
- Numeric/boolean inference for TEXT-staged columns (a future ADR-0144 extension, not a silent widening here).
- Parquet (item 55 Phase 4, demand-gated — typed, needs no inference and no staging).
