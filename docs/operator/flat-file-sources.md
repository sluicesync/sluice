# Importing flat files: CSV, TSV, NDJSON — and mydumper dump directories

sluice migrates flat files into Postgres, MySQL, or SQLite through the standard `migrate` pipeline. Schema-less formats (CSV/TSV/NDJSON, ADR-0163) are staged into a temporary SQLite database and get validated rich-type inference automatically; mydumper/`pscale database dump` directories have a native reader (ADR-0161, see below). Plain mysqldump/pg_dump `.sql` dumps and `pg_dump -Fc` archives are deliberately **not parsed** — sluice refuses them loudly with a scratch-server recipe (also below).

## CSV

```
sluice migrate --source-driver csv --source ./users.csv --csv-header \
  --target-driver postgres --target '<pg-dsn>'
```

The file lands as one table named from the file's basename (`users-2024.csv` → `users_2024`; characters outside `[A-Za-z0-9_]` become `_`). Three things about your file are **producer conventions the format does not encode**, so sluice requires them declared explicitly — it never sniffs:

- **Header** — `--csv-header` (first record carries the column names) or `--csv-no-header` (columns are named `col1..colN`). Passing neither is refused (`SLUICE-E-CSV-HEADER-UNDECLARED`): guessing wrong silently eats a data row or turns data into column names.
- **NULL representation** — `--csv-null=REPR`, the *unquoted* field text that means SQL NULL. `--csv-null=''` adopts the PostgreSQL COPY CSV convention (an unquoted empty field is NULL); `--csv-null='\N'` or `--csv-null=NULL` declare that literal (unquoted empty fields are then empty strings). A **quoted field is always data**: `"NULL"` is the string, `""` is the empty string. Without the flag, a file containing an unquoted empty field is refused (`SLUICE-E-CSV-NULL-AMBIGUOUS`) — RFC 4180 has no NULL, and NULL-vs-empty-string is the #1 silent-loss class of CSV ingest.
- **Delimiter** — comma by default; `--csv-delimiter` accepts a single ASCII character or `\t`/`tab`.

Quoting follows RFC 4180 strictly: quoted fields may embed delimiters, quotes (doubled: `""`), and newlines (CRLF or LF); malformed quoting, ragged records, lone CRs, NUL bytes, and non-UTF-8 bytes are refused loudly naming the record. Files must be UTF-8 (a UTF-8 BOM is stripped with a WARN; UTF-16 is refused with a transcode hint). Blank lines are skipped — except in a one-column file, where an empty line is a real one-empty-field record and follows the NULL contract (`--csv-null=''` → a NULL row; a non-empty repr → an empty-string row; undeclared → the ambiguity refusal).

## TSV

```
sluice migrate --source-driver tsv --source ./data.tsv --csv-header ... 
```

Same engine, delimiter fixed to TAB; all `--csv-null`/`--csv-header` semantics apply. For any *other* delimiter (`;`, `|`, …) use `--source-driver csv --csv-delimiter=';'`. A `.tsv` file fed to the csv driver (or vice versa) is refused naming the right driver — unless you pass an explicit `--csv-delimiter`, which declares your intent and wins. Note the quoting dialect is RFC-4180-with-tab; mysqldump `--tab` output (backslash-escaped, unquoted) is a different dialect this driver does not interpret.

## NDJSON (newline-delimited JSON)

```
sluice migrate --source-driver ndjson --source ./events.ndjson \
  --target-driver postgres --target '<pg-dsn>'
```

One JSON **object** per line (`.jsonl` works too); keys become columns in first-seen order, and the column set may grow mid-file (earlier rows read NULL for later-appearing keys). No `--csv-*` flags apply (JSON `null` is the NULL representation; keys name the columns). Value fidelity:

- **Numbers are carried as their raw source text** — never through a float64 — so integers > 2^53, snowflake IDs, and arbitrary-precision decimals land exact.
- Strings are JSON-decoded (including `\u` escapes); `true`/`false` land as that text; nested objects/arrays land as their raw JSON text (and a name-hinted column of objects is promoted to `jsonb` — see inference below).
- An **absent key and an explicit `null` both land as SQL NULL** (SQL has one nothing where JSON has two).
- A duplicate key within one object, a non-object line, or trailing content after the object is refused loudly naming the line. A single JSON *array* document is not NDJSON — the refusal names the conversion (`jq -c '.[]' file.json > out.ndjson`).

## Types: everything stages as TEXT; inference recovers the rest

Schema-less input stages as all-TEXT columns (lossless: every value byte-exact), and `migrate` auto-engages `--infer-types` (ADR-0144): a name-hinted column (`created_at`, `*_json`/`payload`/`metadata`, `*_id`/`*_uuid`) is promoted to `timestamp`/`timestamptz`, `jsonb`, or `uuid` **only after every non-NULL value is validated to conform** — a `customer_id` column holding `cus_abc123` stays text, reported. Everything else — including numeric-looking columns — stays TEXT; use `--type-override table.col=bigint` (etc.) to place a specific column, which always wins over inference.

`sluice verify --depth count` works against a flat-file source (each verify re-stages the file); sample depth is not supported. The drivers are migrate sources only: no CDC, never a target.

## mysqldump / pg_dump dumps: the scratch-server recipe

A plain mysqldump or pg_dump `.sql` file — or a `pg_dump -Fc` (`PGDMP`) archive — handed to any file-reading driver is refused with `SLUICE-E-SOURCE-FOREIGN-DUMP`. sluice deliberately does not parse full-dialect SQL dumps (and `-Fc` is a private format); the reliable path is to restore the dump to a scratch server with its native tool, then migrate live:

```
# mysqldump .sql
docker run -d --name sluice-scratch -e MYSQL_ROOT_PASSWORD=scratch -p 33306:3306 mysql:8
mysql -h127.0.0.1 -P33306 -uroot -pscratch <db> < dump.sql
sluice migrate --source-driver mysql --source 'root:scratch@tcp(127.0.0.1:33306)/<db>' ...

# pg_dump plain .sql            # pg_dump -Fc archive
docker run -d --name sluice-scratch -e POSTGRES_PASSWORD=scratch -p 55432:5432 postgres:16
psql 'postgres://postgres:scratch@127.0.0.1:55432/postgres' -f dump.sql
# (or: pg_restore -d 'postgres://postgres:scratch@127.0.0.1:55432/postgres' dump.fc)
sluice migrate --source-driver postgres --source 'postgres://postgres:scratch@127.0.0.1:55432/postgres' ...
```

A mydumper / `pscale database dump` **directory** needs no scratch server — it has a native reader:

```
sluice migrate --source-driver mydumper --source /path/to/dumpdir \
  --target-driver postgres --target '<pg-dsn>'
```

(See ADR-0161 for the mydumper reader's contract: per-table schema files, extended-INSERT data chunks, gz/zstd, UTF-8-compatible charsets only, faithful escape/hex binary decode.) Handing the wrong input to the wrong driver — a mydumper directory to `csv`, a CSV to `mydumper`, a binary SQLite `.db` to `csv`, a gzipped/UTF-16 file anywhere — is refused with `SLUICE-E-SOURCE-WRONG-DRIVER`, naming the right driver or preparation step.

## Related docs

- `docs/operator/sqlite-d1-import.md` — SQLite `.db` / `.sql`-dump / Cloudflare D1 sources (the machinery the flat-file staging rides).
- `docs/operator/error-codes.md` — the `SLUICE-E-SOURCE-*` / `SLUICE-E-CSV-*` rows.
- ADR-0163 (design), ADR-0161 (mydumper), ADR-0144 (`--infer-types`), ADR-0130 (dump materialize).
