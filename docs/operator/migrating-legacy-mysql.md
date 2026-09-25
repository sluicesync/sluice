# Migrating legacy MySQL data

`sluice` defaults to strict-mode operation on every MySQL connection it
opens — `STRICT_TRANS_TABLES,NO_ZERO_DATE,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO`.
This default exists because pre-v0.92.1 sluice silently inherited the
server's sql_mode, which on many dev / older / managed deployments
relaxes one or more strict modes — and that quiet relaxation let
several silent-data-loss bugs hide in plain sight (Bug 102 NUMERIC
overflow → silent clamp; Bug 103 TIMESTAMPTZ out-of-range → silent
zero-date). The strict-by-default closes that class.

**The default is right for fresh migrations between modern schemas.**
It's the wrong default for migrating *legacy* MySQL data — schemas
that have been collecting `'0000-00-00'` placeholder dates,
silently-truncated VARCHAR values, and zero-marker columns since
before MySQL 5.7 turned strict mode on by default in 2015. The 20+
year-old WHMCS-shaped corpus is the canonical example.

This doc shows the three things that legacy-MySQL operators need to
know to migrate cleanly with v0.92.1+.

## 1. Zero-dates and partial dates: `'0000-00-00'`, `'YYYY-00-DD'`, `'YYYY-MM-00'`

Legacy MySQL stores three flavors of invalid date under a relaxed
`sql_mode`: the all-zero `'0000-00-00'`, a zero **month** (`'2026-00-15'`),
and a zero **day** (`'2026-06-00'`). None has a valid calendar value.
There are **two independent layers** to control, and getting only one of
them is a trap:

**Read side — `--zero-date` (how sluice *carries* the value off the
source).** When sluice reads a temporal column it gets MySQL's literal
text. The go-sql-driver, left to parse these itself, would feed them to
Go's `time.Date(2026, 0, 0, …)` which **silently normalizes** a partial
date to the wrong calendar day (`'2026-00-00'` → `2025-11-30`) — a
silent-corruption class. sluice reads temporal columns as raw text to
avoid that, then applies your `--zero-date` policy:

| `--zero-date` | Behavior |
|---|---|
| `error` (default) | Refuse loudly, naming the column and value. Nothing silently wrong leaves the source. |
| `null` | Carry the value as SQL `NULL`. Refused loudly if the column is `NOT NULL` (use `epoch`, or repair the data). |
| `epoch` | Substitute `1970-01-01` (`1970-01-01 00:00:01` for DATETIME/TIMESTAMP). The placeholder is one second past midnight on purpose: MySQL's `TIMESTAMP` range starts at `1970-01-01 00:00:01` UTC, so a plain midnight value is unrepresentable there and a relaxed-`sql_mode` target would silently store it back as `0000-00-00`. One second is meaningless on a synthetic placeholder for an invalid date. |

This applies to **every** direction the value is *read* in — including
MySQL→MySQL and the CDC tail — so it also protects the same-engine case
the prior `--mysql-sql-mode=''` write-side workaround did not.

**Write side — `--mysql-sql-mode` (whether the target MySQL *accepts*
the value).** Only relevant when the **target** is MySQL. With
strict-by-default, an INSERT carrying a zero-date fails with
`Error 1292` / `1525` / `1364`. Passing `--mysql-sql-mode=''` relaxes
the target so it stores the legacy zero-date as-is. This is independent
of `--zero-date`: `--mysql-sql-mode=''` alone no longer silently carries
partial dates (the read side refuses them first).

Under `--mysql-sql-mode=''` MySQL also silently *clamps or truncates* any
other out-of-range / over-long value on write (a numeric overflow → MAX, an
over-long string → cut). sluice no longer lets that pass quietly: the bulk
writer reports each such coercion as a loud **WARN** (once per column),
naming the offending values and the data-preserving remedy (map the column
to a fitting type with `--type-override`, e.g. `=decimal(P,S)`,
`=text`/`=varchar`, `=datetime`). It is a WARN, not a refusal — you opted
into relaxed mode — but a silent clamp will never escape unannounced. Drop
`--mysql-sql-mode=''` to have strict mode *refuse* such values instead.

**Recovery.** Pick the read policy that matches your data semantics:

```bash
# Modernize: convert zero/partial dates to NULL (requires the target
# columns to be nullable; sluice refuses loudly on any NOT NULL one).
sluice migrate --zero-date=null \
    --source-driver=mysql --source=$LEGACY_DSN \
    --target-driver=postgres --target=$NEW_PG_DSN

# MySQL→MySQL with a non-null placeholder: substitute the epoch. The
# target accepts 1970-01-01 under strict mode, so no --mysql-sql-mode
# relaxation is needed.
sluice migrate --zero-date=epoch \
    --source-driver=mysql --source=$LEGACY_DSN \
    --target-driver=mysql --target=$NEW_MYSQL_DSN
```

sluice never re-emits a literal `'0000-00-00'` — the read side resolves
every zero/partial date to a refusal, `NULL`, or the epoch. If you must
preserve the literal zero-date convention on a MySQL target, that's an
operator-side ETL step before sluice sees the data.

If you can't tell up front, run the default (`--zero-date=error`) once:
it names every offending column so you can decide per-column whether
`null` or `epoch` is right.

## 2. Silently-truncated VARCHARs

**Symptom.** Under relaxed sql_mode, a `INSERT ... VALUES ('twelve
chars', ...)` into a `VARCHAR(8)` column silently stored `'twelve c'`.
Under strict mode, the same INSERT fails with:

- `Error 1406`: `Data too long for column 'name' at row N`

This will surface if sluice's IR carries values that overflow the
target column's declared length. **It almost never happens on a
PG→MySQL migration** (PG values fit MySQL columns because they came
from a stricter source) but can happen on MySQL→MySQL when the source
schema's column lengths were tightened post-data-load.

**Recovery.** Same as zero-dates: `--mysql-sql-mode=''` if you want
the source's loose semantics to carry, or widen the target column via
`--type-override` if you want to keep strict mode but accept the
historical truncation as the actual data.

## 3. `VARCHAR(0)` / `CHAR(0)` marker columns

**Symptom.** Legacy MySQL allowed `VARCHAR(0)` as a marker column
(the column exists, it can hold `NULL` or `''`, that's it). PG
refuses zero-length char/varchar at CREATE TABLE with
`length for type varchar must be at least 1` (SQLSTATE 22023).

Sluice v0.92.1+ catches this at the schema-apply step and refuses
loudly with:

```
postgres: column type VARCHAR(0) has no cross-engine PG translation
(PG refuses zero-length varchar at CREATE TABLE — SQLSTATE 22023).
VARCHAR(0) is a MySQL idiom for a marker column (exists/doesn't
exist); recovery: --type-override=TABLE.COL=text ...
```

**Recovery options:**

```bash
# Option A — convert to TEXT (the most common workaround):
sluice migrate \
    --type-override='affiliates_data.token=text' \
    ...

# Option B — convert to BOOLEAN (if it's used as a true marker):
sluice migrate \
    --type-override='affiliates_data.token=boolean' \
    ...
```

If the source schema has several VARCHAR(0) columns, every offending
column needs its own `--type-override`. A YAML config block is the
operator-friendly way to declare them all in one place.

## Quick-reference: which flags for which legacy shape

| Legacy shape | What strict mode does | Quickest recovery |
|---|---|---|
| `'0000-00-00'` zero-dates | Rejects INSERT | `--mysql-sql-mode=''` |
| `'2020-00-15'` zero-in-date | Rejects INSERT | `--mysql-sql-mode=''` |
| Over-length strings | Rejects INSERT (Error 1406) | `--mysql-sql-mode=''` OR widen target column |
| Numeric overflow | Rejects INSERT (Error 1264) | `--mysql-sql-mode=''` OR widen target column |
| `VARCHAR(0)` / `CHAR(0)` (MySQL → PG only) | Refuses at sluice schema-emit | `--type-override=COL=text` |
| Division by zero in computed defaults | Raises error | `--mysql-sql-mode=''` (rare; usually you fix the default) |

## A row the target dropped is never a coercion (`LOAD-DATA-ROWS-SKIPPED`)

The bulk-copy path into MySQL uses `LOAD DATA LOCAL INFILE`, and `LOCAL` carries IGNORE semantics on the server: a row that violates a target CHECK constraint is reported as warning 3819 and **skipped**, not written — in strict and relaxed sql_mode alike. That is not a coerced value, it is a lost row, so sluice refuses it under every sql_mode, `--mysql-sql-mode=''` included; the refusal is marked `LOAD-DATA-ROWS-SKIPPED`, names the constraint, and states how many rows were skipped. A first-attempt shortfall between the rows sluice sent and the rows the server reports inserted is refused the same way, whatever the warning sample shows — that shortfall is the independent witness a capped or empty warning list cannot hide.

This matters most for a PostgreSQL source with a `NOT VALID` CHECK: MySQL has no unvalidated state, so sluice creates the CHECK enforced, and rows the source never validated are exactly the ones that violate it. Through v0.148.1 the relaxed-mode gate WARNed that values had been "clamped or truncated" and the migration exited 0 short of rows (audit 2026-09-09, A0909-MYSQL-MEDIUM-2). Ways out: fix or exclude the offending source rows; drop or relax the target CHECK (`ALTER TABLE … DROP CHECK <name>`) and re-run; or connect to the target with `local_infile=OFF`, which routes the copy through batched INSERTs where the same violation fails the statement loudly at errno 3819.

## What `--mysql-sql-mode=''` does NOT change

The MySQL driver-level overrides (UTF-8 charset, `time_zone='+00:00'`,
`utf8mb4` collation, the keep-alive dialer) stay regardless of
`--mysql-sql-mode`. The flag only controls the `sql_mode` SET that
sluice issues post-handshake; if you want to fully control all of
those, pass them in the DSN params and sluice respects them.

## ENUM/SET labels with 4-byte UTF-8 (emoji, supplementary plane) (`ENUM-LABEL-NOT-RECOVERABLE`)

MySQL and MariaDB keep an ENUM/SET label such as `'😀b'` intact in the table the server executes against — a row holding it reads back from `SELECT` as the real bytes — but every catalog surface writes the character outside the Basic Multilingual Plane as `?`: `information_schema.COLUMNS.COLUMN_TYPE`, `SHOW CREATE TABLE` and `mysqldump` all print `enum('?b',…)`, whatever the column's character set and the session's results charset. (Characters inside the BMP, such as `é` or most CJK, survive; emoji and other 4-byte characters do not.) Measured on MySQL 8.0.46 and MariaDB 11.4. sluice reads the schema from the catalog, so the target's ENUM is created with the `'?b'` label, and sluice surfaces that at schema-read time with a WARN:

```
mysql: enum labels contain '?' — likely MySQL data-dictionary
truncation of 4-byte UTF-8 (Bug 106). column_type=enum('a','?')
```

What happens to the rows:

- **Copy** (`migrate`, a `sync` cold start, `backup` fulls, mydumper): the copy reads each row's true label text, which the target's `'?b'` label rejects — a loud failure at the row's INSERT (Postgres `invalid input value for enum`, MySQL `Error 1265 Data truncated`).
- **CDC** (`sync`, `backup stream`) from a binlog source: the binlog identifies an ENUM value by position and a SET by bitmask, and sluice names them through the catalog's labels. A row using a label the catalog rewrote therefore stops the stream with `ENUM-LABEL-NOT-RECOVERABLE`, naming the table, column and label — **unless the source runs `binlog_row_metadata=FULL`**, in which case each row event carries the server's own labels and sluice uses them (checked against the catalog everywhere the catalog did not write `?`). Before v0.156.1 such a row landed on the target as the catalog's `'?b'`, silently, at exit 0; if you streamed such a column on an earlier release, compare it against the source.
- **CDC from PlanetScale / Vitess (VStream)**: vttablet renders the label through the same lossy catalog before sluice sees it, and there is no metadata to recover from, so a row using such a label stops the stream with `ENUM-LABEL-NOT-RECOVERABLE`.

A label that genuinely **is** `'?b'` on a `utf8mb4`/`utf16`/`utf32` column looks exactly like a rewritten one to the catalog, so it is refused the same way — on a binlog source, `binlog_row_metadata=FULL` lets it stream; on VStream, rename it. A `'?'` label on a charset that cannot hold a 4-byte character (`latin1`, `utf8mb3`, …) is always genuine and is never refused. Only rows that actually use such a label are refused; a table whose rows never do, or a table excluded from the stream, streams normally.

Recovery:

- On a binlog source, `SET PERSIST binlog_row_metadata = 'FULL'` and restart the stream. Note that the target's ENUM still carries the `'?b'` label, so a recovered `'😀b'` then fails loudly at the target's INSERT unless the column was widened (next bullet) or the labels renamed.
- Rename the source labels to BMP characters (ASCII, or 3-byte UTF-8) via `ALTER TABLE … MODIFY` before migrating.
- For `migrate`, `--type-override=TABLE.COL=text` emits the column as TEXT on the target so the copy's true label text lands faithfully; ENUM enforcement is lost. For `sync`, the same override also needs `binlog_row_metadata=FULL` on the source for the CDC half.
- Exclude the table.

## Columns in a non-UTF-8 character set: the change stream (`CHARSET-NOT-DECODABLE`)

A legacy schema's text columns are often declared `latin1`, `cp1251`, `sjis`, `gbk`, `utf16` or another non-UTF-8 character set. The bulk copy (`migrate`, a `sync` cold start, `backup` fulls) never sees their bytes: it reads over a `utf8mb4` connection, so the server converts every value to UTF-8 first. The change stream does see them — the binlog row image (MySQL and MariaDB) and the VStream row event (PlanetScale / Vitess) both carry a character column's value as the bytes stored in its own charset. Since v0.156.3 sluice converts those bytes to UTF-8 by the column's declared charset, so `sync`, `backup stream` and `backup incremental` carry exactly what the bulk copy carries: every 8-bit charset, `latin1` (which in MySQL is cp1252, with 0x81/0x8D/0x8F/0x90/0x9D mapped to the C1 controls), the Japanese, Korean and GB charsets, `utf16`/`utf16le`/`utf32`/`ucs2`, and `swe7` (not ASCII-compatible: `{` is `ä`). Every conversion table is graded against the server's own `CONVERT(… USING utf8mb4)` over its whole code space, on MySQL 8.0 and MariaDB 11.4. Keys are converted too, so a row updated or deleted by a non-ASCII text key is found on the target. So are columns with a `_bin` collation (`latin1_bin`, `cp1251_bin`, `utf8mb4_bin`, …), which PlanetScale / Vitess sends typed as `VARBINARY`/`BLOB`/`BINARY` (measured): they are text, not bytes — and a `_bin` `ENUM`/`SET` arrives the same way and is resolved against its labels like any other.

What refuses instead, with `CHARSET-NOT-DECODABLE` naming the table, column and charset:

- A `big5` value that is not pure ASCII: no available table matches MySQL's `big5` (the common one disagrees in 267 places), so sluice refuses rather than guess. Pure-ASCII `big5` values stream normally.
- On PlanetScale / Vitess, any non-ASCII value in a `gbk`, `big5`, `tis620` or `gb18030` column: vttablet sends those columns with no collation (ID 0), so the stream cannot tell which charset the bytes are in. Pure-ASCII values stream normally. (The binlog lanes convert `gbk`, `tis620` and `gb18030` normally — their charset comes from the catalog.)
- A byte sequence that is not a character of its declared charset (which a stored column cannot normally hold), and the few `tis620` bytes MySQL itself converts to U+FFFD (0xA0, 0xDB–0xDE).

- On PlanetScale / Vitess, a non-UTF-8 `ENUM`/`SET` value that could be read two ways: vttablet sends such a cell as UTF-8 label text during CDC but as its stored bytes during the COPY phase, with nothing to tell the two apart, so sluice takes whichever reading is one of the column's labels — and refuses when both are, which happens only when two labels are each other's mis-encoding (a latin1 `ENUM('é','Ã©')`). Rename one of the labels, or exclude the table. (The empty string MySQL stores for an invalid `ENUM` value — index 0 — is a member of every `ENUM` and streams as `''`.)

To proceed past a refusal, convert the column to `utf8mb4` on the source (`ALTER TABLE … MODIFY … CHARACTER SET utf8mb4`) **and re-snapshot the table** (`sync … --restart-from-scratch`), or exclude the table. Do not convert and resume: the stream replays history written in the old charset, so a `big5` value refuses again on the replay, and on the sources below a replayed value is decoded by the new charset until the stream reaches the `ALTER`.

**A charset change on the source while the stream is behind it.** When a stream replays history recorded before an `ALTER … CHARACTER SET` (a warm resume after downtime, a stream that was lagging, a cold start whose copy spanned the `ALTER`), the bytes it replays are in the *old* charset.

- **MySQL 8, and MariaDB under `binlog_row_metadata=MINIMAL` or `FULL`:** each row event's TABLE_MAP states the charset the value was *written* in (MySQL 8 writes it under its default `MINIMAL`), and sluice decodes by it, so the replay is exact. A collation ID that neither the source's collation table nor sluice can name refuses with `SLUICE-E-CDC-SCHEMA-REPLAY-MISMATCH`.
- **MariaDB under its default `binlog_row_metadata=NO_LOG`, and PlanetScale / Vitess,** record no written charset — vttablet reports a replayed row's *current* collation (measured on vttestserver). Rows are decoded by the column's current charset, and sluice logs `CHARSET-HISTORY-UNRECORDED` once per table with non-UTF-8 text columns.

  - **A change into a non-UTF-8 charset** is caught when the replay reaches the `ALTER`: sluice compares the charset **and collation** it sets (an explicit `COLLATE`, else the charset's default, read from the source's own collation table on MariaDB) with the shape it has been decoding the table by. A live stream still holds the old ones there; a replaying one already holds the new ones — and that refuses with `SLUICE-E-CDC-SCHEMA-REPLAY-MISMATCH`. The `ALTER` is read by the SQL parser after MariaDB-only prefix syntax (`ALTER ONLINE`, `ALTER IGNORE`, `IF EXISTS`, `WAIT n`, `NOWAIT`) is normalised, with quoted charset names and MariaDB-only collation names (`latin1_swedish_nopad_ci`, …) resolved. An `ALTER` that mentions a charset or collation but still cannot be classified refuses too, on a table with non-UTF-8 columns — it cannot tell live from replay, and a silent miss would be data loss.
  - **A change into `utf8mb4`/`utf8mb3`/`utf8`** is not refused: the old bytes are carried as sluice v0.156.2 and earlier carried every value — invalid UTF-8 (most non-ASCII) fails loudly at the target and in a backup; bytes that happen to be valid UTF-8 land as a different character. Routine UTF-8 `ALTER`s — a collation change, an ORM restating a column — therefore never refuse a live stream.

  **The rows of that table the stream emitted before the refusal were decoded by the new charset and may already be on the target, or committed to a backup chain.** So the remedy depends on the command: under `sync`, re-snapshot the table (`--restart-from-scratch`); for `backup stream` / `backup incremental`, take a fresh full backup — measured on MariaDB NO_LOG, a `backup stream` rollover committed the replayed rows to the chain before the stream reached the `ALTER` and refused, and no restore of those rows recovers them. The refusal's hint names the right one for the command that raised it.

  **A backup window that ends before the `ALTER`** is caught too. `backup incremental` and `backup stream` read the source schema at the start and the end of every window; when a column's charset changed between the two, into a non-UTF-8 charset, and the stream decoded that table's rows by the *new* charset without reaching the `ALTER`, the window refuses **before it commits** (`CHARSET-HISTORY-UNRECORDED`, `SLUICE-E-CDC-SCHEMA-REPLAY-MISMATCH`, take a fresh full backup). Measured on MariaDB NO_LOG: the two-run `backup incremental` shape that used to commit the misdecoded rows and exit 0 now refuses on the first run, and a one-transaction-per-rollover `backup stream` that used to commit 9 replayed change records commits none. A window whose rows were decoded by the *old* charset (the stream was live) is not refused.

  Switching MariaDB to `binlog_row_metadata=MINIMAL` records charsets only for binlog events written *after* the change; binlogs already written stay unrecorded, so a stream replaying them is still on this path.

  What those checks cannot see, on those sources — re-snapshot (or take a fresh full backup) yourself after such a change if a stream was behind it. **For a change from `utf8mb4` into a non-UTF-8 charset these windows are SILENT where sluice v0.156.2 was exact** (it carried the stored bytes, which for a `utf8mb4` value are its UTF-8); for a change between two non-UTF-8 charsets they are silent where v0.156.2 was loud or silent by the bytes:

  - **`sync` stopped between the replayed rows and the `ALTER`** (a restart): the next run starts with no decoded rows of the table, so when it reaches the `ALTER` it has nothing to compare, and the earlier run's rows stay misdecoded. The backup lanes' window check above does not reach `sync`;
  - **a backup window that decoded the replayed rows after an earlier window had already recorded the charset change** — the change landed in a window where the table had no rows, so the later window's start and end schema agree;
  - a table the same reader session already crossed an `ALTER` on: a second charset change to it in that session is not re-checked by the backup window check;
  - a `MODIFY`/`CHANGE` that changes a column's charset implicitly (no `CHARACTER SET`/`COLLATE` on the column, so it takes the table default), and a table-default change (`ALTER TABLE … DEFAULT CHARSET`, which converts no stored bytes by itself);
  - an online schema change — gh-ost, pt-online-schema-change, PlanetScale deploy requests and Vitess online DDL — which rebuilds a shadow table and swaps it in with a `RENAME`, so the stream never sees an `ALTER … CHARACTER SET` on the table;
  - an unclassifiable statement that names no charset keyword (`CHARACTER SET`, `CHARSET`, `COLLATE`, `CONVERT TO`) at all.

  False positives, loud: an `ALTER` that sets a non-UTF-8 column to the charset **and collation** it already has (a no-op, or a restated migration) looks exactly like a replayed one and refuses, live or not; so does a replayed collation-only change on such a column, whose bytes did not change; and so does an unclassifiable charset `ALTER` on a live stream.

**Backup chains captured before the fix** (`LEGACY-CHARSET-INCREMENT`). Chain restore, `sync from-backup` and `backup verify` WARN, naming the table and columns, for every incremental that sluice v0.156.2 or earlier captured from a MySQL-family source with a non-UTF-8 text column: those incrementals recorded each non-ASCII value they changed in such a column as `�` (U+FFFD) or as a different character, and the chain cannot give the value back. Take a fresh full backup with v0.156.3 or later, and repair rows restored from the old chain against the source. Rows the full backup copied are exact, so a full never WARNs. Restoring a bare full reads no incremental, and `export-as-parquet` exports fulls only; neither WARNs.

**If you streamed such a column on v0.156.2 or earlier:** every change-stream value of a non-UTF-8 column that was not pure ASCII was carried as its raw stored bytes. Most of those were invalid UTF-8 and failed loudly at the target — but a value whose bytes happened to be valid UTF-8 landed as a *different* character at exit 0 (a `latin1` `Ã©` landed as `é`, a `cp1251` `Г©` as `é`, every `swe7` value was wrong), a row keyed on such a value was never found by a later UPDATE or DELETE, and **`backup stream` / `backup incremental` recorded every non-ASCII value of such a column as `�` (U+FFFD) or as a different character**, silently. Compare those columns against the source for rows changed after cutover, and take a fresh full backup of any chain that captured them.

## MariaDB sources and targets

Everything above applies to MariaDB too — it is a first-class
MySQL-family flavor (`--source-driver mariadb` / `--target-driver
mariadb`, since v0.99.268), and it is *prime* legacy territory: MariaDB
only turned `STRICT_TRANS_TABLES` on by default in 10.2.4 (2017), so a
long-lived MariaDB schema carries the same zero-dates, truncated
VARCHARs, and marker columns this doc covers. The same two layers apply
unchanged: `--zero-date` on the read side, `--mysql-sql-mode` on the
write side (when the target is MySQL-family).

Three MariaDB-specific notes:

- **Pick the right flavor.** `--source-driver mariadb` (not `mysql`)
  makes sluice read MariaDB's own catalogs and binlog dialect — domain
  GTIDs for CDC ([ADR-0170](../adr/adr-0170-mariadb-flavor-phase3-cdc.md)),
  `json_valid`-based JSON detection, and the native types below. The
  connect-time fingerprint guard enforces the pairing: `mariadb`
  against a non-MariaDB server is refused loudly (its catalog variants
  would mis-read MySQL 8), and the plain `mysql` driver against a
  server that fingerprints as MariaDB gets a WARN steering you to
  `--source-driver mariadb`.
- **Native `uuid` / `inet6` / `inet4` types** are
  carried as real typed values — a MariaDB `uuid` column lands as
  Postgres `uuid`, `inet6`/`inet4` as Postgres `inet` — on both the bulk
  copy and the CDC tail
  ([ADR-0171](../adr/adr-0171-mariadb-native-uuid-inet-cdc-decode.md)),
  not as opaque text.
- **Trailing spaces and `=` (the PAD-attribute catalog gap).** Whether
  `'EU'` equals a stored `'EU '` depends on the collation's PAD
  attribute, and MariaDB's catalog column for it
  (`information_schema.COLLATIONS.PAD_ATTRIBUTE`) is version-dependent:
  absent through the entire 11.x LTS line and 12.0, added only in 12.1.
  sluice therefore keys on MariaDB's version-independent `_nopad_`
  collation-name token (plus the server's own behavior) instead of the
  catalog — so `--where` filters and collation-sensitive comparisons
  behave identically across MariaDB versions. Background:
  [the field note](https://sluicesync.com/field-notes/mariadb-no-pad-attribute-column/).

## VIRTUAL generated columns on a Postgres target (`GENERATED-VIRTUAL-PROMOTED-TO-STORED`)

MySQL lets a generated column be `VIRTUAL` (computed on read, no storage) and lets you index it. PostgreSQL gained `VIRTUAL` generated columns in 18 and refuses `CREATE INDEX` on one. sluice carries the storage class faithfully where the target can hold it and says so where it cannot: on a PostgreSQL target at 18 or newer a VIRTUAL column stays VIRTUAL; on an older target it is created STORED (the expression and every value are unchanged, only the disk-vs-compute tradeoff moves), and on PG 18+ an **indexed** VIRTUAL column is created STORED so its index can build. Each promotion is one WARN at schema apply — and at `schema preview` — marked `GENERATED-VIRTUAL-PROMOTED-TO-STORED`, naming the table and column. Nothing is dropped and no value changes; grep the log for the marker if disk growth on the target surprises you. A PostgreSQL 18 source declaring VIRTUAL (or the bare form, which 18 spells VIRTUAL) rides the same path and lands VIRTUAL on an 18+ target.

## Migrating, then tightening

A reasonable workflow for moving legacy data onto a new strict-mode
target:

1. **First run** — `sluice migrate --mysql-sql-mode='' ...` with the
   target also configured for relaxed sql_mode at the server level.
   Land the data exactly as it sits in the source.
2. **Audit** — run a SELECT pass on the target to identify
   `'0000-00-00'` dates, over-length values, and any VARCHAR(0)
   marker columns. Decide per-column whether to convert (UPDATE) or
   leave (operator policy).
3. **Re-enable strict mode** on the target server's running config.
   Subsequent application writes hit the strict mode; the migrated
   historical data stays as-is unless / until the operator updates
   it.

This staged approach treats sluice as a faithful data mover rather
than a data cleaner — which is what the loud-failure tenet wants:
sluice's job is to land the data without silent corruption, not to
silently fix data shape decisions the source's owners made
deliberately.
