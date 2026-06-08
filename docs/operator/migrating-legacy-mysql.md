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
| `epoch` | Substitute `1970-01-01` (`1970-01-01 00:00:00` for DATETIME/TIMESTAMP). |

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

## What `--mysql-sql-mode=''` does NOT change

The MySQL driver-level overrides (UTF-8 charset, `time_zone='+00:00'`,
`utf8mb4` collation, the keep-alive dialer) stay regardless of
`--mysql-sql-mode`. The flag only controls the `sql_mode` SET that
sluice issues post-handshake; if you want to fully control all of
those, pass them in the DSN params and sluice respects them.

## ENUM/SET labels with 4-byte UTF-8 (emoji, supplementary plane)

This is a **MySQL server-side limitation, not a sluice bug**. MySQL's
data dictionary silently substitutes `?` for supplementary-plane
characters in ENUM/SET labels at `CREATE TABLE` time, regardless of
the column's character set. `CHARSET=utf8mb4` does not change this;
`SET NAMES utf8mb4` before the CREATE does not change this;
`mysqldump` reproduces the same loss. The label is already gone from
the source server's catalog by the time sluice ever sees the column.

Sluice surfaces this at schema-read time via a WARN line of the form:

```
mysql: enum labels contain '?' — likely MySQL data-dictionary
truncation of 4-byte UTF-8 (Bug 106). column_type=enum('a','?')
```

If the source's row data happens to contain non-`?` values for the
column, the target write will loud-fail at row INSERT (PG: `invalid
input value for enum`; MySQL → MySQL: same data-dictionary loss on
both sides so the write succeeds). Recovery options:

- `--type-override=TABLE.COL=text` — emit the column as PG TEXT (or
  MySQL VARCHAR) on the target so the actual row values land
  faithfully; ENUM enforcement is lost but the data round-trips.
- Fix the source ENUM labels to use ASCII (or 3-byte UTF-8 — CJK
  characters survive; only emoji / mathematical symbols / etc. are
  4-byte) via `ALTER TABLE` before migration.
- Ignore the warning if your source legitimately uses `?` as a label
  and the runtime doesn't surface a false positive.

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
