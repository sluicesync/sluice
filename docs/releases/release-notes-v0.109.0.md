# sluice v0.109.0

**Nine findings from an independent audit, remediated. Every one of them was a case where sluice produced a wrong answer quietly rather than failing loudly — a filter that matched nothing, a resume position that never moved, a constraint that arrived weaker than it left.**

This completes the remediation of the 2026-08-01 audit's high-severity tier. v0.108.0 shipped its three Criticals; this release closes the silent-loss findings underneath them, plus two concurrency defects and a dump-parsing hardening.

Two of these change behaviour you may be relying on — see **Changed** — and one residual is stated rather than fixed, deliberately.

## Fixed

**A `--where` filter on an `inet` or `cidr` column silently matched nothing, and the two column types have opposite correct spellings.** Postgres renders `inet` with the prefix length omitted when it is the full width of the address family and present otherwise, while `cidr` keeps it at every width. So on the same server, in the same table:

| column type | write | not |
| --- | --- | --- |
| `inet` holding a host | `ip = '10.0.0.1'` | `ip = '10.0.0.1/32'` |
| `cidr` holding a host | `net = '10.0.0.1/32'` | `net = '10.0.0.1'` |

Postgres accepts both spellings in an ordinary query — it coerces the literal to the column type before comparing — which is exactly why this was silent: the cold-start copy succeeded, and the continuous leg then scored every change to a matching row as out-of-scope and dropped it, at exit 0. Both wrong spellings are now refused at sync-start, naming the one to use.

Worth knowing if you are checking your own filters: `SELECT ip::text` shows the mask at every width while `SELECT ip` does not. The **uncast** output is what the change stream carries, so that is the form to copy.

MariaDB's native `inet4` / `inet6` columns are a third rendering — they hold an address and cannot hold a network, so they deliver it bare and a literal naming a network is refused outright. Each source engine now declares its rendering per network type rather than having one assumed for it, and an engine that has not declared one refuses network comparisons rather than guessing.

**A continuous sync from PlanetScale, Vitess, or any trigger-CDC source into a MySQL-family target never advanced its durable resume position.** The applier withholds the resume position except at a source-transaction boundary, because a MySQL binlog resume cannot start mid-transaction. But that flag sits on the MySQL *target* while the constraint it encodes is about the *source* — and VStream and the trigger-CDC engines emit no transaction markers at all. With no boundary to hang the write on, the position stayed at its cold-start value for the life of the stream, on the default serial apply path.

The effects compound in the order you would meet them: every restart replays the whole CDC history since cold-start; trigger-CDC auto-prune, which reads that position as its frontier, never advances; and once the source's retention passes the stale position the resume is refused and forces a re-snapshot. The loop now distinguishes "mid-transaction" from "no transaction context at all", so a marker-less source checkpoints normally while the mid-transaction protection is unchanged for the case it was built for.

**MySQL's `ON UPDATE CURRENT_TIMESTAMP` was silently discarded, including MySQL to MySQL.** The attribute rides the same `information_schema` column the reader consulted only for expression defaults and generated-column storage, so it was never read and the IR had no slot for it. A column that re-stamped itself on every update simply stopped doing so on the target — on a same-engine migration, where both ends support it natively.

It is now carried and re-emitted at the correct fractional precision. Targets with no equivalent (Postgres, SQLite) warn rather than dropping it silently: migrated rows are unaffected, and what changes is that a post-cutover `UPDATE` which does not name the column leaves it stale, so the warning names that and points at the trigger idiom. The new field is deliberately excluded from the backup schema fingerprint, so it does not partition existing backup chains.

**A MySQL index prefix on a key that enforces uniqueness now refuses instead of silently weakening the constraint.** `UNIQUE KEY (email(10))` forbids two rows whose first ten characters match. Postgres has no prefix-length equivalent, so the emitted key covered the whole column and **admitted rows the source rejects** — permanently, at exit 0, with the divergence visible nowhere. On a non-unique index the prefix is a size choice with no effect on which rows are legal, so that case drops it with a warning and still migrates. All four Postgres key-emitting paths are covered, including the deferred `ADD CONSTRAINT ... UNIQUE` that a real MySQL-to-Postgres migration actually takes.

**A wide table failed outright on the Postgres batch write paths.** A multi-row `INSERT` binds one parameter per column per row, and the Postgres wire protocol carries that count as a signed 16-bit integer — so at the default 500 rows per batch, a table of **132 or more columns** exceeded the 65535 ceiling and the statement was rejected. The batch size is now clamped against the column count, as MySQL and SQLite already were.

**A data race and nil-dereference on the default native-MySQL cold-start path.** The progress ticker's per-table row estimate read the connection pool without the lock that guards it against mid-copy recovery — twenty lines below a sibling that reads the same fields under that lock and documents why it must. Because the nil check and the use were separate unlocked reads, a recovery landing between them left a nil handle to query on.

**A hostile `mydumper` dump could carry arbitrary text into a `CHARSET` / `COLLATE` position.** A backtick-quoted identifier may contain any bytes up to its closing backtick, and table-option values were taken verbatim. MySQL accepts a quoted collation value but validates the name, and `SHOW CREATE TABLE` — which mydumper dumps verbatim — always writes it unquoted, so a quoted option value cannot occur in a dump any real server produced. It is refused at the read boundary now. Quoted table, column and foreign-key names are unaffected; real dumps quote those on nearly every line.

**A concurrent map write could kill the process outright during schema translation.** The translator-gap scanner cached compiled patterns in a lazily-populated map, under a comment asserting that concurrent calls were "racy but the worst case is duplicate compilation". In Go the worst case is `fatal error: concurrent map read and map write`, which no deferred recover can catch — the process dies. The cache is gone rather than locked: the pattern set is fixed and known, so the table is built once at startup and never written.

## Changed

**`sluice sync start --restart-from-scratch` now clears the in-scope target tables before re-copying, on every source engine.** Previously it did so only for sources whose copy cannot tolerate existing rows, and left them in place for PlanetScale, Vitess and Postgres on the reasoning that the re-copy's upsert absorbs the overlap. It absorbs rows that still exist at the source; it cannot remove one the source **deleted** since the previous copy, so those rows survived on the target permanently. "From scratch" is now taken at its word.

**If you were relying on the merge behaviour, this is a change in what the command does.**

**The automatic re-snapshot deliberately does not clear the target, and now says so.** It fires without operator involvement when a stream's persisted position has been purged from the source's retention window, and dropping tables unattended on a live sync would open an empty-target window nobody asked for. It warns instead, naming what it cannot reconcile — a row deleted at the source during the gap stays on the target — and pointing at `--restart-from-scratch` for an operator who knows that window had deletes.

**That residual is real, and is stated rather than closed.** It is the trade this option buys: no unattended data loss on the target, at the cost of an unattended merge.

## Compatibility

**New refusals, each replacing a silent wrong answer.** A non-canonical `inet`/`cidr` filter literal, a MySQL index prefix on a uniqueness-enforcing key, and a backtick-quoted option value in a dump file. Each fires where sluice previously produced a wrong result quietly, so a run that starts failing was already failing — it just was not saying so.

**No backup-chain format change.** The new column attribute is excluded from the schema fingerprint, so chains written by this release restore on earlier ones and vice versa at the same format version. There is no new epoch.

**Migrate and sync state rows are byte-identical**, except where a terminal entry carries detail that was previously being dropped.

## Who needs this

- **Anyone running `sync --where` against an `inet` or `cidr` column** — check your literal against the table above; the wrong spelling matched nothing.
- **PlanetScale, Vitess, or trigger-CDC sources syncing into MySQL or MariaDB** — your resume position was not advancing.
- **MySQL-to-Postgres migrations** with prefix indexes, or with tables of 132+ columns.
- **MySQL-to-MySQL migrations** using `ON UPDATE CURRENT_TIMESTAMP`.
- **Anyone scripting `--restart-from-scratch`** — read the Changed section before upgrading.
