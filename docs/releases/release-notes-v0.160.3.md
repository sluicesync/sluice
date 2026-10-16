# sluice v0.160.3

**HIGH fix — a generated column, CHECK constraint, view or unique expression index using `TRIM` with a remove-string of more than one character behaved differently on the target than on the source, at exit 0, when it crossed between MySQL and Postgres in either direction (Bug 312).** MySQL strips the remove-string as a repeated substring and Postgres strips any character in it as a set, and both parse the same text. sluice now refuses such an object and names a remedy, instead of carrying it across. The basic MySQL → Postgres case affects every release; v0.160.1 and v0.160.2 each made more shapes silent. The v0.160.2 regression cycle found it, and two pre-tag reviews of the fix found and closed three more shapes (binary columns, views, a fixed-length `char` cast).

## Fixed

- **`TRIM` with a remove-string longer than one character is now refused between MySQL and Postgres in either direction, instead of computing different values at exit 0 (HIGH, silent; Bug 312).** `TRIM(BOTH 'xy' FROM 'yxAyx')` is `yxAyx` on MySQL and `A` on Postgres. The two meanings agree only for a remove-string of at most one unit, and over a binary column (`VARBINARY` or `BLOB`, `bytea`) the unit is a byte: a one-character `'é'` is two bytes, stripped as a pair on MySQL and one by one on Postgres. So sluice now refuses anything but a string literal of at most one byte, optionally cast to `text`, `varchar` or `character varying`, with `TRIM-REMOVE-STRING-SEMANTICS`, in a generated column, a CHECK constraint, a view, or a unique expression index. Other casts are refused because a cast can change the remove-string: on Postgres `' '::char(1)` loses its trailing space, so it trims nothing where MySQL trims spaces. The refusal names the object and a remedy that runs for it: `--expr-override` for a generated column; for the others, drop it on the source and re-create it on the target after the run, or `--exclude-table`. A remove-string that is a column (including a column named `both`, Bug 311's shape) is refused too, because its value is unknown until a row arrives. The likeliest real trigger is the Postgres whitespace idiom `trim(both E' \t\r\n' from s)`, which on MySQL strips only exact repeats of that four-character run at either end, usually nothing. The column, CHECK and index refusals reach `migrate`, `sync` cold start and its resume, `add-table`, restore, chain restore and CDC schema-forward; views, which only `migrate` and restore create, are refused at the views phase and in `schema preview`. Same-engine runs are unaffected.

## Compatibility

**Drop-in for anything that migrated correctly before.** No flag, default or error-code change, and no backup format change (`BackupFormatVersion` stays 14). **The behaviour change:** a MySQL ↔ Postgres table, view or unique expression index whose `TRIM` has a remove-string longer than one byte, or a column or expression as the remove-string, is now refused, where before it migrated with a different meaning. Tables, views and indexes are refused before they are created; a table refused at CREATE TABLE may leave tables ahead of it in the same run already created, and a view is refused at the views phase, after the copy, which loses nothing because a view holds no rows.

**Refused conservatively, though both engines would agree:** a one-character non-ASCII literal over a character column, a hex literal (`0x78`), a collated literal and `NULL` as the remove-string. Rewrite such an expression with `--expr-override`, or re-create the object on the target after the run.

**Not refused, by design:**
- **Non-unique expression indexes.** They change no value and no accepted row.
- **DEFAULT expressions.** Every migrated row, copied or replayed, carries its value explicitly. A row inserted directly on the target after cutover gets the target's meaning, as for any DEFAULT the two engines evaluate differently.
- **SQLite**, either way: its translator already refuses the two-argument `trim(x, chars)`, and a SQLite target rejects TRIM's `FROM` form loudly.

**Not run for this release:** the dispatch-only `psverify` (live PlanetScale), `d1verify` (live Cloudflare D1) and `nekiverify` (live Neki) suites, which need live, billable infrastructure.

## Who needs this — action required

- **Anyone who migrated or synced MySQL or MariaDB → Postgres, or Postgres → MySQL, with a generated column, CHECK, view or unique expression index using `TRIM` with a remove-string of two or more characters (or bytes), or a column as the remove-string.** Generated values and view results may differ from the source's, a CHECK may enforce a different condition, and a unique key may accept rows the source refuses. **Check:** compare generated columns and view results with the source's (`verify --depth count` does not see it), and search the target schema for `trim(`. **Action:** upgrade; a re-run now refuses and names the object. Rewrite it for the target and re-copy the table.
- **Everyone else:** upgrade.

---

**Install:** `brew install sluicesync/tap/sluice` · `go install sluicesync.dev/sluice/cmd/sluice@v0.160.3` · **Container:** `ghcr.io/sluicesync/sluice:0.160.3`

**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
