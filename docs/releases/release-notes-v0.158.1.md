# sluice v0.158.1

**`sluice verify --depth sample` no longer reads several kinds of corrupted target as clean (HIGH, silent false clean), and a backup refusal now names the runnable `binlog_row_metadata` fix for MariaDB (LOW).** The headline verify gap is that sample verify hashed each side's columns in that side's own declared order. A target whose values sit under swapped column names, exactly what the column RENAME swap fixed in v0.158.0 leaves behind on a target that went through it, therefore reported `OK … clean`. While fixing it, two independent review rounds found more ways the same hash read a corrupted target as clean, on MySQL, MariaDB and Postgres; all are fixed. **If you ran `sluice verify --depth sample` on v0.158.0 or older and got a clean result, that result did not rule out the damage below. Re-run it on this release.**

<!-- notes-claims-exempt: VERIFY-SAMPLE-COLUMN-ORDER -- audit-backlog item name; it lives only in docs/dev/audit-backlog.md -->

## Fixed

**Sample verify compares columns by name (HIGH, silent false clean).** Both sides now read one shared plan: the source's column names, sorted, keyed by the source's primary key. A swapped target mismatches, and a target that only declares the same columns in another order stays clean.
- A source column the target lacks fails the table as `COLUMN-MISMATCH` (exit 1, listed in JSON `columns_missing_on_target`).
- A column only the target has is listed in `columns_extra_on_target` and left out of the hash; it does not fail the table.
- Every JSON table entry now carries a one-word `status`: `ok`, `count-mismatch`, `column-mismatch`, `sample-mismatch` or `unverified`. A column mismatch sets none of the older fields, so read `status`.

The same review closed these other ways a differing value could hash as clean:
- **NULL vs empty string, and separators.** NULL and an empty string hashed alike, and a value containing `|` could borrow its neighbour's separator.
- **MySQL and MariaDB `FLOAT`.** It rendered at 6 significant digits, so 1234567 and 1234568 hashed alike, and two such `FLOAT` keys collapsed into one. It is now read through a lossless promotion to `DOUBLE`.
- **MySQL binary values.** A `GEOMETRY`, `BIT`, `BINARY`, `VARBINARY` or `BLOB` value that is not valid UTF-8 rendered as nothing, so any two such values hashed alike. Binary columns are now digested from their raw bytes, and binary keys render as hex.
- **MySQL 8 oversized `JSON`.** A document larger than `max_allowed_packet` casts to an empty string, so two different large documents hashed alike. That table now refuses, naming the row; verify it with a larger `max_allowed_packet`.
- **Postgres session settings.** Two endpoints with different `DateStyle` defaults could read a day/month-swapped `DATE` as clean. `DateStyle`, `IntervalStyle`, `TimeZone` and `bytea_output` are now pinned for the hash.

One loud failure is also fixed: Postgres `--strict-hash` failed on every table with a `bytea` column (`invalid input syntax for type bytea`), and on a `SQL_ASCII` database on any non-UTF-8 byte.

The new tests corrupt one type family at a time on a twin table and require a mismatch, on MySQL 8.0, MariaDB 11.4 and Postgres 16: `TestSampleRowHashes_FamilyMatrix`, `TestSampleRowHashes_FamilyMatrix_MariaDB`, `TestSampleRowHashes_FloatKeyRendersExactly`, `TestSampleRowHashes_ValueLargerThanMaxAllowedPacket`, `TestSampleRowHashes_SessionRenderingPinned`, `TestSampleRowHashes_StrictHashOnSQLASCIIDatabase` and `TestVerify_SampleColumnOrder_MySQL` / `_Postgres`. **Affected releases:** every release with `verify --depth sample`.

**A backup refused for a schema change behind the stream names the runnable `binlog_row_metadata=FULL` statement for its source (LOW, message; Bug 302).** When `backup incremental` or `backup stream` hit the v0.158.0 look-ahead refusal (`SLUICE-E-CDC-SCHEMA-REPLAY-MISMATCH`), the backup remedy dropped the source-specific statement. A MariaDB operator was never told that MariaDB has no `SET PERSIST`, or that `SET GLOBAL` alone reverts on restart. The backup refusal now carries the same statement `sync start` prints: `SET PERSIST …` on MySQL, and `SET GLOBAL …` plus the `[mariadbd]` option-file line on MariaDB. Pinned by `TestBackupCaptureReaderErr_KeepsTheReadersFlavorStatement`. **Affected releases:** v0.158.0.

## Compatibility

- **Exit codes and refusals.** `verify --depth sample` can now fail where it used to pass:
  - `COLUMN-MISMATCH` when the target lacks a source column;
  - a refusal naming the row for a MySQL 8 `JSON` value past `max_allowed_packet`;
  - genuine mismatches it used to miss.
- **JSON output.** The per-table `status` field is new and additive.
- **Unchanged:** no format, flag or default changes. `--depth count` is unchanged and still cannot see a column swap.
- **Cost.** Sample verify now digests each MySQL value separately. The extra work applies only to the sampled rows per table, so it is negligible.
- **Known residuals:**
  - A target column whose type differs from the source's, such as a different `DECIMAL` scale, or two Postgres databases with different encodings, still give a false mismatch (loud).
  - Sharded Vitess / PlanetScale sample verify, whose new hash expressions run inside the vtgate `ORDER BY`, is untested.

**Not run for this release:** the dispatch-only `psverify` (live PlanetScale), `d1verify` (live Cloudflare D1) and `nekiverify` (live Neki) suites, which need live infrastructure and run only on the operator's call.

## Who needs this

- **Anyone who relied on a clean `sluice verify --depth sample`** to confirm a target, especially after a column RENAME swap, or over MySQL `FLOAT`, binary or spatial columns, or `JSON` documents: re-run it on this release.
- **MariaDB operators taking backups under the default `binlog_row_metadata=NO_LOG`:** a refused backup now tells you the full fix, including the option-file line.
- **Everyone else:** upgrade at leisure; nothing to check.

---

**Install:** `brew install sluicesync/tap/sluice` · `go install sluicesync.dev/sluice/cmd/sluice@v0.158.1` · **Container:** `ghcr.io/sluicesync/sluice:0.158.1`

**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
