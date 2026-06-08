# sluice v0.99.19

**CRITICAL fix — legacy MySQL zero and partial dates were silently migrated as the *wrong* calendar date.** If a MySQL source column held a `'0000-00-00'`, a zero-month date like `'2026-00-15'`, or a zero-day date like `'2026-06-00'` (all storable under a relaxed source `sql_mode`), the `migrate` / snapshot bulk-copy read it through the driver's `parseTime=true`, which silently *normalized* the partial date into a neighbouring real one (`'2026-06-00'` → `2026-05-31`) — so the migration carried a different, plausible-looking date and **exited 0**. sluice now reads these as raw text, validates them, and refuses loudly by default. **Behavior change:** `'0000-00-00'` previously became NULL silently; it now refuses by default — see "Who needs this" below.

## Fixed

- **CRITICAL: MySQL zero/partial dates silently corrupted on the bulk-copy path.** A source column holding a legacy invalid date — all-zero `'0000-00-00'`, zero-month `'2026-00-15'`, or zero-day `'2026-06-00'`, all storable under a relaxed source `sql_mode` — was read through go-sql-driver's `parseTime=true`, which hands the value to Go's `time.Date(2026, 0, 0, …)`. Go *normalizes* a zero component into a neighbouring real date (`'2026-06-00'` → `2026-05-31`, `'2026-00-00'` → `2025-11-30`), so the migration carried a **different, plausible-looking date** and reported success. Only the all-zero case was handled sanely (→ NULL); every partial date was silently wrong. (The CDC binlog tail already surfaced these loudly; only the bulk-copy read path corrupted them.)

  **Fix:** sluice now reads `DATE`/`DATETIME`/`TIMESTAMP` columns as their raw text (`CAST(... AS CHAR)`) so the decode layer sees MySQL's literal value before any `time.Time` is constructed, and resolves zero/partial dates per a new `--zero-date` policy:
  - `--zero-date=error` (**default**) — refuse loudly, naming the column. Nothing silently wrong leaves the source.
  - `--zero-date=null` — carry the value as SQL `NULL` (refused loudly for a `NOT NULL` column).
  - `--zero-date=epoch` — substitute `1970-01-01`.

  A genuinely out-of-range but non-zero date (month 13, Feb 30) stays a hard error regardless of the flag. Pinned by unit tests across the full temporal family × every zero shape × each policy, plus an integration test that ground-truths the live-driver normalization against real MySQL 8.0.

- **Temporal primary keys now paginate by the real date column on the chunked copy path.** The zero-date fix projects temporal columns as `CAST(... AS CHAR)`, which aliases a column to its own name. On the >100k-row keyset-paginated bulk copy, an unqualified `ORDER BY` then sorted by that text alias while the cursor predicate compared the real date column — consistent only because ISO date strings sort in calendar order, and it defeated the primary-key index (forced a filesort). The cursor and ordering clauses are now table-qualified so both bind the real column: date-typed throughout and index-ordered. No user-visible behavior change for valid data; caught by the value-fidelity review of the zero-date fix. Pinned by a SQL-shape unit test plus `DATE`/`DATETIME(6)` primary-key pagination integration tests across page boundaries.

## Compatibility

- No breaking API changes. New CLI flag `--zero-date={error,null,epoch}`, default `error`.
- **Behavior change:** prior versions silently mapped `'0000-00-00'` to NULL; it now refuses by default. Pass `--zero-date=null` to restore the old mapping.
- Affects MySQL **sources** on the `migrate` / snapshot path. Postgres sources are unaffected. The PlanetScale/Vitess VStream CDC path does not yet honor `--zero-date` (tracked follow-up; it fails loudly rather than corrupting).

## Who needs this — action required

- **Anyone migrating a legacy MySQL database that may contain zero or partial dates** (the classic `'0000-00-00'`, or zero-month/zero-day values written under a non-strict `sql_mode`). On v0.99.19 such a migrate now **refuses by default**, naming the offending column, instead of silently writing a wrong date. Decide per database: `--zero-date=null` (carry as NULL), `--zero-date=epoch` (substitute `1970-01-01`), or fix the source data. See [docs/operator/migrating-legacy-mysql.md](https://github.com/sluicesync/sluice/blob/main/docs/operator/migrating-legacy-mysql.md).
- **If you migrated such a database on v0.99.18 or earlier, re-verify the affected date columns** — partial dates may have been silently shifted to a neighbouring date.

---

**Install:** `brew install sluicesync/tap/sluice`  ·  `go install sluicesync.dev/sluice/cmd/sluice@v0.99.19`  ·  **Container:** `ghcr.io/sluicesync/sluice:0.99.19`
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
