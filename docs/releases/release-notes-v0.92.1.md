# sluice v0.92.1

## ⚠️ INCOMPLETE — see v0.92.2 for the real fixes

A post-release verification cycle against the published v0.92.1 binary on a real PG↔MySQL rig found that several of the closures this release announces **do not actually work in practice**. **Root cause confirmed by in-process probe (2026-05-30):**

- **The `sql_mode` plumbing IS correct.** The driver `SET sql_mode = 'STRICT_TRANS_TABLES,...'` does reach the MySQL session; `@@SESSION.sql_mode` on a sluice-opened connection contains every strict mode. Direct `INSERT` of an 80-digit NUMERIC into `DECIMAL(65,30)` correctly errors `1264 Out of range value` under that session.
- **But the bulk-copy path uses `LOAD DATA LOCAL INFILE` via `(@var) SET col=@var` indirection, and that path silently bypasses strict mode for type-conversion errors.** Bug 102 (NUMERIC overflow → silent MAX clamp), Bug 103 (TIMESTAMPTZ out-of-range → silent `'0000-00-00 00:00:00'`), and almost certainly anything that produces a per-row conversion warning all flow through the same hole. `@@warning_count` IS non-zero after the load — so a post-load warning check is a viable detection point.
- **Bug 97 (Stage 1+2 sync-CDC OID)** — the CDC reader fix is correct, but the **applier-side type translator** has the same-class gap; first DML touching `money` / `xml` / `tsvector` / etc. still crashes the stream with `pipeline: apply changes: postgres: applier: translate <col>: postgres: unsupported data_type "<type>"`. (Originally closed in v0.92.0.)
- **Bug 106 ENUM 4-byte UTF-8 (emoji) → `?`** — still corrupts during MySQL schema read; the read path needs investigation (the connection's collation IS `utf8mb4_general_ci` at the handshake but the schema metadata's character set is independent of that).
- **`--mysql-sql-mode` flag** — kong tag auto-derives `--my-sqlsql-mode` instead of `--mysql-sql-mode` (acronym-handling typo). Single-character kong-tag fix.

**v0.92.2 ships the real fixes** ([PR #115](https://github.com/orware/sluice/pull/115)): (1) post-LOAD-DATA `@@warning_count` check that refuses loudly on any conversion warning (closes Bug 102 + Bug 103 properly); (2) applier-side type-translator allowlist matching the CDC reader's OID switch (closes Bug 97); (3) kong `name:"mysql-sql-mode"` tag fix (canonical flag name now matches help text); (4) Bug 106 surfaced — turns out it's a **MySQL server-side data-dictionary limitation, NOT a sluice connection-charset problem**: MySQL silently truncates supplementary-plane UTF-8 in ENUM labels at `CREATE TABLE` time regardless of column charset, and `mysqldump` reproduces the same loss — v0.92.2 emits a schema-read WARN and documents recovery in `docs/operator/migrating-legacy-mysql.md`; (5) Bug 109 generated-column redact silent-leak preflight (originally PR #114, bundled here).

**Bugs verified-closed in v0.92.1 that still hold:** 105 (`randomize:int` range preflight), 107 (`VARCHAR(0)`/`CHAR(0)` emit refusal). If your migration is MySQL-target and your source could produce conversion warnings on any column, **wait for v0.92.2** — silent-loss is real and unpatched on v0.92.1.

---

# sluice v0.92.1 — MySQL strict-mode + four silent-loss closures

**Headline:** Deep bug-finding sweep #2 against v0.92.0 surfaced three CRITICAL silent-loss bugs in PG → MySQL — numeric overflow silently clamping, TIMESTAMPTZ out-of-range silently zeroing, and `--redact ... randomize:int` silently clamping (defeating PII randomization). All three shared one root cause: sluice inherited the MySQL server's `sql_mode` instead of forcing strict mode. v0.92.1 closes that class. It also adds an operator-facing fix for `VARCHAR(0)` legacy-MySQL columns (raised by an operator question during the release window) and a CLI flag plus operator docs for migrating legacy data that the strict default would otherwise refuse. **If your destination is MySQL or you migrate from legacy MySQL schemas, read this release's notes carefully.**

## Fixed

- **`fix(mysql): force strict sql_mode + utf8mb4 collation on every connection (Bugs 102 + 103 + 106 closure)`** — pre-fix sluice inherited the MySQL server's `sql_mode` and connection collation, which on dev / older / managed deployments allowed three CRITICAL silent-loss classes:
  - **Bug 102**: PG NUMERIC overflowing MySQL DECIMAL(65,30) silently clamped to MAX every row (one constant for every overflowing row → silent data loss).
  - **Bug 103**: PG TIMESTAMPTZ outside MySQL TIMESTAMP range silently corrupted to `'0000-00-00 00:00:00'`. MySQL itself rejects this on direct INSERT (Error 1525/1292); sluice's session was suppressing the strictness.
  - **Bug 106**: MySQL ENUM labels containing 4-byte UTF-8 (emoji, supplementary-plane glyphs) silently replaced with `?` during MySQL → PG schema-read; the corruption then triggered a loud row-insert failure on the target. The visible failure masked the silent label corruption.
  
  v0.92.1 forces `sql_mode='STRICT_TRANS_TABLES,NO_ZERO_DATE,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO'` and `Collation='utf8mb4_general_ci'` on every sluice MySQL connection. Silent overflows / zero-dates / charset corruption surface as the loud MySQL error path that sluice's existing applier-retry logic already handles. DSN-level `?sql_mode=...` still wins absolutely (operators with per-direction needs set each DSN's own sql_mode).

- **`fix(pipeline): randomize:int target-column range preflight (Bug 105 closure)`** — `--redact='COL=randomize:int:LO,HI'` whose `LO,HI` exceeded the target column's representable integer range previously had its values silently clamped to the column MAX every row at apply time, **defeating PII randomization** (every row got the same surrogate). The new preflight check compares the rule's `Min`/`Max` against the source column's `ir.Integer{Width, Unsigned}` range and refuses loudly via the new `errRedactRandomizeRangeOverflow` sentinel if out-of-range. Works for int8 / uint16 / int24 (MySQL MEDIUMINT) / int32 / uint32 / int64. 8 new unit pins.

- **`fix(postgres): refuse VARCHAR(0)/CHAR(0) at emit with operator-actionable recovery hint (Bug 107)`** — MySQL has allowed `VARCHAR(0)` as a marker column for 20+ years (the column exists, can hold NULL or `''`, that's it); the WHMCS-shaped corpus surfaced this concretely during the v0.92.1 development window. PG refuses zero-length char/varchar at CREATE TABLE with `length for type varchar must be at least 1` (SQLSTATE 22023). Pre-fix sluice forwarded the VARCHAR(0) into the PG schema-apply DDL and crashed with that raw error AFTER the cold-start preamble had already run — late, ugly, recovery non-obvious. v0.92.1 catches this at sluice's PG `emitColumnType` and refuses loudly with the recovery flag named (`--type-override=TABLE.COL=text` to land as PG TEXT — the common workaround — or `--type-override=TABLE.COL=boolean` if the column is used as a flag). Same fix covers `CHAR(0)`.

## Added

- **`feat(cli): --mysql-sql-mode top-level flag (legacy-data escape hatch)`** — the strict-by-default closes the silent-loss class but **would refuse legacy MySQL data** (zero-dates `'0000-00-00'`, silently-truncated VARCHAR values, `'2020-00-15'`-shaped partial zero dates) that pre-MySQL-5.7 schemas commonly carry. The 20+ year-old WHMCS-shaped corpus is the canonical example. The new `--mysql-sql-mode` Globals flag is the escape hatch: pass `--mysql-sql-mode=''` (explicit empty) to fall through to the server's default sql_mode for migrating such data; pass a specific comma-separated mode list to force exactly those modes. See [`docs/operator/migrating-legacy-mysql.md`](https://github.com/orware/sluice/blob/v0.92.1/docs/operator/migrating-legacy-mysql.md) for the full migration story including a quick-reference table mapping legacy data shapes to the right flag.

- **New operator guide:** [`docs/operator/migrating-legacy-mysql.md`](https://github.com/orware/sluice/blob/v0.92.1/docs/operator/migrating-legacy-mysql.md) — full migration story for legacy MySQL schemas: zero-dates, silently-truncated VARCHARs, `VARCHAR(0)` marker columns, the staged migrate-then-tighten workflow, and the quick-reference table mapping each legacy data shape to which flag handles it.

## Compatibility

- **Patch bump (v0.92.1).** Drop-in from v0.92.0 except for the documented behavior changes below.
- **Behavior changes** (all designed to surface previously-silent loss):
  - Strict `sql_mode` and `utf8mb4_general_ci` collation now forced on every MySQL connection. Operators with legacy data that the strict modes would refuse pass `--mysql-sql-mode=''` (escape hatch).
  - `randomize:int` rules whose Min/Max exceed the target column's range now refuse at preflight instead of silently clamping. Operators must narrow Min/Max or widen the column via `--type-override`.
  - `VARCHAR(0)` / `CHAR(0)` columns now refuse loudly at sluice's PG emit step with the recovery hint named, instead of crashing mid-CREATE TABLE with raw PG error.

## Who needs this

- **Anyone with MySQL as a sluice target** — three previously-silent corruption classes (Bug 102 NUMERIC, Bug 103 TIMESTAMPTZ, Bug 106 ENUM emoji) now surface as the loud MySQL error path. **Upgrade.**
- **Anyone using `--redact ... randomize:int`** — the silent-clamp PII-compliance failure (Bug 105) is closed at preflight.
- **Anyone migrating legacy MySQL schemas** (20+ year old, WHMCS-shaped, pre-MySQL-5.7) — read the new `docs/operator/migrating-legacy-mysql.md`; you'll want `--mysql-sql-mode=''` and possibly `--type-override` for any `VARCHAR(0)` markers.
- **Everyone else** — no action needed.

## Coming next

The same investigation campaign that surfaced 102/103/105/106 + 107 (deep sweep #2) has continued. Sweep #3 ran the full ~37-minute budget across seven scenario angles and surfaced eight more bugs (108-115), four of which are CRITICAL silent-loss class. Sweep #4 is now in flight targeting the five out-of-budget items from sweep #3. **v0.92.2 will close the highest-impact silent-loss class from sweep #3: Bug 109 generated-column redact silent leak, Bug 112 source-side RENAME mid-stream silent CDC drop, Bug 113 PG DOMAIN constraint silent downgrade, Bug 108 YAML/CLI redaction override order.** Until that release lands, the open caveats are documented in `sluice-testing/BUG-CATALOG.md`.
