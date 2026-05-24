# sluice v0.78.3 — Bug 88 hotfix: MySQL DELETE Before-image narrowing

**Headline:** Patch release closing Bug 88, a Bug-8-equivalent silent-loss class **discovered by the v0.78.2 #44 hard-delete family-matrix pin doing exactly the job the Bug 74 discipline requires it to do.** Under MySQL `binlog_row_image=MINIMAL` (and `NOBLOB`-with-BLOB-non-PK), the MySQL CDC reader emitted DELETE Before-images with `nil` non-PK columns; the applier's WHERE clause used those nils as `IS NULL` predicates; DELETEs matched zero rows on the target; ADR-0010 idempotency absorbed the miss; position advanced; **source DELETEs silently didn't propagate**. Mirrors PG's already-shipped `filterDeleteBefore` narrowing.

## Fixed

- **`fix(engines/mysql): Bug 88 — narrow DELETE Before-image to PK columns (#51)`**

  The bug existed since the MySQL CDC reader first shipped. The structural answer to silent-DELETE-loss is **always-emit-DELETE-with-narrowed-Before-image**, which PG already does via `internal/engines/postgres/cdc_reader.go:filterDeleteBefore`. The MySQL CDC reader didn't, so the applier received Before-images with non-PK nils, and `buildWhereClause` (`internal/engines/mysql/change_applier.go:1240-1248`) emitted SQL like:

  ```sql
  DELETE FROM `widgets` WHERE `id` = ? AND `name` IS NULL AND `payload` IS NULL
  ```

  The target row's `name` and `payload` were the original non-null values, so the WHERE predicate failed; `rows_affected=0`; ADR-0010 idempotency contract advanced position; silent loss.

  **The fix** is a ~50-LOC mirror of PG's pattern in `internal/engines/mysql/cdc_reader.go`:

  ```go
  // filterDeleteBefore narrows the DELETE Before-image to PK columns only,
  // mirroring internal/engines/postgres/cdc_reader.go:filterDeleteBefore.
  // Required because MySQL binlog under binlog_row_image=MINIMAL (and NOBLOB
  // when BLOB non-PK columns exist) carries nil for non-PK columns; passing
  // those nils to buildWhereClause produces IS NULL predicates that miss the
  // target row → DELETE rows_affected=0 → silent loss.
  func filterDeleteBefore(tbl *tableSchema, before ir.Row) ir.Row { ... }
  ```

  Plus a `PrimaryKey []string` field on the `tableSchema` cache, populated by `schema_reader.go:loadPrimaryKeyDB`. The applier's `buildWhereClause` is unchanged — it produces correct SQL when given only identity-key columns. **The fix locus is the CDC reader emit path; Phase A instrumentation (six `slog.Debug` probes from `DELETE_ROWS_EVENT` emit through `txExec` rows-affected) confirmed this verbatim before the fix landed**, ruling out the applier-side speculation that the bug report initially considered as a hypothesis.

  PK-less fallback: when a table has no primary key, `filterDeleteBefore` returns the full Before-image (same fallback shape as PG's helper). The applier's existing PK-less DELETE handling takes over from there; this fix doesn't alter PK-less behavior.

## Tests

- **`test(pipeline): 4 matrix cells un-skipped in cdc_delete_matrix_mysql_integration_test.go`** — `binlog_row_image=MINIMAL × {plain-delete, update-then-delete}` and `binlog_row_image=NOBLOB × toast-delete`. All previously t.Skip()'d with skipReason citing the Bug 88 finding; all now PASS post-fix. The full MySQL matrix is now green (7/7 cells).
- **`test(engines/mysql): TestFilterDeleteBefore in cdc_reader_test.go`** — Unit pin with 5 sub-cases (MINIMAL, FULL, NOBLOB-with-TOAST, composite-PK, PK-less fallback) ensuring the narrowing behavior is regression-guarded at unit level. Cheaper than the integration matrix; surfaces any regression in `filterDeleteBefore` immediately.
- **`test(engines/mysql): cdc_reader_integration_test.go assertion update`** — `TestCDCReader_BasicChangeStream` previously asserted `delete.Before["email"]` carried the original value; updated to assert PK-only narrowing (`id` present, `email`/`active` absent) per the new policy. Composite-PK delete test already only asserted PK columns and still passes.

## Docs

- **ADR-0057 closure** (`docs/adr/adr-0057-hard-delete-semantics-across-engines.md`) — appended "Bug 88 closure (v0.78.3, 2026-05-24)" subsection under the MySQL matrix section. Documents the fix locus, the 4 un-skipped cells, the unit pin, and a scope note flagging VStream (PlanetScale flavor) DELETE handling as a follow-up audit — VStream's DELETE event format may need separate verification since it's not native MySQL binlog.

## Compatibility

- **Drop-in upgrade from v0.78.2.** Pure bugfix; no flag surface change.
- **Severity a — silent-loss class.** Any operator running MySQL→{MySQL, PG} streams against a source with `binlog_row_image=MINIMAL` (or `NOBLOB` with BLOB/TEXT non-PK columns) silently lost DELETEs prior to v0.78.3. `docs/dev/notes/prep-change-applier.md:26` declares `binlog_row_image=FULL` as the only supported config, so this was technically "out-of-spec config silent-loss" — but operators routinely set MINIMAL for binlog disk-space reasons, and no preflight refused. v0.78.3 closes the gap regardless of declared support; the matrix tests + unit pin together prevent the class regressing.

## Who needs this

- **MySQL source operators on `binlog_row_image=MINIMAL` or `NOBLOB`** with BLOB/TEXT columns in any tracked table. v0.78.2 and earlier silently dropped DELETEs in these configurations.
- **MySQL source operators on `binlog_row_image=FULL`** see no observable change — `FULL` already carries all columns, so the narrowing is a no-op for them.
- **PG-source operators** see no change — PG had this fix since the early MySQL/PG parity work.

## The Bug 74 lesson value: matrix discipline found this

The #44 matrix pin (v0.78.2's regression-guard for the always-emit-DELETE property) explicitly exercised the `binlog_row_image` × shape axes per Bug 74's "pin the family, not the representative" lesson. Without the matrix, the bug would have been: invisible in unit tests (which used `FULL`), invisible in same-engine PG integration tests, invisible in MySQL-FULL tests, and only surfaced when an operator first reported lost DELETEs in production. **The matrix exists exactly because this class of finding is the expensive-to-discover-post-release class.**

This is the third release in 4 days (v0.78.0 → v0.78.1, v0.78.1 → v0.78.2, v0.78.2 → v0.78.3) where a family-matrix test surfaced a class-of-bug that the previous representative-only test missed. The discipline cost: writing the matrix. The discipline win: catching the bug in CI rather than in `BUG-CATALOG.md`.

## Cross-references

- [v0.78.2 release notes](https://github.com/orware/sluice/releases/tag/v0.78.2) — the prior release that shipped the #44 matrix pin
- [ADR-0057](https://github.com/orware/sluice/blob/main/docs/adr/adr-0057-hard-delete-semantics-across-engines.md) — hard-delete semantics across engines (Bug 88 closure section appended)
- Bug 8 (PG's original `filterDeleteBefore`) and Bug 74 ("pin the class") — see `CLAUDE.md`
- Three-phase protocol used for the Phase A instrumentation — see `CLAUDE.md` § *Debugging non-obvious failures*
