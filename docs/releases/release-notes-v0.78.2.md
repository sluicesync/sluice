# sluice v0.78.2 — Bug 87 hotfix: PG-target case-preserved identifier handling

**Headline:** Patch release closing Bug 87, a critical regression that silently broke PG-target migration + continuous-sync for any operator using quoted mixed-case / uppercase table names with an IDENTITY column. The fix is a single-line `quoteIdent` correction in `syncOneIdentity`; the value-add is the 32-scenario cross-engine matrix that now pins case preservation as a regression guard on both engines + both paths.

## Fixed

- **`fix(engines/postgres): Bug 87 — quote schema.table in syncOneIdentity (#43)`**

  `internal/engines/postgres/schema_writer.go:345` constructed the `tableArg` for `pg_get_serial_sequence($1, $2)` by concatenating unquoted strings:

  ```go
  // CURRENT (bug):
  tableArg := w.schema + "." + table.Name

  // FIX:
  tableArg := quoteIdent(w.schema) + "." + quoteIdent(table.Name)
  ```

  `pg_get_serial_sequence(table text, column text)` parses its first argument as identifier text — and per PG's identifier rules, **unquoted identifiers fold to lowercase**. For any target table with a case-preserved name + an IDENTITY or SERIAL column (e.g. `CREATE TABLE "Widgets" ("id" BIGSERIAL PRIMARY KEY)`), `tableArg` became `public.Widgets`, which PG interpreted as `public.widgets` and raised `relation "public.widgets" does not exist (SQLSTATE 42P01)`. The MAX read on the line just above (line 328) already quoted correctly via `quoteIdent`; the fix is the same single-statement consistency.

  **Downstream symptom (initially misdiagnosed as a separate bug):** the CDC streamer's `coldStart` invokes the same `SyncIdentitySequences` phase as the Migrator. When this errored, the streamer's runOnce loop hit its retry backoff and never transitioned to CDC mode — operator-visible as "bulk copy complete (rows arrive) + nothing further replicates", a silent-loss-class shape. The task #42 matrix subagent reported this as "Bug 2 — PG-target CDC silently drops post-snapshot INSERTs"; **Phase A instrumentation in the v0.78.2 hotfix (six DEBUG probes on the pgoutput → applier dispatch chain) ruled out all four hypothesized fold-points in the CDC apply path and traced the symptom back to the failed identity-sync phase.** The one-line `quoteIdent` fix closes both the loud-Migrator-abort and the silent-streamer-stall.

## Tests

- **`test(pipeline): Bug 87 — cross-engine case-preservation matrix (32 scenarios)`**

  Three new integration test files pin a comprehensive case-preservation matrix per the Bug 74 "pin the class, not the representative" lesson:

  - `internal/pipeline/migrate_case_preservation_pg_integration_test.go` (+382 LOC) — PG→PG bulk-copy + CDC, plus shared `caseShape` / `caseShapes` matrix helpers
  - `internal/pipeline/migrate_case_preservation_mysql_integration_test.go` (+485 LOC) — MySQL→MySQL bulk-copy + CDC, plus dedicated `startMySQLCaseSensitive` / `startMySQLBinlogCaseSensitive` testcontainer helpers (both forcing `--lower-case-table-names=0` so the test is hermetic against per-OS folding defaults)
  - `internal/pipeline/migrate_case_preservation_cross_integration_test.go` (+294 LOC) — PG↔MySQL bulk-copy + CDC (both directions)

  Matrix: **4 directions × 4 shapes × 2 paths = 32 scenarios** as subtests.

  | Direction | Bulk-copy (Migrator) | CDC (Streamer) |
  |---|---|---|
  | PG → PG | 4/4 PASS | 4/4 PASS |
  | MySQL → MySQL | 4/4 PASS | 4/4 PASS |
  | PG → MySQL | 4/4 PASS | 4/4 PASS |
  | MySQL → PG | 4/4 PASS | 4/4 PASS |

  Shapes: `lowercase_simple` (control), `UPPERCASE_ONLY`, `MixedCase`, `Snake_With_Caps`.

  Without the Bug 87 fix, the 12 PG-target case-preserved scenarios fail with SQLSTATE 42P01 (Migrator) or silent-no-replication (Streamer). The matrix now pins case preservation as a regression guard on both engines and both paths.

## Compatibility

- **Drop-in upgrade from v0.78.1.** Pure bugfix; no flag surface change.
- **Severity a.** v0.78.1 (and every prior release that shipped the `SyncIdentitySequences` codepath) silently broke PG-target migration + streaming for any operator who used quoted mixed-case or uppercase table names with an IDENTITY column. The Migrator-side surface (loud SQLSTATE 42P01 abort) was at least visible; the Streamer-side surface (silent-loss after a successful bulk copy) is exactly the user-trust-gates-throughput class the project's CLAUDE.md tenets are designed against.

## Who needs this

- **Any PG-target operator** with a schema containing quoted mixed-case identifiers (`"Widgets"`, `"UserName"`) plus an IDENTITY / SERIAL column. v0.78.1 and earlier silently failed to migrate / stream into these schemas — Migrator aborted with a confusing "lowercase table doesn't exist" error, Streamer completed bulk-copy then silently stalled.
- **Operators migrating from SQL Server or any case-preserving source** (where quoted identifiers are the default) into PG: this is the audience most likely to have hit Bug 87 in practice.
- **Anyone NOT using mixed-case identifiers** sees no change.

## The three-phase protocol value: Bug 2 was a downstream symptom

The task #42 matrix subagent filed two bugs against v0.78.1: Bug 1 (loud Migrator abort) and "Bug 2" (silent-CDC-loss). A speculate-and-patch approach would have looked for a second production bug in the CDC apply path. **Phase A instrumentation (six `slog.Debug` probes from pgoutput emit through buildInsertSQL and txExec) ruled out all four hypothesized fold-points and traced the symptom back to Bug 1's coldStart-abort retry-loop.** The CLAUDE.md three-phase protocol is the reason this hotfix shipped as a single-line fix rather than two unrelated patches in the same release.

The discipline cost: one extra subagent cycle for Phase A instrumentation. The discipline win: an accurate root-cause + a fix whose minimality is easy to review + no risk of a wrong "Bug 2 fix" sitting in the codebase as confusing dead code.

## Cross-references

- [v0.78.1 release notes](https://github.com/orware/sluice/releases/tag/v0.78.1) — the prior release; v0.78.2 builds on the Bug 86 hotfix arc
- Bug 74 lesson: see `CLAUDE.md` § *Pin the class, not the representative* — the test matrix expansion is the regression-guard counterpart
- Three-phase protocol: `CLAUDE.md` § *Debugging non-obvious failures*
