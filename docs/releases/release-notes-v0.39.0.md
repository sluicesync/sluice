# sluice v0.39.0 — Translator-gap preflight scan in `sluice schema preview`

**Operators running cross-engine MySQL → Postgres migrations now see an upfront advisory** listing every MySQL expression-body pattern sluice's translator catalog deliberately doesn't auto-rewrite. Before v0.39.0, the deferred rules surfaced as either loud failures at PG apply time (visible but late) or silent runtime divergences (invisible until row data ships through and a downstream consumer notices). The scan brings them forward into the preview, with operator-actionable workaround hints.

## Added

- **`internal/translate/gaps.go` — translator-gap scanner.** `ScanMySQLToPGGaps(schema, sourceEngine, targetEngine, enabledExt)` walks every `DefaultExpression` body, `Column.GeneratedExpr`, and `CheckConstraint.Expr` whose dialect tag is `mysql`, and returns a sorted list of `Gap` entries for any pattern matching the catalog's 7 deliberately-deferred rules:

| Pattern | Rule | Severity | Surfaces as |
|---|---|---|---|
| `GREATEST` / `LEAST` | #11 | silent | PG ignores NULL args; MySQL propagates. Output divergence is silent — no PG error. |
| `REGEXP_LIKE` | #13 | silent (PG 15+) | POSIX vs ICU regex flavour. Patterns with lookaheads / named groups / Unicode-property classes diverge silently. |
| `FIND_IN_SET` | #21 | loud | PG parse failure: no portable equivalent in CHECK / GENERATED contexts. |
| `CONVERT_TZ` | #23 | loud | PG parse failure: no `CONVERT_TZ` in PG core. |
| `INET_ATON` / `INET_NTOA` | #29 | loud | PG parse failure: no portable equivalent without a custom function. |
| `SHA1` / `SHA2` | #10 | loud | Requires pgcrypto. Suppressed when `--enable-pg-extension pgcrypto` is set since the v0.38.0 rewrite ships. |

  Detection is case-insensitive with word-boundary matching (rejects `IS_GREATEST_HIT(` etc.). Returns `nil` for non-MySQL-to-PG engine pairs.

- **`sluice schema preview` renders the gaps section** in both text and JSON outputs:
  - **Text format**: a `Translator gaps (MySQL → Postgres)` section before the per-table DDL listing the catalog rule number, severity (`loud` / `silent`), source location (`table.column` or `CHECK constraint name`), raw expression text, and the operator-actionable note (typically `--expr-override` snippet, `--type-override` recommendation, or `--enable-pg-extension` flag).
  - **JSON format**: new `translator_gaps` top-level field with stable shape (`{table, column, constraint, field, pattern, rule, severity, expression, note}`). Omitted entirely when no gaps detected. CI gates can fail the migration plan on any `"severity": "loud"` entry.

- **Header summary line in text output**: when ≥ 1 gap is detected, the preview's header gains a `-- translator gaps: N (see section below)` line alongside the existing `-- advisory hints: N` line. Operators eyeballing the preview see the count at a glance.

## Migration / Compatibility

- **Drop-in upgrade from v0.38.x.** No CLI flag changes; the scan is enabled by default. Same-engine and PG → MySQL operators see no behaviour change (the scanner returns nil for non-MySQL-to-PG pairs). Cross-engine MySQL → PG operators with no detected gaps see an unchanged preview (the new section is skipped entirely when the gap list is empty).
- **JSON consumers**: the `translator_gaps` field is additive; existing parsers that don't know about it ignore it. Tooling can opt into reading it for CI-gate or migration-plan-review use cases.

## Who needs this release

- **Cross-engine MySQL → Postgres operators preparing a migration**: **upgrade and run `sluice schema preview`** before the actual migrate. The gaps section surfaces any deferred-pattern usage in your source schema — much cheaper than discovering them at PG apply time (loud failures) or in production output (silent divergences).
- **CI gates / migration-plan-review tools**: the JSON shape's `severity` field gives a clean fail-on-loud gate. Sample jq expression:

  ```bash
  jq '.translator_gaps | map(select(.severity == "loud")) | length == 0' preview.json
  ```

  Returns `true` when no loud gaps detected.
- **Operators not using MySQL → Postgres**: drop-in; the scanner is a no-op for other engine pairs.

## Verification surface

11 new unit tests in `internal/translate/gaps_test.go` covering each pattern's detection shape, the pgcrypto gate suppressing SHA1/SHA2 emissions, case-insensitive matching, word-boundary false-positive rejection, non-cross-engine no-op behaviour, DEFAULT/GENERATED/CHECK field coverage, dialect-tag filtering, nil-schema safety, severity stringification, and note-wording-contains-workaround sanity. All pass. Existing preview + translator tests regression-clean.
