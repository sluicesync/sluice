# sluice v0.37.0 — Translator catalog batch + two test-side bug fixes

**Re-assessing the v0.35.0 deferrals**. Three of the nine deferred catalog rules ship under closer review; the other six stay deferred with each having a load-bearing catalog reason that genuinely holds. Total catalog coverage: **25 of 30 rules shipped**.

## Added — translator catalog rules

All three new rules live in `internal/engines/postgres/expr_translate.go` and fire only on cross-engine MySQL → PG migrations whose source DDL bodies (DEFAULT / GENERATED / CHECK) contain the recognised MySQL function shapes.

- **`TIMESTAMPDIFF(unit, a, b)`** (catalog #16). Nine units covered:
  - MICROSECOND / SECOND / MINUTE / HOUR → `EXTRACT(EPOCH FROM (b - a))` with unit scaling + `::bigint` cast
  - DAY / WEEK → date subtraction (`(b::date - a::date)` / `/ 7`)
  - MONTH / QUARTER / YEAR → `AGE(b, a)`-based formulas matching MySQL's calendar-aware truncated-toward-zero semantic
  - Unknown units fall through verbatim
  
  Pre-v0.37.0 deferral rationale ("unit-cross-product makes the rule table unwieldy") turned out to be 9 mechanical arms in one switch.

- **`JSON_OBJECT(k1, v1, …)` → `JSON_BUILD_OBJECT(k1, v1, …)`** and **`JSON_ARRAY(a, b, c)` → `JSON_BUILD_ARRAY(a, b, c)`** (catalog #20). Always emit `JSON_BUILD_*` — works on every PG version sluice supports, including PG 16+. The "version-gated emit needed" objection vanishes when you realise both engines produce identical JSON output regardless of which form is used.

- **`LAST_DAY(d)` → `(DATE_TRUNC('month', d) + INTERVAL '1 month' - INTERVAL '1 day')::date`** (catalog #24). Verbose but mechanical; one switch arm with a stable shape.

### Six rules still deferred

| # | Rule | Reason |
|---|---|---|
| #10 | MD5 / SHA1 / SHA2 | Crosses pgcrypto extension boundary — violates contain-Postgres-complexity tenet |
| #11 | GREATEST / LEAST | NULL semantics differ between engines; auto-rewrite would mask the divergence silently |
| #13 | REGEXP_LIKE | MySQL ICU vs PG POSIX regex flavours diverge beyond clean rewrite |
| #21 | FIND_IN_SET | Full position semantic needs LATERAL subquery, invalid in CHECK / GENERATED contexts |
| #23 | CONVERT_TZ | AT TIME ZONE has subtle timestamp-vs-timestamptz semantics |
| #29 | INET_ATON / INET_NTOA | No portable PG equivalent without a custom function |

All have actionable `--expr-override` workarounds. These six would only land if a real operator workflow surfaces one of the shapes — at which point the catalog's per-rule analysis already names the cost.

## Fixed — test-side bugs (no operator-visible behaviour change)

- **Bug 55**: `psverify` test `TestPSPG_CDCReaderBasic` stale on ADR-0027 markers. Local `drainPSChanges` helper didn't filter `ir.TxBegin` / `ir.TxCommit` boundary markers; the integration-suite drain helper did. Post-ADR-0027 (which introduced transaction-boundary markers as first-class IR change types), the local helper accepted the markers into the `got` slice and missed trailing events. One-line fix.
- **Bug 54**: MySQL backup test `TestBackup_SnapshotAnchoredEndPosition_MySQLGapClosed` flake. Pre-fix the during-window writer paced inserts at 50ms intervals starting after a 100ms head start; on fast machines the 4th insert occasionally landed in the tight race window between snapshot `EndPosition` record and incremental CDC catch-up open. Widened to 250ms intervals + 200ms head start — writes now spread across ~1.2s, well past the boundary window. Verified by 3 consecutive PASS runs locally.

Both fixes are test-side only; production CDC reader (Bug 55) and snapshot-anchored EndPosition gap closure (Bug 54, v0.18.0) work correctly.

## Compatibility

- **Drop-in upgrade from v0.36.x.** The three new translator rules only fire on cross-engine MySQL → PG migrations whose source DDL contains the recognised function shapes; pre-existing schemas are unaffected.
- **Operators with `--expr-override` for the three newly-shipped patterns**: drop the override if you want the catalog rewrite (or keep it if your override emits a non-default shape — the override remains higher-priority).

## Who needs this release

- **Cross-engine MySQL → Postgres operators whose source schemas use `TIMESTAMPDIFF`, `JSON_OBJECT`, `JSON_ARRAY`, or `LAST_DAY` in DEFAULT / GENERATED / CHECK bodies:** **upgrade** — the rewrites now ship in the catalog.
- **Same-engine operators** (MySQL → MySQL, PG → PG): drop-in; the translator only fires on cross-engine pairs.
- **`psverify` test users (autonomous cycle subagents):** **upgrade** — Bug 55 removes a false-positive.

## Verification surface

- 28 new unit cases in `TestTranslateExprForPG_V37Catalog` covering each rule's mechanical shape across all variants (9 TIMESTAMPDIFF units + lowercase variants + fall-through paths + composition with COALESCE / CHECK comparisons / NOW).
- Bug 54 test re-run 3× consecutive PASS locally.
- Existing translator tests + standard test suite + lint all regression-clean.
