# sluice v0.35.0 — Translator catalog batch (6 rules)

**Six additional MySQL → Postgres rewrite rules** from the v1 catalog land in this release. The v0.11.x batches plus v0.35.0 bring the total to **22 of 30 catalog rules shipped**; the remaining 8 are deliberately deferred per the catalog's own per-rule analysis and have actionable `--expr-override` workarounds.

## Added

All six rules live in `internal/engines/postgres/expr_translate.go` and fire only on cross-engine MySQL → PG migrations where the source DDL body (DEFAULT / GENERATED / CHECK) contains the recognised MySQL function shapes.

- **`HEX(int)` → `to_hex(int)`** (catalog #19). Narrow integer-typed form; `HEX(string)` returning hex of bytes is intentionally left for `--expr-override`.
- **`FIELD(x, a, b, c, …)` → `array_position(ARRAY[a, b, c, …], x)`** (catalog #22). Documented sharp edge: PG returns NULL when the value isn't in the list; MySQL returns 0. For ORDER BY proxies and custom enum ranks the divergence is invisible; for strict 0-vs-NULL distinctions in CHECK constraints, use `--expr-override`.
- **`DAYNAME(d)` / `MONTHNAME(d)` → `TO_CHAR(d, 'FMDay')` / `TO_CHAR(d, 'FMMonth')`** (catalog #25). The `FM` prefix suppresses PG's default right-padding to 9 characters. Same STABLE-not-IMMUTABLE caveat as DATE_FORMAT — loud-failure at apply time if used in an IMMUTABLE generated column.
- **`WEEKOFYEAR(d)` → `EXTRACT(WEEK FROM d)::int`** (catalog #26 narrow ISO subset). `WEEK(d, mode)` with mode != 1 / 3 (ISO) uses Sunday/Monday-start semantics PG can't model uniformly — those forms fall through verbatim to preserve loud-failure on divergence.
- **`QUARTER(d)` → `EXTRACT(QUARTER FROM d)::int`** (catalog #27 narrow). YEARWEEK deferred — composes EXTRACT with arithmetic and inherits #26's week-numbering caveats.
- **`DATEDIFF(a, b)` → `(a::date - b::date)`** (catalog #28). PG's date subtraction is an SQL operator, not a function call; the rewrite produces a parenthesised binary expression. Belt-and-braces `::date` casts truncate timestamp arguments to day precision (matching MySQL's behaviour of ignoring the time portion).

## Deliberately not shipped

Eight catalog rules stay deferred per the catalog's per-rule analysis:

| Rule | Reason |
|---|---|
| #10 MD5 / SHA1 / SHA2 | Crosses pgcrypto extension boundary — violates contain-Postgres-complexity tenet. |
| #11 GREATEST / LEAST | Same function name in both engines but NULL semantics differ; auto-rewrite would mask divergence. |
| #13 REGEXP_LIKE | MySQL ICU vs PG POSIX regex flavours diverge beyond clean rewrite. |
| #16 TIMESTAMPDIFF | Unit-cross-product makes the rule table unwieldy. |
| #20 JSON_OBJECT / JSON_ARRAY | Version-gated (PG 16+ vs JSON_BUILD_*); needs version-aware emit. |
| #21 FIND_IN_SET | Full position semantic needs a LATERAL subquery, invalid in CHECK/GENERATED contexts. |
| #23 CONVERT_TZ | AT TIME ZONE has subtle timestamp-vs-timestamptz semantics. |
| #24 LAST_DAY | Verbose 5-token expansion; `--expr-override` is cleaner. |
| #29 INET_ATON / INET_NTOA | No portable PG equivalent without a custom function. |

All have an actionable `--expr-override` workaround. The remaining gaps would only land if a real operator workflow surfaces one of them — at which point the catalog's per-rule analysis already names the cost.

## Compatibility

- **Drop-in upgrade from v0.34.x.** No format changes, no CLI changes, no engine-interface changes. The new rules only fire on cross-engine MySQL → PG migration when the source DDL body contains the recognised MySQL function shapes; pre-existing schemas that didn't trip the rules are unaffected.
- **Operators with `--expr-override` workarounds for the six newly-shipped patterns** can drop the overrides; the catalog rewrite produces the same output. If the override emits a non-default shape (additional casts, etc.), keep it — the override takes precedence.

## Who needs this release

- **Operators with MySQL → Postgres migrations whose source schemas use the six newly-supported patterns in DEFAULT / GENERATED / CHECK bodies:** **upgrade** — the migration now rewrites these to the PG equivalents automatically instead of forwarding them verbatim (which would fail at the target's parse step).
- **Same-engine operators** (MySQL → MySQL, PG → PG): drop-in; the translator only fires on cross-engine pairs.
- **Operators not using any of the six patterns:** drop-in; no behaviour change.

## Verification surface

30 new unit cases in `TestTranslateExprForPG_V35Catalog` covering each rule's mechanical shape, lowercase variants, no-args / wrong-arity fall-through paths, and three cross-rule composition cases (`FIELD` inside `COALESCE`, `QUARTER` inside a CHECK comparison, `DATEDIFF` composed with `NOW()`). All pass; existing translator tests regression-clean; `gofumpt`, `go vet`, `golangci-lint` all clean.
