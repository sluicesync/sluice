# Cross-engine expression translator catalog

sluice's expression translator rewrites expressions that survive
unchanged in PG ↔ MySQL where the surface looks similar but the
semantics aren't identical, and refuses loudly on shapes where the
semantics genuinely diverge.

This is the consolidated reference. The full design lives in
[ADR-0016](adr/adr-0016-layered-expression-translation.md); the
specific deferred rules + their reasoning live in
`internal/translate/gaps.go`. This page is the operator-facing
"what translates, what doesn't, what the escape hatch is" version.

## Two tiers of translation

**Shipped translations** — sluice rewrites the source-dialect
expression into the target-dialect equivalent automatically. No
operator action required. These are the rules with no semantic
divergence (or where the divergence is documented + bounded).

**Deferred rules** — sluice refuses loudly OR emits a structured
WARN with an actionable hint. These are the rules where the
surface looks similar across engines but the semantics diverge in
ways an auto-rewrite would silently misrepresent. The
`--expr-override` flag is the always-available escape hatch.

The third invisible tier — **verbatim passthrough** — exists for
shapes that translate exactly across engines (column references,
arithmetic operators, comparison operators, literals). They don't
need a catalog entry; the IR-side expression text passes through
unchanged.

## Shipped translations

These are the high-frequency cross-engine rewrites operators
encounter on real schemas. Each one is engine-agnostic at the
ADR-0016 layer but lands in the per-engine emit code.

### `DEFAULT` expressions

| Source shape | PG → MySQL | MySQL → PG | Notes |
|---|---|---|---|
| `now()` / `CURRENT_TIMESTAMP` | `CURRENT_TIMESTAMP(6)` | `CURRENT_TIMESTAMP` | Precision matched per ADR-0044 |
| `gen_random_uuid()` | `(UUID())` | n/a | PG core; outer parens required by MySQL grammar |
| `uuid_generate_v4()` | `(UUID())` | n/a | pgcrypto-style; same path as `gen_random_uuid()` |
| `random()` | `(RAND())` | n/a | Parens required |
| `MD5(x)` | `MD5(x)` | `md5(x)` | Identifier case + verbatim passthrough |
| `SHA1(x)` | `encode(digest(x, 'sha1'), 'hex')` | requires `--enable-pg-extension pgcrypto` | v0.38.0 |
| `SHA2(x, N)` | `encode(digest(x, 'sha<N>'), 'hex')` | requires `--enable-pg-extension pgcrypto` | v0.38.0 |
| Bare `CURRENT_TIMESTAMP[(N)]` | passes through | passes through | Both engines accept |

### CHECK / GENERATED expressions

| Source shape | Behavior | Notes |
|---|---|---|
| Column references | Verbatim, identifier-requoted per dialect | ADR-0045 |
| Arithmetic / comparison | Verbatim | Operators identical across engines |
| `LOWER(x)` / `UPPER(x)` | Verbatim | Identifier case follows the dialect rules |
| `LENGTH(x)` | Verbatim | Both engines accept |
| `CAST(x AS TYPE)` | Verbatim for parameterised casts | Some shapes refuse — see "Deferred rules" |
| `COALESCE(...)` | Verbatim | Both engines accept identically |
| Literal subexprs (`(1+2)`, `'literal'`) | Verbatim | Same precedence + escape rules |

### Index expressions

| Source shape | Behavior | Notes |
|---|---|---|
| Functional indexes `(LOWER(email))` | Verbatim through same-engine, refuse loudly cross-engine when expression grammar diverges | ADR-0045 |
| Operator classes (`text_pattern_ops`, `gin_trgm_ops`) | Carried verbatim same-engine; cross-engine extension-owned opclasses refuse with named recovery | ADR-0032 + Bug 47 invariant + v0.95.0 Bug 115 closure |
| Partial-index `WHERE` clause | Verbatim through; per-dialect identifier requoting | Bug 65 closure |
| Covering `INCLUDE` columns | Verbatim through; order preserved | |
| NULLS FIRST / LAST | Emitted only when operator-significant (non-default) | Avoids cosmetic diffs |

### DOMAIN CHECK translation (v0.97.0+)

| Source shape | Behavior | Notes |
|---|---|---|
| Regex DOMAIN `VALUE ~ 'pattern'` | PG → MySQL: `REGEXP_LIKE(<col>, 'pattern')` on MySQL 8.0.16+ | v0.97.0; v0.97.1 backslash-fidelity fix |
| Range DOMAIN `VALUE >= X AND VALUE <= Y` | PG → MySQL: `<col> >= X AND <col> <= Y` on 8.0.16+ | v0.97.0 |
| Other DOMAIN shapes (function calls, IN lists, negated regex, single-sided ranges, non-numeric range) | Silently dropped at MySQL emit; v0.96.2 WARN fires | Operator adds the MySQL CHECK manually if needed |

### Default-expression translation gates

Beyond the per-function table above, sluice respects the source's
`DefaultExpression.Dialect` tag (ADR-0044). A PG default tagged as
PG-only doesn't auto-translate to MySQL even if the function exists
on both — the operator opted into the translator catalog by passing
the appropriate extension flag.

## Deferred rules (refused loudly or WARN-only)

These are the rules where auto-rewriting would silently misrepresent
semantics. The full reasoning per rule lives at
`internal/translate/gaps.go`; this is the summary.

| Rule | Source | Why deferred | Workaround |
|---|---|---|---|
| **#11** `GREATEST` / `LEAST` | MySQL | PG accepts but ignores NULL args; MySQL returns NULL on any NULL arg. Silent semantic divergence. | Wrap with `COALESCE` per arg, or `--expr-override`. |
| **#13** `REGEXP_LIKE` | PG 15+ | PG uses POSIX regex flavour; MySQL uses ICU. Lookaheads, named groups, Unicode property classes don't translate. | `--expr-override` with `x ~ 'pattern'` for compatible patterns. |
| **#21** `FIND_IN_SET` | MySQL | Full position semantic needs a `LATERAL` subquery, which is invalid in CHECK/GENERATED contexts. PG's `(needle = ANY(string_to_array(csv, ',')))` covers membership-only. | Refactor to `IN()` or `--expr-override`. |
| **#23** `CONVERT_TZ` | MySQL | PG's `AT TIME ZONE` semantics depend on timestamp-vs-timestamptz column type. Auto-rewrite would silently misbehave on the timestamp-without-tz case. | `--expr-override` after deciding the right `AT TIME ZONE` shape for your column. |
| **#29** `INET_ATON` / `INET_NTOA` | MySQL | No portable PG equivalent in core; would need a custom `IMMUTABLE` function on every target. | Best path: change the column type to PG's native `inet` via `--type-override`. |

`internal/translate/gaps.go` also classifies each by severity:

- **SeverityLoud** — sluice refuses at `schema preview` AND `migrate`
  preflight, before any DDL lands on the target. Examples: `FIND_IN_SET`,
  `CONVERT_TZ`, `INET_ATON`, `INET_NTOA`. The v0.68.1 structural
  backstop turned these from "late-migrate failure" into "early-preview
  refusal" (closes Bug 8's structural false-green).
- **SeveritySilent** — sluice WARNs at preview but doesn't refuse.
  Examples: `GREATEST`, `LEAST`, `REGEXP_LIKE`. The semantic divergence
  is real but bounded; operators with the affected shape see the WARN
  and decide whether `--expr-override` is needed.

## Escape hatches

When sluice refuses (or warns and you want stricter semantics), three
escape hatches apply:

### `--type-override TABLE.COL=<target_type>`

Rewrites a column's target type. Use when the source's column type
maps to a target type whose semantics are subtly different (e.g.
MySQL `INT UNSIGNED` source → PG, where you want `bigint` not the
naïve `integer`).

### `--expr-override TABLE.COL=<expression>`

Rewrites a column's `DEFAULT` or `GENERATED` expression. Use when
the auto-translator's choice doesn't match what you want. The
override is per-column, applied at the emit boundary.

### YAML `mappings:` + `expression_overrides:`

The CLI flags above also exist as YAML config entries (see
[`docs/examples/sluice.yaml`](examples/sluice.yaml)). CLI flags
override YAML entries per the standard precedence (v0.96.0 Bug 108
closure made this loudly observable for redactions; the same
precedence applies here).

## Why not more?

The translator catalog deliberately stays small. The roadmap's item 5
elaborates on why each of the 5 deferred rules is deferred and what
the actionable workaround looks like; the catalog won't grow without
**a real operator workflow hitting the gap** AND **a per-rule
semantic-equivalence audit** confirming the rewrite is honest. The
v0.97.x Bug 113 family (PG DOMAIN CHECK → MySQL inline CHECK) is the
recent precedent: the v0.96.2 WARN observability tier came first; the
v0.97.0 enforcement tier only landed after the regex/range shapes
could be audited semantically.

The alternative — "best-effort auto-rewrite" of any cross-engine
shape — would silently re-introduce the original Bug 113 silent-loss
class (a wrong CHECK on dst is more dangerous than no CHECK; the
operator sees it in `SHOW CREATE TABLE` and assumes parity). The
catalog stays conservative on purpose.

## See also

- [ADR-0016](adr/adr-0016-layered-expression-translation.md) — the
  full design rationale for the layered translator.
- [ADR-0044](adr/adr-0044-extension-function-defaults-tier3.md) —
  Tier 3 extension function-defaults (pgcrypto SHA, uuid-ossp).
- [ADR-0045](adr/adr-0045-expression-identifier-translation-consolidation.md)
  — identifier-requoting policy across the gen/CHECK/index/default
  emit paths.
- [`docs/type-mapping.md`](type-mapping.md) — the per-type translation
  policy (companion to this doc; types vs expressions).
- [`internal/translate/gaps.go`](../internal/translate/gaps.go) — the
  authoritative deferred-rule registry with `note:` fields explaining
  each refusal.
