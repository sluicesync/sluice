# ADR-0044: ADR-0032 Tier 3 — extension-function defaults & generated expressions (uuid-ossp + pgcrypto)

## Status

**Accepted (2026-05-16); implemented (commit `837769c`), shipping
in v0.65.0.** Two post-implementation corrections applied during
review:

1. The refusal message's escape-hatch wording dropped
   `--expr-override` (verified inapplicable to this read-time gate —
   see §2 and the precedence note) in favour of
   `--enable-pg-extension` / `--exclude-table`; code, test, and this
   ADR were updated in lock-step.
2. **§3 (cross-engine PG → MySQL) rescoped (2026-05-15, pre-v0.65.0
   release).** The original §3 design — translate uuid-ossp
   generators to MySQL `UUID()` and add a new cross-engine pgcrypto
   refusal arm — was found dead-in-production: the pre-existing
   ADR-0032 `validateEnabledPGExtensions` lossless-only policy
   already refuses ANY `--enable-pg-extension uuid-ossp`/`pgcrypto`
   for a non-PG target (only `hstore`/`citext` are cross-engine
   translatable) *before* schema-read, so neither the new
   `pgToMySQL` uuid entries nor the new `cross_engine_supportable.go`
   crypto arm could ever execute. The uuid mapping was also dishonest
   (`uuid_generate_v4()` is RFC-4122 v4/random; MySQL `UUID()` is
   v1/time-based — a silent version drift). The dead translation
   entries, the cross-engine refusal arm + its helpers/test, and the
   failing integration assertion were removed; the integration test
   now pins the REAL ADR-0032-policy refusal. **§1 / §2 (the
   same-engine gate, the scanner, the `gen_random_uuid` core guard)
   are the shipped feature and are untouched.** The pre-existing
   Bug-42 `gen_random_uuid()` / `now()` / `random()` cross-engine
   `pgToMySQLDefaultExpr` translations are NOT part of §3 and remain
   in place.

Implements the ADR-0032 §Consequences "Tier 3 … the natural v2
chunk." Sign-off decision: the same-engine PG → PG **opt-in gate is
adopted as drafted** — extension-function defaults/generated-exprs
require `--enable-pg-extension <ext>` and are refused
early-and-clearly otherwise (a deliberate behaviour change vs.
today's implicit pass-through; correct per the loud-failure-early +
zero-users → cleaner-breaking-change tenets, and consistent with
every other ADR-0032 extension). Core PG functions
(`gen_random_uuid()`, `now()`, …) are never gated.

## Context

ADR-0032 v1 shipped Tier 1 (hstore/citext) + Tier 2 (pgvector/
pg_trgm/PostGIS) extension passthrough. Tier 3 — extensions whose
*functions* appear in column `DEFAULT` clauses and
`GENERATED ALWAYS AS` expressions — was explicitly deferred:

> "Operators with `DEFAULT uuid_generate_v4()` or
> `DEFAULT gen_random_uuid()` columns still need `--expr-override`
> to translate the function-default. The Tier 3 catalog
> (uuid-ossp + pgcrypto) is the natural v2 chunk."

**Ground truth from the code recon (the crux):** for **same-engine
PG → PG**, an extension-function default is *already passed through
verbatim today, by accident of the dialect-match short-circuit*:

- `schema_reader.go::translateDefault` classifies any non-literal
  as `ir.DefaultExpression{Expr, Dialect:"postgres"}` (no refusal,
  no extension awareness).
- `ddl_emit.go::translateDefaultExpr` emits it verbatim when
  `Dialect == "postgres"` (writer dialect). Same for
  `translateGeneratedExpr`.

So **there is no active refusal to lift.** The actual gaps:

1. **No operator-intent signal / no preflight presence-gate.** A
   PG → PG migrate with `DEFAULT uuid_generate_v4()` whose target
   lacks uuid-ossp does *not* fail cleanly at preflight — it fails
   late and ugly with a raw PG parse error at `CREATE TABLE` apply
   time. That violates the loud-failure-**early** tenet.
2. **No catalog declaration for uuid-ossp.** pgcrypto already has a
   presence-gate-only `extensionDef` (v0.38.0, for the SHA
   translator rules); uuid-ossp is absent from `pgExtensionCatalog`.
3. **Cross-engine PG → MySQL is verbatim-then-parse-error.**
   `pgToMySQLDefaultExpr` carries `gen_random_uuid()→(UUID())`,
   `now()`, `random()` — but **not** `uuid_generate_v4()` (uuid-ossp).
   It falls through verbatim → MySQL parse error at apply.
   **Correction (rescope, see Status correction 2):** gap #3 is not
   actually reachable — ADR-0032's `validateEnabledPGExtensions`
   refuses `--enable-pg-extension uuid-ossp`/`pgcrypto` for a non-PG
   target in preflight (before schema-read / before any
   `pgToMySQLDefaultExpr` lookup), and without the flag the §2
   same-engine gate refuses first. The honest resolution is to leave
   that ADR-0032 policy in force, **not** to add a uuid mapping
   (v4→v1 would be a dishonest version drift) or a new arm. See §3.

**Core-vs-extension subtlety (load-bearing):** `gen_random_uuid()`
is **core PostgreSQL 13+** — *not* an extension function on any
supported modern PG. Only `uuid_generate_v1/v1mc/v4/v5()` (uuid-ossp)
and pgcrypto's `digest/hmac/crypt/gen_salt/encrypt/decrypt/…` are
genuinely extension-owned. **Tier 3 must gate only genuinely
extension-owned functions; gating a core function would refuse
valid core-PG schemas.**

## Decision

### 1. New declarative catalog surface

Add one field to `extensionDef`
(`internal/engines/postgres/extension_catalog.go`):

```go
// defaultExprFunctions: bareword names of functions this extension
// owns that legitimately appear in column DEFAULT / GENERATED
// expressions (e.g. "uuid_generate_v4", "digest"). Empty for
// type/index-only extensions. Catalog-driven so the preflight
// gate and the cross-engine policy are not scattered conditionals.
defaultExprFunctions map[string]struct{}
```

- **`uuid-ossp`** — new `pgUUIDOSSPDef`: `defaultExprFunctions =
  {uuid_generate_v1, uuid_generate_v1mc, uuid_generate_v4,
  uuid_generate_v5, uuid_nil, …}`; all other fields empty (no
  types/indexes — same shape as `pgCryptoDef`). Register
  `"uuid-ossp"` in `pgExtensionCatalog`.
- **`pgcrypto`** — extend the existing presence-gate `pgCryptoDef`
  with `defaultExprFunctions = {digest, hmac, crypt, gen_salt,
  gen_random_bytes, encrypt, decrypt, pgp_sym_encrypt, …}`.
  (`gen_random_uuid` is deliberately **NOT** listed — core PG 13+.)

### 2. Same-engine PG → PG: opt-in + preflight presence-gate

When a column `DEFAULT` or `GENERATED` expression references a
catalog-declared `defaultExprFunctions` bareword (conservative
function-call token scan of the expr text — **not** a full SQL
parser; reuse the lightweight matcher style already in
`expr_translate.go`):

- **Extension enabled via `--enable-pg-extension <ext>`** → pass
  through verbatim (unchanged behaviour) **and** the existing
  `validateAndPreflightExtensions` machinery now also runs for
  Tier-3 extensions, so a target missing the extension is refused
  **cleanly at preflight** with an actionable message, instead of
  the late raw parse error.
- **Extension NOT enabled** → **loud refusal at schema-read**:
  `column "users.id" DEFAULT uuid_generate_v4() requires
  --enable-pg-extension uuid-ossp (uuid-ossp owns uuid_generate_v4;
  pass the flag so sluice preflights it on the target, or
  --exclude-table to skip it)`. **Post-implementation correction:**
  the original draft named `--expr-override` as the escape; that is
  inaccurate and was removed from the message + this spec.
  `ApplyExpressionOverrides` rewrites only generated-column
  expressions (never DEFAULTs) and runs *after* `ReadSchema`, while
  this gate fires *inside* `ReadSchema` — the run aborts before any
  override could apply, so `--expr-override` is not a working escape
  for this gate. The honest escapes are `--enable-pg-extension`
  (the fix) or `--exclude-table` (skip). This is a **deliberate
  behaviour change**:
  today the same case passes silently then fails ugly at apply.
  Per the loud-failure-early tenet and the zero-users → cleaner-
  breaking-change tenet, the explicit opt-in is the correct design
  (consistent with how every other ADR-0032 extension already
  works — nothing passes through without `--enable-pg-extension`).
  Core functions (`gen_random_uuid()`, `now()`, …) are never
  gated — they are not in any `defaultExprFunctions` set.

### 3. Cross-engine PG → MySQL: no new translation, no new arm — the ADR-0032 lossless-only policy already governs this

**Rescoped (see Status correction 2).** Cross-engine PG → MySQL
`uuid-ossp` and `pgcrypto` **remain refused by the pre-existing
ADR-0032 `validateEnabledPGExtensions` lossless-only policy.** That
gate refuses ANY `--enable-pg-extension <ext>` for a non-PG target
unless `ext ∈ {hstore, citext}` (the only extensions with a declared
cross-engine default translator), and it fires *before* schema-read.
Therefore:

- **No new `pgToMySQLDefaultExpr` uuid-ossp entries.** A
  `uuid_generate_v4()` (RFC-4122 v4/random) → MySQL `UUID()`
  (v1/time-based) mapping is a dishonest UUID-version drift, not a
  semantically-honest translation. uuid-ossp has no lossless MySQL
  default translator and is not added to
  `crossEngineDefaultTranslatedExtensions`.
- **No new cross-engine pgcrypto refusal arm.** A bespoke
  `cross_engine_supportable.go` crypto scanner would be dead code:
  the ADR-0032 engine-name gate already refuses `--enable-pg-extension
  pgcrypto` for a MySQL target, before any column expression is
  inspected. (And without the flag, the §2 same-engine schema-read
  gate refuses first.)
- **Operator escape:** `--type-override` / `--expr-override` per the
  existing ADR-0032 refusal message, or use a PG target.

The pre-existing Bug-42 cross-engine default translations
(`gen_random_uuid()` / `now()` / `random()` → MySQL equivalents in
`pgToMySQLDefaultExpr`) are core/legitimate, predate Tier 3, and are
**not** part of §3 — they stay.

### What does not change

- Same-engine passthrough mechanics (already verbatim).
- Core-PG function defaults (`gen_random_uuid()`, `now()`,
  `nextval()`, …) — never gated, never refused.
- `--expr-override` does **not** suppress the same-engine schema-read
  gate (§2): it rewrites only generated-column expressions and runs
  *after* `ReadSchema`, whereas the gate fires *inside* `ReadSchema`.
  (The original draft also discussed `--expr-override` suppressing a
  cross-engine §3 crypto arm for generated columns; moot — §3 was
  rescoped and no such arm exists. Cross-engine uuid-ossp/pgcrypto is
  refused by ADR-0032's `validateEnabledPGExtensions` engine-name
  gate, which runs in preflight before `ReadSchema` and is not
  suppressible by `--expr-override`. The honest cross-engine escape
  is `--type-override`/`--expr-override` per the ADR-0032 message, or
  a PG target.)
- IR shape — no `ir` change (`DefaultExpression`/`GeneratedExpr`
  already carry expr+dialect; the gate is reader/preflight-side).
- Other engines, CDC/row data path, value semantics.

## Gotchas

- **Function-name scan must be conservative.** Match
  `\b<name>\s*\(` style tokens, case-insensitive, ignoring matches
  inside string literals. False *negatives* (miss an exotic alias)
  degrade to today's late-failure (acceptable, no worse than
  status quo); false *positives* (gating a same-named core/user
  function) are the real hazard — keep the catalog barewords
  specific and unit-test the scanner against tricky inputs
  (`my_uuid_generate_v4`, `'uuid_generate_v4()'` in a string
  literal, `schema.uuid_generate_v4()` qualified).
- **uuid-ossp catalog name has a hyphen** (`uuid-ossp`) — the
  `--enable-pg-extension` value and `pg_extension.extname` both use
  the hyphen; ensure flag parsing/splitting doesn't choke on it.
- **pgcrypto dual-purpose entry.** `pgCryptoDef` is currently a
  presence-gate for the v0.38.0 SHA translator rules; adding
  `defaultExprFunctions` must not disturb that path — the SHA
  rewrites stay; this only adds the default-expr gate.
- Generated-column expressions take the identical treatment as
  defaults (recon confirmed the same passthrough path) — gate both
  or the generated path is a silent bypass.
- `gofumpt`/lint: `errors.New` for the no-verb refusal messages
  unless a `%`-verb is genuinely needed; no blank line after `{`.

## Testing

- Unit: the function-scan matcher (positive/negative/edge: string
  literals, qualified names, substring names); catalog lookup
  (`uuid_generate_v4`→uuid-ossp, `digest`→pgcrypto,
  `gen_random_uuid`→**not** matched).
- Integration (both relevant paths, testcontainers, build-tagged):
  1. PG → PG, `--enable-pg-extension uuid-ossp`, source+target have
     uuid-ossp → `DEFAULT uuid_generate_v4()` round-trips.
  2. PG → PG, flag set, **target missing uuid-ossp** → refused at
     **preflight** (not a late apply error).
  3. PG → PG, flag **absent**, source uses `uuid_generate_v4()` →
     refused at schema-read with the actionable message.
  4. PG → PG, `DEFAULT gen_random_uuid()`, no flag → **succeeds**
     (core function, never gated) — the core-vs-extension guard.
  5. Cross-engine PG → MySQL: `--enable-pg-extension uuid-ossp` and
     `--enable-pg-extension pgcrypto` against a MySQL target are
     refused up-front by the pre-existing ADR-0032
     `validateEnabledPGExtensions` lossless-only policy (message
     names the extension, `cross-engine`, and the `--type-override` /
     PG-target escape). No new §3 translation or refusal arm to test.
  6. Generated column `GENERATED ALWAYS AS (… uuid_generate_v4())`
     — same gate as defaults.

## Sizing

~250–400 LOC impl (one `extensionDef` field + uuid-ossp entry +
pgcrypto entry extension + the conservative scanner + the
schema-read gate + preflight wiring for Tier-3 extensions) +
~300–400 LOC tests. (The §3 cross-engine refusal arm +
`pgToMySQLDefaultExpr` uuid entries from the original draft were
removed as dead code — see Status correction 2.) One focused
release. No IR change, no new CLI flag (reuses
`--enable-pg-extension`). Closes ADR-0032 to v2.

## References

- ADR-0032 — PG extension passthrough framework (this is its
  deferred Tier 3); §Consequences scopes this chunk.
- ADR-0016 — expression-translator catalog (cross-engine default/
  generated rewrites; `--expr-override`).
- Bug 42 — `pgToMySQLDefaultExpr` (`gen_random_uuid()→(UUID())`,
  the pre-existing core/legitimate cross-engine default
  translations; preserved, NOT extended by §3 after the rescope).
- `docs/research/pg-extensions-deployment-frequency.md` —
  uuid-ossp + pgcrypto named as the Tier-3 v2 candidates.
