# sluice v0.38.0 — pgcrypto catalog entry + MD5/SHA1/SHA2 translator rules

**Re-examines the v0.37.0 deferral verdict for catalog rule #10 (hash family).** Closer analysis split the rule into a core-PG path (MD5, no extension needed) and a pgcrypto-backed path (SHA1, SHA2). Total catalog coverage: **28 of 30 rules.**

## Added — translator catalog rules

- **`MD5(x)` → `md5(x)`** (catalog #10, MD5 subset). PG's core `md5(text)` returns the same 32-character lowercase hex digest MySQL's `MD5()` returns. No extension needed; the rewrite is a mechanical case-fold. **Ships unconditionally.**

- **`SHA1(x)` → `encode(digest(x, 'sha1'), 'hex')`** (catalog #10, SHA1 subset). Gated on `--enable-pg-extension pgcrypto`. Without the flag, falls through verbatim so PG's parse-time error signals the missing extension. With the flag, sluice's preflight confirms pgcrypto is installed on the target before the rewrite fires.

- **`SHA2(x, bits)` → `encode(digest(x, '<algo>'), 'hex')`** (catalog #10, SHA2 subset). Same pgcrypto gate. Bit-width dispatch:
  - `0` / `256` → `sha256` (preserves MySQL's `SHA2(x, 0)` default semantic)
  - `224` → `sha224`
  - `384` → `sha384`
  - `512` → `sha512`
  - Unrecognised bit widths fall through verbatim.

## Added — pgcrypto extension catalog entry

**`pgcrypto` joins `pgExtensionCatalog`** in `internal/engines/postgres/extension_catalog.go` as a **presence-gate entry** — no types passthrough (`typesByName` / `hintTypeNames` empty), no index access methods or opclasses (pgcrypto introduces neither). The entry exists purely so sluice's existing `validateAndPreflightExtensions` machinery runs the standard `SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname='pgcrypto')` preflight check on the target before any data moves. Mirrors hstore's opt-in pattern.

pgcrypto ships with PG contrib (`postgresql-contrib` package) and is available on every major hosted PG service (PlanetScale, AWS RDS, Cloud SQL, Azure Database for PG, Supabase) without `shared_preload_libraries` configuration.

## ExprContext threading (internal refactor)

The translator now receives the operator's enabled-extensions set via a new `ExprContext.EnabledPGExtensions` field, threaded from `emitOpts.EnabledExtensions` at the schema writer boundary. The four `translate*Expr` helpers (`translateDefaultExpr`, `translateIndexExpr`, `translateGeneratedExpr`, `translateCheckExpr`) and their direct callers (`emitDefault`, `emitIndexColumnList`, `emitCheckConstraint`, `emitCreateIndex`) gained an `opts emitOpts` parameter. Internal refactor only — no operator-visible behaviour change for flows that don't use the new SHA1/SHA2 paths.

## Migration / Compatibility

- **Drop-in upgrade from v0.37.x.** Same-engine operators (MySQL → MySQL, PG → PG) are unaffected; the translator only fires on cross-engine pairs. Operators with `--expr-override` workarounds for MD5 / SHA1 / SHA2 can drop the override; the catalog rewrite produces equivalent output.
- **Operators on the cross-engine MySQL → PG path with `MD5(x)` in DDL bodies**: get the rewrite automatically — no flag needed.
- **Operators on the cross-engine MySQL → PG path with `SHA1(x)` or `SHA2(x, n)` in DDL bodies**: pass `--enable-pg-extension pgcrypto` (and ensure `CREATE EXTENSION pgcrypto;` has run on the target).

## Who needs this release

- **Cross-engine MySQL → Postgres operators whose source schemas use `MD5(x)` in DEFAULT / GENERATED / CHECK bodies**: **upgrade** — the rewrite ships in the catalog; no flag needed.
- **Cross-engine MySQL → Postgres operators whose source schemas use `SHA1` or `SHA2`**: **upgrade and pass `--enable-pg-extension pgcrypto`**. Requires `CREATE EXTENSION pgcrypto;` on the target (one-line operator action).
- **Same-engine operators**: drop-in; no behaviour change.
- **Operators not using MD5/SHA family functions in DDL**: drop-in; no behaviour change.

## Verification surface

17 new unit cases in `TestTranslateExprForPG_V38Catalog` covering MD5 always; SHA1/SHA2 with + without pgcrypto context; the five SHA2 bit-width dispatch arms; fall-through arity paths; composition with COALESCE + CONCAT. All pass; existing translator + extension catalog tests regression-clean.
