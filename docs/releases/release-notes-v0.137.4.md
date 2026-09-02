# sluice v0.137.4

**A SECURITY release.** If you run sluice against PostgreSQL as a superuser or any privileged role, and an untrusted role can `CREATE` in a schema on that role's `search_path` — the default for `public` on PostgreSQL 14 and earlier, or any explicit grant — upgrade. There is nothing to re-run on the database this time: the affected SQL is the SQL sluice itself sends, so the binary upgrade is the whole fix.

## Fixed

**sluice's own Postgres queries no longer resolve built-in functions through the session's `search_path`.**

v0.134.1 closed a privilege escalation *inside* the pgtrigger `SECURITY DEFINER` capture functions. The mechanism: PostgreSQL resolves an unqualified function call by the best type match across every schema on the path before it considers schema order, so an exact-typed overload planted by an unprivileged role wins over a polymorphic built-in and runs with the caller's privileges. The same mechanism was live, unswept, one layer out — in the queries sluice sends on its ordinary client connection, which is typically a superuser's (the pgtrigger flavor needs one for `CREATE EVENT TRIGGER`) with the default `"$user", public` path.

The pgtrigger CDC open read its install metadata with an unqualified `to_jsonb(m)`. A role with `CREATE` on `public` could plant `public.to_jsonb(sluice_change_log_meta)` — naming a table's rowtype in a signature needs no privilege on the table — and the next `sluice sync start` executed it as the connecting superuser. Reproduced twice on a real PostgreSQL 16: `ALTER ROLE app SUPERUSER` succeeded from inside the planted function, three times per open, before any downstream refusal.

The class is wider than polymorphic built-ins. Any call whose argument needs an implicit cast — `quote_ident` over a `name` column, `array_to_json` over a `text[]` — is out-resolved by an exact-typed overload the same way, which is why the fix is not one site. Every `pg_catalog` function the Postgres and pgtrigger engines spell — about 200 call sites, in any letter case, in function bodies and package-level constants alike — is now written `pg_catalog.<name>(...)`, which restricts resolution to the one schema no unprivileged role can create in.

**The durable half** is `TestPGClientSQLQualifiesEveryCatalogFunction`. Its universe is the real PostgreSQL 16 `pg_catalog` function list — 2,694 names in `internal/engines/testdata/pg16_catalog_procs.txt` — and every string literal in both engine packages is scanned against it case-insensitively; an unqualified call fails the build unless it carries a reason, and the only admitted reasons are a SQL-standard syntactic form with no function-call spelling (`EXTRACT(... FROM ...)`, the multi-argument `unnest(a, b)`, a type modifier such as `VARCHAR(255)`) or a pattern compared against source-produced text (`nextval(` over `pg_get_expr` output). Mutation-run both directions before landing.

**Its anti-vacuity half runs on a real server.** `TestClientSQL_DecoyOverloadsInPublicNeverFire` plants nineteen exact-typed decoys in `public` as an unprivileged role, proves an unqualified call from the superuser connection *does* hit them, then drives `trigger setup`, the pgtrigger CDC open and the Postgres schema read against them. That pin caught the one thing the source gate could not: the two-argument `unnest(a, b)` is grammar, expanded by the parser into `ROWS FROM(...)` only when spelled bare, so `pg_catalog.unnest(a, b)` does not exist. The source gate was green on it; the server was not.

**And a correction the release owes itself.** The first cut of that gate scanned only function bodies and only lowercase spellings. The pre-tag value-fidelity review — the third release running in which it has caught something the author had already declared covered — found seventeen server-bound calls in package-level constants it never graded, and about a hundred uppercase spellings, one of them the schema reader's `LOWER(c.data_type)`: the read that decides every column's type, hijacked live through a planted `public.lower(character varying)`, because `information_schema.columns.data_type` is a domain over `varchar` and an exact `varchar` overload beats `pg_catalog.lower(text)`. Both gaps are closed here, the decoy pin carries that exact overload, and a mutation run confirmed the schema-read arm fails on it and on nothing else.

## Who was exposed

Every release carrying the Postgres engines — the logical-replication engine's `array_to_json` reads date to v0.84.0, the reproduced pgtrigger site to v0.133.0 — when sluice connects as a privileged role **and** an untrusted role can create functions in a schema on that role's `search_path`.

Not exposed: MySQL, MariaDB, Vitess/PlanetScale, SQLite and Cloudflare D1 sources and targets, which have no schema-search function resolution of this shape; and Postgres deployments where no untrusted role can `CREATE` in `public` or in a schema named after the connecting role, which is the PostgreSQL 15+ default for `public`.

## What is deliberately not in this release

The connection-level `search_path = pg_catalog, pg_temp` pin that would make the roster redundant — the remedy `pg_dump` adopted for CVE-2018-1058 — is not here, and this is stated so nobody assumes it. sluice emits extension types and functions unqualified (PostGIS `geometry(...)`, hstore, citext, `uuid_generate_v4()`, pgcrypto's `digest(...)`), so the pin as-is refuses working PostGIS and uuid-default migrations. It is scheduled behind qualifying those references from `pg_extension`. Until then the per-site qualification plus the roster is the closure. Operator-supplied predicates (`--where`) and expressions read from the source catalog and re-emitted on the target are the operator's own objects and resolve under the operator's path, unchanged.

## Compatibility

Drop-in from v0.137.3. No schema, format, flag, or database-side change. Translated MySQL defaults and CHECK expressions landing on a Postgres target now carry the qualifier — `pg_catalog.md5(...)`, `pg_catalog.LOWER(...)`, `pg_catalog.gen_random_uuid()` and so on — which is semantically identical on PostgreSQL 13 and later (sluice's documented floor is 15). PostgreSQL renders such a constraint or default back bare, so `schema diff` (CHECKs and defaults) and the shard-consolidation modify-check probe now treat `pg_catalog.f(...)` and `f(...)` as the same expression. The release's own CI caught the first cut reporting six phantom CHECK mismatches on a target a migrate had just created; all three comparers now carry the fold, pinned in both directions — a CHECK or default rebound to `public.lower(...)` still reports as drift.

## Install

```
# Homebrew
brew upgrade sluice

# Scoop (Windows)
scoop update sluice

# Go
go install sluicesync.dev/sluice/cmd/sluice@v0.137.4
```

Container images: `ghcr.io/sluicesync/sluice:0.137.4` (multi-arch; the image tag carries no `v` prefix).

**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
