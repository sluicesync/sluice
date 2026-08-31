# sluice v0.134.1

**Security release. If you use the `postgres-trigger` engine, upgrade and then re-run `sluice trigger setup` — the upgrade alone does not remove the exposure.** Found by a scheduled blind audit and confirmed exploitable on a live PostgreSQL before the fix was written.

## Security

**Privilege escalation via an unpinned `SECURITY DEFINER` capture function (affects v0.85.0 through v0.134.0, `postgres-trigger` engine only).** sluice's DDL-capture event-trigger function was created `SECURITY DEFINER` without `SET search_path`, unlike its two sibling capture functions. Because `CREATE EVENT TRIGGER` requires superuser, that function is necessarily owned by a superuser — and its body called `jsonb_build_object(...)` unqualified. The built-in's signature is `VARIADIC "any"`, so a user-created exact-typed overload wins function resolution against it regardless of schema order, including `pg_catalog`'s implicit first position. Since an event trigger fires on *any* role's DDL, an unprivileged login role that could create a function in a schema the definer reaches needed only to run one `CREATE TABLE` to execute arbitrary SQL as the superuser owner. **Observed on PostgreSQL 16 in the fix's own regression test: an ordinary role with no superuser, no ownership and no predefined-role membership successfully ran `ALTER ROLE … SUPERUSER` from inside the event trigger.** Exploitation required the ability to create a function in a reachable schema — the default `PUBLIC` grant on the `public` schema (PostgreSQL ≤ 14) satisfies this; on PostgreSQL 15+ it requires an explicitly granted writable schema.

The fix pins `SET search_path = pg_catalog, pg_temp` on all three capture functions **and** schema-qualifies every call in their bodies, so neither belt is solely load-bearing. A repo-wide AST gate now derives its own universe of `SECURITY DEFINER` emitters and fails if any ships unpinned. A verified premise is recorded alongside: a `pg_temp`-planted overload cannot win function resolution, so the two pinned siblings were never exposed. The pin and the qualification are gated by `TestNoUnpinnedSecurityDefinerEmitters` and `TestCaptureDDLFunction_QualifiesEveryCall`; the escalation itself is pinned end-to-end by `TestDDLCaptureFunctionSearchPath_ShadowedBuiltin` (it fires against the pre-fix shape and is blocked against the shipped one), and the upgrade warning by `TestInsecureCaptureFunctionWarn_AtCDCOpen`. The detector lives in `preflight_definer_search_path.go`.

**Existing installs stay vulnerable until the function is replaced.** `sluice trigger setup` re-runs `CREATE OR REPLACE FUNCTION` and clears it in seconds, preserving the change log, its watermark and the consumer registry. To make that unmissable, every CDC open now probes `pg_proc` for the vulnerable shape and emits a loud `INSECURE-CAPTURE-FUNCTION` warning naming the remedy. It warns rather than refuses deliberately: the probe runs on every warm resume, so refusing would turn a binary upgrade into an immediate outage on every running sync.

## Compatibility

Drop-in from v0.134.0 — no schema, format, flag, or command change. The new warning is the only behavior change, and it stops on its own once `trigger setup` has been re-run. Users of the MySQL, Vitess/PlanetScale, Postgres (logical-replication), SQLite and D1 engines are unaffected: none of them creates a `SECURITY DEFINER` function.

## Who needs this — action required

- **`postgres-trigger` engine users: upgrade, then run `sluice trigger setup --dsn=<source-dsn> --tables=<...>` once per install.** The binary upgrade fixes what sluice *creates*; the re-run is what replaces the function already on your database. Until then every CDC open warns.
- Everyone else: upgrade normally; no action.

## Install

```
# Homebrew
brew upgrade sluice

# Scoop (Windows)
scoop update sluice

# Go
go install sluicesync.dev/sluice/cmd/sluice@v0.134.1
```

Container images: `ghcr.io/sluicesync/sluice:0.134.1` (multi-arch; the image tag carries no `v` prefix).

**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
