# sluice v0.99.287

**Two independent correctness fixes, both found by looking outward rather than inward.** The first closes a silent divergence on Postgres sources: two concurrent syncs with different table scopes would quietly starve each other, with a healthy-looking `sync status` throughout — the shape you hit staging a migration in waves. The second unblocks SQLite/Cloudflare D1 imports that use the canonical boolean idiom (`INTEGER CHECK (col IN (0,1))`), which sluice was refusing outright.

If you run more than one Postgres-source stream against one database, or you import from SQLite/D1, upgrade.

## Fixed

**Concurrent Postgres-source syncs with different table scopes no longer silently starve each other.** The replication *slot* has always been per-stream and operator-overridable (`--slot-name`, whose help text advertises exactly this use case), but the *publication* — the table filter `pgoutput` applies, named per stream at `START_REPLICATION` — was a hardcoded `sluice_pub` with no flag, config key, or environment variable. Each cold start ran `ALTER PUBLICATION sluice_pub SET TABLE <its own tables>`, which replaces the member set atomically. So cold-starting a second stream with a disjoint `--include-table` scope against the same source de-scoped the first: its slot stayed healthy and kept advancing, `pgoutput` emitted nothing for its tables, and both `sync status` and `sync health` reported a current stream. Warm resume never re-asserts scope, so restarting the starved stream did not repair it.

Two changes close it. A cold start that would **remove** tables from a publication another *active* sluice replication slot is reading now refuses with `SLUICE-E-CDC-PUBLICATION-SCOPE-CONFLICT`, naming the at-risk tables and the conflicting slot — and refuses *before* mutating anything, so a refused attempt leaves every running stream untouched. And `sync start` gains **`--publication-name`**, the sibling of `--slot-name` with the same `sluice_` prefix convention, which is how you legitimately run several differently-scoped streams off one Postgres source. Only *narrowing* rescopes are guarded: widening or equal-scope rescopes remove nothing and never reach the check, so a fleet of same-scope streams and the additive `schema add-table` path are unaffected. MySQL, PlanetScale, and Vitess sources were never affected — they have no publication.

**SQLite / Cloudflare D1: a `CHECK (col IN (0,1))` constraint no longer blocks the migration.** SQLite has no `BOOLEAN` type, so the canonical idiom is an `INTEGER` column constrained to 0/1 — and sluice refused it at create-tables as carrying "a non-portable SQLite expression", stopping the migration before any data moved. The expression was in fact perfectly portable; the SQLite→canonical expression translator simply had no `IN` node, so the construct could not be parsed and fell through to the catch-all refusal. `x [NOT] IN (a, b, …)` now translates verbatim to both Postgres and MySQL, which share SQLite's syntax and its NULL three-valued logic. The genuinely non-portable shapes still refuse: `IN (SELECT …)`, SQLite's `IN <table>` shorthand, and the SQLite-only empty list `IN ()` (a Postgres syntax error).

**…and `--infer-types` now re-types those constraints along with the column.** When rich-type inference promotes such a column to Postgres `BOOLEAN`, the surrounding CHECK has to be re-typed with it or Postgres rejects the body (`operator does not exist: boolean = integer`). The 0/1 literals compared against a boolean-promoted column now emit as `false`/`true` — covering `=`, `<>`/`!=`, reversed operand order (`1 = flag`), and `IN`/`NOT IN` members. Deliberately *not* rewritten, because each would be a guess: an ordering comparison (`flag > 0`, whose boolean semantics are their own thing) and an out-of-range literal (`flag = 2`, never valid against a two-valued column) are left alone so Postgres rejects them loudly. MySQL targets are unchanged — its `BOOLEAN` is `TINYINT` and takes 0/1 natively. The rewritten constraints still *enforce*: the integration pin asserts a non-conforming row is rejected and a conforming one accepted, because a CHECK silently relaxed into a tautology would be worse than the original bug.

**`strftime()` column DEFAULTs on a SQLite/D1 source now translate instead of being dropped.** Only `datetime('now')` / `date('now')` / `time('now')` were recognised; every `strftime(...)` spelling took the loud-WARN drop path, so the migration succeeded but the target column had no default and a DEFAULT-omitting `INSERT` after cutover produced NULL where the source would have supplied a timestamp. The whole-format current-instant spellings now map across with precision preserved — these render at second precision whereas Postgres's bare `now()` carries microseconds, so the translation is `date_trunc('second', now())` rather than `now()`. `%s` maps to `floor(extract(epoch from now()))`. Partial formats (`'%Y'` alone) and non-`'now'` bases still refuse and drop loudly: guessing a general strftime→to_char translation is how a DEFAULT silently changes meaning.

## Compatibility

No breaking changes; no configuration migration required.

`--publication-name` defaults to the historical `sluice_pub`, so existing Postgres deployments upgrade with byte-identical behaviour. A stream-id-derived default was deliberately rejected: the publication name is sent on every `START_REPLICATION`, including warm resume, so deriving it would have broken every running Postgres stream on upgrade.

The new refusal only fires on a *narrowing* rescope while another sluice slot is actively reading the same publication — a shape that was silently losing changes before, so nothing that previously worked correctly now refuses.

The SQLite changes only ever *widen* what sluice accepts: expressions and DEFAULTs that previously refused or dropped now translate. Nothing that previously migrated changes shape, with one deliberate exception — a `CHECK` on a column that `--infer-types` promotes to `BOOLEAN` now lands as boolean literals instead of failing, which is the fix.

## Who needs this

- **Anyone running several Postgres-source streams against one database** — including staged/"wave" migrations, where you cut the biggest tables over first and bring the rest across later. Give each stream its own `--publication-name` alongside `--slot-name`. See the [staged (wave) migration guide](https://sluicesync.com/docs/staged-wave-migration/).
- **Anyone importing from SQLite or Cloudflare D1**, especially with `--infer-types`. If a previous import failed with "non-portable SQLite expression" on a CHECK constraint, or you noticed a `strftime()` default missing on the target, this release fixes both.
- Everyone else: no action needed.

## Install

```sh
brew install sluicesync/tap/sluice          # macOS / Linux
scoop bucket add sluicesync https://github.com/sluicesync/scoop-bucket && scoop install sluice   # Windows
```

Binaries, checksums, and the multi-arch container image are attached to this release.
