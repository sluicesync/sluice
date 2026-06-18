# sluice v0.99.72

**Fix (HIGH): the postgres-trigger engine no longer installs its capture trigger on its own internal tables.** On a re-run of `trigger setup`, a caller that enumerates "every table with a primary key" would sweep in sluice's own `sluice_change_log` / `sluice_change_log_meta` (both PK-bearing) once they exist, and the engine installed the capture trigger on them. Since the capture function writes into `sluice_change_log`, a trigger on that table recurses infinitely → `stack depth limit exceeded` → **every write on every triggered table fails** (a source-wide write outage). `Setup` now filters its own internal tables out and warns, regardless of caller hygiene. This was found live during a Heroku→PlanetScale trigger-CDC migration.

## Fixed

- **postgres-trigger: never install the capture trigger on the engine's own internal tables (HIGH; live Track-A finding).** `trigger setup` takes an explicit table list from the caller. A caller enumerating "every table with a primary key" (the common shape) sweeps in `sluice_change_log` and `sluice_change_log_meta` once they exist — both carry a PRIMARY KEY. On the first setup the change-log table doesn't exist yet (never in the list); on a re-setup it does, and the engine installed the `sluice_capture` row trigger on it. The capture function INSERTs into `sluice_change_log`, so a capture trigger on that very table re-fires on every insert → unbounded recursion → PostgreSQL `stack depth limit exceeded`. That fails EVERY INSERT/UPDATE/DELETE on EVERY triggered table — a source-wide write outage, not a localized error (in a real migration it takes the source database offline for writes). `Setup` now filters its own internal tables (`sluice_change_log`, `sluice_change_log_meta`) out of the table list before the §14 preflight and the DDL, emits a loud WARN naming what was excluded, and errors only if nothing caller-supplied remains — the engine is self-protecting regardless of caller hygiene. Pinned by unit tests over the filter (mixed / only-internal / none, order-preserving) and a render assertion that the post-filter DDL never attaches a trigger to an internal table while still triggering the real user tables.

## Compatibility

- **No breaking changes; no behavior change for any correctly-scoped setup.** The change-log and meta tables were never meant to be replicated, so excluding them is a no-op for every healthy install — the fix only prevents the pathological self-trigger that a re-run with an all-PK-tables enumeration could create. No flags, config, or on-disk/position formats change.
- **Scope: the `postgres-trigger` source engine only.** The logical-replication `postgres` engine, the MySQL/Vitess engines, and all target-side paths are untouched.

## Who needs this — action required

- **Anyone driving the `postgres-trigger` CDC engine (e.g. via the Heroku→PlanetScale migrator) who re-runs `trigger setup` / re-configures after the change-log tables already exist:** upgrade. Before this fix, a re-configure that enumerated all PK tables could install the capture trigger on `sluice_change_log` itself and recurse, failing all source writes with `stack depth limit exceeded`. If you hit that, the recovery is to drop the stray triggers on `sluice_change_log` / `sluice_change_log_meta` (`DROP TRIGGER sluice_capture, sluice_capture_truncate`) and re-run setup on this version, which will no longer re-add them.
- **Everyone else: no action.** Healthy installs are unaffected; this is a defensive correctness fix.

---

## Install

```
brew install sluicesync/tap/sluice
go install sluicesync.dev/sluice/cmd/sluice@v0.99.72
# Linux/macOS/Windows binaries + checksums on the release page.
docker pull ghcr.io/sluicesync/sluice:v0.99.72
```

**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
