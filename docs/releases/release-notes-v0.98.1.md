# sluice v0.98.1

## v0.98.1 — `--reap-stale-backends` now actually clears the lockout it's for

A focused fix for the headline connection-resilience feature shipped in v0.98.0. Found by the v0.98.0 post-release regression cycle; no data-loss class, but the feature was unreachable in its primary scenario.

### Fixed

- **Stale-backend reaping ran too late to clear an orphan's lock (Bug 123).** v0.98.0 added `--reap-stale-backends` to terminate a hard-killed prior run's orphaned server-side backend — the one still holding an AccessExclusive lock on a target table and blocking the next cold-start. But in `migrate` the cold-start preflight (which reads each target table to enforce the empty-target contract, taking an AccessShare lock) ran **before** the stale-backend preflight. The orphan's AccessExclusive lock blocks that read — so the preflight stalled or refused *before* the reap could fire, making the flag unreachable in exactly the lockout it exists to resolve. v0.98.1 moves the reap ahead of the cold-start preflight, so it clears the lock (and frees the connection slots the budget probe then measures) first. The `sync start` cold-start path already ran the reap first and was unaffected. The reaper logic itself was correct in v0.98.0 — only the ordering was wrong.

### Compatibility

- No flag, schema, IR, or wire-format changes. `migrate` and `sync start` invocations are identical to v0.98.0; the only behavioral difference is that `--reap-stale-backends` now fires early enough to do its job on a lock-held target.
- Operators on v0.98.0 who tried `--reap-stale-backends` against a locked-out target and saw it hang or refuse at the cold-start preflight should upgrade.

### Who needs this

- **Anyone who had a `migrate` run hard-killed (OOM, SIGKILL, node eviction) mid-copy and then hit a locked-out retry.** This is the exact case `--reap-stale-backends` was built for, and it now works on the first retry.
