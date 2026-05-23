# sluice v0.21.1

Two-item housekeeping release on top of v0.21.0. No functional changes; one CI-flagged test-cleanup race fix and one operator-docs gap closed. Drop-in upgrade — no DDL migrations, no CLI changes, no manifest-format changes.

## Fixed

- **`TestCDCReader_TimestampNonUTCHost` cleanup-race under `-race` on Linux CI.** The Bug-19 regression test mutates `time.Local` to `America/Los_Angeles` for its duration and restores the original via `t.Cleanup`. CI's race detector flagged a write-vs-read race between the cleanup write and the still-active CDC pump goroutine (go-mysql's binlog decoder calls `time.Unix(...)`, which reads `time.Local`). The original ordering relied on `defer rdr.Close()` running before the `t.Cleanup`, but `syncer.Close()` does not synchronously wait for the pump goroutine to exit. Fix is test-side only — convert `defer rdr.Close()` into a `t.Cleanup` that closes AND drains the `changes` channel to completion. The pump's deferred `close(out)` runs as its last act, so channel-close observation gives a happens-before edge against any further pump-side `time.Local` reads. Production CDC reader code is unchanged.

## Documentation

- **`docs/value-types.md` — MySQL binlog-event-volume sizing rule for `--rollover-max-changes`.** New section codifies an operator rule of thumb that surfaced during the v0.20.0 broker cycle: MySQL emits ~3 events per autocommit `INSERT` (`BEGIN` → `WRITE_ROWS_EVENTv2` → `XID`), a multi-row `INSERT ... VALUES (r1), ..., (rN)` collapses to `2 + N` events, and many client sessions emit a spurious empty `BEGIN/COMMIT` pair on the first DML of a new connection. Operators sizing `--rollover-max-changes` against naive INSERT counts under-size the bound by 3-4×. The doc names a **4× expected-INSERT-count** rule of thumb and contrasts against PostgreSQL's `pgoutput` (one event per row change, no inflation) so PG operators don't apply the multiplier where it doesn't apply.

## Compatibility

- **Drop-in upgrade from v0.21.0.** No format changes — manifest schema, control-table schema, change-chunk format are unchanged. No CLI breaking changes — every existing `sluice` subcommand keeps its flag surface verbatim. No DDL migration on `sluice_cdc_state`.
- **No production code changes.** The race fix is test-only; the docs change is markdown-only. Identical binary behaviour to v0.21.0; only the test goroutine ordering and operator-facing docs change.
- **Cross-engine chain restore from v0.21.0** unchanged. Same-engine + cross-engine chain shapes regression-clean.

## Who needs this

- **Anyone running `sluice backup stream` against a MySQL source** — Item A is for you. If you've been sizing `--rollover-max-changes` against expected INSERT counts and hitting "rollovers close earlier than I expected", the docs explain why and give the multiplier to apply. Same docs are useful for capacity planning a new MySQL stream.
- **CI / contributors building sluice with `-race`** — Item B is for you. The Bug-19 cleanup race tripped on Linux CI; v0.21.1 closes it cleanly without changing production CDC reader code.
- **Everyone else can skip this release.** No operational behaviour changes, no schema changes, no CLI changes. v0.21.0 → v0.21.1 is purely housekeeping.

## What's next

The roadmap continues with the items queued ahead of v0.21.0; consult `docs/dev/roadmap.md` for the current ordering. v0.21.1 closes a CI-flagged race and a docs gap so the next chunk can build on a clean baseline.
