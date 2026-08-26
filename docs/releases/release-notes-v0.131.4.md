# sluice v0.131.4

**A robustness patch.** The operator-facing headline: **Vitess/PlanetScale continuous sync now survives a mid-stream reshard's transient primary-routable window** instead of dropping the CDC stream. Plus loud-failure hardening on the SQLite/D1 float-render and MyISAM paths, and a large batch of internal CI-health and gate hardening. Drop-in from v0.131.3 — no schema, format, flag, or command change.

## Fixed

**Vitess/PlanetScale continuous sync rides out a reshard's transient primary-routable window instead of exiting the stream.** On a 1→N reshard sluice follows the new shard layout (reopens the CDC tail), but the newly-resharded shard's PRIMARY becomes routable a beat *after* `SwitchTraffic` returns. During that window a `NotFound` "tablet is either down or nonexistent" classified as terminal and exited the stream, forcing a manual restart. That specific transient shape is now a **narrow retriable carve-out** in `classifyReaderError`, so `reopenAfterReshard` reconnects in-process through the window (bounded by the existing reconnect budget); a genuine `NotFound` — a bogus keyspace/shard, a missing table — still terminates. **Plus a correctness-adjacent sibling-gap fix:** the steady-state CDC reader's reshard-follow reopen was defaulting to a REPLICA tablet (the ADR-0072 default, correct for *normal* CDC), but on a reshard the new shard's replicas aren't caught up yet — so the reopen now pins **PRIMARY**, matching the cold-start snapshot path which already did. Scope: the Vitess/PlanetScale VStream reshard-follow path; the cold-start path was already correct.

## Changed

**SQLite / Cloudflare D1 float-render fidelity is now probed at every stream open, not assumed.** v0.131.2 fixed the lossy `%.17g` float capture with `%!.20g`; this adds a runtime probe at CDC-open that renders a 17-digit double through the production capture expression on the *connected* engine and refuses loudly unless it round-trips bit-exact — so a third-party SQLite that doesn't honour the `!` alternate-form flag can never silently capture lossy floats. (History bounds how real that is: the `!` flag predates the SQL `format()` function itself, so no released SQLite lacks it — but the premise is now checked rather than assumed.)

**MyISAM 1062-tolerance is now engine-checked.** The retry-collision "tolerate duplicate-key on retry" path now verifies the target table is transactional (InnoDB) on the retry arm and withholds the tolerance loudly on MyISAM — where a committed prefix cannot roll back — instead of assuming it.

## Internal

A large CI-health and gate-hardening batch, no user-facing effect: fixed a month-long false-red in the weekly fuzz-roundtrip (a Linux SIGPIPE in the fuzz target-exists guard) so the codec-differential fuzz actually runs again, with tolerance for the golang/go#48591 deadline-boundary flake (a real crasher still fails loudly); fixed four extended-suites legs (KMS count-guard, corpus PG `max_locks_per_transaction`, vstream cross-shard test config, and the reshard leg); new `docsync` gates (the CDC-apply-target engine marker, a README engine-freshness floor); the VStream auto-increment `tinyint(1)` AUTO_INCREMENT-flag premise confirmed live against a real Vitess cluster; and the SQLite `!`-flag and MyISAM premises converted from assumptions into runtime checks.

## Compatibility

Drop-in from v0.131.3 — no schema, format, error-code, flag, or command change. The reshard carve-out + PRIMARY-pin only affect the Vitess/PlanetScale reshard-follow path (a real reshard now survives the transient primary-routable window; nothing else changes). The SQLite/D1 render-fidelity probe and the MyISAM 1062-tolerance check only fire on genuinely lossy / non-transactional configurations, refusing loudly rather than proceeding.

**Who needs this:** anyone running **continuous sync from a Vitess/PlanetScale source that can reshard** — the CDC stream now survives a mid-stream reshard instead of dropping. Operators on SQLite/D1 or MyISAM gain loud-over-silent hardening on those edges. Everyone else: no action.

## Install

```
# Homebrew
brew upgrade sluice

# Scoop (Windows)
scoop update sluice

# Go
go install sluicesync.dev/sluice/cmd/sluice@v0.131.4
```

Container images: `ghcr.io/sluicesync/sluice:0.131.4` (multi-arch; the image tag carries no `v` prefix).
