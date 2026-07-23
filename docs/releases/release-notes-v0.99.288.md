# sluice v0.99.288

**Operational resilience, found by running sluice's own multi-day soaks — plus a new proactive target-health advisory family.** A transient network blip while a sync retry was re-establishing its connections no longer kills the stream (caught live on a scale soak: a ~30-second blip took down a healthy PlanetScale↔PlanetScale sync that a warm resume later recovered in under two minutes), and two new opt-in `--notify-*` rules watch the Postgres TARGET for autovacuum falling behind and transaction-ID wraparound headroom — the failure mode a sustained sluice write load is most likely to provoke. Drop-in upgrade, no breaking changes.

If you run continuous sync for days at a time, upgrade. If you bulk-load into Postgres, the new advisories are worth turning on.

## Features

**Target-side autovacuum advisories: `--notify-dead-tuple-ratio` and `--notify-xid-age` on `sync start` (and the fleet YAML).** A sluice bulk copy into Postgres is a sustained high-write workload, and so is a long CDC catch-up against a high-churn source — exactly the shape where dead tuples outrun autovacuum, with transaction-ID wraparound as the far end of that road. Two new threshold rules in the existing alerter watch the target's own catalog for it: `--notify-dead-tuple-ratio` fires (warning) when the worst user table's `n_dead_tup/(n_dead_tup+n_live_tup)` reaches the threshold — the alert body names the table, the dead/live counts, and when autovacuum last completed there, so "autovacuum is running but losing" vs "autovacuum never reached this table" is answered in the page itself — and `--notify-xid-age` fires (critical) when the database's `age(datfrozenxid)` reaches the threshold (Postgres force-stops near ~2.1B; autovacuum normally holds this near 200M). Like the sync-lag alert these need no PlanetScale telemetry — the signal is probed from the target over the connection sluice already holds — and they run through the same edge-trigger + cooldown + hysteresis machinery and the same webhook/Slack/SMTP sinks. Tables below a 1000-dead-tuple floor are ignored so a tiny scratch table can't page on a meaningless ratio. Postgres targets only (a configured rule on another target warns once and stays inert); both rules are off by default and advisory throughout — a probe or sink failure is logged and swallowed, never able to affect the sync. Pinned by an integration test that ground-truths the probe on real PG 16 with manufactured bloat plus a sub-floor all-dead decoy that must not page.

## Fixed

**A transient network failure while a sync retry attempt re-establishes its connections no longer kills the stream.** The retry loop classifies errors by the `ir.RetriableError` wrapper the engines attach inside a flowing attempt — but each attempt first has to reopen the target applier and source readers, and a failure there carried no wrapper, so it returned terminal. Caught live on the 2026-07-22 scale-soak: a ~30-second network blip broke both legs of a PlanetScale↔PlanetScale sync at once; the VStream read error was correctly classified and the retry engaged, but the reopen then died at `open target change applier: mysql: ping: invalid connection` and the process exited — even though the same warm resume, run later, drained the backlog in under two minutes. Connect-phase failures are now marked where they arise and, when they carry a positively-matched transient network shape (dead pool connection, reset/refused, timeouts, TLS handshake timeout, the Windows winsock wordings), ride the existing bounded retry budget with exponential backoff. The classification is deliberately narrow and the budget is the loud-failure floor: DSN parse errors, bad credentials, unknown-host, coded refusals, and every unknown shape stay terminal exactly as before, and a target that can never be reached still exhausts the budget and fails loudly. The gap is as old as the retry machinery itself (v0.42.0/v0.46.0), so every prior release with continuous sync carries it — but the failure was always loud (a clean exit with a clear error and a durable resume position), never data loss. Pinned both ways by unit tests: the incident shape retries then succeeds, budget exhaustion stays loud and names the cause, and marked-but-terminal / unmarked-but-transient errors both stay terminal.

**A cancelled Cloudflare D1 `--stage-local` staging pass now reports a stable `context.Canceled` error identity.** When staging was cancelled mid-page, `database/sql` surfaced the abort differently depending on which DB operation the cancel landed on — `sql: statement is closed` when the pool reaped the prepared statement, `context canceled` when the exec saw it directly, or a driver-specific message on a `Commit`/`BeginTx`/`Prepare`. All mean "we were cancelled", but callers and the retry classifier need one stable identity, and the nondeterminism surfaced as the v0.99.287 tag-CI flake (a byte-identical build failed an `errors.Is(err, context.Canceled)` assertion that had passed two hours earlier). The fix normalises at the single return boundary: a named-return `defer` wraps the returned error with `ctx.Err()` whenever the context is done and the chain doesn't already carry it, preserving the driver detail — so a DB call added to the loop later can't reintroduce the nondeterminism. This only ever affected which error identity a correctly-aborted stage reported, never whether it aborted safely — no staged file was ever silently truncated. Present since the `--stage-local` staging path landed in v0.99.167; stress-verified 50/50 on the previously-flaky test.

## Compatibility

No breaking changes; no configuration migration required.

Both new `--notify-*` rules default to 0 (disabled), apply to Postgres targets only, and are advisory and failure-isolated throughout — a probe or sink failure can never affect the sync itself. Nothing changes unless you set them.

The connect-phase retry is a behavior change only for a shape that previously exited: a `sync` that hit a transient network failure during a retry's reconnect now retries within the existing `--apply-retry-attempts` budget instead of terminating. Every terminal class stays terminal, the budget is unchanged, and `migrate` is unaffected (the marker is applied on the sync path's setup sites).

The D1 change means a cancelled `--stage-local` stage now always carries `context.Canceled` in its error chain, with the driver-specific detail preserved via wrapping; anything matching with `errors.Is` sees a superset of what it saw before.

## Who needs this

- **Anyone running long-lived continuous syncs** — especially against managed/cloud endpoints where routine network blips are a fact of life. Upgrade; no action beyond upgrading. Streams that previously died at `open target change applier: … invalid connection` (or a sibling transient shape during reconnect) now ride it out.
- **Anyone bulk-loading into or continuously syncing to Postgres** — upgrade and opt in with `--notify-dead-tuple-ratio` / `--notify-xid-age` alongside an existing `--notify-*` sink to get paged before autovacuum debt or wraparound headroom becomes an incident.
- **Cloudflare D1 `--stage-local` users:** nothing to do — the error-identity fix changes no data path and a cancelled stage always aborted safely.
- No re-verification of past runs is needed anywhere: this release contains no silent-loss class.

## Install

- Binaries: https://github.com/sluicesync/sluice/releases/tag/v0.99.288
- Homebrew: `brew install sluicesync/tap/sluice`
- Scoop: `scoop bucket add sluicesync https://github.com/sluicesync/scoop-bucket; scoop install sluice`
- winget: `winget install sluicesync.sluice`
- Docker: `docker pull ghcr.io/sluicesync/sluice:0.99.288`

**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
