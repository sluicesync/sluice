# sluice v0.99.132

**New: a sync "command center" — `sluice sync run --config syncs.yaml` supervises many syncs from ONE process (each failure-isolated), and `sluice sync status --all` gives a fleet-wide status view (ADR-0122, roadmap item 47, staged minimal-first). Opt-in; existing single-sync `sync start` is unchanged. Fully drop-in over v0.99.131.**

## Features

**Sync command center: supervise many syncs from one process (`sync run`) + a fleet status view (`sync status --all`).** This shifts sluice from "a tool you run per-migration" to "a sync fabric you operate." Until now each `sluice sync start` was one source→target stream in its own process; `sync run --config syncs.yaml` supervises N independent streams in a single process.

**Failure isolation is the load-bearing property:** one sync crashing, panicking, or erroring NEVER takes down its peers. Each sync runs in its own goroutine over the existing `pipeline.Streamer` (its full cold-start → CDC machinery, with the Streamer's own retry as the inner loop); the supervisor is the outer loop, restarting a failed sync on a bounded exponential backoff (default 1s→30s, capped) rather than aborting the fleet, and a panic is recovered into an error so it can never crash the process. A consecutive-failure counter resets once a sync has run healthy long enough, so a long-lived sync that finally dies doesn't carry restart debt; an optional `restart.max-consecutive-failures` cap (0 = unbounded, the safe default) transitions a permanently-broken sync to an isolated `failed` state — logged loudly, peers untouched — instead of hot-looping. A clean Ctrl-C / SIGTERM drains every sync and exits 0.

**Config + safety guards.** The fleet config is a small typed YAML (a `syncs:` list plus an optional `restart:` policy), keys in kebab-case to mirror the CLI flags you already know; each sync carries the per-sync knobs that matter (source/target driver + DSN, stream-id, slot-name, target-schema, table filters, type/expr overrides, apply-concurrency / apply-batch-size / apply-delay / the apply-retry dials, metrics-listen, heartbeat & poll intervals, schema-changes, and the notify-webhook / notify-slack / notify-sync-lag / SMTP sinks), reusing the exact `sync start` spec→Streamer helpers so a fleet sync behaves identically to the same flags run standalone. Two data-corruption classes are refused **loudly at config-load** rather than discovered at runtime: two PostgreSQL-source syncs that resolve to the same replication slot (a shared single-consumer slot corrupts both streams — the guard names both colliding stream-ids and the slot), and two syncs sharing a stream-id (they would clobber each other's persisted CDC position). When several syncs target one server they share its connection budget — the supervisor WARNs (does not refuse) at load, naming the colliding stream-ids, so you can size apply-concurrency / max-target-connections rather than silently oversubscribing. `sync run --dry-run` validates the config and prints the resolved plan (per-sync source→target + resolved slot + restart policy) without starting anything.

**`sync status --all`** rolls the existing per-stream status up across every configured target (deduped so a shared target is queried once) into one fleet table via the same renderer (text / `--summary` / `--json`); an unreachable target is reported inline and skipped — a dead target never blanks the whole view.

## Compatibility

Purely additive. The existing single-sync `sluice sync start` and all other commands are unchanged; `sync run` / `sync status --all` are new opt-in subcommands. No behavior changes to any existing flag. Fully drop-in over v0.99.131.

Out of scope for this first cut (documented, not silently dropped): config hot-reload (a clean restart re-reads the file), per-sync process-global MySQL overrides (`--mysql-sql-mode` / `--zero-date` are set once per process and shared by the fleet), per-sync PlanetScale control-plane telemetry (the ungated `--notify-sync-lag-seconds` alert does work fleet-wide), and a TUI / web dashboard (a later layer over the already-exported `/metrics` + this aggregate).

## Who needs this

Operators running several ongoing cross-database syncs who want to drive and observe them as a fleet from one supervised process, with one sync's failure never affecting the others. Everyone running a single sync can keep using `sync start` exactly as before.

---

**Install:** brew install sluicesync/tap/sluice · go install sluicesync.dev/sluice/cmd/sluice@v0.99.132 · **Container:** ghcr.io/sluicesync/sluice:0.99.132
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
