# sluice v0.99.133

**New: fleet config HOT-RELOAD — a running `sluice sync run --config syncs.yaml` re-reads its config and reconciles the live fleet on `SIGHUP`, with no full restart (ADR-0122 §7, roadmap item 47 follow-up). A malformed or colliding reloaded config is refused loudly and the running fleet keeps going unchanged. POSIX-only; opt-in; fully drop-in over v0.99.132.**

## Features

**Fleet config hot-reload on `SIGHUP`.** The sync command center (`sync run`, shipped v0.99.132) can now apply config changes to a running fleet without restarting the process. On `SIGHUP` it re-reads the config file and reconciles the live fleet, diffing by stream-id: a sync added to the config is **started**, one removed is **stopped** (graceful drain — its context is cancelled and its goroutine awaited, so no half-dead stream leaks), and one whose resolved spec **changed** is **restarted** (stop old, start new with the new spec); an unchanged sync is left running untouched. "Changed" is detected by a stable fingerprint of each resolved spec, so a no-op reload touches nothing and says so. Removals drain fully before the matching starts run, so a restarted PostgreSQL sync releases and reacquires its replication slot cleanly (warm-resuming from its persisted position).

**Reject-and-keep-running is the load-bearing property.** The reloaded file is run through the *exact same* load-time validators the initial load uses — required fields, fleet-wide stream-id uniqueness, and the PostgreSQL replication-slot uniqueness guard (the two data-corruption refusals) — *before* anything is built or applied, and the supervisor additionally refuses a new set with a duplicate or empty stream-id up front. If parse or validation fails, the reload is **refused loudly** (logged, naming the violation) and the currently-running fleet keeps going on the old config, unchanged. A malformed or colliding reloaded config can never half-apply, take down, or corrupt the live fleet. Each reload logs its outcome — the started / stopped / restarted stream-ids, or "no changes" when idempotent.

## Compatibility

Opt-in and zero-value-safe: no `SIGHUP` / no reload behaves exactly as before. **POSIX-only** — `SIGHUP` doesn't exist on Windows, so the signal trigger is gated to non-Windows; Windows operators change the fleet by restarting the process (a clean Ctrl-C drains every sync, then re-run). The underlying reconcile is portable and unit-tested on every OS. No other behavior changes. Fully drop-in over v0.99.132.

## Who needs this

Operators running a `sync run` fleet who want to add, remove, or reconfigure syncs without a full-process restart (and without disturbing the syncs that didn't change). Everyone else is unaffected.

---

**Install:** brew install sluicesync/tap/sluice · go install sluicesync.dev/sluice/cmd/sluice@v0.99.133 · **Container:** ghcr.io/sluicesync/sluice:0.99.133
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
