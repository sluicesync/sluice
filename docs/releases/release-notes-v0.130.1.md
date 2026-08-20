# sluice v0.130.1

**A fast-follow patch.** A fresh independent audit of v0.130.0 found three higher-severity issues — a silent sync freeze, a silent-loss-plus-false-refuse on the PlanetScale/Vitess replication path (one half a regression in v0.130.0's own new guard), and a PlanetScale foreign-key preflight that ran on the wrong entry point — plus a false refusal when v0.130.0's new preflight met a `--where` filter. All are fixed here. **Drop-in from v0.130.0 — no new flags, no format change; a migrate or sync that worked on v0.130.0 still works.**

## Fixed

**A misused `TINYINT(1)` on a PlanetScale/Vitess (VStream) source no longer silently loses data, and a legitimate integer `TINYINT(1)` is no longer falsely refused.** On the VStream replication path (PlanetScale, Vitess), sluice decided whether a `TINYINT(1)` column was a boolean from the replication wire's own column type, by a rule that disagreed with the one the schema translator uses everywhere else: it treated an `AUTO_INCREMENT` or `UNSIGNED` `TINYINT(1)` as a boolean, where the schema translator (correctly, per MySQL) maps those to an integer. Two consequences flowed from that split. An auto-increment `TINYINT(1)` primary key — a real, if unusual, shape — had every value `≥ 2` collapsed to `true`/`1` on this path, merging rows on a MySQL-family target: silent loss. And v0.130.0's new out-of-range refusal, which fires on the same VStream path, then *false-refused* those same legitimate integer columns with a message asserting a boolean mapping sluice does not actually make for them. Both are fixed by giving the VStream decode and the schema translator a single shared predicate, so a `TINYINT(1)` is a boolean in exactly the same cases everywhere — signed, non-auto-increment, display width 1. A family matrix over `{signed/unsigned} × {auto-increment/not} × {zerofill} × width` pins the two paths in agreement so they cannot drift again.

**A `sync` cold-start into an auto-sharded keyspace no longer freezes silently after a tablet failover.** On the default multi-table auto-shard cold-start (Vitess, PlanetScale), sluice hands off from the bulk COPY to the CDC tail by reopening the replication stream. That reopen bound the new stream to the pipeline's own context but left the stream's cancel handle pointing at the finished COPY stream — so when the liveness watchdog fired after a tablet failover left the tail silent, it cancelled a stream that had already ended, and the live tail's read parked forever. The sync froze indefinitely, its retriable error unobserved, the normal reconnect never running. The handoff now opens the tail on its own fresh cancellable context (matching the three other stream-open sites), so the watchdog cancels the live stream and the reconnect runs.

**The PlanetScale foreign-key preflight and plan report now run on `sync start`, not just `migrate`.** v0.129.0's pre-copy check — the confirmable `SLUICE-E-PS-FK-NOT-ENABLED` refusal that catches a foreign-key-support-disabled PlanetScale target *before* the copy — and its plan/gotcha report were wired only into `migrate`, though the code and the error-code documentation both described them as covering "`migrate` (or a sync cold-start)". The `Streamer` had no checker, yet a sync cold-start runs the same post-copy foreign-key phase — so `sync start` (and the fleet path) into a fresh PlanetScale database with foreign-key support off burned the entire cold-start copy and then walled at the constraints phase, the exact ordeal the v0.129.0 preflight was built to prevent, on the entry point the docs claimed was covered. Both surfaces now run on the sync cold-start too, and a build-time parity gate holds every copy-phase construction site to wiring the checker so the two entry points cannot diverge again.

**The `TINYINT(1)` pre-copy preflight now honours `--where`.** v0.130.0's fail-fast preflight probed each `TINYINT(1)` column across the whole table, ignoring the operator's `--where` filter — so a one-shot `migrate --where` (or a sync cold-start) whose filter excludes every out-of-range row was hard-refused on rows the copy would never move. The preflight now ANDs the same per-table `--where` predicate the copy honours into its probe, on both `migrate` and `sync` cold-start, so it refuses only on a value that is actually in scope. The per-read-path decode guard remains the correctness floor for any row a continuous sync applies later, so nothing is lost by scoping the preflight to the copied set.

## Compatibility

Drop-in from v0.130.0 — no schema, format, or flag change, and no new error code (all four fixes reuse existing codes and paths). Every change either removes a false refusal, closes a silent-loss or hang shape, or extends an existing v0.129.0 preflight to the sync entry point it already claimed to cover; none changes the outcome of a migrate or sync that was already correct. The one behavior an operator will notice is the intended one: a `sync start` into a foreign-key-disabled PlanetScale database now refuses up front, in seconds, where before it copied for hours and then failed.

**Anyone syncing from PlanetScale or Vitess, or running `sync start` into a fresh PlanetScale database, wants this patch.** A plain MySQL↔Postgres migrate that does not use `--where` and does not touch PlanetScale/Vitess is unaffected.

## Install

```
# Homebrew
brew upgrade sluice

# Scoop (Windows)
scoop update sluice

# Go
go install sluicesync.dev/sluice/cmd/sluice@v0.130.1
```

Container images: `ghcr.io/sluicesync/sluice:0.130.1` (multi-arch; the image tag carries no `v` prefix).
