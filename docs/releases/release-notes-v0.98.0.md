# sluice v0.98.0

## v0.98.0 — connection-resilience hardening + faster index builds

A throughput-and-trust arc for Postgres targets. Five opt-in capabilities harden migrations against connection-slot exhaustion and orphaned backends, and make the deferred secondary-index build phase materially faster on managed Postgres. Every new behavior is default-safe: connection labelling is the only thing on by default, the budget cap auto-sizes (refusing loudly only when a target genuinely cannot host the copy), and reaping / memory / parallelism tuning are explicit opt-ins.

### Added — connection resilience

- **`application_name` labelling on every Postgres connection (default on).** Connections are now stamped `application_name=sluice/<id>/<role>` (role ∈ {snapshot, applier, cdc-reader, schema, control}), never clobbering an operator-set value. This is the enabler for orphan detection and makes sluice's connections trivially findable in `pg_stat_activity`.
- **Connection-budget preflight + auto-cap (`--max-target-connections N`, default `0` = auto).** Before the bulk-copy pool opens, sluice probes `max_connections` / `superuser_reserved_connections` / live `pg_stat_activity` / `rolconnlimit` / `datconnlimit`, clamps requested parallelism to the available budget, and **refuses loudly** when the copy budget falls below 1 — rather than failing mid-copy with `too_many_connections`. Probe failure degrades to a WARN; it never breaks a working migration.
- **Stale-backend detection + opt-in reaping (`--reap-stale-backends`).** A SIGKILL'd / OOM'd / partitioned prior run leaves its server-side COPY backend alive, still holding the target-table lock and a connection slot — blocking the next cold-start's DROP/CREATE. sluice now scans `pg_stat_activity LEFT JOIN pg_locks` for its *own* orphaned backends, reports them loudly by default, and — only under the flag — terminates them. The safety scope (`application_name LIKE 'sluice/%' AND usename = current_user AND pid <> pg_backend_pid()`) is one constant re-applied by both the detect scan and the terminate statement, so a recycled pid can never be hit out of bound. Runs before the budget probe so a reap frees slots the budget math then sees.
- **AIMD backoff on copy-pool slot exhaustion.** A transient mid-copy slot shortage (a peer process grabbing slots *after* the preflight measured them free) no longer fails the whole migration. A `SQLSTATE 53300` on a chunk connection now multiplicatively-decreases effective parallelism (halve, floor 1), backs off, and retries — giving up loudly only after a bounded retry/total-wait. **Only** the slot-exhaustion class is retried; every other open error still fails loudly and immediately. Double-copy-safe: a 53300 fails at connection open, strictly before any row is written, so a retried chunk replays from its recorded cursor and can never duplicate rows.

### Added — index-build throughput (Postgres target)

- **`maintenance_work_mem` + parallel-worker tuning (`--index-build-mem`, size or `auto`).** The deferred secondary-index build runs against an idle target, but `maintenance_work_mem` (the dominant in-memory-sort vs external-merge lever) sat at the provider's steady-state ~4%-of-RAM default. sluice now probes `shared_buffers` as a RAM proxy and raises `maintenance_work_mem` + `max_parallel_maintenance_workers` (never lowers) on a dedicated connection for the build phase. Best-effort: a denied SET / failed probe WARNs and proceeds untuned.
- **Concurrent index builds (`--index-build-parallelism N`, default `0` = auto).** The deferred indexes now build through a bounded concurrent worker pool instead of a serial loop, each worker on its own connection. Because N concurrent builds each consume their own `maintenance_work_mem`, auto-N divides the memory budget across workers and bounds N by **both** the memory budget **and** the target's spare connection budget, plus the index count and an operator cap (conservative hard cap 8). `N=1` degenerates to exactly the prior serial path.

### Performance

- **gzip backup codec is ~2–6× faster to encode.** The non-default gzip codec (`--compression=gzip`; zstd has been the default since v0.67.0) now uses `klauspost/compress/gzip` instead of stdlib `compress/gzip`, at <5% ratio cost.

### Compatibility

- **No backup-format change, no version bump.** The gzip swap is a drop-in with an identical gzip wire format — existing gzip backups read back unchanged.
- **Default behavior is unchanged for existing flags.** The only always-on addition is `application_name` labelling (and only if you haven't set your own). Every other capability is gated behind a new opt-in flag and auto-sizes conservatively when enabled.
- **All new flags are PG-target-aware.** `--max-target-connections`, `--reap-stale-backends`, `--index-build-mem`, and `--index-build-parallelism` are accepted on both `migrate` and `sync start`; the index-build and connection-budget tuning is PG-target only and no-ops on MySQL targets.

### Who needs this

- **Anyone migrating into managed Postgres with a tight connection cap** (Heroku, RDS small tiers, Supabase, Crunchy Bridge, PlanetScale-for-Postgres). The budget preflight turns a mid-copy `too_many_connections` crash into an up-front, actionable refusal — and the AIMD backoff rides out transient slot contention from peer processes.
- **Operators who've had a run killed mid-copy** and then hit a locked-out cold-start on retry. `--reap-stale-backends` clears the orphan that was holding the table lock.
- **Large-schema migrations with many secondary indexes.** `--index-build-mem auto` + `--index-build-parallelism auto` cut the deferred-index phase substantially on tiers with spare memory and workers.

### Open backlog after this release

Zero numbered bugs, zero tracked silent-loss-class follow-ups. This arc was throughput + resilience hardening on top of a clean backlog.
