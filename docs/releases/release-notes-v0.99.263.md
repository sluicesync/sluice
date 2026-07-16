# sluice v0.99.263

The confirming audit's correctness batch plus the findings from the first AWS validation set — three live probes (S3, RDS MySQL, RDS Postgres) that closed one standing verification gap and found two product defects, all fixed here.

## Added

- **AWS RDS MySQL detect-first binlog-retention advisory.** On sync/backup against an RDS source, sluice reads the real `binlog retention hours` from `mysql.rds_configuration` and WARNs when it's NULL — RDS purges each binlog ~5–11 minutes after creation on defaults, regardless of what `@@binlog_expire_logs_seconds` claims, and an attached stream does not hold the purger back (live-probed) — naming the `mysql.rds_set_configuration` remedy. A correctly configured source stays silent: detection beats pattern-guessing.
- **A mutilated mydumper dump (schema file, zero data chunks) now WARNs** instead of streaming silently empty; real empty tables (metadata `rows = 0`) stay quiet.
- **The Bug-191 decoder differential is a permanent gate:** a fuzz target against an independent grammar-derived reference decoder (1M-input run, zero differentials) plus an allocation-bound test, so the quadratic class can't ship green again. parquet-go bumps no longer auto-merge (the DuckDB compat job is the format boundary's only independent reader).

## Fixed

- **RDS/Aurora Postgres slot-CDC false refusal** — the preflight now recognizes `rds_replication` membership (RDS never sets `rolreplication`; live-proven capable); recovery text is provider-aware. FTWRL hints on RDS no longer advise the dead-end "Grant RELOAD"; `trigger setup` no longer probes a role that doesn't exist in stock PostgreSQL.
- **postgres-trigger CDC crash-loop on array columns** (provider-independent, found live): payload array elements are re-parsed type-aware per element family — a blind float coercion would have silently truncated `numeric[]` — pinned by an 11-family × shape × operation matrix. Documented limitation: `to_jsonb` normalizes float `-0` to `+0` source-side.
- **The slot-health terminal page retries delivery until it lands** — a transiently dead sink can no longer swallow the only page an unrecoverable slot event gets.
- **Postgres migrate-state timestamps are UTC** — on non-UTC servers the heartbeat guard mis-read ages by the zone offset in both directions (verified live: a 7-hour guard bypass and a 2-hour false refusal).
- **Shape gate:** same-engine MySQL pairs compare charset/collation; a pre-existing table missing the schema's primary key refuses; the index-drift advisory sees index TYPE.
- **S3 409 `ConditionalRequestConflict` maps to the coded chain-conflict refusal** (ground-truthed on real AWS; ADR-0160's S3 row is now real-cloud-verified) instead of the degrade WARN.
- Remaining un-coded encryption-state conflicts carry `SLUICE-E-BACKUP-ENCRYPTION-MISMATCH`; `compact`/`prune` refuse `--sign`+`--sign-key`; the expand-contract advice loop and review-timeout message are fixed (the timeout escape is approve AND deploy in the PS UI).

## Docs

managed-services.md gains AWS RDS for MySQL and for Postgres sections; the roadmap/ADR staleness sweep landed; duckdb-verify's actions are SHA-pinned.

## Compatibility

**No breaking changes.** New advisory WARNs and refuse-louder codes only; parquet-go dependabot bumps now require a manual merge after the DuckDB check.

## Who needs this

Anyone running sluice against AWS RDS (both engines get true preflights and working slot-CDC), anyone using the postgres-trigger engine with array columns (the crash-loop), scheduled backfills on non-UTC Postgres servers (the heartbeat guard), and anyone relying on slot-loss paging through a sometimes-flaky sink.

## Install

- Binaries: https://github.com/sluicesync/sluice/releases/tag/v0.99.263
- Homebrew: `brew install sluicesync/tap/sluice`
- Scoop: `scoop bucket add sluicesync https://github.com/sluicesync/scoop-bucket; scoop install sluice`
- winget: `winget install sluicesync.sluice`
- Docker: `docker pull ghcr.io/sluicesync/sluice:v0.99.263`
