# sluice v0.151.0

**A real operator ran a migration and wrote down where they stumbled. Most of this release is that list.** Five stumbling blocks, three of which were one problem seen three times: sluice knew the remedy and never showed it to them. The fourth needed a fix on the website. The fifth turned out to be a data-loss hazard nobody had asked about.

Also here: PlanetScale Neki's MoveTables is now measured underneath a live stream, and two sharded-target refusals that only ever reached half the commands they were written for.

## Features

**A coded refusal's remedy now reaches the terminal.** sluice had two hint mechanisms with opposite human visibility: `migcore.WrapWithHint` folds its hint into the error text, while a `sluicecode.CodedError`'s `Hint` appeared **only** in the structured slog record — the dense machine-facing line an operator skims past. The last line of output, where anyone actually looks, carried the diagnosis and no fix. Every coded construction site in the tree carries a remedy (242 of 243), and for many it is the only statement of what to do. The exit boundary now appends it:

```
sluice: error: source: the DSN host "aws.connect.psdb.cloud" is a PlanetScale endpoint …
hint: pass --source-driver planetscale
```

An error whose text already ends in its own hint block does not grow a second one, and an uncoded error is untouched.

**`migrate` and `sync start` now ask whether the target accepts DDL *before* the schema phase.** On a PlanetScale branch with Safe Migrations enabled, sluice used to discover this partway through schema-apply, leaving a half-built target that the next run had to `--resume` past. It now probes with a throwaway `CREATE TABLE`/`DROP` first and refuses in about 200 ms with nothing created (`SLUICE-E-PS-DIRECT-DDL-BLOCKED`).

The check is **behavioural, not API-derived, and that is the point**: PlanetScale's `safe_migrations` flag flips the moment you disable it in the UI, but the change propagates asynchronously to each gateway. In that window the API says "disabled" and the cluster still refuses — which is exactly the window that cost the reporting operator a second wasted cycle. The flag and the behaviour are two different facts, and only one of them decides whether your DDL succeeds.

A refusal is conclusive; a pass is **not**, so a pass is silent. sluice never reports "Safe Migrations is off", because an accepted DDL means *this gateway, this connection, this moment*. This is a fail-fast, not a clearance. A probe that cannot run logs a WARN and the run proceeds — it must never become a new way for a working configuration to fail.

It also does not probe when the run needs no DDL. The [ADR-0166](docs/adr/adr-0166-migrate-precreate-shape-gate.md) bootstrap — ship the schema through deploy requests, then run `migrate`, which skips every pre-created table — is unaffected, because the probe runs after the pre-create gate and only when something is actually left to create. And it is scoped to the Vitess-descended flavors: a vanilla MySQL or MariaDB target has no mode that refuses direct DDL, so it issues no probe at all.

**A new advisory when your replication slot will not survive the cluster changing underneath it.** sluice has created slots with `FAILOVER true` on PG 17+ since [ADR-0012](docs/adr/adr-0012-pglogrepl-bypass-for-failover.md). That flag is **necessary and not sufficient** — it marks a slot eligible for synchronization, and something still has to do the synchronizing. `sync` cold start now reads `sync_replication_slots` and `hot_standby_feedback` from the source and warns when it cannot confirm that anything will.

**Being single-node does not exempt you, and this is measured.** On PlanetScale Postgres 18.6 with zero replicas (2026-09-10): a slot created with `failover => true`, read back as `failover=t, synced=f` at `restart_lsn=0/96001428`, was **destroyed by a routine PS-10 → PS-20 resize** — `count(*) FROM pg_replication_slots` went to `0`. A resize replaces the node, which strands a primary-local slot exactly as a promotion would. For a stream that is a lost position and a full re-copy, from an operation most operators consider routine maintenance.

It **warns and deliberately cannot refuse**. Patroni permanent slots — PlanetScale's "Logical slot name" field — preserve slots through a mechanism that leaves no trace in `pg_settings`, so a correctly-configured cluster reads identically to an unprotected one. The message says "sluice cannot confirm you are protected", never "you are unprotected".

**PlanetScale Neki MoveTables, measured underneath a live stream** — the last open row of the Neki readiness matrix, and the analogue of the Vitess filtered move-OUT class that shipped a Critical. Byte-identical over 2,000 rows across `move_tables_create` → `switch_reads` → `switch_writes` → `reverse_traffic`, with a writer running and a stream restart mid-cycle.

`create` and the read switch are transparent to a running stream — structurally so, since a Postgres client chooses its database at connect time. The **write** switch blocks the table on the source database and the stream halts. That halt is the correct outcome and it is why the pass holds: the persisted CDC position stops *before* the block rather than past the unapplied changes, so reversing the cutover and restarting replays the gap to exact parity. New `SLUICE-E-TARGET-TABLE-BLOCKED-BY-WORKFLOW` turns a bare `SQLSTATE NK213` into a sentence naming `__neki.list_blocked_tables()`, `move_tables_status()`, and both end states.

## Fixed

**Two sharded-target refusals reached `migrate` and not `sync start`.** `PreflightShardPlacement` and `PreflightShardKeyUpsert` shipped in v0.150.0 wired into migrate's target phase only. Against the same Neki target with the same schema, deterministic 2/2: `migrate` refused at preflight with the table never created, while `sync start` copied all 60 rows, reached CDC, and died on the first change — an INSERT was enough, so no workload survived the shape. Both now run at sync cold start too, before the snapshot opens.

The gate that should have caught this said, in its own scope note, that *"the target-side and emit-side preflights … the fan-out already shares with its siblings through migcore"*. Sharing a migcore helper is not sharing a call. `TestTargetPreflightRoster_ColdStartReachesMigrateSiblings` now derives migrate's target-preflight universe from the AST and fails when the cold start does not reach one.

**The safe-migrations message omitted the two facts that cost a cycle each.** It said "temporarily disable Safe Migrations, run sluice". The operator did exactly that, re-ran, and hit the same refusal — because disabling is not instantaneous. Then, because the first attempt had already created part of the schema, a plain re-run was wrong too and they needed `--resume`. Neither fact was anywhere in the message. Both are now, and the message is line-broken rather than one run-on sentence.

**Two published claims about Neki were measured false.** `postgis` is **not** creatable on Neki — the default role is not a superuser and `CREATE EXTENSION postgis` fails with `permission denied` (42501), even though it is listed in `pg_available_extensions` at 3.6.4. A PostGIS user reading the old sentence would have planned a migration that cannot complete. Replaced with the measured boundary (nine extensions that install, three that refuse) and a warning that the allowlist is **not** PostgreSQL's own `trusted` flag: `vector` is untrusted and installs, `postgres_fdw` and `pg_stat_statements` are untrusted and do not. Separately, `sync` out of Neki fails on a **refused replication connection** (`FATAL: replication connections must target a specific shard`, measured on an *unsharded* database), not on the snapshot-import mechanism the page named. The verdict was right; the mechanism was not, and a wrong mechanism sends an operator hunting for a workaround at the wrong layer.

## Compatibility

No flags added or removed; no configuration changes. Three behaviour changes worth knowing:

- **Coded errors print one extra line.** Anything parsing sluice's stderr for exact error text should expect a trailing `hint: …` on coded failures. Exit codes are unchanged — the wrap keeps the `CodedError` reachable through `errors.As`, pinned by a test, because a refusal that stopped exiting 3 would be a worse bug than the one being fixed.
- **`sync start` can now refuse where it previously copied first.** The two sharded-target refusals reach the cold start. This only affects sharded PlanetScale Neki targets; the refusal was always correct, it just used to arrive after the whole copy.
- **A new WARN on PG 17+ sources** whose cluster will not synchronize failover slots. Advisory only; nothing refuses.
- **PlanetScale and Vitess targets see one extra throwaway statement** per `migrate` / `sync` cold start that has schema to create: `CREATE TABLE IF NOT EXISTS sluice_direct_ddl_probe (id INT NOT NULL PRIMARY KEY)` followed by a `DROP`. Vanilla MySQL and MariaDB targets issue nothing — they have no mode that refuses direct DDL, so there is nothing to probe for. The table carries the `sluice_` prefix and is on the control-table roster, so if a dropped connection ever leaves one behind it is classified as sluice bookkeeping rather than enumerated as one of your tables.

## Who needs this

**Anyone migrating to PlanetScale MySQL** — the safe-migrations preflight and the hint-visibility fix together remove the three stumbling blocks that cost a real operator most of an afternoon.

**Anyone running `sync` from PostgreSQL 17 or newer, and especially on PlanetScale Postgres** — read the new advisory if it fires. A cluster resize destroying your replication slot is measured behaviour, not a hypothetical, and it applies to single-node instances.

**Anyone using PlanetScale Neki as a sharded target** — the sharded-target refusals now fire before `sync start` copies anything, and a MoveTables cutover underneath a live stream is a known, loudly-handled state rather than an unexplained `NK213`.

## Install

```
brew install sluicesync/tap/sluice
scoop bucket add sluicesync https://github.com/sluicesync/scoop-bucket && scoop install sluice
docker pull ghcr.io/sluicesync/sluice:0.151.0
```

Or download a binary from the assets below.
