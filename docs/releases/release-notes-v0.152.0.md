# sluice v0.152.0

**PlanetScale Neki works now.** A first migration into a fresh Neki database used to fail — twice, measured, at 116s and 191s — and every defence sluice had was watching it happen. This release is a day of live work against real Neki clusters: six transient failure classes now classified correctly, a raw-copy lane that can survive being interrupted, index builds that no longer die at thirty seconds, and a `metrics-watch` that reports the machine that is actually saturated.

A 39.9-million-row copy that previously could not finish now completes in 32m22s with zero retries and zero gate trips.

## The headline: the fast lane could not survive the most ordinary thing that happens to it

A fresh managed volume grows on demand, and the grow window interrupts writers. sluice was ready for that: `53100` (insufficient resources) and `08006` (broken pipe) were both classified retriable *specifically* for this event, and ADR-0110's grow-gate dutifully tripped and quiesced every copy lane.

The copy died anyway. The retry classification decides *whether* to retry; the raw-copy lane had nothing to retry **with** — it calls `runRawCopyChunk` once and returns the error. Its typed-lane sibling has had `copyChunkWithRetry` all along. It was never tried and abandoned; it was never wired.

It is wired now, at both call sites — the chunked path and the whole-table path — with a bounded exponential backoff and a loud refusal when the envelope is exhausted. Re-running a chunk does not double-copy: each attempt builds a fresh pipe with the same PK bounds, and a failed attempt rolls back inside its own transaction — with one carve-out at the commit boundary, below. On the run that proved it, retries fell from 202 to 6, the worst chunk from 52 attempts to 8, and completed chunks rose from 3 of 64 to 49.

**What the whole investigation turned out to be about.** Once a volume was pre-sized to 100 GiB, all six "independent" failure classes vanished together — zero retries, zero gate trips, zero errors across 39,924,001 rows. They were symptoms of storage pressure, not separate faults. Pre-sizing a managed volume ahead of a large first load is the real advice, and the retry is what makes the un-pre-sized case survivable instead of fatal.

**One case is deliberately NOT retried, and it is the interesting one.** A retry is safe because a failed attempt leaves nothing behind — the import runs in a transaction, and a connection that dies mid-COPY takes the transaction with it. That argument holds everywhere except the commit itself: a `COMMIT` whose *response* never arrives is in doubt, because the server may have committed and lost the reply. Re-exporting there would stack a second copy on top of the first. On a chunked table the duplicates would fail loudly when the primary key is added; on the whole-table lane, which needs no primary key, nothing would catch them. That single case is now terminal and says so — the table fails, names the uncertainty, and a re-run cleans up rather than appending. Found by the pre-tag value-fidelity review of the retry that introduced it.

## A server statement timeout was silently a cap on table size

PostgreSQL's own `statement_timeout` default is 0, which is why this went unnoticed for so long. A managed platform may not agree: PlanetScale Neki ships `statement_timeout = 30s`. A bulk `COPY` is ONE statement over a whole table or chunk, so a non-zero timeout is a wall-clock cap on how much data can be moved — cross it and the copy dies with `57014` having written nothing, and re-running hits the same wall deterministically.

Both directions are pinned now, transaction-scoped so the pin survives a transaction-mode pooler:

- **The copy lanes** (`COPY` on the write side and the raw byte-pipe).
- **The source read** — `ReadRows`, keyset-boundary sampling, and the exact `COUNT(*)` preflight. Every read whose cost scales with the table. The LIMIT-bounded keyset page is deliberately left unpinned, and the reasoning is written where the decision lives.

Neither pin leaks: a session-scoped GUC on a pooled connection would stay set for whatever ran next, because pgx's `ResetSession` issues no `DISCARD ALL`.

## Index builds on a Neki target

`CREATE INDEX` on a 26.3M-row table died at 31 seconds — the platform's 30s statement timeout, and the copy-lane pin deliberately does not reach index builds (a backend inside `CREATE INDEX` is not reading its socket, so sluice's own cancellation could not stop it; the server's timeout is the only bound there is). In practice that meant **any secondary index on a large table blocked the migration**.

A Neki target now routes index creation through the platform's own online DDL (`__neki.online_ddl_create`), polls per-shard readiness, and completes the workflow. Measured end to end: ~36 minutes for a single-column index on 28.9M rows, where the direct statement could not finish at all.

## `metrics-watch` was reporting the wrong pod, and could not see the routing layer

Two independent problems, both found by looking rather than reasoning.

**It reported a REPLICA's CPU.** The primary selector matched on the container label alone, and on Neki both the primary and every replica carry it — so it returned whichever pod the exposition happened to list first. It went unnoticed because the branch that was checked live was thrashing, with every pod near 100%: the wrong answer equalled the right one. The fix also enumerates the blast radius, which was narrower than it looked — CPU and memory were affected; storage, capacity and lag carry no container label and were always correct.

**It had no field for the front of the database.** An operator watched their Neki routing layer sit pegged at 100% while throughput collapsed and every number sluice could show was about the database — which was fine. There are now five new series, deliberately split into two groups that are NOT interchangeable:

| | in sluice's path? | when it saturates |
|---|---|---|
| `sluice_target_router_cpu_util` / `_mem_util` | **Yes.** A Neki connection is an ordinary port-5432 Postgres connection that lands on a router first, the way Vitess routes through VTGate. There is no way around it. | Every statement sluice issues is queueing. The remedy is a larger router tier — a separate control from the database's size. |
| `sluice_target_pgbouncer_cpu_util` / `_mem_util` / `_client_wait_seconds` | **No.** sluice connects to PlanetScale Postgres directly, and logical replication cannot traverse a transaction pooler at all. | Your *application's* clients are queueing against a database sluice is loading. Worth watching during a migration; never a reading about sluice's own throughput. |

`--notify-router-cpu-util` arms an alert on the first of those, on both `sync start` and `metrics-watch`. It is a separate rule from `--notify-cpu-util` because the two saturate independently and are fixed by different controls — arming one is not arming the other.

Both groups report the **busiest** instance rather than an average: connections spread across instances, so one pegged instance is a real stall for the traffic it serves, and averaging three dilutes a 100% reading to 33%. Each carries its own "observed" flag, so a platform without one of these components reports nothing rather than a comfortable-looking zero.

## Fixed

**A SQLite `DATETIME` is tz-naive, and a MySQL target now gets `DATETIME` instead of `TIMESTAMP`.** The declared-type resolver produced `ir.Timestamp`, which the MySQL writer emits as `TIMESTAMP` — an instant type, zone-converted on store, spanning 1970-01-01 to 2038-01-19. An ordinary pre-1970 or post-2038 SQLite datetime therefore refused at insert with a bare server `1292`, and one inside the window silently acquired a zone conversion it never asked for. It now resolves to `ir.DateTime`, which MySQL emits as `DATETIME` and which carries the value. `--infer-types` makes the same split from the data: all-offset value sets stay zoned, anything naive is naive. **Postgres targets are byte-identical** — both IR families emit PG `TIMESTAMP` — which is why this was MySQL-only.

**Six Neki failure classes were being graded wrongly**, each turning a transient into a dead stream or a fault into an infinite retry:

- `NK205` (query-buffer timeout) is an overload signal, not a fault.
- A shard that goes read-only on low disk (`25006`) is transient, and is grow-gate evidence.
- A sidecar connection-pool timeout (`XX000`) killed a running stream permanently.
- A `57014` raised by the PLATFORM is transient; sluice's own echoed refusal is not — and telling them apart needed a new `ir.TerminalError`, because a classifier that text-matches through `%w` wraps will happily re-retry a refusal sluice itself issued (it replayed 26 times before this).
- A dead snapshot-PINNED connection is terminal: the snapshot is gone, so retrying on it can only fail differently.
- `quiesceAndReportTransient` computed a classification and then threw it away.

**A COPY-concurrency cap has to bound the PRODUCT of the copy axes — on every lane, not just `migrate`.** The first cut capped one axis, so a 4-table x 4-chunk fan-out still opened 16 concurrent COPYs against a limit of 4. The fix for THAT then landed on `migrate` alone: `sync start`'s cold copy and `restore` kept resolving their axes through the older path and dropped the target's ceiling entirely, so both still over-subscribed a Neki router. Caught by the pre-tag perf-parity review; all three lanes now share one resolver, held by a gate that fails on any new caller that bypasses it.

## Compatibility

- **Behaviour change, MySQL targets only:** a SQLite/D1/flat-file source column declared `DATETIME` or `TIMESTAMP` now lands in MySQL `DATETIME` rather than `TIMESTAMP`. Values previously outside 1970–2038 now migrate instead of refusing. `DATETIME` is not zone-converted, so a value that was being shifted by the session zone is now stored as written. Postgres targets are unaffected.
- **New flag:** `--notify-router-cpu-util` on `sync start` and `metrics-watch`. Default 0 (inert).
- **New metrics:** five `sluice_target_router_*` / `sluice_target_pgbouncer_*` series. Each is emitted only where the platform has that component, so existing dashboards and scrapes are unchanged on targets that do not.
- No config, state-table, or on-disk format changes. No resume/backup compatibility impact.

## Who needs this

- **Anyone migrating into PlanetScale Neki.** Before this release a first load into a fresh database was likely to fail; now it completes, and a secondary index on a large table is buildable.
- **Anyone whose source or target sets a non-default `statement_timeout`** — a managed platform, or a per-role setting meant for interactive queries. Your maximum copyable table size was silently your timeout times your throughput.
- **Anyone migrating out of SQLite, D1, or a flat file into MySQL** with dates outside 1970–2038.
- **Anyone running `metrics-watch` against a Neki branch** — the CPU number you were reading was a replica's.
