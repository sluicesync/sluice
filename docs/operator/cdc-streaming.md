# CDC streaming: the applier retry policy

`sluice sync start` opens a change-data-capture stream and applies
source changes to the target indefinitely. On a managed database
(PlanetScale-Vitess, managed Postgres / Patroni), the apply path will
periodically hit a **transient** infrastructure error — a Vitess
transaction-killer rollback, a vtgate restart, a tablet failover, a
throttler engagement, a Postgres serialization failure, a standby
promotion, or a connection reset. None of these mean the operator's
data or configuration is wrong; they are normal operational events on
a managed deployment.

Sluice absorbs these with a **bounded, observable retry policy** on
the applier's batch loop. The full design rationale lives in
[ADR-0038](../adr/adr-0038-applier-retry-on-transient-errors.md); this
page is the operator-facing summary.

## What gets retried (and what does not)

The retry is **default-deny**: an error is retried only if it matches
a documented transient shape. Anything else exits the stream loudly so
a real problem surfaces immediately rather than being masked by a
retry loop.

**MySQL / Vitess — retriable:**

- `Error 1213` (InnoDB deadlock victim) — the idempotent replay
  succeeds against the new lock order.
- `Error 1105 (HY000)` whose message contains `vttablet` **and** one
  of `code = Aborted` / `code = Unknown` / `code = Unavailable` /
  `code = ResourceExhausted` — the Vitess transient class (tx-killer,
  vttablet not ready, failover in flight, throttler engaged).
- Connection lost / EOF / bad-connection / per-exec timeout — the
  driver reconnects on the next attempt.

**PostgreSQL — retriable:**

- `40001` (serialization failure), `40P01` (deadlock detected).
- `57P01` / `57P02` / `57P03` (admin / crash shutdown, cannot connect
  now) — standby promotion or restart.
- The entire `08*` class (connection exception).
- Connection lost / EOF / per-exec timeout.

**Trigger-CDC transports (pgtrigger / sqlite-trigger / d1-trigger) —
retriable (v0.99.286):** transient transport shapes on the change-log
poll — connection reset/refused, timeouts, TLS handshake timeout, and
(D1) HTTP 408/429/5xx. Since v0.99.289 the `postgres-trigger` poll
also retries PG's connection-availability SQLSTATEs — `57P01`/`57P02`/
`57P03` (admin/crash shutdown, cannot connect now — a managed-PG
maintenance restart or standby promotion) and the class-08 connection
exceptions. Wrong DSN, bad token, a missing change-log table (`42P01`),
and decode faults stay terminal.

**Connect-phase transients — retriable (v0.99.288):** each retry
attempt first has to re-establish its connections (reopen the target
applier and source readers), and a transient network failure there —
a dead pool connection (`invalid connection`), reset/refused,
timeouts, TLS handshake timeout — now rides the same bounded budget
instead of exiting the stream. Positively-matched shapes only: DSN
parse errors, bad credentials, unknown-host, and coded refusals stay
terminal, and a target that can never be reached still exhausts the
budget loudly.

**Explicitly NOT retriable (both engines):** duplicate-key
(`1062` / `23505`). A duplicate key during continuous sync is either
an operator data issue (a non-PK uniqueness violation) or a sluice
idempotency gap — both deserve a hard, loud failure, never a
retry-and-mask. This is a deliberate fence (see ADR-0038 and GitHub
issue #14); it is not a gap.

## The retry shape

When a retriable error fires, the streamer waits and re-applies the
batch:

- **Exponential backoff**, starting at `--apply-retry-backoff-base`
  (default `100ms`), doubling each attempt, capped at
  `--apply-retry-backoff-cap` (default `30s`). With defaults the
  per-attempt sequence is `100ms → 200ms → 400ms → 800ms → 1.6s →
  3.2s → 6.4s → 12.8s`.
- **Bounded attempts.** After `--apply-retry-attempts` (default `8`)
  *consecutive* failures of the same un-progressed position, the
  stream exits with a terminal `apply retry budget exhausted` error
  that preserves the most recent transient. Eight attempts with the
  default schedule is roughly four minutes of total wait — long
  enough to ride out a vtgate restart or a Patroni failover, short
  enough that a genuinely stuck batch does not hide for hours.
- **Counter resets on progress.** If the persisted CDC position
  advanced between attempts (a partial batch committed before the
  failure), the consecutive-failure counter resets to 1. A stream
  surviving for days does not accumulate retry debt from unrelated,
  widely-spaced transients.
- **Idempotent by construction.** A retried Insert is absorbed by the
  PK-keyed UPSERT; a retried Update/Delete is a no-op against the
  already-applied state (ADR-0010). On the batched path, position and
  data are written in the same target transaction, so a rolled-back
  attempt rolls back both. On the per-change path, rows inside a
  source transaction commit position-free and the position lands with
  the delivered TxCommit (ADR-0007's 2026-08-12 amendment) — the
  invariant either way is directional: a persisted position never
  covers un-applied data, and the reachable divergence
  (data-ahead-of-position) replays idempotently on resume. The retry
  adds no new correctness requirement.

## Observability

Every retry logs at INFO:

```
level=INFO msg="applier: transient error; retrying"
  stream_id=… engine=… attempt=3 max_attempts=8
  backoff=400ms err="<the wrapped driver error>"
```

Budget exhaustion logs/returns a terminal error at the stream's exit:

```
pipeline: apply retry budget exhausted after 8 consecutive
failures at position "<token>": <last transient>
```

Same transient eight times means the dependency is genuinely down
(e.g. vtgate is not coming back); different transients across the
eight means intermittent infrastructure.

## Configuration

| Flag | Default | Range | Notes |
|---|---|---|---|
| `--apply-retry-attempts N` | `8` | `1`–`64` | `1` = no retry; exit on the first transient (the pre-v0.42.0 behaviour). |
| `--apply-retry-backoff-base DUR` | `100ms` | `10ms`–`10s` | Starting backoff before exponential doubling. |
| `--apply-retry-backoff-cap DUR` | `30s` | `1s`–`300s` | Per-attempt upper bound. |

Out-of-range values are **rejected at startup** with a precise error
(not silently clamped) so the worst-case envelope an operator computes
from the docs is always the one the policy actually uses.

Operators on bare-metal MySQL with an unbounded transaction lifetime,
or anyone who wants the strict fail-fast behaviour, can opt out with
`--apply-retry-attempts=1`. Operators expecting a slow Patroni
failover under throttler load can widen the envelope, e.g.
`--apply-retry-attempts=20`.

## Tables the target lacks: skip-and-count, never halt

When the stream carries changes for a table the **target** doesn't have — a drifted publication scope, a table the operator dropped on the target, a keyspace-wide binlog stream whose target was migrated with `--include-table` — every engine's applier **skips those events** instead of halting the stream. The choice is deliberate, because the blast radius is inverted: the source still holds every skipped row, so a skipped table is always recoverable with `sluice schema add-table` (a fresh table snapshot), while a halted stream lags *every* table and — if the halt outlives binlog/slot retention — loses the resume position itself, converting one table's drift into a whole-database re-snapshot.

A skip is never log-only:

- The applier **WARNs once per table** (not once per event) with the remedy.
- **Every skipped event is counted durably** in the per-target `sluice_cdc_skipped_tables` control table: one row per (stream, table) with the cumulative count and the first/last skipped source position tokens, surviving restarts.
- `sluice sync status` renders the ledger (`SKIPPED TABLES` block; `skipped_tables` in `--format json`), the `sync stop` summary and the stream's exit log repeat it, and **`sluice sync health` exits 1 while any skip count is nonzero** — there is no threshold flag, because skipped tables only resolve through operator action.

The remedy is always one of two explicit choices: re-attach the table with `sluice schema add-table` (fresh table snapshot — nothing was lost; the source still holds every row), or make the exclusion explicit with a table filter so the stream no longer carries the table at all.

The skip resolves when the applier looks the table up (metadata resolution). A table that vanishes *mid-stream after rows were already applied to it* is a different condition and still fails loudly at execution time — destroying replicated state out from under a running stream deserves a halt, and on the batched apply paths a failed statement aborts the whole batch transaction anyway.

## Foreign keys during CDC apply

A CDC change stream is **not foreign-key-dependency-ordered**, so the
applier deliberately bypasses target FK enforcement for the duration of
each apply transaction. This is the standard logical-replication
technique (it is what Postgres's own logical replication does):
constraint integrity is the **source's** responsibility — it has already
validated every change — so the target faithfully mirrors the source,
including any FK-inconsistency the source itself permits.

The two engines differ in what else the bypass suppresses. On Postgres,
`session_replication_role = replica` also disables **user triggers**
during replay, so replicated rows do not double-fire target triggers
(again matching Postgres logical replication). On MySQL,
`foreign_key_checks = 0` disables **FK constraints and FK cascades
only** — target user triggers still fire on replayed rows.

Why this is necessary: a source that does not enforce FKs (SQLite with
the default `PRAGMA foreign_keys=OFF`, MySQL MyISAM, or any application
that deletes a parent row that still has children) emits orphaning
changes, and sluice's concurrent key-hash apply lanes
(`--apply-concurrency`) can commit a child INSERT before its parent in a
different lane. Enforcing the target FK against such a stream would
reject a routine source operation and halt replication. The applier
therefore:

- **Postgres** — sets `SET session_replication_role = replica` on each
  apply transaction.
- **MySQL** — sets `foreign_key_checks = 0` on each apply session.

The bypass is scoped to sluice's own apply work; the constraints remain
on the target schema and are enforced for every other client. (A bulk
migrate, separately, defers and re-validates constraints after the copy
— this section is specifically about continuous CDC apply.)

### Managed-Postgres privilege caveat

`SET session_replication_role` requires elevated privilege — superuser,
`rds_superuser`, or a role explicitly granted it. On a managed Postgres
where the apply role lacks it, sluice cannot bypass FK/trigger
enforcement; rather than failing cryptically it emits a **one-time
WARN** at the first apply and continues. The sync still works for
FK-consistent streams, but an FK-violating or out-of-order change will
then fail the apply loudly. To get the full bypass on such a target,
grant the apply role the privilege to set `session_replication_role`,
or make the target FK constraints `DEFERRABLE`. MySQL's
`foreign_key_checks` needs no special privilege.

## MariaDB sources

MariaDB is a first-class MySQL-family flavor (`--source-driver mariadb`,
since v0.99.268), not "MySQL with a different banner": `sluice sync start`
reads its binlog the same way it does vanilla MySQL's, through the same
snapshot→CDC handoff and the same retry policy above. Three
flavor-specific behaviors are worth knowing as an operator:

- **Domain GTIDs.** MariaDB replicates with domain GTIDs
  (`domain-server-sequence`, e.g. `0-1-38`), not MySQL's UUID-set GTIDs.
  sluice's CDC position, warm resume, and cold-start anchor
  (`@@gtid_binlog_pos`) all speak the MariaDB shape natively
  ([ADR-0170](../adr/adr-0170-mariadb-flavor-phase3-cdc.md)); a restart
  resumes exactly-once from the persisted domain-GTID position, and a
  source that has purged the binlog past that position refuses loudly
  (errno 1236) instead of silently restreaming from the wrong point.
- **Native `uuid` / `inet6` / `inet4` columns** decode faithfully off the
  binlog
  ([ADR-0171](../adr/adr-0171-mariadb-native-uuid-inet-cdc-decode.md)).
  The wire format is subtle — MariaDB strips trailing `0x00` bytes from
  these fixed-width values, and its `inet6` text rendering differs from
  most languages' — so the decode was ground-truthed byte-exact against
  live 11.4 and 10.11 and pinned per type × shape. CDC values converge
  byte-for-byte with what a bulk copy of the same rows produces, on both
  same-engine and cross-engine (e.g. MariaDB → Postgres `uuid`/`inet`)
  targets.
- **Version-dependent collation catalog.** MariaDB's
  `information_schema.COLLATIONS.PAD_ATTRIBUTE` column is absent through
  the 11.x LTS line and 12.0 (added in 12.1), so sluice keys trailing-
  space `=` semantics off the version-independent `_nopad_` collation
  name token and the server's own behavior — relevant when a filtered
  `--where` predicate compares string columns. Details in the
  [field note](https://sluicesync.com/field-notes/mariadb-no-pad-attribute-column/)
  and in
  [migrating-legacy-mysql.md](migrating-legacy-mysql.md#mariadb-sources-and-targets).

## Trigger-based CDC sources: `sqlite-trigger` and `d1-trigger`

Most CDC sources read a native change stream — Postgres logical
replication, MySQL binlog, Vitess VStream. SQLite and Cloudflare D1 have
no such stream: SQLite's WAL is a *physical* page-log, not a logical
record of row changes. So sluice captures their changes with **triggers**,
the same technique the `postgres-trigger` engine uses for managed Postgres
that blocks logical replication ([ADR-0066](../adr/adr-0066-postgres-trigger-engine-variant.md)).

`sluice trigger setup --source-driver sqlite-trigger` (a local file) or
`--source-driver d1-trigger` (a live D1 over the HTTP query API) installs:

- **Per-table AFTER INSERT / UPDATE / DELETE triggers** that write the
  before/after row image into a `sluice_change_log` table. The image is
  encoded as the same `(typeof, text/hex)` pairs the lossless `d1` reader
  uses (NOT `json_object()`, which would round integers above 2⁵³ to
  float64 and can't represent BLOBs) — so capture is value-exact
  ([ADR-0135](../adr/adr-0135-sqlite-trigger-cdc.md) /
  [ADR-0136](../adr/adr-0136-d1-trigger-cdc.md)).
- A **monotonic-`id` watermark** on `sluice_change_log`: the polling
  reader emits changes in `id` order and persists the last applied `id`,
  so a restart resumes **exactly-once** from the durable position.
- A **`MAX(id)` snapshot anchor** captured at snapshot start, so the
  cold-start → CDC handoff is gap-free (every change after the anchor is
  streamed; everything at or before it is already in the snapshot).
- A **captured-column fingerprint** (`sluice_change_log_columns`) that
  **refuses loudly on schema drift** rather than silently dropping a
  column added after setup (SQLite has no DDL triggers, so an added
  column is otherwise invisible to the capture path).

**Known limitation — `OR REPLACE` across a UNIQUE conflict.** With SQLite's
default `PRAGMA recursive_triggers` OFF (a per-connection setting of *your
application's* writing connections), the row an `INSERT OR REPLACE` /
`UPDATE OR REPLACE` implicitly deletes to satisfy a **non-PK UNIQUE**
conflict fires no DELETE trigger, so that delete is never captured and the
target keeps the row — a silent divergence. `trigger setup` WARNs on every
table with a non-PK UNIQUE constraint. Mitigate by running writers with
`PRAGMA recursive_triggers = ON`, or by writing upserts as
`INSERT ... ON CONFLICT DO UPDATE` (which never implicitly deletes); on D1
prefer the latter. Details in
[ADR-0135](../adr/adr-0135-sqlite-trigger-cdc.md).

`d1-trigger` polls D1's **primary** (strongly-consistent) query path, not
a read replica, so commit order equals `id` order. Because every change
appends a `sluice_change_log` row that is never auto-reaped, run
`sluice trigger prune` periodically to bound source growth — it deletes
only rows strictly below the target's durable applied watermark (see
[trigger-changelog-retention.md](trigger-changelog-retention.md)). The
full operator walkthrough, including teardown, is in
[sqlite-d1-import.md](sqlite-d1-import.md).

## Re-copying onto a target that already holds data

`sync start --restart-from-scratch` and the automatic re-snapshot (which fires
when a stream's persisted position has aged out of the source's retention
window) both re-copy onto a target that already holds the previous copy. What
happens to the rows already there depends on whether the source's snapshot
reader is **idempotent**:

- A **non-idempotent** reader's copy is a plain `INSERT`, so the in-scope
  target tables are cleared first — they always were, or the copy would
  dup-key. This is every source except the one below.
- The **idempotent** reader — the VStream snapshot, which backs the
  **PlanetScale** and **Vitess** flavors — upserts, so its target used to be
  left in place. From v0.109.0 an explicit `--restart-from-scratch` clears it
  there too, because an upsert converges on rows that still exist at the source
  and cannot remove one the source deleted. The **automatic** re-snapshot still
  merges, by design, and warns about exactly that.

A native MySQL binlog snapshot is **not** idempotent, so a plain `mysql` source
is on the cleared side even though it lives in the same engine package as the
VStream reader.

<!-- idempotent-copy-engine-packages: mysql -->

The marker above is held to the code by `internal/docsync` — it lists the
engine PACKAGE that pins `ir.IdempotentCopyReader`, which is why its key says
`-engine-packages` rather than `-engines`. It is the one marker in that package
that names packages instead of `--source-driver` values, because the pin sits on
the VStream snapshot reader rather than on a type every flavor reaches. The
flavor sentence beside it is the part the gate cannot check, so read both
together.

## See also

- [ADR-0038](../adr/adr-0038-applier-retry-on-transient-errors.md) —
  full design, classifier tables, and the operator-review sign-off.
- ADR-0010 — idempotent applier (the load-bearing precondition).
- ADR-0007 — position/data atomicity (preserved by retry).
- ADR-0027 — source-transaction-boundary batching (preserved by retry).
- [ADR-0135](../adr/adr-0135-sqlite-trigger-cdc.md) /
  [ADR-0136](../adr/adr-0136-d1-trigger-cdc.md) — the SQLite / D1
  trigger-CDC engines.
