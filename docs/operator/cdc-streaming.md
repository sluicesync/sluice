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

The remedy is always one of two explicit choices: re-attach the table with `sluice schema add-table` (fresh table snapshot — nothing was lost; the source still holds every row), or make the exclusion explicit with a table filter so the stream no longer carries the table at all. One cause needs a different first step: a table whose **privileges were revoked** from the apply role reads as absent too (`information_schema` hides tables the role cannot see), and there `add-table` is the wrong remedy — restore the grant and the skip clears on the next change for that table, no re-snapshot needed.

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
  Since v0.138.0 a MariaDB position also carries a lineage anchor — the
  binlog `(file, offset)` it was captured at and `BINLOG_GTID_POS` of
  that byte — so a resume against a rebuilt or foreign instance whose
  GTIDs happen to collide refuses instead of replaying that instance's
  whole history; see the resume-signals section below for the
  `UNVERIFIED-INSTANCE-IDENTITY` warning an anchorless (pre-v0.138.0)
  position logs.
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

**Recovering from an invalid-UTF-8 / lone-surrogate halt (the local
`sqlite-trigger` lane).** The reader refuses loudly (batch withheld
atomically, watermark unmoved — nothing is skipped or duplicated) when a
captured TEXT value carries bytes JSON cannot represent faithfully, e.g.
`CAST(x'FFFE61' AS TEXT)`. On this lane **repairing the source row alone
does not unblock the stream**: the refusal's `id=N` names a row of
`sluice_change_log` whose captured image already holds the poison, and
the repairing `UPDATE` captures a *new* change whose before-image
carries it again, so a restart halts again. The runnable recovery —
which the refusal message also prints — is `sluice trigger teardown`,
repair the source row, `sluice trigger setup`, then a fresh sync.
(Advanced alternative: scrub the affected `before`/`after` images in
`sluice_change_log` directly; SQLite's JSON functions cannot locate
invalid UTF-8, so the scrub is byte-level, e.g. matching on
`hex(before)`.) For the local file's non-capturing consumers of the same
guard — catalog scalars, the snapshot phase — repairing the source value
is sufficient as written. (Bug 245.)

**The `d1-trigger` and `d1` lanes never halt on invalid UTF-8 — the
value arrives pre-mangled.** Measured on real D1 (2026-08-13): the
`/query` API replaces invalid bytes with U+FFFD *server-side*, for
direct reads and change-log images alike, so the invalid-UTF-8 arm of
this guard cannot fire on live-D1 sources — there is no halt to recover
from, and the substituted value is indistinguishable from a genuine
U+FFFD. See the caveat in
[the D1 import guide](sqlite-d1-import.md) for what is and is not
recoverable. (The lone-surrogate arm on D1 is unmeasured; this
paragraph scopes only the invalid-UTF-8 vector.)

`d1-trigger` polls D1's **primary** (strongly-consistent) query path, not
a read replica, so commit order equals `id` order. Because every change
appends a `sluice_change_log` row that is never auto-reaped, run
`sluice trigger prune` periodically to bound source growth — it deletes
only rows strictly below the target's durable applied watermark (see
[trigger-changelog-retention.md](trigger-changelog-retention.md)). The
full operator walkthrough, including teardown, is in
[sqlite-d1-import.md](sqlite-d1-import.md).

## `postgres-trigger` sources: replica-role writes are not captured by default

The `postgres-trigger` engine's capture triggers are plain `CREATE TRIGGER` by default, so Postgres fires them for **origin** writes only: any DML executed under `session_replication_role = 'replica'` bypasses them entirely. Two real configurations write under replica role, and on both the sync **exits 0 with rows silently missing from the target**:

- **The source is itself a logical-replication subscriber.** A native `CREATE SUBSCRIPTION` apply worker runs under replica role, so every row a subscription applies to a subscribed table is invisible to the capture triggers. Only rows written directly on the source database are captured. This pairing is natural on exactly the managed tiers the trigger engine targets (slot-blocked databases that *receive* logical replication but cannot *serve* it), which is why it deserves this callout.
- **The all-sluice relay (A→B→C).** sluice's own Postgres change applier sets `SET LOCAL session_replication_role = replica` on every apply transaction when the apply role is privileged (see [Foreign keys during CDC apply](#foreign-keys-during-cdc-apply)). So if database B is the target of an A→B sluice sync and simultaneously the `postgres-trigger` source of a B→C sync, the B→C capture sees **nothing** the A→B sync applies. The failure is shaped to pass testing: an unprivileged dev applier's rows *are* captured (the privilege probe fails and the bypass is skipped), while the privileged production applier's are not.

`sluice trigger setup` and every stream open probe for both shapes — `pg_subscription` rows for the subscriber shape, sluice's own `sluice_cdc_state` apply bookkeeping for the relay shape — and emit a loud `SILENT-CAPTURE-GAP RISK` WARN naming the loss scenario. It is a WARN rather than a refusal because both shapes can be intentional and safe (a subscription feeding tables outside the replication set; a decommissioned relay's leftover control table). If the WARN fires on your topology: for the subscriber shape, opt in to replicated-write capture (below); otherwise sync from the origin database directly, keep subscribed tables out of the replication set, or stop the upstream sync before trusting the capture.

### Capturing replicated writes: `trigger setup --capture-replicated-writes` (ADR-0185)

For the subscriber shape there is a supported opt-in: re-run `sluice trigger setup --capture-replicated-writes` and the per-table capture triggers (row **and** truncate) are set `ENABLE ALWAYS`, so writes applied under `session_replication_role = 'replica'` **are** captured. This is the topology it exists for: a locked-down primary you cannot install anything on → a native `CREATE SUBSCRIPTION` subscriber → sluice trigger-capture → elsewhere. Replicated INSERT/UPDATE/DELETE **and replicated TRUNCATE** are captured — ground-truthed through a real publication→subscription pair on PG 16, including the subscriber's separate TRUNCATE apply path — and with the opt-in active the subscriber-shape WARN no longer fires (it is the supported case). Re-running `trigger setup` without the flag reverts to plain origin-only triggers, provided the run names every captured table (see below). Four things to know:

- **The relay shape refuses under the opt-in** (`SLUICE-E-CDC-TRIGGER-ECHO-LOOP`, at setup and at every stream open): if the source also carries sluice's own apply bookkeeping (`sluice_cdc_state`, looked for in **every schema of the database** — an upstream sync run with `--target-schema` keeps its control table in the target DSN's schema while its rows land elsewhere, so the bookkeeping and the capture schema routinely differ; the message names where it was found), another sluice sync applies into this database, and `ENABLE ALWAYS` triggers would re-capture its applied rows as new changes — an echo loop. Capture from the origin database instead, or decommission the upstream sync (and drop its control table), or stay on the default posture. A subscription whose `copy_data = true` initial sync runs after setup is captured too — the initial copy arrives as ordinary insert rows.
- **Adding a table to an opt-in install: keep the flag on.** The posture is recorded once per install and enforced per trigger, so `sluice trigger setup --tables=<the new table>` **without** the flag would leave the tables it does not name at `ENABLE ALWAYS` while recording origin-only. Since v0.137 that run **refuses** and prints both runnable commands (re-run with `--capture-replicated-writes` to keep the opt-in, or re-run with `--tables=<every captured table>` to convert the whole install back to origin-only); an opt-in run naming a subset instead **widens** the tables it did not name, so the install can never end up half-converted. Before v0.137 the half-converted state was reachable and wedged the next stream open with a message blaming a hand-flip; if you have one, the refusal at open now prints the full `--tables=` list that repairs it.
- **The flag sets one posture for the whole install, including DDL capture.** Since v0.135 the opt-in also sets both DDL event triggers `ENABLE ALWAYS`. PostgreSQL filters event triggers by their enablement exactly as it filters row triggers, so through v0.134.x the opt-in captured replica-role *DML* while replica-role *DDL* stayed invisible — an ETL session running `SET session_replication_role='replica'; ALTER TABLE t DROP COLUMN c; INSERT INTO t …` had the INSERT captured in its post-DDL shape and the ALTER not, and the applier wrote that row into an unchanged target with no refusal. **If you have an existing `--capture-replicated-writes` install, its next stream open will refuse** naming `sluice_capture_ddl_trg`: re-run `sluice trigger setup --capture-replicated-writes` (seconds; the change log, watermark and consumer registry are preserved). Default-posture installs are unaffected.
- **The enablement posture is recorded and verified.** Setup records the opt-in in `sluice_change_log_meta`, and every stream open checks the installed triggers — the per-table pair *and* both event triggers — against it: an opt-in install whose trigger was hand-flipped back to plain refuses (its replicated writes, or replica-role DDL, would be silently missed), and a default install whose trigger was hand-flipped to `ENABLE ALWAYS` refuses too (replica-role capture without the echo-loop vetting). Re-running `sluice trigger setup` with the matching flag repairs either — **name every captured table in `--tables`**, which is what the refusal message prints for you: the posture is recorded once for the whole install, so a re-run that names fewer tables cannot converge it.

Separately, every `postgres-trigger` stream open now verifies the installed capture artifacts themselves (the same capture-shape door the `sqlite-trigger`/`d1-trigger` engines got in v0.131.2): a manually dropped or `DISABLE TRIGGER`-d capture trigger, a trigger set `ENABLE REPLICA`, a trigger rewired to a foreign function, or a dropped/disabled `sluice_capture_ddl_trg` / `sluice_capture_drop_trg` event trigger **refuses loudly at open** — re-running `sluice trigger setup` reinstalls everything and preserves the change-log and resume watermark. Without the door, a dropped trigger is invisible to both drift tiers (the DDL event triggers only watch table DDL and drops of captured tables; the `--allow-polled-fingerprint` tier watches nothing — its fingerprint loop is not yet implemented, and every stream open on that tier logs a `DDL-DETECTION-ABSENT` warning) and every subsequent change on that table would be silently uncaptured.

### Dropping a synced table (and the `DROP-CAPTURE-ABSENT` warning)

Through v0.134.x, `DROP TABLE` on a synced source table was **invisible to capture**. The event trigger's tag list named `DROP TABLE`, but PostgreSQL reports dropped objects only through `pg_event_trigger_dropped_objects()` (a `sql_drop` event trigger) — never through the `pg_event_trigger_ddl_commands()` the DDL capture function reads — so the function found nothing to record. The stream kept running at exit 0 and the target kept the dropped table's last-synced rows forever.

From v0.135, `sluice trigger setup` installs a second event trigger (`sluice_capture_drop_trg`) on `sql_drop`. Dropping a table this install captures — directly, or via `DROP SCHEMA … CASCADE` / `DROP OWNED BY` — now refuses the stream at the next poll, naming the relation and telling you what to do about it. Two deliberate non-events: `DROP INDEX` on a synced table records nothing (sluice never forwards index DDL, so an index drop cannot change any row the applier writes), and a `DROP TABLE` elsewhere in the database records nothing at all — the capture is scoped to tables carrying this install's own `sluice_capture` trigger.

Note that the remedy is **not** `sluice migrate`: that reads the *source* schema, so it cannot land a drop. Drain the stream, decide the target's fate for that table (drop it to match the source, or keep it as a deliberate archive), re-run `sluice trigger setup` with the tables that still exist, remove the dropped table from the sync's `--tables`, then `sluice sync start --restart-from-scratch`.

**Existing installs need one re-run.** Upgrading the binary does not add an event trigger to a database. Every stream open on an install that predates the arm logs a `DROP-CAPTURE-ABSENT` warning naming the missing trigger; `sluice trigger setup --dsn=... --tables=...` installs it and preserves the change log, the resume watermark and the consumer registry. (This is the same re-run the security note below asks for, so one setup pass covers both.)

### Security: re-run `trigger setup` after upgrading past v0.134.0

Releases v0.85.0 through v0.134.0 installed the DDL capture function (`sluice_capture_ddl`) as `SECURITY DEFINER` **without** a pinned `search_path`. `CREATE EVENT TRIGGER` requires superuser, so that function is owned by one, and an unpinned definer resolves the built-ins it calls against the search_path of whichever session fires it — which is any user's DDL session. An unprivileged user who can create a function in a schema their search_path reaches (the default `PUBLIC` grant on `public` before PG 15, or any schema they own) could shadow one of those built-ins and have it execute with superuser privileges by running a single `CREATE TABLE`. This was reproduced end to end on PG 16.

The renderer is fixed, but **upgrading the sluice binary does not repair a database that already has the function installed** — only re-running setup does, because `CREATE OR REPLACE FUNCTION` is what rewrites the function's settings:

```
sluice trigger setup --dsn=<source-dsn> --tables=<same tables as before>
```

Add `--capture-replicated-writes` if that is your recorded posture. The re-run preserves the change log, its resume watermark and the consumer registry, so a running sync resumes where it left off; do it at your next convenient window. Until then every `postgres-trigger` stream open logs an `INSECURE-CAPTURE-FUNCTION` warning naming the affected function and this remedy. It is a warning rather than a refusal deliberately: the same check runs on every warm resume, so refusing would turn a binary upgrade into an outage on every running sync. Sources on the `--allow-polled-fingerprint` tier never had the DDL capture function installed and are not affected.

**The same re-run is also what clears a `STALE-CAPTURE-FUNCTION` warning.** From v0.137 every stream open compares the capture functions the source actually has — their body, their `SET` pins and their `SECURITY DEFINER` flag, read from `pg_proc` — against the definitions this binary renders. An install made by an older sluice keeps capturing through its OLD function body until setup re-runs, and that is not cosmetic: before the `bytea_output` pin every captured bytea is corrupted on the way to a MySQL or SQLite target, and before the `extra_float_digits` pin every captured float is silently rounded when the writing application's session setting is lower. The warning names the functions and this remedy; it is a warning, not a refusal, for the same reason as the one above. Two shapes DO refuse, because no sluice version could have produced them: a capture function whose body no longer writes into the change log at all (it would capture nothing while every trigger still looks correct), and — on installs created from v0.137 onward, which record what setup installed — a definition that was changed after setup installed it. If you edit a `--dry-run` plan's function bodies by hand, expect that second refusal; re-running setup restores the shipped definition.

The same re-run also repairs a second defect found in the same audit, and it is worth knowing what it was. Releases v0.133.1 through v0.134.1 suppressed DDL capture for sluice's own `trigger setup` statements by setting a session variable, `sluice.setup_in_progress`, that the capture function checked. PostgreSQL puts **no privilege** on setting a variable with a dotted name, so any session on the source could set it and run schema changes that were never recorded — and because the trigger engine's `op='X'` markers are its only DDL-detection tier, nothing downstream noticed. From this release the suppression additionally requires evidence a session cannot fabricate (a one-shot record written into `sluice_change_log_meta`, a table setup grants to nobody, keyed to the running backend), so setting the variable by hand no longer suppresses anything.

**If you apply the `--dry-run` plan by hand, apply it whole, in one psql session.** It now begins with `BEGIN` and ends with `COMMIT`, and it uses `SET LOCAL`, so PostgreSQL reverts the suppression marker at both commit and rollback — which is the point: previously the marker was cleared by a `RESET` at the very end, and an operator whose paste stopped on an error (say, under `\set ON_ERROR_STOP on`) kept the suppression for the rest of that session, silently swallowing the `op='X'` marker for their own next `ALTER TABLE`. Stopping partway is now safe: roll back (or just disconnect) and the marker goes with it. Applying only part of the plan still leaves the engine partly installed, so re-run `sluice trigger setup` to converge either way.

## MySQL-family sources: resume signals (`POSITION-MODE`, `UNVERIFIED-INSTANCE-IDENTITY`)

A warm resume — `sync start` with an existing `--stream-id`, `backup incremental`, chain replay, the backup→CDC handoff — asks the source two questions before it opens the stream: *has the source purged anything this position still needs?* (the retention check; a "no" is the position-invalid refusal that takes the auto-resnapshot route, or a hard stop under `--no-auto-resnapshot`) and, since v0.137.2/v0.138.0, *did this source ever produce this position at all?* (the identity/lineage check; the same refusal and the same route). The second question exists because a replaced, restored, reset or rebuilt instance answers the first one with "nothing purged" and then streams its own unrelated history as the continuation, at exit 0. How it is asked depends on the position's mode, and two grep-stable log markers tell you which check a resume is running without.

- **`POSITION-MODE`** (INFO, once per CDC open, vanilla `mysql` binlog sources only — MariaDB is always in GTID mode and the `planetscale`/`vitess` flavors resume on VStream positions, so neither has a choice to advise about). Logged when the source has `gtid_mode=OFF`, MySQL 8's shipped default: sluice then resumes from a binlog **file and offset**. That is supported and correct, and it is the weaker of the two resume modes — binlog filenames are instance-local, so if the source is ever replaced, rebuilt or failed over onto a different server, the position's meaning does not carry over. sluice stamps the source's `@@server_uuid` onto such positions and refuses a mismatch, whereas a GTID set is instance-bound by construction and needs no stamp. It is an INFO line rather than a WARN because nothing is wrong with the configuration; it exists so an operator can *know* which arm they are on. Enabling GTID mode on the source is the stronger configuration if you have the choice (PlanetScale imports require it anyway).
- **`UNVERIFIED-INSTANCE-IDENTITY`** (WARN). The resume is proceeding **without** the identity/lineage check, so a replaced or rebuilt source would not be caught on this resume. It has four sources, and each names its own remedy:
  - a MySQL file/pos position with no `server_uuid` — captured before v0.137.2 stamped it, or its capture could not read the value. The resume falls back to the filename check alone. One fresh full backup (or cold start) moves the chain onto the identity check; that population cannot grow, since every capture door now stamps.
  - a MySQL file/pos position that *has* a `server_uuid`, but the source's `@@server_uuid` could not be read on this resume. That is a probe failure on a refusal-gating check, allowed through so a transient read does not force a full re-snapshot; if it persists, treat it as a real finding.
  - a MariaDB position with no lineage anchor (persisted by v0.137.4 or earlier). MariaDB GTIDs carry no instance identity, so a rebuilt source whose history reads the same GTIDs would not be caught; the domain check (every domain in the resume set must appear in `@@gtid_binlog_state`) still runs. One fresh full backup or cold start moves the chain onto the anchor check.
  - a PlanetScale/Vitess resume whose per-shard lineage probe (`GTID_SUBSET(<resume>, @@global.gtid_executed)`) failed. The pre-flight proceeds to the retention check; a replaced keyspace would not be caught on this resume, and a persistent failure is a real finding.

What refuses, with no marker, and takes the position-invalid route: a MySQL file/pos `@@server_uuid` mismatch; a MySQL GTID resume set not contained in the source's `@@global.gtid_executed` — the message says whether the source is merely *behind* the position (every source UUID present, sequence numbers lower: a lagging replica behind a load-balanced or failover endpoint) or a different lineage (a UUID the source never executed); a MariaDB anchor that answers a different GTID state, or NULL with no retained file above it that could have purged it on the same lineage; a VStream shard that never executed a source UUID the position names (a shard that is merely behind is replica lag, logged at INFO, and left to vtgate's tablet picker). Lineage is checked before retention on every arm, so a foreign source that has also purged binlogs is diagnosed as foreign rather than with a recovery hint that does not apply. Postgres is out of scope here — it has its own source-identity pin ([ADR-0051](../adr/adr-0051-pg-cdc-source-identity-pinning.md)).

## MySQL binlog sources: writes that never enter the binlog

sluice's MySQL/MariaDB CDC **is** the binlog — a write the server never logs cannot reach the target, and nothing errors, because there is no event to mis-handle. Most members of this class are refused loudly at CDC start: `binlog_format` ≠ ROW and partial row images (`SLUICE-E-CDC-BINLOG-FORMAT-NOT-ROW`, `SLUICE-E-CDC-ROW-IMAGE-PARTIAL`), a source that is itself a replica with `log_replica_updates=OFF` (`SLUICE-E-CDC-REPLICA-NO-LOG-UPDATES`), server-side `--binlog-ignore-db`/`--binlog-do-db` filters covering a synced database (`SLUICE-E-CDC-BINLOG-DB-FILTERED`), and a session-level `binlog_format=STATEMENT` override's DML is stopped mid-stream by a dispatch belt (`SLUICE-E-CDC-STATEMENT-DML`). Two shapes remain that sluice **cannot detect at all**, and they are documented here rather than implied covered:

- **`SET SESSION sql_log_bin=0`.** A session holding `SUPER`/`SYSTEM_VARIABLES_ADMIN` can opt its own writes out of the binlog entirely — DBA hotfixes, ops scripts, `mysqldump --set-gtid-purged` restores. Ground-truthed on real `mysql:8.0.46` (2026-08-26): the rows are SQL-visible on the source, the binlog position does not move, **no GTID is allocated**, and afterwards there is zero server-side trace — no event, no counter, no position delta — so no preflight, belt, or resume check can ever see it. Writes that landed before the cold-copy snapshot are carried (they are SQL-visible); anything written this way during the live tail is silently absent from the target. The only *point-in-time* probe is a currently-connected session's variable (`SELECT t.PROCESSLIST_ID, v.VARIABLE_VALUE FROM performance_schema.variables_by_thread v JOIN performance_schema.threads t USING (THREAD_ID) WHERE v.VARIABLE_NAME='sql_log_bin'`), which cannot see past or future sessions. That lookup works on **MySQL only** (where it requires `performance_schema=ON`, the default). On **MariaDB the query hard-errors regardless of the setting** — `variables_by_thread` is not in MariaDB's performance_schema table set at all (verified live on 11.4: error 1146 with the setting ON and OFF alike), so enabling `performance_schema` cannot fix it; on MariaDB find the writer by interrogating candidate sessions from `SHOW PROCESSLIST` (the same precondition applies to `SLUICE-E-CDC-STATEMENT-DML`'s session-hunt remedy, which uses the same tables). **`sluice verify` is the only independent after-the-fact catch** — it compares real rows, not binlog state. If your operational practice includes `sql_log_bin=0` sessions on the source, schedule `verify` runs and treat the source's primary as the write path during a sync.
- **Vitess/PlanetScale: tablet-side writes with binlogs disabled.** On **self-hosted** Vitess the same class is reachable via `vtctldclient ExecuteFetchAsDBA --disable-binlogs` (the flag wraps the statement in `SET sql_log_bin = OFF` on the tablet) or any direct-mysqld session. Reproduced on a real Vitess v24 cluster (2026-08-26): the row lands on the primary, VStream — which re-serves the tablet's binlog — never carries it, and the stream stays healthy around the hole. Worse, MySQL replication inside the shard rides the same binlog, so the hidden write also splits primary/replica state — a later cold copy or re-snapshot from a REPLICA tablet (sluice's default) misses the row even in SQL, and `verify` must read `@primary` to see it on the source at all. PlanetScale customers have neither `vtctld` nor mysqld access, so on the managed flavor this class is unreachable by customers. As with `sql_log_bin=0`, there is no after-the-fact server-side trace; `sluice verify` (against the primary) is the independent catch.

One further member of this class **is** detected, because the evidence lives in the schema rather than in a session: **foreign keys with referential actions** (`ON DELETE CASCADE`, `ON UPDATE CASCADE`, `ON DELETE SET NULL`, `SET DEFAULT`). When a parent-key `DELETE`/`UPDATE` cascades, MySQL writes only the **parent table's** events to the binlog and expects the replica to re-run the cascade locally through its own FK definitions — and sluice's applier deliberately suppresses exactly that re-cascade (target-side FK enforcement is disabled so out-of-order apply cannot poison-pill, the Bug 164 fix), so the target's child rows silently survive or keep stale FK values while the sync exits 0. This applies to **both** MySQL lanes (binlog and VStream — the vstreamer re-serves the tablet's binlog, where the cascaded rows are equally absent) and to every target except an unprivileged Postgres, where enforcement stays on and the translated FK re-runs the cascade itself. Because the FK declarations are visible in `information_schema`, every CDC open censuses the in-scope tables and logs a `FK-REFERENTIAL-ACTION-CAPTURE-GAP` warning naming each affected constraint. Plain FKs and `RESTRICT`/`NO ACTION` actions cause no invisible writes and never trigger it. If the warning names your schema: run `sluice verify` after parent-key deletes/updates, or replace the referential actions with application-level deletes for the sync's duration. Re-enabling target-side FK enforcement is **not** the remedy — it re-opens the ordering failures the suppression exists to prevent.

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
