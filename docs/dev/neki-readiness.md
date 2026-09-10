# Neki (sharded PlanetScale Postgres) — readiness matrix

**Status:** investigation, 2026-09-10. Nothing here is implemented. The Tier-B probes have now been **measured against a live cluster** — see "First probe run" below; the rest are still hypotheses, and the whole point of this file is to keep the two apart.

## First probe run — 2026-09-10, live `neki-test` (PS-10, AWS us-east-1, 2 replicas)

Measured with `scripts/neki-probe.sh` plus follow-ups, against `PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2) (Neki)`. The database was empty and unsharded (one shard group, uid `shygpz21uygor7`).

**The headline: sluice's entire snapshot→CDC mechanism works — PER SHARD.**

| # | Measured | Raw |
|---|---|---|
| **R-1** | The router **speaks the replication sub-protocol but refuses a cluster-wide replication connection**, naming the fix | `FATAL: replication connections must target a specific shard` / `HINT: Set the target shard in the connection string, e.g. options=-c __neki.shard=<shard-uid>` |
| **R-1b** | With `options=-c __neki.shard=<uid>`, `IDENTIFY_SYSTEM` **succeeds** | `7683915551569174560\|1\|0/A0015A0\|postgres` |
| **R-3** | `CREATE_REPLICATION_SLOT … TEMPORARY LOGICAL pgoutput EXPORT_SNAPSHOT` **succeeds on a shard connection** | `sluice_probe_tmp\|0/A0015D8\|0000003F-00000002-1\|pgoutput` |
| R-4 | `pg_replication_slots` is readable; it shows Neki's own **physical** slots | `neki_shygpz21uygor7_aws_useast1a_4_…\|\|physical\|\|t` (×2, one per replica) |
| R-4b | Replication settings are CDC-capable | `wal_level=logical`, `max_replication_slots=1000`, `max_wal_senders=20` |
| B-2 | `__neki.fanout` default is **`scatter`** — sluice's full-table scans are not rejected out of the box | `scatter` |
| B-3 | `__neki.tx_mode` default is **`multi`** — cross-shard transactions are permitted, and therefore non-atomic by default | `multi` |
| A-3 | `COPY (SELECT …) TO` **rejected**, confirming the docs | `ERROR: not implemented: COPY (SELECT …) TO is not supported` |
| A-7 | `EXCEPT` **rejected** | `ERROR: not implemented: only UNION is supported` |
| A-8 | `pg_is_in_recovery()` returns `f` on a normal connection | `f` |
| R-8 | Topology **is** exposed, as a `__neki` schema — but `data_topology_log.data_topology` is a **protobuf blob**, not queryable rows | `\x12220a0e73687967707a…` (the shard uid `shygpz21uygor7` is legible inside it) |
| R-8b | The `__neki` schema exposes **45 tables** through `information_schema` | `data_topology_log`, `dtxn_state`, `onlineddl_state`, `repl*`, `heartbeat`, `blocked_tables`, `cutover_phase_audit`, `*_snapshot`, `distributed_lock`, … |

**What this changes.** R-1 was the probe the whole file hinged on, and the answer is better than either branch anticipated: not "yes, unchanged" and not "no, impossible", but **"yes, once per shard."** So sluice's PG CDC reader can attach essentially as it stands — and its *position model* cannot. One stream would need N replication connections, N slots, N `consistent_point`s and N LSNs, with N exported snapshots that (per PlanetScale's own docs) **do not share a snapshot**. The copy consistency question in A-1 is therefore not merely theoretical: it is the same question the position model has to answer.

Note also that v0.149.0's brand-new source-identity pin behaves *correctly* here by accident of good design: a per-shard connection returns that shard's own `systemid`, so the identity check is meaningful per shard. But the resume's single recorded anchor is not, which is A-1 again from a third direction.

### Running sluice itself against it — first end-to-end attempt

Built from the v0.149.0 tree, `migrate` from a local `postgres:16` into Neki. `--dry-run` planned cleanly (source read, target reachable, raw-copy passthrough lane eligible). The real run died before moving a single row:

```
pipeline: write initial migrate-state: postgres: write migrate-state:
ERROR: cannot add new column to INSERT RETURNING (SQLSTATE XX000)
```

**N-1 (blocker, loud): a Neki router rejects any `INSERT` whose `VALUES` list contains MORE THAN ONE VOLATILE FUNCTION CALL.**

The first bisect got this wrong and the second caught it, which is worth showing rather than tidying away. The initial run pointed at `ON CONFLICT … DO UPDATE SET`, because the probe that failed happened to introduce the *second* volatile call via the SET clause. Re-testing with the count as the variable rather than the clause:

| Shape | Result |
|---|---|
| plain `INSERT`, `INSERT … RETURNING`, `ON CONFLICT DO NOTHING`, `ON CONFLICT (id) DO UPDATE`, multi-row `DO UPDATE` | all **work** |
| **one** `now()` in `VALUES`, with or without `ON CONFLICT` | **works** |
| one `now()` + a plain timestamp literal, with `ON CONFLICT` | **works** |
| **two** `now()` in `VALUES`, **with** `ON CONFLICT` | **`ERROR: cannot add new column to INSERT RETURNING`** |
| **two** `now()` in `VALUES`, **no `ON CONFLICT` at all** | **same error** — which is what disproves the first hypothesis |
| two identical timestamp **literals** | **works** — so it is volatility, not repetition |
| one bound parameter referenced twice (`VALUES ($1,$2,$3,$3)`) | **works** — this is the fix shape |
| CTE to evaluate `now()` once, then `INSERT … SELECT … ON CONFLICT` | **`ERROR: not implemented: INSERT … SELECT … ON CONFLICT DO UPDATE is not yet supported`** — a *separate* limitation, recorded as N-1b |

So the idempotent-apply machinery sluice depends on everywhere — `ON CONFLICT … DO UPDATE`, single- and multi-row — is **fine**, and that is the important half. What fails is narrow and specific: two clock reads in one `VALUES` list.

**Affected statements, enumerated rather than promised.** `internal/engines/postgres/migration_state.go` has three upserts, and each writes `started_at` and `updated_at` from two separate `pg_catalog.timezone('utc', pg_catalog.now())` calls: `UpsertHeader:111`, `UpsertSnapshotAnchor:124`, `UpsertProgressRow:130`. All three fail on Neki. (`UpsertProgressRow` writes only one timestamp and may survive — it needs its own check rather than being assumed in or out.)

**The fix is a design choice, not a one-liner, and it is deliberately not made here.** The comment above `UpsertHeader` explains why the clock is read *server-side*: a client-supplied value was rendered against the session `TimeZone` and the stored digits went hours off, which broke the backfill heartbeat guard's 5-minute freshness window. Any fix has to preserve "naive UTC digits" while making one call instead of two. Candidates:

1. **Bind one Go-computed UTC timestamp and reference the placeholder twice.** Measured working. Moves the clock from server to client, which the existing comment is wary of — though its actual concern (session-TZ rendering) is satisfied by `time.Now().UTC()`.
2. **Give `started_at` a column `DEFAULT timezone('utc', now())` and omit it from the INSERT.** Keeps the server clock and leaves one call in the statement. Costs a control-table DDL change, so it is a compatibility question for existing installs.
3. Leave it, and let the Neki flavor supply its own SQL — which is what a flavor is for.

Not changed here regardless: v0.149.0's tag was already cut when this was found.

**N-1b: `INSERT … SELECT … ON CONFLICT DO UPDATE` is not supported.** Found while testing a CTE workaround for N-1. Worth checking whether any sluice write path renders that shape — the idempotent bulk-copy writer is the obvious candidate.

Note the shape: sluice died on **its own control-table SQL at phase 1.75**, before any user data, with a router error that names nothing an operator could map back to sluice. That is Bug 280 exactly — the MariaDB target that died on the migrate-state row-alias upsert before the flavor steer could fire — on a different engine, one release later.

**N-2 (design-level, from a NOTICE nobody asked for): DDL is EVENTUALLY CONSISTENT across routers.** Every `CREATE TABLE` / `ALTER TABLE` returned:

```
NOTICE: DDL is committed and the change is visible on this router; other routers may not see it yet.
        To wait until it is visible on every router, use __neki.wait_for_ddl(9, 0)
```

sluice's whole migrate shape is *create the tables, then copy into them*, across a connection **pool** — and a pool can land on different routers. A `CREATE TABLE` acknowledged on router A followed by a `COPY` dispatched to router B is a `relation "x" does not exist` mid-copy, which is precisely the loud-but-baffling failure class. Any Neki flavor must call `__neki.wait_for_ddl(...)` at the end of each DDL phase — schema apply, index build, constraint add — before the next phase opens a connection. This is not in the docs pages read so far; it surfaced only as a NOTICE on a live statement, which is its own argument for probing rather than reading.

**N-3 (safe by construction, but worth naming): the `__neki` schema is visible to `information_schema`, and sluice does not trip over it** — 45 internal tables (`dtxn_state`, `onlineddl_state`, `repl*`, `heartbeat`, `data_topology_log`, the `*_snapshot` catalog mirrors). sluice's PG reader is namespace-scoped (`WHERE table_schema = $1`, `schema_reader.go:182/481/520`), so it never enumerates them unless someone passes `--schemas __neki`. That is a real premise the flavor should not quietly rely on: a `--schemas`/`--databases` run, or any future all-schema enumeration, would try to migrate Neki's own control plane.

**N-4 (connection): the minted DSN carries `sslmode=verify-full`.** `pscale role reset-default` returns a `database_url` at `verify-full`, which fails for any client without the CA (`root certificate file … does not exist`). PlanetScale Postgres wants `sslmode=require` — the same note the existing PS-Postgres recipe carries. Worth a preflight hint rather than letting operators meet a TLS error.

**N-5 (flavor detection): `version()` returns `PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2) (Neki)`.** A `(Neki)` suffix on the version string is a clean detection signal, exactly analogous to how the MySQL engine detects MariaDB.

### SHARDED, measured — `neki-torture`, two shards, `xxhash(tenant_id)`

A second Neki database was created (via the API — the CLI rejects `--engine neki`, NEKI-003), a second shard added (`POST …/branches/main/shards`), and a two-shard topology applied by `PUT`ting a data-topology JSON with an `xxhash` shard index on `tenant_id`. `EXPLAIN (NEKI_PLAN)` confirms real routing: `Route [EqualUnique]` on a shard-key predicate, `Aggregate [Ordered] → Collapse → Route [Scatter]` without one. 100 rows across 20 tenants.

| # | Question | Measured |
|---|---|---|
| **A-5** | Does an unordered cross-shard read return PK order? | **No** — `SELECT … LIMIT 8` with no `ORDER BY` returned one row per tenant (`1.1 2.1 3.1 4.1 …`), not `1.1 1.2 1.3`. Exactly as the docs warn. |
| **A-5** | Is an explicit `ORDER BY` honoured globally across shards? | **Yes** — correct global order. |
| **A-5** | **Does keyset pagination work across shards?** | **Yes — 100 rows, 100 distinct.** Nothing skipped, nothing duplicated, over 10-row pages with `WHERE (tenant_id,id) > (…) ORDER BY tenant_id,id`. |
| — | Is sluice safe here by construction? | **Yes**, and verified rather than assumed: the PG batched reader emits `ORDER BY pk1, pk2` (`row_reader_batch.go`), which is precisely the form measured correct above. |

**N-2 reproduced as a real failure, not just a NOTICE.** A `CREATE TABLE` acknowledged on one router, followed immediately by an `INSERT` on a new connection, failed with `relation "sharded_events" does not exist` from a *different* router cell — the exact create-then-write sequence sluice performs across a connection pool. `SELECT __neki.wait_for_ddl(<seq>, 0)` on a fresh connection fixes it, and after that the write succeeds. That confirms both the hazard and its remedy.

### Neki as a SOURCE — works cross-engine, BLOCKED same-engine

| direction | lane | result |
|---|---|---|
| Neki → **MySQL** (cross-engine) | IR copy path | **100/100 rows, clean** |
| Neki → **PostgreSQL** (same-engine) | ADR-0078 raw-copy passthrough | **FAILS loudly**: `ExportRawCopy: COPY TO STDOUT: ERROR: not implemented: COPY (SELECT …) TO is not supported` |

**N-7 (sluice, design): the raw-copy passthrough lane is unconditional for PG→PG and has no off switch.** `--raw-copy-format` selects `text|binary` only; `ir.RawCopyFormat` has no disabled value. So a Neki source cannot be migrated to a PostgreSQL target at all today, while the same source to a MySQL target works — the fast lane is the only thing in the way. **This is the flavor's first concrete job**: gate `asRawCopyEndpoints` on a capability the Neki flavor declines, so the lane is skipped and the IR path (plain `SELECT`) is used, exactly as it already is cross-engine. An operator-facing `--raw-copy-format=off` would be a cruder second-best.

**Neki as a CDC SOURCE: blocked, loudly, and the reason is precise.** The replication protocol itself is reachable per shard — `IDENTIFY_SYSTEM` and `CREATE_REPLICATION_SLOT … LOGICAL pgoutput EXPORT_SNAPSHOT` both succeed with `options=-c __neki.shard=<uid>`, returning a consistent point *and* a snapshot name. What cannot be done is USE that name: `SET TRANSACTION SNAPSHOT` is not implemented, and neither is `pg_export_snapshot()`. So the export half of the shared-snapshot mechanism works and the import half does not, which makes the exported name decorative (NEKI-007).

sluice's PG cold start opens a snapshot stream and has its readers `SET TRANSACTION SNAPSHOT` onto it, so this is a hard stop — and sluice reaches it correctly: two explicit WARNs (the replication-headroom census and the prepared-xact probe each degrade, naming what could not be checked) and then a loud refusal. Nothing silent.

A second constraint found in the same run: a shard-pinned session cannot call router-managed functions (`current_setting`, `set_config`, `nextval`, SQLSTATE `0A000`). That is what made both preflights degrade, and it compounds — the shard-pinned connection is the only one that accepts replication and also the only one that cannot read a GUC.

**Sharded WRITES: the hazard is real and it is sluice-shaped (NEKI-006).** Assigning an already-populated table to a shard group without running a data-movement workflow leaves routing and placement disagreeing, silently: scatter reads return all 100 rows while every equality-routed read returns **0**. Worse, `INSERT … ON CONFLICT (pk) DO UPDATE` — the exact statement sluice's CDC applier and idempotent bulk-copy writer use — **created a duplicate primary key**, because the conflict check runs on the routed shard, which does not hold the original row. Two rows now claim PK `(1,1)`.

We induced that state and may have skipped a step; no Reshard/MoveTables endpoint is exposed to check (`/workflows` on a Neki branch 404s; the database-level one answers "only available for Vitess databases"). But the failure mode — no error at topology-update time, no error at query time, and a breakable PRIMARY KEY — is worth a preflight on sluice's side regardless: **comparing a keyed count against a scattering count for one sampled key would detect it cheaply**, and a target in that state should be refused before CDC apply rather than quietly accumulating duplicates. Filed as a gate proposal rather than built.

**N-6: `pg_export_snapshot()` is not implemented** (`SQLSTATE NK013`), which is the cross-shard "no shared snapshot" fact (A-1) showing up at sluice's own door. **sluice handles it correctly** — it WARNs that the shared source snapshot is unavailable, that readers may observe different mid-copy states, and to quiesce the source or read a primary for a fully consistent copy, then falls back to independent per-connection readers. Loud, accurate, and with the right remedy; no change needed beyond the flavor recording it as expected rather than exceptional.

### Cross-engine: PlanetScale MySQL → Neki, 12-table torture schema — MIGRATED CLEAN

With N-1 and N-5 fixed locally, `sluice migrate` completed a full cross-engine migration: 12 tables, 45 rows, every phase (tables → bulk_copy → indexes → identity_sync → constraints → views). Source was a PlanetScale MySQL database seeded from `sluice_torture_mysql.sql` plus a boundary-value payload.

**sluice's own refusals fired correctly on the way**, which is the torture schema doing its job rather than a defect:

- `SLUICE-E-VALUE-TINYINT1-RANGE` on a `TINYINT(1)` holding `2` — carrying it as a boolean would have collapsed it to `true` and lost the integer. Refused before any row was written, naming the remedy.
- WARNed that `bigint unsigned` maps to PG `bigint` and values above 2⁶³−1 are unrepresentable, naming `--type-override … = decimal(20,0)`. With the override, `18446744073709551615` landed exactly.
- WARNed that MySQL `TIME` is a DURATION (−838:59:59..838:59:59) while PG `time` is a time-of-day, and that out-of-range values would refuse rather than clamp. `--type-override … = interval` carried them.

**Value fidelity across the engine boundary, verified:** integer extremes exact at every width; `decimal(65,0)` and `decimal(65,30)` exact at full 65-digit precision; `octet_length` identical for every string including a 4-byte-emoji row (byte-identical hex) and the trailing-space PAD SPACE row; `float4`/`float8` exact.

#### A retraction, kept because the mistake is the lesson

Mid-run this file briefly recorded a **CRITICAL silent-loss finding**: that every single-precision FLOAT migrated MySQL→PostgreSQL was being rounded to 6 significant digits (`8388608` → `8388610`, float32 max → `3.40282e38`). It reproduced on vanilla MySQL 8.0 → vanilla PostgreSQL 16 with Neki nowhere in the picture, which made it look like a core-path defect in sluice's flagship direction.

**It was not a defect. The measurement instrument was the lossy thing.** The oracle was `c_float::numeric(50,0)`, and PostgreSQL's `real → numeric` cast goes through a rounded text form. The control settles it — a `real` column holding the *correct* `8388608`:

```
SELECT v::numeric(50,0) FROM ctl;   -- 8388610      <-- the CAST rounds
SELECT v::float8::text  FROM ctl;   -- 8388608      <-- the stored value is exact
```

Rendered through `::float8`, every migrated value matches its source exactly, on both the local PG target and Neki. Instrumenting the reader had already shown sluice reading `3.4028234663852886e+38` and `8388608` exactly, and the ADR-0153 `(col * 1E0)` projection in the emitted SQL — two pieces of evidence that contradicted the "finding" and should have stopped it sooner.

This is the project's own rule turned on its author: **name the independent expected value, and check that your oracle is not the thing under test.** A verification whose "loss" derives from a lossy renderer is the same defect class as a verification whose "all clear" derives from the artifact it verifies. Worth noting too that the earlier `pg_dump | psql` Tier-C pass got the right answer precisely because it compared like with like.

### Tier C, first pass — value fidelity through the router: CLEAN

A 26-column fixture covering every family the Bug 74 rule names — native (`integer`/`bigint`/`boolean`/`real`/`double precision`/`numeric(20,8)`), string-leaf (`text`/`varchar`/`uuid`/`inet`/`cidr`/`macaddr`), temporal (`date`/`time`/`timestamp`/`timestamptz`), `bytea`, `json`, `jsonb`, and arrays × {1-D, **2-D**, NULL-element} — seeded on a local `postgres:16`, loaded into Neki, and compared column-by-column as `::text` with `array_dims` ground-truthed on both sides.

**Result: byte-identical. Both rows, all 26 columns, matching md5 (`e6c93b83…`).** Specifically exercised and clean:

- `real` at **8388608**, the exact value vtgate's float-to-text formatter mangles to `8388610`. No divergence here.
- `double precision` at `1.7976931348623157e308` and `2.2250738585072014e-308`.
- `numeric(20,8)` at `12345678901.23456789` and `-0.00000001`.
- `numeric[][]` and `integer[][]` with `array_dims` equal on both sides — Bug 74 itself was `numeric[][]` silently flattening to 1-D while `int[][]` was fine, so this is the discriminating cell, not a representative one.
- `text[]` containing a NULL element; empty array `{}`; a text value with U+2603, an embedded single quote and a tab; `bytea` compared as `hex()`.

**Stated because the gate is only as good as its scope:** this went through `pg_dump | psql`, i.e. the COPY **text** protocol, and therefore measures **the router**, not sluice's own pgx codec through the router. Those are different code paths and a divergence could live in either. It is genuine independent evidence about Neki — an independent reader, per the new-surface checklist — and it is **not** evidence that sluice's reader/writer round-trip cleanly here. That second pass needs N-1 fixed first, since sluice cannot currently get past its own control-table write.

### Two findings about the probe harness itself, recorded because they are the more general lesson

 The first run reported eleven confident verdicts — including "COPY (SELECT …) TO is rejected" and "no replication protocol through the router" — off a connection that had never been established (the minted DSN carries `sslmode=verify-full`; the client container has no root cert; PlanetScale Postgres wants `sslmode=require`). Every verdict derived from the same failure, which is exactly the evidence-sharing defect `CLAUDE.md` names. The harness now aborts when the baseline probe cannot reach the router. Then the *fix* misfired in the opposite direction: the "unreachable" classifier matched libpq's `connection to server … failed:` wrapper and swallowed R-1's `FATAL:` — suppressing the single most valuable finding in the run. A server that answers `FATAL:` has spoken to you. The classifier now checks that first.



**The decision this exists to make:** does sluice need a new `neki` **engine**, or a new **flavor** of the existing `postgres` engine? See [ADR-0186](../adr/adr-0186-neki-is-a-postgres-flavor.md) for the answer and its evidence; this file is the measurement plan behind it.

## What is established, from PlanetScale's own docs

Cited rather than paraphrased, because the whole risk here is reasoning from the name. Sources are `planetscale.com/docs/neki/*` as of 2026-09-10.

- **A router speaks the Postgres wire protocol in front of real Postgres.** "A router is the Neki service that accepts Postgres connections and decides which shard or shards should run each statement." Routers are stateless and horizontally scaled; sidecars sit beside each Postgres instance; a Replicator runs data-movement workflows; a Topology Service holds the map. Under it are "true Postgres clusters," so extensions and SQL compatibility are real rather than emulated.
- **Sharding is optional.** A Neki database starts unsharded. **This matters more than anything else in this file**: the unsharded case is very close to today's PlanetScale Postgres, and the sharded case is where every hazard lives. A capability set that keys on "is this Neki" and not on "is this table sharded" would be wrong in both directions.
- **No cross-shard snapshot and no atomic cross-shard commit.** "Once a transaction spans more than one shard, those shards do not share a snapshot or an atomic commit." And: "Neki commits each shard separately. It does not use two-phase commit." `SET __neki.tx_mode = 'single'` forces single-shard transactions.
- **No cross-shard row order without `ORDER BY`.** "A query without `ORDER BY` has no cross-shard row-order guarantee. Add an explicit order when the application depends on result sequence."
- **Fan-out is a governed setting.** `__neki.fanout` ∈ {`single`, `multi`, `scatter`}; a statement exceeding it fails loudly — "statement's fan-out (scatter) exceeds `__neki.fanout` (single)." A full-table `SELECT` with no shard-key predicate is a `Route [Scatter]`.
- **COPY is heavily restricted** (platform preview): COPY "must use the simple query protocol and must be the only statement"; COPY over the extended protocol is rejected; `COPY TO` is **unsharded tables only**; `COPY (SELECT ...) TO` is **rejected**; `COPY FROM` with a `WHERE` clause is rejected; `COPY FROM PROGRAM` / `COPY TO PROGRAM` rejected.
- **Other preview rejections:** `SELECT ... INTO`; `INTERSECT` / `EXCEPT` on application tables (`UNION` is fine); `COMMIT AND CHAIN` / `ROLLBACK AND CHAIN`; cross-database object references; reading an inheritance parent without `ONLY` when children exist; a `search_path` containing `pg_temp`; tablespaces; large objects; procedural languages with custom handlers; `LOAD`.
- **Replicas route by startup option**, not by username suffix: `-c __neki.target=REPLICA`, and "Replica connections are read-only. Writes fail instead of going to a primary."
- **Neki's own import path is OFFLINE dump-and-restore, into an unsharded database only.** "The import is offline. The source application must stop writing before the dump and remain stopped until import validation and cutover are complete." Also: "pg_restore is not a topology-aware sharded import workflow," and "External Postgres connections for migration workflows not supported."

That last bullet is the product finding, and it is worth stating separately.

## Why this is an opportunity, not just a compatibility exercise

**Neki's documented migration path requires downtime and lands unsharded.** sluice's entire reason to exist is the other thing: an online copy with a gapless snapshot→CDC handoff, cut over when the lag is acceptable. If sluice can migrate *into* Neki online, that is a capability the platform does not document for itself.

It is also the harder half, and the honest framing is that we do not yet know whether it is possible — it depends almost entirely on probe **R-1** below. Read the rest of this file as scoping that question, not as assuming the answer.

## Tier A — established by the docs; encode as capability + refusal

These need no measurement to *plan*, only to confirm. Each becomes a `Capabilities` field, a preflight, or a refusal, and each refusal follows the tenet: **refuse loudly rather than produce a copy that exits 0 and is wrong.**

| # | Fact | What breaks in sluice today | Intended answer |
|---|---|---|---|
| A-1 | No shared snapshot across shards | The cold start opens one `SERIALIZABLE` snapshot and calls the copy a point-in-time. Across shards it is **not** — tables on different shards are read at different points, so a FK-related pair can be mutually inconsistent at exit 0 | A capability declaring "no cross-shard consistent snapshot," and a **refusal** when the in-scope table set spans shard groups without the operator opting in. This is the multi-database lane's own precedent: it records no snapshot anchor precisely because it cannot prove one |
| A-2 | No atomic cross-shard commit | The idempotent applier batches changes per transaction and assumes all-or-nothing. Across shards a partial apply is possible | Either force `__neki.tx_mode = 'single'` on apply connections and shard-partition each batch, or declare the applier non-atomic and make the position advance per-shard. **Do not** let a batch straddle shards silently |
| A-3 | `COPY (SELECT ...) TO` rejected; `COPY TO` unsharded-only | sluice's fast PG read path and its ADR-0119 chunked/parallel reads are built on `COPY (SELECT … WHERE pk BETWEEN …) TO` | A read fallback lane on plain `SELECT` for sharded tables — the same shape as the Vitess A0 client-side-COPY fallback, which exists for exactly this reason |
| A-4 | COPY must be simple-protocol and the only statement | pgx's `CopyFrom` on the write side, and any place sluice pipelines a COPY with another statement | Measure (W-1); likely fine for `CopyFrom`, but it must be confirmed rather than assumed |
| A-5 | No cross-shard row order without `ORDER BY` | **Silent-loss class.** Keyset pagination assumes rows arrive in PK order. Unordered scatter reads would mis-page: rows skipped, rows repeated, and the run exits 0 | Every paging read against a sharded table gets an explicit `ORDER BY` on the full keyset tuple. This is Bug 266 wearing new clothes — an ordering assumption true on one engine and false on another — and it gets a gate, not a comment |
| A-6 | `__neki.fanout` can reject a scatter | sluice's full-table scans *are* scatter queries. If an operator's default is `single`, every sluice read fails | A preflight that reads the effective `__neki.fanout` and says plainly what sluice needs, rather than letting the first read fail with a router error nobody can map back |
| A-7 | Large objects, tablespaces, custom PL handlers, `SELECT INTO`, `INTERSECT`/`EXCEPT`, `pg_temp` in `search_path` unsupported | Schema translation may emit some of these; `verify` may use `EXCEPT` | Enumerate sluice's emitters against this list. `EXCEPT` in particular: check whether any verify/diff path renders it |
| A-8 | Replica routing is `-c __neki.target=REPLICA`, read-only | sluice's PG standby refusal (`SLUICE-E-CDC-STANDBY-SOURCE`) keys on `pg_is_in_recovery()`. A Neki replica connection may not answer that the same way | Confirm the standby refusal still fires; a CDC stream pointed at a read-only replica route must refuse, not fail obscurely on the first write |

## Tier B — undocumented and load-bearing; MEASURE FIRST

The Neki docs do not mention client-facing logical replication, replication slots, publications, or CDC **anywhere**. That silence is the single largest unknown in this file and it gates everything downstream.

| # | Probe | Command | What each outcome decides |
|---|---|---|---|
| **R-1** | **Does the router accept a replication connection at all?** | Connect with `replication=database` in the startup packet, then `IDENTIFY_SYSTEM` | **This is the load-bearing probe.** Works → sluice's existing PG CDC lane may attach essentially unchanged on an unsharded Neki, and the flavor is a small delta. Refused → sluice has **no CDC path** against a Neki DSN and the flavor needs a different mechanism entirely (see R-6), which is the Vitess/VStream situation repeating |
| R-2 | What does `IDENTIFY_SYSTEM` return through a router? | `IDENTIFY_SYSTEM` | One `systemid` for the cluster, or one shard's? sluice v0.149.0 stamps `system_identifier` + timeline into the snapshot position and refuses a resume whose source identity changed. If the router returns a *per-shard* identity that varies by connection, that check fires on a healthy cluster — a false refusal on the release I shipped this morning |
| R-3 | Can a logical slot be created, and does it export a snapshot? | `CREATE_REPLICATION_SLOT s LOGICAL pgoutput EXPORT_SNAPSHOT` | The snapshot→CDC handoff's whole gaplessness argument is this call's `consistent_point` + exported snapshot. No export → no gapless handoff by today's mechanism |
| R-4 | Is `pg_replication_slots` readable, and does `confirmed_flush_lsn` mean what sluice thinks? | `SELECT * FROM pg_replication_slots` | v0.149.0's stopped-cold-start resume compares the recorded anchor against `confirmed_flush_lsn` in `pg_lsn` ordering. Per-shard slots make "the" LSN ill-defined |
| R-5 | Does `CREATE PUBLICATION … FOR ALL TABLES` work, and what is its blast radius? | `CREATE PUBLICATION` + `pg_publication_tables` | On plain PG this already breaks UPDATE/DELETE on replica-identity-less tables database-wide (audit A2-4b, measured on PG 16.15). On a sharded cluster the blast radius question is per-shard |
| R-6 | If R-1 fails: is the **sidecar** reachable for a per-shard replication connection? | Attempt a direct connection to a shard's Postgres endpoint | Decides whether a per-shard CDC lane is even buildable, or whether CDC into/out of Neki is off the table for now |
| R-7 | Do the `__neki.*` settings survive `SET`, and can they be set per-connection at startup? | `SET __neki.fanout`, `SHOW __neki.fanout`, and via `-c` | sluice would need to pin `tx_mode`/`fanout`/`target` per connection kind. If they can only be set at startup, that shapes the DSN handling |
| R-8 | What identifies a table as sharded, and can sluice read the topology? | Inspect catalogs / `__neki` introspection for shard-key metadata | Every Tier-A refusal keys on "is this table sharded." Without a readable answer, sluice must refuse conservatively for the whole database — a much worse product |
| W-1 | Does pgx's `CopyFrom` work against a sharded table? | Copy into a sharded table via the pgx `CopyFrom` path | It is the write core. If rejected, the writer needs a batched-INSERT lane for sharded targets |
| W-2 | Does a sharded `CREATE TABLE` require a shard key, and what happens without one? | `CREATE TABLE` with no shard key on a sharded database | This is the H-2 sharded-target door's analogue — MySQL refuses a vindex-less table on a sharded keyspace. Neki's answer decides whether sluice needs the same door |
| W-3 | Sequences / identity across shards | `CREATE TABLE … GENERATED BY DEFAULT AS IDENTITY`, insert on two shards | A cluster-wide sequence is a hard problem; if identity is per-shard, sluice's `SyncIdentitySequences` phase is wrong across shards |
| W-4 | Foreign keys across shards | Declare a cross-shard FK | Almost certainly unsupported. sluice's deferred-constraint phase must then refuse or skip, and say which |

## Tier C — value fidelity (the Bug 74 discipline)

A router that parses, plans and re-assembles results is a **new serialization boundary**, which is exactly the surface the new-surface checklist exists for. Vitess taught this the expensive way: vtgate's row streamer rendered single-precision `FLOAT` through mysqld's float-to-text formatter, so a stored `8388608` arrived as `8388610`, and sluice needs a whole exact-re-read phase to repair it.

| # | Probe | Why |
|---|---|---|
| V-1 | Full type-family matrix, src==dst byte-exact, through the router | native / string-leaf / temporal × {scalar, array 1-D, array ≥2-D, NULL-element}. Do **not** pin one representative — that is the Bug 74 lesson verbatim |
| V-2 | `float4` / `float8` round-trip at the precision boundary | The exact shape Vitess got wrong. Does the router ever render a value as text? |
| V-3 | `numeric` at extreme scale/precision | pgx's numeric codec is target-OID-dependent; the router adds a hop |
| V-4 | Arrays, especially multi-dimensional | Bug 74 itself: `numeric[][]` flattened to 1-D while `int[][]` was fine |
| V-5 | Extension types (PostGIS, `hstore`, `citext`, `pgvector`) | Docs claim real extension support; measure through the router |
| V-6 | Invalid-UTF-8 / byte-exact `bytea` and `text` | The D1 lesson: a transport that rewrites text server-side is invisible client-side |
| V-7 | Scatter result **assembly** | Does a scatter `SELECT` with `ORDER BY` return a correct global order, and does a `LIMIT` interact correctly? An assembly bug is a silent-loss class |

## Tier D — the operational hazards, from the Vitess lane

sluice already paid for these in MySQL-land. They map almost one-for-one and each deserves a probe once basics work.

| # | Hazard | Vitess precedent |
|---|---|---|
| D-1 | Reshard mid-stream | v0.131.4: sync had to survive the transient primary-routable window, and the reshard-follow reopen was REPLICA-defaulting — wrong across a reshard |
| D-2 | MoveTables mid-stream | The filtered move-OUT gate exists because a filtered-sync Critical shipped. Neki's MoveTables is the analogue and needs the same end-to-end gate before anything ships |
| D-3 | Planned switchover / unplanned failover | The Admin service does both. What does an in-flight sluice copy or CDC stream see? |
| D-4 | Online schema change (shadow table) | Does a shadow-table build appear to sluice's schema reader as a real table? A stray shadow copied to the target would be silent garbage |
| D-5 | Router restart / topology change | "components pick up topology changes without restarting" — but what does an open connection see? |
| D-6 | Scan caps under scatter | Vitess truncates full scans at its OLTP row cap unless `set workload=olap` — a **silent** truncation. Neki's fanout rejection is loud, which is better, but confirm there is no quiet cap behind it |

## Running the probes

`scripts/neki-probe.sh` runs the mechanically checkable ones and prints a verdict per row. It is **read-only and refuses to write** unless `--allow-writes` is passed, because the first run should not be the thing that creates state on a cluster we do not yet understand.

Credentials: a Neki role connection string in `NEKI_DSN`. On this machine the PlanetScale service token at `C:\code\PLANETSCALE_SLUICESYNC.env` can see the `neki-test` database at org level but has **no branch-level grant**, so it cannot mint one — that grant (or a directly-supplied DSN) is the current blocker.

## What would change the conclusion

Recorded here so the ADR's premise is falsifiable rather than decorative:

1. **R-1 refuses.** Then there is no CDC lane through the router and the flavor carries a wholly different capture mechanism — still a flavor by the Vitess precedent, but a far larger chunk.
2. **R-8 finds no readable topology.** Then sluice cannot tell a sharded table from an unsharded one and must refuse conservatively database-wide, which is a bad enough product that it changes the priority, not just the design.
3. **Tier C finds a rendering divergence.** Then Neki gets its own exact-re-read phase, as VStream did, and the value contract needs a per-family verdict before anything ships.
