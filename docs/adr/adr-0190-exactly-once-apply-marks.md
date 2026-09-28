# ADR-0190: Exactly-once apply marks — a restart skips the changes of an interrupted source transaction that already reached the target

- **Status:** Accepted 2026-09-28 (operator answered the open questions — see "Operator decisions" below); proposed 2026-09-26 as DESIGN ONLY. Operator decision on GC-38 (l) (option (a) chosen over (b) net-effect replay and (c) transaction-aligned apply; see §"Alternatives considered"). Nothing here is implemented; the build waits on this ADR's approval and on the open questions at the end.
- **Date:** 2026-09-26
- **Related:** [ADR-0007](adr-0007-position-persistence.md) (position written in the batch's own transaction); [ADR-0010](adr-0010-idempotent-applier.md) (idempotent UPSERT apply, the assumption this ADR finds is not enough); [ADR-0027](adr-0027-source-transaction-boundary-cdc-batching.md) (source-transaction cohesion on the serial batched path); [ADR-0089](adr-0089-default-adaptive-apply-batch-size.md) (its keyless guard: keyless tables are at-least-once); [ADR-0104](adr-0104-mysql-pipelined-cdc-apply.md) / [ADR-0105](adr-0105-postgres-concurrent-cdc-apply.md) (the lane path and its position relaxation); audit backlog GC-38 (l) (the measurement); `internal/pipeline/streamer_crash_midtxn_integration_test.go` (the gate that pins today's contract and will flip its loud cells when this lands).

## Context

A restart re-delivers every change after the persisted position, and every apply path relies on ADR-0010 to make that harmless: an UPSERT of a change onto a target that already holds it is a no-op. GC-38 (l) measured where that assumption breaks. When a crash leaves a **prefix** of a source transaction committed on the target, the restart replays the transaction from its first change onto rows that the transaction itself has already changed. Most shapes converge. Three do not, because the replayed change meets a uniqueness constraint that the transaction's own later changes have already moved:

- a transaction that frees a unique value and reuses it (`DELETE u='x'` … `INSERT u='x'`),
- a swap through a temporary (`a.u=tmp; b.u=a0; a.u=b0`),
- a primary-key change (`UPDATE k1 → k2`), whose replay finds `k1` gone and `k2` present.

The replayed change fails with `23505` / `Error 1062`. The persisted position never moved past the transaction, so **every** restart re-delivers the same prefix onto the same target and fails the same way. It is loud and never silent (the gate asserts the position never passes the transaction and no row outside it is damaged), but there is no configuration that avoids it and no recovery short of re-copying the table (`--restart-from-scratch`).

### What was measured (GC-38 (l), `TestStreamer_CrashMidTxn_*`)

Sources: MySQL 8.0 GTID, MySQL 8.0 file/pos, MariaDB 11.4. Targets: Postgres 16, MySQL 8.0. The kill is a cancel while a target row lock holds the applier mid-transaction; the gate asserts the persisted position did not move, so it is equivalent to a SIGKILL at that point.

| Apply path | Before the kill | After restart |
|---|---|---|
| per-change, serial (`--apply-batch-size 1`) | a prefix applied | stops on the collision, every restart |
| per-change, lanes | a prefix applied | same |
| lanes, batched — **the operator default** (`--apply-concurrency auto`) | a prefix applied (a lane flushes early at every PK-changing barrier and at its size cap) | same |
| serial batched, transaction fits one batch | nothing applied | converges exactly |
| serial batched, transaction larger than a batch | a prefix applied | same collision |

By code-reading, not measured: (1) the lane path's checkpoint is a separate write, so a crash after a transaction fully committed but before the next checkpoint replays the **whole** transaction onto its own end state — the same collision with no mid-transaction kill at all; (2) VStream emits no transaction markers, so no apply path has cohesion on PlanetScale/Vitess; (3) Postgres sources re-deliver whole transactions from the slot and share the same appliers.

### When rows commit and when the position is saved, per path

- **Serial batched** (`appliershared.runOneBatch`, `internal/appliershared/batch_loop.go:400`). Data and position are written in one target transaction (`commitBatch`, `:781`), but the batch flushes mid-source-transaction at the row cap (which the ADR-0052 controller can shrink far below the configured ceiling), the byte cap, a keyless row, and a 100 ms delivery gap. `CheckpointOnlyAtTxBoundary` (doc at `:120`, enforced at `:796`) then commits the data **without** the position, so a mid-transaction flush leaves a durable prefix with the position still before the transaction. Only a transaction that fits one batch gets ADR-0027 cohesion.
- **Per-change** (`postgres/change_applier.go:1354`, `mysql/change_applier.go:1090`). Each row commits in its own transaction with no position (`applyOneImpl(..., writePosition=false)`); the position is written separately at the `TxCommit` (`persistSourceTxCommit`, `postgres/change_applier.go:1437`, `mysql/change_applier.go:1330`). Every row of a transaction is a durable prefix point.
- **Lanes** (`internal/laneapply/laneapply.go`, the ADR-0104 relaxation documented at `:56`). Each lane commits its routed sub-batch in its own transaction (`ApplyLaneBatch`); the merged position is persisted by the coordinator in a **separate** transaction (`WriteCheckpoint`, `:172`) up to the contiguous frontier's last transaction boundary, every 2000 routed changes (`checkpointEveryChanges`, `:230`), every idle second (`checkpointIdlePeriod`, `:246`), and around each barrier (`barrier`, `:903`). A transaction's rows scatter across lanes and commit independently; the position can lag the data by up to a checkpoint interval but never lead it.
- **Barrier** (keyless rows, PK-changing updates, malformed rows, Truncate, SchemaSnapshot; `laneapply.go:903`). Applied alone on the coordinator in global order, position-free, followed by a checkpoint. A PK-changing update is therefore always its own committed prefix point.

The invariant that holds everywhere — *persisted position ≤ durably committed data* — is exactly what makes the prefix possible: the data can lead the position by a partial transaction on every path, and by whole transactions on the lane path.

## Decision

Record, **in the same target transaction as the rows**, a small per-key *apply mark* naming the last change of the current source transaction that reached that key. On a restart, compare each re-delivered change's identity against the mark for its key and **skip** it when the mark shows it was already applied. Marks are written only by the changes whose replay is not idempotent; the skip check consults them for every change.

The two pieces that make this correct are a **stable change identity** (§1) and the **per-key prefix property** the lane router already guarantees (§2). Everything else is bookkeeping.

### 1. Change identity: `(TxID, Seq)`, assigned by the reader

Every row change carries a new field `ApplyID{TxID string; Seq uint64}` (on `ir.Insert` / `ir.Update` / `ir.Delete`; `internal/ir/change.go:164` and siblings), assigned by the **reader** — never by the pipeline or the applier — so that pipeline-side filtering (`filterChanges`, `internal/pipeline/filter.go:205`), `--include-table` changes, and add-table scope changes cannot shift it. `TxID` identifies the source transaction; `Seq` orders the change within it **per table**: the index of this change among the transaction's changes to the same table, counted before any sluice-side filter. A per-table ordinal is chosen over a per-transaction one because it is unaffected by changes to *other* tables' scope (a publication that gains a table, an `--exclude-table` edit).

The identity is only useful if a re-delivery produces the **same** `(TxID, Seq)` for the same change. Per source:

| Source | `TxID` | Stable across re-delivery? |
|---|---|---|
| MySQL, GTID mode | the transaction's own GTID `uuid:n` — the reader already stages it (`stageGTID`, `mysql/cdc_reader.go:2169`) before the rows; note it is **not** the row's `Position` (rows carry the pre-transaction set, `positionFor`, `:2312`) | Yes by construction: a GTID names one transaction on every server that holds it, and a resume always restarts at a transaction boundary. `Seq` counts ROWS (not rows events), so a replica that re-encodes the same transaction into differently-split rows events yields the same ordinals — **UNVERIFIED PREMISE** (row-image identity across a failover); verified by a test that resumes the same transaction from a replica. |
| MariaDB, GTID | `domain-server-seqno` | Same argument; same premise. |
| MySQL, file/pos | `server_uuid` + binlog file + the transaction's `BEGIN` / GTID-event offset | Yes: the v0.137.2 `server_uuid` stamp already refuses a position on a different instance, and a resume restarts at a transaction boundary (`CheckpointOnlyAtTxBoundary`). |
| Postgres (pgoutput) | the transaction's `CommitLSN` (already every row's `Position`; `postgres/cdc_reader.go:1361`, `positionAt` `:1952`), plus the pinned system id and timeline | Yes: logical decoding re-emits a transaction's changes in WAL order; the timeline pin refuses a different history. `Seq` counts the transaction's changes to the table **within the publication**, so a publication row filter (`--where` on PG 15+) that changes between runs shifts it — bound by the drift check in §5. |
| PlanetScale / Vitess (VStream) | `keyspace/shard` + that shard's GTID **before** the transaction (the shard's component of `r.currentVgtid`, which the reader advances only on the trailing VGTID event, `mysql/cdc_vstream.go:1526`; the ROW event names its shard, `:1856`) | Yes for the per-shard component: a resume restarts each shard from its own GTID, so a shard transaction's pre-transaction component is the same on every delivery. The **merged** VGTID the rows carry today is NOT stable — shard interleaving can differ between deliveries — which is why `TxID` must be per shard. Requires a reader change: VStream currently drops `BEGIN`/`COMMIT` (`:1567`) and does not stamp the shard onto the IR. **UNVERIFIED PREMISE**: vtgate re-delivers a shard transaction's ROW events in the same order on resume; verified by `TestVStream_*` replaying a mid-transaction resume and comparing identities. |
| postgres-trigger, sqlite-trigger, d1-trigger | empty; `Seq` = the change-log id | Yes: the id is a persisted, unique, monotone column of the capture table (`laneapply.go`, `flushPendingBoundary` doc at `:785`). With an empty `TxID` the skip rule below degenerates to "skip if the mark's id ≥ this id". |

A new optional reader capability (`ir.ApplyIdentityProvider`) declares that a reader stamps `ApplyID`. A reader that does not — every reader before its phase lands — produces changes with a zero `ApplyID`, and the applier treats a zero identity as *never skippable* (today's behaviour exactly).

### 2. Why per-key marks, and which changes write them

The lane router guarantees that every change sharing a route lands on one lane in source order (`laneapply` package doc), so **for any single key, the changes applied before a crash are a prefix of that key's changes in the transaction** — on every path (serial is trivially in order; lanes by the router; the barrier by global ordering). That is the property a skip needs. A per-*table* high-water mark would need the same property for the whole table, which holds only for tables routed `RouteScopeTable` (`laneapply/router.go:61`) and not for PK-only tables spread across lanes. A per-*lane* mark would be cheaper, but it is valid only if the restart routes every change to the same lane — the lane count, the hash (which v0.156.4 changed), and the per-table scope probe can all differ after a restart, and a routing mismatch there would skip a change that was never applied: silent. Per-key marks are independent of routing, so they are the only one of the three that is safe across an upgrade or a changed `--apply-concurrency`.

**Which changes write marks.** Replay is idempotent for a primary-key-only table whose transaction changes no primary key, so marking every change would double the write volume for no benefit. A change writes a mark when it is one of:

1. a change to a table with any non-PK uniqueness constraint (the tables the router already scopes `RouteScopeTable` — the probe exists),
2. a primary-key-changing update (it marks **both** the before-key and the after-key),
3. a change to a keyless table (the key is the empty string, so the mark is per table; keyless tables are already barriered in global order, so a per-table mark is a prefix there too — this makes keyless CDC exactly-once across a crash, which ADR-0089 could not).

**Every** change — marked class or not — consults the marks before it is applied. That is what closes the resurrection case: in `UPDATE k1 SET v=5; UPDATE k1→k2; UPDATE k2 SET v=7` fully applied and then replayed, the first update is on an idempotent PK-only path and writes no mark of its own, but the PK change marked `k1` at a higher `Seq`, so the replay skips it instead of re-inserting `k1`.

### 3. The mark table and the write protocol

A new control table, **`sluice_cdc_apply_marks`**, created by `ensureControlTable` (`postgres/control_table.go:81`, `mysql/control_table.go:289`) and added to `appliershared.ControlTableNames` so every schema reader excludes it:

| Column | Meaning |
|---|---|
| `stream_id` | the stream (a Shape A fan-in has one per shard stream, so shards never read each other's marks) |
| `table_name` | the target table, schema-qualified (multi-database routing marks the routed table) |
| `key_digest` | a digest of the target primary-key values, canonicalized the way `laneapply.WriteCanonicalKeyValue` already canonicalizes them (empty for a keyless table) |
| `tx_id` | `ApplyID.TxID` of the last applied change to this key |
| `seq` | `ApplyID.Seq` of that change |
| `change_digest` | a digest of that change (operation + key + after-image), the tripwire in §5 |
| `scope_digest` | the digest of the stream's row-filter and scope configuration at write time (§5) |

Primary key `(stream_id, table_name, key_digest)`; each write is an UPSERT, so the table holds one row per marked key, not one per change.

**Protocol.** On every apply path, a marked-class change's mark is written **inside the same target transaction** as the change's own row write:

- serial batched: inside `runOneBatch`'s batch transaction, coalesced to one UPSERT per key per batch (the last change wins);
- per-change: inside `applyOneImpl`'s transaction;
- lanes: inside `ApplyLaneBatch`'s transaction, coalesced per key per sub-batch;
- barrier: inside `ApplyBarrierChange`'s transaction.

Because the mark and the rows commit together, the target can never hold a row without its mark or a mark without its row. The mark does **not** need the position: `sluice_cdc_state` keeps its current semantics and cadence untouched. Moving the lane checkpoint into the lane transactions was considered and rejected — lanes commit independently, so no single lane transaction can own a merged position. The marks close the lane post-commit window instead: a fully-applied transaction replayed after a lost checkpoint finds every marked key already at its last `Seq` and skips the non-idempotent changes, while its idempotent changes re-apply harmlessly.

**The skip decision.** When `Apply` starts, the applier loads this stream's marks into memory (the table holds only keys touched by marked-class changes of transactions not yet garbage-collected, so it is small). For each delivered row change with a non-zero `ApplyID`, before dispatch: look up the marks for the change's keys (before-key and after-key for an update); if a mark has `tx_id == TxID` and `seq ≥ Seq`, skip the change. A skip counts toward neither `rows_applied` nor the skip ledger, and is logged at DEBUG with a per-run INFO total. The lookup is in memory; no per-change round trip.

**Garbage collection.** A mark is needed only while its transaction can be re-delivered, that is, until the persisted position passes the transaction's commit.

- Serial batched and per-change: the `TxCommit` position write deletes the marks whose `tx_id` is the committing transaction, in the same transaction as the position (`commitBatch` / `persistSourceTxCommit`).
- Lanes: the coordinator records the `TxID` of each transaction whose `TxCommit` it passes; `writeCheckpoint` (`laneapply.go:1227`) deletes the marks of every transaction at or below the boundary it persists, in the checkpoint's own transaction.
- After a crash, some marks can outlive their transaction (a checkpoint lost between the lane commit and the delete). They are harmless — a stale `tx_id` never equals the `TxID` of a change delivered after the persisted position (§5, identity uniqueness) — and are swept at the first checkpoint after a restart: any mark loaded at start whose transaction was not re-delivered before that checkpoint is stale and deleted.
- Trigger sources (empty `TxID`): delete marks with `seq` at or below the persisted change-log watermark.

### 4. Every path that must honour the marks

| Path | Honours marks | Notes |
|---|---|---|
| `sync start` / `sync run`, serial batched | yes | Phase 1 |
| per-change (`--apply-batch-size 1`) | yes | Phase 1 |
| lanes / `auto:N`, including the barrier | yes | Phase 2 |
| fleet supervisor restarts | yes, by construction | the supervisor restarts the same stream; the applier loads the marks |
| Shape A fan-in (`--inject-shard-column`) | yes | marks are per `stream_id`; the injected shard column is part of the target key, so the digest covers it |
| multi-database routing | yes | `table_name` is the routed target table |
| sync cold start (bulk copy) | exempt | the copy is not a CDC replay; the cold start clears the stream's marks (§5) |
| `sync from-backup` broker | **out of scope, follow-up** | the broker replays chain records with their own identity (link, chunk, record ordinal), which is intrinsic and stable; the same mark table fits, but the broker is a separate phase with its own pins |
| chain restore | exempt | a restore applies a chain into a target it owns from the start and re-runs from the beginning on failure |
| `migrate` | exempt | no CDC |

### 5. Silent-loss analysis

The tenet decides this design: GC-38 (l) is loud today, and a skip that fires wrongly turns it into silent loss. The rule the implementation must hold is that **a mark lookup can only ever cause a skip of a change the target already holds; any doubt resolves to applying the change**, which is today's loud behaviour at worst.

| Failure mode | Why it cannot skip an unapplied change, or how it refuses |
|---|---|
| Mark written but the rows not | Impossible: the mark and the rows are one target transaction. |
| Rows written but the mark not | Impossible for the same reason; a marked-class change never commits without its mark. |
| Stale marks after `--restart-from-scratch`, `--reset-target-data`, a slot-loss re-snapshot, or any cold start | Every cold-start path deletes the stream's marks before its copy begins, in the same place it resets `sluice_cdc_state`. Pinned by a test that re-snapshots and asserts the marks table is empty for the stream. |
| Identity collision across unrelated transactions | `TxID` is unique per source transaction by construction for every source in §1 (a GTID, a server-pinned file/offset, a timeline-pinned CommitLSN, a per-shard GTID, a unique change-log id). A source that would reuse an identity (a rebuilt server, a new timeline) is already refused by the existing identity pins. |
| Ordinal drift (a re-delivery numbers the same change differently) | The **tripwire**: when a replayed change's `Seq` equals a mark's `seq`, the applier compares the change's digest with the mark's `change_digest`; a mismatch refuses loudly (`APPLY-MARK-MISMATCH`, terminal, naming the table, key and identities) instead of skipping. This is the independent expected value on the skip path. It also catches a reader bug that drops or reorders rows. |
| A changed `--where` row filter or scope between runs | `scope_digest` records the stream's row-filter and scope configuration at write time; a mark whose `scope_digest` differs from the current run's is never used to skip — the applier refuses loudly (the same drift class `rowFilterHashDriftAny` already guards, `internal/pipeline/streamer_publication_ratchet.go:220`). |
| Schema change mid-transaction (a changed primary key or key column type) | The key digest is computed from the target's current key columns. A changed key yields a different digest, so the lookup misses and the change applies — today's behaviour, never a skip. |
| Lane re-partitioning after an upgrade or a changed `--apply-concurrency` | Per-key marks do not depend on routing (§2), so a different lane layout neither invalidates nor misapplies them. |
| Clock drift | No clock is read anywhere in the protocol. |
| Upgrade with a transaction in flight | The in-flight transaction's prefix has no marks (written by the older binary), so its replay behaves exactly as today. |
| Downgrade to an older binary | An older binary does not know the table and replays as today: loud at worst, never silent. The table is inert to it. |
| A reader that does not stamp `ApplyID` | A zero identity is never skippable (§1). |
| Partial after-images (Postgres unchanged TOAST) | Marks do not change what a replayed change writes; they only suppress changes already applied. A partial image that is applied is applied exactly as today. |

**Independent expected value for the pins:** the source's own final table state (`SELECT … ORDER BY` key, or an md5 over it), compared against the target after the crash and restart, with the persisted position asserted not to have passed the transaction before the restart (the existing gate's contract).

### 6. Cost

- **Writes.** One extra UPSERT per distinct marked key per batch, and only for the marked classes of §2. A PK-only workload with no PK changes writes none. A table with a secondary UNIQUE index — already pinned to one lane by the router — writes roughly one mark per changed row, coalesced within a batch.
- **Deletes.** One `DELETE … WHERE stream_id = ? AND tx_id IN (…)` per position save or checkpoint, only when the committing transactions wrote marks.
- **Reads.** One `SELECT` of the stream's marks at `Apply` start. The skip check is in memory.
- **Growth.** Bounded by the marked keys of transactions not yet covered by a persisted position, which on the lane path is at most about one checkpoint interval (2000 changes) plus the in-flight transactions.
- **Migration for existing deployments.** `ensureControlTable` creates the table when absent, following the `unforwarded_refusal` pattern exactly (`ensureUnforwardedRefusalColumn`, `postgres/unforwarded_refusal_store.go:31`, `mysql/unforwarded_refusal_store.go:33`): detect first, because a DML-only role gets "must be owner" on an unconditional DDL even when the object exists; a `--schema-already-applied` run and a PlanetScale safe-migrations branch cannot create it, so the ADR proposes (open question 2) that sluice then logs a grep-stable WARN (`APPLY-MARKS-UNAVAILABLE`) and runs without marks — today's loud behaviour — rather than refusing to start.

### 7. Alternatives considered

- **(b) Net-effect replay of the first post-restart transaction** — delete every key it touches, then write each key's final image. Rejected. It converges from any prefix, but on a Postgres target whose sync role cannot bypass foreign keys the deletes fire `ON DELETE CASCADE` into rows the transaction never touched: silent loss, exactly the conversion the tenet forbids. It also needs complete after-images (Postgres omits unchanged TOAST), buffers a whole transaction in memory, and does not cover the lane post-commit window.
- **(c) One target transaction per source transaction, with the position** — true today of the serial batched path only while a transaction fits one batch. Rejected as the fix: making it hold for every transaction means never flushing mid-transaction, which bounds nothing for a large transaction (memory, lock time); it is impossible across lanes without giving up lane concurrency for every table with a secondary unique index; and it does nothing for VStream until that reader emits transaction boundaries. It remains a reasonable complement for small transactions and is not ruled out.
- **Per-table or per-lane marks** — cheaper, but correct only under table-scoped routing (per-table) or an identical lane layout on restart (per-lane). §2 explains why either can skip an unapplied change.

(a) is the only option that covers the mid-transaction crash, the lane post-commit window, every source including VStream, and every apply path, while keeping every uncertain case on the loud side.

### 8. Phased implementation and pins

1. **Identity.** `ApplyID` on the IR and `ir.ApplyIdentityProvider`; MySQL GTID and file/pos, MariaDB, and Postgres readers stamp it. Pins: for each reader, reopen the stream from the same transaction boundary twice and assert the delivered `(TxID, Seq)` sequences are identical, including from a replica for the GTID premise.
2. **Serial and per-change.** The mark table, its migration, the write in the serial batch and per-change transactions, the in-memory skip, the tripwire, the `scope_digest` check, the cold-start clear, and GC at the position save. Pins: flip `TestStreamer_CrashMidTxn_*`'s serial and per-change cells from "stops on every restart" to "converges exactly", graded against the source's own final state.
3. **Lanes and barrier.** Writes inside `ApplyLaneBatch` and `ApplyBarrierChange`, and GC at `writeCheckpoint`. Pins: the default-lane cells flip; a new post-commit/pre-checkpoint crash cell (kill after a transaction fully commits across lanes, before the checkpoint) converges; lane count changed between the crash and the restart converges.
4. **VStream.** Per-shard identity in the reader (stop dropping `BEGIN`/`COMMIT`, stamp the shard). Pins: the same mid-transaction and post-commit cells on vttestserver.
5. **Trigger sources.** Change-log-id identity. Pins: postgres-trigger mid-batch crash converges.
6. **Tripwire and drift pins, all phases.** A deliberately shifted ordinal refuses with `APPLY-MARK-MISMATCH`; a changed `--where` refuses; a re-snapshot clears marks; a zero identity never skips; a downgrade (marks present, older-binary behaviour simulated by disabling the skip) replays loudly. Mutation-run each branch in both directions.

The pin matrix is source (MySQL GTID, MySQL file/pos, MariaDB, Postgres, VStream, postgres-trigger) × target (Postgres, MySQL) × apply path (per-change, serial batched, lanes) × crash point (mid-transaction, post-commit before checkpoint).

**Out of scope:** the `sync from-backup` broker (a follow-up with chain-record identity), chain restore, keyless-table exactly-once beyond the crash-replay window (ADR-0089's other cases), and any change to the position's cadence or format.

## Operator decisions (2026-09-28)

1. **Marked classes:** non-idempotent classes only (secondary-unique tables, PK changes, keyless tables); every change still CHECKS marks.
2. **Missing mark table:** WARN `APPLY-MARKS-UNAVAILABLE` and run without marks (today's behaviour); no new refusal.
3. **Keyless tables:** included, in Phase 2.
4. **The broker (`sync from-backup`):** NOT in this ADR — a separate ADR later.
5. **VStream reader change:** accepted as Phase 4.

## Open questions for the operator (as proposed; answered above)

1. **Marked classes.** This ADR marks only non-idempotent classes (secondary-unique tables, PK changes, keyless tables) to avoid doubling writes for PK-only workloads. The alternative — mark every change — is simpler to prove and costs one extra write per changed row. Recommendation: the classes.
2. **Missing mark table** (safe-migrations branch, non-owner role, `--schema-already-applied`). Recommendation: WARN (`APPLY-MARKS-UNAVAILABLE`) and run without marks, which is today's behaviour; the alternative is to refuse to start.
3. **Keyless tables.** Including them makes keyless CDC exactly-once across a crash, which is new behaviour beyond GC-38 (l). Recommendation: include, in Phase 2.
4. **The broker.** In scope for a later phase, or left to a separate ADR?
5. **VStream reader change.** Phase 4 changes what the VStream reader emits (transaction boundaries, the shard). It is the only way to give VStream a stable identity; confirm it is acceptable before that phase.
