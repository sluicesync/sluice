# ADR-0191: Exactly-once broker replay — a recorded source identity, a mid-incremental frontier, and ADR-0190's marks

- **Status:** Accepted 2026-10-05 (operator, §12): design only, no code yet; sequenced after F-E1-SEVERED-TAIL-REPLAY and ADR-0190 Amendment E. Written against `98ad6ec6` (v0.156.11 + the NEKI-012 backlog correction). The F-E1 follow-up that ADR-0190's operator decision 4 ("the broker: NOT in this ADR — a separate ADR later") and amendment E's E-Q5 sequenced ahead of amendment E. Open questions in §12; nothing here is decided until the operator answers them.
- **Date:** 2026-10-05
- **Related:** [ADR-0190](adr-0190-exactly-once-apply-marks.md) (the apply-mark machinery this ADR reuses unchanged, and the invariants (i)–(iii) of its amendment C); [ADR-0007](adr-0007-position-persistence.md) (position in the data transaction); [ADR-0010](adr-0010-idempotent-applier.md) (the idempotency the broker's BRK-1 comment leaned on); [ADR-0027](adr-0027-source-transaction-boundary-cdc-batching.md); [ADR-0046](adr-0046-inline-backup-chain-rotation.md) / [ADR-0067](adr-0067-contiguous-rotation-handoff.md) (segments and rotation); [ADR-0064](adr-0064-backup-smart-compaction.md) (smart compaction); [ADR-0087](adr-0087-compact-group-split-and-rotation-boundary-resume.md); [ADR-0113](adr-0113-restore-reparent-reconciliation.md) (reconcile); audit backlog **F-E1** and its sub-items F-E1-KEY-REUSE-REPLAY and F-E1-ROTATED-SEGMENT-OVERLAP.

## Why a new ADR and not ADR-0190 amendment F

ADR-0190's amendments A–E each change the mechanics of ONE thing ADR-0190 already owns — the apply side's marks, fence and checkpoints — and every one of them leaves the identity and the position model alone. This design leaves the apply-side mark machinery exactly as ADR-0190 left it (§3.4 changes no line of `internal/applymarks`) and instead changes three surfaces ADR-0190 never touched: the **backup change-chunk codec** (a recorded identity — a persisted format, so it gets the new-surface codec checklist), the **broker's position model** (a token that can stand inside an incremental), and a **producer-side dedupe** shared by the broker and chain restore. Operator decision 4 also asked for a separate ADR in so many words. ADR-0190 is 830 lines; an amendment F there would bury a format change inside an apply-path document.

## Context

### What v0.156.11 shipped, and what is still open

`sync from-backup` replays a backup chain's incrementals into a target. Every change of an incremental is stamped with the PARENT token (BRK-1, `internal/pipeline/broker.go:1253`–`1257`, `rewritePosition` at `:1518`), and the position advances to the incremental's own id only in the post-stream `writePositionDirect` (`:1286`). So a run interrupted partway through an incremental re-applies the **whole** incremental on the next run. The changes carry no `ApplyID` (`internal/ir/change.go:164`–`181`: "a backup chunk replay leaves it zero"; held by `TestReplayPathsNeverFold`, `internal/pipeline/replay_never_folds_test.go:112`/`:142`), so ADR-0190's marks can skip none of them.

v0.156.11 shipped the interim loud door (operator decision 2026-10-04): keyless tables refused before anything is applied (`SLUICE-E-BROKER-KEYLESS-TABLE`, `refuseKeylessTables`, `internal/pipeline/broker_keyless.go:185`), and `BROKER-INCREMENTAL-PARTIAL` (exit 1) on a cancel inside an incremental. Two harms remain:

1. **Keyless tables cannot be replayed at all** by the broker.
2. **F-E1-KEY-REUSE-REPLAY** (measured on both engines): re-applying an incremental that moved a key value onto another row either fails on the key on every re-run (`[INSERT 1, UPDATE 1→2]`) or — `[UPDATE 1→2, DELETE 2, UPDATE 3→1]` onto the end state `{1}` — applies a change to the wrong row and empties the table at a nil error.

The goal: re-applying a partially applied incremental is exactly-once, so (1) the keyless refusal can be lifted or narrowed and (2) KEY-REUSE closes.

### A third harm found while establishing the ground truth: the chain itself overlaps between incrementals

§1.4 shows that consecutive incrementals of one segment can carry the **same source transaction twice**, with no crash anywhere: a backup window that ends mid-transaction is completed by the next window re-delivering the whole transaction, and on Postgres chains written before v0.138.0 every resumed window re-delivers its parent's last transaction. The broker and chain restore apply both copies. That is the F-E1 mechanism in the steady state, and it decides the identity question: **only an identity recorded from the SOURCE can tell the two copies are one change.** Filed as F-E1-SEVERED-TAIL-REPLAY (§11).

## 1. Ground truth, from the code

### 1.1 What a broker change carries, and what a chunk records

- **On the wire** (`internal/pipeline/blobcodec/backup_change_chunk.go:571`–`579`, `changeWire`): `_t` (kind), `schema`, `table`, `row` / `before` / `after`, and the ORIGINAL source `position`. `TxBegin` / `TxCommit` are recorded with their positions (`:646`–`649`). **No `ApplyID`, no transaction id.** Every change kind but `ir.SchemaSnapshot` is recorded (`recordedInChangeChunkStream`, `internal/pipeline/incremental.go:1479`); snapshots ride the manifest's `SchemaHistory`.
- **The source position per change** is the reader's: MySQL GTID rows carry the executed set BEFORE their transaction and the `TxCommit` the set after it (item 132); Postgres rows carry the transaction's commit LSN and, since v0.138.0, the `TxCommit` carries `TransactionEndLSN` (CHANGELOG A2-1); VStream rows carry the pre-transaction MERGED VGTID, which ADR-0190 §1 shows is NOT stable across deliveries (shard interleaving). A position is therefore not a usable identity on every source, and parsing engine tokens in the broker would put engine knowledge in the pipeline (IR-first). The reader's `ApplyID` already IS the per-source identity, with its stability premises pinned per engine (ADR-0190 phase 1, `TestCDCReader_ApplyIdentity_*`, `TestPGCDC_ApplyIdentity_StableAcrossRedelivery`, `TestVStream_ApplyIdentity_StableAcrossMidStreamResume`, `TestCapture_ApplyIdentityIsTheChangeLogID`) — and the backup capture path computes it and then discards it at `encodeChange`.
- **The broker rewrites every change's position** to the parent token (`rewritePosition`) after recording the original in `lastApplied` for the F1 tail backstop (`broker.go:1482`–`1489`).

### 1.2 Is an incremental's change stream deterministic across re-reads?

| Mechanism | Effect on the decoded stream of one incremental | Evidence |
|---|---|---|
| Re-fetch | byte-identical or refused: every chunk is fetched through `FetchChunkVerified(…, chunk.SHA256)` against the manifest's recorded SHA-256 | `broker.go:1453` |
| Encryption | deterministic: GCM-decrypt of verified bytes under a fixed AAD (`ChangeChunkAADFor(owner, chunk, chunkIdx)`) | `broker.go:1464` |
| Compression | deterministic: decompress of verified bytes | — |
| Chunk boundaries | fixed by the manifest's ordered `ChangeChunks` | — |
| Rotation | creates new segments; never rewrites an existing incremental's chunks | ADR-0067 |
| Naive compaction (`backup compact`) | chunks MOVED verbatim into the merged segment; bytes, SHAs and order unchanged; `BackupID` unchanged (it hashes created_at/engine/kind/end_position, not the parent link) | `internal/pipeline/backup/chain_compact.go:56`–`64`, `:1095`–`1097` |
| Smart compaction (`--smart-compaction`) | **rewrites** the chunks of every incremental in a merge group: per-key collapse across the incremental's transactions, new chunk SHAs | `chain_compact_smart.go:7`–`60`, `:1919` |
| Re-keying | none re-encrypts chunks (KEK rotation re-wraps keys) | `grep` of `internal/pipeline/backup` |

So the stream is a pure function of the manifest's ordered chunk SHA list, **except across smart compaction**, which changes the list. A digest of that list (§3.2) is therefore an exact, cheap test of "the same stream". Whether smart compaction leaves `BackupID` unchanged was not re-derived and the design does not depend on it.

### 1.3 How ADR-0190's marks work, and why they do nothing for the broker today

- Marks are written only for non-idempotent classes (secondary-unique tables, PK-changing updates, keyless tables), checked by every change, loaded at the start of EVERY apply run (`startApplyMarks` at `postgres/change_applier_batch.go:100`, `mysql/change_applier_batch.go:90` — the broker calls `ApplyBatch` once per incremental, so once per incremental), and garbage-collected by the position write that passes their transaction (`CloseOpen`, `internal/applymarks/applymarks.go:427`; `CloseTxs`, `:443`; the restart sweep, `:476`).
- **The skip rule trusts a mark only for the FIRST transaction an apply run delivers** (`consult`, `:366`–`401`; amendment B and its runtime check `APPLY-MARK-UNTRUSTED`).
- The broker reaches every mark-writing path: serial batched, and — by default — the lanes, since `ResolveReplayApplyConcurrency(0)` is `DefaultApplyConcurrency = 4` (`internal/pipeline/migcore/apply_concurrency.go:21`, `:49`–`58`; `broker.go:435`). It never reaches amendment D's fold (`ExactlyOnceLanes` is set only from `streamer_run_phases.go`), and every change it sends has a zero `ApplyID`, so `verdict` returns "not involved" at its second line.
- **Cost per transaction** where marks are written: serial — one coalesced UPSERT per marked key per batch plus one `DELETE … tx_id IN (…)` at the commit's position write, all inside transactions that happen anyway; lanes — barrier marks ride the barrier's own transaction (keyless and PK changes are barriers already); secondary-unique lane marks only under `--exactly-once-lanes` (amendment C).
- **Marks need source-transaction structure.** GC fires at a `TxCommit` (serial) or a recorded boundary (lanes); the trust rule needs "the first transaction after the persisted position". Broker chunks DO carry `TxBegin`/`TxCommit` for marker sources. What breaks the machinery for the broker is not missing structure — it is the **constant position** (§1.5).

### 1.4 Windows can be severed, and resumed windows re-deliver

- `backup stream`'s stop-poll flushes whatever is buffered — "could be a partial mid-transaction chunk" — and returns (`internal/pipeline/stream.go:1788`–`1816`); the channel-close exit flushes unconditionally (`:1817`–`1828`). One-shot `backup incremental` has the same two exits (`incremental.go:1124`–`1130`). So an incremental can END INSIDE a source transaction T.
- The next window resumes from the parent's `EndPosition` — the position of the last change captured. On MySQL GTID that is a T row's pre-transaction set; on Postgres it is T's commit LSN. Either way the reader re-delivers **all of T**, and the window writer keeps the re-delivered events on purpose: "the replay is the only thing that carries the severed tail" (`stream.go:1655`–`1666`, `incremental.go:1124`–`1130`). Both comments assert that "chain restore replays them idempotently" — the written invariant this design finds false for keyless tables and key-reusing transactions.
- On a Postgres source a resumed window ALSO re-delivered its parent's last, fully committed transaction whenever the parent ended on a `TxCommit`, because that `TxCommit` carried the commit record's START and logical decoding skips only transactions whose commit record starts strictly before the request (`windowAdvancedPast`'s doc, `incremental.go:1040`–`1050`; measured for sync in CHANGELOG v0.138.0 A2-1). v0.138.0 moved the `TxCommit` to `TransactionEndLSN`; chains whose `EndPosition`s were written by older binaries keep the replay. Whether any current source still re-delivers a fully committed parent transaction is an **UNVERIFIED PREMISE** (§3.3 handles it either way).

So two consecutive incrementals N, N+1 of a segment can share a source transaction T in two shapes: **(A) severed** — N carries T's prefix with no `TxCommit`, N+1 opens with all of T; **(B) closed** — N carries all of T, N+1 opens with all of T again.

### 1.5 The position model, and why marks alone cannot work

The broker's position is per INCREMENTAL. Inside an incremental every change and every `TxCommit` carries the parent token, so the serial loop writes the parent token at each `TxCommit` and the lane frontier records the parent token at each boundary. Suppose source-transaction identities reached the applier (marks alone, option (a) below). A crash inside transaction T_k of incremental X leaves: T_1…T_{k−1} committed with their marks already DELETED (their `TxCommit` position writes passed them), and T_k's prefix with its marks. The re-run restarts from the parent, re-delivers T_1 first, and so distrusts T_k's marks (`APPLY-MARK-UNTRUSTED`) and re-applies T_1…T_k with no evidence at all — every keyless row duplicates, and every key move in T_1…T_{k−1} is replayed onto its end state. Keeping the marks until the incremental ends does not rescue it: a mark is per KEY and names the LAST transaction that touched the key (a keyless table has ONE mark row), so T_1's replay finds a mark naming T_k and can prove nothing — the skip would need an order on transaction ids, which is ADR-0190 amendment A's rejected option B. **The position has to stand inside the incremental.**

## 2. Options

| | (a) marks only | (b) mid-incremental frontier only | (c) frontier + marks |
|---|---|---|---|
| What persists | per-key marks, position stays at the parent | a token "incremental X applied through event e", written at every source-transaction boundary in place of today's constant parent token | both |
| Crash mid-T_k | re-delivers T_1…T_k; marks distrusted (§1.5): **duplicates, KEY-REUSE** | re-delivers only T_k: its committed prefix re-applies unmarked — a keyless prefix duplicates; key reuse WITHIN T_k can misapply | re-delivers only T_k, skipping its committed marked changes: **exactly-once** |
| Lane post-commit window (data ahead of the checkpoint) | as above | re-delivers T_k…T_m from the frontier; a barrier always persists its own transaction's start first, so only the first can carry a key change | as (b), and that first transaction's barrier marks skip what landed |
| Severed / closed overlap (§1.4, no crash) | (A) dedupes if the open transaction's marks survive; (B) does not | neither | (A) via marks; (B) via a recorded tail (§3.3) |
| New persisted state | none | token fields | token fields + a chunk field |
| Extra target writes | marks | **none** (the token replaces a write that happens anyway) | marks, only for marked classes (as `sync start`) |
| Complexity | low, and unsound | a producer-side skip + content check | (b) + identity on the wire |

(c) is the `sync start` model exactly: a position at transaction granularity, and ADR-0190's marks for the one transaction a restart re-delivers. It is the only option that is exactly-once at every crash point, and it reuses ADR-0190's soundness argument instead of needing a new one.

**Identity options, inside (c):**

- **Chunk-derived** (`TxID = <backupID>:<chunk-list digest>:<ordinal of the TxBegin>`, `Seq` counted over the decoded stream). Stable whenever the stream is, needs no format change, works on old chains — but it names the SAME source transaction differently in N and N+1, so it cannot dedupe §1.4's overlap, and the keyless refusal could not be lifted on its strength.
- **Position-derived** (parse the recorded source position). Not stable on VStream (§1.1), and engine knowledge in the pipeline.
- **Recorded source identity** — the reader's `ApplyID`, written into the chunk at capture. Stable by the premises ADR-0190 already pins per reader, identical across N and N+1, and engine-neutral on the broker side. Old chains lack it.

## 3. Decision (recommended): option (c), with the recorded source identity

### 3.1 The identity: record the reader's `ApplyID` in the change chunk

- **Wire.** `changeWire` gains an optional `aid` object, `{"t": <TxID>, "s": <Seq>}`, on insert/update/delete records, written only when the change's `ApplyID` is non-zero; `decodeChange` restores it. `encoding/json` ignores an unknown field (`backup_change_chunk.go:516` decodes with plain `json.Unmarshal`), so an older binary reads a new chain unchanged (with no identity). `Seq` is a `uint64` and must round-trip exactly: it is decoded as an integer field, never through `any`, and the codec's value-family pin (Bug 172's lesson) gains `Seq` above 2⁵³ and a `TxID` carrying every reader's alphabet (GTID `uuid:n`, MariaDB `d-s-n`, `filepos:<uuid>:<file>:<off>`, `pg:<sysid>:<tl>:<lsn>`, `vstream:<ks>/<shard>:<gtid>`, trigger `<engine>:<id>:<stamp>` with a non-ASCII `captured_at` neighbour).
- **Manifest.** A new field, `ApplyIdentity: true`, on every incremental manifest written by a binary that records identities **from a reader that declares `ir.ApplyIdentityProvider`**. It is what lets the broker tell "this incremental has no identity" (an older writer, a non-stamping reader, a smart-compacted incremental — §3.5) from "this change happened to carry none" (VStream interleaved groups, a non-GTID component — ADR-0190 phase 4). It is covered by the manifest signature exactly as the other manifest fields are; forging it either way is loud (§3.5 refuses a keyless change without identity whatever the flag says; a flag cleared by an adversary only re-imposes the refusal).
- **Stability: what it rests on.** The identity is the reader's, recorded once and read back byte-exact (chunks are SHA-verified, §1.2), so for the CRASH case — the same incremental re-read — it rests on **nothing but the codec round-trip**, which the codec pin covers. For the OVERLAP case (§1.4) it rests on the reader re-delivering the same transaction with the same `(TxID, Seq)` — exactly ADR-0190 §1's premise, verified per reader (GTID including from a replica, MariaDB, file/pos, Postgres, two-shard VStream; trigger sources by construction). A new premise appears only for the BACKUP capture path: that the identity the backup reader stamps is the one the CDC reader stamps on re-delivery. They are the same reader opened through `openCDCReaderWithSlot`, but nothing binds the two today; §9 P2 is that binding test.

### 3.2 The frontier: a broker position that can stand inside an incremental

**Token v2.** The broker's position token (`encodeBrokerPosition`, `broker.go:328`) gains:

| Field | Meaning |
|---|---|
| `_engine` | `backup-broker-v2` (a NEW sentinel — §6 downgrade, operator question Q3) |
| `chain_url`, `last_applied_backup_id` | unchanged: P, the last FULLY applied incremental |
| `in_progress` | absent between incrementals; inside incremental X: `{backup_id: X, chunks: D, through: e}` — D is the SHA-256 of X's ordered chunk SHA-256 list, e the event ordinal (over X's RAW decoded stream, before any drop) of the last boundary whose effects are durable |
| `tail` | the source identity of the last identity-bearing transaction of the last fully applied incremental, and whether it was CLOSED or OPEN (severed) at that incremental's end, with a digest of its changes (§3.3) |

**What each change carries.** The producer (`streamOneChunkWithPosition`) counts events over X's stream and stamps, instead of the constant parent token:

- a `TxCommit` at ordinal e, and any change OUTSIDE a source transaction (a MySQL `Truncate`, every change of a marker-less trigger chunk) at ordinal e: `in_progress.through = e` — "applied through this event";
- `TxBegin` and every change inside a transaction: `through` = the ordinal of the boundary BEFORE the transaction — that transaction's START. A row's token is therefore never a mid-transaction resume point, which is the MySQL-file/pos rule `CheckpointOnlyAtTxBoundary` (`internal/appliershared/batch_loop.go:122`–`160`) and the lane `handle` (`internal/laneapply/laneapply.go:741`–`800`) already enforce; even if one were persisted it would be safe (T's start).

No applier change: the serial loop already persists only at `TxCommit` (marker streams) or every flush (marker-less), and the lane frontier already persists only the highest recorded boundary at or below the contiguous frontier — a **safe frontier by construction** even though lanes commit out of order. Today those writes carry the parent token; under this design they carry a token that names how far into X they are. **No new target write.**

**Resume.** On a warm start whose token has `in_progress`, the broker re-reads X and, before emitting anything:

1. recomputes D from X's CURRENT manifest; a mismatch refuses, `BROKER-INCREMENTAL-REWRITTEN` (terminal, naming X, both digests and "the incremental was rewritten — e.g. by `backup compact --smart-compaction` — after this broker applied part of it; recover with `--reset-target-data`"). It never skips over a rewritten stream: smart compaction collapses a key's changes ACROSS transactions (an INSERT in T_1 merged with an UPDATE in T_5), so "through e" in the old stream is not a prefix of the new one, and skipping by ordinal would drop T_5's update, silently.
2. reads events 0…e and drops them — still running the F1 tail-backstop bookkeeping and the SHA/GCM checks over every chunk, so a skipped chunk is still verified;
3. emits from e+1. A stream with fewer than e+1 events refuses (`BROKER-INCREMENTAL-REWRITTEN`, the same cause).

**The independent expected value for the skip** is D (recorded when the work was done, compared against the chain as it is now) plus the event count; neither derives from the events being skipped. A skip of committed work without mark evidence is justified only by the persisted token, which the applier wrote in the same transaction as that work.

**End of an incremental.** `writePositionDirect(X)` writes `{P = X, tail = …, no in_progress}`. It deletes no marks: the only marks that can exist then are those of an OPEN (severed) tail transaction, which the next incremental needs (§3.3).

### 3.3 The overlap between incrementals (§1.4), and the shared dedupe

Both shapes are handled in the PRODUCER, engine-neutrally, by one helper used by the broker and by chain restore (`pipeline/backup`, so the broker imports it, as it already imports `backup`):

- **(A) severed: by marks, no new rule.** N ends inside T; N's applied prefix wrote T's marks; nothing closed T, so no position write deleted them. The next apply run (N+1) loads them, delivers T first (it is N+1's leading transaction), so `consult` trusts them, and every marked change of T at or below its mark's ordinal is skipped; idempotent changes re-apply harmlessly. This is precisely ADR-0190's crash-replay case, and amendment B's invariant holds (P = N is T's start; marks name T only).
- **(B) closed: by the recorded tail.** N ended on T's `TxCommit`, so T's marks were deleted at that commit. The broker knows T from the token's `tail` (closed, identity, digest). When applying N+1, if N+1's FIRST transaction carries T's identity, the producer computes its digest; equal → the transaction is dropped (`TxBegin`…`TxCommit`); different → refuses `BROKER-OVERLAP-MISMATCH` (an identity that names two different change sets is a reader or chain defect, never a skip). Only the leading transaction is eligible, and only against `tail`, so a TxID recurring anywhere else (impossible by the identity premise) can never cause a drop.
- **The digest** is over the transaction's changes in order, each through `applymarks.ChangeDigest` (kind + table + key + images, already value-family-pinned by ADR-0190), so it is the same canonical form the tripwire uses.
- Neither rule applies to an incremental without identities (§3.5 decides what happens then).

### 3.4 The marks themselves: unchanged, with four obligations

`internal/applymarks` and the engines' mark SQL are untouched. What the broker owes them:

1. **Identity reaches the applier.** The producer passes decoded `ApplyID`s through `rewritePosition` (it rewrites only the position). The scope digest is the applier's `rowFilterHash`, empty for the broker — unchanged.
2. **Every broker cold start clears the stream's marks** through `ir.ApplyMarksClearer`, before anything is applied: `coldStartReset` (`broker.go:791`) and `coldStartAtChainID` (`:908`). Today there is nothing to clear; under this design a stale mark naming T could be trusted if a later incremental's leading transaction is T. Chain restore clears `ChainRestoreStreamID`'s marks at its start for the same reason (a failed restore re-runs from scratch).
3. **The `--reset-target-data` hand-off adopts the restore's open-tail marks.** The cold start runs a `ChainRestore` under `ChainRestoreStreamID` (`chain_restore.go:60`, `:1196`) and then writes the broker's position. If the chain's last incremental ends with a severed T, T's marks are under the restore's stream id and the broker's first incremental opens with T: without them T's prefix duplicates. A new optional applier surface, `ir.ApplyMarksAdopter.WritePositionAdoptingMarks(ctx, streamID, pos, fromStreamID)`, writes the broker token and `UPDATE … SET stream_id = $broker WHERE stream_id = $restore` in ONE target transaction. `ChainRestore` reports the chain's tail (closed/open, identity, digest) for the token's `tail`.
4. **`--at-chain-id` derives `tail` from the chain**, not from the target: the broker re-reads the asserted incremental's stream. If it ends with a severed transaction that touches a keyless table, the broker cannot know which of its rows the target holds (no marks were written by the broker) and refuses `SLUICE-E-BROKER-KEYLESS-TABLE` with that reason; otherwise the closed-tail rule applies as on a warm start.

### 3.5 What happens to the keyless refusal

Lifted for an incremental that can prove exactly-once; KEPT, per incremental and per change, where it cannot. The door moves from "the table is keyless" to "a keyless change would be replayed without exactly-once evidence". It still refuses before anything of the incremental is applied, and it still fires at start for the in-progress and next incremental.

| Case | Verdict | Why |
|---|---|---|
| keyless table, incremental `ApplyIdentity: true`, marks usable, target commits atomically | **replayed** | §4 |
| incremental without `ApplyIdentity` (older writer, non-stamping reader, smart-compacted) | **refused** for keyless tables it touches | no identity: neither the crash nor the overlap can be deduped |
| a keyless change with a zero `ApplyID` inside an identity incremental (VStream interleaved groups) | **refused** at stream time, before it is emitted (`BROKER-KEYLESS-NO-IDENTITY`) | the manifest flag cannot see per-change gaps |
| marks unavailable on the target (`APPLY-MARKS-UNAVAILABLE`) | **refused** for keyless tables — the broker turns the WARN into the refusal | the evidence store is missing |
| vtgate MULTI target with a `--control-keyspace` sidecar | **refused** for keyless tables | GC-41 (c): a tear leaves rows without marks — at-least-once by design |
| sharded Neki target | **refused** for keyless tables until the Neki Tier-3 pass verifies cross-shard-group atomicity (Q5) | the premise ADR-0190 left UNVERIFIED (`postgres/apply_marks.go:20`) |
| a target keyed only on an unsupplied surrogate, or a multi-shard vtgate keyspace whose primary vindex the rows do not supply | **refused, unchanged** | the mark's key digest is computed from the TARGET key; when the rows do not carry it, `changeKeys` yields nothing and the change applies unmarked (`applymarks.go:330`–`335`). Marks cannot help; `ProbeReplayKey` stays the judge |
| `--at-chain-id` whose asserted incremental ends severed inside a keyless transaction | **refused** | §3.4 (4) |

`BROKER-INCREMENTAL-PARTIAL` stays, reworded: on an identity incremental the re-run is exactly-once and the message says so; on an incremental without identity it keeps today's advice (`--reset-target-data` for a source that changes keys).

## 4. Per crash point (identity incremental X, marks usable, atomic target)

Let T_1…T_n be X's transactions, P the parent. "Marks" are ADR-0190's; the skip rule's (i)–(iii) are amendment C's.

| # | Crash point | Persisted | Durable marks | Re-run |
|---|---|---|---|---|
| C1 | before X's first boundary committed | P (or the previous incremental's tail) | none of X's, or a severed T_1's (§3.3 A) | streams X from event 0; T_1 first; its marks trusted |
| C2 | serial, mid T_k (a cap flush committed a prefix) | `X through e(T_{k−1})` | T_k's prefix's (serial writes every marked class) | drops ≤ e; T_k first; marked changes ≤ mark skipped, the rest idempotent: **exactly-once** |
| C3 | lanes, post-commit window: T_k…T_m data committed, checkpoint at `e(T_{k−1})` | `X through e(T_{k−1})` | only T_k's: any barrier persisted its own transaction's start before writing marks (drain + pre-apply checkpoint, or amendment E's fold), so a barrier in T_j > T_k would have moved the position to T_j | drops ≤ e; T_k first; its barrier marks skip what landed; T_{k+1}…T_m carry no barrier (else the position would be past T_k), so they are PK-only or secondary-unique lane changes: idempotent, or amendment C's documented LOUD unique collision — never a skip |
| C4 | after the last `TxCommit`'s position, before `writePositionDirect(X)` | `X through e(T_n)` | an open severed tail's, if X ends severed | drops all committed; replays only a severed tail (marks trusted: it is first); then writes P = X |
| C5 | inside `writePositionDirect` / the adoption hand-off | one transaction | — | before or after, never between |
| C6 | no crash: overlap (A) / (B) | — | — | §3.3 |
| C7 | between `applySchemaDeltas` and X's first boundary | P | — | deltas re-applied: their idempotency is the existing claim at `broker.go:1200`–`1202` (not re-derived here) |
| C8 | a torn commit (vtgate sidecar, Neki) | — | — | keyless refused there (§3.5); keyed tables as `sync start` |

**F-E1-KEY-REUSE-REPLAY, proved per crash point.** The harm needs a key-CHANGING update replayed onto a state where its key was since reused. A key-changing update is a PK-changing update: marked on both keys on the serial paths, and a barrier — drain, persist its own transaction's start, write marks — on the lanes.

- *Across transactions* (C2–C4): a re-run never re-delivers a transaction before the persisted token, and the token is at least the start of the last transaction containing a key change (serial: every `TxCommit` persists; lanes: every barrier persists its own start). So no key change from a committed earlier transaction is ever replayed, which is the whole of the measured `[UPDATE 1→2] [DELETE 2] [UPDATE 3→1]` shape when its statements are separate transactions.
- *Within the re-delivered transaction T* (all three in one transaction): each PK change carries a mark on both keys at its ordinal. Replaying `UPDATE 1→2` (seq 1) onto `{1 (the old 3)}` finds key 1's mark at seq 3 (from `UPDATE 3→1`) — same transaction, trusted, 3 > 1 → **skipped**; `DELETE 2` (seq 2, PK-only, unmarked but checked) finds key 2's mark at seq 1 < 2 → applied, a no-op; `UPDATE 3→1` (seq 3) finds its own mark, equal ordinal and digest → skipped. Target `{1}`: converges. `[INSERT 1, UPDATE 1→2]` replayed after both committed: `INSERT 1` finds key 1's mark at seq 2 → skipped; `UPDATE 1→2` skipped on its own mark → no 23505. On the lanes the same holds: the PK changes are barriers in global order with their marks, the PK-only changes between them CHECK the marks (amendment C (ii)).
- *Residual*: an incremental without identities. There the frontier still confines a re-run to the in-flight transaction, so KEY-REUSE narrows from "any key move in the incremental" to "a key move and its reuse inside ONE interrupted source transaction", and the `BROKER-INCREMENTAL-PARTIAL` advice stays for it.

`TestApplier_KeyChangingReplay_KeyReuseIsSilent` (an APPLIER-level characterization of identity-less replay) stays as it is; the broker-level `TestFE1_Broker_KeyChangingIncremental_RerunRefusesLoudly` flips to "converges" on an identity chain and keeps a no-identity twin that still refuses.

## 5. Chain restore, restore re-runs, and ROTATED-SEGMENT-OVERLAP

- **Chain restore in-run overlap (§1.4).** Chain restore streams original source positions under `ChainRestoreStreamID`, one `ApplyBatch` per incremental (`chain_restore.go:1196`), so with recorded identities shape (A) dedupes through marks with no restore change at all, and shape (B) through the shared producer helper (the tail kept in memory between incrementals — a restore never resumes). Restore needs no frontier: it re-runs from the beginning on failure.
- **Chain-restore and single-restore RE-RUNS stay with the v0.156.11 door** (`SLUICE-E-RESTORE-KEYLESS-TABLE-NOT-EMPTY`). A re-run starts from the full, whose rows carry no identity, onto a target that already holds them; identities cannot help, and the door is the right answer. The chain-restore cold start clears its stream's marks first (§3.4 (2)).
- **ROTATED-SEGMENT-OVERLAP: identities do NOT dedupe it, and the task's premise needs a correction.** The rotation overlap is not "the same source change in two segments". Segment N's incrementals end at P_N; segment N+1's FULL is a snapshot at S > P_N and its incrementals replay (P_N, S] (`rotationBoundaryResumeStart`, `stream.go:1246`–`1272`) onto a target that the full already brought to S. The (P_N, S] changes appear ONCE as changes; their effects appear a second time inside a snapshot whose rows carry no identity. The other three sources the backlog lists are the same shape: a resumed full's re-streamed tables (snapshot rows), a compacted chain's (P_N, S] overlap (the same rotation overlap, kept by compaction), and ADR-0113's reconcile (a DataOnly snapshot re-apply). Snapshot-versus-change overlap is the snapshot→CDC hand-off problem; per-change identity has nothing to compare against. That item's own suggested fix (judge the target's key supply on EMPTY tables too) stands; this ADR does not close it. **Only §1.4's change-versus-change overlap is an identity problem, and it is new (§11).**

## 6. Compatibility

| Situation | Behaviour |
|---|---|
| An old chain (no `aid`, no `ApplyIdentity`) on a new broker | frontier applies (it needs no identity): a re-run re-delivers only the in-flight transaction. Keyless tables stay refused (§3.5). |
| A chain mixing old and new incrementals (a binary upgraded mid-chain) | judged per incremental. The first new incremental after an old one cannot dedupe a leading overlap with it (the old copy has no identity): the producer refuses a keyless change in a leading transaction that follows an old incremental ending severed; a closed (B) overlap there is undetectable and stays at today's behaviour for keyed tables. |
| An old target mark table | none: the table is ADR-0190's, unchanged; created by `EnsureControlTable` when absent; unavailable → §3.5's refusal. |
| A target holding a partially applied incremental from v0.156.11 at upgrade | its token is a classic `backup-broker` token at the parent with no `in_progress` and no marks. The new binary re-applies that whole incremental once, with no marks for it — today's behaviour, for one incremental. Keyless tables cannot be in that state: v0.156.11 refused them before applying anything. A run older than v0.156.11 could have left keyless duplicates; nothing on the target records that, so it is undetectable and is named a residual. The new binary writes v2 tokens from then on. |
| Downgrade to v0.156.11 or older | a `backup-broker-v2` token is refused by every older binary as "owned by a non-broker writer" (`isBrokerToken` keys on the `_engine` field) — loud, though the message misattributes the cause. Recovery: `--at-chain-id=<last fully applied id>` (the operator's assertion; a keyless table touched by a severed tail is then the operator's risk, which the older binary's own door would refuse anyway). Without the sentinel change an older binary would read `last_applied_backup_id`, re-apply the in-flight incremental with no marks, and — pre-v0.156.11 — duplicate keyless rows silently. |
| Old binaries reading a NEW chain | `aid` ignored (plain `json.Unmarshal`), `ApplyIdentity` ignored: they replay as they do today. |
| `TestReplayPathsNeverFold`'s second reason ("no replay-path file mentions `ApplyID`") | deliberately becomes false. Amendment D's fold stays unreachable from the replay paths by its FIRST reason (`ExactlyOnceLanes` is set only in `streamer_run_phases.go`), so the gate is rescoped, not deleted (§9 P14). |

## 7. Scope and siblings

| Path / implementor | Verdict |
|---|---|
| `sync from-backup` warm resume, serial (`--apply-concurrency 1`) | IN: frontier + marks + overlap dedupe |
| `sync from-backup` warm resume, lanes (default W = 4) | IN: same; barrier marks by default; secondary-unique lane marks NOT written (amendment C; `--exactly-once-lanes` is not plumbed to the broker — Q7) |
| `--reset-target-data` cold start | IN: clears marks; chain restore; mark adoption + `tail` in one transaction (§3.4 (3)) |
| `--at-chain-id` cold start | IN: clears marks; `tail` derived from the chain; severed-keyless refusal (§3.4 (4)) |
| chain restore (`ChainRestore.Run`) in-run | IN: marks (A) + shared tail drop (B); clears its marks at start |
| chain restore / `restore` RE-RUN | OUT: v0.156.11 door, unchanged (§5) |
| single-full `restore` | n/a: no change chunks |
| backup capture writers: `BackupStream.captureWindow`, `IncrementalBackup.captureWindow`, ADR-0067 rotation `skipThrough` | IN: record `aid`; set `ApplyIdentity` iff the reader declares `ir.ApplyIdentityProvider` |
| naive compaction | IN by construction: moves bytes verbatim; carries `ApplyIdentity` through to the merged manifests (a pin, P11) |
| smart compaction | OUT deliberately: clears `ApplyIdentity` and strips `aid` on every incremental it rewrites (keyless replay of those is refused; Q9 offers a finer rule) |
| `backup verify` / `verify` | unchanged; the codec pin is their concern |
| MySQL / PlanetScale / Vitess target (unsharded or `--control-keyspace`-less) | IN |
| Vitess with `--control-keyspace` sidecar | keyed tables IN; keyless refused (§3.5) |
| Postgres target | IN |
| Neki target | keyed IN; keyless on a sharded Neki refused pending Q5 |
| SQLite / D1 target | n/a: `OpenChangeApplier` refuses, so neither the broker nor chain restore reaches them |
| readers: MySQL GTID / file/pos / MariaDB / Postgres / VStream / postgres-trigger / sqlite-trigger / d1-trigger | each stamps per ADR-0190 phases 1, 4, 5; a non-stamping reader's incrementals get no `ApplyIdentity` |
| `TestApplyIdentityEngineListMatchesTheCode` (docsync stamping roster) | the codec's decode assigns `ApplyID`; it must be classified as a PASS-THROUGH, not a stamper, or the roster fails |

## 8. Performance and measurement plan

**Expected cost.**

- **Frontier: zero target writes.** The tokens replace writes that happen today with the parent token; the producer pays one ordinal counter and a token encode per boundary (a pre-encoded prefix plus an integer).
- **Marks:** identical to `sync start` on the same apply path. Serial: one coalesced UPSERT per marked key per batch, one DELETE per `TxCommit` position write that closed a marking transaction. Lanes: one extra statement in each barrier's own transaction (amendment E removes the barrier's separate checkpoint when it lands). PK-only work writes no marks.
- **Mark load:** one `SELECT` per incremental (`ApplyBatch` per incremental).
- **Chunk size:** one `aid` per row change. A GTID TxID is ~45 bytes, repeated on every row of a transaction, so gzip/zstd should absorb most of it; the uncompressed JSONL grows by roughly 55–70 bytes per row. UNMEASURED.
- **Restart:** one re-read and decode of the in-progress incremental's prefix (no apply), bounded by one incremental.

**Measurement plan** (`benchmarks/fencebench/`, Docker only). Add a `brokerbench` arm: run `backup stream` against the fencebench source for workloads A (secondary-unique), B (PK-only), D (PK-changing), and a new K (keyless, 100% inserts with 10% keyless deletes), producing one chain per workload with a fixed `--max-changes`; then time `sync from-backup` replay of the whole chain into a fresh target, base (`98ad6ec6`, keyless arm skipped since base refuses it) versus head, serial and lanes, three reps; `fb verify` (a full-row SHA-256 of source versus target, read by the harness) in every cell. Also report compressed and uncompressed chunk bytes per workload, base versus head, and a resume cell: kill the broker at 50% of a 100k-change incremental, time the restart to its first new commit.

**Acceptance:** B within ±5% of base on both arms (no marks, no new writes); D and A within ±10% on serial; compressed chunk growth ≤ 10% on every workload (above that, revisit the encoding before landing — e.g. a per-transaction `aid` on `TxBegin` with per-row `Seq` only, at the cost of a stateful decoder); `VERIFY OK` everywhere, including K, which base cannot run. A miss goes through the three-phase protocol, not tuning.

**Perf-parity:** reaches the broker and chain-replay cells; the backup capture cells pay the chunk growth.

## 9. Pins, each with its mutation

Every mutation follows CLAUDE.md's protocol (checkpoint commit immediately before, `grep` the mutant, read the red to confirm the named assertion fired, revert by targeted edit).

| # | Pin | Grades | Mutant it must catch | Reverse |
|---|---|---|---|---|
| P1 | `TestChangeChunk_ApplyIDRoundTrip` (unit, family matrix) | `aid` round-trips byte-exact for every reader's TxID shape, `Seq` above 2⁵³, absent `aid` decodes to the zero identity | decode `Seq` through `float64`; drop `aid` on update records | an old-format chunk decodes with zero identities |
| P2 | `TestBackupCapture_RecordsTheReaderIdentity` (real servers, every stamping reader) | the `aid` in a captured chunk equals the identity the same reader stamps on a re-delivery from the same boundary — the binding of §3.1's backup-path premise | the capture path re-numbers `Seq` after a filter | — |
| P3 | `TestBroker_FrontierTokenAtEveryBoundary` (unit, fake applier) | rows carry their transaction's start, `TxCommit` and out-of-transaction changes carry their own ordinal, ordinals count the RAW stream | stamp rows with their own ordinal; count after the tail drop | a marker-less chunk: every change its own boundary |
| P4 | `TestBroker_ResumeSkipsThroughTheFrontier` | a token `through e` emits exactly events > e, and still verifies every chunk | off-by-one (skip through e−1 / e+1); skip without the chunk SHA check | — |
| P5 | `TestBroker_RewrittenIncrementalRefuses` (unit + a smart-compacted chain) | a changed chunk list, or a shorter stream, refuses `BROKER-INCREMENTAL-REWRITTEN` | compare the digest of chunk FILE names instead of SHAs (naive-compaction move passes, smart passes too) | a naively compacted chain (moved files) resumes |
| P6 | crash suite `TestBroker_CrashMidIncremental_*` (every source family × PG/MySQL target × serial/lanes; kill inside T_k by a target row lock, as `runCrashMidTxnSuite` does) | at the kill the token is `X through e(T_{k−1})`, marks name T_k only; the re-run converges to the SOURCE's final state (`SELECT … ORDER BY`, read from the source — the independent expected value) with a keyless table, a PK-changing transaction and a secondary-unique table in the incremental | write the parent token (today's BRK-1): T_k's marks are distrusted and T_1…T_{k−1}'s keyless rows duplicate, failing convergence; stamp rows with their own ordinal (a mid-transaction token): the "token is `X through e(T_{k−1})`" assertion fails | anti-vacuity: at least two committed transactions before the kill, and a keyless row among them |
| P7 | `TestBroker_KeyReuseIncremental_Converges` | the measured F-E1 shapes `[INSERT 1, UPDATE 1→2]` and `[UPDATE 1→2, DELETE 2, UPDATE 3→1]` (each as one transaction AND as three), killed at every statement, converge on both engines | strip `aid` in the producer (no-identity twin must still refuse/loud) | the no-identity twin keeps today's outcome |
| P8 | `TestBroker_SeveredTailOverlap_Deduped` (real servers) | a chain made by `backup stream stop` landing mid-transaction (keyless + PK-change rows) then resumed; broker AND chain restore converge to the source | delete the open tail's marks in `writePositionDirect` | the same chain without identities: keyless refused by the broker, and the restore arm documents §11's duplicate |
| P9 | `TestBroker_ClosedTailOverlap_DroppedOrRefused` (unit) | a leading transaction equal to `tail` is dropped; same identity with a different digest refuses `BROKER-OVERLAP-MISMATCH`; a non-leading recurrence is never dropped | drop on identity alone; drop any recurrence | a leading transaction with a different identity is kept |
| P10 | `TestBroker_ColdStartsClearAndAdoptMarks` (real servers) | both cold starts clear the stream's marks; the reset hand-off moves the restore's open-tail marks and writes the token in one transaction (fail at a commit hook: neither lands) | adopt in a separate transaction; skip the clear | — |
| P11 | `TestCompaction_ApplyIdentityFlag` | naive keeps `ApplyIdentity` and `aid`; smart clears both on rewritten incrementals | smart keeps the flag | — |
| P12 | `TestBrokerKeylessDoor_PerIncrementalMatrix` | every §3.5 row, judged before anything of the incremental is applied | lift on "table keyless" alone (the old door, inverted) | the replayed row of the table |
| P13 | `TestBrokerToken_DowngradeIsLoud` | a v2 token is refused by the v0.156.11 decoder (a frozen copy of `isBrokerToken` from that tag, NOT the current one — a fixture built from post-change values would be self-referential) | write `backup-broker` while `in_progress` is set | a v2 token at a clean boundary is still refused |
| P14 | `TestReplayPathsNeverFold` rescoped and renamed `TestReplayPathsNeverFoldAMarkFence` (amendment E's E-Q6 asked for that rename already) | reason 1 only; the ApplyID reason replaced by `TestReplayIdentityIsPassThrough` (no replay-path file ASSIGNS an `ApplyID` other than the codec's decode) | plant an `ApplyID{…}` literal in `broker.go` | the decode site is the one allowed assignment |

## 10. `-race`

New concurrency: none in the applier; the producer goroutine gains the ordinal counter and the tail bookkeeping (confined to it, as `lastApplied` already is), and the lane coordinator sees distinct boundary tokens where it saw one. It changes crash-recovery ordering on the broker's default lane path, so by CLAUDE.md it is a concurrency chunk: CI's `-race` Integration job green before any tag.

## 11. Findings made on the way (pre-existing; none introduced by this design)

- **F-E1-SEVERED-TAIL-REPLAY — (2026-10-05: CONFIRMED HIGH by repro and FIXED, unreleased: windows no longer end inside a source transaction, and old chains of either shape are refused with `SLUICE-E-BACKUP-CHAIN-SEVERED-TRANSACTION`; see docs/dev/audit-backlog.md. The text below is the original filing.) HIGH candidate, silent at exit 0, by code reading, UNMEASURED; the grade is a hypothesis until a repro.** Consecutive incrementals of one segment share a source transaction in two shapes (§1.4), and the broker and chain restore apply both copies with no crash anywhere. **(A) severed tail, every source engine:** `backup stream stop` (`stream.go:1788`–`1816`) or a channel close (`:1817`–`1828`) commits a window inside a transaction; the resumed window re-delivers all of it. **(B) closed tail, Postgres chains written by binaries before v0.138.0:** every resumed window (each one-shot `backup incremental`, each restarted `backup stream`) re-delivered its parent's last committed transaction (the A2-1 mechanism, measured for sync in the v0.138.0 CHANGELOG). Harms: a keyless row in the shared transaction is restored twice by **chain restore into an empty target** (the restore door judges only non-empty tables) — the broker refuses keyless tables since v0.156.11; and a key-reusing shared transaction misapplies on BOTH the broker and chain restore exactly as F-E1-KEY-REUSE-REPLAY does (`[UPDATE 1→2, DELETE 2, UPDATE 3→1]` replayed onto its own end state empties the table), with no crash. The written invariant that hides it: "chain restore replays them idempotently" (`stream.go:1655`–`1657`, `incremental.go:1124`–`1126`), true for PK-only inserts and same-key updates only. **Fix:** this ADR's §3.3 for chains with identities; for old chains, at minimum the comments, and a decision on whether chain restore should refuse a keyless table touched by an incremental whose predecessor ended severed (structurally detectable: the predecessor's stream ends inside an open `TxBegin`) — Q8. **Repro needed:** a PG and a MySQL GTID chain with a stop landing mid-transaction over a keyless table and a key-reusing transaction, restored and brokered; and a pre-v0.138.0 PG chain (build the old binary) restored.
- **The ROTATED-SEGMENT-OVERLAP backlog entry's mechanism is snapshot-versus-change** (§5), so "give broker changes an apply identity" does not close it — the entry's own empty-table supply judgment is the fix. Recorded as a correction of a premise, not a new defect.
- **LOW, doc drift:** `ir.ApplyID`'s doc says "a backup chunk replay" leaves the identity zero (`change.go:179`–`181`); BRK-1's comments in `broker.go` describe the parent token as the only token. Both change with this design and are rewritten by it.

## 12. Open questions for the operator

**Answered (operator, 2026-10-05).**
- Q1: option (c) is approved.
- Q4: narrow the keyless door per incremental, as tabled.
- Sequencing (Q8, Q10): fix F-E1-SEVERED-TAIL-REPLAY first. It was reproduced on 2026-10-05 and is wider than filed: MySQL file/pos chains LOSE rows, so a producer-side fix is needed, which this ADR cannot supply. Then land ADR-0190 Amendment E. Then this ADR.
- Q2, Q3, Q5, Q6, Q7 and Q9 take the recommendations.

The questions as put:

- **Q1. Option (c) with the recorded source identity?** Recommendation: **yes**. (a) is unsound (§1.5); (b) leaves the in-flight transaction at-least-once and cannot lift the keyless refusal; chunk-derived identities cannot dedupe §1.4's overlap.
- **Q2. Format: an additive `aid` field plus a manifest `ApplyIdentity` flag, no `FormatVersion` bump?** Recommendation: **additive, no bump.** A bump would make older binaries refuse new chains for a field they can safely ignore; the flag, not the version, is what the broker judges.
- **Q3. Token sentinel `backup-broker-v2`, making a downgrade loud?** Recommendation: **yes**, always written by the new binary (not only while an incremental is in progress: a clean token can still sit before a severed tail whose marks an older binary would ignore). The cost is a misattributed refusal on downgrade, recovered with `--at-chain-id`.
- **Q4. The per-incremental keyless door of §3.5 (lift where proven, refuse where not)?** Recommendation: **yes**, as tabled.
- **Q5. Sharded Neki targets:** lift keyless under ADR-0190's UNVERIFIED cross-shard-group-atomicity premise (as `sync start` does), or refuse until the Neki Tier-3 pass? Recommendation: **refuse keyless on a sharded Neki target** until that pass; the broker's door already distinguishes target shapes, and the cost of being wrong is silent duplication.
- **Q6. Chunk-derived identities as a fallback for OLD chains** (exactly-once for a crash inside one incremental; no overlap dedupe, so keyless stays refused)? Recommendation: **no** — a second identity scheme to maintain for chains that age out, buying only the narrow "key reuse inside one interrupted source transaction" residual of §4.
- **Q7. `--exactly-once-lanes` for the broker** (secondary-unique lane marks)? Recommendation: **not now.** The broker inherits amendment C's default; the in-flight transaction's secondary-unique collision stays loud. Revisit with amendment D's measurements if an operator asks.
- **Q8. F-E1-SEVERED-TAIL-REPLAY:** file as its own HIGH item, reproduce first (§11), and decide the old-chain answer then? Recommendation: **yes**; the chain-restore keyless arm needs a structural refusal for old chains, and the key-reuse arm on old chains can only be documented.
- **Q9. Smart compaction:** strip identities on every rewritten incremental (this ADR), or keep `aid` on pass-through events (keyless tables are never collapsed — `chain_compact_smart.go:53`–`56`) and strip it only on collapsed ones? Recommendation: **strip all, for now.** A partially identified incremental needs its own argument about collapsed events sitting inside identified transactions; the conservative rule re-imposes a loud refusal and costs nothing on chains that are not smart-compacted.
- **Q10. Sequencing against amendment E:** amendment E (the barrier fold) was sequenced behind "the F-E1 broker fix", which v0.156.11's door satisfied. Recommendation: **land amendment E first**; this design's lane cost claims assume the barrier's single commit, and §4 C3's argument is the same under either.

## Alternatives considered

- **(a) Marks alone, incremental-scoped** (keep marks until the incremental ends; trust any transaction of the incremental). Rejected: per-key marks name only the last transaction to touch a key (one row per keyless table), so an earlier transaction's replay can prove nothing without an order on transaction ids — ADR-0190 amendment A's rejected option B (§1.5).
- **(b) Frontier alone.** Rejected as the whole fix: exactly-once only between transactions; kept as half of (c).
- **Net-effect replay of the in-flight incremental** (delete every touched key, write final images). Rejected for ADR-0190 §7's reasons: `ON DELETE CASCADE` into untouched rows (silent), partial TOAST images, memory.
- **Re-chunk the broker's work as one target transaction per incremental.** Rejected: unbounded transactions, no lanes, and it does nothing for §1.4's overlap.
- **Filter the overlap at backup time** (drop the re-delivered events when writing N+1). Rejected: the window writer keeps them deliberately — they are the only copy of a severed tail (`stream.go:1655`–`1666`) — and filtering at capture cannot fix existing chains.
