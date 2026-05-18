# ADR-0050 — Reconciling / incremental re-snapshot for CDC position-loss recovery

**Status:** **Proposed (design-first; sign-off pending).** Design pass
*before* code; from the Track-1 PlanetScale/Vitess readiness
investigation and an operator-reported cost/outage pain (design
evidence:
[`docs/dev/notes/prep-planetscale-vitess-readiness.md`](../dev/notes/prep-planetscale-vitess-readiness.md)
§"Phase 1c"). **Proposed → dialogue → Accepted** via the decision
points below. **Independent of Roadmap #4 (#37)** — recovery-path, not
multi-source. Refines the recovery behaviour of
[ADR-0022](adr-0022-position-invalid-coldstart.md) (cold-start
fall-through) / [ADR-0023](adr-0023-reset-target-data.md)
(`--reset-target-data`); pairs with
[ADR-0049](adr-0049-cdc-schema-history.md) (the schema version applied
to re-selected rows must be the history-resolved version *as of the
re-snapshot watermark*).

**Dialogue progress (2026-05-18, owner session).** Direction
**endorsed** by the owner — worthwhile to pursue, with real cost data
to be gathered during testing to confirm it stays worth its weight
(see Consequences: this is source-heavy by nature; the empirical
validation is an explicit gate, not a formality). **DP-1 resolved**
(recorded below). New **DP-5** added (checksum compute-location) out
of the storage/egress sub-thread. DP-2/3/4/5 remain open; status stays
**Proposed** until those are signed off. Still demand-gated: do not
implement ahead of a real operator hitting the position-loss pain.

## Context

When a CDC position becomes unrecoverable — binlog purged /
`@@gtid_purged` advanced past the resume set / a node
replaced-or-restored-from-backup with no carried binlog history (the
PlanetScale 3-day-retention + node-replacement class) / a reshard
restart — sluice's `verifyPositionResumable` correctly wraps
`ir.ErrPositionInvalid` and the orchestrator falls through to a
**cold-start re-snapshot (ADR-0022)**. Today that re-snapshot is a
**full re-copy** (Phase 1c is characterising the exact granularity;
cold-start additionally guards a populated target, so recovery
typically needs `--force-cold-start` / `--reset-target-data` + a full
re-copy). For a large table that is the operator-reported pain:
multi-day downtime **and** real PlanetScale **egress cost** to re-ship
rows the target already holds correctly.

sluice already persists the two durable primitives this needs: the CDC
position (`sluice_cdc_state`, ADR-0007) and the bulk-copy PK cursor
(`sluice_migrate_state.table_progress`), and already requires a
PK/unique key for resumable copy and CDC apply. PlanetScale
[`binlogsrv`](https://github.com/planetscale/binlogsrv) corroborates
the GTID-first persisted-state resilience pattern (and suggests a
*complementary* mitigation — see Alternatives).

## Decision

Replace the full re-copy on position-loss recovery with a **reconciling
re-snapshot**: ship only the delta between source and target,
consistently with the resumed CDC stream, per-table, resumable per
chunk.

**Mechanism = DBLog watermark-based chunked select** (Netflix DBLog;
[blog](https://netflixtechblog.com/dblog-a-generic-change-data-capture-framework-69351fb9099b),
[arXiv 2010.12597](https://arxiv.org/abs/2010.12597); a local copy is
in `C:\code\sluice-useful-papers` — implementers verify the exact step
sequence against it) **plus a sluice-specific chunk-fingerprint skip
gate** (the contribution beyond DBLog).

DBLog watermark algorithm (as understood; authoritative reference is
the saved paper):

1. Own a **dedicated single-row watermark table**; process the
   change-log single-threaded with an in-memory buffer.
2. Per chunk (next PK-ordered window of size N; cursor = prior chunk's
   last PK): (a) pause log consumption; (b) write a unique UUID to the
   watermark table = **LOW watermark** (a recognisable log event); (c)
   run the chunk `SELECT … WHERE pk > :cursor ORDER BY pk LIMIT N`; (d)
   write another UUID = **HIGH watermark**; (e) resume log consumption.
3. Stream log events: discard until LOW; between LOW and HIGH, record
   the **set of PKs changed in the log** (rows modified concurrently
   with the chunk SELECT); at HIGH, **drop from the in-memory chunk
   result any row whose PK is in that set** — the **change-log wins**
   for concurrently-modified rows. Emit the deduplicated chunk as
   snapshot events; continue log streaming.
4. Progress = the persisted PK cursor → lock-free, no log pause beyond
   the brief in-memory pause, **pausable/resumable, concurrent with
   CDC**. Requires a PK and watermark-table write access.

**sluice chunk-fingerprint skip gate (beyond DBLog):** DBLog still
*selects and ships every chunk*. For position-loss *recovery* the
target already holds most rows correctly, so before the DBLog
select+ship for a PK-range chunk, compute a **block fingerprint of
that range on source and target** (per DP-1 below: a strong per-row
hash over `(PK ‖ value-types.md-canonical column values)`, combined as
a single running hash over the PK-ordered chunk stream — *not* a
cheap XOR/CRC; the skip-decision is asymmetric, a false *match* is
silent data loss). Match → **skip the chunk entirely** (already
correct in the target — zero rows shipped, zero egress). Diverge →
run the DBLog watermark select+reconcile and apply **only the delta**.
Target-only PK ranges → **deletes**. Net: recovery moves only the
delta, stays consistent with the resumed CDC stream, is per-table and
resumable per chunk. **Graceful degrade:** a table with no usable PK →
full re-copy *of that table only* (sluice's existing no-PK
truncate-and-redo), never whole-dataset.

**Storage model (clarified 2026-05-18).** Fingerprints are
**ephemeral** — computed per side at recovery time, compared, and
discarded; never persisted (a stored hash is stale the instant CDC
applies the next change — freshness is the point, and a hash store's
invalidation cost exceeds the bytes saved). The only durable state is
the **recovery progress cursor** (which PK ranges are
verified/reconciled, for resumability), which lives in sluice's
**target-side `sluice_*` control tables** alongside
`sluice_migrate_state.table_progress` (sluice's control tables are
per-target; verified against the codebase). The DBLog **watermark
table** is the one piece on the **source** (its LOW/HIGH writes must
appear in the source change-log). A storage-efficient columnar hash
store only earns its keep for the *proactive drift-audit* variant
(DP-4, out of v1 scope) — noted there, not built for v1.

The loud floor is unchanged: if reconciliation cannot be performed
safely, fall back loudly to ADR-0022/0023 — never a silent partial
recovery.

## Decision points requiring sign-off

1. **Fingerprint function & collision tolerance** — **RESOLVED
   (2026-05-18).** The skip-decision error is asymmetric: a false
   *diverged* costs a needless chunk reconcile (harmless); a false
   *match* skips a divergent chunk = silent data loss (the Bug 74/75
   cardinal sin under the loud-failure tenet). So the gate is tuned to
   make false-*match* effectively impossible, false-*diverged* may be
   sloppy. Decision: **per-row `SHA-256(PK ‖ canonical column values
   per `docs/value-types.md`)`**, combined as a **single running
   SHA-256 over the PK-ordered chunk stream** (the DBLog chunk SELECT
   is already `ORDER BY pk` on both sides — so an ordered running hash
   is free and deletes the order-independent-fold collision surface
   entirely; no XOR/CRC, no near-boundary epicycle). Stored/compared
   truncated to 128 bits. Rationale over xxh128/BLAKE3: cryptographic
   collision-resistance by construction (no structured/compensating-
   edit weakness to argue about), **zero new dependency** (Go stdlib
   `crypto/sha256`, SHA-NI-accelerated — speed is the secondary axis
   on a recovery scan), frozen spec (stable across a version-upgraded
   resume). Canonicalization MUST be exactly the value-types.md
   contract — over-normalizing could mask a real diff (→ false match →
   loss); under-normalizing only causes harmless false-diverged.
   **BLAKE3 is the documented escalation** *iff* profiling shows
   SHA-256 bottlenecks recovery on non-SHA-NI hardware (take the dep
   then, not speculatively).
2. **Chunk sizing / adaptivity** and watermark-table provisioning
   (sluice creates/owns it like the other `sluice_*` control tables).
   *(open)*
3. **Consistency contract with [ADR-0049](adr-0049-cdc-schema-history.md)**
   — the schema applied to re-selected rows must be the
   position-anchored version as of the chunk's watermark, not "now".
   *(open)*
4. **Recovery-only vs. also proactive drift-verification** — v1 is
   recovery-only; using the same engine for periodic source/target
   drift audit is explicitly out of v1 scope. *(open; this is also
   where a storage-efficient columnar hash store would belong if/when
   drift-audit is promoted — not v1.)*
5. **Checksum compute-location** *(open; new — from the storage/egress
   sub-thread).* Two modes with different cost/coverage:
   **(a) push-down** — the source/target engine computes the block
   hash in SQL (`SHA2()` / `digest()`), only the 128-bit fingerprint
   crosses the wire → near-zero egress (directly the PlanetScale
   *cost* relief), but comparable only **same-engine** and depends on
   engine value-rendering (not the IR contract).
   **(b) in-sluice** — sluice reads both sides' rows and hashes over
   the value-types.md IR-canonical form → **cross-engine**-correct and
   contract-stable, but the source read is itself wire traffic / PS
   egress (still a big net win vs full re-ship). Decision needed:
   whether v1 ships (b) only (simplest, cross-engine, correctness-
   general) or both (b)+(a) (a as the same-engine egress-optimal fast
   path). DP-1's hash resolves per-mode: SHA-256-in-Go for (b);
   engine-native `SHA2(...,256)`/`digest(...,'sha256')` for (a).

## Consequences

- Position-loss recovery cost drops from O(table) re-ship to O(delta)
  + a checksum scan — directly the PlanetScale egress/outage relief.
- New: watermark table, chunk-checksum, the watermark reconciliation
  state machine in the snapshot path; must interoperate with ADR-0049
  and the existing `table_progress` cursor.
- Recovery becomes resumable and concurrent with the resumed CDC stream
  (DBLog property), shrinking the outage window further.
- **Cost honesty (vdiff-sourced).** This is **source-heavy by
  nature**: an O(table) ordered scan on every position-loss recovery,
  and on PlanetScale the source read is egress-billed. Lighter than
  full re-copy (read-only; no re-ship / no target rewrite on matching
  chunks) but *not* free. Vitess vdiff — the closest operational
  analogue — documents the same shape: comparison CPU on the serving
  primary "potentially requiring larger provisioning," and vdiff1
  large-table runs that "could take more than a day." Vitess's answer
  was *resumability + parallelism + locality + smaller snapshots*, not
  a cheap trick. Mitigations that are therefore load-bearing here, not
  optional: the `last_updated_at` pre-filter (skip unchanged rows
  before hashing, where present), scan throttling, and the DP-5
  push-down mode for the same-engine egress-sensitive case. **The
  empirical validation is an explicit gate:** real cost data gathered
  during testing must confirm reconciling-resnapshot stays cheaper
  than full re-copy for representative tables before this leaves
  Proposed — owner-endorsed direction, evidence-gated commitment.

## Alternatives considered

- **Full re-copy (status quo)** — the cost/outage pain; rejected as the
  default, kept as the no-PK / unsafe-reconcile fallback.
- **Trigger / `last_updated_at` / surrogate-key diff** (the operator's
  prior poor-man's approach) — rejected as a *requirement*: not
  universal, source-invasive; `last_updated_at` is retained only as an
  optional accelerator (skip unchanged rows before checksum) where
  present, never required.
- **`replicase/pgcapture`-style approach** — PG-logical-replication
  pub/sub relay; useful prior art but PG-only and not a
  re-snapshot/diff design; not engine-neutral. Rejected as the model;
  DBLog+checksum is engine-neutral via the `ir`.
- **`binlogsrv` relay (complementary, not a substitute)** — pointing
  sluice at a PlanetScale `binlogsrv` relay persists binlog beyond
  source retention, reducing how often position-loss recovery triggers
  at all. Noted as a future complementary operational mitigation
  (docs/runbook), orthogonal to this ADR's recovery correctness.
- **Vitess `vdiff2` (closest prior art — reviewed in
  `vitess.io/vitess@v0.24.0` `go/vt/vttablet/tabletmanager/vdiff`).**
  vdiff syncs source/target streams to a consistent **GTID snapshot**,
  then streams both sides via RowStreamer **`ORDER BY PK`** and
  merge-compares; resumable via a persisted per-table `lastPK` in a
  control table. This *validates* three choices here: ordered-stream
  compare (DP-1), the per-table progress-cursor in a target-side
  control table (storage model), and the consistent-anchor model
  (DBLog's watermark table is the **engine-neutral** equivalent of
  vdiff's VReplication-snapshot anchor — same family, no VReplication
  dependency). **Critical distinction:** vdiff does a full per-column
  value compare and **streams every row — it has no skip gate**, which
  is exactly why it is multi-hour/day on large tables. ADR-0050's
  chunk-fingerprint skip is therefore *the* load-bearing cost lever
  (the reason this is worth more than "just run a vdiff"), not an
  optional optimization. Corollary: vdiff offers **no prior art for
  the fingerprint** (it doesn't hash) — DP-1's hash design stands on
  its own reasoning, correctly.

## Status / next

**Proposed; direction owner-endorsed (2026-05-18).** DP-1 resolved;
**DP-2/3/4/5 still open** — do **not** implement before owner sign-off
on the remaining four *and* the empirical cost-validation gate
(Consequences: real testing data must show reconciling-resnapshot
beats full re-copy on representative tables). Still demand-gated on a
real operator position-loss case. Independent of #37 (pinned).
Phase-1c's empirical characterisation of the *current* re-snapshot
granularity feeds DP-3 and the Consequences. May be merged with
ADR-0049 into a single "robust CDC recovery" ADR if the owner prefers
one design.

Next dialogue rounds: DP-2 (chunk sizing + watermark-table
provisioning), DP-3 (ADR-0049 schema-version-as-of-watermark
contract), DP-4 (recovery-only vs drift-audit + the columnar
hash-store question), DP-5 (compute-location: in-sluice only vs
+push-down).
