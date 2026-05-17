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
select+ship for a PK-range chunk, compute a **block checksum of that
range on source and target** (a `pt-table-checksum`-style XOR/CRC of
per-row hashes over the PK window). Match → **skip the chunk entirely**
(already correct in the target — zero rows shipped, zero egress).
Diverge → run the DBLog watermark select+reconcile and apply **only the
delta**. Target-only PK ranges → **deletes**. Net: recovery moves only
the delta, stays consistent with the resumed CDC stream, is per-table
and resumable per chunk. **Graceful degrade:** a table with no usable
PK → full re-copy *of that table only* (sluice's existing no-PK
truncate-and-redo), never whole-dataset.

The loud floor is unchanged: if reconciliation cannot be performed
safely, fall back loudly to ADR-0022/0023 — never a silent partial
recovery.

## Decision points requiring sign-off

1. **Fingerprint function & collision tolerance** — per-PK-block
   XOR/CRC of row hashes (cheap, `pt-table-checksum`-style) vs. a
   stronger per-row hash; the false-match (skip-a-diverged-chunk) risk
   budget. Loud-failure tenet says skipping a genuinely-diverged chunk
   = silent data loss, so the function must be conservatively
   collision-resistant or the gate must verify on any near-boundary.
2. **Chunk sizing / adaptivity** and watermark-table provisioning
   (sluice creates/owns it like the other `sluice_*` control tables).
3. **Consistency contract with [ADR-0049](adr-0049-cdc-schema-history.md)**
   — the schema applied to re-selected rows must be the
   position-anchored version as of the chunk's watermark, not "now".
4. **Recovery-only vs. also proactive drift-verification** — v1 is
   recovery-only; using the same engine for periodic source/target
   drift audit is explicitly out of v1 scope.

## Consequences

- Position-loss recovery cost drops from O(table) re-ship to O(delta)
  + a checksum scan — directly the PlanetScale egress/outage relief.
- New: watermark table, chunk-checksum, the watermark reconciliation
  state machine in the snapshot path; must interoperate with ADR-0049
  and the existing `table_progress` cursor.
- Recovery becomes resumable and concurrent with the resumed CDC stream
  (DBLog property), shrinking the outage window further.

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

## Status / next

Proposed. Do **not** implement before owner sign-off on the four
decision points. Independent of #37 (pinned). Phase-1c's empirical
characterisation of the *current* re-snapshot granularity feeds DP-3
and the Consequences. May be merged with ADR-0049 into a single
"robust CDC recovery" ADR if the owner prefers one design.
