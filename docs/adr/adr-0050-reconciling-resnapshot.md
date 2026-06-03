# ADR-0050 — Reconciling / incremental re-snapshot for CDC position-loss recovery

**Status:** **Accepted (2026-05-18) — design dialogue complete: all
5 DPs + the structural question signed off & owner-endorsed.
Implementation gated on 3 NON-design conditions (empirical
cost-validation incl. the DP-2 Vitess A/B · ADR-0049 implementation ·
real operator demand); still demand-gated — see "Status / next".**
(Header reconciled 2026-05-18: the dialogue is complete; the prior
"Proposed; sign-off pending" wording was superseded.) Design pass
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
validation is an explicit gate, not a formality). **ALL decision
points resolved — DP-1–DP-5 (recorded below) plus the structural
question** (ADR-0049/0050 stay **separate but hard-sequenced**:
ADR-0049 DP-1 + Phase-1c before any 0050 implementation). The
design dialogue is **complete**. Status remains **Proposed** —
implementation is gated now only on the three non-design gates
(empirical-validation, ADR-0049-DP-1 sequencing, real operator
demand), not on any further design decision. Do not implement ahead
of a real operator hitting the position-loss pain.

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
in `sluice-useful-papers` — implementers verify the exact step
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
2. **Chunk sizing / adaptivity + watermark provisioning** — **RESOLVED
   (2026-05-18).**
   - **Chunk sizing.** Fixed-N is a footgun (sets memory + the
     LOW→HIGH log-pause window; trivial for a BIGINT table, OOM for a
     wide-blob one). **Adaptive, wall-time-targeted** (the proven
     `pt-table-checksum` model): grow/shrink N to keep each chunk's
     checksum/select ≈ `Ttarget` (default ~1s, configurable), with
     hard **row *and* byte bounds** (reuse the ADR-0028 byte-cap
     precedent; `Nmin` avoids a watermark-write storm, `Nmax` caps
     memory + pause window). This is fully sluice-controlled **only on
     the vanilla-MySQL watermark path**; on Vitess/PG the granularity
     is the stream's natural boundary (VStream `LASTPK` cadence / the
     PG message bracket), not a free N — sluice checksums per natural
     boundary there. **v1 = flat adaptive chunks; recursive
     (Merkle/`pt-table-sync`-style) bisection is the evidence-gated
     optimization** (descend to localize the minimal divergent
     sub-range only on a coarse-block miss) — promoted only if the
     testing gate shows over-shipping-on-divergence is material.
   - **Watermark provisioning is engine-asymmetric (three-way), not a
     single source table.** Verified against the codebase: sluice's
     source role is read+REPLICATION-only today, and its VStream
     filter is already `{Match:"/.*/"}` (all tables), VStream v1
     unsharded-only.
     - **PostgreSQL:** **`pg_logical_emit_message(prefix=>'sluice-wm',
       content=><LOW/HIGH uuid>)`** — native WAL marker, no table, no
       DDL; sluice's reader already sees-and-discards such messages
       (`docs/postgres-source-prep.md`). No source write footprint.
     - **Vitess / PlanetScale-MySQL:** **native VStream copy +
       `LASTPK` + VGTID is the preferred default** (`--resnapshot-
       anchor=vstream-native`) — Vitess already provides the
       snapshot-consistent chunked-copy-interleaved-with-CDC that
       DBLog reconstructs by hand; sluice already consumes it
       (`cdc_vstream_snapshot.go`, `COPY_COMPLETED`/VGTID). **No
       source write.** A `sluice_watermark` **opt-in A/B mode**
       (`--resnapshot-anchor=watermark-table`) is *also supported here*
       for empirical comparison: the `/.*/` filter means its row
       events already surface in the existing VStream (no raw-binlog
       access needed — the gating worry is answered), unsharded scope
       keeps the A/B uncontaminated, and the write→replica-visibility
       lag the native path avoids becomes a **measured output** of the
       comparison (feeds the empirical gate), not a defect. On
       PlanetScale the watermark table may need operator-pre-creation
       (PS DDL-gating) — a documented prerequisite for the A/B mode;
       the per-chunk UPDATE itself is ordinary user DML.
     - **Vanilla MySQL (not behind Vitess):** no in-binlog marker
       primitive → a sluice-owned **`sluice_watermark`** table is
       *required* (per-`stream_id` keyed, idempotent `EnsureControl
       Table`-style, UPDATE-in-place). This is the one real new
       operator cost: source `CREATE`+`UPDATE`. **Loud-refuse → fall
       back to full re-copy (ADR-0022)** when the source role can't
       write — never a silent skip; no regression vs status quo.
   Both anchor mechanisms yield the *identical* correctness contract
   (a consistent chunk bracketed relative to the resumed stream); only
   the anchoring differs, so the Vitess A/B isolates exactly the
   anchoring cost/consistency tradeoff for the evidence gate.
3. **Consistency contract with [ADR-0049](adr-0049-cdc-schema-history.md)**
   — **RESOLVED (2026-05-18).** Shared pact with ADR-0049 DP-3 (same
   contract, both sides). The trap: a reconciling re-snapshot re-
   `SELECT`s the source *now* (the table physically has its current
   columns). Two consistent designs — **(i) down-project** current
   rows to the pre-DDL schema and let resumed CDC replay the
   ALTER+backfill, vs **(ii) re-anchor** the watermark *now*, select
   in current schema, CDC resumes forward from the watermark (the
   DBLog property). **(i) is rejected — silently lossy:** an instant
   `ADD COLUMN … DEFAULT` emits **no per-row events**, so a
   down-projected chunk relying on the log to backfill the new column
   stays permanently wrong, exit 0 (the Bug 74/75 class). Decision:
   **(ii), with a single position anchor per table-reconcile** taken
   at that table's reconcile start; ADR-0049 resolves the `ir` schema
   as-of that position; every chunk of the table is interpreted/
   applied in that resolved schema; CDC re-anchors at the watermark
   and continues forward; **never down-project to a pre-DDL schema.**
   A **DDL detected before a table's reconcile completes voids that
   table's in-progress reconcile → loud fall-back to ADR-0022 full
   re-copy of that table** under the new schema (per-chunk PK-cursor
   resumability holds only within a stable-schema window; a schema
   change is the rare reset event — this is ADR-0049's already-stated
   loud floor, made specific here). Buffered LOW→HIGH events resolved
   per-event via ADR-0049 (a DDL inside that sub-second window is
   narrow; same loud floor). **Contingent dependency (load-bearing):**
   DP-3's safety rests entirely on ADR-0049 **DP-1** (per-engine
   DDL-boundary detection — the hard case, *VStream schema-tracking
   OFF* via FIELD-event-delta, is the open Phase-1c empirical
   question). DP-3 is *decided* but its correctness is gated on
   ADR-0049 DP-1. **Structural decision (owner, 2026-05-18):** keep
   ADR-0049 and ADR-0050 **separate** (independently reviewable;
   ADR-0049 has standalone value for plain resume-after-DDL) **but
   hard-sequenced** — ADR-0049 DP-1 + its Phase-1c evidence MUST land
   before any ADR-0050 implementation. Recorded symmetrically in
   ADR-0049.
4. **Recovery-only vs. also proactive drift-verification** —
   **RESOLVED (2026-05-18).** v1 is **recovery-only**; proactive
   periodic source/target drift-audit is explicitly out of scope. The
   DP-1 fingerprint *is* a source-vs-target block-diff, so drift-audit
   is a small *code* delta but a different *product* surface (runs
   against a live healthy stream → cadence/scheduling, "expected
   drift while CDC mid-apply/lagging," false-positive mgmt, alerting/
   metrics) — exactly the machinery the narrow-v1/demand-gated/loud-
   floor stance excludes. Pinned forward pointers (don't lose, but
   future separately-demand-gated ADR): **(a)** DP-1's fingerprint is
   deliberately engine-neutral / `value-types.md`-canonical so a
   future drift-audit reuses it with no rework; **(b)** a persisted
   **columnar per-range hash store** earns its keep *only* for
   drift-audit (recovery computes-compares-**discards** — the DP-1
   freshness argument; zero storage is correct for recovery). The
   columnar shape (compresses hash columns; append-only; aligns with
   sluice's JSON-Lines+gzip backup-chunk precedent + the Parquet-
   export research) is the right *future* design, not v1; **(c)** the
   future drift-audit's home is **`sluice verify --mode=checksum`**
   (reusing this fingerprint), not a new top-level feature on the
   recovery path — keeps the existing `sluice verify` surface
   coherent.
5. **Checksum compute-location** — **RESOLVED (2026-05-18).** Modes:
   **(a) push-down** — engine computes the block hash in SQL
   (`SHA2()`/`digest()`), only the 128-bit fingerprint crosses the
   wire → near-zero egress (the *direct* lever for the PlanetScale
   source-read cost), but **same-engine only** and carries the
   `pt-table-checksum` SQL-canonicalization footguns
   (NULL-vs-empty / type / collation / float / decimal / session
   settings) — must be engineered so they can only ever cause a
   harmless false-*diverged*, never a false-*match* (DP-1 asymmetry).
   **(b) in-sluice** — sluice hashes over the `value-types.md`
   IR-canonical form (DP-1 as written) → cross-engine-correct,
   contract-stable; the source read is the egress (still a big net
   win vs full re-ship).
   **Decoupling that resolves the tension:** push-down only needs to
   power the **skip-gate decision** (sole invariant: *never
   false-match*; **not** position-anchored — a skip is safe whenever
   ranges currently match, CDC syncs forward from the re-anchor). The
   **reconcile of diverged ranges always goes through the position-
   anchored watermark/VStream path (DP-2/DP-3) regardless of mode** —
   cheap divergence *detection* (push-down) is separable from
   consistent divergence *repair* (anchored).
   **Decision: v1 = (b) in-sluice only.** Push-down **(a) deferred to
   v1.1, evidence-gated** — the DP-2 empirical A/B is designed to
   *measure* the in-sluice source-read egress; that data decides
   whether (a)'s same-engine-only second mechanism + canonicalization
   hardening is justified (consistent with the measure-first,
   complexity-on-evidence stance held throughout). The fingerprint /
   skip-gate is built **mode-pluggable** so (a) slots in with **no
   rework** when evidence justifies. DP-1's hash resolves per-mode:
   SHA-256-in-Go for (b) now; engine-native `SHA2(...,256)` /
   `digest(...,'sha256')` for (a) in v1.1.

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

**Proposed; design dialogue COMPLETE (2026-05-18, owner-endorsed).**
**All decision points resolved: DP-1–DP-5 + the structural
question.** No design questions remain open. Implementation is gated
**only** on three non-design gates — do **not** implement until all
three clear:
1. **Empirical cost-validation** — real testing data must show
   reconciling-resnapshot beats full re-copy on representative
   tables; the Vitess native-vs-`sluice_watermark` A/B (DP-2) is a
   deliberate part of that evidence.
2. **Hard-sequencing (DP-3)** — **ADR-0049 must be *implemented*
   before any ADR-0050 implementation**; ADR-0050 DP-3's correctness
   is contingent on ADR-0049's per-engine DDL-boundary signal. Update
   (2026-05-18): ADR-0049's **design dialogue is now complete (all its
   DPs resolved) and its Phase-1c evidence is in hand** — so the
   design+evidence halves of this prerequisite are satisfied; the
   **sole remaining gate-2 condition is ADR-0049's implementation**
   (it is implement-ready, not demand-gated). ADR-0049/0050 stay
   **separate, not merged**.
3. **Real operator demand** — still demand-gated on an actual
   position-loss case; not scheduled.
Independent of #37 (pinned). Phase-1c's empirical characterisation of
the *current* re-snapshot granularity feeds DP-3 and the
Consequences. When promoted: v1 = in-sluice checksum (b), flat
adaptive chunks, three-way engine-asymmetric watermark, recovery-only;
v1.1 evidence-gated = push-down (a) + recursive bisection.
