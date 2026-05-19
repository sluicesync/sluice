# ADR-0049 — sluice-native position-anchored CDC schema history

**Status:** **Accepted (2026-05-18) — design signed off across all
decision points (DP-1/2/3) after a design dialogue; implementation
pending. Not demand-gated (standalone resume-after-DDL value) →
implement-ready modulo owner prioritization; see "Status / next".**
(Header reconciled 2026-05-18: the dialogue is complete; the prior
"Proposed; sign-off pending" wording was superseded.) Design pass
*before* code; produced from the Track-1 PlanetScale/Vitess readiness
investigation (design evidence:
[`docs/dev/notes/prep-planetscale-vitess-readiness.md`](../dev/notes/prep-planetscale-vitess-readiness.md)
§"Phase 1c"). **Proposed → dialogue → Accepted**: the three decision
points in §"Decision points requiring sign-off" must be resolved with
the owner before implementation. **Independent of Roadmap #4 (#37)** —
this is a CDC-correctness/recovery feature, not multi-source. Builds on
the position-and-data atomicity of
[ADR-0007](adr-0007-position-persistence.md) and the control-table
additive-column pattern of
[ADR-0030](adr-0030-mid-stream-live-add-table.md) /
[ADR-0034](adr-0034-mysql-phase-2-live-add-table.md). Pairs with
[ADR-0050](adr-0050-reconciling-resnapshot.md) (the schema version
resolved here must be consistent with that ADR's re-snapshot
watermark).

## Context

A live `sluice sync` consumes a source change-log (MySQL binlog/GTID,
PG logical replication, Vitess VStream). Decoding a ROW event requires
the **column layout that was in effect at that event's position** — a
schema that can change under the stream (an `ALTER` / a PlanetScale
deploy-request applied mid-sync).

Current behaviour (code-truth, verified):

- **Binlog path** detects DDL and invalidates its schema cache →
  re-introspects `information_schema`. Correct in steady state, but the
  re-introspection is *not position-anchored*: it reads "schema now",
  which races events still in flight for the pre-DDL shape, and is
  wrong on **resume/replay** (a resumed stream replays log positions
  whose schema differs from "now").
- **VStream path** depends on **Vitess schema-tracking, which is OFF by
  default.** With it off, a deploy-request DDL mid-stream is the
  operator-reported failure: the CDC consumer's field metadata no
  longer matches the events. Today's loud floor (the minimum, and what
  ADR-0050/Phase-1c assert) is: detect the field-metadata mismatch →
  loud refuse → [ADR-0022](adr-0022-position-invalid-coldstart.md)
  cold-start re-snapshot. Never silent — but a re-snapshot for a
  schema change is the multi-day-outage-class pain operators have hit.

The IR-first tenet makes the IR the natural home for a schema that
*varies over the CDC timeline*: a position-keyed `ir` schema history.
This is the established pattern in robust CDC tooling (Debezium's
schema-history topic; Maxwell's schema store; corroborated by PlanetScale
[`binlogsrv`](https://github.com/planetscale/binlogsrv)'s GTID-first
persisted-replication-state design — position-anchored durable state is
the resilience primitive).

## Decision

Add a **position-anchored schema-history control table**
(`sluice_cdc_schema_history`, additive — same control-table discipline
as ADR-0030/0034). At every detected DDL boundary, snapshot the
affected table's `ir` schema keyed by the **source position**
(GTID/VGTID/LSN, the same token ADR-0007 already persists). The change
applier resolves each event to *the schema version as of that event's
position* rather than "schema now". This makes CDC schema-correctness
**independent of the source's optional schema-tracking feature** and
**correct across resume/replay**, and converts the
schema-change-induced re-snapshot into a metadata lookup. Engine-neutral
(the history is `ir`; engines supply only the DDL-boundary signal).

The loud floor (detect-mismatch → refuse → ADR-0022 re-snapshot) is
retained as the safety net for any gap the history doesn't cover —
this ADR is the *correctness/efficiency upgrade on top*, never a
removal of the loud guarantee.

## Decision points requiring sign-off

1. **DDL-boundary detection, per engine.** — **RESOLVED (2026-05-18,
   owner). Phase-1c evidence is IN HAND** (delivered + verified
   2026-05-17, `docs/dev/notes/prep-planetscale-vitess-readiness.md`
   §"Phase 1c" Phase A; superseding this DP's prior "open empirical
   question" wording). Per-engine triggers:
   - **MySQL binlog:** the `QUERY` DDL event (ALTER/CREATE/DROP/
     RENAME/TRUNCATE — DDL is statement-logged even under ROW format).
     Boundary = that event; anchor = its GTID.
   - **PostgreSQL logical replication:** a pgoutput **`Relation`-
     message column-set delta** (pgoutput re-sends `Relation` before
     the first change to a table after its definition changes);
     anchor = that Relation's LSN. The periodic catalog check is
     demoted to a **belt-and-suspenders backstop** for the benign
     edge (DDL on a table with no subsequent DML → nothing to
     mis-decode until the next DML re-sends `Relation`).
   - **Vitess VStream:** a **`FIELD`-event field-set delta**; anchor =
     the VGTID at that FIELD event. **Phase-1c-proven** (ADD/DROP/
     MODIFY ground-truthed vs vttestserver; VStream re-emits a fresh
     `FIELD` post-DDL, `dispatchDDL → clear(r.fields)` forces re-fetch
     before the next ROW → faithful; tests
     `cdc_vstream_schema_evolution_integration_test.go`).
   **Sign-off points (owner-accepted 2026-05-18):**
   (i) **Use the VStream `FIELD`-delta signal *uniformly*, regardless
   of Vitess schema-tracking on/off** — no special-case for
   tracking-ON; a single uniform signal *is* this ADR's stated goal
   ("schema-correctness independent of the source's optional
   schema-tracking feature"); special-casing adds a second path for
   zero correctness gain.
   (ii) **Boundary = a *true delta*, not "an event arrived."** VStream
   emits `FIELD` on (re)start / per-table first-touch and PG re-sends
   `Relation` on reconnect *without* any DDL; a history version is
   snapshotted **only when the (column-name set, ordered types)
   actually differs from the cached schema** — avoids no-op-version
   bloat and feeds DP-2 (retention sees only real deltas).
   **Common invariant (Phase-1c-confirmed already-true):** the
   boundary must be detected *before* the next ROW decodes in the new
   schema; a ROW racing ahead of its `FIELD`/`Relation` is a **loud
   hard error**, never silent — the loud floor holds today; this ADR
   is the efficiency upgrade (position-anchored history → resume-
   after-DDL stops forcing a whole-stream re-snapshot) on top of it,
   NOT a fix for an active silent bug (Phase-1c proved none exists on
   this path).
2. **Retention / compaction.** — **RESOLVED (2026-05-18, owner).**
   DP-1's true-delta rule already bounds growth to ∝ DDL count (not
   stream volume), so this is a correctness-of-resume question, not a
   space one — with one non-obvious hazard.
   - **Retention floor = `min( live persisted CDC safe-point
     (ADR-0007 sluice_cdc_state), oldest retained backup-chain resume
     position )`.** Backups pull the floor *backward*: a backup taken
     at `P_backup`, restored later and CDC-resumed from there, needs
     the schema version active at `P_backup`; "compact past the live
     safe-point" alone would discard it. Compact only past the
     *combined* floor.
   - **Schema-history is part of the backup envelope** (append-only —
     already in Consequences): a restored backup carries the schema
     versions needed to resume from its own position, closing the
     restore hazard at the source rather than via retention
     bookkeeping.
   - **Conservative / non-eager compaction:** growth is tiny (∝ DDL),
     so default = **retain generously**; compaction past the combined
     floor is an available maintenance operation, not an aggressive
     automatic default (cheap-insurance posture).
   - **Compaction safety invariant (loud floor):** `resolve(position)`
     for a position older than the oldest retained version = **loud
     refuse → ADR-0022 cold-start re-snapshot**, never a silent
     mis-decode. Compaction can therefore never cause silent
     corruption — worst case is loud + recoverable.
   - **No backward retention pressure from ADR-0050:** DP-3's
     re-anchor-at-watermark means a reconciling re-snapshot resolves
     schema at ≈now, needing no old versions — retention is governed
     solely by normal resume + backups; the two ADRs stay consistent
     (this does not reopen DP-3).
3. **Consistency contract with [ADR-0050](adr-0050-reconciling-resnapshot.md).**
   — **RESOLVED (2026-05-18, owner; recorded symmetrically in
   ADR-0050 DP-3, which carries the full reasoning).** Contract: a
   reconciling re-snapshot uses a **single position anchor per
   table-reconcile**; this history resolves the `ir` schema as-of
   that anchor; rows are applied in that resolved schema; CDC
   re-anchors at the watermark and continues forward; **never
   down-project current rows to a pre-DDL schema** (an instant
   `ADD COLUMN … DEFAULT` emits no per-row events → down-projection is
   silently lossy — the Bug 74/75 class). A DDL detected before a
   table's reconcile completes voids that reconcile → loud fall-back
   to ADR-0022 full re-copy of that table (this ADR's loud floor,
   made specific). **Hard-sequencing decision (owner):** ADR-0049 and
   ADR-0050 stay **separate, not merged** (independently reviewable;
   this ADR has standalone value for plain resume-after-DDL), **but
   ADR-0049 DP-1 + its Phase-1c evidence MUST land before any
   ADR-0050 implementation** — ADR-0050 DP-3's correctness is
   contingent on this ADR's per-engine DDL-boundary detection.

## Consequences

- CDC correctness no longer depends on Vitess schema-tracking being
  enabled; resume-after-DDL stops forcing a re-snapshot.
- New durable control table + a per-engine DDL-boundary signal +
  position→schema resolution in the apply hot path (must be O(1)
  amortised — cache the active version, swap on boundary).
- Backup envelope / state-format addition (append-only, zero-users
  clean per the project tenet).

## Alternatives considered

- **Rely on Vitess schema-tracking** — rejected: off by default; *is*
  the reported failure mode; doesn't help binlog/PG resume-replay.
- **Re-introspect-on-DDL only (status quo, binlog path)** — rejected as
  the durable answer: not position-anchored, wrong on resume/replay,
  no signal on VStream-tracking-off.
- **Always re-snapshot on any schema change** — rejected: the
  operator-reported cost/outage pain this ADR exists to remove.

## Status / next

**Proposed; design dialogue COMPLETE (2026-05-18, owner-endorsed).**
**All decision points resolved: DP-1, DP-2, DP-3.** No design
questions remain; the Phase-1c evidence DP-1 needed was always in
hand (verified 2026-05-17) — never a research gap, only an
un-recorded resolution, now recorded. **Not demand-gated** (unlike
ADR-0050) — this has standalone resume-after-DDL value — so it is
**implement-ready, modulo owner prioritization** (design-first
sign-off now given across all DPs). Independent of #37 (pinned); can
proceed on its own branch. **Cross-ADR (owner, 2026-05-18):**
ADR-0049 and ADR-0050 stay *separate, not merged*, but
hard-sequenced — ADR-0050 DP-3's correctness is contingent on this
ADR's per-engine DDL-boundary detection, so **ADR-0049 must be
*implemented* before any ADR-0050 implementation** (the design +
Phase-1c halves of that prerequisite are now satisfied; the
remaining half is this ADR's implementation). **Reframe (Phase-1c):**
the CDC schema-evolution path is *already faithful at the loud
floor* — no active silent bug — so this ADR is a value/efficiency upgrade
(resume-after-DDL without a whole-stream re-snapshot), priority/
demand-gated, not a correctness emergency.

**Implementation-readiness brief (2026-05-19):**
[`docs/dev/notes/adr-0049-implementation-readiness.md`](../dev/notes/adr-0049-implementation-readiness.md)
— code-grounded phased chunk plan (A–E), the three implementation
ambiguities, four hot-path checkpoint decisions, the test matrix, and
the ADR-0050 A–D sequencing gate. **Owner checkpoint CLEARED
(2026-05-19): all 5 go/no-go decisions ratified as recommended — see
§"Implementation checkpoint sign-off" below.** Design is complete, no
design questions remain, and the implementation-level decisions are
now locked. **Chunk A is implement-ready.**

## Implementation checkpoint sign-off (2026-05-19)

Owner-ratified (all five accepted as recommended; not a re-opened
dialogue — these are implementation-level decisions, distinct from the
design DP-1/2/3 sign-off above). Locked:

1. **History payload format = the existing `internal/ir/backup.go`
   tagged-union JSON codec, reused verbatim.** Rationale: the IR's
   sealed `Type`/`DefaultValue` can't round-trip plain `encoding/json`;
   that codec is the only proven serializer, and DP-2 already requires
   schema-history to ride the same backup envelope. A new codec would
   duplicate sealed-interface logic for no gain.
2. **The VStream `[]*query.Field → ir.Table` projector is in-scope
   new load-bearing code.** The snapshot is built from in-stream
   position-anchored metadata, **never** re-introspection (the ADR
   rejects re-introspection in Alternatives). This is the single
   largest new surface; binlog/PG boundary paths are cheaper.
3. **Position ordering = an engine-supplied predicate** ("is P ≤
   anchor A"), a new optional engine interface mirroring the existing
   `verifyPositionResumable`/`verifyGTIDSetReachable` pattern — NOT a
   generic token string-compare. Keeps the history table
   engine-neutral storage with engine-owned ordering.
4. **Hot-path correctness invariants (all three ratified):**
   (a) the history-version write is in the **same target tx** as the
   ADR-0007 position write (a cross-tx crash desyncs them → spurious
   ADR-0022 cold-start); (b) a history-write failure is **fatal/loud**,
   never logged-and-continued (a lost version silently degrades future
   resume — loud-failure tenet); (c) the version anchor is the
   **DDL/FIELD/Relation event's own position captured at detection**,
   not the first subsequent row's (else replay between DDL and first
   post-DDL row silently resolves to the pre-DDL schema — the exact
   silent-mis-decode class this ADR exists to kill).
5. **Chunk sequencing approved:** `A → (B1 ∥ B2 ∥ B3) → C → D → E`;
   B1/B2/B3/C are concurrency chunks (the `-race`-integration-gate-
   before-tag rule applies). **ADR-0050 implementation stays
   hard-blocked until ADR-0049 Chunks A–D land + green** (ADR-0050
   DP-3 correctness is contingent on ADR-0049's per-engine boundary
   detection being live in code).

Chunk A (additive `sluice_cdc_schema_history` control table +
`ir`-schema serialization via the (1) codec + `resolveSchemaVersion`
with the below-floor → `ir.ErrPositionInvalid` loud refuse) is a
**non-concurrency** chunk (pure storage + lookup) → standard
tag-then-watch flow; it is the implement-ready entry point.
