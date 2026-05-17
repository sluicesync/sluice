# ADR-0049 — sluice-native position-anchored CDC schema history

**Status:** **Proposed (design-first; sign-off pending).** Design pass
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

1. **DDL-boundary detection, per engine.** Binlog: the `QUERY`/DDL
   event. PG: relation-message / a periodic catalog check. **Vitess
   VStream with schema-tracking OFF:** there is no clean DDL event —
   detection must be a `FIELD`-event field-set delta (the open
   empirical question Phase 1c is characterizing). The history is only
   as good as the boundary signal; sign-off needs the per-engine
   trigger confirmed (Phase-1c evidence).
2. **Retention / compaction.** How far back the history is kept (bounded
   by the oldest resumable position; compacted past the persisted
   safe-point) vs. unbounded growth.
3. **Consistency contract with [ADR-0050](adr-0050-reconciling-resnapshot.md).**
   A reconciling re-snapshot selects at a watermark position; the
   schema version applied to selected rows must be the
   history-resolved version *as of that watermark*, not "now".

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

Proposed. Do **not** implement before owner sign-off on the three
decision points (DP-1 gated on Phase-1c's empirical
VStream-tracking-off finding). Independent of #37 (pinned); can proceed
on its own branch when accepted. Could be merged with ADR-0050 into one
"robust CDC recovery" ADR if the owner prefers a single design.
