# ADR-0073: Handling Vitess internal + online-DDL lifecycle tables in VStream

## Status

Proposed. Surfaces a reproduced gap from the Bug-125 investigation and an operator-flagged robustness requirement. Builds on the VStream snapshot/CDC path ([ADR-0071](adr-0071-vstream-snapshot-bounded-memory.md), [ADR-0072](adr-0072-resumable-coldstart-copy.md)) and the existing `_vt_*` exclusion (GitHub issue #22 / "Bug 22"), whose coverage this ADR completes for the VStream COPY filter.

## Context

PlanetScale *is* Vitess under the hood, and on Vitess **online DDL is the default schema-change mechanism**. An online `ALTER` doesn't mutate the table in place — Vitess builds a **shadow table**, copies rows into it via VReplication, then **atomically cuts over** (renames) and cleans up. The transient artifacts carry reserved names:

- **Internal-operation tables, under a _version-dependent_ naming scheme** ([vitessio/vitess#14582](https://github.com/vitessio/vitess/issues/14582)). The **legacy** format (Vitess 19–20) used per-state GC hints — `_vt_HOLD_*`, `_vt_PURGE_*`, `_vt_EVAC_*`, `_vt_DROP_*` — plus UUID-leading online-DDL / vreplication names (e.g. `…_vrepl`). **Vitess 20+ replaced these with a single unified pattern** `_vt_<op>_<uuid>_<timestamp>_` (fixed-width, `_vt_` prefix + trailing underscore), where `<op>` is a 3-char code: `hld`/`prg`/`evc`/`drp` (GC states), **`vrp`** (online-DDL / vreplication), `gho`/`ghc`/`del` (gh-ost shadow / changelog / deleted). The `_vt_vrp_*` table the Bug-125 probe hit is this new format's online-DDL code. **The format demonstrably changed across Vitess versions** — which is precisely why the decision below anchors to Vitess's own helper rather than a name list sluice maintains.

sluice's VStream reader requests **every** table — the COPY filter rule is `Match:"/.*/"` (`cdc_vstream.go`, `cdc_vstream_snapshot.go`). So these Vitess-internal artifacts are pulled into the stream.

**This is not hypothetical — it was reproduced.** During the Bug-125 hunt, a probe run against a branch with an *in-flight deploy* (an online schema change) found sluice's COPY filter **picked up a `_vt_vrp_*` shadow table** instead of (or alongside) the real table, which then tripped the ADR-0071 scope-name-mismatch loud-refusal (`activeTable` mismatch) and aborted the cold-start with zero rows. A real schema change on a PlanetScale source can therefore break a sync today.

This functionality must be **rock-solid**: every PlanetScale user will run schema changes, so the first online `ALTER` during a sync cannot corrupt, drop, or wedge the stream.

### Two distinct requirements

1. **Exclude Vitess-internal tables** — `_vt_*` workflow tables, `_vt_vrp_*` shadows, and the lifecycle-hint names — from BOTH the COPY filter AND the CDC dispatch path. They are never logical user tables and must never be copied, applied, or counted.
2. **Survive an online-DDL cutover mid-sync** — the shadow build, the atomic rename/swap, and the cleanup must not break the logical table's stream: the logical table keeps its identity across the cutover, the internal events are ignored, and COPY/CDC continue with zero loss.

## Decision

1. **Filter Vitess-internal tables by Vitess's own convention, not a blanket match.** Use Vitess's `schema.IsInternalOperationTableName()` (it recognizes the internal-table patterns known to the Vitess version sluice vendors — both the legacy per-state hints and the v20+ unified `_vt_<op>_<uuid>_<timestamp>_` form; see #14582) — applied (a) when constructing the VStream `Filter` so vtgate doesn't even stream them where possible, and (b) defensively at the dispatch level (`dispatchRow` / the snapshot pump's row buffering) so a leaked internal-table ROW/FIELD event is dropped, not copied or applied. Keeping the matcher anchored to the Vitess helper (rather than a hand-maintained prefix list) means it tracks Vitess's lifecycle-naming evolution; a pin asserts the exclusion set so a Vitess wording change fails a test rather than silently leaking.
2. **Treat the logical table as identity-stable across an online-DDL cutover.** The stream follows the user-named table; the internal rename/swap events for `_vt_*` tables are excluded by (1), so the cutover is transparent to the logical stream. Where the cutover surfaces a FIELD event (schema delta) on the logical table, the existing ADR-0049 schema-history path handles it.

## Consequences

- **A *version-dependent* dependency on Vitess's internal-table naming convention.** #14582 is the proof the names change across Vitess releases (legacy per-state hints in 19–20 → the unified `_vt_<op>_…` form in 20+), so a static prefix list sluice maintains would be wrong on some cluster version. Anchoring to `schema.IsInternalOperationTableName()` outsources this to Vitess — but **the helper only knows the formats of the Vitess version sluice vendors**, so (c) must confirm the vendored version recognizes the formats real PlanetScale / self-hosted clusters emit (and bump the vendored Vitess if not), with a pin that fails on a recognized-set change rather than silently leaking.
- **The exclusion must apply on both paths** (COPY filter + CDC dispatch) — a COPY-only or CDC-only fix is incomplete (the Bug-125 probe hit the COPY path; a steady-state online DDL hits CDC).
- **New test surface (the high-value part, gated on Bug 126's fix that lets the CLI drive vttestserver):** a `vttestserver` harness run with `ENABLE_ONLINE_DDL=true` that (i) seeds a table, (ii) starts a sync, (iii) runs an online `ALTER` mid-stream, and asserts sluice **excludes** the `_vt_*` artifacts and **keeps syncing the logical table byte-clean through the cutover** (zero loss, no spurious copies/applies/refusals). This is correctness-gating — same tier as the silent-loss work.

## Alternatives considered

- **Rely on vtgate's own internal-table exclusion.** Insufficient — the reproduced `_vt_vrp_*` leak shows the blanket `/.*/` request still surfaces them to sluice; sluice must filter.
- **A static `_vt_` prefix list.** Fragile against the lifecycle-hint names and future Vitess conventions; prefer the Vitess helper as the single source of truth.
