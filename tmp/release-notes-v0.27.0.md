# sluice v0.27.0

**MySQL Phase 2 mid-stream live add-table — parity for v0.24.0's PG-only `--no-drain`.** Operators with high-availability MySQL workloads can now bring a new source table into an active CDC stream's scope without the `sluice sync stop --wait` drain that Phase 1 required. Same operator UX as PG (`sluice schema add-table TABLE --no-drain`); different mechanism (filter-flip via `sluice_cdc_state` column polled by the streamer, since MySQL's binlog auto-includes every table — the gate is in the streamer's table-filter, not in a publication). ADR-0034 documents the design.

## Features

- **`sluice schema add-table TABLE --no-drain`** now works against MySQL sources (was PG-only since v0.24.0). Same flag, same operator UX. The orchestrator dispatches by source engine: PG → existing publication-add path (ADR-0030); MySQL → new filter-flip path (ADR-0034). Mixed-engine refusals stay clean with operator-actionable messages.

- **`tableFilterFlipper` engine surface.** New optional surface implemented by MySQL's `ChangeApplier` (`RecordLiveAddedTable` / `ReadLiveAddedTables`). PG doesn't implement it (uses publication scope instead); discovered structurally so the orchestrator stays engine-neutral.

- **`live_added_tables TEXT NULL` column on `sluice_cdc_state` (MySQL).** Idempotent migration — same shape as v0.24.0's `slot_name`, v0.25.0's `source_dsn_fingerprint`, v0.25.1's `target_schema`. Comma-separated list; the streamer polls and merges on each tick (5s cadence, paired with the existing `stop_requested_at` poll).

- **Streamer filter-flip plumbing.** New `streamer_filter_flip.go` and `liveAddedFilter` (atomic.Pointer-backed) thread the polled-from-cdc-state additions into `applyTableFilter`'s scope mid-run. The filter is **additive**: existing operator-supplied `--include-table` / `--exclude-table` rules continue to apply; the live-added table joins the include list at the next poll tick.

- **ADR-0034 — MySQL Phase 2 mid-stream live add-table.** Decision rationale (filter-flip vs accept-no-filter), threat model (4 scenarios — operator forgot `--no-drain`; two operators race add-table for different tables; streamer crash mid-flip; high-write-rate during filter-flip window), best-effort caveat documentation, parity with PG `--no-drain` operator UX.

## Use cases this unlocks

| Scenario | Before v0.27.0 | With v0.27.0 |
|---|---|---|
| **HA MySQL workload with rolling schema migrations** | `sluice sync stop --wait` drains the stream (seconds-to-minutes of lag); operator runs `add-table`; resumes via `sync start --resume`. Customer-visible lag spike during the drain. | `sluice schema add-table TABLE --no-drain` runs against the live MySQL stream; CDC keeps consuming throughout. Same UX as PG since v0.24.0. |
| **MySQL operator who started a stream with `--include-table=accounts,orders`** and later wants to add `customers` | Stop, restart with extended filter, lose the resume position OR cycle through `--reset-target-data`. | Live add-table extends the in-memory filter via the cdc-state column poll; operator's existing filter rules preserved. |

## Compatibility

- **No format-breaking changes.** Manifest schema, change-chunk format, CLI surface — all unchanged for existing flows.
- **Default behavior unchanged.** Operators not using `--no-drain` see no behaviour change — the v0.24.0 + earlier add-table flow continues to require a drained stream.
- **Drop-in upgrade from v0.26.0.** No DDL migration on `sluice_cdc_state`; the new `live_added_tables` column lands on first `EnsureControlTable` call.
- **PG operators unaffected.** PG `--no-drain` continues to use the publication-add mechanism from ADR-0030; the MySQL filter-flip path is engine-gated.
- **Existing `--include-table` / `--exclude-table` semantics preserved.** Live-added tables are additive; explicit exclusions still apply.

## Known limitations

- **Best-effort during the filter-flip window** (parallel to PG Phase 2's documented gap, ADR-0030 item 3). Events on the new table that arrive between the bulk-copy snapshot's binlog position and the streamer's filter-flip observation (~5s poll interval) may not be delivered. Under-load test observed ~3 events lost out of 59 in CI's worst-case sustained-INSERT scenario. Operators with high write rates on the new table at the moment of live-add should use the drained add-table flow (zero-loss by construction) or quiesce writes for the seconds-long window. The strict-correctness mechanism (ADR-0033) is open for both engines pending further design work.

- **5-second filter-flip poll cadence** (matches the existing `stop_requested_at` poll). A future refinement could shorten the cadence or add a notification mechanism (LISTEN/NOTIFY for PG; MySQL has no equivalent — would need a polling-rate trade-off).

- **VStream / PlanetScale not in scope.** Different binlog source surface; Phase 2.5 follow-on if real demand surfaces.

## Test coverage

- **Unit tests**: `tableFilterFlipper` semantics; `liveAddedFilter` additive behavior; orchestrator engine-dispatch (PG vs MySQL); 4-case resolution table for live-add MySQL paths.
- **Integration tests** (gated `//go:build integration`):
  - **Phase A verification** (`TestFilterFlip_Verify_PostFlipEventsDelivered`): empirically confirms the filter-flip mechanism delivers post-flip events to the applier (the design's load-bearing assumption).
  - **TestAddTable_LiveMode_MySQL**: happy path — MySQL → MySQL, active stream with filter, add a new table, verify it joins the stream's scope cleanly.
  - **TestAddTable_LiveMode_MySQL_UnderLoad**: best-effort under sustained INSERTs; pin: snapshot rows + post-flip CDC pinned; in-flight events during the filter-flip window logged but not asserted (best-effort caveat).
  - **TestAddTable_LiveMode_MySQL_FilterRespectedAfterFlip**: verifies the filter-flip is additive — existing tables in scope remain, new table added, tables NOT in either scope still get filtered out.
  - **TestAddTable_LiveMode_PG**: regression check — existing PG path unchanged.

## Who needs this

- **MySQL HA operators** who have been running v0.24.0+ with the PG-only `--no-drain` envy. v0.27.0 closes the parity gap.
- **MySQL operators using `--include-table` / `--exclude-table` for safety isolation** who want to extend the scope mid-run without losing resume position or cycling through `--reset-target-data`.
- **Anyone planning the Vitess / PlanetScale Phase 2.5 work** — v0.27.0's `tableFilterFlipper` interface is the natural extension point; the Phase 2.5 chunk would implement it on the VStream code path.

## What's next

- **Roadmap item 3 — Mid-stream live add-table strict zero-loss correctness.** ADR-0033 documented that Path A (slot-pause) doesn't work as designed (pgoutput's per-LSN catalog snapshot pins membership at decode time). Three forward options: Path B (dual-slot), Path C (operator quiesce), Path D (diagnose actual residual loss surface first). The MySQL Phase 2 best-effort caveat shipped here is parallel to PG's; the strict-correctness fix would close both engines together.
- **Roadmap item 6 — GEOMETRY/SPATIAL support.** Closes Bug 26 (PostGIS SRID dropped) + Bug 27 (VStream POINT bytes mis-parsed). PostGIS for both PG-to-PG (parented under item 11 PG extension passthrough) and cross-engine (parented under item 6).
- **Roadmap items 7-8 (compression investigation, analytics-friendly source research)** — see `docs/dev/roadmap.md`.
