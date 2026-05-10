# ADR-0034: Mid-stream live add-table — MySQL Phase 2 (filter-flip)

## Status

Accepted. Implemented in `internal/pipeline/add_table.go::AddTable.LiveMode` (orchestrator dispatch by source engine), `internal/pipeline/streamer_filter_flip.go` (mutable filter + poll integration), `internal/engines/mysql/control_table.go::live_added_tables` column + reader/writer helpers, and `internal/engines/mysql/change_applier.go` (the new `RecordLiveAddedTable` / `ReadLiveAddedTables` surfaces).

Phase A verification landed first as `internal/engines/mysql/filter_flip_verify_integration_test.go` to confirm the load-bearing claim before the orchestrator changed.

## Context

ADR-0030 shipped Phase 2 mid-stream live add-table (`--no-drain`) for Postgres in v0.24.0. The PG mechanism — publication-add-then-snapshot — leans on pgoutput's per-LSN catalog snapshot of publication membership: events on the new table at LSN ≥ `LSN_pubadd` get delivered to the slot. **There is no equivalent on MySQL.** The binlog is a positional log of every committed write across every database; the binlog has no notion of "scope" or "publication membership." Every event for every table is in the binlog by construction. The "is this table in the stream's scope" decision therefore lives elsewhere — in the streamer's in-memory `pipeline.TableFilter` (`--include-table` / `--exclude-table`), which the dispatch loop consults to drop events for tables outside scope before the applier sees them.

For Phase 2 on MySQL, the orchestrator needs a way to tell a *running* streamer "now also include table foo" so the dispatch filter starts admitting events for the new table from this point forward. The roadmap entry sketched two options:

- **Filter-flip:** propagate "now include foo" to the running streamer via the control table or a signal channel; the streamer mutates its in-memory filter mid-run.
- **Accept the no-filter default:** if the operator has no filter (`Include == nil && Exclude == nil`), the streamer's dispatch already passes every event; live-add reduces to bulk-copy + the schema-cache miss-and-load pattern from ADR-0021.

This ADR picks **filter-flip**. Reasoning:

- The accept-no-filter path is operator-hostile: production deployments commonly use `--include-table` for safety isolation (limit blast radius to a known table set) or `--exclude-table` to skip noisy/internal tables (`audit_*`, Vitess `_vt_*` shadows). Forcing those operators to drop their filter to use live-add would be a bait-and-switch.
- Filter-flip is additive — it preserves the operator's existing scope and adds one more table without touching the rest of the configuration.
- The mechanism (control-table column + streamer poll) reuses infrastructure that already exists for `stop_requested_at` (ADR-0025), `slot_name` (ADR-0030), `target_schema` (Bug 46), and `source_dsn_fingerprint` (ADR-0031). No new transport — just one more nullable column on `sluice_cdc_state` and one more applier-side recorder method.

## Decision

When `LiveMode=true` and the source engine doesn't implement `publicationAdder` (i.e. MySQL), the orchestrator dispatches to a binlog-source live-add path:

1. **Pre-flight (parallel to PG):** verify the active stream row exists; verify the new table exists on the source; verify the target table is empty; resolve `--target-schema` (no-op on MySQL, validated upstream); refuse loudly when the target applier doesn't expose `liveAddedTablesWriter` (the orchestrator's ladder for writing the filter-flip column).
2. **Capture pre-flip binlog position.** Read the source's current binlog position via the new optional engine surface `cdcSourcePositionReader` (MySQL implements via `SHOW BINARY LOG STATUS` / `SHOW MASTER STATUS`). This is the floor for the snapshot's binlog position — same shape as PG's `slotConfirmedFlushLSN` capture.
3. **Open snapshot.** MySQL uses `START TRANSACTION WITH CONSISTENT SNAPSHOT`; the snapshot captures the binlog position alongside its consistent point. PG-style `publicationAdder` is a no-op on MySQL (the engine doesn't implement it; the orchestrator's existing structural check skips the call).
4. **Bulk-copy** the new table from the snapshot.
5. **Filter-flip:** write the new table's name into `sluice_cdc_state.live_added_tables` (comma-separated, idempotent) for the active stream. The change is committed as a single SQL statement and visible to the streamer's poll on its next tick.
6. **Streamer's poll** detects the new value and merges it into the dispatch filter's effective allow-list, additively. From the next event onwards, binlog events for the new table reach the applier.
7. **CDC catch-up:** events on the new table at binlog positions ≥ snapshot's position arrive via the running stream's binlog tail. Events at positions ≥ the filter-flip column write but < snapshot's position are absorbed by the idempotent applier (INSERT ... AS new ON DUPLICATE KEY UPDATE — ADR-0010), same overlap shape as PG's [snapshot-LSN, slot-LSN] window.

## Mechanism — how the filter-flip propagates

The new column on `sluice_cdc_state`:

```
live_added_tables TEXT NULL
```

Holds a comma-separated list of unqualified table names that have been live-added to this stream's scope. Empty / NULL means "no live additions; use the original `--include-table` / `--exclude-table` configuration verbatim." When the orchestrator runs `add-table --no-drain TABLE`, it appends `TABLE` to this list (deduplicating). Idempotent on re-run: a table already in the list is left alone.

The streamer side has two pieces:

- **`liveAddedFilter`**: an `atomic.Pointer[map[string]struct{}]` on the running streamer instance. Initialised empty at `Run` start. Mutated by the poll goroutine; read by `changeAllowed` on every event.
- **Poll integration**: the existing `pollStopSignal` goroutine (which already reads `stop_requested_at` every 5s) gains a sibling read of `live_added_tables` on the same tick. New value → new map allocated, atomic-stored, debug-log-emitted.

The dispatch-side change in `filterChanges`/`changeAllowed`: a table passes if **either** the base filter allows it (operator's original `--include-table` / `--exclude-table`) **or** the live-added set contains its unqualified name. Non-empty live-added set is purely additive.

### Why control-table column rather than signal channel

A signal channel (or named pipe, or HTTP RPC) would require the operator running `add-table` to share a process address space with the streamer, which violates the "control-plane works across machines" property the control-table approach already buys us (see ADR-0025 § "Why control-table-based signaling rather than PIDs / pipes / SIGTERM via SSH"). The control table is already the source of truth for stream identity; one more column is the cheapest possible plumbing.

### Why mutate rather than restart

Restarting the streamer to pick up a new filter would cost a full snapshot + bulk-copy cycle for **every** existing table in scope (the cold-start path doesn't know which tables are "new"). Filter mutation lets the existing tables keep streaming uninterrupted; only the newly-added table goes through bulk-copy. This is also why the persisted CDC position is intentionally NOT updated by add-table — same as Phase 1 / PG Phase 2.

## Best-effort caveat (parallel to PG)

ADR-0030's Phase 2 ships with a documented best-effort property: events on the new table inserted DURING the brief publication-add window may not be delivered, because pgoutput evaluates publication membership per WAL record at decode time and events filtered before publication-add is committed don't get redelivered. ADR-0033 ran Phase A verification on a slot-pause "fix" and falsified it.

MySQL Phase 2 ships with a parallel best-effort caveat:

**Hazard: in-flight-event loss during the filter-flip window.** The streamer's filter is consulted on every event. Events on the new table arriving at the dispatch loop BEFORE the poll goroutine observes the new `live_added_tables` value are dropped by `filterChanges` (same as before the live-add). The poll cadence is 5s by default; with a sub-second poll override (test mode), the loss window is correspondingly smaller. The hazard is structurally identical to PG's: events that arrived during the brief window between bulk-copy completion and the running stream's filter mutation are gone.

The snapshot captures everything committed BEFORE the snapshot's binlog position (MVCC sees rows regardless of whether they're "in the filter" — filtering is a streamer concept, not an InnoDB one). The filter-flip + poll picks up everything from the moment the streamer's filter sees the new table. The gap is the binlog window between [snapshot-binlog-pos, filter-flip-observed-by-poll]. ADR-0030's PG-side loss surface is roughly analogous.

Operators with high write rates on the new table at the moment of live-add should:

- Use the drained add-table flow (`sluice sync stop --wait` then `sluice schema add-table` without `--no-drain`), which has zero-loss semantics by construction (the stream isn't filtering anything during the add).
- Quiesce writes to the new table for the seconds-long window of the live add (operator-coordinated; tooling deferred).

The strict-zero-loss correctness work (ADR-0033 § "Forward options") is its own roadmap entry; it covers PG and MySQL together once a viable mechanism (Strategy B dual-stream variants, source quiesce, etc.) is picked.

## Threat model

| Risk | Mitigation |
|------|------------|
| Operator forgot `--no-drain` flag against an active MySQL stream | Phase 1's `stop_requested_at` refusal still fires; same UX as before this chunk. |
| Two operators race add-table for different tables on the same stream | The control-table write is `UPDATE ... SET live_added_tables = ?` with a load-modify-store under a single SQL statement. The orchestrator reads the existing value, appends, and writes back — last-writer-wins. In practice the "lost" operator's add-table run still completed bulk-copy and snapshot; only the filter-flip column update was clobbered, surfacing as "events for that operator's table aren't being delivered." Operator re-runs `add-table --no-drain`, which re-issues the column write idempotently. The bulk-copy preflight refuses the second run because the table now has rows; operator recovery is `sluice schema add-table` re-run with `--allow-populated-target` (deferred — falls back to dropping the table on the target and re-running). The double-run pattern is rare; documented behaviour. |
| Streamer crash mid-flip (column written, streamer hadn't polled) | On restart, the streamer reads `live_added_tables` at startup and merges into the initial filter. No work lost; the warm-resume path picks up CDC from the persisted position with the union filter in effect. |
| Operator runs `add-table --no-drain` on a stream with `Include=[a,b]` for table `c` | Filter-flip writes `c`; streamer merges → effective allow-list `{a, b, c}`. Works as expected. |
| Operator runs `add-table --no-drain` for table `c` whose name happens to be in the operator's `Exclude=[c]` list | The base exclude-list excludes `c`; the live-added set adds `c`. With the OR-merge rule, live-added wins (`c` is allowed). Documented as additive override; if an operator wants to permanently exclude a table they've live-added, the recovery is `sync stop` and reconfigure. |
| MySQL applier missing the `live_added_tables` column (pre-v0.27.0 control table) | `ensureControlTable` runs `ADD COLUMN IF NOT EXISTS`-equivalent (detect-then-ALTER) on every applier startup. The streamer's poll tolerates a column-missing error as "no live additions" — same shape as the existing `stop_requested_at` migration path. |
| Live-add target schema mismatch (operator passes `--target-schema=X` against a MySQL target) | Validate gate already refuses `--target-schema` for MySQL targets via the SchemaScope=Flat check; no new code-path needed here. |

## Why not the no-filter accept path

ADR-0030 deferred MySQL Phase 2 with two options sketched. The accept-no-filter approach (rely on schema-cache miss-and-load) has narrow applicability:

- It works only when the operator has no filter at all. Operators with `--include-table` (the safety-isolation pattern) get nothing — their stream stays scoped to the original table set forever, even after `add-table` ostensibly succeeds.
- The schema-cache miss is already the recovery path for "unknown table" events on MySQL today (the CDC reader's `tableFor` lazily loads from `information_schema`). It's not a filter-flip mechanism; it's a robustness layer beneath the filter. For an operator running with no filter, both layers happen to give the right answer; for an operator with a filter, the schema-cache only kicks in for events the filter has already passed.

Filter-flip is the proper fix because it addresses the actual scope-control surface that operators interact with on MySQL.

## Operator-facing UX symmetry with PG

The CLI surface is unchanged:

```
sluice schema add-table TABLE --stream-id ID --no-drain \
  --source-driver=mysql --source=...
```

The orchestrator's source-engine dispatch picks the right mechanism:
- PG (publication-add) → ADR-0030 path.
- MySQL (filter-flip) → this ADR's path.

Same flag, same ergonomics, same best-effort caveat. The success log is the same nudge ("the active stream's tail will pick up new-table events on its next consumption"), no operator restart required for either engine.

## Verification

Phase A verification (`internal/engines/mysql/filter_flip_verify_integration_test.go`): boots MySQL, runs a streamer with `Include=[users]`, drives INSERTs on `orders` (filtered out, drops), writes `orders` into `live_added_tables`, verifies subsequent INSERTs on `orders` arrive at the applier. Pin: post-flip events delivered. The verdict line in the test logs surfaces in CI for any future regression.

Unit tests in `internal/pipeline/add_table_test.go`:
- `LiveMode=true` against a MySQL-shaped engine routes through the filter-flip path.
- `LiveMode=true` against an engine with neither `publicationAdder` NOR a target applier implementing `liveAddedTablesWriter` refuses with a clear "engine doesn't support live add" error.
- The filter-flip orchestrator records the table on the cdc-state column.

Integration tests in `internal/pipeline/add_table_live_mysql_integration_test.go`:
- `TestAddTable_LiveMode_MySQL`: happy path. Active stream with `Include=[users]`, live-add `orders`, verify orders snapshot rows + post-add CDC delivery.
- `TestAddTable_LiveMode_MySQL_UnderLoad`: best-effort under sustained inserts. Pins snapshot rows + post-flip CDC; logs in-flight gap as best-effort.
- `TestAddTable_LiveMode_MySQL_FilterRespectedAfterFlip`: pins additive semantics. Operator's existing `Include=[users]` stays in scope, live-added `orders` joins, an OUT-OF-SCOPE table `audit_log` stays excluded.

## Consequences

- **MySQL operators with HA workloads no longer need a drain window.** `sluice schema add-table TABLE --no-drain` runs against an active MySQL stream; the new table joins the running stream's scope and is bulk-copied with no lull in CDC consumption for the existing tables.
- **Default behaviour unchanged.** The flag defaults off; Phase 1's drained-stream refusal remains the conservative default for both engines.
- **One new optional applier-side surface (`liveAddedTablesWriter`/`liveAddedTablesReader`)** in the pipeline package, with MySQL as the only implementor today. PG operators continue to use the publication-add path; the orchestrator dispatches by source-engine capability rather than by engine name.
- **Existing PG-only refusal path is replaced** with engine-capability-driven dispatch. The MySQL refusal in `TestAddTable_LiveMode_MySQL_Refused` (Phase 1's coverage from ADR-0030 verification) is updated to test that MySQL now succeeds via the new mechanism rather than refusing.
- **Best-effort caveat tracked.** Same shape as PG Phase 2's; the strict-zero-loss roadmap entry now covers both engines under one chunk.

## See also

- `docs/adr/adr-0030-mid-stream-live-add-table.md` — PG Phase 2 design; § "MySQL deferred" was the pointer to this work.
- `docs/adr/adr-0033-mid-stream-live-add-strict-zero-loss.md` — PG strict-zero-loss falsification; same best-effort posture applies to MySQL Phase 2.
- `docs/adr/adr-0010-idempotent-applier.md` — the INSERT...ON DUPLICATE KEY UPDATE semantics that absorb the [snapshot-binlog-pos, filter-flip-observed] overlap.
- `docs/adr/adr-0021-publication-scope-by-table.md` — schema-cache WARN-and-skip pattern that sits beneath the filter (the "robustness layer" referenced above).
- `docs/adr/adr-0025-graceful-drain-stop.md` — control-table-based signaling rationale; this ADR reuses the same poll infrastructure.
- `internal/engines/mysql/filter_flip_verify_integration_test.go` — Phase A verification artifact.
