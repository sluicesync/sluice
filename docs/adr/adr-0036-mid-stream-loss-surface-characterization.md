# ADR-0036: Mid-stream live add-table residual loss surface — Phase A characterization

## Status

**Phase A pending run.** Branch `path-d-phase-a-diagnose` carries the targeted DEBUG-level slog instrumentation in the v0.24.0 mid-stream live add-table (`--no-drain`, PG-only) flow plus a single integration test that captures the diagnostic logs and emits VERDICT_M[1-4] lines per the four candidate mechanisms ADR-0033 § "What we still don't know" enumerated. The verdict lines below are intentionally left as TODOs until the test is run on the Vultr box (see `docs/dev/notes/path-d-phase-a-status.md` for the runbook). The ADR will be amended with the captured numbers and a Decision before any production fix is considered.

This ADR is the operator-facing record of "we instrumented, here's what the data said." Path A (slot-pause) remains falsified per ADR-0033 — this Phase A is Path D ("diagnose the actual v0.24.0 loss surface FIRST") from ADR-0033's "Forward options" list.

## Context

ADR-0030 shipped Phase 2 mid-stream live add-table in v0.24.0 with a documented best-effort gap: events on the new table inserted during the brief publication-add window may not be delivered. The under-load CI test `TestAddTable_LiveMode_PG_UnderLoad` exhibits ~36% loss at high burst rates (1000 rows / sub-second) of the in-flight load-* rows; snapshot rows + post-add CDC are pinned and pass.

ADR-0033's Phase A verification falsified Path A (slot-pause): pgoutput's per-LSN historical catalog snapshot pins publication membership at decode time, so a re-decode pass at the same LSN produces the same filtering result. ADR-0033's complement test confirmed H2 (the temp-slot snapshot DOES capture pre-publication-add rows on the new table), so the loss does NOT come from rows missing from the snapshot itself. Yet the under-load test reliably loses ~36% of in-flight load-* rows. ADR-0033's "What we still don't know" section enumerated four candidate mechanisms — this ADR's Phase A test characterizes which one(s) actually hold.

## Phase A: empirical verification

### Diagnostic instrumentation — what we capture and where

The instrumentation is gated behind DEBUG-level slog (invisible in normal runs; surfaces with `--log-level=debug` or in the diagnostic test which forces a JSON debug-handler). Four log-tag families:

- **`addtable.diag` — phase=`pub-add-window`** in `internal/pipeline/add_table.go::AddTable.Run`. Logs `pg_current_wal_lsn()` immediately before AND after `ALTER PUBLICATION ... ADD TABLE` runs. Bounds the LSN window for the catalog change.
- **`addtable.diag` — phase=`snapshot-open`** in `internal/pipeline/add_table.go::AddTable.Run`. Logs the snapshot stream's consistent-point LSN alongside the LSN_pubadd window above. Drives M2 verdict.
- **`cdc.diag` — phase=`begin`/`commit`** in `internal/engines/postgres/cdc_reader.go::dispatchWAL`. Logs the WAL position of every BEGIN message (`xld.WALStart`) and the FinalLSN/CommitLSN of the corresponding txn. Drives M1 verdict.
- **`cdc.diag` — phase=`row`** in `internal/engines/postgres/cdc_reader.go::dispatchWAL` for every Insert/Update/Delete event. Logs the row's WAL LSN, the txn boundaries, the relation name, and a `first_seen_for_rel` flag. Drives M3 verdict (first event LSN per relation vs LSN_pubadd) and is the dispatch-level ground truth for what the slot actually delivered.
- **`cdc.diag` — phase=`relation`** in `internal/engines/postgres/cdc_reader.go::dispatchWAL` for every RelationMessage. Records when pgoutput first emits the relation entry for the new table.

### The diagnostic test

`internal/pipeline/add_table_live_pg_diagnose_integration_test.go::TestAddTable_LiveMode_PG_DiagnoseLossSurface` runs the same burst-writer scenario as `TestAddTable_LiveMode_PG_UnderLoad` (50 seed rows + sustained loader at 10 ms cadence + post-add sentinel) but additionally:

1. Installs a JSON slog handler at DEBUG level capturing every diag line into an in-memory buffer.
2. After the live add completes and CDC drains, performs a set-diff between source-side committed `load-*` rows (queried back via `SELECT body FROM events`) and target-side delivered `load-*` rows.
3. Parses the captured logs and emits four `VERDICT_M[1-4]` `t.Logf` lines naming the empirical result for each mechanism.

The test does NOT enforce zero-loss assertions — that's the existing under-load test's job. This test is purely observational; its output is the ADR's data.

### Verdict per mechanism

> The verdict lines are TODO until the diagnostic test runs against a real Postgres on the Vultr box. The test emits each line via `t.Logf` so they show up in `go test -v` output verbatim. Quote them here directly when copying back from the run.

#### M1. Long-running transactions across the publication-add boundary

Hypothesis: a transaction whose BEGIN landed at WAL LSN < LSN_pubadd but whose COMMIT landed at WAL LSN ≥ LSN_pubadd would have its row events filtered by pgoutput's per-LSN catalog snapshot at decode time according to the catalog state at each row record's LSN — even though the publication includes the table at commit. Under the burst loader, every `INSERT INTO events` is its own implicit transaction with BEGIN+COMMIT on the same WAL flush, so this mechanism's expected magnitude is small. Worth empirically confirming.

```
VERDICT_M1: TODO_run_to_populate
```

Diagnostic-line shape: `VERDICT_M1: long_txn_observed=N affected_rows=K txns_total=T <HOLDS|FAILS|INCONCLUSIVE>`.

Verdict interpretation:
- HOLDS — straddling txns observed AND their row events landed in the captured trace; confirms the mechanism produces deliverable events that we'd then filter or not depending on pgoutput's catalog-snapshot decision at the row record's LSN.
- FAILS — no straddling txns observed in this run (burst loader as expected uses single-statement implicit txns); rules M1 OUT for the observed sample. (Cannot generalize to long-txn workloads without a separate test.)
- INCONCLUSIVE — straddling txns observed but row events for them not in the trace; could indicate filtering at the dispatch layer that the trace doesn't see.

#### M2. Snapshot consistent-point race (LSN_S vs LSN_pubadd)

Hypothesis: if LSN_S < LSN_pubadd, rows committed in the gap (LSN_S, LSN_pubadd) would be neither in the snapshot's MVCC view nor delivered by pgoutput post-publication-add (because pgoutput at LSNs in that gap still uses the historical catalog from before the publication-add commit). ADR-0030's "what could go wrong" item 1 names this as the hazard the orchestrator's invariant check guards against; the invariant has held in every observed v0.24.0 run, so the standard expectation is FAILS (mechanism not active in practice). Phase A confirms or refutes that.

```
VERDICT_M2: TODO_run_to_populate
```

Diagnostic-line shape: `VERDICT_M2: lsn_snapshot=X lsn_pubadd_before=Y lsn_pubadd_after=Z ordering=<before|equal|after> <HOLDS|FAILS|INCONCLUSIVE>`.

Verdict interpretation:
- HOLDS — `ordering=before` (LSN_S < LSN_pubadd_after); the gap exists; the orchestrator's invariant check would have caught it but didn't fire because the threshold is `LSN_S < slot_confirmed_flush_lsn`, not `LSN_S < LSN_pubadd`. This would mean the mechanism is real and the existing invariant is the wrong shape.
- FAILS — `ordering=after` or `equal` (LSN_S ≥ LSN_pubadd_after); the publication-add-then-snapshot ordering holds; rules M2 OUT.

#### M3. pgoutput catalog-snapshot lag

Hypothesis: between the WAL commit of `ALTER PUBLICATION ... ADD TABLE` (at LSN_pubadd) and the active stream's slot's pgoutput-internal recognition of the new table membership, there's a window where the slot decodes events at LSN ≥ LSN_pubadd but its catalog cache hasn't yet refreshed, so events on the new table in that window are filtered.

```
VERDICT_M3: TODO_run_to_populate
```

Diagnostic-line shape: `VERDICT_M3: rel_first_event_lsn=X lsn_pubadd_after=Y delta_bytes=N <HOLDS|FAILS|INCONCLUSIVE>`.

Verdict interpretation:
- HOLDS — `delta_bytes` is significantly larger than a single ALTER PUBLICATION WAL record (~few hundred bytes); pgoutput's first delivered event for the new table arrived well after publication-add committed, suggesting catalog-snapshot lag.
- FAILS — `delta_bytes` is near zero (the first event for the new table arrived almost immediately after LSN_pubadd); no observable lag.
- INCONCLUSIVE — no first-event-for-relation captured for the new table (suggests the events table never had any events delivered in the trace; could indicate the mechanism produced 100% loss in the captured window, in which case the absence is itself the signal — re-run with longer drain).

#### M4. Test-side counter artifact

Hypothesis: the under-load test's `finalInserted` counter (incremented after successful Exec returns from the loader goroutine) is not perfectly synchronized with what's actually committed on the source. Phase A: query the source for actual committed `load-*` rows by `body` content and set-diff against the target's delivered `load-*` rows. If `finalInserted != source_committed`, the counter is wrong and some of the "loss" is fictitious. If they agree but `target_delivered < source_committed`, the loss is real.

```
VERDICT_M4: TODO_run_to_populate
```

Diagnostic-line shape: `VERDICT_M4: source_committed=N target_delivered=K counter=C missing_count=M missing_ids_preview=[...] <HOLDS|FAILS>`.

Verdict interpretation:
- HOLDS — counter ≠ source_committed; the under-load test's loss measurement is partly an artifact of how it counts.
- FAILS (with missing_count > 0) — counter is right; rows are genuinely missing on the target. The loss is real and one of M1/M2/M3 (or a fifth mechanism) is responsible.
- FAILS (with missing_count == 0) — counter is right and target delivered every committed loader row; no loss in this run (could mean the burst rate or scheduler behavior in the diagnostic test container differs from CI's; re-run with adjusted parameters).

## What we still don't know

The verdicts above will narrow the hypothesis space. Open questions Phase A cannot directly answer:

- **Per-event filter decisions inside pgoutput.** The Go side can observe what pgoutput delivered, not what it decided to drop. If M3 verdicts as HOLDS, characterizing the lag's exact mechanism (publisher-side vs subscriber-side cache refresh? backend-process catalog snapshot vs pgoutput-internal? PG version sensitivity?) requires either pgoutput source reading or a server-side trace (`log_min_messages=DEBUG2`) that this branch doesn't ship.
- **Workload variance.** The diagnostic test fixes a 10 ms loader cadence, no concurrent transactions on `customers`, no DDL during the add window. CI's failures may carry workload variance the Vultr box doesn't replicate. If verdicts come back FAILS for everything but the under-load test continues to fail in CI, the next iteration must either reproduce CI's specific scheduler dynamics or move the diagnostic into the under-load test directly.
- **Cross-engine extensibility.** Phase A is PG-only. MySQL Phase 2 (ADR-0034) ships its own filter-flip mechanism with the same best-effort caveat; whether the same loss surface translates is a separate Phase A.

## Decision

> Decision is gated on the verdict lines. Do not write a fix without empirical attribution.

Three plausible decisions depending on the run:

- **If M2 HOLDS:** the mechanism is the orchestrator's. Fix shape: tighten the existing snapshot-LSN ≥ slot-LSN invariant to snapshot-LSN ≥ LSN_pubadd, and add a `LOCK TABLE ... IN ACCESS EXCLUSIVE MODE` (or equivalent serialization) around the publication-add → snapshot pair so the consistent-point necessarily lands at-or-after LSN_pubadd. Limited blast radius; still operator-friendly.
- **If M3 HOLDS:** the mechanism is pgoutput's. The fix is structural — Path B (dual-slot) becomes the correct mitigation since the second slot would be created at-or-after LSN_pubadd and avoid the lag window entirely. Larger code change; deterministic test fixtures need careful construction.
- **If M4 HOLDS (counter artifact):** the mechanism is the test's. Fix the under-load test's counter (use the source-side set-diff already implemented in this branch as the authority) and the documented v0.24.0 best-effort property may need to be re-stated as actually-zero-loss-effectively, with the original measurement having been misleading.
- **If M1 HOLDS:** the mechanism is workload-dependent (long open transactions during the add window). Fix shape: refuse the live add when long-running transactions are present on the source (operator coordination), or add a wait-for-clean-snapshot step.
- **If all four FAIL or are INCONCLUSIVE:** Phase A failed to characterize the surface. Next iteration: either deepen the instrumentation (server-side trace, pgoutput source reading) or accept the v0.24.0 best-effort property as the shipping behavior with no further mitigation attempts beyond Path C (operator-coordinated quiesce).

## Forward options

- **Path B — dual-slot.** Reserved for the M3-HOLDS case per ADR-0033's forward-options list. Not pursued ahead of Phase A's verdicts to avoid the speculate-and-patch trap.
- **Path C — source quiesce.** Operator-coordinated brief lock on the new table around publication-add. Documented in `docs/postgres-source-prep.md` as the workaround if best-effort isn't acceptable. Always available regardless of Phase A's verdicts.
- **Continue with v0.24.0 best-effort.** If Phase A's verdicts land in FAILS-or-INCONCLUSIVE space across all four mechanisms, the right call is to update ADR-0030 to make the best-effort property more visible (e.g. surface a per-run estimate of in-flight loss in the success log) and call the ground covered.

## See also

- `docs/adr/adr-0030-mid-stream-live-add-table.md` — Phase 2 design and the "What could go wrong" entry this ADR characterizes.
- `docs/adr/adr-0033-mid-stream-live-add-strict-zero-loss.md` — Phase A verification that falsified Path A; ADR-0036 is the next iteration of its "Forward options" Path D entry.
- `internal/pipeline/add_table_live_pg_diagnose_integration_test.go` — the Phase A diagnostic test artifact. Re-run on any future chunk that revisits the v0.24.0 loss surface.
- `docs/dev/notes/path-d-phase-a-status.md` — runbook for executing the diagnostic test on the Vultr box.
