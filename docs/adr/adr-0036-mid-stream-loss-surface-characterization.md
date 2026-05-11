# ADR-0036: Mid-stream live add-table residual loss surface — Phase A characterization

## Status

**Phase A.1 run complete (2026-05-10, Vultr vhf-3c-8gb / Postgres 16.13 / Linux).** 6 runs of the diagnostic test produced stable verdicts:

| Mechanism | Outcome | Key signature |
|---|---|---|
| M1 — Long txns straddling pub-add | FAILS in 5/6, HOLDS in 1/6 (1 affected row) | Loader uses implicit single-statement txns; rare straddler when loader cadence and ALTER coincide |
| M2 — Snapshot consistent-point race | **FAILS in 6/6** | `lsn_snapshot ≥ lsn_pubadd_after` invariant always held |
| M3 — pgoutput catalog-snapshot lag | **INCONCLUSIVE in 6/6** but data anomalous | `rel_first_event_lsn` consistently lands **inside** the `[lsn_pubadd_before, lsn_pubadd_after]` window (delta_bytes = -184 to -624) — the existing M3 threshold of "delta_bytes positive and large" doesn't match the observed shape |
| M4 — Test-side counter artifact | **FAILS in 6/6** with `missing_count > 0` | Real loss; counter agrees with source-side row count |

**Loss is small but reproducible.** 1–2 rows out of 17–23 across runs (~5–9%). Missing rows are always among the loader rows that committed in the same time-window as the ALTER PUBLICATION transaction (load-15 / 16 / 18 / 19 / 20 in the 6 runs). The mechanism is the WAL-window between ALTER PUBLICATION's BEGIN and the catalog-effective LSN where pgoutput first sees the new table in scope — a sharper articulation than "M3 catalog-snapshot lag" as originally framed.

**Phase A.2 run complete (2026-05-11, Vultr `-count=10`).** Per-row source LSN cross-reference falsifies the reframed M3 hypothesis: **10/10 runs show every missing row's commit LSN is STRICTLY AFTER `lsn_pubadd_after`** (`missing_inside=0` in every run). The loss is not in the ALTER PUBLICATION transaction's WAL window — it's in a later window that ends before the post-add-sentinel reliably arrives. A fifth mechanism (M5) is in play; three candidate descriptions are documented under Phase A.2's verdict section. **Phase A.3 needed** before any production fix to distinguish pgoutput-internal catalog lag (M5a), streamer-snapshot-handoff race (M5b), or applier-side drop (M5c). Path B vs Path C decision is contingent on Phase A.3 mechanism attribution.

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
Run 1: VERDICT_M1: long_txn_observed=0 affected_rows=0 txns_total=9  FAILS
Run 2: VERDICT_M1: long_txn_observed=0 affected_rows=0 txns_total=8  FAILS
Run 3: VERDICT_M1: long_txn_observed=0 affected_rows=0 txns_total=9  FAILS
Run 4: VERDICT_M1: long_txn_observed=1 affected_rows=1 txns_total=10 HOLDS
Run 5: VERDICT_M1: long_txn_observed=0 affected_rows=0 txns_total=11 FAILS
Run 6: VERDICT_M1: long_txn_observed=0 affected_rows=0 txns_total=11 FAILS
```

**Empirical reading:** M1 is a contributing surface 1-in-6 runs at 1 affected row. Not the dominant mechanism; rare and small magnitude. The straddling txn in run 4 was a coincidence of loader cadence with ALTER's BEGIN — not a workload pattern that requires the v0.24.0 best-effort behavior to be redesigned around. Could be addressed by either (a) advisory wait for in-flight long txns to drain before ALTER, or (b) accepting the minor exposure as part of best-effort.

Diagnostic-line shape: `VERDICT_M1: long_txn_observed=N affected_rows=K txns_total=T <HOLDS|FAILS|INCONCLUSIVE>`.

Verdict interpretation:
- HOLDS — straddling txns observed AND their row events landed in the captured trace; confirms the mechanism produces deliverable events that we'd then filter or not depending on pgoutput's catalog-snapshot decision at the row record's LSN.
- FAILS — no straddling txns observed in this run (burst loader as expected uses single-statement implicit txns); rules M1 OUT for the observed sample. (Cannot generalize to long-txn workloads without a separate test.)
- INCONCLUSIVE — straddling txns observed but row events for them not in the trace; could indicate filtering at the dispatch layer that the trace doesn't see.

#### M2. Snapshot consistent-point race (LSN_S vs LSN_pubadd)

Hypothesis: if LSN_S < LSN_pubadd, rows committed in the gap (LSN_S, LSN_pubadd) would be neither in the snapshot's MVCC view nor delivered by pgoutput post-publication-add (because pgoutput at LSNs in that gap still uses the historical catalog from before the publication-add commit). ADR-0030's "what could go wrong" item 1 names this as the hazard the orchestrator's invariant check guards against; the invariant has held in every observed v0.24.0 run, so the standard expectation is FAILS (mechanism not active in practice). Phase A confirms or refutes that.

```
Run 1: VERDICT_M2: lsn_snapshot=0/1D8C978 lsn_pubadd_before=0/1D8BD00 lsn_pubadd_after=0/1D8C568 ordering=after FAILS
Run 2: VERDICT_M2: lsn_snapshot=0/1D8C5E8 lsn_pubadd_before=0/1D8BC48 lsn_pubadd_after=0/1D8C240 ordering=after FAILS
Run 3: VERDICT_M2: lsn_snapshot=0/1D8C6E8 lsn_pubadd_before=0/1D8BB90 lsn_pubadd_after=0/1D8C188 ordering=after FAILS
Run 4: VERDICT_M2: lsn_snapshot=0/1D8C6A0 lsn_pubadd_before=0/1D8BC48 lsn_pubadd_after=0/1D8C340 ordering=after FAILS
Run 5: VERDICT_M2: lsn_snapshot=0/1D8CB30 lsn_pubadd_before=0/1D8BDB8 lsn_pubadd_after=0/1D8C620 ordering=after FAILS
Run 6: VERDICT_M2: lsn_snapshot=0/1D8CA30 lsn_pubadd_before=0/1D8BDB8 lsn_pubadd_after=0/1D8C520 ordering=after FAILS
```

**Empirical reading:** M2 conclusively ruled out. The orchestrator's publication-add-then-snapshot ordering invariant holds in every observed run; the snapshot's consistent point is always at-or-after `lsn_pubadd_after`. Mechanism is not active.

Diagnostic-line shape: `VERDICT_M2: lsn_snapshot=X lsn_pubadd_before=Y lsn_pubadd_after=Z ordering=<before|equal|after> <HOLDS|FAILS|INCONCLUSIVE>`.

Verdict interpretation:
- HOLDS — `ordering=before` (LSN_S < LSN_pubadd_after); the gap exists; the orchestrator's invariant check would have caught it but didn't fire because the threshold is `LSN_S < slot_confirmed_flush_lsn`, not `LSN_S < LSN_pubadd`. This would mean the mechanism is real and the existing invariant is the wrong shape.
- FAILS — `ordering=after` or `equal` (LSN_S ≥ LSN_pubadd_after); the publication-add-then-snapshot ordering holds; rules M2 OUT.

#### M3. pgoutput catalog-snapshot lag

Hypothesis: between the WAL commit of `ALTER PUBLICATION ... ADD TABLE` (at LSN_pubadd) and the active stream's slot's pgoutput-internal recognition of the new table membership, there's a window where the slot decodes events at LSN ≥ LSN_pubadd but its catalog cache hasn't yet refreshed, so events on the new table in that window are filtered.

```
Run 1: VERDICT_M3: rel_first_event_lsn=0/1D8C2F8 lsn_pubadd_after=0/1D8C568 delta_bytes=-624 INCONCLUSIVE
Run 2: VERDICT_M3: rel_first_event_lsn=0/1D8C188 lsn_pubadd_after=0/1D8C240 delta_bytes=-184 INCONCLUSIVE
Run 3: VERDICT_M3: rel_first_event_lsn=0/1D8C0D0 lsn_pubadd_after=0/1D8C188 delta_bytes=-184 INCONCLUSIVE
Run 4: VERDICT_M3: rel_first_event_lsn=0/1D8C188 lsn_pubadd_after=0/1D8C340 delta_bytes=-440 INCONCLUSIVE
Run 5: VERDICT_M3: rel_first_event_lsn=0/1D8C3B0 lsn_pubadd_after=0/1D8C620 delta_bytes=-624 INCONCLUSIVE
Run 6: VERDICT_M3: rel_first_event_lsn=0/1D8C3B0 lsn_pubadd_after=0/1D8C520 delta_bytes=-368 INCONCLUSIVE
```

**Empirical reading: data shape is anomalous and important.** The original ADR's HOLDS interpretation expected `delta_bytes > 0` (first event for the new table arrives well AFTER pubadd_after — classic catalog-cache lag). Observed: `delta_bytes` is **always negative** (-184 to -624), meaning the first delivered event for `events` lands at an LSN that's STRICTLY INSIDE the [lsn_pubadd_before, lsn_pubadd_after] window — i.e., during the ALTER PUBLICATION transaction's WAL footprint. All `lsn_pubadd_before` LSNs are `0/1D8BC48`-ish and all `rel_first_event_lsn`s are `0/1D8C0D0`-ish through `0/1D8C3B0`-ish, confirming the first delivered event sits roughly halfway inside the ALTER's WAL window across runs.

**What this means:** the orchestrator's `lsn_pubadd_after` (captured AFTER the SQL command returns) is fatter than the actual catalog-effective LSN. The actual commit LSN for the catalog change lands somewhere INSIDE [lsn_pubadd_before, lsn_pubadd_after]. Rows committed at LSN < (true catalog-commit LSN) are filtered by pgoutput; rows at LSN ≥ (true catalog-commit LSN) are delivered. The orchestrator can't see this boundary precisely with the current instrumentation.

**M3 reframed**: not "catalog-snapshot lag at the slot" but "the WAL-window between ALTER PUBLICATION's BEGIN and the catalog-effective LSN where pgoutput first sees the new table in scope." This is consistent with both the rel_first_event landing inside the window AND with rows committed in the same window being filtered. The two observations point to the same surface.

**Why INCONCLUSIVE remains:** the test's threshold logic in `internal/pipeline/add_table_live_pg_diagnose_integration_test.go` requires positive delta_bytes for HOLDS. The observed shape needs Phase A.2 instrumentation to confirm: cross-reference each missing row's source-side commit LSN against the [pubadd_before, pubadd_after] window. If missing rows land in that window, M3 (reframed) is confirmed.

Diagnostic-line shape (Phase A.1 + Phase A.2 fields merged): `VERDICT_M3: rel_first_event_lsn=X lsn_pubadd_before=B lsn_pubadd_after=Y delta_bytes=N missing_total=T missing_before=Bn missing_inside=In missing_after=An missing_unknown=Un inside_preview=[...] outside_preview=[...] <HOLDS_reframed|FAILS|INCONCLUSIVE>`.

Verdict interpretation (reframed for Phase A.2):
- HOLDS_reframed — every missing row's commit LSN (captured at INSERT time by the loader) falls INSIDE [lsn_pubadd_before, lsn_pubadd_after]. Confirms M3 (reframed): rows committed during the ALTER PUBLICATION transaction's WAL window are filtered by pgoutput's per-LSN catalog snapshot.
- FAILS — no missing rows fall inside the window; reframed M3 doesn't hold and a fifth mechanism is in play. Phase A.3 needed.
- INCONCLUSIVE — mixed classification (some missing rows inside, some outside) suggests M3-reframed contributes but isn't the only surface; OR no missing rows captured this run; OR loader LSN capture failed for the missing rows.

### Phase A.2: per-row source LSN cross-reference

Phase A.1 left M3 INCONCLUSIVE with anomalously negative `delta_bytes` (the first delivered event for the new table consistently lands INSIDE the ALTER PUBLICATION transaction's WAL window). The reframed hypothesis is that rows committed inside that same WAL window are filtered. Phase A.2 directly tests it by capturing the source-side commit LSN for every loader row at INSERT time and cross-referencing missing rows' LSNs against the [pubadd_before, pubadd_after] window.

**Instrumentation** (added in `internal/pipeline/add_table_live_pg_diagnose_integration_test.go`):

- The burst loader goroutine, after each `INSERT INTO events (body) VALUES ($1)` returns, additionally runs `SELECT pg_current_wal_lsn()` and stores `(body, lsn)` into an `lsnByBody` map (`map[string]string` guarded by a `sync.Mutex`). The captured LSN is the WAL flush position immediately after the implicit commit — a conservative upper bound on the row's actual commit LSN. For Phase A.2's classification question ("did this row commit inside the [pubadd_before, pubadd_after] window?"), an upper bound suffices: if `lsn_after_insert ≤ pubadd_after`, the commit definitely landed at-or-before pubadd_after.
- LSN capture failures are non-fatal — they bump a `missing_unknown` counter rather than abort the loader or skew the counter.
- At end-of-test, `renderVerdictM3` walks the set-diff's missing-row list, looks up each row's captured LSN, and classifies it into one of four buckets: `before` (lsn < pubAddBefore), `inside` (pubAddBefore ≤ lsn ≤ pubAddAfter), `after` (lsn > pubAddAfter), or `unknown` (LSN not captured or unparseable). The verdict is rendered from these totals per the interpretation table above.

**Verdict captures** (Vultr vhf-3c-8gb, Postgres 16, `-count=10`, 2026-05-11):

```
Run 1:  missing_total=2 inside=0 after=2 unknown=0   outside=[load-17@0/1D8CA78(after) load-18@0/1D8CC30(after)]   FAILS
Run 2:  missing_total=1 inside=0 after=1 unknown=0   outside=[load-19@0/1D8CCE8(after)]                            FAILS
Run 3:  missing_total=2 inside=0 after=2 unknown=0   outside=[load-19@0/1D8CCE8(after) load-20@0/1D8CEA0(after)]   FAILS
Run 4:  missing_total=1 inside=0 after=1 unknown=0   outside=[load-17@0/1D8CA78(after)]                            FAILS
Run 5:  missing_total=3 inside=0 after=3 unknown=0   outside=[load-17@0/1D8CB78(after) load-18@0/1D8CD30(after) load-19@0/1D8CEE8(after)]   FAILS
Run 6:  missing_total=2 inside=0 after=2 unknown=0   outside=[load-18@0/1D8CB30(after) load-19@0/1D8CBE8(after)]   FAILS
Run 7:  missing_total=1 inside=0 after=1 unknown=0   outside=[load-18@0/1D8CC30(after)]                            FAILS
Run 8:  missing_total=1 inside=0 after=1 unknown=0   outside=[load-17@0/1D8C978(after)]                            FAILS
Run 9:  missing_total=1 inside=0 after=1 unknown=0   outside=[load-17@0/1D8C978(after)]                            FAILS
Run 10: missing_total=2 inside=0 after=2 unknown=0   outside=[load-19@0/1D8CBE8(after) load-20@0/1D8CD70(after)]   FAILS
```

**Empirical reading: reframed M3 conclusively FALSIFIED in 10/10 runs.** Across all runs, every missing row's commit LSN is STRICTLY AFTER `lsn_pubadd_after` by ~1.5KB–3KB. `missing_inside` is 0 in every run. The hypothesis that "rows committed inside the ALTER PUBLICATION transaction's WAL window are filtered" does not hold — the lost rows are NOT in that window. They're all from the tail of the loader's burst (load-17 through load-20), committed well after the publication-add transaction completed.

The post-add-sentinel row (inserted at the very end of the test, well past pubadd_after AND past the missing rows' LSNs) IS reliably delivered in every run. So the gap is a specific window AFTER pubadd_after that ends before the post-add-sentinel — the lost rows sit in a finite slice of the timeline that's neither in the snapshot nor in the streamer's delivered output.

**A fifth mechanism (M5) is in play.** Candidate descriptions:
- **M5a: streamer's pgoutput-internal catalog catches up lazily after the visible catalog change.** Even though `pg_publication_rel` is updated at LSN_pubadd_commit (somewhere in `[lsn_pubadd_before, lsn_pubadd_after]`), the streamer's pgoutput may not refresh its catalog view immediately; events between LSN_pubadd_commit and "pgoutput-refresh-LSN" stream past but are filtered as if `events` weren't in scope. This would explain the after-window loss.
- **M5b: streamer-side apply race with the snapshot handoff.** ADR-0030's flow opens a temp slot at LSN_S ≥ pubadd_after, bulk-copies rows visible at LSN_S, then hands off to the streamer. If the existing streamer's confirmed_flush_lsn is past LSN_S (because it was already running and ACKed past the snapshot's LSN), events between (some LSN < LSN_S) and (some LSN > LSN_S) may slip through the gap.
- **M5c: applier-side drop of events that pgoutput delivered.** If pgoutput delivered the events but the applier's batched-apply path lost them (e.g. an early termination of the apply loop when `addtable.Run` returns), the loss would show up as data-on-target-missing despite pgoutput visibility.

Distinguishing M5a / M5b / M5c requires **Phase A.3**:
- Add per-event applier-side instrumentation that records `(relation, lsn, body)` for every Insert reaching the applier. At end-of-test, cross-reference missing rows' LSNs against this applier-side log.
- If missing-row LSN appears in applier log → M5c (applier dropped). 
- If absent → M5a or M5b (pgoutput filtered or streamer-snapshot-handoff race).
- Server-side `log_min_messages=DEBUG2` would further distinguish M5a from M5b but is heavyweight.

**Decision deferred until Phase A.3 narrows further.** Path B (dual-slot) was the M3-HOLDS fix; with reframed M3 falsified, Path B may or may not address the actual mechanism. Path C (operator quiesce + LOCK TABLE on events) closes all M5 variants by stopping writes during the snapshot+handoff window — operator-friendly even if "not strictly live". The Path B vs Path C choice is now contingent on Phase A.3's mechanism attribution.

#### M4. Test-side counter artifact

Hypothesis: the under-load test's `finalInserted` counter (incremented after successful Exec returns from the loader goroutine) is not perfectly synchronized with what's actually committed on the source. Phase A: query the source for actual committed `load-*` rows by `body` content and set-diff against the target's delivered `load-*` rows. If `finalInserted != source_committed`, the counter is wrong and some of the "loss" is fictitious. If they agree but `target_delivered < source_committed`, the loss is real.

```
Run 1: VERDICT_M4: source_committed=20 target_delivered=19 counter=20 missing_count=1 missing_ids=[load-18]               FAILS (real loss)
Run 2: VERDICT_M4: source_committed=17 target_delivered=16 counter=17 missing_count=1 missing_ids=[load-15]               FAILS (real loss)
Run 3: VERDICT_M4: source_committed=17 target_delivered=16 counter=17 missing_count=1 missing_ids=[load-15]               FAILS (real loss)
Run 4: VERDICT_M4: source_committed=19 target_delivered=18 counter=19 missing_count=1 missing_ids=[load-16]               FAILS (real loss)
Run 5: VERDICT_M4: source_committed=23 target_delivered=21 counter=23 missing_count=2 missing_ids=[load-19, load-20]      FAILS (real loss)
Run 6: VERDICT_M4: source_committed=23 target_delivered=21 counter=23 missing_count=2 missing_ids=[load-19, load-20]      FAILS (real loss)
```

**Empirical reading:** counter is correct in every run; under-load test's "best-effort gap" measurement is NOT a counter artifact. Loss is real. 1-2 rows out of 17-23 (~5-9% in this scenario; CI's under-load test reports ~36% at higher burst rates which are not reproduced here on the 3-vCPU Vultr box). Missing IDs cluster at the upper end of the loader's output — consistent with rows committed during the ALTER PUBLICATION window being the affected ones.

Diagnostic-line shape: `VERDICT_M4: source_committed=N target_delivered=K counter=C missing_count=M missing_ids_preview=[...] <HOLDS|FAILS>`.

Verdict interpretation:
- HOLDS — counter ≠ source_committed; the under-load test's loss measurement is partly an artifact of how it counts.
- FAILS (with missing_count > 0) — counter is right; rows are genuinely missing on the target. The loss is real and one of M1/M2/M3 (or a fifth mechanism) is responsible.
- FAILS (with missing_count == 0) — counter is right and target delivered every committed loader row; no loss in this run (could mean the burst rate or scheduler behavior in the diagnostic test container differs from CI's; re-run with adjusted parameters).

## What we still don't know (Phase A.2 candidate work)

Phase A.1 narrowed the hypothesis space but left M3 INCONCLUSIVE. Open questions to drive Phase A.2:

- **Per-row source-side commit LSN.** The current instrumentation captures source-vs-target row diff at end-of-test but doesn't record each loader row's commit LSN. Phase A.2 should add a source-side `pg_current_wal_lsn()` capture immediately after each loader INSERT commits, and on diff-time cross-reference each missing row's LSN against [lsn_pubadd_before, lsn_pubadd_after]. This converts M3 from INCONCLUSIVE to definitive.
- **Catalog-effective LSN precision.** The orchestrator currently bounds the catalog change with [lsn_pubadd_before, lsn_pubadd_after] but can't see the exact commit LSN. Probing `pg_publication_rel`'s txid via `pg_xact_commit_timestamp` (if `track_commit_timestamp=on`) could pin the boundary tighter.
- **Per-event filter decisions inside pgoutput.** The Go side observes what pgoutput delivered, not what it filtered. Server-side `log_min_messages=DEBUG2` would surface filter decisions but is heavyweight. Defer until Phase A.2 + the LSN cross-reference fail to attribute the loss.
- **Workload variance.** The diagnostic test fixes a 10 ms loader cadence on a 3-vCPU box. CI's under-load test reports ~36% loss at 1000 rows / sub-second on 4-vCPU runners. Phase A.2 should run with the higher cadence to confirm the loss-LSN-pattern scales linearly (more affected rows, same window-attribution shape) vs nonlinearly (a different mechanism kicks in at high burst).
- **Cross-engine extensibility.** Phase A is PG-only. MySQL Phase 2 (ADR-0034) ships its own filter-flip mechanism with the same best-effort caveat; whether the same loss surface translates is a separate Phase A.

## Decision

**Phase A.1 verdicts:** M1 contributes rarely (1/6 runs, 1 row); M2 ruled out; M3 INCONCLUSIVE but the data shape strongly suggests a reframed M3 (WAL-window between ALTER's BEGIN and catalog-effective LSN) is the dominant mechanism; M4 ruled out (loss is real). **Pre-conditions for committing to Path B (dual-slot) are NOT yet met** — Phase A.1 didn't conclusively pin M3 with positive lag delta. **Pre-conditions for Path C (operator quiesce) are met regardless** — it works for any mechanism.

**Recommended next step: Phase A.2 before any production fix.** Refine the diagnostic instrumentation to cross-reference each missing row's source-side commit LSN against the [pubadd_before, pubadd_after] window. If missing rows fall inside that window, M3 (reframed) is confirmed and Path B becomes the right mitigation. If missing rows fall OUTSIDE the window, a fifth mechanism is in play and a deeper trace is needed.

**Path C remains available regardless** — `LOCK TABLE ... IN SHARE MODE` (or coordinated app quiesce) around the publication-add → snapshot pair would close the surface for any of M1/M3/M5 at the cost of a brief write pause on the new table. Operator-friendly; should be documented as the workaround for operators who can quiesce briefly even before the strict-zero-loss code lands.

**Mechanism-specific decisions for the next iteration:**

- **If Phase A.2 confirms M3 (reframed) HOLDS:** Path B (dual-slot) is the right structural fix. The second slot, created at-or-after the catalog-effective LSN, sees the new table in pgoutput's scope from the start; the WAL-window's filtered events get re-decoded by the second slot. ~1500-2000 LOC per ADR-0033's estimate.
- **If Phase A.2 reveals a fifth mechanism (M5) we haven't enumerated:** characterize it before committing to a fix shape. Server-side `log_min_messages=DEBUG2` trace may be the next instrumentation tier.
- **If M1 frequency increases under longer-running workloads:** add a "wait for clean snapshot" preflight that delays ALTER until no active txn started before the call has been open > N ms. Bolt-on; not a structural redesign.

**v0.24.0 best-effort property remains the shipping behavior** until the next iteration commits to a path. Loss is small (~5-9% in the diagnostic test's parameters; up to ~36% in CI's higher-burst under-load test) and confined to rows committed inside the ALTER PUBLICATION's narrow WAL window. ADR-0030's documented best-effort caveat continues to apply; nothing in Phase A.1 invalidates the existing release behavior.

## Forward options

- **Path B — dual-slot.** Reserved for the M3-HOLDS case per ADR-0033's forward-options list. Not pursued ahead of Phase A's verdicts to avoid the speculate-and-patch trap.
- **Path C — source quiesce.** Operator-coordinated brief lock on the new table around publication-add. Documented in `docs/postgres-source-prep.md` as the workaround if best-effort isn't acceptable. Always available regardless of Phase A's verdicts.
- **Continue with v0.24.0 best-effort.** If Phase A's verdicts land in FAILS-or-INCONCLUSIVE space across all four mechanisms, the right call is to update ADR-0030 to make the best-effort property more visible (e.g. surface a per-run estimate of in-flight loss in the success log) and call the ground covered.

## See also

- `docs/adr/adr-0030-mid-stream-live-add-table.md` — Phase 2 design and the "What could go wrong" entry this ADR characterizes.
- `docs/adr/adr-0033-mid-stream-live-add-strict-zero-loss.md` — Phase A verification that falsified Path A; ADR-0036 is the next iteration of its "Forward options" Path D entry.
- `internal/pipeline/add_table_live_pg_diagnose_integration_test.go` — the Phase A diagnostic test artifact. Re-run on any future chunk that revisits the v0.24.0 loss surface.
- `docs/dev/notes/path-d-phase-a-status.md` — runbook for executing the diagnostic test on the Vultr box.
