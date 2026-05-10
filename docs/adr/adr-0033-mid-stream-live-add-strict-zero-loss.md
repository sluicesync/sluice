# ADR-0033: Mid-stream live add-table strict zero-loss — Phase A verification (Path A falsified)

## Status

Phase A complete; Path A's design hypothesis empirically falsified. Path B (dual-slot) and other mitigation paths remain on the table; no strict-zero-loss implementation has shipped. The verification test
(`internal/engines/postgres/slot_pause_verify_integration_test.go`) is committed as a permanent ground-truth artifact for the next iteration.

This ADR documents the empirical finding so the next chunk doesn't re-litigate it.

## Context

ADR-0030 shipped Phase 2 mid-stream live add-table (`--no-drain`, PG-only) in v0.24.0 with a documented best-effort gap (ADR-0030 § "What could go wrong" item 3): events on the new table inserted DURING the brief publication-add window may not be delivered. The under-load CI test (`TestAddTable_LiveMode_PG_UnderLoad`) explicitly relaxes its assertions — snapshot rows + post-add CDC are pinned (load-bearing); in-flight events during the add window are best-effort logged but not asserted. The v0.24.0 cycle's Scenario 4 exploration confirmed real loss: 0% at slow rate, ~36% at high-burst (1000 rows / sub-second).

The roadmap entry for this chunk picked **Path A — Slot-pause** as the strict-zero-loss approach:

> Temporarily stop the streamer's apply ack so the main slot retains WAL across the publication-add boundary, then re-decode the retained WAL with the updated publication state.

The proposed mechanism: pause the LSN tracker's ACKs (so the slot's `confirmed_flush_lsn` doesn't advance past `LSN_pre`), run `ALTER PUBLICATION ... ADD TABLE`, take the snapshot, bulk-copy, and then "rewind" the streamer to re-decode WAL from `LSN_pre` with the updated publication. The chunk prompt sketched two implementation variants for the rewind step: (a) sleep + rely on slot retention (no explicit re-issue of `START_REPLICATION`); (b) send a `SQ` message to pgoutput to force a re-read.

The chunk's own framing made the verification non-negotiable: "Phase A is non-negotiable; if hypotheses change, adjust before writing the fix." This ADR documents that adjustment.

## Phase A: empirical verification

The verification test (`TestSlotPause_Verify_RetainedWALReDecodesWithUpdatedPublication`) sets up a focused replication of the slot-pause scenario and asks one load-bearing question:

**Hypothesis H1.** When the slot's `confirmed_flush_lsn` is held at `LSN_pre` (the consumer never sends a standby-status update), and a new table is added to the publication AFTER the slot has streamed past `LSN_pre` once, can a subsequent `START_REPLICATION` at `LSN_pre` re-decode the WAL between [`LSN_pre`, now] with the updated publication scope and deliver events on the new table that occurred BEFORE the publication-add LSN?

**Test shape.**

1. Create publication `pub_existing` containing only `existing_tbl`.
2. Create logical slot `s_verify`. Capture `LSN_pre`.
3. INSERT on `existing_tbl` (`e1`).
4. CREATE TABLE `new_tbl` (NOT in publication).
5. INSERT on `new_tbl` at `LSN_F1` (BEFORE publication-add).
6. INSERT on `existing_tbl` (`e2`).
7. Pass 1: `START_REPLICATION` at `LSN_pre`. Drain. **Send NO standby-status update**, so `confirmed_flush_lsn` stays at `LSN_pre`.
8. Close the replication connection.
9. `ALTER PUBLICATION pub_existing ADD TABLE new_tbl;` lands at `LSN_pubadd`.
10. INSERT on `existing_tbl` (`e3`) and on `new_tbl` (`f2-post-pub-add`) at LSN > `LSN_pubadd`.
11. Confirm `pg_replication_slots.confirmed_flush_lsn` is still `LSN_pre`.
12. Pass 2: `START_REPLICATION` at `LSN_pre` again, with `pub_existing`. Drain.

**Question:** does Pass 2 contain `new_tbl(f1-pre-publication-add)` (the row whose INSERT had `LSN_F1` < `LSN_pubadd`)?

### Verdict: H1 FAILS

```
LSN_pre (before any test inserts):                    0/194A850
pass1 (before publication-add):                       2 events  [existing_tbl(e1) existing_tbl(e2)]
publication-add boundary:                             before=0/19560B0  after=0/1956520
confirmed_flush_lsn after pass1+ALTER:                0/194A850          (== LSN_pre — slot retention held)
pass2 (after publication-add, restart from LSN_pre):  4 events
                                                      [existing_tbl(e1)
                                                       existing_tbl(e2)
                                                       existing_tbl(e3)
                                                       new_tbl(f2-post-publication-add)]
```

`new_tbl(f1-pre-publication-add)` is **NOT delivered** on the restart-from-`LSN_pre` pass.

The `existing_tbl(e1)` and `existing_tbl(e2)` events ARE re-delivered (because `existing_tbl` was in the publication at all relevant LSNs). The `new_tbl(f2-post-publication-add)` event IS delivered (its LSN is past `LSN_pubadd`, so the catalog snapshot for that LSN includes `new_tbl` in scope). But `new_tbl(f1-pre-publication-add)`, whose WAL record sits at LSN < `LSN_pubadd`, is filtered both times.

This is consistent with how pgoutput evaluates publication membership: **at the historical catalog snapshot for the LSN being decoded**, not "is the table currently in the publication?" `ALTER PUBLICATION ADD TABLE` updates `pg_publication_rel`, but the catalog change itself commits at `LSN_pubadd`. Decoding events at LSN < `LSN_pubadd` uses the historical catalog from before the change, where `new_tbl` was not a member.

The slot's WAL retention IS working — `confirmed_flush_lsn` stayed at `LSN_pre`, the WAL was on disk, and the second `START_REPLICATION` successfully streamed records at LSN ≥ `LSN_pre`. But pgoutput's filter is LSN-aligned to the historical catalog, so the second pass produces the same filtering decision as the first pass for the same LSN.

### Complement: H2 HOLDS

The complement test (`verifyH2_TempSlotSnapshotCapturesPrePublicationAddRows`) addressed a separate question: does the temp-slot snapshot taken AFTER publication-add capture rows committed BEFORE publication-add on the new table?

```
verifyH2: temp slot created. consistent_point=0/196B940 snapshot_name=00000006-00000006-1
verifyH2: temp-slot snapshot view of h2_new: [h2-f1-pre-pub-add]
VERDICT_H2: HOLDS.
```

The temp-slot snapshot's MVCC view sees `h2_new(h2-f1-pre-pub-add)` — a row inserted before `ALTER PUBLICATION ADD TABLE` ran. This is standard PG MVCC: the snapshot's consistent point is at-or-after the publication-add LSN, and any committed row with LSN ≤ `LSN_S` is visible regardless of the publication scope (the publication scope only affects logical replication, not snapshot visibility).

**Implication:** v0.24.0's bulk-copy path correctly covers ALL rows committed on the new table BEFORE publication-add. The observed loss in `TestAddTable_LiveMode_PG_UnderLoad` must originate from a different surface than the one Path A targeted.

## Why H1 failing falsifies Path A

Path A's load-bearing claim: pause ACKs → ALTER PUBLICATION → restart streamer at `LSN_pre` → pgoutput re-decodes WAL with the now-updated publication and delivers the previously-filtered new_tbl events. **H1 demonstrates this does not happen.** pgoutput's per-LSN historical catalog snapshot pins publication membership at decode time according to the catalog state at that LSN; a second decode pass at the same LSN produces the same filtering result.

This eliminates the variant the chunk prompt sketched as option (a) ("Sleep the streamer briefly + rely on the slot retention"). It also strongly suggests option (b) ("Send a `SQ` message to pgoutput to force a re-read") wouldn't work either — `SQ` messages exist in the streaming-in-progress protocol for chunked txn delivery, not as a re-decode-from-LSN mechanism, and pgoutput's catalog snapshot logic doesn't have a knob for "use the current catalog instead of the historical one."

## What we still don't know

H1 falsifies Path A. H2 holds. So where does the v0.24.0 under-load test's observed ~36% loss actually come from?

Candidate mechanisms (not yet empirically tested):

1. **Long-running transactions across the publication-add boundary.** A transaction that started before `LSN_pubadd` and commits after may have its events filtered based on the catalog at transaction-start, not transaction-commit — so even events with WAL LSN > `LSN_pubadd` could be dropped if the txn started before. The under-load test's burst writer probably issues each INSERT as its own implicit transaction, but it's worth verifying.
2. **A bookkeeping race in the v0.24.0 orchestrator.** Some condition under which a row committed between snapshot start and snapshot consistent-point capture is excluded from the snapshot AND its CDC event is filtered.
3. **A pgoutput-internal lag** between the WAL commit of `ALTER PUBLICATION` and the catalog-snapshot update on the active stream's slot.
4. **A test-side artifact** — the under-load test's `finalInserted` counter tracks committed rows, but the count isn't perfectly synchronized with bulk-copy completion.

The next iteration of this chunk should start by characterizing the actual loss mechanism (with debug logging in the v0.24.0 path) before designing a mitigation. Targeting a hypothesised mechanism that hasn't been empirically confirmed is exactly the speculate-and-patch trap CLAUDE.md warns about.

## Decision

**Do not ship a slot-pause implementation in v0.27.0.** The Phase A verification has falsified the design's load-bearing premise. Implementing Path A as a "strict zero-loss" feature when we have empirical evidence that the mechanism cannot achieve zero loss would be a worse outcome than continuing to ship v0.24.0's documented best-effort path.

Land the verification test as a permanent regression artifact. Update ADR-0030's "What could go wrong" item 3 to reference this ADR. Update the roadmap to reflect that the chunk's mechanism choice is now an open question, not a chosen path.

## Forward options for the next iteration

The next chunk that picks this up should consider:

- **Path B — Strategy B (dual-slot)** as originally sketched in `docs/dev/design-mid-stream-add-table.md`. A second slot created at `LSN_pubadd` (or strictly after) that streams events on the new table from that point forward; main slot continues without scope change. The events at `LSN ∈ [LSN_pubadd, LSN_S]` are delivered by both slots' pgoutput streams (because both decode at LSN ≥ `LSN_pubadd` after the publication includes the table); idempotent applier handles overlap. The "LSN race" the original ADR-0030 wanted to avoid is the price; deterministic test fixtures will need careful construction.
- **Path C — Quiesce the source.** Take a brief `LOCK TABLE` or coordinate with the application to stop writes on the new table (or all tables) for the publication-add window. Operator-coordinated; not "live" in the strictest sense, but a viable middle ground for ops who can quiesce briefly.
- **Path D — Diagnose the actual v0.24.0 loss surface FIRST.** Per "what we still don't know" above. Maybe the loss is smaller-scope than thought and a targeted fix (e.g. preventing long-running transactions across the boundary, or improving the snapshot consistent-point capture) closes the gap without needing a dual-slot.

Path D is the most CLAUDE.md-aligned next step: it's the "instrument the actual failure surface, then design" pattern, not the "pick a mechanism, then build" pattern that produced this falsified iteration.

## See also

- `docs/adr/adr-0030-mid-stream-live-add-table.md` — Phase 2 design and the "What could go wrong" entry this ADR references.
- `docs/dev/design-mid-stream-add-table.md` — proto-ADR with Strategy B (dual-slot) sketch.
- `internal/engines/postgres/slot_pause_verify_integration_test.go` — the load-bearing Phase A test artifact. Re-run on any future chunk that revisits Path A or proposes a similar slot-retention-based mechanism.
