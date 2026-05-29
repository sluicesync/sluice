# ADR-0067: contiguous-segment rotation handoff (rotated chains are born compactable)

## Status

**Proposed (2026-05-29) — pending owner sign-off before implementation.**
Driven by Bug 95 (sluice-testing `BUG-CATALOG.md`): `sluice backup
compact --smart-compaction` can never merge across a rotation boundary
on a continuously-written PG source, so the headline "rotate a long
chain, then compact the churn" value-prop is unreachable via the
documented operator flow. Builds on ADR-0046 (native bounded-segment
lineage + inline rotation) and ADR-0064 (smart compaction). No code
written yet — this ADR locks the design first, per the "lay out the
design before touching the rotation/crash-recovery FSM" working
agreement (ADR-0046 calls the rotation correctness core the review
focus).

## Context

### What Bug 95 actually is (verified against code, not speculated)

A multi-segment chain produced by `backup stream run --retain-rotate-at`
has, between every consecutive segment, a position gap: prior segment's
`EndPosition` (`P_N`) **strictly precedes** the next segment's
`StartPosition` (`S`). The gap is **by design**, and it is a *real data
gap in incremental coverage*, not a metadata artifact. The decisive
mechanism is in two places:

- `stream_rotation.go::performRotation` caps the prior segment at `P_N`
  (its last committed TxCommit boundary), opens the next segment's full
  snapshot at anchor `S` (hard-asserting `S ≥ P_N`), and records the new
  segment `StartPosition = EndPosition = S` (the full's anchor).
- `stream.go:760-766` then sets `b.skipThrough = S`, which **drops every
  pump event whose position ≤ S** so the new segment's first incremental
  begins *strictly after* `S`. The `(P_N, S]` changes the pump would
  otherwise deliver are intentionally discarded because the new full's
  snapshot at `S` already reflects them.

Consequence: the `(P_N, S]` changes live **only** in the new segment's
full snapshot. Nothing in any incremental covers that range.

### Why restore is fine but compaction refuses (the asymmetry)

- **Restore** walks segments in order. Its segment-to-segment boundary
  validator (`chain_restore.go`, the `false` variant) tolerates
  `prev.End ≤ seg.Start` — a gap is fine, because the next segment's
  full@`S` re-seeds the complete state and *covers* the gap. (The
  *within-segment* full→first-incremental boundary is checked
  **exactly**: `full.End == firstIncr.Start == S`.)
- **Naive compaction** (`chain_compact.go`) merges N consecutive
  segments into one whose full = the **oldest** source's full and whose
  incrementals = the concatenation of every source's incrementals — and
  **discards every later segment's full** (`chain_compact.go:39-46`,
  `:731-745`). So the `(P_N, S]` range, which lived only in the discarded
  later full, becomes uncovered. `assertGroupBoundaryContiguous`
  (`chain_compact.go:646-660`) correctly refuses any merge group with a
  position gap to avoid this silent loss.

Because churn implies continuous writes, every rotation boundary on a
churny source gaps, so the contiguity pre-flight refuses every merge —
the feature is unreachable for exactly the workload it targets. The
refusal is *correct* (loud, no data loss); the gap is the problem.

### Why the "snapshot at a historical LSN" framing was wrong

Bug 95's option (a) framed the fix as "anchor the new full at exactly
`P_N`," which would need the source to produce a consistent base
snapshot *as of a past LSN*. PG cannot do that (an exported snapshot is
consistent at slot/snapshot creation = current). That framing is
infeasible and is **not** what this ADR proposes.

## Decision

**Make rotated segments born-contiguous by keeping the `(P_N, S]`
overlap in the new segment's incrementals instead of dropping it.** The
new segment's full is still taken at the current LSN `S` (no historical
snapshot); we simply stop discarding the CDC events the pump already
delivers for `(P_N, S]`, and we record the segment boundary honestly so
the lineage is gapless.

Concretely:

1. **Rotation (`stream.go` / `stream_rotation.go`):** set
   `skipThrough = P_N` (the prior segment's `EndPosition`) instead of
   `S`. The new segment's first incremental then begins at `P_N`,
   physically covering `(P_N, S]` (plus everything after). `StartPosition`
   is **unchanged** (`= S`, the full's anchor / restore base); a **new
   field `IncrementalCoverageStart = P_N`** records where the segment's
   incrementals begin.

2. **Add `LineageSegment.IncrementalCoverageStart` (new field) rather
   than overload `StartPosition`.** `StartPosition` keeps its existing
   meaning (the segment's full anchor = its restore base), so every
   existing reader of it is unaffected. `IncrementalCoverageStart` is the
   segment's earliest *incremental* coverage, which equals `P_N` for a
   rotated segment and may precede `StartPosition`. **Back-compat:** an
   empty/absent `IncrementalCoverageStart` (never-rotated single-segment
   chains, pre-existing chains) defaults to `== StartPosition` — i.e.,
   today's behavior. Only the two **contiguity** checks change to key off
   the new field:
   - `assertGroupBoundaryContiguous` (`chain_compact.go:646-660`):
     compare `prior.EndPosition` vs `cur.IncrementalCoverageStart`
     (`P_N == P_N` → contiguous → merge allowed). Still refuses a real
     gap.
   - restore's segment-to-segment boundary validator: validate
     `prior.End` against `cur.IncrementalCoverageStart` (gone:
     `prior.End == P_N`), not against `StartPosition (= S)`.

3. **Restore replays the idempotent overlap, made explicit by the new
   field.** On restore, the full@`S` is applied, then incrementals replay
   from `IncrementalCoverageStart (= P_N)`; the `(P_N, S]` events re-apply
   onto a snapshot that already contains them. This is **idempotent**
   under ADR-0010 — the *same* snapshot→CDC handoff dedup sluice already
   proves for the initial full→stream transition (ADR-0007/0010/0027),
   now exercised at each segment's restore. The within-segment
   full→first-incremental boundary check tolerates
   `firstIncr.Start ≤ full.End` *when* `IncrementalCoverageStart <
   StartPosition` (the field documents the expected overlap), and stays
   exact otherwise.

4. **Compaction is unchanged.** A born-contiguous chain merges by pure
   concat exactly as today; discarding the later full is now safe because
   `(P_N, S]` lives in the merged incrementals. Smart compaction
   (ADR-0064 §14e) likewise collapses over a gapless event stream. This
   fixes **both** modes, for **all** table shapes (PK and no-PK) and
   encrypted or not — because the fix is upstream of the compactor.

### What this costs

- **Standalone restore of any rotated segment now replays a small
  idempotent overlap** (`(P_N, S]`) it previously skipped. Correctness
  rests on idempotent apply (proven mechanism); the cost is a few
  redundant event applies per segment boundary — negligible vs. restore
  of the segment's full.
- **A redundancy on disk:** `(P_N, S]` events are stored both in the new
  segment's full and in its first incremental. Bounded by the rotation
  cadence; acceptable for DR archives.

### Why now (zero-users)

ADR-0046's own tenet: "the chain format is free *now* and never again."
There are zero on-disk chains in the field. Fixing rotation contiguity
is the same clean-break-while-it's-free reasoning that justified the
segment model; it is strictly more expensive once real DR chains exist.
This is the cheapest this fix will ever be.

## Gotchas (the implementation review focus)

- **New field, not a reinterpretation (the deliberately lower-risk
  choice).** `StartPosition` keeps meaning "full anchor / restore base,"
  so its existing readers (prune floor / `RestorableFromSegment`, restore
  base selection, any PITR scaffolding, diagnostics) are **untouched**.
  The work is: add `IncrementalCoverageStart`, repoint the **two**
  contiguity checks (§14d + restore segment-to-segment boundary) to it,
  and default empty → `StartPosition` for back-compat. Still audit those
  two checks and any serialization/round-trip of `LineageSegment`, but
  this is far narrower than overloading `StartPosition` everywhere.
- **Restore validator relaxation must stay loud on a true regression.**
  Tolerate `firstIncr.Start ≤ full.End` (idempotent overlap) but keep
  refusing a *segment-to-segment* gap or a position *regression* that
  would actually drop data. The relaxation is narrow and must be
  pinned both ways (accept overlap; still refuse a real gap).
- **The `S ≥ P_N` hard-fail assertion stays.** Contiguity is achieved by
  the cap/record positions, not by weakening the monotonicity guard.
- **Crash-recovery FSM semantics (ADR-0046 §2 COMMIT linearization)
  are unchanged** — the only behavioral change is `skipThrough` and the
  recorded `StartPosition`. The crash-injection matrix must still pass
  unchanged (re-run it; treat any edge regression as a blocker).
- **`-race` + integration before the tag.** This is the concurrency /
  crash-recovery class per CLAUDE.md: the `-race` Integration gate (PG +
  MySQL) and the crash matrix must be green **before** the tag is cut —
  push-first, tag-after; never tag-then-watch for this chunk class.
- **MySQL parity.** The same handoff change applies to the binlog/GTID
  rotation path; verify `skipThrough = P_N` and the overlap-replay are
  correct for MySQL positions too (idempotent apply already covers it).

## Testing

- **New load-bearing integration pin (both engines):** drive a
  ≥3-segment rotation under *continuous* write churn (repeated UPDATEs to
  the same PKs across rotation boundaries — the exact Bug 95 shape), then
  `backup compact --merge-window` large enough to merge all segments →
  assert it **succeeds** (no position-gap refusal), the merged chain
  restores **byte-identical** to the source end-state, and (with
  `--smart-compaction`) the event count actually collapses. This is the
  regression pin for Bug 95.
- **Standalone-segment restore with overlap:** restore a single rotated
  segment whose incrementals now start at `P_N < S`; assert byte-identical
  to source, proving the idempotent overlap replay is correct (incl. a
  row inserted-then-deleted, updated, and deleted within `(P_N, S]`).
- **Restore-validator unit matrix:** accept `firstIncr.Start ≤ full.End`
  (overlap); still refuse a genuine segment gap and a position
  regression.
- **Crash-injection matrix (ADR-0046):** re-run unchanged at every FSM
  edge; assert no loss + correct ≤/>COMMIT resolution + restore
  correctness with the new `skipThrough`/`StartPosition`.
- **Never-rotated one-segment lineage** byte-identical restore (the
  strict-generalization guard) — must be untouched.
- **`-race` integration (PG + MySQL) green before tag.**

## Alternatives considered

- **Path B — teach smart-compaction to bridge the gap** (fold the later
  full as row-authoritative + diff for gap-deletes). Partial: helps
  *only* smart compaction, *only* PK tables on *plaintext* chains (smart
  refuses on encrypted, `chain_compact.go:449-452`; can't key no-PK
  tables); naive compaction stays gap-blocked; and it adds a new
  snapshot-rebase transform on DR data. Rejected — A makes it
  unnecessary and fixes the general case.
- **Multiple fulls in a merged segment** (don't discard later fulls; use
  them as mid-chain re-seeds). More invasive on the segment model + the
  restore path than the rotation-side fix; rejected.
- **Anchor the new full at a historical `P_N`** (Bug 95 option a as
  literally written). Infeasible on PG (no as-of-past-LSN base snapshot).
- **(c) Docs + clearer refusal only.** Honest stopgap; doesn't deliver
  the value-prop. **Decided NOT to ship as a standalone stopgap:** with
  zero users, no operator hits the misleading "split the merge window"
  refusal in the interim, and the docs note ("rotated chains aren't
  compactable") would be written-then-reversed by this ADR. Instead, the
  refusal-message honesty fix is **folded into this PR** (which already
  edits `chain_compact.go`): post-fix the gap refusal effectively never
  fires for rotated chains, and the message stays accurate for any
  residual genuine-gap/corruption case.

## References

- Bug 95 — sluice-testing `BUG-CATALOG.md`; report
  `session-reports/v0.87.0-complex.md`.
- ADR-0046 — native bounded-segment lineage + inline rotation (the FSM,
  the `S ≥ P_N` spine, COMMIT linearization, crash matrix).
- ADR-0064 — backup smart compaction (§14d contiguity pre-flight, §14e
  event collapse).
- ADR-0007/0010/0027, `docs/snapshot-cdc-handoff.md` — the idempotent
  snapshot→CDC handoff this reuses at each segment boundary.
- Code: `internal/pipeline/stream_rotation.go`,
  `internal/pipeline/stream.go` (`skipThrough`, `:760-766`),
  `internal/pipeline/chain_compact.go` (`:646-660`, `:731-745`),
  `internal/pipeline/chain_restore.go` (boundary validators,
  `:745-791`).
