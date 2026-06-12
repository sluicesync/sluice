# ADR-0087: compact splits at coverage gaps + rotation-boundary resume replays from P_N (Bug 139)

## Status

**Accepted (2026-06-12).** Closes Bug 139 (project regression catalog).
Builds on and amends ADR-0067 (born-contiguous rotation handoff); depends
on ADR-0046 (native bounded-segment lineage + inline rotation FSM) and
ADR-0064/§14d (compaction + contiguity pre-flight).

## Context

ADR-0067 made a *continuously-written* rotated chain born-contiguous: each
rotation-opened segment keeps the `(P_N, S]` overlap in its first
incremental and records `IncrementalCoverageStart = P_N`, so consecutive
segments are gapless and `backup compact` merges them.

But the stamp is recorded from the **actual first incremental** when it
commits (`updateLineageForManifest`), *not* at rotation COMMIT — deliberately,
so it stays honest across a crash that resumes at `S` instead of `P_N`
(ADR-0067 §gotcha). Bug 139 is the case ADR-0067's honesty argument
*creates*: a rotation-born segment whose creating session **never commits an
incremental at all**.

That happens on two ordinary paths:

- **Idle stop (the common operator workflow).** Rotate on a timer, then stop
  the stream while the source is idle. The rotation snapshot `S` is taken at
  a quiesced position (`S == P_N`, empty overlap), the freshly-opened segment
  receives no rollover, and a graceful stop leaves it zero-incremental.
- **Crash/end at the rotation boundary.** A process death between the
  rotation COMMIT and the first overlap incremental leaves the same shape.

Such a segment has no `IncrementalCoverageStart`, so
`incrementalCoverageStartOrStart()` falls back to its full anchor `S` — a few
WAL bytes past the prior segment's `EndPosition` `P_N`. The two consequences:

1. **Compact refused the WHOLE run.** `assertGroupBoundaryContiguous` saw the
   `P_N != S` gap and returned a hard error blaming "a pre-ADR-0067, imported,
   or corrupted lineage" — for a chain this binary's own rotation produced.
   The `compact` DR-maintenance feature became permanently unusable across
   that boundary, with a diagnosis that pointed at corruption and offered
   only "split the merge window" (which cannot help: the un-stamped segment's
   creation gap equals its predecessor's lifetime, so every usable window
   groups them). LOUD, zero data loss — but a real, misleading dead end.

2. **A later resume never healed it.** Resuming the stamp-less open segment
   set `startPos = parent.EndPosition = S` (the full's anchor), so the first
   post-resume incremental started at `S`, equalled `StartPosition`, and the
   stamp stayed unset forever (rig ground truth: `cmp3chain` segment
   `bd0c5e94`). The boundary was un-compactable permanently.

The restore path was always fine (it tolerates `prev.End <= seg.Start` and
re-seeds from each segment's full). Only `compact` and the resume stamp were
affected.

### Why the bug catalog's suggested fixes are UNSOUND (rejected)

The catalog proposed (a) stamp `IncrementalCoverageStart = P_N` at segment
**CREATION** (rotation already knows `P_N`), and/or (b) **backfill** the stamp
from the catalog's prior-segment end on resume/cap.

Both are **rejected** because they claim incremental coverage of the
`(P_N, S]` window that **no durable artifact proves**. At a crash — or at a
walsender lag on a graceful stop — real committed events can exist in
`(P_N, S]` that live ONLY in the new segment's full snapshot. `compact`
discards every non-oldest segment's full. So a stamp asserting "my
incrementals cover from `P_N`" would let `compact` drop those events — turning
today's LOUD refusal into **silent loss inside a DR artifact**. The stamp
must remain a *fact derived from a committed incremental that actually
starts at `P_N`*, never a hopeful annotation.

## Decision

Two coordinated changes, neither of which ever stamps coverage that isn't
backed by a committed incremental.

### 1. `compact` SPLITS at coverage gaps instead of refusing the run

After the `CreatedAt`-window grouping, each group is **subdivided** at every
consecutive-pair boundary where `prev.EndPosition !=
cur.incrementalCoverageStartOrStart()` (`subdivideAtCoverageGaps`,
`chain_compact.go`). A stamp-less rotation-born segment thus stays in its own
merge group; the contiguous runs around it still merge by pure concat
(ADR-0067), and the `(P_N, S]` window stays safe in that segment's untouched
full snapshot. Each split boundary emits ONE operator-accurate `slog.Warn`
naming both segment IDs, the prior end + the coverage-start positions,
whether the later segment is zero-incremental and rotation-born, and the
explanation (born by rotation, never committed an incremental in its creating
session; the window lives only in its full; segments stay separate; **no data
is lost, chain remains fully restorable**).

The split runs **before** the naive/smart branch, so both modes — and
`--dry-run` plans — see the same subdivision and WARNs. The codec-uniform and
encryption-keyset refusals are unchanged (those *are* genuine refuse-loudly
boundaries). `assertGroupBoundaryContiguous` is **kept** as a defensive
internal invariant in the per-group preflight: post-split it is unreachable,
so if it fires it is a subdivision bug, surfaced loudly before any byte-level
merge drops DR data.

### 2. A resume of a stamp-less rotation-born segment replays from P_N

When a `backup stream` (or one-shot `backup incremental`) resume lands on a
rotation-born OPEN segment with ZERO recorded incrementals and the resolved
parent is that segment's full (`startPos == segment.StartPosition`),
`rotationBoundaryResumeStart` resumes from the **prior segment's EndPosition
`P_N`** instead of the full's anchor `S`. This exactly reconstructs the
creating session's post-COMMIT state (`currentParent =` the segment's full,
`startPos = P_N`): the first incremental then starts at `P_N`, the existing
`updateLineageForManifest` first-incremental logic stamps
`IncrementalCoverageStart = P_N`, and the lineage becomes born-contiguous and
compactable — **honestly**, because an incremental that actually starts at
`P_N` now exists.

Soundness: the slot's ack ceiling was only ever released through committed
incremental ends `<= P_N` (`releaseChainAckTo`), so the source retains
everything after `P_N`; in-order CDC delivery makes the first incremental's
`(P_N, end]` coverage complete; the `(P_N, S]` overlap re-applies
idempotently on restore (ADR-0010, the snapshot→CDC handoff dedup). **No
`skipThrough` is needed** on this path: `skipThrough` exists in the in-process
rotation handoff only because the in-flight channel is NOT re-opened there; a
fresh `StreamChanges(P_N)` resumes strictly-after `P_N` like any normal
resume (verified against both PG and MySQL `StreamChanges` — both deliver
strictly-after `from`, the same semantics every rollover resume already
relies on).

The two fixes are complementary: even a chain that is never resumed (the
operator just compacts the idle-stop chain) compacts cleanly via the split;
and a chain that IS resumed heals fully and then compacts N→1.

## Consequences

- The "rotate on a timer, stop when idle" workflow no longer produces a
  permanently un-compactable boundary, and `compact` never blames corruption
  on a chain rotation produced.
- A crash/idle-stop at the rotation boundary no longer permanently strands the
  `(P_N, S]` coverage intent: the next resume re-establishes it from `P_N`.
- A `compact` run on a chain with a residual stamp-less boundary now succeeds
  (merging around it) rather than failing — operators must read the WARN to
  know one boundary stayed unmerged. Acceptable: the alternative was total
  failure.
- The honest-stamp invariant is preserved: `IncrementalCoverageStart` is still
  only ever written from an incremental that genuinely starts there. No silent
  DR-loss surface is introduced.

## Gotchas (implementation review focus)

- **Never creation-time-stamp or backfill** (the rejected fixes) — see the
  soundness argument. The split + resume-replay are the only sound shapes.
- **The split predicate is the SAME comparison as the old refusal**
  (`incrementalCoverageStartOrStart` engine+token equality). Only the
  *response* to a gap changed (split, not refuse).
- **Resume-replay only fires for the precise shape** (rotation-born, >= 2
  segments, zero incrementals, parent == the full, non-empty prior end). Every
  other resume keeps today's behaviour; the helper returns ok=false and is
  best-effort on a transient catalog read.
- **Concurrency / `-race`.** This touches the stream resume path and the
  rotation FSM's documented semantics. The `-race` Integration gate (PG) must
  pass before any tag — push-first, tag-after.

## Testing

- **Unit (compact split, `chain_compact_test.go`):** table-driven over lineage
  shapes — trailing zero-incremental rotation-born OPEN (the exact idle-stop
  shape), trailing CAPPED, mid-chain stamp-less-with-incrementals (the
  `bd0c5e94` resumed shape), multiple gap boundaries in one window — asserting
  the grouping outcome AND that the merged content/plan and restore-walk are
  correct; fully-contiguous rotated chain still merges 4→1 (no split, no
  WARN); codec/encryption refusals unchanged; `--dry-run` produces the same
  subdivided plan + WARNs. The pre-ADR-0087 position-gap *refusal* pin is
  REPLACED by the split pin.
- **Unit (resume rule, `stream_resume_heal_test.go`):** an open rotation-born
  zero-incremental segment resumes at `prior.EndPosition`; a first incremental
  committed at `P_N` stamps `IncrementalCoverageStart == P_N`; the negative
  cases (segment 0, has incrementals, parent != full, empty prior end) keep
  today's behaviour.
- **Integration (PG, testcontainers, `backup_compact_idle_rotation_integration_test.go`):**
  the Bug-139 idle-stop repro end-to-end — age-rotation chain whose trailing
  segment is born stamp-less under an idle stop → compact (naive AND smart)
  splits at the boundary, WARNs, merges the pre-boundary run, and the
  compacted chain restores count/content-equal to the source oracle; plus the
  resume-heal leg — a second stream session resumes at `P_N`, stamps the
  segment, and compact then merges the WHOLE chain N→1 with a matching
  restore.

## References

- Bug 139 — project regression catalog (`sluice-testing/BUG-CATALOG.md`); the
  v0.99.40 cycle.
- ADR-0067 — born-contiguous rotation handoff (the `IncrementalCoverageStart`
  field + the honest-stamp rule this amends).
- ADR-0046 — native bounded-segment lineage + inline rotation FSM (COMMIT
  linearization, the `S >= P_N` spine).
- ADR-0064 §14d — compaction contiguity pre-flight.
- ADR-0010 — idempotent applier / snapshot→CDC handoff dedup (the overlap
  re-apply soundness).
- Code: `internal/pipeline/chain_compact.go` (`subdivideAtCoverageGaps`,
  `boundaryHasCoverageGap`, `assertGroupBoundaryContiguous`),
  `internal/pipeline/stream.go` + `incremental.go`
  (`rotationBoundaryResumeStart`), `internal/pipeline/stream_rotation.go`,
  `internal/pipeline/chain_catalog.go`.
