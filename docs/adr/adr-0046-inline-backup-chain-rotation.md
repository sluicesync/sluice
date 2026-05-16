# ADR-0046: native bounded-segment lineage model + inline rotation (§14b Phase 2)

## Status

**Accepted (2026-05-16).** Design signed off after a design
dialogue; implementation pending, ships **v0.67.0**. This
**supersedes the earlier grafted-rotation draft of this ADR**
(tombstone + `SucceededBy` markers bolted onto an unbounded chain).
The decision is the **native bounded-segment lineage model**, as
written below, with all three residual scope calls confirmed at
sign-off: clean-break `lineage.json` (no old-`chain.json` shim);
Phase 1 `--exit-after-*` removed; **14c prune reframed onto
segments in this chunk** (the surface is reopened once, not
twice); `--compression=none|gzip|zstd` folded in, **zstd default**
(v0.67.0). The default flipped gzip→zstd on the decode-inclusive
compressbench re-run (zstd decodes 55–85% faster — restore is the
DR-critical axis — at ~1–5% ratio cost vs klauspost gzip); a
clean break, no gzip-default shim (zero-users tenet), retiring
the compression-benchmark doc's earlier "gzip default / zstd
demand-gated" hold. The rotation correctness core is unchanged
from the prior draft. make capped segments + ordered
succession the first-class chain format, not rotation-as-an-event.
Rationale: zero users + zero on-disk chains means the chain
format is free *now* and never again; the chain format is the
most long-lived, load-bearing artifact in the product (operators'
actual DR data); and the grafted model bakes in a permanent
"rotated vs not / tombstoned vs live / restore-walks-successor vs
not" bimodality — exactly the scattered-conditional class this
session spent eight releases consolidating (ADR-0045). Same
zero-users→cleaner-break tenet applied to ADR-0041/0044, and
*stronger* here (a backup format outlives a CLI flag). Builds on
§14b Phase 1 (v0.51.0), `prefixedStore` (v0.50.0), chain.json
(14a, v0.47.0), prune (14c, v0.50.0) — all zero-users, so their
schema/design is reopened freely.

## Context

The grafted approach (prior draft): a "chain" is one unbounded
`full + N incrementals`; rotation is an exceptional event —
`RotatedAt`/`RotationReason`/`SucceededBy` markers, `Tombstoned`
entries, a separate sibling chain, and restore's `buildChain`
*extended* to chase `SucceededBy` (a second, structurally
different monotonicity check alongside the intra-chain one). Every
future chain-touching feature must then handle the rotated/not and
tombstoned/not forks or introduce a bug — the precise pattern
(#5→Bug 61→63→64→65) ADR-0045 just unwound.

**Native model:** the first-class on-disk object is a **lineage**
— an ordered list of **segments**. A segment = one `backup full`
anchor + its incrementals, capped by policy (age / length /
bytes). "Rotation" is not an event; advancing is
`lineage.appendSegment()`. **A never-rotated backup is a
one-segment lineage** — identical on-disk shape, operator
experience, and restore path for the common case. Strict
generalization, not a heavier common path.

**Prior art (this is a proven shape, not novel):** pgBackRest /
WAL-G "backup sets + WAL with set-granular retention"; XtraBackup
base+incremental+binlog; conceptually the log-segmented-storage
pattern (Kafka topic segments with whole-oldest-segment retention;
LSM SSTable compaction). The native model is that pattern applied
to CDC backup chains.

**The rotation-window correctness spine is UNCHANGED from the
prior draft** (prep doc `docs/dev/notes/prep-backup-chain-rotation.md`):
the same goroutine that owns the CDC pump drives the segment
handoff over the *same in-flight source handle*; the new
segment's snapshot anchor `S ≥ P_N` (prior segment's last
incremental position) **by construction** because the source is
position-monotonic. Same snapshot→CDC consistency pattern sluice
proves for the initial full→stream transition
(`docs/snapshot-cdc-handoff.md`, ADR-0007/0010/0027), replicated
at each segment boundary.

## Decision

### 1. The lineage catalog (replaces chain.json; clean break — no migration shim, zero on-disk chains)

`lineage.json` at the lineage root:

```
{ lineage_id, created_at, updated_at,
  segments: [ { segment_id, dir, full_manifest_path,
                incrementals: [manifest_path...],
                start_position, end_position,
                capped_at?, cap_reason?,        // open segment: absent
                codec } ... ],
  restorable_from_segment }   // prune floor
```

- A segment lives in its own sub-dir via
  `newPrefixedStore(store, segment.dir)` (the v0.50.0 scaffolding,
  exactly its intended consumer).
- **One-segment, never-capped lineage = today's single chain**,
  zero behavioural/UX difference for operators who never set a
  rotation flag.
- The old single-chain `chain.json` reader is **removed**, not
  dual-pathed (zero users; clean break — the whole rationale).
  14a's catalog *concept* (O(1) chain-state lookup) is preserved
  and generalized to the lineage.

### 2. Rotation = append-segment, via the same-handle FSM (correctness core, unchanged)

Inside the existing rollover-loop goroutine that owns `cdc`:
`STREAMING → DRAIN → SNAPSHOT → BULKCOPY → COMMIT → STREAMING`.

1. **DRAIN** — at the next TxCommit boundary (ADR-0027), finish
   the in-flight rollover, commit the open segment's final
   incremental at `P_N`.
2. **SNAPSHOT** — on the *same* source handle,
   `OpenBackupSnapshot` → anchor `S`. **Hard-assert `S ≥ P_N`;
   loud-abort the rotation and stay on the open segment if
   violated** (a source-monotonicity violation must never
   silently gap the next segment).
3. **BULKCOPY** — write the next segment's `backup full` under its
   sub-dir; `start_position = end_position = S`.
4. **COMMIT (atomic linearization point)** — append the new
   segment to `lineage.json` and cap the prior segment
   (`capped_at`, `cap_reason`) in **one** atomic catalog write,
   *after* the new full is durable. This single write flips
   authority; there is no window where the lineage is
   non-authoritative.
5. **STREAMING** — continue CDC from `S` on the same handle;
   incrementals now append to the new open segment.

### 3. One boundary-monotonicity invariant (the simplification)

`segment[i].end_position ≤ segment[i+1].start_position`, validated
by restore **the same way** it already validates incremental
boundaries *within* a segment. Restore walks segments in order;
there is **no bimodal `buildChain` "is it rotated" branch**, no
`SucceededBy`-chase code path. Point-in-time restore (future) is a
natural query over the segment list. One invariant, one check
site, segment-internal and segment-to-segment alike.

### 4. Prune / Compact reframed as segment-list operations

14c prune is reframed (zero users) from "parent-link re-stitch +
tombstone bookkeeping" to: drop leading whole segments and/or
leading incrementals within the oldest kept segment; advance
`restorable_from_segment`. 14d compact (unbuilt) becomes
"merge incrementals within a segment" — built *on* the lineage
model, not beside it. Both are list/segment-local ops; no
tombstone/parent-link surgery.

### 5. Per-segment compression codec (folded in — the surface is open)

Each segment records its `codec` (`"zstd"` default | `"gzip"` |
`"none"`). New `--compression=none|gzip|zstd` on `backup full` /
`backup stream run` (**default `zstd`**, v0.67.0 — was `gzip`).
Restore reads each segment's recorded codec — **mixed-codec
lineages are naturally supported** (a `none` segment captured for
local inspection alongside `gzip`/`zstd` segments restores
correctly). `none` is the operator-inspectability case the
validation friction surfaced (local-FS target, eyeball `.jsonl`);
object stores never auto-compress, so compression is always
sluice-side for egress/at-rest only — `none` is principled for
local targets. **`zstd` is the default**: the compressbench
decision doc was re-run with decode throughput measured (warm
median, not single-pass) and the conclusion reversed — zstd at
SpeedDefault decodes **55–85% faster than klauspost gzip on every
corpus** (restore speed is the DR-critical axis the original
encode/ratio-only analysis omitted) and encodes 0–30% faster, at
a ~1–5% ratio cost on representative chunk data (the "~21%" the
old doc cited was vs *stdlib* gzip, the encoder it also said to
abandon). gzip→zstd is a clean break — no gzip-default shim, zero
on-disk backups predate it (zero-users tenet). Implement zstd via
`klauspost/compress/zstd` at **SpeedDefault** — already a direct
module dependency, no new dep. The codec is **recorded per
segment, never inferred** from file contents.

### 6. CLI: one rotation model (Phase 1 `--exit-after-*` removed)

`--retain-rotate-at=DUR` / `--retain-rotate-at-chain-length=N` are
THE rotation knobs; rotation is always in-process. **Phase 1's
`--exit-after-age` / `--exit-after-chain-length` are removed**
(zero-users clean break — they were an explicit interim stopgap
for the unbuilt inline path; keeping them as a co-equal model is
the retrofit-thinking this redesign rejects). The
`chain.json` `RotatedAt`/`RotationReason`/`SucceededBy`/`Tombstoned`
fields are removed with the old catalog.

### Crash recovery (D2, unchanged in spirit — "open segment is durable truth until COMMIT")

`rotation_state.json` at the lineage root records FSM state + the
provisional next-segment dir. On restart: **≤ COMMIT** → discard
the provisional next segment, resume STREAMING on the still-open
prior segment from its persisted position (it never lost
position; no resume-the-FSM-mid-rotation replay). **> COMMIT** →
the new segment is authoritative; resume on it (idempotent
re-cap of the prior segment is a no-op). COMMIT is the single
linearization point.

### What does NOT change

- Chunk/manifest **body** format (JSON-Lines records), the
  position model, CDC, the snapshot→CDC correctness mechanism.
  Only the *catalog/segment metadata* + the codec wrapper change.
- Common-path operator experience (never set a rotation flag → a
  one-segment lineage, same as today).
- Out of scope: `zstd`; backup-broker (Phase 4.5) following a
  multi-segment lineage (flagged, deferred — prep doc open Q3);
  per-segment encryption keying stays per the existing per-chain
  rule (now per-segment; prep doc open Q1, documented).

## Gotchas

- **`S ≥ P_N` is a hard-fail assertion, not advisory** — loud
  abort + stay on the open segment; pin with an injected
  non-monotonic anchor test.
- **COMMIT ordering is the linchpin**: next-segment full durable
  *strictly before* the atomic lineage.json append+cap. Crash
  between → ≤COMMIT resolution (discard provisional). Test a
  crash injected at every FSM edge.
- **Codec is recorded, never sniffed.** A restore that infers
  gzip-vs-none from bytes is a latent corruption path; read it
  from the segment metadata only.
- **DRAIN respects the TxCommit boundary** — `P_N` is always a
  transaction-consistent position or the anchor comparison is
  meaningless.
- Restore must reject a malformed lineage (out-of-order
  segments / position regression across a boundary / missing
  full) **loudly**, never silently assemble a partial restore
  (loud-failure tenet — this is DR data).
- The never-rotated one-segment path must be exercised as a
  first-class test, not assumed equivalent.
- Reframing 14c prune onto segments must keep restore-after-prune
  correct (the `restorable_from_segment` floor + StartPosition
  validation) — the invariant prune always had, expressed on
  segments.

## Testing

- Unit: FSM transition table; the `S ≥ P_N` hard-fail; lineage
  catalog (de)serialization incl. mixed-codec segments;
  boundary-monotonicity validator (intra-segment and
  segment-to-segment via the *same* code).
- Integration (both engines, testcontainers): a full multi-segment
  rotation under continuous write load — **assert zero
  loss/duplication across every segment boundary** (write a known
  sequence spanning rotations; restore the lineage; assert exact
  final state). The load-bearing test.
- Crash-injection matrix (ADR-0036-style permanent
  proof-of-falsification): kill at each FSM edge; assert no data
  loss + correct ≤/>COMMIT resolution + restore correctness.
- Restore: 1-, 2-, 3-segment lineages; **mixed-codec lineage with
  all three (`gzip`+`none`+`zstd`) segments restores correctly**;
  malformed lineage → loud refusal; **never-rotated one-segment
  lineage byte-identical restore to a pre-ADR single chain's
  data** (the strict-generalization proof).
- Prune-on-lineage: drop leading segments, restore-after-prune
  correct; interaction with an open segment.
- Each of `--compression=none|gzip|zstd` round-trips (write →
  restore exact); `none` `.jsonl` is human-readable on a local-FS
  target; `zstd` decode via `klauspost/compress/zstd`; an
  unknown/garbled recorded codec → loud refusal (never sniff).
- Regression: the standard RUNBOOK backup paths; Phase 1 flags
  are *gone* (assert the old flags now error clearly — clean
  break, not silent ignore).

## Sizing

Larger than the grafted draft — it reopens the 14a catalog
schema, 14c prune, and `buildChain`/restore (all zero-users) plus
the codec plumbing: ~1100–1600 LOC impl + ~900–1300 LOC tests
(the crash-injection matrix + zero-loss rotation + mixed-codec
restore are the bulk). One focused release, **v0.67.0** (minor;
clean-break catalog format, additive runtime behaviour, no engine
break). The correctness crux (the `S≥P_N` assertion, COMMIT
linearization, crash matrix, loud malformed-lineage refusal) is
the review focus — not LOC. Net long-term: removes a permanent
bimodality, makes 14d simpler, makes PITR/retention first-class.

## References

- Supersedes this ADR's prior grafted-rotation draft.
- `docs/dev/notes/prep-backup-chain-rotation.md` — the rotation
  correctness spine (unchanged); open Qs 1–3.
- §14b Phase 1 (v0.51.0, removed here), 14a chain.json (v0.47.0,
  generalized), 14c prune (v0.50.0, reframed), `prefixedStore`
  (v0.50.0, consumed), the compression-benchmark decision doc
  (the `--compression` flag + the `zstd`-at-SpeedDefault target it
  scoped — both landed here; its "zstd demand-gated" hold retired
  by explicit operator demand).
- ADR-0007/0010/0027, `docs/snapshot-cdc-handoff.md` — the
  snapshot→CDC consistency pattern replicated per segment
  boundary.
- ADR-0045 — the bimodality-consolidation precedent this applies
  proactively to the backup format.
- ADR-0036 — the crash/proof-of-falsification test discipline the
  crash-injection matrix mirrors.
