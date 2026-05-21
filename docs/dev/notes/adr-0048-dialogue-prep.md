# ADR-0048 dialogue prep — one DP from Accepted

Quick read for the next session: ADR-0048 (multi-source aggregation
Shape A — sharded → consolidated) is **one owner confirmation away
from Accepted**. The current state is in
[`docs/adr/adr-0048-multi-source-aggregation-shape-a.md`](../../adr/adr-0048-multi-source-aggregation-shape-a.md);
the spike harness sits in
`internal/pipeline/shapea_spike_vstream_integration_test.go`
(`//go:build integration vstream`); the design-evidence prep is in
[`prep-multi-source-shape-a.md`](prep-multi-source-shape-a.md).

This file exists so the dialogue can resume without re-reading 900 LOC
of context first.

## Status summary

| DP | Topic | Status |
|---|---|---|
| DP-1 | CDC-path injection surface (a / b / c) | **RESOLVED 2026-05-21 — option (a), two-surface split** |
| DP-2 | Populated-target first-shard detection | RESOLVED 2026-05-16 — discriminator-value-presence only |
| DP-3 | Cross-shard DDL coordination: live vs drained for v1 | RESOLVED 2026-05-16 — drained for v1 (live deferred to Phase 2) |

ADR-0048 Status: **Accepted** (design-only; implementation demand-gated
per roadmap §4). The DP-1 dialogue, code-grounded against
`internal/engines/{mysql,postgres}/change_applier.go`, sharpened three
findings the original ADR text under-stated; they are recorded in
ADR-0048's DP-1 resolution block. Implementation waits for a concrete
operator workload.

The other four pieces of the design (translate-pass for the schema
half; `ir.Column.SluiceInjected` provenance marker; the loud
three-point `preflightShardConsolidation`; composite-PK rewrite into
`IdempotentRowWriter` + CDC identity) are not decision points — they
fall out cleanly from DP-1's two-surface split (or would, on a `(c)`
unified-surface refactor — the same six pieces in a different shape).

## DP-1 — the actual question on the table

When a CDC `ir.Change` (Insert / Update / Delete) arrives at the
applier, the discriminator value `--inject-shard-column NAME=VALUE`
needs to be stamped onto the row AND into the Update/Delete
PK-identity (so the composite-PK WHERE clause locates the row in the
correct shard partition). Three implementation shapes:

**(a) Two-surface split — bulk wrap + optional applier surface.**
Schema half: pure `internal/translate.InjectShardColumn` IR pass.
Value half on bulk-copy: orchestrator-side `redactRows`-shaped wrap.
Value half on CDC: an optional engine applier surface
`ShardColumnSetter` (mirrors the existing `RedactorSetter` /
`applyRedactor` pattern). Each engine implements `ShardColumnSetter`
if it supports Shape A; engines that don't implement it surface
loudly (the redaction precedent's same shape).

**(b) Orchestrator-side change-stream wrap.** Schema half: same.
Value half: a single orchestrator-side wrap that applies to BOTH the
bulk-copy row stream AND the CDC change stream before they reach the
engine applier. Pros: one place to look. Cons: the CDC stream's
`Update`/`Delete` PK-identity is engine-shaped already by the time it
reaches the orchestrator (the engine constructed the
Before/After tuple from binlog/WAL bytes); rewriting it
orchestrator-side requires the orchestrator to understand each
engine's identity-tuple shape — a layering inversion.

**(c) Unified surface — promote key-identity to a first-class
`ir.Change.Key`.** Schema half: same. Value half AND identity half:
a single mechanism that operates on a typed `ir.Change.Key` field
which every engine constructs and every applier consumes. Pros:
collapses identity-construction into one place; the discriminator
just becomes "one more column on Key". Cons: a cross-engine refactor
of the most correctness-critical surface (CDC-apply identity ownership
shifts from the applier to the IR contract); not a v1 vehicle for
Shape A — it's a separately-ADR'd FEATURE-BRANCH refactor.

**Spike lean (and ADR's recommended decision):** **(a)**. Matches the
redaction precedent exactly (already-shipped, already-pinned), keeps
the v1 scope bounded, leaves (c) available as a future
simplification-refactor without blocking Shape A. The ADR explicitly
notes: "v1 leans option (a). Final owner confirmation of (a)-for-v1
pending; does not block recording the rest of the design."

## What "confirming (a)" does

1. ADR-0048 Status: Proposed → **Accepted** (CHANGELOG hygiene matters
   per CLAUDE.md's "doc-truth: ADR statuses must reflect reality").
2. Roadmap §4 entry stops being "design pending" and becomes "design
   accepted, implementation demand-gated" (waits for a concrete
   operator workload — the ADR explicitly says don't implement ahead
   of one).
3. Implementation scope, when demanded: ~600–1000 LOC per the ADR's
   own estimate. One `internal/translate` pass + one `ir.Column`
   field + value-wrap on 4 codepaths + one preflight branch + one
   control-table coordination column. Concurrency-class (touches the
   CDC apply path and a control-table migration) → push-first /
   CI-Integration-green-before-tag discipline applies.

## What "confirming (c) instead" would do

Move Shape A's v1 to a different shape AND open a parallel feature-
branch ADR for the unified-surface refactor (it'd be its own ADR,
co-equal with 0048 rather than nested in it). Larger blast radius,
larger payoff if it lands (every future CDC-apply consumer benefits
from a typed `ir.Change.Key`), but explicitly deferred by the spike
findings as "not a v1 Shape A vehicle." If owner wants this, the next
move is filing the new ADR, not implementing 0048.

## What "confirming (b) instead" would do

Pick the orchestrator-side wrap. The spike's analysis flags the
layering-inversion cost as the dealbreaker (CDC identity-tuple shape
is engine-specific; orchestrator-side rewrite requires per-engine
knowledge in the orchestrator, which violates the IR-first tenet).
ADR text effectively rules this out via reasoning rather than as an
explicit alternative; if owner wants it, the dialogue needs to
address the layering-inversion concern.

## Likely outcome (zero-prep estimate)

Owner reads this file → confirms (a) → next session moves ADR-0048
Status to Accepted, marks task #9 completed, files a new task
"ADR-0048 implementation (demand-gated)" so the work is queued behind
the existing demand-gate. Total session time: ~10 minutes if no
further questions surface.

## Adjacent state worth knowing

- ADR-0050 (reconciling re-snapshot) is also Proposed and gate-(1)
  empirical evidence has been gathered (see
  [adr-0050-cost-validation-report.md](adr-0050-cost-validation-report.md)
  in the same notes/ folder). Bottom line: conditional-go; cost shape
  closed at 1 GiB anchor; reconciling-resnapshot-vs-full-recopy delta
  needs the implementation to measure. Independent of 0048.
- ADR-0049 (CDC schema history) is fully implemented + the Chunk E
  pin (long-deferred) finally runs end-to-end live as of v0.71.0. No
  open work.
- Demand-gating: the roadmap §4 entry for Shape A explicitly waits
  for a concrete operator workload before implementation. The ADR
  text reinforces this. Accepting the ADR is NOT a commitment to
  implement; it's a commitment to the *design* for when an operator
  asks.
