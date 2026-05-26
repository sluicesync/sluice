# Copywriting guardrails

Short list of constraints on how sluice talks about itself — in release notes, README copy, blog drafts, and PR descriptions. Apply when writing for an external audience (operators, evaluators, social media). Internal docs (ADRs, design notes, code comments) are not bound by these — they should be precise rather than positioned.

The rule of thumb: claim only what the loud-failure machinery actually enforces. If sluice would refuse loudly when a claim doesn't hold, the claim is fair. If sluice would silently drift, don't claim it.

---

## What to say

- **"Continuous sync"** for the steady-state behaviour after the snapshot lands. The CDC apply path drives the target toward source-current; latency is operator-tunable via `--apply-batch-size` and the source-side heartbeat cadence.
- **"Initial snapshot + CDC catch-up"** for the lifecycle shape. Snapshot is bulk-copy; CDC catch-up is the row-event stream from binlog (MySQL) or pgoutput (Postgres).
- **"Cross-engine MySQL ↔ Postgres in all four directions"** when describing scope. The four directions are: MySQL → MySQL, MySQL → Postgres, Postgres → Postgres, Postgres → MySQL. PlanetScale is a flavour of MySQL with its own capability declarations.
- **"Refuses loudly"** for the safety discipline. Sluice prefers to halt with an operator-actionable error than to drift silently. Cite the tenet (in `CLAUDE.md`) when this comes up.

## What not to say

- **"Real-time"** / **"millisecond-latency"** / **"sub-second"**. Sluice's latency is bounded by `--apply-batch-size`, source heartbeat cadence, network round-trip, and target apply throughput. None of those are millisecond-bounded by default, and sluice doesn't enforce a latency SLO at the apply layer. Reach for "continuous" or "near-real-time" if positioning against a batched alternative is the goal; let operators discover their own bounded numbers.
- **"Zero data loss"** as a flat claim. The loud-failure floor is *zero silent data loss* — sluice's tenet is that the failure modes are surfaced, not suppressed. "Loud failure on the silent-loss class" is the honest framing.
- **"Production-ready"** without qualifier. The README's current posture (`alpha`, zero production users) is load-bearing. Until that changes, claim *operator-actionable*, *integration-tested*, *opinionated about correctness* — not *production-ready*.
- **"Faster than X"** without a published benchmark. Pricing comparisons are fine (per-MAR vs. per-instance vs. none); throughput comparisons need data behind them.

## When in doubt

If you find yourself reaching for a stronger claim than the loud-failure machinery enforces, two paths:

1. **Soften the claim.** "Continuous sync" rather than "real-time." "Refuses loudly" rather than "guarantees no data loss." This is the default.
2. **Add the enforcing machinery first.** If the claim is desirable and operator-validated, write the test that pins it, then write the copy. Bug 74's lesson applies: the claim is only as strong as its test.

The README's "When NOT to use sluice" section is the model for this kind of honesty — naming what sluice doesn't do is more credible than over-claiming what it does.

---

## Cross-references

- [`CLAUDE.md`](../../CLAUDE.md) — the loud-failure tenet, the "validate end-to-end before building more" principle
- [`docs/dev/release-template.md`](release-template.md) — applies these guardrails to release-notes drafting
