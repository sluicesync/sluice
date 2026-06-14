# ADR-0089: Adaptive `--apply-batch-size` by default (with a keyless-table guard)

## Status

Accepted. Supersedes the *default-value* clause of [ADR-0017](adr-0017-batched-cdc-apply.md)
(`--apply-batch-size` default `1`) and the *"default behaviour unchanged / controller
opt-in"* clause of [ADR-0052](adr-0052-aimd-apply-batch-size-controller.md)
§"Interaction with ADR-0017". The conservative single-row behaviour remains available
verbatim via `--apply-batch-size=1` (and `--no-auto-tune` still pins a static cap).

## Context

[ADR-0052] shipped an AIMD apply-batch-size controller: it adapts the per-transaction
batch size to a p95-latency target, additive-increase on headroom, multiplicative-decrease
on latency/error pressure, **floor = 1** (ADR-0017's safety value, never undershot). It is
"on by default" — but [ADR-0017]'s default `--apply-batch-size=1` makes the controller's
*cap* equal to its *floor*, so the controller can never move off 1. **The adaptive
controller is therefore dormant for every default user**: sluice ships a throughput
controller that does nothing out of the box, and operators only benefit if they already
know to pass `--apply-batch-size=auto` (or `=N>1`).

This was measured directly on the first real PlanetScale long-haul soak (2026-06-13;
`sluice-testing` Bug 141, `vitess-selfhosted-repro.md`): the default single-row apply
drained a backlog at **~240 rows/s**, versus **~6,500 rows/s** at `=auto` (cap 1000, AIMD
adapting) — a **>10×** difference. The slow single-row default also materially compounds
recovery from a source-side VStream throttle (a wedged/lagging stream takes far longer to
catch up than it needs to), and "you must pass a flag to get reasonable throughput" is a
reliability-by-omission trap: the safe-looking default is the slow one.

## Decision

Change the `sync start` default `--apply-batch-size` from `1` to **`auto`** (the engine-default
ceiling: 1000 for mysql/postgres, 100 for planetscale; the AIMD controller adapts within
`[1, ceiling]`). `--no-auto-tune` still makes the resolved value a strictly static cap;
`--apply-batch-size=1` still restores the pre-v0.x conservative one-change-per-tx behaviour
exactly.

### The keyless-table guard (load-bearing safety)

[ADR-0010] makes Insert/Update/Delete idempotent **via the table's identity key**, so
replay-on-crash is benign — *for tables that have a usable key*. Per the applier package
docs (Bug 125 cross-engine symmetry), three classes exist:

1. **PRIMARY KEY** → keyed upsert. Idempotent. Safe to batch.
2. **No PK but a (NOT NULL) UNIQUE index** → upsert keyed on the unique index
   (`ON CONFLICT (cols)` / `ON DUPLICATE KEY UPDATE`). Idempotent. **Also safe to batch.**
3. **Truly keyless** (no PK *and* no usable unique index) → falls back to plain `INSERT`.
   **Not idempotent**: a replayed Insert produces a duplicate row.

Batching amplifies the class-3 hazard: a crash mid-batch replays up to N changes, turning
class-3's 1 duplicate per replayed Insert into up to N. Silent row duplication is the
cardinal user-trust violation, so the new default **must not batch class-3 tables**.

**Mechanism.** A change targeting a *truly-keyless* table is treated as a **flush boundary**
in the shared batch loop ([ADR-0081] `appliershared.RunBatchLoop`), mirroring the existing
schema-event ("apply alone") handling: the change is dispatched, then the batch commits
immediately. This bounds a keyless table's crash-replay blast radius to **exactly 1
duplicate per keyless change — identical to `--apply-batch-size=1`** — while PK / unique
tables in the same (mixed) stream continue to batch and adapt. The loop learns "is this
change's table keyless" via a nil-safe `BatchConfig.IsKeylessTable` predicate each engine
fills from the per-table key knowledge it already computes for its upsert clause
(PG `conflictKeyCache` empty; MySQL `pkCache` ∧ no usable unique key). A nil predicate
(or an engine that lacks the signal) means "never treat as keyless" — no behaviour change
for that path. The applier emits a one-time WARN per keyless table held at single-row so the
operator sees why that table isn't getting the throughput.

This deliberately keys on **truly keyless**, not "no primary key": the Bug-125 class-2
(no-PK-but-UNIQUE) tables are idempotent and keep batching — the guard penalises only the
genuinely non-idempotent case.

## Consequences

- **>10× out-of-box throughput** on bulk CDC traffic; the ADR-0052 controller finally engages
  for normal users and adapts to the target's real latency ceiling (and backs off to 1 under
  pressure — *more* reliable under load than a static conservative 1, not less).
- **Larger replay-on-crash window** for PK/unique tables (up to N changes replay in one tx) —
  benign under ADR-0010 idempotency.
- **No new silent-duplication exposure**: class-3 keyless tables are clamped to batch=1
  semantics, the same blast radius as before this change.
- Conservative operators opt back via `--apply-batch-size=1` or `--no-auto-tune`.
- Reverses the explicit "default unchanged / opt-in" decisions of ADR-0017 and ADR-0052 —
  recorded as superseded-in-part above.

## Alternatives considered

- **Stream-level downgrade** (keep the whole stream at batch=1 if *any* table is keyless):
  simpler, but penalises every table in a mixed stream for one keyless table. Rejected in
  favour of the per-change flush boundary, which preserves throughput for the keyed tables.
- **Loud preflight refusal** of keyless continuous-sync tables under batching: too strict —
  it breaks workflows that work today at batch=1. The auto-downgrade-with-WARN preserves
  them, safely.
- **Leave the default at 1, document `=auto`:** the status quo — leaves the controller dormant
  and the reliable setting behind operator knowledge. Rejected: defaults should be the
  reliable path.
