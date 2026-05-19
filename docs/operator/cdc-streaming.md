# CDC streaming: the applier retry policy

`sluice sync start` opens a change-data-capture stream and applies
source changes to the target indefinitely. On a managed database
(PlanetScale-Vitess, managed Postgres / Patroni), the apply path will
periodically hit a **transient** infrastructure error — a Vitess
transaction-killer rollback, a vtgate restart, a tablet failover, a
throttler engagement, a Postgres serialization failure, a standby
promotion, or a connection reset. None of these mean the operator's
data or configuration is wrong; they are normal operational events on
a managed deployment.

Sluice absorbs these with a **bounded, observable retry policy** on
the applier's batch loop. The full design rationale lives in
[ADR-0038](../adr/adr-0038-applier-retry-on-transient-errors.md); this
page is the operator-facing summary.

## What gets retried (and what does not)

The retry is **default-deny**: an error is retried only if it matches
a documented transient shape. Anything else exits the stream loudly so
a real problem surfaces immediately rather than being masked by a
retry loop.

**MySQL / Vitess — retriable:**

- `Error 1213` (InnoDB deadlock victim) — the idempotent replay
  succeeds against the new lock order.
- `Error 1105 (HY000)` whose message contains `vttablet` **and** one
  of `code = Aborted` / `code = Unknown` / `code = Unavailable` /
  `code = ResourceExhausted` — the Vitess transient class (tx-killer,
  vttablet not ready, failover in flight, throttler engaged).
- Connection lost / EOF / bad-connection / per-exec timeout — the
  driver reconnects on the next attempt.

**PostgreSQL — retriable:**

- `40001` (serialization failure), `40P01` (deadlock detected).
- `57P01` / `57P02` / `57P03` (admin / crash shutdown, cannot connect
  now) — standby promotion or restart.
- The entire `08*` class (connection exception).
- Connection lost / EOF / per-exec timeout.

**Explicitly NOT retriable (both engines):** duplicate-key
(`1062` / `23505`). A duplicate key during continuous sync is either
an operator data issue (a non-PK uniqueness violation) or a sluice
idempotency gap — both deserve a hard, loud failure, never a
retry-and-mask. This is a deliberate fence (see ADR-0038 and GitHub
issue #14); it is not a gap.

## The retry shape

When a retriable error fires, the streamer waits and re-applies the
batch:

- **Exponential backoff**, starting at `--apply-retry-backoff-base`
  (default `100ms`), doubling each attempt, capped at
  `--apply-retry-backoff-cap` (default `30s`). With defaults the
  per-attempt sequence is `100ms → 200ms → 400ms → 800ms → 1.6s →
  3.2s → 6.4s → 12.8s`.
- **Bounded attempts.** After `--apply-retry-attempts` (default `8`)
  *consecutive* failures of the same un-progressed position, the
  stream exits with a terminal `apply retry budget exhausted` error
  that preserves the most recent transient. Eight attempts with the
  default schedule is roughly four minutes of total wait — long
  enough to ride out a vtgate restart or a Patroni failover, short
  enough that a genuinely stuck batch does not hide for hours.
- **Counter resets on progress.** If the persisted CDC position
  advanced between attempts (a partial batch committed before the
  failure), the consecutive-failure counter resets to 1. A stream
  surviving for days does not accumulate retry debt from unrelated,
  widely-spaced transients.
- **Idempotent by construction.** A retried Insert is absorbed by the
  PK-keyed UPSERT; a retried Update/Delete is a no-op against the
  already-applied state (ADR-0010). Position and data are written in
  the same target transaction, so a rolled-back attempt rolls back
  both and the next attempt writes both atomically (ADR-0007). The
  retry adds no new correctness requirement.

## Observability

Every retry logs at INFO:

```
level=INFO msg="applier: transient error; retrying"
  stream_id=… engine=… attempt=3 max_attempts=8
  backoff=400ms err="<the wrapped driver error>"
```

Budget exhaustion logs/returns a terminal error at the stream's exit:

```
pipeline: apply retry budget exhausted after 8 consecutive
failures at position "<token>": <last transient>
```

Same transient eight times means the dependency is genuinely down
(e.g. vtgate is not coming back); different transients across the
eight means intermittent infrastructure.

## Configuration

| Flag | Default | Range | Notes |
|---|---|---|---|
| `--apply-retry-attempts N` | `8` | `1`–`64` | `1` = no retry; exit on the first transient (the pre-v0.42.0 behaviour). |
| `--apply-retry-backoff-base DUR` | `100ms` | `10ms`–`10s` | Starting backoff before exponential doubling. |
| `--apply-retry-backoff-cap DUR` | `30s` | `1s`–`300s` | Per-attempt upper bound. |

Out-of-range values are **rejected at startup** with a precise error
(not silently clamped) so the worst-case envelope an operator computes
from the docs is always the one the policy actually uses.

Operators on bare-metal MySQL with an unbounded transaction lifetime,
or anyone who wants the strict fail-fast behaviour, can opt out with
`--apply-retry-attempts=1`. Operators expecting a slow Patroni
failover under throttler load can widen the envelope, e.g.
`--apply-retry-attempts=20`.

## See also

- [ADR-0038](../adr/adr-0038-applier-retry-on-transient-errors.md) —
  full design, classifier tables, and the operator-review sign-off.
- ADR-0010 — idempotent applier (the load-bearing precondition).
- ADR-0007 — position/data atomicity (preserved by retry).
- ADR-0027 — source-transaction-boundary batching (preserved by retry).
