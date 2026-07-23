# ADR-0038: Applier retry on transient errors

## Status

**Accepted (2026-05-18) — design reviewed and signed off by the owner;
implemented.** The design below is complete and adopted as-written,
with the four operator-review pin-downs in the next section. Not
demand-gated and no upstream hard-sequencing dependency (see
pin-down 2). The classifier + retry loop + flags landed in v0.42.0
and were extended in v0.46.0 / v0.48.0 / v0.52.x; the final
pin-down-conformance pass (Vitess `code = Unknown` substring added to
the MySQL classifier, the mandatory pin-down-4 literal-substring
test, pin-down-3 startup range validation on the three dials, and the
operator guide) completes the ADR as-written. **Extended v0.99.288:**
the retry loop's scope now also covers the CONNECT phase of each
attempt — a transient network failure while re-establishing the
target applier / source readers (`connectPhaseError` marker +
positive-match transient shapes, `internal/pipeline/streamer_connect_retry.go`)
rides the same budget instead of exiting; pre-fix, only errors raised
inside a flowing attempt were classified (the 2026-07-22 scale-soak
incident). v0.99.286 similarly closed the trigger-CDC transport
classification gap (`internal/engines/internal/triggercdc`). Originating bug:
GitHub #13.

## Operator-review sign-off (2026-05-18)

The design was reviewed for sign-off (not a multi-round open-question
dialogue — the ADR was already fully specified). Adopted as-written
with these four pin-downs, owner-accepted:

1. **`RetriableError` interface lives in `internal/ir`** (resolves the
   "in `internal/ir` or `internal/pipeline`" ambiguity in the
   Decision / step 1). Rationale: it is the engine-neutral contract
   home alongside `Change`/`Position`; engines and pipeline both
   already import `ir`; the per-engine classifiers stay engine-local
   and only the interface is shared.
2. **Risk posture blessed — no hard-sequencing behind GitHub #14.**
   Retry safety rests entirely on ADR-0010 idempotency; the
   ambiguous-commit path (target tx committed, ack lost →
   connection-lost retriable → batch replays) leans on it hardest.
   The design fences this correctly: the one *suspected* idempotency
   gap (#14, a duplicate-PK shape) is kept in the **non-retriable**
   set (`1062`/`23505`) so it surfaces loudly, never masked/amplified.
   The ambiguous-commit replay path is covered by ADR-0010 (PK-keyed
   UPSERT / tolerated zero-rows update+delete) + ADR-0007 (atomic
   position+data). Therefore ADR-0038 is **not** gated on #14 being
   resolved first (contrast the ADR-0049→0050 hard-sequencing); the
   suspected gap is fenced in the non-retriable classification.
3. **Defaults blessed as-written:** 8 attempts, 100ms base → 30s cap
   (~4 min worst case). Accepted as the right default for the managed
   Vitess / managed-PG envelope; operators override via
   `--apply-retry-attempts` (1–64) when a slow Patroni failover under
   throttler load needs a longer envelope. No change to the table.
4. **Vitess `Error 1105` substring classification accepted as the
   pragmatic choice** (Vitess wraps all transients in `1105 (HY000)`
   with a free-text payload — no structured code exists to match on).
   Mandatory mitigation: a unit test that **pins the exact matched
   substrings** (`vttablet` / `code = Aborted` / `code = Unknown` /
   `code = Unavailable` / `code = ResourceExhausted`) plus an inline
   comment + this ADR ref so a future Vitess wording change is caught
   by a failing test, not a silently-non-retried production error.

## Context

`sluice sync start` against PlanetScale-MySQL exits non-zero on the first transient Vitess error, e.g.:

```
Error 1105 (HY000): target: ks.-.primary: vttablet: rpc error:
code = Aborted desc = transaction <id>: in use: in use:
for tx killer rollback (CallerID: ...)
```

This is **GitHub issue #13**. The error is a transaction-killer signal — Vitess aborts the in-flight tx when it crosses the configured `--transaction_lifetime`, when vtgate restarts, when a tablet failover happens, or when the throttler engages. All of these are normal operational events on a managed Vitess deployment; an operator running `sluice sync start` for hours or days will hit them. Today the applier rolls back the batch, logs `WARN msg="mysql: applier: batch rollback on error"`, and the streamer exits.

The same shape exists on the Postgres side for serialisation failures (`SQLSTATE 40001`) and deadlocks (`40P01`) — neither has been reported by an operator, but the code paths are symmetric and a fix for MySQL that ignores PG would be conspicuously asymmetric.

Today an operator's only options are:
1. **Wrap in a supervisor.** systemd `Restart=on-failure`, k8s pod restart policy, or `while :; do sluice sync …; done`. The streamer warm-resumes from the persisted position, so this *works* — but the operator carries the retry policy.
2. **Reduce batch size.** `--apply-batch-size 10` shrinks each target tx so it crosses tx-lifetime less often. Mitigates frequency, doesn't eliminate.

Both are workable for niche deployments but unusable for a "drop in and run for a week" experience. The fix is a bounded, observable retry inside the applier dispatch loop.

## Decision

Add a retry policy to the applier's batch dispatch loop, gated by an error classifier that distinguishes **retriable transients** from **terminal failures**.

The retry lives **on the pipeline side**, not in each engine's applier. The pipeline already owns the batch loop (`pipeline.Streamer`); the applier returns a typed error and the pipeline decides whether to retry. This:

- keeps the retry policy in one place (one set of dials, one log shape, one test surface),
- lets PG and MySQL share the policy without copying it,
- matches sluice's IR-first tenet — engines surface what they observe; the orchestrator picks the strategy.

The engine appliers gain one new contract: they return errors that satisfy a new `RetriableError` interface, which carries enough information for the pipeline to classify:

```go
// in internal/ir or internal/pipeline
type RetriableError interface {
    error
    // Retriable reports whether this error is a transient that the
    // operator-side retry policy should attempt to recover from.
    Retriable() bool
    // RetryHint is an optional minimum backoff floor — set when the
    // underlying engine has a meaningful "don't retry sooner than X"
    // signal (Vitess vttablet sometimes returns one). Zero means
    // "use the default policy floor".
    RetryHint() time.Duration
}
```

Each engine's applier wraps its raw driver errors in a typed wrapper that classifies the error. The wrapper is engine-local; the interface is pipeline-shared.

### Error classification

**MySQL / Vitess** (in `internal/engines/mysql/change_applier.go`):

| Source | Retriable | Rationale |
|---|---|---|
| `Error 1105 (HY000)` with message matching `vttablet`/`code = Aborted`/`code = Unknown`/`code = Unavailable`/`code = ResourceExhausted` | **yes** | Vitess class of transients — tx-killer, vttablet not ready, throttler, failover. |
| `Error 1213 (40001)` (InnoDB deadlock victim) | **yes** | Idempotent retry replays the same change; second attempt succeeds against the new lock order. |
| `Error 1062 (23000)` (duplicate key) | **no** | Either non-PK uniqueness violation (operator data bug) or an applier-side idempotency gap (sluice bug); both deserve a hard fail. |
| Connection lost / EOF / `driver.ErrBadConn` | **yes** | Driver auto-reconnects on next exec; retrying the batch on a fresh conn is the right move. |
| All other `Error N` | **no** | Default-deny — adding to the retriable set requires a documented justification. |

**PostgreSQL** (in `internal/engines/postgres/change_applier.go`):

| Source | Retriable | Rationale |
|---|---|---|
| `SQLSTATE 40001` (serialisation failure) | **yes** | The standard PG retry signal under `SERIALIZABLE`. |
| `SQLSTATE 40P01` (deadlock detected) | **yes** | Same as MySQL 1213. |
| `SQLSTATE 57P01` (admin shutdown) / `57P02` (crash shutdown) / `57P03` (cannot connect now) | **yes** | Standby promotion / restart. |
| `SQLSTATE 08*` (connection exception) | **yes** | Auto-reconnect on next attempt. |
| `SQLSTATE 23505` (unique violation) | **no** | Same rationale as MySQL 1062. |
| All other SQLSTATEs | **no** | Default-deny. |

The classification lives in each engine's applier so the engine-specific error shape (Vitess wraps PG-style `code = X desc = Y` inside a MySQL Error 1105) stays contained. The pipeline only sees the interface.

### Retry shape

Pipeline-side, in the batch loop:

- **Exponential backoff**, base `100ms`, doubles each attempt, capped at `30s`. So: `100ms → 200ms → 400ms → 800ms → 1.6s → 3.2s → 6.4s → 12.8s → 25.6s → 30s → 30s → …`.
- **Maximum attempts: 8.** After 8 consecutive failures of the same `(batch_id, source_position)` pair the pipeline gives up loudly with a terminal error that preserves the most recent transient. Eight × max(30s) ≈ 4 minutes — enough to ride out vtgate restarts and Patroni failovers, short enough that a *real* stuck batch doesn't hide for hours.
- **"Same batch" tracking**: the pipeline remembers `(batch_id, source_position)` of the last failing attempt. If the next failure has the same pair, increment the counter; if the pair changes (because a partial batch committed and the next batch is new), reset to 1. This prevents a slow drift of unrelated transients from accumulating into a false-positive "stuck" verdict.
- **Reset on success.** Any successful batch resets the consecutive-failure counter to 0. A streamer surviving for hours doesn't carry retry debt forward.
- **Honor `RetryHint()`.** If the error carries a minimum-backoff hint larger than the computed backoff, use the hint. This is forward-looking — no engine emits hints today, but Vitess `RESOURCE_EXHAUSTED` errors sometimes carry a `retry-after` field.

### Observability

Every retry attempt logs at INFO:

```
level=INFO msg="applier: transient error; retrying"
  stream_id=foo engine=mysql attempt=3 max_attempts=8
  backoff=400ms err="<the wrapped error message>"
```

The terminal-give-up logs at ERROR:

```
level=ERROR msg="applier: retry budget exhausted; stream exiting"
  stream_id=foo engine=mysql attempts=8 last_err="..."
```

The terminal error message includes the entire retry chain in a structured form so the operator sees what was happening — same transient eight times means "vtgate is genuinely down", different transients means "intermittent infrastructure issue".

### Configuration

Three new flags on `sluice sync start`, all optional:

| Flag | Default | Range | Notes |
|---|---|---|---|
| `--apply-retry-attempts N` | 8 | 1–64 | `1` means "no retry; exit on first transient" (the v0.39.x behaviour). |
| `--apply-retry-backoff-base DURATION` | 100ms | 10ms–10s | Starting backoff before exponential doubling. |
| `--apply-retry-backoff-cap DURATION` | 30s | 1s–300s | Per-attempt cap. |

Defaults are tuned for "managed Vitess + light-to-medium workload" since that's the reported pain point. Operators on bare-metal MySQL with infinite tx-lifetime can set `--apply-retry-attempts=1` to opt out.

A config-file equivalent (`sluice.yaml`) takes the same three keys under `apply:` (matching the existing `apply.batch_size` shape).

## Consequences

- **Vitess and managed-PG operators stop seeing the "exits after 30 seconds in CDC mode" failure mode.** This is the load-bearing motivation — see GitHub issue #13.
- **Retry budget is bounded and observable.** A stuck batch surfaces after ≤ 4 minutes (with defaults), not silently forever.
- **Idempotency is the load-bearing precondition.** ADR-0010 already requires idempotent apply (PK-keyed UPSERT for inserts; tolerated zero-rows-affected for update/delete). The retry policy depends on this: a retried Insert can produce the same row twice → ON DUPLICATE KEY / ON CONFLICT absorbs it. A retried Update is a no-op on the already-applied state. A retried Delete is the same. The retry path adds no new idempotency requirement.
- **Tx-boundary alignment (ADR-0027) is preserved.** A failed batch rolls back at the target; the retry re-opens a fresh tx. Source TxCommit-driven flushes still align target tx boundaries with source tx boundaries; the retry is invisible to that alignment.
- **Position-and-data atomicity (ADR-0007) is preserved.** Position writes happen inside the same target tx as the data writes. A retry rolls back both; the next attempt writes both atomically again.
- **New surface to test**: the classifier (one unit test per retriable / non-retriable code) + the retry loop (one unit test per scenario: success-after-retry, exhaustion, mixed-error-reset, RetryHint floor). Both can be exercised against a fake error-returning applier without needing a real database; integration coverage is one or two cases.

## Why not retry inside each engine's applier

Two reasons:

1. **Single source of truth for the policy.** Operators learn the policy once (`--apply-retry-attempts`, the log shape, the give-up condition). Per-engine retry would mean MySQL and PG could drift apart, and adding a third engine doubles the policy surface.
2. **The orchestrator already owns the batch loop.** `pipeline.Streamer` is the layer that knows about batch_id, source position, and the stream's lifecycle. The applier is positioned correctly to *classify* errors but not to *decide* how the orchestrator responds. The interface boundary already exists; we just typify the error contract.

## Why not infinite retry with operator timeout

Two-knob policies (max-attempts + per-attempt cap) bound the worst case at a known value; an operator reading the docs can compute "this stream will exit after at most N × cap seconds of failure". An infinite-retry-until-timeout shape requires the operator to also configure a timeout knob, which (a) defeats the "drop in and run for a week" goal that motivated this ADR and (b) makes the failure mode harder to reason about — "did it retry or did it not retry" depends on a global wallclock, not on the cause.

## Why not retry on **all** errors

Default-deny is the conservative choice. Adding an error code to the retriable set is reversible (one-line change); making a non-retriable error retriable in a release that's then unwound is not. The list above starts narrow and grows with operator reports.

In particular, **`23505` / `1062` (duplicate key) is explicitly non-retriable.** A duplicate-key error during continuous-sync either means the source produced a non-PK uniqueness violation (operator data issue) or that the applier's PK-keyed UPSERT didn't fire (sluice bug — see GitHub issue #14 for a candidate of this shape). Either way, the right move is fail loudly so the operator notices, not retry-and-mask.

## Implementation plan

1. **Define `RetriableError` interface** in `internal/ir` (sits alongside `Change`, `Position`, etc).
2. **Add per-engine error classifiers** — `internal/engines/mysql/applier_errors.go` and `internal/engines/postgres/applier_errors.go`. Each exports a `classify(err error) error` that returns the wrapped, classified error (or the original for non-classified errors).
3. **Plumb classifiers into the appliers** — wrap `dispatch` and `commitBatch` return sites in the classifier. Surgical: one or two lines per call site.
4. **Add the retry loop to `pipeline.Streamer`** — wrap the per-batch `applier.ApplyBatch` invocation. Tracks `(batch_id, source_position)`, applies backoff, observes the give-up budget.
5. **Add the three CLI flags** + matching `sluice.yaml` keys.
6. **Tests**: classifier (unit), retry loop with a fake-applier (unit), one PS-Vitess + one PG integration test exercising a simulated transient.
7. **Docs**: extend `docs/operator/cdc-streaming.md` (or create it) with the retry policy. Cross-link from this ADR.

Total: probably ~700-1000 LOC + tests + a docs page. Single PR; cuts a v0.40.x or v0.41.0.

## See also

- [Operator guide: CDC streaming retry policy](../operator/cdc-streaming.md) — the operator-facing summary of this ADR
- ADR-0010 — idempotent applier (the load-bearing precondition)
- ADR-0027 — source-transaction-boundary CDC batching (preserved by retry)
- ADR-0007 — position persistence (preserved by retry)
- GitHub issue #13 — the originating bug report
- GitHub issue #14 — duplicate-PK cold-start on PS-MySQL; explicitly NOT in scope (the retry policy would mask it; this ADR keeps `23505/1062` non-retriable)
