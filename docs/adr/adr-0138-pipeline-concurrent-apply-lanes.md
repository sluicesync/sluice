# ADR-0138: Pipeline the concurrent key-hash apply lanes (Bug 168)

## Status

**Accepted (2026-06-28).** Fixes Bug 168 — found by the v0.99.152 Vultr cross-region
(`ewr`↔`ams`, 80.9 ms RTT) test round. Postgres scope (the validated cross-region target
and the path with a pipelining primitive); MySQL is a documented follow-up (it has no
pipelined-apply primitive at all — see "MySQL" below).

## Context

ADR-0092 identified that the serial CDC-apply path applies a batch of N changes as **N
serial `tx.ExecContext` round trips** + a position write + a commit (N+2 round trips), so
on a high-latency link apply throughput is capped at ~1/RTT *regardless of batch size* —
batching amortises only the commit fsync, not the per-row execs. ADR-0092 fixed this for
the **single-lane** PG batch path by queuing every statement onto one `pgx.Batch` and
sending them in a single `SendBatch` flush (round trips per batch drop from N+2 to ~3,
independent of N), in `QueryExecModeDescribeExec` so the wire encoding stays
byte-identical to the serial per-OID binary codecs.

ADR-0105 then added the **concurrent key-hash apply** path (W in-order lanes committing
concurrently, `internal/laneapply`). It is the **default** for CDC apply
(`defaultApplyConcurrency = 4`). For correctness conservatism its PG lane adapter
(`laneApplierAdapter.ApplyLaneBatch`) dispatched each change with the **serial**
`dispatch` on a `*sql.Tx` — explicitly *not* the ADR-0092 `pgx.Batch` pipeline — so that
value encoding was provably byte-identical to the serial path. On a LAN (~0.1 ms RTT)
that per-row round trip is invisible; **the ADR-0092 pipelining was never carried into
the lane path.**

### Bug 168 (the cost)

The Vultr cross-region round drove a `sqlite-trigger` → PG continuous sync over an 80.9 ms
WAN. Steady-state apply pinned at **~45–63 changes/s** and `--apply-batch-size 1000`
*did not move it*. Root cause (confirmed by code-reading — four symptoms all explained):
the default W=4 concurrent path applies each change as its own network round trip inside
the lane tx, so throughput = `lanes × (1/RTT)` = 4 × (1/0.08 s) ≈ 50/s — exactly the
observed number; batch size is irrelevant because batching only saves the commit, not the
per-row execs; and it is invisible on a LAN. Under a ~3000 ops/s source workload the lag
**diverged** (780k backlog, never drained). **No data loss** — exactly-once held
throughout; this is purely an apply-throughput / convergence ceiling on high-latency
links (cross-region, and to a milder degree managed targets like PlanetScale at ~7 ms).

The irony: ADR-0092's own file header describes this exact failure mode as the problem it
solved — but the default apply path (concurrent lanes) silently reintroduced it.

## Decision

**Route each concurrent lane's batch through the existing ADR-0092 pipelined primitives**
instead of serial per-row exec. Concretely, on the PG `laneApplierAdapter`:

1. The concurrent lane pool is opened in **`QueryExecModeDescribeExec`**
   (`openPgxDBDescribeExec`, the same constructor `pipelinePool` uses), sized
   `MaxOpenConns = MaxIdleConns = lanes`, registering the PostGIS geometry codec exactly
   as the serial/pipelined pools do (the Bug-74 codec-coverage requirement). Previously it
   used the Exec-mode `openDBAs` pool.
2. `ApplyLaneBatch` begins a **pipelined tx** on a lane connection
   (`beginPipelinedTxOn(laneDB)` — the body of `beginPipelinedTx` parameterised on the
   pool), queues each change via the existing **`dispatchPipelined`** (after the same
   `redactChange` + `stampShardChange` in the same order the serial lane path used), and
   flushes with **`flushAndCommit`** — one `SendBatch` round trip for the whole lane
   batch instead of one per row. The lane still writes **no position** (the frontier
   checkpoint owns it, ADR-0104), so the batch carries only data statements.
3. **Fallback preserved.** If the raw-conn escape is unavailable
   (`errPipelineUnavailable` — a non-pgx/wrapped conn), the lane falls back to the serial
   `*sql.Tx` dispatch with a one-time WARN (`warnPipelineFallbackOnce`) — loud, never
   silent, no throughput claim — identical to the single-lane `BeginTx` closure.

### Why this is correctness-safe (load-bearing)

- **Encoding fidelity is inherited, not re-derived.** `dispatchPipelined` is the *same*
  builder + `prepareApplierValue` codec path the proven single-lane ADR-0092 path uses,
  under the same `DescribeExec` mode (fresh re-describe per distinct statement → live-OID
  binary). The value-fidelity pins
  (`change_applier_pipelined_*_integration_test.go`) are the oracle; the lane path now
  exercises the identical encoding path. This is *not* a new value codec — it is the same
  one, reused on a second caller.
- **Exactly-once is untouched.** Only the in-lane *transport* changes (serial Exec →
  pipelined `SendBatch`); each lane still commits the same set of changes atomically in
  one tx, the orchestrator's contiguous-frontier checkpoint and the position relaxation
  (ADR-0104/0105) are unchanged, and warm-resume still re-streams + idempotently
  re-applies from the durable frontier.
- **Concurrency shape is unchanged.** W goroutines, each owning its own pinned backend /
  `pgx.Tx` / `pgx.Batch` (never shared across goroutines); the metadata caches were
  already guarded (`cacheMu`) because the serial lane path already called the same
  cache-reading helpers from W goroutines. The `-race` integration gate is the proof
  obligation and runs before the tag (concurrency-chunk rule).
- **Barriers stay serial.** Keyless / PK-changing-update / schema-snapshot changes still
  take the serial barrier path on the coordinator backend (rare, correctness over speed).

### Expected effect

Lane apply round trips drop from `N`/lane to ~3/lane-batch, so cross-region apply
throughput becomes governed by batch size and lane count rather than RTT. The 20 GB
Vultr cross-region round validates convergence under a sustained workload that previously
diverged.

## MySQL (out of scope here — follow-up)

The MySQL applier has **no pipelined-apply primitive at all**: both its single-lane batch
path and its concurrent lanes dispatch serially, so MySQL CDC apply is uniformly
RTT-bound over WAN (this is *not* an ADR-0105 regression — it predates the concurrent
path). Bringing MySQL to parity needs a genuine new primitive (protocol pipelining or
vetted multi-statement batching) with its own value-fidelity review — interpolated
multi-statement would change the encoding path, so it is a separate chunk, tracked as a
roadmap follow-up. The upcoming PlanetScale-MySQL-target test will exercise the current
serial MySQL apply at ~7 ms RTT.

## Consequences

- The default (and only fast) CDC apply path on PG is now RTT-independent over WAN,
  closing the convergence gap that made cross-region / high-latency continuous sync
  impractical at non-trivial write rates.
- The concurrent lane pool changes exec mode (Exec → DescribeExec); this is the same mode
  the single-lane pipelined pool has shipped in since ADR-0092, so the wire behaviour is
  already battle-tested.
- One refactor (`beginPipelinedTxOn`) is shared by the single-lane and lane paths,
  keeping a single source of truth for the pipelined-tx lifecycle (escape, F7
  synchronous_commit pin, Bug-164 FK bypass, fallback).
