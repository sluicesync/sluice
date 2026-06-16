# ADR-0094: Wire VStream reshard `Reopen` into the Streamer (auto-reshard-follow)

## Status

Accepted. Implements **Option A** of the reshard production-wiring decision (operator-chosen 2026-06-16). Builds on the reader-side reshard machinery already shipped in `internal/engines/mysql/cdc_vstream.go` (`ShardLayoutChangedError`, `ErrShardLayoutChanged`, `Reopen`) and proven exactly-once across a live reshard by the `vitessreshard`-tagged chaos test. **Shape-A (`--inject-shard-column` consolidation) reshard interplay is explicitly deferred** to a later ADR. Discovered while analysing `planetscale/fivetran-source` issue #90 (the sharded-source behaviour sweep).

## Context

A Vitess reshard — a shard split, merge, or `MoveTables` — surfaces to a VStream consumer as a vtgate **JOURNAL** VEvent. sluice's VStream reader already handles the source half completely:

- It opens the stream with `StopOnReshard: true`, so when the journal commits the gRPC stream terminates cleanly with no further events on the retired shards.
- It surfaces a typed `ShardLayoutChangedError` carrying the **new** shard layout, each new shard tagged with the **journal-stamped GTID** vtgate emitted at the seam — so a stream opened against that layout resumes exactly at the cut, with no gap and no overlap.
- `reader.Reopen(ctx, resh)` rebuilds the stream against the new layout from those GTIDs, clears the per-shard field cache (new tablets emit fresh FIELD events), and returns a fresh `ir.Change` channel.
- The chaos integration test `TestVitessReshard_ChaosExactlyOnce` (`internal/engines/mysql`, `vitessreshard` tag, scheduled `extended-suites.yml`) drives a real reshard on a `vitess/lite` cluster and asserts **exactly-once** delivery across the seam — but it drives `Reopen` from a collector loop **inside the test**, explicitly standing in for "the production Streamer loop".

**The gap:** the production `pipeline.Streamer` never calls `Reopen`. `ShardLayoutChangedError` implements only `Error()` and `Is()` — not `ir.RetriableError` — so when the change channel closes mid-stream and the Streamer surfaces the reader's `Err()` (the GitHub #19 `sourceErrFn` path, settled in `phaseSettleDispatch`), `runWithRetry`'s `classifyRetriable` returns false and the Streamer **exits loudly** (`"shard layout changed … reopen required"`). So today reshard is **LOUD-terminal, no silent loss** — but the reader's `Reopen` is dead code from the product's perspective.

That terminal exit also leaves an **unverified restart path**: the persisted position is the *old* vgtid. On `sync start --resume`, the reader re-discovers the *new* shards but decodes *old* GTIDs against them — which most plausibly trips `ir.ErrPositionInvalid` → an ADR-0093/ADR-0022 cold-start **re-snapshot** (a full, expensive re-copy), with a worst-case risk of a partial-GTID-match gap. That path has no test.

## Decision

Wire the existing `Reopen` into the Streamer so a reshard is **followed automatically and seamlessly**, instead of exiting.

1. **New IR capability `ir.ReshardReopener`.** The pipeline cannot import the engine package, and `ShardLayoutChangedError` is engine-specific, so the detect-and-reopen step is exposed as an optional reader capability that keeps the typed error inside the engine:

   ```go
   type ReshardReopener interface {
       // ReopenAfterReshard inspects the reader's own terminal error.
       // If it is a shard-layout-change (reshard) signal, it rebuilds
       // the stream against the new layout (journal-stamped GTIDs) and
       // returns a fresh change channel with ok=true. If the terminal
       // error is NOT a reshard, returns ok=false (caller handles it as
       // a normal error). A reshard whose reopen fails returns ok=true
       // with a non-nil error.
       ReopenAfterReshard(ctx context.Context) (<-chan ir.Change, bool, error)
   }
   ```

   The mysql `vstreamCDCReader` implements it by `errors.As`-matching its cached `Err()` to `*ShardLayoutChangedError` and delegating to the existing `Reopen`. Engines without it (binlog MySQL, Postgres) are a silent no-op — reshard is a Vitess-only concept.

2. **Bounded reopen loop in `runOnce`'s apply phase.** After `dispatchApply` returns on a clean channel close, the Streamer probes the reader for a reshard via `ReopenAfterReshard`. On a successful reopen it re-wires the intercept chain over the **new** channel (no cold-start seed re-injection — a reopen is a continuation, not a fresh stream) and re-enters `dispatchApply`; otherwise it falls through to the existing `phaseSettleDispatch` terminal/retry handling unchanged. The loop is bounded by a reopen-attempt budget so a source that journals repeatedly (or a persistently failing `Reopen`) cannot spin forever — budget exhaustion is a loud terminal error, not a silent stall.

3. **Position persistence is unchanged (ADR-0007).** The applier persists the CDC position in the same transaction as the data for every batch, so once the post-reshard stream applies its first batch the new-layout vgtid is durably persisted. A crash mid-reopen leaves the last *old-layout* position persisted; the restart path (below) recovers it.

4. **Restart-after-crash safety net.** Auto-follow handles the live case. If the process dies between the journal and the first post-reopen batch, restart resumes from the old-layout position — the same unverified path as before. This ADR does **not** make that restart seamless (the journal GTIDs are only available live, on the error); it relies on the ADR-0093/ADR-0022 loud invalid-position → cold-start re-snapshot floor. A characterization test pins that this restart is loud-and-correct (re-snapshot), never a silent gap.

5. **Scope: single-stream only.** Shape-A multi-shard consolidation (`--inject-shard-column`) layers per-shard streams + a lease; a reshard of an underlying shard mid-consolidation interacts with the lease and the composite-PK disjointness invariant in ways that need their own analysis. When `--inject-shard-column` is engaged, reshard auto-follow is **not** enabled in this ADR — the stream stays on the existing loud-terminal behaviour, and the interplay is deferred to a follow-up ADR.

### Test strategy

- **Unit (`-race`, required CI — the `Test (ubuntu-latest)` job).** A fake `ir.CDCReader` implementing `ReshardReopener` drives the Streamer's reopen loop: channel close → reopen returns a fresh channel carrying post-reshard events → assert the apply continues over the new channel with no lost/duplicated event; reopen-budget exhaustion → loud terminal; a non-reshard close → unchanged terminal/retry. This is the gating `-race` coverage for the concurrency of the channel swap + intercept-chain re-wrap.
- **End-to-end (scheduled `extended-suites.yml`, `vitessreshard`).** Add a **Streamer-level** reshard test (the existing one is reader-level) driving a real `vitess/lite` reshard through `pipeline.Streamer` into a target, asserting src==dst exactly-once across the seam. Validated locally on the multi-process Vitess cluster before the tag (the `-race`-before-tag rule applies — this is a concurrency chunk).

## Alternatives considered

**Option B — document a halt-and-resume contract, keep terminal.** Keep the loud exit; document "restart on reshard". Rejected: it leaves the proven `Reopen` machinery dead, forces a full (expensive) re-snapshot on every reshard rather than the seamless journal-GTID continuation the reader already provides, and still requires verifying the currently-untested restart path. Option A reaches the *correct* behaviour with the seam validation already in hand.

**Auto-Reopen with no bound.** Rejected: a pathological source could journal in a tight loop; an unbounded reopen loop would mask that as a silent stall. The bounded budget keeps the failure loud.

## Consequences

- A Vitess/PlanetScale reshard on a single-stream sync is now followed automatically and exactly-once, with no operator intervention and no re-snapshot — the seam is bridged by the journal GTIDs.
- The reader's `Reopen` stops being dead code; the chaos test's in-test collector loop is now genuinely mirrored by production.
- New optional capability `ir.ReshardReopener`; the engine keeps `ShardLayoutChangedError` private. The blank-var capability assertion in the mysql package guards the method set.
- Concurrency-adjacent (channel lifecycle in the core apply loop) → `-race`-before-tag; the bounded loop and no-seed-on-reopen detail are the load-bearing correctness points and are unit-pinned.
- Shape-A consolidation reshard remains loud-terminal (deferred); documented as a known limitation until its own ADR.
