# sluice v0.99.92

**Bulk cold-copy now rides through a transient target reparent instead of crash-looping.** The headline is roadmap item 33 (ADR-0108): a cold-copy that runs for minutes can outlive a transient target primary reparent — on a non-Metal PlanetScale target a storage auto-grow at a volume boundary (~39 GB and up) triggers a primary reparent that kills the in-flight `INSERT` connection. sluice now reconnects and retries the in-flight batch instead of aborting the process, so a large migration no longer dies mid-copy when the target resizes underneath it. This release also lands the default-off Phase 1 telemetry seam (ADR-0107) for future proactive target-health adaptivity.

## Added

**MySQL cold-copy reparent / "not serving" resilience (ADR-0108, item 33).** A bulk cold-copy that crosses a managed-Vitess / PlanetScale reparent (commonly a non-Metal storage auto-grow at a volume boundary) used to fail fatally: the `RowWriter` returned the raw "not serving" / vttablet `code = Unavailable` driver error unwrapped, the process exited, and a supervisor crash-looped straight back into the still-in-progress reparent. The per-batch flush is now wrapped in a bounded, observable retry — the copy-phase analog of the apply-phase ADR-0038 retry — that routes the error through the same `classifyApplierError` transient classifier the CDC apply path uses (so a reparent / connection-reset / vttablet-`Unavailable` retries; any non-retriable error fails loudly, unchanged). The retry envelope is 12 attempts × 100 ms→30 s exponential backoff (~4 min), long enough to ride a storage-grow reparent and short enough that a genuinely-down target surfaces rather than hiding. After a reparent the pinned connection is dead, so every retry re-acquires a **fresh** pooled connection (the pool reconnects to the new primary) and re-runs the exec plus the session-scoped `SHOW WARNINGS` probe on it — the dead connection is never reused. Because the retry is local to each batch/worker, a transient on one table no longer aborts its sibling table copies; a genuine non-retriable error still cancels peers, unchanged. The idempotent (UPSERT) path absorbs an ambiguous-commit replay natively. The plain-`INSERT` path carries one named, tested wart: a plain cold-copy batch is a single atomic multi-row `INSERT`, so a classified transient leaves it either fully rolled back (the retry succeeds clean) or committed-but-the-ack-was-lost — in which case the byte-identical retry collides on the rows it already landed with Error 1062. Because cold-copy is the sole writer onto a fresh target and the batch is byte-identical, a 1062 *on the retry of the same batch* proves those exact rows are already durable, so it is tolerated with a loud WARN (no silent loss). That tolerance is scoped strictly to a post-transient retry — a first-attempt 1062 stays terminal, so a real uniqueness violation / dirty target still fails loudly. Scoped to MySQL cold-copy (the demonstrated path); the analogous Postgres `COPY`-path gap is noted as a follow-up in ADR-0108.

**Advisory target-health telemetry seam — default-off preview (ADR-0107 Phase 1, item 32).** The engine-neutral seam for an optional control-plane telemetry provider (target CPU / memory / storage utilisation), plus its advisory consumers, ships now driven entirely by an in-test fake — the real PlanetScale Prometheus-metrics provider is Phase 2. When a provider is wired in a later release, it lets the apply path react proactively *before* the reactive signals push back: the per-lane AIMD controller consults a `TelemetryHint` under its existing mutex and, on a fresh CPU/memory high-water crossing (default 0.85), applies at most one multiplicative-decrease then holds — it can only hold or shrink within `[Floor, Ceiling]`, never grow a batch or advance a position; and a storage-headroom sidecar emits a one-WARN-per-edge "target storage approaching capacity" signal (pairs with the cold-copy reparent work above). It is strictly advisory — the exactly-once frontier, AIMD, and tx-killer recovery stay authoritative, and a stale / no-signal / nil provider degrades to today's behaviour byte-for-byte. No PlanetScale specifics leak into the engine-neutral core. **With no provider wired (the default), nothing engages — this release is a no-op for the telemetry path until Phase 2.**

## Compatibility

No configuration changes and no behaviour change for an untroubled migration. The cold-copy retry only engages on a classified transient target error that previously aborted the copy — where the old behaviour was a fatal exit, the new behaviour is a bounded reconnect-and-retry that either succeeds or, on exhaustion, fails loudly with a clearer message. The telemetry seam is entirely default-off (no provider, no flags) until Phase 2. No resume-format, wire, or result-state changes.

## Who needs this

Anyone running a large `sluice` migration or cold-copy into a **managed-Vitess / PlanetScale MySQL target**, especially a **non-Metal PlanetScale** database whose storage auto-grows mid-copy — the copy now rides through the resize-induced primary reparent instead of dying. No action required to benefit; it is automatic.

## Install

```
go install sluicesync.dev/sluice/cmd/sluice@v0.99.92
```

Container image:

```
docker pull ghcr.io/sluicesync/sluice:0.99.92
```
