# ADR-0014: Phase-aware error-hint registry

## Status

Accepted. Implemented in v0.2.0 (`internal/pipeline/hints.go`).

## Context

Wrapped pipeline errors arrive at the operator in this shape:

```
pipeline: bulk copy: postgres: insert into "users":
ERROR: relation "users" does not exist
```

The phase prefix (`pipeline: bulk copy:`) is good — it tells the operator which phase failed. The driver-level inner error is correct but cryptic to anyone who hasn't memorised PG's surface. Common diagnoses (e.g. "schema-apply silently failed") take longer than they should.

Three obvious approaches: (a) translate every driver error into a sluice-shaped one (inflexible, drift over driver versions, infinite scope), (b) attach structured hint metadata via `errors.As` (ergonomic for programmatic consumers but invisible to humans reading logs), (c) append a one-line `hint:` to the error text (additive, presentation-only, zero impact on `errors.Is`/`As` traversal).

## Decision

Option (c). A small registry of `(phase, errorContains, hint)` triples. Each phase boundary in the orchestrator wraps its returned error through `wrapWithHint(phase, err)`, which appends `\nhint: <line>` when a registry entry matches. The wrapping uses `fmt.Errorf("%w\nhint: %s", err, h)` so traversal helpers still find the underlying error.

Substring matching is intentional. Driver error messages drift slightly across versions; matching against SQLSTATE codes would be more robust but requires a per-driver shape and forces the registry into engine-specific buckets.

The registry is **deliberately tiny** — seven entries at v0.2.0:

- bulk-copy "does not exist" / "doesn't exist" → "target table not found — did the schema-apply phase fail or apply to a different schema?"
- connect "connection refused", "password authentication failed", "does not exist" → DSN diagnostics
- schema-apply "permission denied for schema" → grant guidance
- cdc "permission denied for replication" → REPLICATION attribute guidance

Each hint must be load-bearing — addressing a class of failure operators reliably hit. Anything that fires on <1% of operators is noise.

## Consequences

Operators get a one-glance diagnosis for the most common failures without losing any of the original error detail. The hint shape is invisible to programmatic consumers (`errors.Is`/`As` skip past presentation), so wrapping doesn't break library users.

The cost is a registry that needs occasional pruning. The discipline of keeping it tiny is the load-bearing constraint — adding hints for every error pattern that anyone hits would dilute the signal. New hints land with a comment naming the failure mode that justified them. The registry tests pin the count loosely; a future PR that doubles the size should warrant scrutiny.
