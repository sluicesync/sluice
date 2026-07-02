# ADR-0146: Cold-copy chunk connection-OPEN reconnect-retry

- **Status:** Accepted
- **Date:** 2026-07-02
- **Related:** ADR-0108 (cold-copy *target-write* reparent-retry), ADR-0109 (cold-copy *source-read* reconnect + resume — the two mid-copy siblings), ADR-0096 (keyset-chunked reads — the dup-free resume substrate), ADR-0116 (MySQL connection-budget prober / SQLSTATE 53300 slot-exhaustion retry), ADR-0123 (run-wide connection-budget gate). Roadmap item: cold-copy resilience.

## Context

ADR-0108 rides a transient that strikes a chunk's **target write**; ADR-0109 rides one
that strikes a chunk's **source read**. Both assume the chunk's reader + writer are
already **open**. There was a remaining hole: a transient connection drop at the moment
a non-zero chunk **opens** its source reader + target writer.

`acquireChunkConn` (`internal/pipeline/migrate_parallel.go`) retried ONLY the
connection-slot-exhaustion class (SQLSTATE 53300, ADR-0116). Every other open error —
including a transient `ping: invalid connection` blip — hit the fail-fast branch and
aborted the **whole migrate**. A live 49 GB PG→PlanetScale run lost ~45 GB of copy
progress to a single such blip:

```
pipeline: copy table "bench" (parallel): open connections for chunk 24:
    mysql: ping: invalid connection
```

Loud (no silent loss) but not resilient — a copy that crosses a transient blip at a
chunk open dies and needs a manual `--resume`.

## Decision

Layer a **bounded reconnect-retry** onto the chunk connection-OPEN path, mirroring the
ADR-0108/0109 envelope. The retry loop lives in a new engine-neutral file
`internal/pipeline/copy_chunk_open_retry.go`:

1. **Classifier `isRetriableChunkOpenError(err)`** — returns true for the transient
   connection-drop class only: an engine-classified `ir.RetriableError`, the stdlib
   `driver.ErrBadConn` / `io.EOF` / a `net.Error` with `Timeout()`, and a conservative
   allow-list of driver/OS text shapes (`invalid connection`, `connection reset`,
   `broken pipe`, `did not properly respond` (Windows wsarecv), `connection refused`,
   `bad connection`, `unexpected EOF`). **Permanent faults** (`Access denied`,
   `Unknown database`, `invalid DSN`, `Authentication failed`, `parseDSN`,
   `permission denied`) are checked FIRST and return false. **Unknown shapes default to
   false** (fail fast) — unlike `streamer_retry.go`'s default-transient
   `isTransientOpenError`, so a real fault is never masked. The pipeline package imports
   no engine package: classification rides `ir.RetriableError` via `errors.As`.

2. **`openChunkConnWithRetry`** — the pure retry core `acquireChunkConn` delegates to.
   On a slot-exhaustion error it shrinks parallelism + backs off (EXACT prior behaviour);
   on a classified transient it backs off within a bounded wall-clock/attempt budget and
   retries the open, **keeping the caller's gate token** (same chunk, same budget slot);
   on budget exhaustion it returns a LOUD terminal error wrapping the most recent
   transient; on anything else it fails fast + loud with the exact prior message.

## Retry / double-copy safety

`openOneChunkConn` fails at reader-open OR writer-open, closes any partial
(`closeIf(rdr)`) and returns `(nil, nil, err)` — i.e. the open fails **before any
COPY / WriteRows**, exactly like a 53300. So reconnecting and re-running the chunk from
its recorded `chunk.LastPK` cursor (`WHERE (pk) > LastPK`, the existing keyset resume in
`copy_source_read_retry.go` / `resumeFromChunkCursor`) is provably **dup-free**: no rows
were written before the failed open, so the resume cannot double-copy. This ADR only
makes the OPEN reconnect instead of aborting; the dup-free resume itself is already
handled downstream (ADR-0096).

## Bounds

Package vars (zero-value-safe — no config field, no EnableX-defaulting-true trap):
`chunkOpenRetryMaxWall` = 30 min (matches ADR-0109's source-read wall-clock; a
chunk-open blip and a mid-read drop are two faces of the same class of event — a target
storage auto-grow), a high attempt cap as a runaway backstop, and exponential backoff
(100 ms → 30 s cap). Long enough to ride a grow/failover, short enough that a
genuinely-down endpoint surfaces loudly. The vars are mutated only by unit tests to
shrink the envelope.

## Scope / follow-ups

- Covers the **cold-copy chunk-open** path (chunks 1..M-1). Chunk 0 reuses the
  orchestrator's already-open primaries and never goes through `acquireChunkConn`.
- The per-table single-stream open (`openTablePair`) is a separate seam; not addressed
  here. If a live incident shows an open blip there, extend the same classifier to it.

## Testing

- `isRetriableChunkOpenError` table test: every transient shape → true (incl.
  case-insensitive, wrapped, and `ir.RetriableError`), every permanent shape → false,
  permanent-beats-transient ordering pinned, `nil` + unknown → false.
- `openChunkConnWithRetry` behaviour test against a fake open func: transient-N-then-
  success reconnects; always-transient gives up LOUDLY at a bounded attempt cap (asserts
  no infinite loop); permanent fails on the first open with zero retries; ctx-cancel
  breaks the backoff promptly.
- This is a **concurrency** change (cold-copy retry loop): CI's `-race` Integration job
  must pass before it is tagged.
