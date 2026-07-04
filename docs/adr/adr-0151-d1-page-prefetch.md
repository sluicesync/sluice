# ADR-0151: D1 reader single-page prefetch

## Status

**Accepted (2026-07-04).** Roadmap item 54 (perf-parity backlog) follow-up on ADR-0132
(the `d1` query-API reader). Applies the chain-replay read-ahead pattern
(perf-parity gap #4's closure, `pipeline.streamIncrementalChanges`) to the D1 HTTP
page loop in `internal/engines/sqlite/d1_rows.go`.

## Context

The `d1` reader keyset-paginates a live Cloudflare D1 database over HTTP (pages of
[`d1PageSize`] = 1000, bound sent as an exact-text string param — ADR-0132 §6). The
loop was strictly sequential: fetch page N (a full HTTP round-trip to the Cloudflare
API), decode, stream its rows downstream, then fetch page N+1. Because each page's
JSON body is fully received before any of its rows stream, page N's last key — the
only input page N+1's request needs — is known the moment page N arrives. Every page
therefore paid one avoidable HTTP RTT of dead time, and for a WAN API the RTT is the
dominant per-page cost.

## Decision

A single fetcher goroutine issues the page requests strictly in order and hands each
page to the decode loop over an **unbuffered** channel. Because the handoff is
unbuffered, the fetcher holds at most ONE page beyond the one being consumed —
exactly one page of read-ahead, never an N-deep pipeline; memory is bounded at ≈ one
extra page. The fetcher derives page N+1's bound from page N's last raw row via the
same `extractKey` exact-text path, so requests are byte-identical to the sequential
loop (same SQL, same string bounds — the > 2^53 discipline included) and row order is
unchanged (single fetcher, single consumer, FIFO handoff).

Contract preservation (the load-bearing details):

- **Errors surface in sequence.** A failed page is delivered as that page's handoff
  and surfaces only when the consumer reaches it — after the prior page's rows have
  streamed, exactly as sequentially. A key-extraction refusal stops the fetcher
  silently; the consumer's own per-row decode deterministically reproduces the same
  loud error with full row context (never duplicated, never lost).
- **No silent truncation on cancellation.** The terminal page is explicitly marked
  (`d1Page.final`). If the channel closes without one — the fetcher was aborted by
  ctx cancellation mid-table — the consumer reports the cause via `Err()` instead of
  a clean short read. (The naive handoff would have made cancellation look like a
  complete table — a silent-truncation hazard this ADR names and pins.)
- **No goroutine leak.** The decode loop cancels a fetcher-scoped context on every
  return path, aborting the fetcher's in-flight HTTP request and its blocked handoff.
- The LIMIT/OFFSET fallback (tables with no orderable key) prefetches through the
  same loop — it needs no key extraction, only the offset.

This is RTT-hiding only, **not** within-table chunking — D1 within-table chunking
parallelism remains the deferred follow-up noted at `d1PageSize`.

## Coverage declaration (perf-parity matrix)

Reached: the D1-source page-read path, which serves both **MIG** (D1 as a migrate
source) and **CS** (the `d1-trigger` flavor's snapshot leg reuses the same reader).
Everything else n/a: D1 is never a write/apply target (ADR-0132/0134), has no backup/
restore/chain role, and the local-file `sqlite` reader has no network RTT to hide.
Matrix row 8 (read batching) CS cell updated in the same PR.

## Consequences

- One HTTP RTT per page is hidden behind downstream decode/write; on a ~1000-row
  page over a WAN API this is the dominant per-page latency term.
- The reader gains a goroutine, so the change rides the `-race` integration gate
  before any tag (the concurrency-chunk rule).
- Pinned by httptest units: overlap proof (page-2 request observed while page 1 is
  still undrained), byte-exact request order/bounds incl. a > 2^53 string bound,
  in-sequence loud failure of a prefetched page, and cancel-mid-stream reaping the
  fetcher with `Err() = context.Canceled`.

## Addendum (2026-07-04): the stage-local leg shares the fetcher

The `--stage-local` staging materializer (ADR-0145, `stage_d1.go`) originally kept its
own strictly-sequential page loop over the same `buildD1PageQuery` requests. It now
drives the SAME `fetchPages` fetcher goroutine (audit item 2.2 residual): `stageD1Table`
consumes the unbuffered `d1Page` handoff, so page N+1's HTTP request overlaps page N's
local insert — with the identical contract set (byte-identical request order/bounds,
in-sequence error surfacing, the explicit `final` marker so an aborted fetch can never
produce a silently truncated staged file, fetcher reaped on every return path). The
fetcher's silent stop on a key-extraction failure is reproduced loudly by the staging
loop's per-row `extractKey`, mirroring the reader's `decodeRow`. Pinned by the
`stage_d1_prefetch_test.go` units mirroring `d1_prefetch_test.go` at the staging seam
(overlap proven by holding the staging file's write lock while observing the page-2
request).

## Alternatives considered

- **N-deep pipeline / concurrent page fetches.** Rejected: memory grows with depth,
  and out-of-order fetches would need re-sequencing plus speculative bounds (page
  N+1's bound depends on page N). One page of read-ahead captures the full RTT win.
- **Prefetch via HTTP pipelining/keep-alive tuning.** Rejected: the win comes from
  overlapping the request with downstream work, not from transport micro-tuning; the
  Go client already reuses connections.
