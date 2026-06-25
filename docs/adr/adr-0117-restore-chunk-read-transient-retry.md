# ADR-0117: Transient-retry for object-store chunk reads on the restore path

- Status: Accepted
- Date: 2026-06-24
- Deciders: maintainer
- Related: [ADR-0114](adr-0114-ddl-phase-reparent-retry.md) (DDL-phase reparent retry — the *write/DDL*-phase analog), [ADR-0113](adr-0113-reparent-reconciliation-concurrent-restore.md), [ADR-0112](adr-0112-restore-within-table-chunk-parallelism.md)

## Context

The restore path streams each backup chunk straight from object storage into the target's bulk `COPY`/`WriteRows`: `streamChunkRows` (table data) and `streamOneChangeChunk` / `streamOneChunkWithPosition` (change chunks for incremental + broker replay) open the chunk with a single `Store.Get` and emit rows **as they decode**. There was no retry around that read.

A flaky object-store body — a short / truncated GET, a mid-stream connection reset — therefore surfaced deep in the decode path as `chunk reader: row decode: unexpected end of JSON input`, and because rows from the partially-read chunk had already been pushed into the open `COPY`, the read could not be safely re-streamed (that would duplicate the emitted rows). The error propagated up and **aborted the entire restore**.

Found live on the Track-C large-scale program (2026-06-24): a cross-engine MySQL→PlanetScale-Postgres cold-start restore of a 3220-chunk (~43 GB) corpus died ~6 tables in on a single chunk (`audit_trail-137.jsonl.gz`) with the decode error above. A `sluice backup verify` of the same chain immediately afterward reported **all 3220 chunks SHA-OK** — the chunk was intact at rest; the failure was a transient truncated read. With no retry, one transport blip wasted a ~50-minute restore, and across thousands of chunks the probability of hitting at least one such blip per restore is high. This is the read-phase counterpart of the gap [ADR-0114](adr-0114-ddl-phase-reparent-retry.md) closed on the DDL/index phase. Loud, never silent — but a robustness hole for a backup/restore tool, whose whole job is to tolerate flaky storage.

## Decision

Read each content chunk **fully into memory with a bounded transient-retry before decoding any row**, via one shared helper `fetchChunkVerified(ctx, store, file, expectedSHA256)`:

1. GET the object and `io.ReadAll` the body through a SHA-256 `TeeReader` (`readChunkBytesAndHash`).
2. Compare the digest to the manifest's `chunk.SHA256`. The manifest hash is over the **raw object bytes** — gzip plaintext for an unencrypted chunk, ciphertext for an encrypted one (the chunk writer hashes post-codec / post-encrypt) — so a short read mismatches and a clean object matches, for **both** the plaintext and encrypted read paths.
3. On a transient `Get`/read error **or** a digest mismatch, retry up to `chunkFetchMaxAttempts` (4: 1 + 3 retries) with a short backoff (200/400/800 ms). A truncated read is a transport blip, not a reparent, so the next GET almost always returns the full object.
4. Return a `bytes.NewReader`-backed `io.ReadCloser` that the existing `newChunkReader` / `newChangeChunkReader` consume unchanged (and which re-verify the SHA on `Close` — a cheap in-memory double-check).

Because the whole object is in hand and SHA-verified **before** the first row is emitted, the retry is safe (no partial emit to duplicate) and idempotent (chunks are content-addressed). A **persistent** mismatch — genuine at-rest corruption, which re-fetching identical bad bytes cannot fix — surfaces loudly as `ErrChunkHashMismatch` once the attempts are exhausted, preserving the loud-failure tenet.

Call sites switched to the helper: `streamChunkRows` (restore table data — the live failure), `streamOneChangeChunk` (chain-restore incremental segments), `streamOneChunkWithPosition` (broker incremental replay), and `verifyChunk` (so `sluice backup verify` no longer reports a false mismatch on a transient short read).

This subsumes, for these content-chunk reads, the previous streaming model. The encrypted path already `io.ReadAll`-buffered the whole chunk, so the buffered-read memory profile is unchanged there; the plaintext path now also buffers one chunk's raw (compressed) bytes per concurrent worker — bounded by the existing chunk-parallelism budget and small relative to a restore's footprint.

## Consequences

**Positive.** A transient object-store read no longer aborts a multi-hour restore; the restore (and the `sync from-backup` broker replay) tolerate flaky storage the way a backup tool must. `backup verify` gains the same resilience. Genuine corruption still fails loudly and exactly as before. One shared helper covers data + change chunks across restore, chain-restore, and broker.

**Negative / trade-offs.** Each plaintext content chunk is now buffered whole in memory before decode (compressed size, one per concurrent worker) rather than streamed — accepted, bounded by chunk-parallelism and already the behavior for encrypted backups. A persistent-corruption chunk now incurs the bounded backoff (≈1.4 s) before surfacing — negligible against a restore, and only on the genuinely-broken path.

## Testing

- **Unit (`chunk_fetch_retry_test.go`):** truncated-first-then-full read → retried, returns complete SHA-verified bytes (the live failure shape); happy path → exactly one GET (no added latency); persistent corruption → loud `ErrChunkHashMismatch` after all attempts (never silently accepted); transient `Get` open error → retried then succeeds; cancelled context → prompt `ctx.Err()`, not all attempts burned.
- **`-race` integration (push-first gate):** the existing restore / chain-restore / broker integration suites exercise the new helper under the concurrent chunk-worker pool; treated as a concurrency-class change, so the `-race` Integration job passes before the tag is cut.
- **Live re-validation:** the Track-C c4 cold-start re-run on the fixed binary rides the same object store to completion (the run that exposed the gap).
