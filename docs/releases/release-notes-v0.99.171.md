# sluice v0.99.171

**A transient connection drop while OPENING a cold-copy chunk's connections now reconnects and retries instead of aborting the whole `migrate` — closing the last connection-drop hole in the cold-copy retry fabric (ADR-0146), found when a live 49 GB copy lost ~45 GB of progress to a single blip.**

## Fixed

**Transient connection-drop at chunk-open is now retriable (ADR-0146).** The parallel bulk copy opens a fresh source-reader + target-writer pair per chunk via `acquireChunkConn`. That open retried ONLY the connection-slot-exhaustion class (SQLSTATE 53300); every other open error — including a transient `ping: invalid connection` (a stale pooled connection after a WAN hiccup or a PlanetScale storage-grow reparent) — hit the fail-fast branch and aborted the entire migrate. A live 49 GB Postgres→PlanetScale-MySQL run lost ~45 GB of already-copied progress to a single such blip; the only recovery was `--resume`, which re-drops and re-copies the table.

Now a classified transient connection-drop at chunk-open — `driver.ErrBadConn`, `io.EOF`, a timed-out `net.Error`, or the driver-and-OS connection-drop text shapes (`invalid connection`, `connection reset`, `broken pipe`, the Windows `wsarecv … did not properly respond`) — reconnects and retries the open within a bounded ~30-minute wall-clock + exponential-backoff envelope, keeping the chunk's gate-token budget slot. It is **dup-free by construction**: the open fails at ping, BEFORE any COPY/WriteRows runs, so re-running the chunk from its recorded `chunk.LastPK` cursor (`WHERE (pk) > LastPK`) reuses the existing ADR-0109 keyset-resume substrate with zero double-copy risk — the same safety argument the 53300 path already documents.

This closes the one remaining hole in the cold-copy reconnect fabric: the mid-write path (ADR-0108 `flushWithReparentRetry`) and the source-read path (ADR-0109) already reconnect on `driver.ErrBadConn` / `ErrInvalidConn` / errno-2013, but a drop caught at the *moment of opening* a chunk's connections had no such retry. Permanent faults — auth, bad DSN, unknown database, permission denied — stay fail-fast and loud; unknown shapes stay fatal (conservative, no default-to-transient). Pinned by `TestIsRetriableChunkOpenError_Classification` (the full transient × permanent × unknown matrix) and `TestOpenChunkConnWithRetry_*` (bounded retry with no infinite loop, immediate-fatal on a permanent error, ctx-cancel breaks the backoff).

## Compatibility

No behavior change to any happy path or to any other error class. This converts one specific **loud abort** (`rc=1` when a transient blip dropped a chunk's connection at open) into a **bounded reconnect** that continues the copy. It stays loud-safe either way: there was never silent loss (the pre-fix path aborted loudly), and the retry is bounded by the same ~30-minute wall-clock envelope as the sibling reconnect paths — an endpoint that genuinely never recovers still fails loudly after it.

## Who needs this

Operators running large `sluice migrate` copies across a WAN or into a **PlanetScale-MySQL** target, where a transient connection blip (a stale pooled connection, a brief network hiccup, or a storage-grow reparent) can land exactly when a chunk opens its connections. The larger the copy, the more copy progress a single such abort throws away — this is what turns that into a reconnect-and-continue. Everyone else is unaffected.

---

**Install:** brew install sluicesync/tap/sluice · go install sluicesync.dev/sluice/cmd/sluice@v0.99.171 · **Container:** ghcr.io/sluicesync/sluice:0.99.171
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
