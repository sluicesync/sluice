# sluice v0.99.9

**A CRITICAL silent-loss fix for the resumable PlanetScale cold-start.** Drop-in upgrade from v0.99.8; strongly recommended for anyone relying on cold-start resume. Found by post-release validation against a real PlanetScale production branch.

## Fixed

- **CRITICAL: resuming a hard-crashed PlanetScale (VStream) cold-start no longer silently drops rows.** The resumable-COPY checkpoint (ADR-0072) persisted the cursor for rows **received from vtgate and buffered**, not rows **durably written to the target** — and the target writer lags the receive path by up to `--max-buffer-bytes`. So after a **hard crash** (OOM-kill, SIGKILL, container/node kill, power loss) of an in-flight cold-start, the persisted checkpoint sat *ahead* of the durably-written data; resume restarted at the cursor and **silently skipped the gap** (up to a full buffer of rows), finishing "bulk copy complete" with no error.

  Measured on a real 19M-row branch with a 2 GB buffer: the checkpoint sat **~5.1M rows ahead** of the durable frontier, and an end-to-end resume left the target missing **~5.26M rows (~27.6%)** — silently.

  The checkpoint now tracks a **durable-write watermark**: the COPY pump records a position breadcrumb at each VGTID boundary, the row writer reports per-flush durable deltas after each successful commit, and the checkpoint persists only the highest breadcrumb whose covered rows are all durably written — **invariant: persisted position ≤ durable frontier**. A crash + resume now restarts at-or-before the last durable row and the idempotent COPY writer absorbs the bounded re-copied overlap (zero loss). The fix covers the Postgres target path too (PK VStream→PG resume had the same exposure). Pinned by a structural unit test (checkpoint never ahead of the durable frontier across interleaved receive/commit steps) and a hard-crash-then-resume zero-loss integration test.

## Compatibility

- No breaking API or CLI changes. Drop-in from v0.99.8.
- Graceful stop/restart and steady-state CDC were never affected. This hardens the **hard-crash-then-resume** path of an in-flight cold-start. Pre-existing since v0.99.5; activated by v0.99.8 (which made resume actually run). It corrects v0.99.5's "resumes from the last-copied PK, zero loss" claim, which held only for the in-place reconnect, not a full process restart.

## Who needs this

- **Anyone relying on resuming an interrupted PlanetScale cold-start** — especially in environments where the process can be hard-killed (Kubernetes OOM-kills, spot-instance reclaims, container restarts). Before this fix, such a resume could silently complete with missing rows.

---

**Install:** `brew install sluicesync/tap/sluice`  ·  `go install sluicesync.dev/sluice/cmd/sluice@v0.99.9`  ·  **Container:** `ghcr.io/sluicesync/sluice:0.99.9`
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
