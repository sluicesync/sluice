# sluice v0.99.36

**CRITICAL fix-forward for Bug 135 (a v0.99.35 regression, caught by the post-release battle-test within hours): resuming an interrupted backup no longer silently corrupts the artifact. If you run v0.99.35, upgrade — and if you ever resumed an interrupted backup ON v0.99.35, discard that artifact and re-run it fresh.**

## Fixed

- **Resuming an interrupted `backup full` produced duplicate AND missing rows while exiting 0 (Bug 135, CRITICAL).** The per-chunk resume reuse kept a prior partial run's chunk N verbatim and skipped that many rows of the NEW row stream — which assumed both runs deliver rows in identical order. Full-table reads carry no `ORDER BY` by design (an ordered full-table read guts throughput), so scan order was only ever repeatable by accident; v0.99.34's serial single-connection sweep preserved the accident, and v0.99.35's parallel sweep broke it reliably. The corruption was detectable only when a restore tripped a duplicate-key error on a PK'd table — a table without unique constraints restored silently wrong, and the missing rows were unrecoverable from the artifact. The same path poisoned the `--chain-slot` crash-retry flow.

  **The fix retires order-dependent chunk reuse entirely.** Resume is now table-granular: fully-completed tables are still kept verbatim (whole chunk sets are order-independent), partially-written tables re-stream from scratch — bounded by the crash contract, at most `--table-parallelism` tables were in flight — and byte-identical re-produced chunks still skip their upload via a content-addressed comparison. Pinned by a revert-tested order-divergence test that reproduces the exact corruption shape on pre-fix code.

## Compatibility

- Resume of an in-progress backup re-streams partially-written tables instead of reusing their chunks; expect at most `--table-parallelism` tables' worth of redo after a crash. No format or flag changes.
- Backups taken (and never resumed) on v0.99.35 are correct. Only artifacts produced by RESUMING an interrupted backup on v0.99.35 are suspect — discard and re-run those.

## Who needs this

- **Everyone on v0.99.35** — this is the fix-forward for its known-issue banner.
- Anyone relying on `--resume` for interrupted backups or the `--chain-slot` crash-retry flow.

## Install

Binaries for Linux/macOS/Windows (x86_64 + arm64) are attached below; container image at `ghcr.io/sluicesync/sluice:0.99.36`. Verify downloads against `checksums.txt`.
