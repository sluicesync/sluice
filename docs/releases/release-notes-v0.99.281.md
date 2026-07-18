# sluice v0.99.281

**Final wave of the 2026-07-18 audit remediation (batch B)** — a filtered-sync throughput fix and an internal collation-layering cleanup. Fully additive: nothing that doesn't use `--where` changes. This completes the audit; the remaining items are its explicit not-fix calls.

## Fixed / Changed

### Filtered `sync --where` on PlanetScale MySQL / Vitess now filters server-side on warm resume

A filtered VStream sync pushes the `--where` predicate into the VStream filter rule so the source transfers only in-scope rows. That push-down applied to the cold-start copy — but *not* to a **warm resume**: after any restart or crash-resume, the sync streamed the **entire keyspace** and discarded ~99% of it client-side. Correctness was never at risk (the client-side filter still ran), but the steady-state cost after every resume was the full unfiltered keyspace stream.

The predicate is now pushed into the VStream filter rule on resume as well — `select * from <t> where (<predicate>)` per filtered table, with unfiltered tables still streamed via the catch-all — so a resumed filtered stream transfers only in-scope change traffic. The client-side row-move classification (move-in → INSERT, move-out → DELETE) is unchanged; the server-side filter is purely an efficiency layer. Validated end-to-end on a real Vitess cluster: on resume from a persisted position, an out-of-scope INSERT is dropped **server-side** (never reaches the reader), while the in-scope INSERT and the move-out UPDATE still arrive with both images for the target to apply.

### Internal: the row-predicate evaluator no longer depends on a MySQL collation library

The client-side `--where` evaluator lives in the engine-neutral orchestrator layer, but it had grown a direct compile-time edge to Vitess's collation library — a leak of engine-specific machinery into the IR-first core. Collation resolution now goes through an engine-supplied `CollationResolver` interface: MySQL provides the Vitess-backed comparator (with the PAD-space and charset and case-insensitive rules added in v0.99.279–280), and Postgres/SQLite provide a byte-exact-or-refuse resolver (with the `collisdeterministic` check from v0.99.280). The engine-neutral evaluator no longer imports an engine-specific collation library.

This is behavior-preserving by contract: the real-MySQL and real-Postgres collation family matrices (which assert sluice's client-side classification against each server's own `WHERE`) pass **unchanged** after the refactor.

## Compatibility

**No behavior change.** The warm-resume fix is a throughput improvement (fewer discarded events on resume) — filter semantics are identical. The collation-resolver refactor is internal.

## Who needs this

Operators running a long-lived filtered `sync --where` against **PlanetScale MySQL / Vitess** that restarts or crash-resumes — the resumed stream is now server-side-filtered instead of pulling the whole keyspace. Everyone else can upgrade at leisure.

## Install

- Binaries: https://github.com/sluicesync/sluice/releases/tag/v0.99.281
- Homebrew: `brew install sluicesync/tap/sluice`
- Scoop: `scoop bucket add sluicesync https://github.com/sluicesync/scoop-bucket; scoop install sluice`
- winget: `winget install sluicesync.sluice`
- Docker: `docker pull ghcr.io/sluicesync/sluice:v0.99.281`
