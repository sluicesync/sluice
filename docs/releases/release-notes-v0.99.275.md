# sluice v0.99.275

The tail of the 2026-07-17 confirming audit: a Postgres control-read resilience touch-up, plus the test/coverage and CI closures that finish the audit program. There is **no change to any successful path**.

## Fixed

- **Postgres control reads (`ReadPosition` / `ListStreams`) now classify a transient connection error the same way the apply path already does.** The concurrent apply lanes run in `QueryExecModeDescribeExec` (no client statement cache) and route every error through the transient classifier, so a momentary connection blip on the hot path is already retriable. The control reads — the startup resume-position read and the `sync status` list — ride the default cached-statement pool, and returned their error raw. So a degraded pooled connection surfacing pgx's cached-statement cleanup timing out (`failed to deallocate cached statement(s): i/o timeout`) on a startup or status read propagated as a hard fault instead of a retriable transient. They now route through the same classifier, so that class carries the retriable signal and the supervisor backs off and reconnects on a fresh pooled connection rather than treating a momentary blip as a hard start-up fault. Data integrity was never at stake — the read is a control-table `SELECT` and the write/rollback path is untouched — and non-transient errors still pass through verbatim.

## Changed

- **Test & coverage hardening (2026-07-17 confirming-audit tail — no user-facing behavior change):**
  - The item-74 **NOBLOB partial-row-image belt** (v0.99.273) was **validated end-to-end against a real self-hosted `binlog_row_image=NOBLOB` Vitess cluster** — it fires the loud `SLUICE-E-CDC-ROW-IMAGE-PARTIAL` refusal on **both** CDC dispatch paths (warm-resume and cold-start), naming the omitted column, and does **not** over-fire on full-image rows (INSERTs, blob-changing UPDATEs, and the full-image snapshot copy all flow). The partial after-image was independently ground-truthed at the binlog level before the sluice run.
  - A broker cold-start `--reset-target-data` restore **under a signed backup chain** is now pinned end-to-end. The signature verification and tamper-refusal were already correct on that leg — this closes a **coverage hole, not a code gap**.
  - Three long-red **extended-suites** CI legs were diagnosed as non-product and fixed: a relocated upstream example-schema URL (the corpus fetch 404'd after `vitessio/vitess` moved `examples/local` → `examples/common`); a cold-image-pull timing flake in the Vitess rolling-upgrade chaos test (both hardcoded upgrade tags are now warmed into the local cache off the tablet-ping path, and the recreate ping budget is widened); and stale reshard test scaffolding that asserted shard discovery before the stream opened, left over from the connection-free-reader refactor (the assertions now run after `StreamChanges` populates the shards).

## Compatibility

- **No behavior change.** The Postgres change only affects how a *transient* control-read connection error is retried; a successful read is byte-identical. Everything else is added test coverage and CI hardening.

## Who needs this

Postgres CDC-sync operators on flaky networks get a marginally more resilient stream start-up and `sync status` — a momentary connection blip during the resume-position read now backs off and reconnects instead of surfacing as a hard fault. For everyone else this is coverage and CI hardening with no action required.

## Install

- Binaries: https://github.com/sluicesync/sluice/releases/tag/v0.99.275
- Homebrew: `brew install sluicesync/tap/sluice`
- Scoop: `scoop bucket add sluicesync https://github.com/sluicesync/scoop-bucket; scoop install sluice`
- winget: `winget install sluicesync.sluice`
- Docker: `docker pull ghcr.io/sluicesync/sluice:v0.99.275`
