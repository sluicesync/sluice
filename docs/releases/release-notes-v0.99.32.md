# sluice v0.99.32

**Silent-loss fix — restoring a PG-lineage backup chain to a `vitess`-flavor target on v0.99.15–v0.99.31 silently skipped every PG-native refusal check, so a PG schema carrying `EXCLUDE` constraints or extension opclasses restored with those constraints silently dropped instead of refusing loudly. If you ran that exact restore path, re-check that schema's constraints (see "Who needs this"); `planetscale`/`mysql` targets and PG→PG restores were never affected.** Also in this release: idle `backup stream` broker ticks drop from one store GET per manifest in the chain to O(1), bulk-copy decode and write now overlap with a zero-allocation COPY bridge, and the batched-INSERT `SHOW WARNINGS` probe is sampled (up to ~30 min saved on large cross-region PlanetScale loads). **Drop-in from v0.99.31 for the `mysql`, `planetscale`, and Postgres engines; the `vitess` flavor's apply defaults intentionally change (see Changed).**

## Fixed

- **PG → `vitess`-flavor chain restores no longer silently skip the PG-native refusal checks.** The `vitess` self-hosted flavor (shipped v0.99.15) was missing from the MySQL-family target check in the cross-engine restore gate, so the gate classified a `vitess` target as neither PG nor MySQL-family and skipped every PG-native unsupportability refusal — a PG-lineage backup chain carrying `EXCLUDE` constraints or extension opclasses restored with exit 0 and those constraints **silently dropped**, instead of refusing loudly. Found while converting the orchestrator's engine-name dispatch to capability dispatch (the exact new-engine-silently-misses-a-branch bug class that conversion exists to kill). **Affected releases: v0.99.15 through v0.99.31** — every published version carrying the `vitess` flavor. `planetscale` and `mysql` targets were always in the check; PG→PG restores take the same-engine path and were never affected. Now pinned by a restore-gate test covering the `vitess` flavor.

## Changed

- **The `vitess` flavor now inherits the PlanetScale-tuned apply defaults it was always meant to have** (it aliases `planetscale`'s capabilities — same vtgate semantics): conservative AIMD p95 target latency of 5s (was the generic 10s), the `apply-batch-size`>50 transaction-killer warning now fires, and schema-diff / ALTER-suggestion rendering uses MySQL dialect (was PG-style). Explicitly set flag values are honored unchanged — only the defaults move.

## Performance

- **Idle `backup stream` broker ticks are now O(1) store reads instead of one GET per manifest in the chain** (~2,000 GETs per 30s tick on a week-old 5-minute-rollover stream → exactly 2 — real S3 cost, and the tick could outlast its own interval). The walked chain is cached on the raw-byte identity of the lineage catalog plus the tail/open manifest; any structural change (rotation, compaction, prune, incremental append, tail checkpoint rewrite) invalidates, identity is read before the rebuild walk so a racing writer can only make the cache key older than the chain — never stale — and any mismatch or read hiccup falls back to the full walk including its loud refusals. Pinned by GET-count unit tests at chain lengths 5 and 50 plus invalidation tests for each structural change; legacy catalog-less stores never cache.
- **Bulk-copy decode and write now overlap, and the PG COPY bridge no longer allocates per row.** The cross-engine row pipeline was a chain of unbuffered channels, so source decode and target write strictly alternated (per-row cost decode+write instead of max(decode, write)); every stage now carries a bounded 64-row buffer — backpressure preserved, and the resume cursor is only persisted after the whole batch's idempotent write returns, so buffering can never advance a checkpoint past unwritten rows. Separately, the COPY bridge recomputed the non-generated-column filter and allocated a fresh values slice per row though both are invariant per table; both are hoisted, pinned at **0 allocs/op** by benchmark.
- **The per-flush `SHOW WARNINGS` probe on the batched-INSERT path is sampled** — first 10 flushes exhaustive (a systematic coercion clamp is caught on the first flush), then 1-in-16, final flush always — up to ~30 min of pure round-trips saved on a large cross-region PlanetScale load. Under default strict `sql_mode` conversion problems error the INSERT itself, so the probe is defense-in-depth on this path; the LOAD DATA writer (where strict mode genuinely downgrades errors to warnings) keeps its every-statement check, as does the idempotent resume path. Pinned by a sampling-cadence unit test; the seeded-warning integration pin lands inside the exhaustive phase.

## Internal

- Repo-audit M2 refactor arc, all behavior-identical: applier column-metadata shapes and builder signatures converged across engines plus byte-identical helpers extracted to `internal/appliershared` (groundwork for one-applier-fix-lands-once); orchestrator engine dispatch re-anchored to `ir.Capabilities` with per-engine compile-time optional-interface declarations (a method-set break now fails compile instead of silently downgrading to the fallback path); the three orchestrator mega-functions carved into named phase methods and `streamer.go` split by its seams (3,205 → 1,235 lines) with teardown ordering byte-identical.

## CI

- Weekly fresh-seed fuzz-roundtrip + `govulncheck` runs, weekly scheduling for the heavy suites (chaos, reshard, pipeline-vstream), and SHA-pinned write-privileged workflow actions. No product behavior change.

## Compatibility

- **No breaking changes.** Drop-in from v0.99.31 for the `mysql`, `planetscale`, `postgres`, and `postgres-trigger` engines — no flag, default, or invocation changes behavior there.
- **`vitess` flavor:** apply defaults change by design (AIMD 5s target, batch-size>50 WARN, MySQL-dialect diff rendering). Explicit flag values are unaffected; to keep the old AIMD target, set it explicitly.
- **PG → `vitess` restores that previously "succeeded" by silently dropping unsupportable constructs will now refuse loudly** — that is the refuse-loudly contract engaging, not a regression. Chains without PG-native unsupportable constructs restore exactly as before.
- **Performance changes are mechanically transparent:** the row-channel buffers are bounded (a stalled writer still blocks the reader) and resume checkpoints are unchanged; the broker cache falls back to the full walk on any doubt; LOAD DATA keeps its every-statement warnings check.
- **Unaffected:** sync/CDC paths, migrate schema/index/constraint phases, PG→PG and MySQL-family→anything restores, and all cross-engine value translation.

## Who needs this — action required

- **Anyone who restored a PG-lineage backup chain to a `vitess`-flavor target on v0.99.15–v0.99.31** — that restore may have silently dropped `EXCLUDE` constraints and extension-opclass-dependent objects from the schema. **Action: re-check that schema's constraints against the source** (or re-run the restore preflight on v0.99.32, which now refuses loudly if the chain carries PG-native constructs the MySQL family can't represent). If the source PG schema carried neither construct, nothing was dropped. `planetscale` and `mysql` targets, PG→PG restores, and all non-restore paths: nothing to do.
- **`vitess`-flavor `sync` operators** — apply defaults move with the upgrade (5s AIMD target, batch-size WARN, MySQL-dialect diffs). No action unless you relied on the old 10s default — set it explicitly if so.
- **Everyone else** — the broker, bulk-copy, and `SHOW WARNINGS` improvements engage automatically; no action, and no re-verification of prior *migrations* is needed (the silent-loss fix is confined to the cross-engine restore gate).

---

**Install:** `brew install sluicesync/tap/sluice`  ·  `go install sluicesync.dev/sluice/cmd/sluice@v0.99.32`  ·  **Container:** `ghcr.io/sluicesync/sluice:0.99.32`
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
