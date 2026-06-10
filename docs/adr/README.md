# Architecture Decision Records — Index

One-line summaries of every ADR in `docs/adr/`. Use this page to find the right ADR fast; click through for the full reasoning, consequences, and alternatives section.

ADRs are numbered in the order they were proposed. A few notable conventions:

- **Status** lines at the top of each ADR record whether the decision is Accepted, Superseded, or Discovery (research-only).
- Some ADRs were promoted from a design doc in `docs/dev/notes/` after extended dialogue; the dialogue artifacts stay in `notes/` for traceability.
- **ADR-0051 collision:** two ADRs share the number — `adr-0051-core-pg-type-verbatim-carry.md` (the canonical one referenced by the roadmap and `ir.VerbatimType`) and `adr-0051-pg-cdc-source-identity-pinning.md` (a sibling concern). Renumbering hasn't been done because both are widely cross-referenced; future ADRs continue from 0082.
- **ADR-0066 collision:** `adr-0066-postgres-trigger-engine-variant.md` is the actual ADR; `adr-0066-task-62-planning-brief.md` is a planning brief for the same chunk and not a true ADR.

## Foundations (0001–0009)

| ADR | Decision |
|---|---|
| [0001](adr-0001-ir-first-translation.md) | IR-first translation: a typed dialect-neutral schema + value IR is the only shared contract between engines |
| [0002](adr-0002-sealed-interfaces.md) | Sealed engine interfaces in `internal/ir`; engine packages register via `init()` |
| [0003](adr-0003-kong-koanf.md) | kong for CLI, koanf for YAML+env config |
| [0004](adr-0004-three-phase-apply.md) | `tables → bulk_copy → indexes → constraints` three-phase apply (later: + `identity_sync`, `views`) |
| [0005](adr-0005-mysql-flavors.md) | MySQL flavors (Vanilla, PlanetScale) share engine code but register under different names with different `Capabilities` |
| [0006](adr-0006-pgoutput.md) | pgoutput plugin (not wal2json) for PG CDC |
| [0007](adr-0007-position-persistence.md) | Per-target `sluice_cdc_state` control table; position commit in the same tx as data writes |
| [0008](adr-0008-go-mysql.md) | go-mysql-org/go-mysql for MySQL binlog (over alternatives) |
| [0009](adr-0009-streamer-vs-mode-flag.md) | Streamer surface choice: subcommand-shaped, not a mode flag on `migrate` |

## CDC + bulk-copy core (0010–0029)

| ADR | Decision |
|---|---|
| [0010](adr-0010-idempotent-applier.md) | Idempotent applier — INSERT…ON CONFLICT semantics across both engines |
| [0011](adr-0011-slot-manager-optional-surface.md) | Slot manager as an optional engine surface, not required for `migrate`-only paths |
| [0012](adr-0012-pglogrepl-bypass-for-failover.md) | pgxconn bypass of pglogrepl for PG 17 failover-survival flag setting |
| [0013](adr-0013-applier-value-shaping.md) | Applier value shaping — per-engine value-prep before the parameterized statement |
| [0014](adr-0014-phase-aware-error-hints.md) | Phase-aware error hints registry (`internal/pipeline/hints.go`) |
| [0015](adr-0015-migration-resume.md) | `sluice migrate --resume` via per-target `sluice_migrate_state` |
| [0016](adr-0016-layered-expression-translation.md) | Layered expression translator — 30 catalog rules + verbatim fallthrough |
| [0017](adr-0017-batched-cdc-apply.md) | Batched CDC apply with `--apply-batch-size`; default per-change (=1) per ADR-0017 |
| [0018](adr-0018-per-batch-bulk-copy-checkpointing.md) | Per-batch bulk-copy checkpointing for resumability |
| [0019](adr-0019-parallel-within-table-bulk-copy.md) | Parallel within-table bulk-copy with `--bulk-parallel-min-rows` threshold |
| [0020](adr-0020-slot-ack-after-apply.md) | Slot acknowledgment only after the apply has committed |
| [0021](adr-0021-publication-scope-by-table.md) | Publication scope by table list (not `FOR ALL TABLES`) |
| [0022](adr-0022-slot-missing-fall-through.md) | Slot-missing fall-through behavior — refuse, don't recreate silently |
| [0023](adr-0023-reset-target-data.md) | `--reset-target-data` semantics with typed confirmation |
| [0024](adr-0024-schema-preview.md) | `sluice schema preview` — show the DDL sluice would emit |
| [0025](adr-0025-graceful-drain-stop.md) | Graceful drain `sync stop --wait` via control-table polling |
| [0026](adr-0026-mysql-load-data-infile-writer.md) | MySQL LOAD DATA INFILE writer for bulk copy |
| [0027](adr-0027-source-transaction-boundary-cdc-batching.md) | CDC batching respects source-transaction boundaries (no mid-tx flush) |
| [0028](adr-0028-memory-bounded-streaming.md) | Memory-bounded streaming via channel-and-worker model |
| [0029](adr-0029-schema-diff.md) | `sluice schema diff` — diff a target against what sluice would produce |

## Live add-table + multi-source (0030–0036)

| ADR | Decision |
|---|---|
| [0030](adr-0030-mid-stream-live-add-table.md) | Mid-stream live add-table — `--no-drain` (PG path; Phase 2) |
| [0031](adr-0031-multi-source-aggregation-target-schema.md) | Multi-source aggregation via `--target-schema NAME` (PG-only) |
| [0032](adr-0032-pg-extension-passthrough.md) | PG extension passthrough catalog — 7-extension enumerated allowlist |
| [0033](adr-0033-mid-stream-live-add-strict-zero-loss.md) | Strict zero-loss correctness for live add-table (Bug 36 closure) |
| [0034](adr-0034-mysql-phase-2-live-add-table.md) | MySQL Phase 2 live add-table via streamer-side filter-flip |
| [0035](adr-0035-postgis-geometry-spatial-support.md) | PostGIS-aware geometry translation with SRID preserved cross-engine |
| [0036](adr-0036-mid-stream-loss-surface-characterization.md) | Mid-stream loss-surface characterization (post-Bug-36 audit) |

## Redaction + perf + bulk-copy (0037–0043)

| ADR | Decision |
|---|---|
| [0037](adr-0037-key-management.md) | Key management for redaction strategies (precursor to ADR-0041) |
| [0038](adr-0038-applier-retry-on-transient-errors.md) | Applier retry on transient errors (deadlock, lock wait timeout, etc.) |
| [0039](adr-0039-randomize-strategy-determinism.md) | `randomize:*` strategy determinism — per-row seed contract |
| [0040](adr-0040-dictionary-strategy-determinism.md) | Dictionary strategy determinism — PK-keyed vs input-value-keyed |
| [0041](adr-0041-operator-keyset-persistence.md) | Operator keyset persistence (`--keyset-source=file/env/db`) |
| [0042](adr-0042-mysql-bulk-copy-throughput-investigation.md) | MySQL bulk-copy throughput investigation (Discovery) |
| [0043](adr-0043-native-bulk-loader-on-parallel-copy-path.md) | Native fast loader on cold-start parallel-copy path |

## Translation policy + backup chain (0044–0050)

| ADR | Decision |
|---|---|
| [0044](adr-0044-extension-function-defaults-tier3.md) | Tier 3 extension function-defaults (uuid_generate_v4 → UUID(), pgcrypto SHA family) |
| [0045](adr-0045-expression-identifier-translation-consolidation.md) | Consolidate expression-identifier translation (Bug 64 closure) |
| [0046](adr-0046-inline-backup-chain-rotation.md) | Inline backup chain rotation — `--retain-rotate-at*` flags |
| [0047](adr-0047-verbatim-extension-passthrough.md) | Verbatim extension passthrough for uncatalogued PG extensions (`ir.VerbatimType`) |
| [0048](adr-0048-multi-source-aggregation-shape-a.md) | Multi-source aggregation Shape A (sharded → consolidated; ADR-0048-class) |
| [0049](adr-0049-cdc-schema-history.md) | CDC schema-history table for resume-across-DDL correctness |
| [0050](adr-0050-reconciling-resnapshot.md) | Reconciling re-snapshot — drift detection + targeted re-snapshot |

## Verbatim carry, source-identity, AIMD, EXCLUDE (0051–0059)

| ADR | Decision |
|---|---|
| [0051a](adr-0051-core-pg-type-verbatim-carry.md) | Core-PG-type verbatim carry beyond tsvector/tsquery (range/multirange/Stage 2 family) |
| [0051b](adr-0051-pg-cdc-source-identity-pinning.md) | PG CDC source-identity pinning (refuse to resume against a different source after failover) |
| [0052](adr-0052-aimd-apply-batch-size-controller.md) | AIMD apply-batch-size controller for batched CDC |
| [0053](adr-0053-exclude-constraint-verbatim-carry.md) | EXCLUDE constraint verbatim carry (same-engine PG → PG) |
| [0054](adr-0054-shape-a-phase-2-live-cross-shard-ddl-coordination.md) | Shape A Phase 2 live cross-shard DDL coordination |
| [0055](adr-0055-pgoutput-streaming-protocol-audit.md) | pgoutput streaming protocol audit (Discovery) |
| [0056](adr-0056-sluice-diagnose-operator-bundle.md) | `sluice diagnose` — operator capability + state bundle |
| [0057](adr-0057-hard-delete-semantics-across-engines.md) | Hard-delete semantics across engines |
| [0058](adr-0058-online-schema-change-forwarding.md) | Online schema change forwarding (`ADD COLUMN` auto-forwarding) |
| [0059](adr-0059-pg-slot-health-prewarning.md) | PG slot-health pre-warning (70%/85% retention + 30min inactivity) |

## Schema drift, heartbeat, cutover, RLS (0060–0066)

| ADR | Decision |
|---|---|
| [0060](adr-0060-cdc-schema-drift-diff.md) | CDC schema-drift diff in refuse-loudly messages |
| [0061](adr-0061-source-side-heartbeat-writer.md) | Source-side heartbeat writer to keep slots alive against quiet sources |
| [0062](adr-0062-cutover-sequence-priming.md) | Cutover sequence priming (`sluice cutover` subcommand) |
| [0063](adr-0063-pg-rls-ir-capture-and-emit.md) | PG RLS IR capture + emit; loud refusal on cross-engine PG → MySQL |
| [0064](adr-0064-backup-smart-compaction.md) | Backup smart-compaction (preserves restorability while reducing segment count) |
| [0065](adr-0065-shape-a-catalog-check-constraints.md) | Shape A catalog: CHECK constraints (PG → MySQL inline-CHECK precursor) |
| [0066a](adr-0066-postgres-trigger-engine-variant.md) | `postgres-trigger` engine variant for slot-less managed-PG sources |
| [0066b](adr-0066-task-62-planning-brief.md) | Task 62 planning brief (not a canonical ADR; companion to ADR-0066a) |

## Born-contiguous rotation, capture-payload, ready, Stage 2 (0067–0070)

| ADR | Decision |
|---|---|
| [0067](adr-0067-contiguous-rotation-handoff.md) | Born-contiguous rotation handoff — compactable rotated chains (Bug 95 closure) |
| [0068](adr-0068-changed-columns-capture-payload.md) | `sluice trigger setup --capture-payload=full/changed/minimal` — lighter capture payload mode |
| [0069](adr-0069-service-mode-readyz.md) | Service-mode `/readyz` endpoint for k8s/load-balancer probes |
| [0070](adr-0070-stage-2-verbatim-carry-promote.md) | Promote Stage 2 core-PG verbatim carry (xml/money/pg_lsn/txid_snapshot/pg_snapshot) per ADR-0051a evidence |

## VStream resilience + multi-database (0071–0074)

| ADR | Decision |
|---|---|
| [0071](adr-0071-vstream-snapshot-bounded-memory.md) | Bounded-memory VStream cold-start COPY (byte-capped backpressured pump, `--max-buffer-bytes`) |
| [0072](adr-0072-resumable-coldstart-copy.md) | Resumable VStream cold-start COPY (carry Vitess `TablePKs` cursor, checkpoint during COPY, in-place reconnect) |
| [0073](adr-0073-vitess-internal-and-online-ddl-tables.md) | Exclude Vitess internal / online-DDL `_vt_*` tables from COPY+CDC; survive an online-DDL cutover zero-loss |
| [0074](adr-0074-multi-database-mysql-migration-and-sync.md) | Accepted (shipped v0.99.16) — multi-database MySQL migrate + sync (one server → N databases → N target namespaces; server-wide binlog CDC with per-event apply-routing) |
| [0075](adr-0075-postgres-source-multi-schema-migration-and-sync.md) | Accepted (shipped v0.99.24) — Postgres-source multi-schema migrate + sync (one PG database → N schemas → N target namespaces; one database-wide logical slot, per-event apply-routing — the symmetric reverse of ADR-0074) |
| [0076](adr-0076-cross-table-copy-worker-pool.md) | Accepted (shipped) — cross-table copy worker pool for `sluice migrate` (`--table-parallelism`; bounded errgroup over tables composed with within-table `--bulk-parallelism`; combined budget split at the single chokepoint so the product can't exhaust the target's slots; resume-under-concurrency discipline; sync path stays serial). Roadmap item 3(a) |

## Index-build overlap (0077)

| ADR | Decision |
|---|---|
| [0077](adr-0077-overlap-index-builds-with-bulk-copy.md) | Accepted — overlap secondary-index builds with the bulk copy in `sluice migrate` (PG `ir.IncrementalIndexBuilder`: build each table's indexes as its copy lands, concurrently with still-copying tables, under one errgroup; combined copy+index connection budget split at the single chokepoint so simultaneously-open copy+index connections can't exhaust the target's slots; `IndexesBuilt` resume flag; MySQL falls back to the post-copy whole-schema index phase). Roadmap item 3b(a) |

## Identity passthrough — raw COPY byte-pipe (0078)

| ADR | Decision |
|---|---|
| [0078](adr-0078-pg-pg-identity-passthrough-raw-copy.md) | Accepted — PG→PG identity passthrough: byte-pipe the raw COPY stream (`COPY (SELECT …) TO STDOUT` → `COPY tbl (…) FROM STDIN` via pgx `pgconn`) to close the per-stream copy-rate gap vs pgcopydb, bypassing the typed IR. Engine-neutral optional surfaces (`ir.RawCopyExporter`/`RawCopyImporter`/`RawCopyVersionProber`/`RawCopyChunk`/`RawCopyFormat`); engages ONLY behind a single auditable value-fidelity gate (`rawCopyGate`: same-engine + no redaction/type-override/expr-override/shard-injection + per-table identity projection excluding extension/verbatim/bit/geometry) — any transform present falls back to the IR path. Slotted in the ADR-0043 cold-start fast-loader branch (`migrate`-only; resume + sync stay on the IR path); text COPY default, binary opt-in on matched server majors. Roadmap item 3b(b) |

## Fast cold-start for the sync path (0079)

| ADR | Decision |
|---|---|
| [0079](adr-0079-fast-cold-start-for-sync-path.md) | Accepted (design) — bring the migrate cold-start speedups (cross-table pool ADR-0076 + index-overlap ADR-0077 + raw passthrough ADR-0078) to `sluice sync start`'s cold-start, so copy+follow gets the fast copy (the one-command pgcopydb-`--follow`-at-full-speed equivalent). Shape (A), PG-source-first, behind a source-capability gate: qualifies only when the `ir.SnapshotStream` carries a shareable exported-snapshot name AND the source implements `ir.SnapshotImporterOpener` (wires the latent, never-called `SnapshotImporter` `SET TRANSACTION SNAPSHOT` surface to pin all N parallel readers to ONE consistent snapshot). MySQL + VStream/PlanetScale stay serial (loud INFO), deferred — the durable-watermark + idempotent-COPY coupling lives only on the VStream path, which never coexists with the raw byte-pipe. `rawCopyGate` refactored off `*Migrator` onto a shared transform-config struct so the value-fidelity guarantee is identical on both paths. CDC connection slot reserved in the budget; resume stays serial. **v1 shipped v0.99.29. v1.1 (within-table chunking on the fast path) shipped v0.99.30** — the first v1.1 attempt (relax pinned-reader `CountRows`) regressed and was reverted because the during-copy ETA probe then raced the row-stream on the pinned conn; the as-built CORRECTION keeps `CountRows` returning 0 for pinned readers and adds a separate `ir.RowCountEstimator` that reads reltuples off a FRESH non-pinned conn for the pre-stream chunk decision (ADR v1.1-CORRECTION addendum). Roadmap items 3d + 3d v1.1. |

## MySQL index-build overlap (0080)

| ADR | Decision |
|---|---|
| [0080](adr-0080-mysql-index-build-overlap.md) | Accepted (design) — extend ADR-0077 index-build overlap to MySQL targets: the MySQL `SchemaWriter` implements `ir.IncrementalIndexBuilder`/`TableIndexedNotifier` (mirrors the PG file), so every MySQL-target migrate (MySQL→MySQL, PG→MySQL) collapses the separate post-copy index phase into the copy. Orchestrator unchanged (gated on the surface). MySQL has no connection-slot prober, so the index pool self-sizes (fixed N=4, clamped [1,8], bounded by job count + `--max-target-connections`) rather than from `splitCopyAndIndexBudget` (always 0 for MySQL). PlanetScale/Vitess targets decline the overlap (internal drain-and-defer to the platform's online-DDL via the post-copy `CreateIndexes`). One index per job (parallel), idempotency via the existing `indexExists` probe (no portable `ADD INDEX IF NOT EXISTS`). No published throughput number until an at-scale MySQL measurement (PG's overlap was −60 s on disk-bound storage). Roadmap item 3c. |

## Applier control-plane extraction (0081)

| ADR | Decision |
|---|---|
| [0081](adr-0081-applier-control-plane-extraction.md) | Accepted — repo-audit M2.2: extract the engines' mirrored applier control plane into `internal/appliershared` behind a flat config-of-closures dialect seam (`BatchConfig`, exprident.Config precedent). The audit measured the two `change_applier_batch.go` files ≥85 % identical with 16/19 commits forced to touch both (the item-18 latency fix landed twice). Tier (b) — the batched-apply loop (accumulation, AIMD consult/observe, idle-grace timer, byte cap, ADR-0007/0010 position-write-then-commit ordering) — now lives ONCE in `RunBatchLoop`/`RunOneBatch`; engines fill the divergent leaves (engine name, `TransactionalDDL` flag for MySQL's implicit-commit DDL vs PG's in-tx schema events, F7 BeginTx, slot-ack AfterCommit, cache-after-commit hook). SQL builders, value codecs, classifiers stay engine-side. All item-18 timing pins + idempotency pins passed unchanged. Tiers: (a) helpers PR #170 + (b) batch loop DONE; (c) control-table CRUD + (d) lease/keyset/heartbeat OPEN. |

## Notes / dialogue prep / readiness briefs

Some ADRs were drafted from dialogue artifacts in `docs/dev/notes/`. Notable companions:

- [`docs/dev/notes/adr-0048-dialogue-prep.md`](../dev/notes/adr-0048-dialogue-prep.md)
- [`docs/dev/notes/adr-0049-implementation-readiness.md`](../dev/notes/adr-0049-implementation-readiness.md)

These aren't ADRs themselves — they're the source material the ADR was promoted from. Treat them as historical context.

## How to read an ADR

The shape is: Context → Decision → Consequences → Alternatives considered. Each ADR is meant to be self-contained — you should be able to read it without reading the previous ones. The cross-references (`[ADR-XXXX]`) are bidirectional safety nets: forward-direction "this builds on X" and reverse-direction "X was relevant when this was written."

If a decision was reversed or superseded, the original ADR isn't deleted — it's marked superseded with a pointer at the top, and the new ADR explains the change. Reading the chain in number order is also a (slow) way to trace sluice's design evolution.

## When to write a new ADR

Read [CLAUDE.md](../../CLAUDE.md) § Tenets first. The current rule of thumb: write an ADR when a design choice has **non-obvious consequences a reader two years from now would benefit from knowing about.** Refactors and bug fixes generally don't warrant an ADR; they're documented by the change itself + the closure note. Adding an engine, changing a tenet, introducing a new IR shape, or making a backwards-incompatible API choice all do.
