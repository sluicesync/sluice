# Index-build phase tuning (deferred-index speedup)

**Status:** proposal / design note. Spun out of a 2026-06-01 discussion about
speeding up bulk loads by deferring secondary indexes until after `COPY` and then
building them with a large `maintenance_work_mem` + parallel workers.

## Context: sluice already defers the indexes

The deferral that discussion proposes is **already foundational in sluice**, not a
feature to add. `internal/pipeline.Migrator`'s phases are explicit:

```
CreateTablesWithoutConstraints → bulk COPY → CreateIndexes → CreateConstraints
```

Every **secondary index and every constraint (FK, unique, check) is already built
after the COPY** (pgcopydb-derived — see the external-references list in CLAUDE.md).
On this axis sluice is already ahead of the Bucardo migrator's
`pg_dump --schema-only | psql`, which creates everything up front.

**The one deliberate exception is the primary key.** sluice emits the PK *inline*
in `CREATE TABLE` (`internal/engines/postgres/ddl_emit.go` ~L994; `emitCreateIndex`
explicitly refuses the PK because it's inline). That's a correctness trade, not an
oversight: the PK is the row identity `COPY` needs (dup protection) and the CDC
applier needs (apply UPDATE/DELETE by key) for the snapshot→CDC handoff. So sluice
pays the one unavoidable index cost during COPY (the PK) and defers the bulk of it.

## The remaining opportunity: the build phase is serial + untuned

`SchemaWriter.CreateIndexes` (`internal/engines/postgres/schema_writer.go`) loops
tables × indexes running `CREATE INDEX` **one at a time on a single connection,
with no GUC tuning** (sluice tunes only `synchronous_commit`, and only in the
applier). Three knobs are on the table:

1. **`maintenance_work_mem`** (large) — usually the single biggest index-build
   speedup (in-memory sort vs. small external-merge passes).
2. **`max_parallel_maintenance_workers`** — PG 11+ intra-index parallel build.
3. **Concurrent index builds** across indexes/tables — a bounded worker pool over
   the existing loop (the "parallel workers" part of the discussion).

Use plain `CREATE INDEX`, **not `CREATE INDEX CONCURRENTLY`** — during a migration
the table isn't taking live traffic, so the faster locking build is correct.

## Can these be dynamically adjusted? (confirmed on PlanetScale PG 18.4)

`pg_settings.context` is the authority on runtime-adjustability. Probed against the
validation PlanetScale node as the non-superuser `pscale_api` role:

| GUC | context | adjustable by sluice at runtime? |
|---|---|---|
| `maintenance_work_mem` | **user** | **YES** — `SET` per-session; confirmed `SET '512MB'` succeeded as non-superuser |
| `max_parallel_maintenance_workers` | **user** | **YES** — confirmed `SET 4` succeeded |
| `max_parallel_workers` | user | YES (read as a ceiling) |
| `max_worker_processes` | **postmaster** | NO (restart-only, provider-set) — readable **hard ceiling** on parallel workers |
| `shared_buffers` | postmaster | NO — readable RAM proxy |
| `effective_cache_size` | user | readable RAM proxy |

So the two knobs sluice wants are **`user`-context and settable per-session by the
managed non-superuser role — no UI change, no cluster restart**. The ceilings are
read-only but readable. (Observed values on that small node: `maintenance_work_mem`
default 16 MB, `max_worker_processes`=4, `shared_buffers`≈67 MB,
`effective_cache_size`≈203 MB — a small instance.)

## Autotuning: what Postgres exposes (and what it doesn't)

The hard part of autotuning: **Postgres exposes no direct host RAM or CPU via SQL**
— there is no `pg_num_cpus()` / `pg_total_memory()`. The usable proxies from
`pg_settings`:

- **Parallelism ceiling (reliable):** `max_worker_processes` (hard, restart-set)
  and `max_parallel_workers`. Whatever sluice `SET`s for
  `max_parallel_maintenance_workers`, the *effective* worker count is bounded by
  these (a shared pool). On the probed node both = 4, so parallel-maintenance is
  capped at ~4 regardless. This is a dependable, readable bound.
- **Memory proxy (soft):** `shared_buffers` (~25% RAM by convention) and
  `effective_cache_size` (~50–75% RAM). Imperfect — an operator *can* misconfigure
  them — but on managed providers they're auto-sized to the instance, so they're a
  usable signal for "is this a 256 MB node or a 64 GB node."

**Honest limitation:** absolute RAM/CPU can't be read robustly, so *aggressive*
full-auto is fragile. The safe shape is **conservative-auto derived from the
readable proxies, always overridable by an operator flag** — never an auto value
that could OOM a small managed node.

## Proposed design

- Flags: `--index-build-mem` (per-build `maintenance_work_mem`, 0 = auto) and an
  index-build parallelism control (reuse/extend a maintenance-workers flag, 0 = auto).
- Auto heuristic (pure function of the probe values, so it's unit-testable):
  - `parallel_maintenance = min(auto_or_requested, max_parallel_maintenance_workers, max_worker_processes − headroom)`.
  - `mem_proxy = min(effective_cache_size, k × shared_buffers)`; `index_mem_budget = fraction × mem_proxy`.
  - `maintenance_work_mem = clamp(index_mem_budget / concurrency, floor≈64MB, cap≈2GB)`.
  - `concurrent_builds = min(connection_budget_for_index_phase, mem_proxy / per_build_mem)`
    — bounded by **both** the connection budget (resilience item 4) **and** memory.
- Apply via `SET maintenance_work_mem` / `SET max_parallel_maintenance_workers` on
  the index-build session(s), **best-effort** (mirror the `synchronous_commit`
  SET-LOCAL precedent — don't fail the migration if a `SET` is denied; log the
  values actually applied alongside what the probes returned).

## The memory × concurrency trap (critical)

Total index-build memory ≈ `maintenance_work_mem × concurrent_builds ×
(~parallel workers)`. On the probed tiny node (`effective_cache_size`≈203 MB),
`maintenance_work_mem=512MB` × 4 concurrent builds ≈ 2 GB → OOM. So concurrency
must be bounded by the **memory proxy**, not just the connection budget. This is
exactly why the connection-/memory-budget seam from the connection-resilience work
(see [`orphaned-backend-resilience.md`](orphaned-backend-resilience.md) item 4) is
the natural home for the bound — the two features compose.

## Difficulty / phasing

The hard architectural part (the deferral seam, a dedicated `CreateIndexes` phase)
is **already built**, so this is low-difficulty:

- **Phase A (cheap, safe, high-leverage):** `SET maintenance_work_mem` +
  `max_parallel_maintenance_workers` on the existing *serial* `CreateIndexes`
  session, with the flag + conservative auto from `pg_settings` probes. No
  concurrency, no new failure modes.
- **Phase B:** concurrent index builds — a bounded worker pool over the existing
  loop, bounded by the connection + memory budget. Larger; depends on the budget
  work landing first.

Engine-neutral note: MySQL index builds have their own characteristics
(`ALGORITHM=INPLACE`, `innodb_sort_buffer_size`, …) — out of scope for v1. This is
PG-target-only, contained in the PG `SchemaWriter`, no orchestrator change.

## Test ideas

- **Unit:** the auto heuristic is a pure function of the probe struct → table-test
  (tiny node clamps low, large node scales up, operator override wins, ceilings
  respected, OOM-guard holds).
- **Integration:** assert `SET maintenance_work_mem` takes on the test PG and the
  index phase still produces correct indexes. Throughput isn't a CI assertion; a
  manual large-table benchmark (e.g. the Heroku dyno rig) validates the actual win.
