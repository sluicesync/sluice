# sluice v0.99.111

**Postgres-target cold-copy now rides a PlanetScale storage auto-grow instead of dying on it (roadmap item 38 — the Postgres analog of the MySQL grow-window arc).** This is the fix that lets a full cross-engine MySQL→PlanetScale-Postgres cold-copy complete through the storage auto-grows that previously killed it.

## Added

Found live by the #94 MySQL→PlanetScale-Postgres test: the bulk-copy `COPY` into a non-Metal PlanetScale Postgres target died fatally with `SQLSTATE 53100 (could not extend file / No space left on device)` ~4 GB in. The PG volume didn't grow ahead of the streaming `COPY`, and — unlike the MySQL target path (items 30/33/37) — the Postgres write path had no storage-grow resilience: a single monolithic `COPY` per table with no retry, no pause, no resume point.

This release brings the ADR-0110 coordinated grow-gate to the Postgres `COPY` path. When a grow-gate is attached (it's constructed for every cold-copy run — the ADR-0110 universal signal-driven floor, so any auto-grow target benefits, not just PlanetScale), the PG cold-copy writes each table in **bounded chunks**, each chunk a single atomic `COPY`, wrapped in a reparent-retry loop mirroring the MySQL `flushWithReparentRetry`:

- classifies the transient grow faces as retriable — `53100` (disk-full / could-not-extend) plus the `57P0x` admin-shutdown and `08x` connection faces of a serving transition (the new class-53 arm is shared with the CDC apply classifier);
- re-acquires a fresh connection on each retry (the prior one may be pinned to a reparented primary);
- awaits the coordinated grow-gate pause so all lanes quiesce together while the volume grows, and trips the gate to quiesce siblings on a surviving transient;
- is bounded (~30 min wall-clock) and **loud on genuine exhaustion** — a truly-dead target still fails the run.

A rolled-back chunk wrote nothing, so replaying it into the append-only fresh table neither dups nor drops.

**Value-fidelity.** The chunked path reuses the *exact same* per-row `COPY` encoder as the monolithic path — the chunk boundary is a pure row-count split, no value straddles a chunk. This is pinned by a byte-identical integration test that copies a multi-family fixture **both ways** (monolithic no-gate vs chunked gate) and asserts an identical md5 over PG's canonical `::text` rendering — the fixture covers the Bug-74 `numeric[][]` multi-dimensional array, arrays with NULL elements, uuid / inet / cidr / macaddr, date / time / timetz (which exercises the per-connection codec registration), bit / varbit, bytea (with NUL + high bytes), json / jsonb, numeric(38) extremes, timestamps, bool, and scattered NULLs. Two more pins: retry-convergence (a fault injected mid-chunk converges with no dup/drop) and terminal-error loudness (a non-retriable mid-chunk error surfaces — never a silent partial table).

## Compatibility

No configuration changes. The grow-gate seam was already wired for MySQL cold-copy; the Postgres `RowWriter` now implements it (`ir.GrowGateSetter`). A Postgres cold-copy now writes in chunks rather than one streaming `COPY` per table; the per-value encoding is byte-identical and the gate stays inert (the chunks just stream sequentially) until a classified grow-transient or the coordinated pause engages. Non-cold-copy paths and the exactly-once CDC apply contract are unchanged.

## Who needs this

Anyone running a `sluice sync` or `migrate` **into a PlanetScale Postgres** (or any auto-growing-storage Postgres) target with a dataset large enough to cross a storage auto-grow during the initial cold-copy. Previously such a copy could die with `could not extend file`; it now rides the grow to completion, the same way MySQL targets have since v0.99.92–v0.99.100. Automatic; no action required.

## Install

```
go install sluicesync.dev/sluice/cmd/sluice@v0.99.111
```

Container image:

```
docker pull ghcr.io/sluicesync/sluice:0.99.111
```
