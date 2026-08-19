# ADR-0184: Per-index `ALTER` splitting for large index builds on a statement-time-limited target

- **Status:** Accepted — experiment validated 2026-08-18; implementation landing v0.129.0 (`migrate` + `sync` cold-start index phase). The PlanetScale leg (does the split help on the real ~900 s wall, and does Vitess even allow the alternative partition path) is a follow-up experiment, tracked below.
- **Date:** 2026-08-18
- **Related:** [ADR-0080](adr-0080-mysql-index-build-overlap.md) (the **combined-`ALTER`** this conditionally splits — its single-table-scan win is why the default stays combined); [ADR-0148](adr-0148-planetscale-deploy-request-index-build.md) (the deploy-request index build, the safe-migrations-ON escape this complements); [ADR-0182](adr-0182-raise-planetscale-query-timeout.md) (raising the ~900 s wall to 3600 s — the same errno-3024 wall, a different lever); roadmap item 109 / `SLUICE-E-CONSTRAINT-STATEMENT-TIME-LIMIT` (the FK half of the wall family).

## Context

PlanetScale enforces a per-statement time limit (`queryserver-config-query-timeout`, default ~900 s, errno 3024). sluice defers secondary-index creation to a post-copy phase and, per ADR-0080, emits the **minimum set of `ALTER` statements** — all of a table's combinable indexes collapse into ONE `ALTER TABLE t ADD INDEX …, ADD UNIQUE INDEX …, …` so InnoDB scans the table once instead of once per index. That is a real win on ordinary tables.

On a **large** table it backfires. A real user's PlanetScale us-east-1 → us-east-2 region move (30 GB, safe-migrations OFF so no deploy-request escape) failed on exactly this statement, on the 12.8 GB `audio_plays_daily` table:

```
alter table audio_plays_daily
  add unique key ..._unique (date, seller_id, pack_id, sample_id) USING BTREE,
  add key idx_apd_pack_date_total (pack_id, date, total) USING BTREE,
  add key idx_..._on_sample_id (sample_id) USING BTREE,
  add key seller_id_date (seller_id, date) USING BTREE
```

It hit the wall (errno 3024, elapsed 900,004 ms). Two things made it unrecoverable: sluice's existing "clearly huge" probe fires only at **64 GiB** (`indexFallbackHugeTableBytes`), so a 12.8 GB table gets no special handling; and `--resume` re-issues the *identical* combined `ALTER`, which re-hits the same wall deterministically — so the build never converges and the operator restarts from scratch (this was a large part of a 7–8 hour, 4–5 restart ordeal). The other escapes did not apply: the deploy-request build (ADR-0148) needs safe-migrations ON, and `--planetscale-raise-query-timeout` (ADR-0182) was not in play.

## Experiment (local, 8.4 M-row narrow table in the `audio_plays_daily` shape, MySQL 8, 2 GiB buffer pool)

| Approach | Max statement | Total | Stays under a wall the combined build exceeds? |
|---|---|---|---|
| Combined — 4 indexes, one `ALTER` (today's default) | **220 s** | 220 s | scales super-linearly → walls at scale |
| **Per-index — 4 separate `ALTER`s, same table** | **64 s** | 191 s | **yes — ~1/N per statement, no total penalty here** |
| Split into 4 physical shards, index each | 54 s | 194 s | yes (but needs a recombine) |
| Recombine by `INSERT … SELECT` + re-index | 234 s | 234 s | no — re-pays the full cost |
| Recombine by `EXCHANGE PARTITION` (keys compatible) | 0.6 s | 0.6 s | yes — metadata-only |
| Recombine by `EXCHANGE PARTITION` (audio_plays_daily's real keys) | **BLOCKED** (ERROR 1503) | — | no |

The headline: **splitting the combined `ALTER` into one `ALTER` per index cut the longest single statement from 220 s to 64 s with no increase in total time** (191 s ≤ 220 s at a buffer-pool-resident scale; on a table that does not fit in memory the per-index path pays extra table scans, so total time rises — but each statement stays short, which is the property the wall cares about). And because each index is its own statement, `--resume` — which already skips already-present indexes — finishes only the *remaining* indexes instead of re-hitting a monolithic wall.

## Decision

On a **statement-time-limited target** (PlanetScale/Vitess flavor) and a table **large enough to risk the wall**, split the deferred index build into **one `ALTER TABLE … ADD <index>` per index** instead of the ADR-0080 combined `ALTER`. Ordinary tables, and non-Vitess targets, keep the combined `ALTER` (its single-scan win is real and the wall does not apply). The size trigger is a new, **much lower** threshold than the 64 GiB `indexFallbackHugeTableBytes` — that probe exists for the FK metadata-only shortcut and is deliberately conservative; the index-wall trigger must catch a 12.8 GB table, so it keys off a modest `DATA_LENGTH` floor (tuned so the combined build's projected time approaches the wall), not the 64 GiB one. FULLTEXT/SPATIAL indexes are already emitted per-statement, so the machinery exists.

This is deliberately the **cheap, universal** win:
- It applies to **any** table shape — no key constraints (contrast the partition alternative below).
- It converts `--resume` from "re-hit the same wall forever" into "finish the indexes still missing," which is the behaviour operators expect.
- It trades some total wall-clock (extra scans on a memory-non-resident table) for **statements that stay under the limit** — the correct trade when the alternative is a hard failure.

It composes with, and does not replace, the existing levers: `--upfront-indexes` (build during copy), `--planetscale-raise-query-timeout` (ADR-0182), and the deploy-request build (ADR-0148, safe-migrations ON). A pre-flight advisory (shipping alongside, v0.129.0) surfaces the large-table risk before the copy so the operator can also choose those.

## Alternatives considered

- **Partitioned load + `EXCHANGE PARTITION` (the "split into N tables and recombine" idea).** Genuinely elegant and experimentally validated: build each partition's shard separately (each under the wall), then `ALTER … EXCHANGE PARTITION … WITHOUT VALIDATION` swaps each pre-indexed shard in as **metadata only — 0.6 s for the whole recombine**, never re-indexing, and the application sees one (partitioned) table. **Not pursued (operator decision, 2026-08-18), and documented here so the finding is not lost.** Three reasons it is not the default: (1) MySQL requires *every* unique key to contain the partition column, which a surrogate-`id`-PK-plus-independent-business-unique table like `audio_plays_daily` cannot satisfy (its PK `id` and UNIQUE `(date,seller_id,pack_id,sample_id)` share no column → ERROR 1503); (2) `EXCHANGE PARTITION … WITHOUT VALIDATION` silently misplaces any row outside a partition's range, so a correct implementation must split by *actual* key min/max with an open-ended top partition and either validate or prove range-alignment (our own experiment lost ~458 K high-`id` rows to a fixed-range assumption — the caveat made concrete); (3) whether Vitess/PlanetScale Online DDL supports `EXCHANGE PARTITION` at all is unverified and doubtful. The per-index split gets most of the benefit (under-the-wall statements + resumability) with none of these constraints.
- **Recombine by `INSERT … SELECT` into one physical table, then re-index.** Rejected — measured 234 s ≈ the baseline; it re-pays the entire index cost, so there is no win.
- **Always route index builds through the deploy-request (ADR-0148).** Not universal — it requires safe-migrations ON plus a service token; the failing user had safe-migrations OFF.
- **Only raise the query timeout (ADR-0182).** Helps and composes, but a large-enough build exceeds even 3600 s, and the raise is a keyspace-wide rolling restart — a heavier, opt-in lever, not a default.

## Consequences

- One knob's worth of new behaviour on the Vitess/PlanetScale index phase; every other target and every small table is byte-for-byte unchanged (the combined `ALTER` still emits).
- `backup`/`restore`/`sync` cold-start share the index phase, so the split reaches them via the same `CreateIndexes` chokepoint — the sibling-sweep is one change, not several. (Flag the exact call sites in the implementing PR.)
- Total wall-clock on a memory-non-resident huge table rises modestly (extra scans); this is logged so it is not a surprise, and it is strictly better than the hard failure it replaces.
- **Open experiment (the PlanetScale leg):** confirm on a real PlanetScale branch that (a) per-index `ALTER`s each finish under the 900 s wall where the combined one dies, and (b) `--resume` finishes only the missing indexes. Results amend this ADR.
