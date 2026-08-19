# ADR-0184: Per-index `ALTER` splitting for large index builds on a statement-time-limited target

- **Status:** Accepted — experiment validated 2026-08-18; implementation landing v0.129.0 (`migrate` + `sync` cold-start index phase). The PlanetScale leg ran on a real PlanetScale branch (2026-08-18): the wire behaviour is confirmed, but it exposed a size-gate bug (the split sized off the freshly-copied *target's* stale `information_schema` stats, so it never engaged on PlanetScale). Fixed by sizing off the SOURCE estimate — see the Amendment below.
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

## Amendment (PlanetScale leg, 2026-08-18)

The open PlanetScale experiment ran on a real PlanetScale branch, and it confirmed the wire behaviour **and** exposed a gate bug that made the feature inert on its target platform.

**The wire behaviour is fine.** With the released binary the migrate completed and every index landed: both the combined `ALTER` (on tables below the floor) and the per-index `ALTER`s (on tables above it) succeeded on the PlanetScale/Vitess wire. So the split's *emission* — the ADR-0080-combined vs per-index shape — is correct end-to-end; nothing about the DDL shape needed changing.

**The size gate was wrong.** The first cut sized the split from the **target's** `information_schema.tables.DATA_LENGTH` (via the same `tableDataLengthBytes` probe the ADR-0148 FK-shortcut uses), read right after the bulk copy. On PlanetScale/Vitess a freshly-copied table's `information_schema` stats are **stale/uninitialized**: measured, a 524 K-row / 36 MB table reported `DATA_LENGTH = 16384` (16 KB) and `TABLE_ROWS = 5925`. So the 8 GiB gate never crossed on PlanetScale, and the split never engaged — exactly where it exists to help. Local MySQL updates `DATA_LENGTH` promptly, which is why the unit tests and the local experiment (both against local MySQL / an in-memory probe) passed.

**The fix — size off the SOURCE, not the freshly-copied target.** The gate now keys off a per-table **source** byte-size estimate the orchestrator derives before the index phase (a long-lived *source* table has accurate `information_schema` stats even on PlanetScale; only a *freshly-copied target* is stale). Threading, consistent with ADR-0182's source-derived size gate:

- A source `ir.RowReader` optionally implements `ir.TableByteSizeEstimator` (`EstimateTableBytes` — MySQL reads the source's `information_schema` `DATA_LENGTH`, the same read the target probe did but against the accurate source).
- The migrate / sync-cold-start orchestrator builds a per-table `{name → source bytes}` map at the shared copy-phase entry points (`runBulkCopyPhases` for migrate + fast-parallel cold-start; `runBulkCopyWithOpts` for the serial / multi-database cold-start) and threads it to the target `SchemaWriter` via the optional `ir.IndexSplitSizeHintSetter` (`SetIndexSplitSizeHint`), mirroring how ADR-0080 passes the index-build budget.
- `buildTableIndexes`/`emitIndexBuildStatements` consult that source hint instead of probing the target. Byte semantics and the exact 8 GiB threshold (and its whole sizing rationale) are preserved — only the *data source* moved from stale-target to accurate-source. The flavor gate (`usesVStream()`) is unchanged.

**Scope / siblings.** The hint reaches the single `buildTableIndexes` chokepoint, so migrate and the fast-parallel sync cold-start get the source-driven decision from one code path. Where a path's source reader is a *snapshot-pinned* stream (the serial/multi-db cold-start) or an archive (`backup restore`), `EstimateTableBytes` returns "unknown" and the writer keeps the safe combined `ALTER` — no worse than before, and filed as a follow-up (a pinned reader would need an off-snapshot probe; restore could use a manifest-recorded size). The 64 GiB `indexFallbackHugeTableBytes` FK-shortcut is **not** changed here, but it shares the same stale-target-stats weakness (its target `DATA_LENGTH` probe can under-read on PlanetScale) — noted at its definition as a filed follow-up, not fixed in this change since a mis-route there only costs a doomed direct attempt, not correctness.

**Gate ratchet.** A unit pin now drives the exact PlanetScale shape — SOURCE large while the TARGET reports the stale 16 KB — and asserts the split still engages, plus the converse (a huge *target* stat with no source hint must NOT split), so a regression that re-couples the decision to the target's post-copy stats fails the build.
