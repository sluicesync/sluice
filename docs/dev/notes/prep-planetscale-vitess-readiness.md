# Prep — PlanetScale / Vitess production-readiness validation (Track 1)

Design contract. Audience: the make-or-break one — Vitess and especially **PlanetScale** users. The v0.69.x campaign validated vanilla MySQL↔PG only; PS/Vitess is the unexercised frontier (`psverify` / `integration vstream` tags + the separate PlanetScale validation rig exist as scaffolding).

## Confirmed topology (the principled split — reasoned, not arbitrary)

**PlanetScale *is* Vitess.** What sluice's CDC reader must survive across a reshard is the VStream **VGTID / JOURNAL** event sequence at the shard cut — the *identical Vitess code path* whether self-hosted or PS-managed. So:

- **Local Vitess = authoritative** for reshard-correctness + topology/CDC mechanics. `vtctldclient Reshard create` → `SwitchTraffic` is fully scriptable + deterministic; exercises exactly the sluice logic a PS reshard would.
- **Real PlanetScale = the PS-managed product envelope** local Vitess can't represent (edge/auth, connection caps, branching/deploy-request, no-`LOCAL INFILE`, vtgate `information_schema` as PS exposes it).

**`pscale keyspace` capability (verified against the docs):**
- `pscale keyspace create --shards <N>` + `vschema update` — **automatable**: a *static* already-sharded PS source.
- `pscale keyspace resize` is **cluster-size / replica-count, NOT shard-count**. There is **no CLI-automatable shard-count reshard**. ⇒ dynamic reshard on real PS = **documented manual/periodic check only**; the local-Vitess automated test is authoritative and carries the reshard-correctness load (zero coverage lost — same Vitess code path).

## Phasing

### Phase 1a (FIRST — the authoritative correctness core)

Local Vitess testcontainers infra (gated tag; reuse/extend the existing `integration vstream` surface). Tests:
- **VStream CDC basics** — VGTID tracking, snapshot→VStream handoff under continuous writes, large transactions.
- **Reshard chaos (the headline test)** — sharded keyspace → seed → start sluice VStream CDC → `vtctldclient Reshard create` + `SwitchTraffic` *mid-stream* → assert: **no gap, no dup, correct VGTID-journal follow, src==dst after cutover**. The oracle: every source row appears exactly once on the target across the journal cut; the reader's VGTID resumes on the new shard topology without re-reading or skipping.
- Scatter / cross-shard reads; Vitess no-runtime-FK-enforcement vs sluice's FK-DDL phase; `set workload=olap` read path.

### Phase 1c (CDC resumability under position-loss + schema evolution — co-equal priority; arguably higher real-PS impact than reshard)

Operator-reported: PlanetScale users hit multi-day sync outages requiring full table re-syncs when a node is replaced / restored-from-backup / failed-over (binlogs don't carry to the new instance) or the sync is down past binlog retention (PS default 3 days) — `gtid_purged` advances past the consumer position. Separately, deploy-request schema changes break CDC tools when Vitess schema-tracking (off by default) isn't carrying post-DDL field metadata.

**Current sluice behavior (code-truth, verified — NOT naive):**
- Position-loss is handled by design: `cdc_reader.go::verifyPositionResumable` checks `SHOW BINARY LOGS` (file/pos) and `GTID_SUBSET(@@global.gtid_purged, resume)` (GTID) on every resume; on loss it wraps `ir.ErrPositionInvalid` and the orchestrator **falls through to cold-start re-snapshot (ADR-0022)** — loud detection + automatic re-snapshot, never a silent gap or dead stall.
- Binlog-path DDL → schema-cache invalidation (re-read). VStream-path schema awareness depends on Vitess schema-tracking, **off by default**.

**Validation gaps (the Phase-1c scope — oracle = NEVER a silent gap/corruption; loud + actionable + recoverable is the bar):**
- **PS-realistic position-loss chaos**: not just synthetic binlog-purge — reproduce the actual mechanism (local: replace/restore the source so binlogs genuinely don't carry over, or purge + advance `gtid_purged`; real PS: the operations that cause it). Assert: loud `ErrPositionInvalid` → ADR-0022 cold-start fall-through executes cleanly end-to-end → data correct after re-snapshot (no gap, no dup).
- **Recovery granularity/cost**: is the ADR-0022 fall-through whole-stream or per-table re-snapshot? (Operators' pain was "re-sync an entire table.") Validate + document what is re-snapshotted (all vs only affected) and whether the loud message names the affected tables + the recovery command (actionability per the loud-failure tenet).
- **Schema evolution mid-stream**: a deploy-request-style DDL (column add/drop/type change) while streaming, on BOTH paths, and specifically the **VStream + schema-tracking-DISABLED** case — does sluice loud-fail with actionable guidance (acceptable: "enable Vitess schema-tracking" / re-snapshot) or silently apply mis-aligned rows (silent corruption — a FAIL)?

### Phase 1b (real PlanetScale via `pscale`, on the existing PLANETSCALE_CREDENTIALS.env / `psverify` scaffolding)

- **Static sharded source** — `pscale keyspace create --shards N` + `vschema update` → sluice migrates a real already-sharded PS keyspace (real vtgate/scatter/edge).
- **No-`LOCAL INFILE` batched-INSERT at scale** — PS's *default* copy path (the #18 LOAD-DATA hardening does NOT apply to PS); throughput + correctness at 10M+ / wide rows.
- **Branching workflow** — `pscale branch` create → migrate into branch → `pscale deploy-request` → promote; does sluice migrate/CDC state survive a branch promotion?
- **vtgate `information_schema` fidelity + latency** as PS exposes it (sluice's schema reader depends on it; vtgate aggregates differently than vanilla; the rig already flagged ~30s serial introspection for 329 tables).
- **Connection caps / pooling** under PS's aggressive limits.

## Phase-A mandate for the build (NON-NEGOTIABLE — the fuzz-harness reuse lesson)

STUDY and reuse the existing scaffolding before writing anything: `internal/engines/mysql/cdc_vstream_snapshot.go`, the `integration vstream` build-tagged tests, and `shapea_spike_vstream_integration_test.go` (the Roadmap-#4 Vitess-testcontainers spike). The local-Vitess infra is a *generalisation* of that spike, not a new framework. Document what's reused.

## Synergy / boundaries

- Track 2's fuzz harness is **engine-parameterized** — its generator+oracle extend to a Vitess source/target flavor. Track 2-Phase-2 (cross-engine value oracle) ∩ Track 1 should share that, not duplicate.
- **Roadmap #4 (#37) stays PINNED.** Track 1a's sharded-Vitess infra de-risks/enables #37 (Multi-source Shape A) when the owner unpins it, but Track 1 is *validation infra for Vitess readiness*, **not** the #37 feature. Do not conflate or start #37.

## Review focus (main session, independent + adversarial)

The reshard no-gap/no-dup oracle (it must *prove* exactly-once across the journal cut, not just "no error") — to be **personally re-run by the maintainer** against the local Vitess container, mirroring the campaign's accountability discipline. Plus: reuse-not-reinvent verification; the documented manual-only boundary for real-PS dynamic reshard stated honestly, not papered over.
