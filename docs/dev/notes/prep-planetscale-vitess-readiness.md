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

## Track-1c outcome + node-replace loud-failure floor (durable record)

Phase 1c delivered + verified vs Docker (2026-05-17, Windows/Rancher, `TESTCONTAINERS_RYUK_DISABLED=true`):

**Phase A — VStream + Vitess schema-tracking DISABLED + mid-stream DDL (the genuinely-open behaviour):** empirically ground-truthed against vttestserver (vtcombo's vttablet runs WITHOUT `--track_schema_versions` by default — exactly the schema-tracking-OFF case) for all three deploy-request DDL shapes (ADD / DROP / MODIFY column). **Result: FAITHFUL in all three.** VStream re-emits a fresh `FIELD` event after the DDL and sluice's `dispatchDDL` → `clear(r.fields)` forces a re-fetch before the next `ROW` decode. **No silent corruption exists on this path** — the doc-hypothesized field-cache-mismatch hazard manifests as either faithful (FIELD precedes ROW, the observed case) or a loud hard error (`row event ... without preceding FIELD event` if a ROW ever raced ahead — loud + recoverable). The schema-evolution path is already at the loud-failure floor; validation-only, no production change. Tests: `internal/engines/mysql/cdc_vstream_schema_evolution_integration_test.go` (`integration vstream` tag).

**Phase A — position-loss code-truth correction:** the MySQL snapshot→CDC handoff ALWAYS persists a **file/pos** position even on a `gtid_mode=ON` source (`cdc_snapshot.go` captures `SHOW MASTER STATUS`). So the streamer's resume-validation path is `verifyBinlogFilePresent` in both GTID-mode and non-GTID deployments; the GTID branch (`verifyGTIDSetReachable`) is reached only by a caller-supplied GTID position (covered at the reader level).

**Phase B — position-loss chaos results:**
- GTID retention-exceeded (reader level, `verifyGTIDSetReachable`): **LOUD + ACTIONABLE** — `StreamChanges` refuses with a message naming the cause and wrapping `ir.ErrPositionInvalid`. Test: `cdc_reader_gtid_position_loss_integration_test.go`.
- `gtid_mode=ON` binlog-purge (streamer level): **LOUD** — file/pos fall-through unaffected by GTID mode; ADR-0022 cold-start re-snapshots, src == dst, exactly-once.
- **Node-replace / restore-from-backup (the highest-value finding): WAS a silent-gap-class bug.** `verifyBinlogFilePresent` matched on binlog **filename only**; a fresh instance reuses the same names (`mysql-bin.000003`, …) for an unrelated lineage, so the name check false-positived and the syncer silently started at a byte offset in an unrelated file. **Hardening landed (minimal loud floor, mirrors `verifyGTIDSetReachable`'s pattern):** bind file/pos positions to the source `@@server_uuid`; on resume, a differing uuid → wrap `ir.ErrPositionInvalid` → existing ADR-0022 cold-start. GTID mode needs no equivalent (GTID UUIDs are instance-bound; `verifyGTIDSetReachable` already catches it). Empty persisted/current uuid degrades to the old filename-only check (no false refusal, zero-users transitional). A full schema-history store remains OUT of scope.

**Recovery characterization (per the loud-failure tenet's actionability requirement):** the ADR-0022 fall-through is a **whole-stream cold-start re-snapshot** (re-reads source schema → re-bulk-copies all in-scope tables → fresh position), not per-table. The loud message names the unrecoverable position and the recovery path ("cold-start is the only recovery path" / "binlog lineage does not carry over"); the streamer emits a WARN naming the stream id + position token before falling through. Operators get a clear, actionable signal — the cost is a full re-sync (the operator-reported pain is inherent to position-loss; sluice makes it loud + automatic rather than a silent gap or a dead stall).

## Synergy / boundaries

- Track 2's fuzz harness is **engine-parameterized** — its generator+oracle extend to a Vitess source/target flavor. Track 2-Phase-2 (cross-engine value oracle) ∩ Track 1 should share that, not duplicate.
- **Roadmap #4 (#37) stays PINNED.** Track 1a's sharded-Vitess infra de-risks/enables #37 (Multi-source Shape A) when the owner unpins it, but Track 1 is *validation infra for Vitess readiness*, **not** the #37 feature. Do not conflate or start #37.

## Track-1a outcome + characterized Streamer-Reopen gap (durable record; code-verified)

Track 1a delivered: scripted `vitess/lite`+etcd resharding cluster (new `vitessreshard` tag, out of normal CI), `ProofOfReshardability` + a reshard-chaos **exactly-once oracle** (committed=2031, delivered-distinct=2031, 0 boundary-replays — VGTID followed the journal to the new layout). Reviewed; the maintainer **personally re-ran** the proof + chaos oracle vs Docker (256s, PASS). The reader's reshard logic is **proven correct**.

**Characterized gap (code-traced end to end, severity-corrected):** the production `Streamer` does not yet drive `reader.Reopen()` on reshard. Path: reshard `JOURNAL` → `vstreamCDCReader.dispatch` returns `*ShardLayoutChangedError` → `pump` `r.setErr(err)` (cdc_vstream.go) → channel closes → Streamer `surfaceSourceError(s.sourceErrFn)` (streamer.go ~1129-1133; the GitHub-#19 / v0.46.0 fix — the CDC analog of Bug 68's `Err()`) → `wrapWithHint(PhaseCDC, …)` → `ShardLayoutChangedError` is not an `ir.RetriableError` → **fatal LOUD `sync` exit**; operator restart → `warmResume` vs the new topology → position-invalid → **[ADR-0022](../../adr/adr-0022-position-invalid-coldstart.md) cold-start re-snapshot**. ⇒ a real PlanetScale/Vitess reshard today is **LOUD, never a silent gap** (loud-failure tenet satisfied); the Bug-68-class silent-swallow does **not** exist on this path.

So the gap is **NOT a silent-loss bug / not a fix-loop emergency.** It is a **HIGH-value missing graceful-continuation feature**: wire the Streamer to drive `reader.Reopen(*ShardLayoutChangedError)` so a reshard is a seamless continuation instead of loud-fail→restart→re-snapshot (the multi-day-outage-class pain — and the cost case for [ADR-0050](../../adr/adr-0050-reconciling-resnapshot.md)'s reconciling re-snapshot if a re-snapshot is still needed). The reader logic it builds on is proven by the Track-1a oracle. **Tightly coupled to Roadmap #4 (#37, PINNED)** — the Streamer reshard/Reopen orchestration is the same layer Shape A's multi-sharded-source consolidation needs; **decide with the owner before starting (do not autonomously implement** — production feature next to a pinned roadmap item). **Owner decision (recorded): DEFERRED — continue Track 1 validation (Phase 1c → 1b); #37 stays pinned.**

## Review focus (main session, independent + adversarial)

The reshard no-gap/no-dup oracle (it must *prove* exactly-once across the journal cut, not just "no error") — to be **personally re-run by the maintainer** against the local Vitess container, mirroring the campaign's accountability discipline. Plus: reuse-not-reinvent verification; the documented manual-only boundary for real-PS dynamic reshard stated honestly, not papered over.
