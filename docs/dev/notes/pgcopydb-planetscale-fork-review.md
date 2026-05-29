# PlanetScale `pgcopydb` fork — review for sluice

Author: research scratch, 2026-05-29.
Sources:

- Divergent commit list: `gh api repos/dimitri/pgcopydb/compare/main...planetscale:pgcopydb:main` (64 ahead, 14 behind, head `83568e89`).
- PR #33 (the headliner): <https://github.com/planetscale/pgcopydb/pull/33>.
- PRs sampled in detail: #2, #4, #5, #6, #7, #8, #9, #10, #11, #12, #17, #18, #19, #21, #22, #24, #25, #27, #28, #33. (Skimmed for shape, not read line-by-line. PRs #1, #14, #15, #16, #20, #32 were read by commit title only.)
- AGENTS.md at fork root (rendered HTML returned a refusal; not load-bearing — the README/commit messages are enough).

The fork shipped its first cut tag `v0.18.0` on 2026-04-06 and has continued landing fixes since. PlanetScale's positioning: a hardened pgcopydb for production Postgres→Postgres cutovers, especially against managed Postgres sources (RDS / Aurora / Cloud SQL / their own PS-Postgres) where the operator does not own the host.

## 1. TL;DR

- **PlanetScale's fork is overwhelmingly *correctness/operator-UX* polish on top of upstream**, not a structural redesign: resume-safety, FK validity flexibility, read-only-standby support, REPLICA IDENTITY FULL with `json`, XID-wraparound preflight, error-tolerance knobs, snapshot-release timing. The flag surface roughly doubles.
- **The single most relevant item for sluice is PR #28** (`json` columns under REPLICA IDENTITY FULL): sluice's CDC applier currently builds `col = $N` predicates with no special handling for `json`, so the same equality-operator failure would fire on PG-source tables that use REPLICA IDENTITY FULL with a `json` column. This is a latent silent-failure (loud error, but unhandled) we should pin.
- **PR #27's NOT VALID FK retry pattern is also genuinely useful for sluice's `migrate` cutover** — gracefully tolerating dirty-source orphans by attaching constraints as NOT VALID and reporting them, rather than aborting late in the pipeline.
- **PR #17's XID-wraparound preflight is cheap and on-tenet** (loud-failure discipline): one query against `pg_database` before snapshot acquisition would catch a class of "migration mysteriously runs out of XIDs on a long-running source" disasters.
- **Most of the rest is already covered by sluice's architecture** (deferred indexes, parallel COPY, deferred constraints, resume via per-batch checkpoints rather than TRUNCATE), is **C-specific** (SQLite WAL fork corruption, libpq EPIPE handling, pre-fork connection lifecycles), or is **pg_dump/pg_restore plumbing** (extension filtering, ACL parsing, restore-tolerance counter) that doesn't translate to sluice's IR-first model.
- **Structural insight worth noting:** PlanetScale's biggest wins are about *gracefully degrading* against imperfect sources — read-only standbys, orphaned FKs, slow-to-flush prefetch, dirty extension metadata. sluice already prefers loud-failure over silent-degrade; the question for several of these is **which degradations should sluice surface as actionable rather than refuse**, and that's tenets-territory the maintainer owns.

## 2. PR #33 in detail — split-table TRUNCATE on resume (data-loss path)

**What it does.** pgcopydb's `--split-tables-larger-than` partitions large tables into N COPY parts that run in parallel. The COPY supervisor's per-table hook unconditionally issued `TRUNCATE` before workers started. On `--resume` after a post-COPY crash, the supervisor truncated each split table even though every part was already marked done; workers then saw the completed-part markers, skipped COPY entirely, and left the target empty. *"A large migration that hit a failure after the COPY phase and attempted `--resume` had every split table truncated and re-copied, undoing completed work."*

**The fix** gates per-table TRUNCATE on the parts-done count:

- 0 parts done → TRUNCATE as before (first run unchanged).
- All parts done → skip TRUNCATE and skip the enqueue entirely.
- Some parts done → skip TRUNCATE, enqueue all parts; workers skip done ones via the existing lockfile check.

Justified by COPY's atomicity: *"Postgres COPY is atomic in its transaction, so an interrupted part leaves no rows behind."* New regression test `tests/unit/script/5-split-resume.sh` pins it.

**Mapping onto sluice.** sluice's resume model is *fundamentally different*: it does not TRUNCATE on `--resume`. Per-table progress lives in `sluice_migrate_state.table_progress` (ADR-0015/0018); the bulk loader (`internal/pipeline/migrate_bulk.go`) reads the last completed batch boundary and resumes from there, never re-issuing destructive DDL. Parallel chunked COPY (`internal/pipeline/migrate_parallel.go`) writes per-chunk completion entries to the same map; resume reads them and skips done chunks.

So **this exact bug class does not exist in sluice** — and importantly, the *architectural choice* sluice made (per-batch checkpoint in a state table instead of TRUNCATE-on-resume) is the *more defensible* design. PlanetScale's fix here is "salvage the existing protocol"; sluice's design avoids the protocol entirely.

**However**, there's one transferable lesson: when sluice adds anything like a "reset partial work before retry" path (e.g. a `--reset-table` flag for forced re-copy, or a chunked-PG-COPY writer that pre-truncates), the protocol must read the checkpoint *before* the destructive step — not unconditionally fire and trust the worker to be idempotent. The pin in PlanetScale's test (`5-split-resume.sh`: insert a marker row into a done table, resume, verify the marker survives) is a good pattern to copy into sluice's resume integration tests.

**Sluice relevance verdict:** **already have** (the bug class) + **borrow the test pattern** (marker-row-survives-resume) for any future destructive-on-resume code we write.

## 3. PlanetScale-vs-upstream — categorized

### 3a. Correctness / silent-loss

| PR | What it does | Sluice verdict | Rationale |
|---|---|---|---|
| #28 | `LogicalMessageAttribute.typname` + cast `json` cols as `col::text = $N::text` in UPDATE/DELETE WHERE under REPLICA IDENTITY FULL | **would add real value** | `internal/engines/postgres/change_applier.go:buildWhereClause` builds `col = $N` for every column without checking type — under REPLICA IDENTITY FULL a `json` column will trigger `42883 could not identify an equality operator`. We have the type map (`colTypes ir.Type`) right there; cheap fix, missing pin. |
| #27 | Bypass pg_restore for FK constraints; CREATE first, on `23503` retry as `NOT VALID`; report degraded constraints | **would add real value** | sluice's `SchemaWriter.CreateConstraints` aborts on any FK violation; a dirty-source operator gets a late-stage hard fail. NOT-VALID-with-report is on-tenet (loud, surfaces, doesn't silently drop). |
| #12 | CDC: skip materialized views during apply, DEALLOCATE ALL at txn boundary, pipeline-sync at 512 MB param accumulation, filter pgoutput heartbeats from other tools | **partial overlap** | sluice's CDC reader already filters relations not in its catalog and ignores heartbeats; MV-skip and DEALLOCATE ALL are not relevant to our pure-pgoutput path. The 512 MB pipeline-sync is a libpq-extended-protocol concern; sluice doesn't have an equivalent buildup. |
| #7 | test_decoding parser: fix null-literal, escaped-quote, and json/jsonb quoting | **not applicable** | sluice uses pgoutput, not test_decoding. |
| #25 | Reorder `sentinel_update_apply(true)` after schema finalization so FKs/triggers exist before CDC apply begins (fixes silent cascading-delete failures during `--defer-indexes --follow`) | **already have / partial overlap** | sluice already structures the snapshot→CDC handoff so the target schema (incl. constraints) is in place before the streamer starts applying. Worth confirming the *trigger* path holds the same invariant for `pgtrigger`, but the orchestrator-level ordering is built in. |
| #33 | Resume safety for split tables (see §2) | **already have** | sluice's checkpoint-not-TRUNCATE model dodges this entirely. |

### 3b. Operator UX / managed-source accommodation

| PR | What it does | Sluice verdict | Rationale |
|---|---|---|---|
| #17 | XID-wraparound preflight: `SELECT age(datfrozenxid)` against `pg_database`, warn at 75%, refuse at 95% of `2^31`. `--skip-xid-check` + `PGCOPYDB_SKIP_XID_CHECK` to override | **would add real value** | Cheap, on-tenet (loud refuse-loudly). One query in `internal/pipeline/sync_preflight.go` alongside the existing #61 Heroku-permission check. Pattern: same shape as the refuse-loudly preflight we just shipped. |
| #11 | Read-only standby as a source: rewrite filtering queries as CTEs (no temp tables), correct read-only detection, `pg_is_in_recovery()` handling for filtered list ops + clone --follow | **would add real value (deferred)** | sluice currently assumes the source can be written to (for slot creation, sentinel updates, etc.). Slot-based CDC can't run from a read-only standby (slots are primary-only) so this only matters for *snapshot-only* and *pgtrigger-style* sourcing. Real ask in managed-PG land but not yet a roadmap item — worth promoting if a customer asks. |
| #18 | `--restore-tolerance N` (was constant 10): allow N pg_restore errors before aborting; covers extension-version-mismatch noise | **not applicable** | sluice doesn't shell out to pg_restore. Same *intent* (tolerate non-critical schema noise) is interesting but sluice's IR-first model already chooses which DDL to emit; the analogue would be "tolerate N target-side DDL errors", which we should *not* do (silently dropping objects is exactly the silent-loss pattern we refuse). |
| #6 | Tolerate minor pg_restore errors during schema restore (extension version mismatches etc.) | **rejected** | Same reasoning as #18. |
| #9 | `--skip-publications` flag | **not applicable** | sluice doesn't replicate publications to the target. |
| #10 | `--exclude-extension` for clone/copy: filter extension-owned objects + dependencies + ACLs | **partial overlap** | sluice already has filter machinery (`internal/pipeline/filter_test.go`); extension-aware filtering is meaningful for cross-engine (PG→MySQL won't carry PG extensions anyway). Low priority unless a customer needs PG→PG extension exclusion. |
| #14/#15 | `[exclude-event-trigger]` filter + cross-schema dependency filtering | **partial overlap** | Event triggers aren't yet on sluice's translation policy; cross-schema dep filtering is a tactic we may need when sluice grows multi-schema filter granularity. Note for later. |

### 3c. Performance / pipeline orchestration

| PR | What it does | Sluice verdict | Rationale |
|---|---|---|---|
| #17 | Early snapshot release after COPY + blob work (lets source VACUUM advance during index/constraint build on target) | **partial overlap** | sluice's snapshot is held by the source-side transaction backing the bulk reader; once the reader closes, the snapshot releases. The improvement applies *only* if sluice holds a snapshot across index+constraint creation, which we don't for the simple Migrator path. Worth re-checking for `sync start` (long-running snapshot→CDC). |
| #21 | `--defer-indexes` flag (skip per-table index queue; bulk-queue all indexes after COPY done) | **already have** | sluice's pipeline is exactly "create tables → bulk-copy rows → create indexes → create constraints" (see `internal/ir/interfaces.go` `CreateIndexes` / `CreateConstraints`, and `internal/engines/{mysql,postgres}/schema_writer.go`). pgcopydb is catching up to a default we already have. |
| #22 | `--defer-indexes --follow`: move index building from clone subprocess to parent process; subprocess exits early to release source conns | **not applicable** | sluice doesn't fork a subprocess for clone; the orchestrator owns the lifecycle in-process. |
| #24 | `--defer-analyze`: skip per-table VACUUM ANALYZE during the migration steps, run one `vacuumdb --analyze-only` at the end | **would add real value (small)** | sluice does not currently issue `ANALYZE` against the target after bulk load (operator runs it themselves). Adding an opt-in `--analyze-after` at the orchestrator tail is cheap and improves first-query-after-cutover performance. Small enough to bundle. |
| #4 | COPY worker retry: time-budgeted exponential backoff (up to 30 min, 10 attempts), connection-level 5-min/30-sec retry | **partial overlap** | sluice's reader/writer retry posture is currently "fail loud, let the orchestrator decide" — appropriate for the simple Migrator, less appropriate for `sync start` long-runners. Worth borrowing the bounded-budget shape (not the C-specific implementation) for the streamer's reconnect path, similar to how we landed TCP keepalives via `internal/netkeepalive`. |
| #21 (part 2) | EPIPE on streaming `--to-stdout`: stop reconnecting when downstream exits | **not applicable** | C/libpq-specific. sluice's stream broker already detects clean shutdown via Go channels. |

### 3d. Resilience / process lifecycle (C-specific or partially)

| PR | What it does | Sluice verdict | Rationale |
|---|---|---|---|
| #25 | Close catalog SQLite connections before fork to prevent WAL checkpoint corruption | **not applicable** | sluice is Go, no fork-vs-SQLite issue. |
| #22 (part 1) | Close snapshot connection after large-object check to release source XID pin | **rejected as direct port (already have at architectural level)** | sluice's bulk reader uses a single REPEATABLE READ transaction it closes deterministically. No equivalent "leaked check connection" pattern. |
| #16 | Release catalog semaphore before creating constraints | **not applicable** | C/SQLite catalog concurrency mechanic. |
| #5 | "source is already in use" fixes, catalog mismatch in --follow, namespace DATA_SECTION_SCHEMA population | **not applicable** | C/SQLite catalog mechanics. |
| #23 | Earlier snapshot-done signal from COPY supervisor | **not applicable** | C subprocess signaling. |

### 3e. Build / CI / governance

| PR | What it does | Sluice verdict | Rationale |
|---|---|---|---|
| #1 | PG 17 / 18 support | **already have** | sluice's PG matrix is already broad; pgvector / pg17 / pg18 are exercised. |
| #2 | Tiered CI, podman support, retry apt | **partial overlap** | sluice's CI already runs retry-on-flake via `gh run rerun --failed` and `ci-ghcr-pull.sh`. Tiered testing (fast unit → slow integration) is something the maintainer has been chipping at. |
| #3 | Tests for `--skip-vacuum`, `--skip-large-objects` | **not applicable** | flag-specific. |
| #20 | actions/checkout v4 → v6 (Node 24) | **already have** | routine maintenance. |
| #32 | Pin GitHub Actions to full-length SHAs | **would add real value (governance)** | We pin some, not all. Trivial PR; worth a one-shot sweep across `.github/workflows/`. |
| #34 | Disable timescaledb integration test (upstream failure) | **not applicable** | pgcopydb test infra. |

## 4. Recommended adoptions, ranked

1. **REPLICA IDENTITY FULL + `json` column equality fix** (mirrors PR #28).
   - **What:** in `buildWhereClause` (`internal/engines/postgres/change_applier.go:1678`), when `colTypes[c]` is the IR equivalent of PG `json` (and arguably `xml`, which has the same no-equality problem), emit `quoteIdent(c) + "::text = $N::text"` instead of `quoteIdent(c) + " = $N"`. The argument value stays as-is — Postgres casts the bind to text. Apply to UPDATE WHERE *and* DELETE WHERE; INSERT is unaffected.
   - **Where it lives:** `internal/engines/postgres/change_applier.go` + a new integration test under `internal/engines/postgres/` that creates a table with `id int pk, payload json`, sets `REPLICA IDENTITY FULL`, runs INSERT/UPDATE/DELETE through the streamer, and asserts the target converges. (`xml` can ride along if cheap.)
   - **Blockers:** none. The `colTypes` map is already in scope. `jsonb` does have equality so it's not strictly necessary, but a cast is also harmless and removes a class of "do we have to know?" — recommend casting both.
   - **Sizing:** ~20-line code change + one integration test (~150 LOC). One PR.

2. **NOT VALID FK retry with reporting** (mirrors PR #27 in spirit, not in code).
   - **What:** When `CreateConstraints` hits SQLSTATE `23503` (foreign-key violation) attaching an FK, retry with `NOT VALID`. Surface the degraded constraint in a structured report (the existing capability/report surface in `internal/pipeline` is the natural home). Default opt-in (`--allow-degraded-fks` or similar — the operator opted into a known-dirty source).
   - **Where it lives:** `internal/engines/postgres/schema_writer.go` (FK creation), `internal/engines/mysql/schema_writer.go` (parallel — MySQL also supports `NOT VALID`-like behavior via `SET FOREIGN_KEY_CHECKS=0` but the report contract is the same), and a new "constraint degradation report" type in `internal/pipeline`.
   - **Blockers:** design choice on tenets — surfacing degraded constraints is "loud failure with named wart"; silently retrying with NOT VALID without surfacing would violate "contain Postgres complexity". Default-off is the conservative call.
   - **Sizing:** ~150 LOC + tests, plus a CHANGELOG and ADR. One PR.

3. **XID-wraparound preflight** (mirrors PR #17).
   - **What:** Pre-clone query against the source: `SELECT datname, age(datfrozenxid) FROM pg_database WHERE datname = current_database()`. Warn at 75% of `2^31`, refuse-loudly at 95% (with `--skip-xid-check` escape hatch). Same shape as the #61 Heroku-permission preflight we just shipped.
   - **Where it lives:** `internal/pipeline/sync_preflight.go` (or `internal/engines/postgres/preflight.go` if we want to keep engine specifics out of pipeline). Probably the latter, given IR-first.
   - **Blockers:** none. `pg_database` is readable by every connectable role.
   - **Sizing:** ~80 LOC + one integration test that fakes high `age(datfrozenxid)` (or just unit-tests the threshold math against fixture values).

4. **Pin GitHub Actions to full-length SHAs** (mirrors PR #32).
   - **What:** Sweep `.github/workflows/*.yml` for `uses: actions/foo@vX` and replace with the SHA-pinned form. Supply-chain hygiene; same trivial improvement.
   - **Where it lives:** `.github/workflows/`.
   - **Blockers:** none.
   - **Sizing:** <1 hour. Bundle with the next governance PR.

5. **`--analyze-after` orchestrator tail** (mirrors PR #24).
   - **What:** opt-in flag that triggers `vacuumdb --analyze-only` (or its `ANALYZE` SQL equivalent through the writer interface) after `CreateConstraints`. For PG target: `ANALYZE;` over the connection. For MySQL target: `ANALYZE TABLE` per table or skip. Nice-to-have for cutover.
   - **Where it lives:** `internal/pipeline/migrate.go` post-constraint phase; engine writers expose `Analyze(ctx)`.
   - **Blockers:** new method on `SchemaWriter` interface — small IR change.
   - **Sizing:** ~100 LOC + tests. Skip if cutover performance isn't a current complaint.

(Items 1–3 are the high-conviction ones. Items 4–5 are quality-of-life.)

## 5. Explicitly rejected / deferred

- **`--restore-tolerance N` (#18) and "tolerate minor pg_restore errors" (#6).** Sluice doesn't shell out to pg_restore; the IR is the source of truth. The intent — "let us survive 10 schema errors before aborting" — is *anti-tenet* for sluice (silently dropping target objects is exactly the silent-loss pattern we exist to prevent). The PlanetScale-style flag would silently degrade the target schema; sluice should refuse-loudly with a structured report instead.
- **`--skip-publications` (#9) and `--skip-large-objects` (#3).** Not part of sluice's surface — sluice does not copy publications or large objects today.
- **`--exclude-extension` (#10), `[exclude-event-trigger]` (#14/#15).** Sluice already has filter wiring; these are specific filter knobs we can add when a customer asks for them. Not worth pre-building.
- **Catalog-fork-corruption fixes (#25 part 1), catalog semaphore release (#16), pre-fork DB-handle hygiene (#22 part 1), DEALLOCATE ALL at txn boundary (#12 part 1), EPIPE reconnect avoidance (#21 part 2).** All C/libpq/SQLite-specific. Go's runtime + sluice's architecture don't have the analogous bugs.
- **`--defer-indexes` (#21).** Already the default in sluice.
- **Read-only-standby source (#11).** Deferred: real ask in managed-PG-land, but not a roadmap item yet. Pull forward only on customer demand. Note that slot-based CDC can't work from a standby (slots are primary-only) — the standby path is for snapshot-only and pgtrigger-style sourcing.
- **Early snapshot release timing (#17 part 1).** Already structurally clean in sluice's simple-mode Migrator; potentially relevant only for `sync start` snapshot→CDC handoff and worth re-checking if a long-running sync ever blocks source VACUUM in the wild.
- **Resilient COPY retry with exponential backoff (#4) — as a direct port.** sluice already prefers loud-failure for the simple Migrator. *Borrow the bounded-budget retry shape* for the streamer's reconnect path (we'd want this regardless of pgcopydb's lead), but don't add it to the bulk copy path without a concrete failure mode.

## 6. Open questions for the maintainer

1. **REPLICA IDENTITY FULL + `json` (item #1) — is there already a translation-policy ruling somewhere that PG sources with `json` columns + REPLICA IDENTITY FULL aren't supported?** I didn't find one; the applier code looks like it would just blow up at apply time. If we want to support these sources, the cast fix is the right move.
2. **NOT VALID FK (item #2) — is "tolerate dirty FK source" on the tenets' good side or bad side?** I lean good *if and only if* the report surfaces the degraded constraints unambiguously and the default is off. Want a maintainer call before designing the report shape.
3. **XID-wraparound preflight (item #3) — does this belong in `sync_preflight` next to the Heroku-permission check, or in a per-engine preflight that the orchestrator composes?** I lean toward engine-local (`internal/engines/postgres/preflight.go`) because the query is PG-specific. Light preference.
4. **Read-only standby source (deferred) — is anyone asking for this?** Worth knowing whether to keep on the deferred list or drop entirely.
5. **`--defer-analyze` analogue (item #5) — is target-side ANALYZE-after-cutover something operators currently do by hand, or do they want sluice to drive it?** If by-hand is fine, skip.
6. **PR #28's `LogicalMessageAttribute.typname` analogue** — sluice's CDC path already carries IR type info through `colTypes`, so we don't need a wire-format addition. Confirming that's accurate vs. having to plumb anything new through the pgoutput parser.
