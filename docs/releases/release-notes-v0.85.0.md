# sluice v0.85.0 — postgres-trigger engine (Phase 1) + CI infrastructure rebuild

**Headline:** Minor release shipping the **`postgres-trigger` engine variant** (Phase 1) — a Go-native alternative to Perl-based Bucardo for PG environments where logical replication slots are unavailable (Heroku Postgres, certain managed-PG tiers). Phase 1 covers composition + setup/teardown + same-engine e2e end-to-end. Phase 2 (cross-engine `postgres-trigger → mysql/planetscale`, full Bug-74 family-matrix pin, sequence/cutover handling) will land in a future release after Phase 1 has had cycle time in `sluice-testing`.

Also in this release: closure of ADR-0054 Phase 2e (the streamer-orchestrated cross-shard consolidation path, end-to-end), CHECK constraint support in the ADR-0054 shape catalog (ADR-0064), backup chain 14e smart compaction (task #16), and a substantial CI infrastructure rebuild — pre-baked DB images on GHCR (MySQL/PG/PostGIS/pgvector) eliminate the per-test first-boot init step + the docker.io single-point-of-failure, plus mid-shard-death sentinels on both engines' shared-container fixtures so a docker daemon hiccup mid-shard produces one loud signal instead of 80 silent-noise cascade failures.

## Added

- **`feat(engines/pgtrigger): task #62 Phase 1 — postgres-trigger engine variant (ADR-0066)`**

  ### Engine surface

  - New `postgres-trigger` engine registered alongside `postgres` (engine name: `"postgres-trigger"`).
  - Composition over embedding: `pgtrigger.Engine` holds a `*postgres.Engine` as a field and explicitly forwards `Engine` / `SchemaReader` / `SchemaWriter` / `RowReader` / `RowWriter` interface methods. Delegation (not Go embedding) is load-bearing per ADR-0066 §9 — embedding would auto-promote the parent's `ir.SlotManagerOpener` / `CDCReaderWithSlotOpener` type-assertions, but `postgres-trigger` deliberately rejects the slot-manager surface (no replication slot to manage), so the explicit-forwarding shape lets the type-assertions return false cleanly.
  - Trigger-based row-change capture: setup installs a per-table trigger that writes the changed row to `sluice_pgtrigger_capture` (JSONB log table, BIGSERIAL ordering + `xmin` safety-lag so concurrent transactions can't be partially observed). CDC reader tails the capture table and feeds the same `ir.ChangeEvent` stream the parent engine produces.
  - JSONB with `json.Decoder.UseNumber()` for numeric round-trip safety — float64 default would truncate `BIGINT` and lose decimal precision.

  ### CLI surface

  - **New subcommand**: `sluice trigger setup --config sluice.yaml` installs the capture table + triggers on the configured tables.
  - **New subcommand**: `sluice trigger teardown --config sluice.yaml` removes them.
  - Explicit lifecycle (not auto-managed): operators see the trigger surface, can audit it, and remove it cleanly. Refuse-loudly when triggers already exist with mismatched signature.

  ### Tests

  - **Same-engine e2e integration test** (`migrate_pgtrigger_integration_test.go`) — bulk-copy + CDC tail of `postgres-trigger → postgres-trigger`, exercising the full snapshot + change-stream path.
  - **Unit tests** for engine wiring, capture-row JSONB shape, trigger-setup idempotency.

  ### Scope ceiling (Phase 1)

  - **Same-engine only.** Cross-engine `postgres-trigger → mysql` / `postgres-trigger → planetscale` lands in Phase 2.
  - **No Bug-74 family-matrix pin yet.** The cross-engine matrix (each element family × scalar/multi-dim/NULL-element shape) is Phase 2 work.
  - **No sequence priming or cutover handling for the variant.** Phase 2.

  See ADR-0066 (`docs/adr/adr-0066-postgres-trigger-engine-variant.md`) for the full design decisions; the planning brief (`docs/adr/adr-0066-task-62-planning-brief.md`) records the operator decisions that closed the open ADR questions.

- **`feat(pipeline): task #23 (ADR-0054 Phase 2e) — MySQL multi-shard test + remaining crash-injection boundaries`** + **`feat(pipeline): task #24 Phase 2e v3 — 3-source streamer-driven harness (ADR-0054)`** — Closes ADR-0054 Phase 2e.

  ### Coverage

  - **3-source PG streamer harness**: three concurrent source PG instances feed one consolidated target through the streamer (`shard_consolidation_phase2e_streamer_pg_integration_test.go`). Proves the streamer-orchestrated cross-shard consolidation works end-to-end.
  - **MySQL multi-shard counterpart**: matches the PG harness shape for MySQL sources, plus the remaining crash-injection boundaries (mid-DDL crash, post-DDL pre-commit crash, post-commit pre-checkpoint crash, etc.) the Phase 2e catalog enumerated.
  - **`BoundaryRouter` routing/observe-flow class diagnosed** (tasks #65 / #66 / #67): the Phase 2e pipeline initially deadlocked under multi-source load. Phase A instrumentation surfaced two root causes: (a) `go-sql-driver/mysql` `ClientFoundRows=false` default returns 0 rows-affected on same-value `UPDATE`, so `RecordDDLText`'s self-takeover idempotency check thought it had failed when in fact it had no-op'd — fixed in #66 by gating the record on `priorDDLText != ddlText`; (b) the PG streamer harness asserted that a post-`ALTER` column had landed via pgoutput before issuing any post-DDL DML, but `pgoutput` only emits `RelationMessage` on the next DML referencing the table — fixed in #67 by adding a per-source post-ALTER INSERT before the column-wait assertion.

- **`feat(pipeline,backup): task #16 — backup chain 14e smart compaction (same-row event collapsing)`** — `sluice backup compact --merge-window DUR` now collapses redundant per-row events:

  - Multiple `UPDATE`s on the same primary-key within the window reduce to the latest before/after-image pair.
  - `DELETE` preceded by `INSERT` on the same primary-key within the window cancels both events.
  - Behavior verified by a property-style test: replaying the compacted chain on a fresh target produces a byte-equal final-state to replaying the uncompacted chain.

  Replaces the placeholder naive-concat compaction from #15. The smart variant is the operator-facing one going forward.

- **`feat(ir,engines): task #22 (ADR-0054 catalog) — CHECK constraint shape support + ADR-0064`** — Extends the ADR-0054 Phase 2 catalog.

  - **IR**: `ir.Table.Checks []*ir.CheckConstraint{Name, Expression}` carries source-side CHECKs.
  - **PG schema writer**: reproduces them as `ALTER TABLE … ADD CONSTRAINT <name> CHECK (<expr>)`.
  - **Shape-delta applier**: `ShapeDeltaApplier_AlterAddDropCheck` (idempotent round-trip) + `ShapeDeltaApplier_AlterModifyCheck`.
  - **Cross-engine refuse-loudly**: PG → MySQL with non-trivial CHECK expressions refuses with an operator-actionable error per ADR-0064 (MySQL's CHECK constraint surface is partial — silent translation would lose semantics).

- **`docs(adr): ADR-0066 postgres-trigger engine variant + task #62 planning brief`** — New ADR documents the engine-variant pattern: compose over embed (§9); trigger-table + JSONB + `xmin` safety-lag instead of replication slot (§3); refuse-loudly §14 boundaries; hybrid DDL detection (§5); cross-references ADR-0007 (engine pattern) and the original Bucardo comparison points.

## Fixed — CI hardening

- **`test(engines/postgres): task #59 — Option B shared PG container for engines/postgres integration suite`** — Mirrors task #56 (Option B shared MySQL fixture) for the PG side. `TestMain` boots one PG container per `engines/postgres` shard; every test calls `resetSharedDB(t, dbName)` to drop slots/databases per-test. Replaces ~80 per-test container boots with one shard-lifetime boot.

- **`test(ci,pipeline): task #69 — mysqlBootTimeout 2min → 4min + WithWaitStrategyAndDeadline`**

  - Bumps the per-helper MySQL boot timeout from 2min to 4min for self-hosted runner disk-I/O contention.
  - Switches from testcontainers-go `WithWaitStrategy` (which hardcodes a 60s outer deadline that wraps whatever `WithStartupTimeout` is passed, silently truncating) to `WithWaitStrategyAndDeadline` (passes the deadline through). Discovered by reading the testcontainers-go source during the v0.85.0-cycle CI investigation.
  - Applied at three sites: `internal/pipeline/mysql_boot_retry_integration_test.go`, `internal/engines/mysql/shared_container_integration_test.go`, and `internal/engines/mysql/cdc_reader_gtid_position_loss_integration_test.go` (the per-test GTID boot site that bypasses the shared TestMain by design).

- **`test(ci): task #68 — pre-bake MySQL/Postgres/PostGIS CI container images`** + **`test(ci): task #70 — pre-bake pgvector to GHCR (#68 follow-up)`** — Four GHCR-hosted pre-baked images replace the upstream Docker Hub pulls for CI integration tests:

  - `ghcr.io/orware/sluice-mysql:8.0-prebaked` (first-boot `mysqld --initialize-insecure` pre-applied; `auto.cnf` cleared so each container boots with a distinct `server_uuid`).
  - `ghcr.io/orware/sluice-postgres:16-prebaked` (`initdb` pre-applied; `pg_hba.conf` configured for the docker bridge network with `host all all all trust`; `test` superuser created with `BYPASSRLS LOGIN`; system identifier cleared so each container boots distinct).
  - `ghcr.io/orware/sluice-postgis:16-3.4-prebaked` (PostGIS extension preinstalled).
  - `ghcr.io/orware/sluice-pgvector:0.7.4-pg16-prebaked` (pgvector preinstalled). **This one was added in task #70 specifically because `pipeline-rest-other` failed 3 consecutive times pulling `pgvector/pgvector:0.7.4-pg16` from Docker Hub with `TLS handshake timeout` — moving to GHCR eliminates docker.io as a single point of failure for the pgvector path.**

  ### Plumbing

  - `scripts/build-prebaked-images.sh` is the canonical bake script; `.github/workflows/build-prebaked-images.yml` runs it weekly (Sunday 06:00 UTC) + on workflow_dispatch + on push/PR that touches the bake yml or script (so changes to the bake logic itself are exercised end-to-end before merging — the `FORCE_REBUILD=1` env var bypasses the base-digest-match skip in this case, which was the lesson PR #70 had to encode after a first iteration silently reused an old image).

  ### Wall-time impact

  - First-boot init that was ~30-60s on a clean runner (or 2-3 min under contention) drops to ~5s pull. Multiplied across the integration matrix per CI run.

- **`test(engines/postgres): task #64 — PG shared-container mid-shard-death TCP-dial sentinel`** + **`test(engines/mysql): task #71 — MySQL shared-container TCP-dial sentinel (#64 follow-up)`**

  ### The failure mode this fixes

  When the shared container dies mid-shard (docker daemon restart on the self-hosted runner pool; postgres-process or mysql-process death inside an otherwise-alive container; port-mapping loss), every subsequent test in the shard would individually fail in its `reset()` hook with `dial tcp 127.0.0.1:PORT: connect: connection refused` — **80x identical error stacks** that buried the actual root cause (the docker daemon, NOT a sluice bug).

  ### The fix

  New `checkSharedContainerAlive(t)` helper on each engine's `shared_container_integration_test.go`:

  - Probes the SQL port directly via `net.DialTimeout("tcp", host:port, 500*time.Millisecond)` — fast, no SQL round-trip overhead.
  - On dial success: closes the conn and returns `true` (caller proceeds).
  - On dial failure: `sync.Once`-gated `containerDead` flag flips, emits one loud `DOCKER-ENGINE-DEAD:` log line at first detection (mentioning that this is a runner-infrastructure issue NOT a sluice bug), then `t.Fatalf` for the rest of the shard.
  - `TestMain` echoes the same loud line at end-of-run so an operator reading CI logs from either end sees the runner cause unmistakably.

  ### Why TCP-dial not `IsRunning()`

  First iteration of the fix (PR #72 initial commit) used `testcontainers-go`'s `Container.IsRunning()`. The cascade recurred in PR #72's own CI: `IsRunning` returned `true` while the SQL port refused connections (failure mode is "container alive by Docker's lights but inner postgres/mysql process dead or port-mapping broken", not "Docker daemon restarted entirely"). The TCP-dial probe is the actual liveness signal — it catches both daemon restart AND inner-process death.

## Compatibility

- **Drop-in upgrade from v0.84.0.** Existing engines (`mysql`, `planetscale`, `postgres`) are unchanged; the new `postgres-trigger` engine self-registers but only activates when explicitly named in `engines.Get("postgres-trigger")` / config.
- **CHECK constraints**: PG → PG migrations now carry CHECKs to the target. Pre-v0.85.0, CHECKs on the source were silently dropped on the target. This is the documented fix for the CHECK class in the ADR-0054 catalog — operators relying on the prior drop behavior (none, presumably) would see CHECK enforcement on target post-cutover.
- **CHECK constraints cross-engine**: PG → MySQL with non-trivial CHECK expressions now **refuses loudly** per ADR-0064. Pre-v0.85.0, silent semantic loss. Recovery: drop the CHECK on the source pre-migration, or supply the explicit translation in the operator's `--checks` flag (per ADR-0064).
- **Minor version bump (v0.85.0)** — new engine + new IR field (`Checks`) + new emit + new refuse-loudly + CI infrastructure rebuild.
- **Severity b** — Phase 1 of `postgres-trigger` is a new engine on the same-engine path (no cross-engine yet); CHECK constraint preservation is a meaningful semantic fix; CI hardening reduces flake rates substantially. Operators not on Heroku-class managed PG and not using CHECK constraints can defer; operators with those should upgrade.

## Who needs this

- **Operators on Heroku Postgres or managed-PG tiers without logical replication slots** — Phase 1 of the `postgres-trigger` engine. Same-engine PG → PG via trigger-based capture instead of slot-based. **Phase 2 (cross-engine) will follow** — until then, this is a same-engine variant only.
- **Operators with CHECK constraints on source PG schemas** — v0.85.0 preserves them through PG → PG migration (and refuses loudly cross-engine where they can't survive without translation).
- **Operators of `sluice-testing` / running sluice's own integration suite locally** — the pre-baked image work eliminates ~30-60s of per-test container boot on every shard plus the docker.io TLS-handshake-timeout flake class.
- **Operators on self-hosted CI runners** — the mid-shard-death sentinels turn an opaque 80x-noise cascade into one loud `DOCKER-ENGINE-DEAD:` signal so the runner-infrastructure cause is visible immediately.
- **Operators chasing ADR-0054 Phase 2e shard-consolidation** — the 3-source streamer-driven harness + MySQL multi-shard test close the Phase 2e catalog. Cross-shard consolidation through the streamer is end-to-end verified.
