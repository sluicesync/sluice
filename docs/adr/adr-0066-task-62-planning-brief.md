# Task #62 planning brief — `postgres-trigger` engine

Companion to [ADR-0066](adr-0066-postgres-trigger-engine-variant.md). The
ADR makes the calls; this brief is the one-page summary of the load-bearing
ones plus the explicit open questions the operator should personally weigh
in on before any code is written.

## Key calls (the things to push back on first)

- **Engine name: `postgres-trigger`, single name, no flavor split.** Rejected
  `postgres-bucardo` (foreign tool tie-in), `pg-trigger` (ambiguous with
  package name), per-service flavor split (the lockdown differences are
  about *eligibility* for the pgoutput engine, not capability differences
  for the trigger engine).

- **Change-log table is `sluice_change_log` with `BIGSERIAL id` as the
  polling watermark.** Key insight: `id` order is allocation order, NOT
  commit order — overlapping transactions can allocate `id=5` and
  `id=6` but commit in `6, 5` order. The polling reader mitigates with
  an `xmin < pg_snapshot_xmin(pg_current_snapshot())` safety-lag query,
  the same mitigation Debezium's PG engine uses for its incremental-
  snapshot mode. This is non-obvious and the operator should confirm
  the safety-lag check is on by default (it is).

- **One shared `plpgsql` capture function dispatched by `TG_TABLE_NAME`,
  one trigger per replicated table referencing it.** Not per-table
  functions; 200 functions to maintain is the failure mode. The
  function is `SECURITY DEFINER` so a non-table-owning role can drive
  the engine as long as the function-owning role has INSERT on
  `sluice_change_log`.

- **JSONB only.** Not `json` text, not `hstore`. Type-fidelity matters:
  the decode side must use `Decoder.UseNumber()` to preserve PG's
  unbounded `numeric` (the silent-loss class otherwise — and worth a
  Bug-74-style class-pin matrix in the test suite, which §15 of the
  ADR commits to).

- **Hybrid DDL detection: event triggers (PG 14+ via `pg_create_event_trigger`,
  or superuser pre-14) AND a polled schema-fingerprint as fallback.** Both
  paths refuse-loudly on DDL — no auto-apply. The drained-model recovery
  pattern (ADR-0054) is the canonical recovery. On the most restricted
  tiers that grant neither event-trigger creation nor superuser, the
  operator opts in to polled-fingerprint mode via
  `--allow-polled-fingerprint`, rather than have it silently degrade.

- **`SupportsGeneratedColumns: false`** — the trigger engine refuses
  replication of `GENERATED ALWAYS AS ... STORED` columns, because
  replicating their captured value to a target that has its own
  expression for the same column is either confusing (best case) or
  silently divergent (worst case). The pgoutput engine handles this
  correctly via column-list filtering; the trigger engine doesn't,
  and refusing is the right call.

- **Composition, not fork.** `pgtrigger.Engine` embeds `postgres.Engine`,
  overrides the two CDC-related methods, deliberately omits
  `OpenSlotManager` and `OpenCDCReaderWithSlot`. The ~10K-LOC PG
  reader/writer stays in exactly one package.

- **Setup is explicit: `sluice trigger setup --dsn=... [--dry-run]`.** Not
  implicit on first `sync start`. The operator sees the DDL, applies it
  deliberately, and can tear it down with `sluice trigger teardown`. The
  pgoutput engine's implicit publication-and-slot creation has been a
  source of operator confusion (ADR-0011); this engine's explicitness
  is the design lesson learned.

- **Design ceiling: 5000 changes/sec sustained, 1000 changes/sec on
  Heroku Essential / Render Basic-class restricted tiers, p99
  source-to-target latency of 2 seconds.** Above 2× the ceiling for 5
  minutes, the engine refuses-loudly and exits with "use the
  `postgres` engine on a tier that supports logical replication." Loud,
  not silent, per the loud-failure tenet.

## Open questions for the operator

All resolved 2026-05-27 (orware@gmail.com). Originals retained for
context; the operator's decisions are summarised inline.

1. **Design ceiling (§11) — Resolved: ship as stated.** 5000 chgs/sec
   default-tier, 1000 chgs/sec restricted-tier, p99 < 2s, refuse-loudly
   at 2× ceiling for 5 minutes. The operator confirms these are
   "honest numbers" for the engine's positioning — low-to-medium
   throughput migrations from restricted PG tiers, not a replacement
   for the logical-replication-based `postgres` engine on capable
   tiers.

2. **JSONB encoding — Resolved: keep JSONB.** Single change-log table
   with JSONB payload and `Decoder.UseNumber()` on decode. The Bug-74
   class-pin matrix (§15) is the safety net against `numeric`
   round-trip silent-loss. Rejected: column-per-value sub-table per
   replicated table (N-times setup-DDL surface).

3. **DDL detection — Resolved: hybrid (as ADR §7).** Event triggers
   on PG 14+ (or superuser pre-14), polled schema-fingerprint as the
   restricted-tier fallback. Both refuse-loudly on DDL — no
   auto-apply. The drained-model recovery pattern (ADR-0054) is the
   canonical recovery path.

4. **Setup CLI namespace — Resolved: `sluice trigger setup/teardown`
   (as ADR §10).** Generic "trigger" subcommand keeps future
   trigger-based engines (a hypothetical `mysql-trigger`) sharing the
   namespace. Rejected: `sluice pgtrigger ...` (one-to-one
   engine-naming) and `sluice schema setup --engine=postgres-trigger`
   (generic schema subcommand).

5. **~~Should v1 ship `postgres-trigger → planetscale` integration tests
   alongside `postgres-trigger → mysql`?~~** **Resolved: yes, ship in v1.**
   The "Heroku Postgres Essential → PlanetScale" migration story is a
   distinct customer narrative from "Heroku Postgres Essential → AWS
   RDS MySQL" and worth pinning in v1. The cost is one additional
   integration-test cell, not a different design. ADR §12 updated.

## What's NOT in scope for v1 (called out in the ADR)

- Auto-apply of source-side DDL via the trigger plane (§7 — explicitly
  rejected, drained-model recovery only).
- Live mid-stream add-table (ADR-0030 — refused on the trigger engine
  in v1).
- `--position-from-manifest` integration (ADR-0049 Chunk D — works
  conceptually; v1 defers until a real user surfaces).
- NOTIFY/LISTEN-based pull (rejected on correctness and back-pressure
  grounds; possible v1.5 as a wake-the-poller hint, not as a
  correctness primitive).
- `mysql-trigger` or any other non-PG trigger engine. The IR contract
  accommodates it but this ADR is scoped to PG; MySQL's binlog is
  the right CDC primitive on every MySQL tier the author has
  surveyed.
- Conflict resolution / active-active replication (out of scope per
  CLAUDE.md's "decisions deferred" — sluice sync is unidirectional
  in v1).
