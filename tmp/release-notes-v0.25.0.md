# sluice v0.25.0

**Multi-source aggregation Phase 1 + Phase 2: `--target-schema` (PG) + stream-id collision detection.** Operators with N source databases landing in one target Postgres can now namespace each source's tables into its own schema (`customer_svc.users`, `billing_svc.users`) with a single CLI flag — N independent `sluice sync start` processes, one per source, each with its own `--target-schema NAME` + `--stream-id`. Schema collisions (two sources both defining `users`) become a non-issue because each source's tables live in a distinct PG schema namespace. The `sluice_cdc_state` control table picks up a `source_dsn_fingerprint` column and refuses on stream-id collision (the operator-typo case where two streams accidentally share `--stream-id` and would silently overwrite each other's position). ADR-0031 formalises the design (Shape B per `docs/dev/design-multi-source-aggregation.md`); Shape A (sharded → consolidated, e.g. Vitess shards landing in PG analytics) is queued as a long-term roadmap entry, and MySQL native parity is a documented follow-up (today MySQL operators get equivalent coverage via `--target` DSN choice — different MySQL databases on the same server).

## Features

- **`--target-schema NAME` flag on `migrate`, `sync start`, `schema preview`, `schema diff`.** Default empty (use the target DSN's default schema, today's behavior). When set:
  - Every emitted CREATE TABLE / ALTER TABLE prefixes the table reference with the schema name.
  - PG enums get schema-namespaced (`customer_svc.accounts_status_enum`) so two sources with same-named tables don't collide on type names.
  - `CREATE SCHEMA IF NOT EXISTS` runs automatically on first emit — no operator pre-step required.
  - PG schema reader / writer / row reader / row writer / change applier all thread the schema through via the new optional `ir.SchemaSetter` interface.
  - The PG ChangeApplier carries a separate `controlSchema` field pinned at construction so `sluice_cdc_state` stays in the DSN's default schema (one control table per target, regardless of how many `--target-schema` streams point at it).

- **Stream-id collision detection.** New `source_dsn_fingerprint TEXT NULL` column on `sluice_cdc_state` (idempotent migration). On every position-write, the streamer records a SHA-256-truncated fingerprint of the normalized source DSN (host + port + database; user/password excluded so password rotation doesn't break collision detection). On `sync start`, if the existing row's fingerprint differs from the new source's fingerprint, sluice refuses with:
  > `stream "X" exists on target with a different source DSN — pick a different --stream-id or --reset-target-data to wipe and start fresh`

- **MySQL `--target-schema` refusal.** MySQL has no schema concept distinct from databases. The flag refuses cleanly at validate time with an operator-actionable message:
  > `MySQL has no schema concept distinct from databases; use a different --target DSN database to namespace per-source streams (e.g. --target=mysql://...:3306/customer_svc). Phase 1 multi-source is PG-only.`

- **ADR-0031 — Multi-source aggregation: --target-schema + stream-id collision detection.** Decision rationale (Shape B + N-processes + PG-only first), threat model with 5 scenarios (operator typo'd stream-id, mid-flight schema change, Shape A workload using Shape B, etc.), type-name derivation (PG enums namespaced through the schema), and impl summary. References the proto-ADR at `docs/dev/design-multi-source-aggregation.md`.

## Use cases this unlocks

| Scenario | Before v0.25.0 | With v0.25.0 |
|---|---|---|
| **Multiple microservices → one PG analytics warehouse** | Each source lands in the SAME default schema; same-named tables collide silently or operator manually-prefixes via type-overrides. | `--target-schema customer_svc` per source; each source's tables land in their own namespace. |
| **N sluice processes against one target** | Stream-id collision (operator typo'd `--stream-id`) silently overwrites the other stream's position; no warning. | Stream-id collision is refused with operator-actionable error; existing fingerprint vs new source DSN compared on every `sync start`. |
| **Operator rotates source DB password** | Stream-id check (if it existed) would refuse because credentials differ. | Fingerprint excludes user/password; password rotation transparent. Only host+port+database determine the fingerprint. |

## Compatibility

- **No format-breaking changes.** Manifest schema, change-chunk format, CLI surface — all unchanged for existing flows. The new `source_dsn_fingerprint` column on `sluice_cdc_state` is additive (idempotent `ADD COLUMN IF NOT EXISTS`); legacy rows surface as empty fingerprint via `COALESCE` and skip the collision check.
- **Drop-in upgrade from v0.24.0.** No DDL migration needed; the new column lands on first `EnsureControlTable` call.
- **Default behavior unchanged.** Without `--target-schema`, every existing migrate / sync-start invocation lands tables in the target DSN's default schema exactly as before. CLI surface for existing commands is unchanged.
- **MySQL operators unaffected** — `--target-schema` refuses cleanly with the DSN-choice-workaround error message; existing MySQL flows work identically. MySQL native parity (per-table-rename mechanism) is a future chunk if real demand surfaces. See `docs/dev/roadmap.md` "Multi-source aggregation — MySQL native parity" entry.

## Known limitations

- **Mid-flight `--target-schema` change on warm-resume not detected.** If the operator changes the `--target-schema` value between `sync start` invocations on the same stream-id, sluice doesn't refuse — both schemas would receive the stream's new writes. Documented in ADR-0031's threat model (item 5) as a known caveat. Same-shape future refinement could add `target_schema TEXT` to `sluice_cdc_state` and refuse on mismatch (parallel to the new fingerprint check). Not addressed in v0.25.0 because the canonical operator pattern is "set --target-schema once at sync start"; mid-flight changes are operator-typo territory.

- **Shape A (sharded → consolidated) is NOT covered.** v0.25.0 is Shape B (microservices: distinct schemas → per-source target schema). Shape A (N functionally-identical sources → one consolidated target table per type with a discriminator column) needs additional machinery (`--inject-shard-column NAME=VALUE`, populated-target bulk-copy, cross-shard schema-migration coordination). Tracked as a long-term roadmap entry; ship-when-an-operator-asks. The Vitess shards → PG analytics pattern is the canonical Shape A case.

## Test coverage

- **Unit tests** (`internal/pipeline/target_schema_test.go`): `Migrator.TargetSchema` round-trip, `Streamer.TargetSchema` round-trip, fingerprint helper (host+port+database normalization, user/password exclusion, stable across password rotation), stream-id collision detection (existing row matching fingerprint → no refusal; existing row different fingerprint → loud refusal; legacy row empty fingerprint → skip check).
- **PG engine unit tests** (`internal/engines/postgres/target_schema_test.go`): SchemaReader respects `--target-schema` override (reads from configured schema, not `public`); SchemaWriter emits `customer_svc.users` (not `public.users`) when target-schema is set; type-name derivation namespaces PG enums (`customer_svc.accounts_status_enum`); default behavior (no target-schema) unchanged.
- **MySQL engine unit tests** (`internal/engines/mysql/target_schema_test.go`): `--target-schema` refusal with the clear PG-only error message; pinned via test that asserts MySQL doesn't implement `ir.SchemaSetter`.
- **Integration tests** (`internal/pipeline/migrate_target_schema_integration_test.go`, gated `//go:build integration`):
  - PG → PG migrate with `--target-schema=customer_svc`: target schema auto-created, table lands in the namespace, no `public.customers` spillover.
  - Two-source isolation: TWO PG sources with their own `--stream-id` + `--target-schema`; both stream's tables land in their respective schemas; cross-schema isolation verified.
  - Schema preview correctly emits schema-prefixed DDL.
  - Stream-id collision refused (different source) / allowed (same source — warm-resume case).

## Who needs this

- Operators consolidating multiple microservice databases into one PG analytics warehouse with per-service schemas.
- Operators running sluice in test/dev environments with multiple staging streams against the same target — collision detection catches the typo class of "I forgot to update --stream-id."
- Anyone planning to land Shape A (sharded → consolidated) in the future — v0.25.0's foundation supports it; the discriminator-column injection is the additional Shape A work tracked on the roadmap.

## What's next

- **PG Phase 2 strict zero-loss correctness** (mid-stream live add-table follow-up) — close the in-flight-event gap from v0.24.0 via slot-pause or Strategy B dual-slot. Tracked on roadmap.
- **MySQL Phase 2** (mid-stream live add-table for binlog sources) — different mechanism (table-filter flip vs publication scope). Tracked on roadmap.
- **Multi-source — Shape A (sharded)** — discriminator column injection + populated-target bulk-copy. Demand-driven; tracked on roadmap.
- **Multi-source — MySQL native parity** — per-table-rename mechanism if a real operator surfaces "land N MySQL streams in ONE database with namespacing." Demand-driven; tracked on roadmap.
- **PlanetScale MySQL+Vitess test-matrix expansion** — operator-run release checklist (Path A) or CI-integrated coverage (Path B). Tracked on roadmap.
