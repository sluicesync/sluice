# Prep: PlanetScale Postgres support

Roadmap reference: not in the original roadmap (PlanetScale Postgres launched after the roadmap was written). Surfaces from the §8 wrap-up conversation about real-world workload coverage.

## Goal

Verify that sluice's existing Postgres engine works correctly against PlanetScale's managed Postgres service, and document any vendor-specific configuration the operator needs to know about. Most of the work is verification; any code changes that surface should be small (vendor-specific quirks, error-message refinements). If significant new code is needed, that's a sign this should split into a separate flavor (à la `FlavorPlanetScale` for MySQL).

Out of scope:

- **PlanetScale MySQL via Vitess.** That's §3b (its own substantial chunk).
- **Performance benchmarking** against PlanetScale-PG specifically. Numbers will appear incidentally during verification.
- **Vendor-specific orchestration features** (PlanetScale's deploy requests, branching workflow, etc.). Sluice is "two DSNs and a CLI"; vendor-specific lifecycle management is the operator's concern.

## What we expect to work

PlanetScale Postgres (PS-PG) advertises standard Postgres compatibility built on a Vitess-like architecture. Documented features as of late 2025:

- **Standard pgwire protocol.** pgx connects normally.
- **Standard SQL.** Schema reads via `information_schema` and `pg_catalog` should work as usual.
- **Logical replication via pgoutput.** PS-PG explicitly supports it (per their CDC documentation), which means the §3 PG CDC reader should Just Work.
- **`CREATE TABLE`, `CREATE INDEX`, foreign keys.** Standard DDL surface.

Plausible verification points:

1. **Schema reader** — connect to PS-PG, run `SchemaReader.ReadSchema(ctx)` against a representative schema. Assert IR shape matches the expected types. No code changes anticipated.
2. **Simple-mode migration** — run `Migrator{Source: mysqlEng, Target: psPgEng}` with a small dataset (sakila → pagila, see [the walkthrough prep](prep-real-world-walkthrough.md)). Verify rows arrive. The COPY-protocol writer (§6) should work; if PS-PG rejects COPY for any reason, the BatchedInsert fallback covers us.
3. **CDC reader** — run `CDCReader.StreamChanges()` against PS-PG with `wal_level=logical` and the `REPLICATION` role attribute. Verify events arrive. Likely needs operator-side configuration that we surface as preconditions.
4. **Continuous-sync streamer** — full snapshot+CDC handoff against PS-PG as the source. Same shape as the §4 PG→PG test against vanilla PG.

## What we anticipate finding (best guesses)

These are educated guesses; the actual list comes from running it.

- **Connection-string format quirks.** PS-PG may use a vendor-specific connection-string parameter (e.g., `?application_name=sluice`, `?sslmode=verify-full` mandatory). Document.
- **Privilege requirements for replication slots.** Managed services often restrict `REPLICATION` role. PS-PG's docs name the specific GRANT; we should surface it as a startup-error message rather than a mid-stream "permission denied".
- **Replication slot creation latency.** Vitess-like architectures sometimes have higher latency for slot operations than vanilla PG. The §3 reader should still work but may need timeout tuning.
- **Schema introspection edge cases.** PS-PG's pg_catalog may have vendor-specific tables that confuse the schema reader's queries. Unlikely but possible; we'd see it as "unsupported data_type" errors and add the needed cases.
- **Cross-instance replication slot semantics.** Vitess-backed Postgres might surface replication slots differently across the underlying shards. For v1 we probably target a single PS-PG database; sharded scenarios are a future concern.

## Files to add / touch

The deliverables depend on what we find. Best case (everything Just Works):

- `docs/managed-services.md` — new file with a "PlanetScale Postgres" section covering: connection-string format, privilege/configuration prerequisites, any vendor-specific limitations sluice itself surfaces, links to upstream PS docs.
- `README.md` — note in the "What it does" section that PlanetScale Postgres is supported.

If we find code-level issues (likely small):

- Per-issue inline fixes in `internal/engines/postgres/`. Each fix gets a comment naming the PS-PG quirk it addresses.
- A new flavor variable if quirks are sufficiently different from vanilla PG (`FlavorPostgres` vs `FlavorPlanetScalePostgres`). This would parallel the MySQL flavor pattern (`FlavorVanilla` vs `FlavorPlanetScale`). Decision point: if 3+ quirks cluster, flavor; if 0-2, inline conditionals.

## Anticipated rough edges

- **Test environment.** Verifying against PS-PG requires a real PS account. Not something we can spin up in CI's testcontainers. Two options:
  - Manual verification by a maintainer with a PS account (one-time, results land in docs).
  - A new "external-service" test tag with credentials supplied via env var — runs in a separate CI job that has the credentials. Heavier infrastructure.
  *Recommendation:* manual verification for v1; "external-service" tag is post-v1 if it becomes load-bearing.
- **PlanetScale's pricing.** Free tier exists; verification budget should be near-zero. Document this.
- **Documentation drift.** PS-PG is new (launched late 2025) and likely to evolve. Anything we document is a snapshot; we should link upstream docs for live information rather than copy-paste.
- **Sharded sources.** PlanetScale's value prop includes Vitess sharding. Whether that surfaces meaningfully on the PG side (vs MySQL-side Vitess) is a real question. v1 targets a single logical database; sharded sources are a follow-up.

## Open questions for the user

1. **Verification strategy: manual vs. external-tag CI.** *Recommendation:* manual for v1; verification work happens once and the results land in `docs/managed-services.md`. Confirm?
2. **Flavor split criterion.** If we find 3+ quirks clustering, declare `FlavorPlanetScalePostgres`. Otherwise inline conditionals with comments. *Recommendation:* keep this rule and decide based on what surfaces. Confirm?
3. **Test data for verification.** Sakila/pagila (per the walkthrough) keeps things consistent. *Recommendation:* same dataset; verification reuses the walkthrough's setup. Confirm?
4. **Scope when we hit a real bug.** If we find a non-trivial issue (say, schema introspection breaks against PS-PG), do we fix in this chunk or defer? *Recommendation:* fix small things (1-day scope) in-chunk; punt anything bigger to its own chunk with its own prep doc. Confirm?

## Suggested first-cut prompt

> "Read CLAUDE.md, docs/dev/notes/prep-planetscale-postgres.md, and the existing PlanetScale MySQL flavor in internal/engines/mysql/flavor.go. Propose the verification plan before writing: (1) the manual verification checklist (what gets run, in what order, against a real PS-PG instance), (2) the docs/managed-services.md outline, (3) the rule for declaring a separate FlavorPlanetScalePostgres vs. inline conditionals. Note any deviation from the prep doc with a why. Stop after the design for review."
