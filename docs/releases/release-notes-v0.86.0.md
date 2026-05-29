# sluice v0.86.0 — postgres-trigger goes cross-engine (postgres-trigger → MySQL / PlanetScale)

> ⚠️ **Known issue — fixed in v0.86.1. Upgrade past v0.86.0 for the `postgres-trigger` CDC path.**
> Post-release testing found that `sync start` / `migrate` with `--source-driver=postgres-trigger` route the CDC stream through the **slot-based** reader instead of the trigger capture-log poller — so on managed PG that genuinely cannot create replication slots (the engine's entire purpose), the documented `trigger setup → migrate → sync start` flow does not engage the trigger capture path. Additionally, `migrate` includes the engine's own `sluice_change_log` capture tables in the user set, which hard-fails cross-engine create-tables on MySQL (workaround: `--exclude-table=sluice_change_log,sluice_change_log_meta`). The bulk-copy, cutover/AUTO_INCREMENT priming, and all non-trigger directions in this release are unaffected. **v0.86.1 fixes both.**

**Headline:** The `postgres-trigger` engine — sluice's Go-native, slot-less CDC capture for managed PG that locks down logical replication (Heroku Postgres Essential, Render Basic, Supabase free, some RDS/Cloud SQL tiers) — now migrates **cross-engine to MySQL and PlanetScale**, not just same-engine `postgres-trigger → postgres-trigger`. This completes ADR-0066 Phase 2. Building it surfaced and fixed three cross-engine value-fidelity bugs in the MySQL applier (one a silent-corruption, Bug-92 class) that the trigger capture path exposed.

## Features

- **postgres-trigger → MySQL / PlanetScale cross-engine migration + CDC.** A Heroku-class PG source (no replication slots) can now bulk-copy and tail changes into a MySQL or PlanetScale target. The trigger engine installs its per-table capture (`sluice_pgtrigger_capture` JSONB log + `xmin` safety-lag) on the source and the existing MySQL `ChangeApplier` lands the changes — the IR contract carries it, with the value-fidelity fixes below making it byte-correct.

- **Cross-engine refusal gate now covers `postgres-trigger`.** `checkCrossEngineSupportable` / `checkCrossEngineDeltaSupportable` previously gated the PG-native-type refusals (PostGIS `Geometry`, `pg_trgm` operator-class indexes, `EXCLUDE` constraints) on `sourceEngine == "postgres"`, so a `postgres-trigger` source **silently skipped every one of them** — a Phase-2 trust hole. A trigger source now trips the same loud refusals a vanilla `postgres` source does.

- **Sequence / cutover works for the trigger source** via SchemaReader delegation (no new code): `sluice cutover` reads PG `IDENTITY`/serial state through the delegated postgres `SchemaReader` and primes the MySQL target's `AUTO_INCREMENT`. Pinned by a cross-engine cutover integration test.

## Fixed (cross-engine value fidelity — surfaced by the Phase 2 differential test)

The trigger CDC reader decodes its JSONB capture log into a **different value shape** than the proven pgoutput path (numerics → `json.Number`, bytea → `\x`-hex TEXT, timestamps → ISO strings, jsonb → nested `map[string]any`), and that shape flows straight into the MySQL `ChangeApplier`. The MySQL value-prepare path gained three branches so every value family lands correctly:

- **bytea — SILENT corruption (Bug-92 class).** A `\x`-hex string bound to a MySQL `VARBINARY`/`BLOB` column stored the literal ASCII of the hex text (`\xdeadbeef` → 10 bytes) instead of the 4 raw bytes. Now hex-decoded to raw bytes.
- **jsonb — LOUD failure.** A nested `map[string]any` was rejected by the driver (`unsupported type map`). Now marshaled to a JSON object string, with `json.Number` leaves preserving numeric precision.
- **timestamptz — LOUD failure.** An ISO string with a zone offset (`...+00`) was rejected by MySQL's `TIMESTAMP`/`DATETIME` parser (Error 1292). The offset is now stripped (documented zone-flatten policy, same as `timetz`).

These three branches are gated on **both** the IR column type **and** the trigger-only value shape, so the proven pgoutput→MySQL and MySQL→MySQL paths (which pass `time.Time`/raw `[]byte`) are untouched.

Pinned by a cross-engine `postgres-trigger`-vs-`postgres` **congruence** integration test across the full Bug-74 value-family matrix (int4/int8/numeric(30,12)/text/varchar/boolean/timestamp/timestamptz/bytea/jsonb × scalar/NULL/unchanged-rich-UPDATE), a MySQL-side per-column digest oracle, the cross-engine cutover test, and unit pins on the value-prepare path and the gate.

## Changed

- **Stale empty publication now WARNs (silent-CDC-stall diagnostic).** When the PG CDC reader's no-scope path finds a publication that exists, is not `FOR ALL TABLES`, and has no tables, it emits a loud `WARN` naming the publication and the recovery (such a publication replicates nothing — the slot's `confirmed_flush_lsn` never advances). It warns rather than refuses because empty publications occur legitimately on that path; the streamer's scoped `EnsurePublication` establishes scope in the normal flow. `FOR ALL TABLES` and PG 15+ `FOR TABLES IN SCHEMA` publications are correctly treated as non-empty.

## Compatibility

- **Minor version bump (v0.86.0)** — new cross-engine capability, additive.
- **No behavior change to the shipped pgoutput→MySQL or MySQL→MySQL paths.** The three MySQL value-prepare fixes trigger only on the new `postgres-trigger` JSON-scalar value shapes; raw `[]byte` / `time.Time` values flow through unchanged.
- **No config / schema / IR changes.** `postgres-trigger` cross-engine targets are now permitted where they were previously deferred.

## Who needs this

- **Operators on managed PG without logical-replication slots (Heroku, Render, Supabase free, some RDS/Cloud SQL tiers) who want to migrate or continuously sync to MySQL or PlanetScale** — this is the engine's reason to exist (a Go-native Bucardo alternative), now cross-engine.
- **Anyone already using `postgres-trigger` same-engine** — drop-in upgrade; the new value-fidelity fixes harden the capture→apply path.
