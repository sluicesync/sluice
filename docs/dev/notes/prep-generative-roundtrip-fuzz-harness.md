# Prep — generative round-trip fuzz harness (Track 2)

Design contract for the property-based correctness harness. Motivation: the entire v0.69.x campaign found Bug 68/69/70/73/74/75 **one latent per release, reactively** (post-publish battle-tests). This harness makes that discovery **proactive and pre-release**, and operationalizes the `CLAUDE.md` "pin the class, not the representative" discipline at scale.

## What it does (one sentence)

Generate random valid schema+data → apply to a real *source* DB via raw DDL/DML → run the real `Migrator` → diff source vs target via direct DB reads → on any divergence, fail with the seed + a replayable fixture.

## Design decisions (rationale = the proven battle-test methodology + IR-first/loud-failure tenets)

1. **Independent oracle — generate raw source-dialect DDL/DML, NOT sluice IR.** The source is established by the database itself (apply `CREATE TABLE` + `INSERT`s directly), then `sluice migrate`, then compare by reading *both* DBs directly. Generating via sluice's own writers would let a writer bug mask a generator/source mismatch. This mirrors exactly how the battle-test fixtures work (maximal independence).
2. **Type-family registry is the heart.** A curated registry; each entry knows: (a) emit the column DDL in each source dialect; (b) generate N random values incl. edge cases (NULL, empty, min/max, boundary precision, multi-byte/emoji, for arrays: empty, NULL element, NULL-inside-multi-dim, ≥2-D); (c) render a value to a per-engine canonical text form for the oracle (`::text` / `format_type` / `array_dims` — the battle-test approach); (d) per direction: expected behavior = **faithful** OR **documented-loud-refuse**. The generator's axes ARE the "pin the class" matrix: every family × {scalar, 1-D array, multi-dim ≥2-D, NULL, NULL-element}. Families MUST include every one that produced a v0.69.x bug: integers (signed/unsigned, all widths), decimal (constrained + **unconstrained**), float, bool, char/varchar/text (incl. **wide >16383**), binary/varbinary/blob, **bit/varbit**, date/time/timestamp/timestamptz/datetime/**timetz**, json, uuid, inet/cidr/macaddr, enum, **arrays incl. multi-dim + NULL elements**.
3. **Three-outcome oracle (loud-refuse is a PASS — loud-failure tenet).** Per generated case classify: (1) src==dst faithful → PASS; (2) sluice **loud-refused** at preview/preflight, exit≠0, **no partial target** → PASS *iff* that construct is in the known-loud-refuse set, else **FAIL** (unexpected refusal = false-positive regression — the v0.69.0 #16 hazard class); (3) mismatch / silent loss / crash / **partial target** → **FAIL**. Distinguishing (2)-expected from (3) is the load-bearing logic; the known-loud-refuse set is sourced from `docs/type-mapping.md` + the catalogued cross-engine limitations.
4. **Reproducible → feeds the pin suite.** One seeded master RNG; a failure prints the seed and writes the failing schema+data SQL to a fixtures dir, deterministically replayable and promotable to a permanent named regression pin. The harness *generates* future pins instead of waiting for a battle-test to find the latent.
5. **Engine-parameterized for Phase 2.** Source/target are engine-flavor parameters. Phase 1 = vanilla `mysql:8.0` / `postgres:16` testcontainers. Phase 2 (Track 1) extends the same generator+oracle to Vitess/PlanetScale flavors — an extension, not a rewrite. This is why it is built engine-neutral now.
6. **Placement & invocation.** `internal/pipeline`, `//go:build integration` (cross-engine round-trip lives there). Generator/registry/oracle in support files; the property driver in a `_integration_test.go`. Env-configurable: iteration count, seed (default random; settable to replay), per-run time budget — so CI nightly runs a large budget, local a small one. Deterministic with a fixed seed.

## Phase 1 scope (this task)

Directions: MySQL→PG, PG→MySQL, PG→PG, MySQL→MySQL. Shapes: scalar + 1-D + multi-dim arrays + NULL/NULL-element across the full family registry. Deliver the generator, registry, oracle, reproducible driver, and a fixtures-dump-on-failure path. Pin a handful of *known* v0.69.x shapes through the harness as a self-check (it must independently re-find nothing — they're fixed — and must NOT false-positive on the documented loud-refuse set).

## Out of scope (later)

Phase 2 = Vitess/PlanetScale flavors (Track 1). Shrinking (seed + dumped fixture is the pragmatic minimum). Perf/throughput (Track 4).

## Review focus (main session, independent + adversarial)

The oracle's silent-vs-loud classification correctness; the registry genuinely covering every family × shape (the Bug 74 class — verify, do not trust); no false-positive on the documented loud-refuse set; reproducibility actually replays a dumped fixture.
