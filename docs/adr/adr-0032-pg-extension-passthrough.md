# ADR-0032 — PG → PG extension passthrough (allowlist + framework)

**Status:** Accepted (v0.26.0)

**Context:** Postgres' extensibility — pgvector, PostGIS, pg_trgm, hstore, citext, ltree, pgcrypto, uuid-ossp — is a major reason operators choose PG specifically. Today sluice's IR doesn't represent extension types, so PG-source columns of those types either get silently dropped (not OK per the loud-failure tenet) or surface a refusal at schema-read time. **For PG → PG syncs where both sides have the same extensions installed, those columns should "just work"** — pass through with native fidelity rather than being treated as hostile.

This ADR formalises the v1 framework that lands `pgvector` as the first concrete extension and shapes subsequent extensions (pg_trgm, hstore, citext, postgis) as catalog-only follow-ons rather than per-extension surface-area expansions.

Related research: [`docs/research/pg-extensions-deployment-frequency.md`](../research/pg-extensions-deployment-frequency.md). Roadmap entry 12.

## Decision

### Opt-in allowlist (not auto-detect)

A new repeatable CLI flag `--enable-pg-extension EXT` opts the operator into passthrough for a single named extension. The flag is wired on `migrate`, `sync start`, `schema preview`, and `schema diff`.

**Why allowlist not auto-detect.** Operator explicitly affirms intent. Failure-mode analysis stays tractable (the set of extensions in flight is closed; the fan-out of tests to write is bounded). v1 is conservative; auto-detect (or `--auto-pg-extensions`) is a v2+ option once the allowlist has battle-tested the per-extension catalog shape.

### Same-engine target only

PG → PG with the same extension installed on the target = passthrough. Cross-engine targets (PG → MySQL with `--enable-pg-extension pgvector`) keep the existing **loud-failure default** — the cross-engine refusal in `pipeline.checkCrossEngineSupportable` extends to refuse `ir.ExtensionType`. Operator-supplied `--type-override` remains the escape hatch.

### Three-tier framework

Per roadmap item 12's Tier classification:

- **Tier 1 — type-only.** Extension defines new column types whose values pass through as opaque bytes/text. ~50–100 LOC per extension. Examples: hstore, citext, ltree, cube.
- **Tier 2 — type + indexes.** Type plus index access methods (GIN, GiST, BRIN-via-extension, ivfflat, hnsw) operators rely on. ~150–300 LOC per extension. Examples: pgvector (ivfflat / hnsw), pg_trgm (gin operator classes), PostGIS (gist).
- **Tier 3 — type + functions in defaults / generated columns.** Extension-defined functions appear in `DEFAULT` clauses or generated-column expressions. Adds expression-translator catalog entries. Examples: uuid-ossp's `uuid_generate_v4()`, pgcrypto's `gen_random_uuid()`.

**v1 covers Tier 1 + 2.** Tier 3 deferred — `uuid-ossp` and `pgcrypto` are the v2 candidates per the research doc's surprises section (universal across all four hosted-PG providers, but the hard part is the function-default catalog, not the type passthrough).

### pgvector as the first concrete extension (Tier 2 fully exercised)

`pgvector` is the v1 leader because (a) it has the strongest demand trajectory (AI/ML adoption since 2023), (b) it covers Tier 2 fully — type (`vector(384)`) **and** index methods (`ivfflat`, `hnsw`) **and** index operator classes (`vector_l2_ops`, `vector_cosine_ops`, etc.) — and (c) the index-method machinery established here is what PostGIS will reuse when the v1 shortlist's last extension lands.

The framework is shaped so adding pg_trgm / hstore / citext / postgis is **"add a catalog entry"**, not "extend interfaces." Each catalog entry declares:

- **typesByName** — the (schema, typname) pairs the extension owns; the schema reader recognises columns of these types when the operator opts in via `--enable-pg-extension`.
- **emitColumn** — the writer's column-DDL renderer (e.g. `vector(384)` for pgvector with dimension; `geometry(POINT, 4326)` for PostGIS).
- **indexAccessMethods** — the list of access-method names the extension introduces (`ivfflat`, `hnsw` for pgvector; `gist` for PostGIS — though gist is core-PG so not an extension-only AM). Used to validate index-method passthrough.

### IR variant: `ir.ExtensionType`

Engine-neutral by name (`Extension` + `Name`); the binary representation is opaque to the IR. Modifiers carries optional type-modifier values (dimension count for pgvector; `[subtype, srid]` for postgis). The reader emits this; the writer recognises it and dispatches to the catalog's emitter.

```go
type ExtensionType struct {
    Extension string  // canonical extension name (e.g. "vector", "postgis", "hstore")
    Name      string  // canonical type name within the extension (e.g. "vector", "geometry")
    Modifiers []int   // optional type-modifier values (dimension, SRID, etc.)
}
```

Cross-engine targets receiving `ir.ExtensionType` get the same loud refusal as today's PostGIS Geometry path.

### Optional engine surface: `ir.ExtensionAware`

```go
type ExtensionAware interface {
    EnableExtensions(extensions []string) error
}
```

PG implements; MySQL does not. The pipeline orchestrator type-asserts on every freshly-opened source SchemaReader / RowReader / target SchemaWriter / RowWriter and threads the operator's `--enable-pg-extension` list through. Engines that don't implement skip cleanly (the structural assertion is the no-op gate).

### Pre-flight semantics

Three checks fire before any data moves:

1. **Flag parse.** `--enable-pg-extension EXT` validates `EXT` against the catalog's recognised set. Unknown name → refuse loudly with the recognised set listed.
2. **Source presence.** `EnableExtensions` queries `pg_extension` on the source. Extension missing on source → refuse loudly (operator typo / wrong DSN).
3. **Target presence.** Same check on the target side at writer-open. Missing on target → refuse loudly.

The operator-explicit flag means a no-op (extension enabled but no columns of that type exist on either side) is acceptable. The presence checks still fire — they catch the operator-typo case where the flag was misspelled or pointed at a wrong DSN.

### Threat model — five scenarios

1. **Operator enables an extension on source but target doesn't have it installed.** → Target-presence preflight refuses loudly at writer-open. Operator-actionable message: "install pgvector on the target via `CREATE EXTENSION vector;` and re-run."
2. **Extension version skew (pgvector 0.7 source → 0.5 target).** → Out of scope for v1. v1 checks presence only, not version. Documented as a caveat in v0.26.0 release notes; operator-supplied version-pinning would arrive in v2 if real-world drift causes pain.
3. **Operator enables `--enable-pg-extension EXT` but no columns of that type exist.** → No-op for the schema phase (the catalog lookup just doesn't fire on any column). Source-presence preflight still runs to catch the operator-typo case.
4. **Cross-engine target (PG → MySQL) with `--enable-pg-extension pgvector`.** → Refused at the existing cross-engine supportability check (`pipeline.checkCrossEngineSupportable`), which now refuses `ir.ExtensionType` columns regardless of the extension flag. The flag itself stays valid (the target-presence preflight runs against PG only); the refusal fires at schema-translation time before any DDL emits.
5. **Operator misspells extension name (`--enable-pg-extension pgvecotr`).** → Refused at flag-parse with the catalog's recognised set listed in the error.

### Why pgvector first

Per the research doc:

- **Demand trajectory.** ps-extensions.io's vote tracker shows pgvector with the highest demand among Tier 2 extensions providers haven't already shipped (AI/ML adoption).
- **Tier 2 lighthouse.** Establishes the index-method-passthrough machinery PostGIS will reuse — same catalog shape, just a different OID set + emitter.
- **Tractable scope.** ~200 LOC per the research doc's complexity estimate; manageable as a single chunk while still validating the framework.

## Consequences

### Positive

- PG → PG operators with pgvector workloads can migrate without dropping columns or hand-rolling type overrides.
- The framework's catalog shape means subsequent extensions in the v1 shortlist (pg_trgm, hstore, citext, postgis) land as catalog entries plus per-extension tests, not interface additions.
- Loud-failure default for cross-engine targets is preserved — operators who run PG → MySQL with vector columns still see the refusal they'd see today; the new flag doesn't open a silent-coercion path.

### Negative / known-deferred

- **Tier 3 not in v1.** Operators with `DEFAULT uuid_generate_v4()` or `DEFAULT gen_random_uuid()` columns still need `--expr-override` to translate the function-default. The Tier 3 catalog (uuid-ossp + pgcrypto) is the natural v2 chunk.
- **Allowlist vs auto-detect.** Per-extension flags are higher operator burden than "we have the same extensions, pass them through." The conservative shape is intentional; an `--auto-pg-extensions` opt-in escape hatch can land in v2 once the allowlist is battle-tested.
- **Version skew silent.** v1 checks extension presence, not version. A PostGIS 3.4 source → 3.0 target sync could surface subtle behaviour differences (e.g., GeoJSON output formatting, new operator additions). Documented; v2 can layer richer per-extension version checks.

## v0.26.0 status

Shipped in v0.26.0:

- **Framework.** `ir.ExtensionType` IR variant, `ir.ExtensionAware` optional engine surface, `pgExtensionCatalog` registry pattern, `--enable-pg-extension` CLI flag plumbed through migrate / sync start / preview / diff.
- **pgvector (Tier 2).** Type-only path (`vector(N)`) + index-method passthrough (`ivfflat`, `hnsw`). Operator classes captured for the index-recognition path.

Deferred to subsequent point releases:

- **pg_trgm** (Tier 2 lite — operator classes only, no new column type) **— shipped post-v0.29.1.** Validated the per-opclass passthrough path: `extensionDef.indexOperatorClasses` promoted from `[]string` metadata to `map[string]struct{}` queryable set; new `extensionOperatorClassEnabled` helper; schema reader's `populateIndexes` now consults both the `idx.Method != ""` (extension-AM) and the `extensionOperatorClassEnabled` (extension-opclass-on-core-AM) gates so `gin (col gin_trgm_ops)` round-trips without dropping the opclass. Cross-engine PG → MySQL refusal extended to refuse indexes with `ir.IndexColumn.OperatorClass` non-empty (the IR field is only populated for extension-owned opclasses by Bug 47 design — a clean signal without re-importing the engine catalog).
- **hstore** (Tier 1 — first opaque-text validation) **— shipped together with citext.** Tier 1 type-only entry: catalog `pgHstoreDef` declares the `hstore` udt; same-engine PG → PG emits the bareword `hstore` and round-trips values as PG-canonical text (`"k"=>"v"`). Cross-engine PG → MySQL gets a built-in default translator: `hstore` → MySQL `JSON`, with the writer's `prepareValue` reparsing the hstore wire form into a JSON object string at value-write time (`prepareHstoreToJSON` / `parseHstoreText` in `internal/engines/mysql/row_writer.go`). The flag-validate gate (`validateEnabledPGExtensions`) relaxes for hstore against non-PG targets via the new `ir.CrossEngineExtensionTranslator` optional engine surface — PG declares hstore + citext as cross-engine-translatable; other extensions preserve the strict refusal.
- **citext** (Tier 1 — text + collation) **— shipped together with hstore.** Tier 1 type-only entry: catalog `pgCiTextDef` declares the `citext` udt; same-engine PG → PG emits bareword `citext` and round-trips values byte-for-byte (server-side case-insensitive collation is a property of the type, not the wire format). Cross-engine PG → MySQL maps to `VARCHAR(255) COLLATE utf8mb4_0900_ai_ci` — the `_ai_ci` suffix is the load-bearing piece (without it, the case-insensitive comparison the operator relied on is silently lost). Operators wanting a non-default length use `--type-override`.
- **postgis** (Tier 2 — last in v1; coordinates with the existing GEOMETRY/SPATIAL roadmap entry's PG-side path)

Each subsequent extension is a separate point release; the v1 chunk's landing pattern is "add a catalog entry + integration tests, ship as a patch release." Tier 1 entries (hstore, citext) bundle two extensions per release where the implementation cost is dominated by the test-image + integration harness rather than per-extension code.

## Cross-engine policy (revisited per § 5 of research doc)

The original ADR-0032 status block declared "Same-engine target only" as the v1 scope. The hstore + citext PR refines this: a small carve-out admits two extensions whose cross-engine MySQL mappings are genuinely lossless and operator-helpful. The relaxation is **per-extension and engine-declared**, not blanket:

- **`ir.CrossEngineExtensionTranslator`** is a new optional engine surface. An engine implements `HasCrossEngineDefaultTranslator(name string) bool` declaring which of its extensions sluice can translate without operator intervention.
- **PG declares** `hstore` (→ MySQL JSON) and `citext` (→ MySQL VARCHAR with `_ai_ci` collation). `vector` / `pg_trgm` / `postgis` are not declared — their cross-engine semantics are ambiguous (vector storage on MySQL loses index support; pg_trgm has no MySQL counterpart; postgis is roadmap item 4's responsibility).
- **The pipeline's `validateEnabledPGExtensions`** consults the source engine's translator surface (when implemented) on the target-not-PG branch. Extensions with a declared translator pass through; others get the existing loud-failure refusal pointing at `--type-override`.
- **The MySQL writer's `emitColumnType`** carries the load-bearing rewrite directly — `ir.ExtensionType{Extension:"hstore"}` → `JSON`, `ir.ExtensionType{Extension:"citext"}` → `VARCHAR(255) COLLATE utf8mb4_0900_ai_ci`. The pipeline's `checkCrossEngineSupportable` exempts these from refusal via the helper `isCrossEngineTranslatablePGExtension` (kept inline in the pipeline package to avoid cross-package coupling; the small static list must stay in lock-step with the catalog's `crossEngineDefaultTranslatedExtensions`).

The carve-out preserves the opt-in tenet — operators still pass `--enable-pg-extension hstore` (or citext) explicitly; the translator just kicks in on non-PG targets when the source engine declares it. Operators wanting non-default cross-engine shapes (e.g. citext → MySQL `TEXT` instead of VARCHAR) supply `--type-override` per column as before.

## References

- Roadmap entry 12 ([`docs/dev/roadmap.md`](../dev/roadmap.md))
- Research doc ([`docs/research/pg-extensions-deployment-frequency.md`](../research/pg-extensions-deployment-frequency.md))
- ADR-0001 (IR-first translation) — the IR-variant principle this builds on
- ADR-0002 (Sealed interfaces) — `Type` interface remains sealed; `ExtensionType` is package-internal
- ADR-0016 (Layered expression translation) — Tier 3 (function-in-defaults) will reuse this in v2
