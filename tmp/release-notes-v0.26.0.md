# sluice v0.26.0

**PG → PG extension passthrough framework + pgvector.** Postgres' extensibility — pgvector for vector embeddings, PostGIS for geospatial, hstore / citext / pg_trgm for text fuzziness, etc. — has been treated as hostile by sluice's IR up to now (loud-failure refusal at schema-read for unrecognized custom types). v0.26.0 lands the framework that flips this for same-engine PG → PG syncs where the operator opts in: `--enable-pg-extension EXT` (repeatable) tells sluice to round-trip column types defined by named extensions with native fidelity, including their index methods. Cross-engine targets (PG → MySQL) keep the loud-failure default; explicit operator translations (`--type-override`) stay the escape hatch. **v1 ships pgvector** as the first concrete extension — establishes both the type-only path (Tier 1) and the index-method path (Tier 2: `ivfflat` and `hnsw`). Subsequent extensions from the v1 shortlist (pg_trgm, hstore, citext, PostGIS) ship as catalog-only follow-ons; the framework stays put. ADR-0032 documents the design; the v1 shortlist is pinned by the deployment-frequency survey at `docs/research/pg-extensions-deployment-frequency.md`.

## Features

- **`--enable-pg-extension EXT` flag (repeatable)** on `migrate`, `sync start`, `schema preview`, `schema diff`. Default empty (today's behavior — extension types refuse loudly). When set, sluice:
  1. Validates each name against the recognized-extensions catalog at flag-parse time (refuses unknown names with the recognized set in the error — catches operator typos before any DB connection).
  2. Preflights against the source DB (`SELECT extname FROM pg_extension WHERE extname = ANY(...)`) at construction time to ensure the extension is actually installed. Same preflight runs on the target.
  3. Schema reader recognizes column types whose OID matches a catalog entry, emits `ir.ExtensionType` variants.
  4. Schema writer renders these via the catalog's `emitCol` function on same-engine PG targets; refuses cleanly on cross-engine MySQL targets with operator-actionable error pointing at `--type-override`.

- **pgvector — first concrete extension supported.** Both Tier 1 (type-only opaque-text/binary) and Tier 2 (index methods) covered:
  - **`vector(N)` column type.** Dimension preserved end-to-end. Bulk-copy uses a registered pgvector binary codec (`int16 dim, int16 unused, dim × BE float32` per pgvector's wire format) — the naive text-passthrough approach fails because pgx's binary COPY protocol parses the first two bytes of the value as a dimension count, which would interpret `[0.1,0.2...` as a 23344-dimension vector and trip pgvector's 16000-dim ceiling. The codec is registered per-connection in `writeViaCopy` when the table has any vector column; the OID is resolved from `pg_type` at registration time.
  - **`ivfflat` and `hnsw` index methods** recognized and recreated on the target. The `ir.Index.Method` field carries verbatim extension-introduced access-method names.
  - **Operator classes emitted** for indexes whose access method is extension-introduced and requires them (`hnsw` needs `vector_l2_ops` / `vector_ip_ops` / `vector_cosine_ops` / `vector_l1_ops`). New `ir.IndexColumn.OperatorClass` field, populated by a `pg_index/pg_opclass` join. Default-PG indexes (B-tree, hash, etc.) emit unchanged.

- **Three-tier classification framework** (per ADR-0032). Each pinned tier has predictable LOC cost per extension added:
  - **Tier 1 — type-only opaque bytes/text.** ~50-100 LOC per extension. Examples: hstore, citext, ltree.
  - **Tier 2 — type + index methods.** ~150-300 LOC per extension. Examples: pgvector (shipped), pg_trgm, PostGIS.
  - **Tier 3 — type + functions in defaults.** Adds expression-translator catalog entries. Examples: uuid-ossp, pgcrypto. Deferred to v2 per ADR-0032 §"Consequences."

- **`ir.ExtensionType` IR variant + `ir.ExtensionAware` optional engine surface.** Engine-neutral by name (Extension + Name); modifiers carry per-type metadata (e.g. `vector(384)` → Modifiers=[]int{384}). PG implements `ExtensionAware`; MySQL doesn't (no extension concept in the same shape — MySQL's "feature flags" are server-level, not type-defining). Structural type-assertion skips cleanly.

- **PG extension catalog (`internal/engines/postgres/extension_catalog.go`).** Registry mapping extension name → recognized type OIDs + emit functions + index access methods. Adding a new extension is "add a catalog entry," not "extend interfaces." pgvector ships as the first entry; pg_trgm / hstore / citext / PostGIS planned as catalog-only follow-ons in subsequent point releases per the v1 shortlist.

## Use cases this unlocks

| Scenario | Before v0.26.0 | With v0.26.0 |
|---|---|---|
| **AI/ML PG → PG sync with pgvector embeddings** | Schema reader rejects vector columns with loud refusal; operator either drops the column or rewrites it to `bytea` (loses query semantics). | `--enable-pg-extension vector` → sluice round-trips `vector(N)` columns + ivfflat/hnsw indexes natively. |
| **Same-engine PG sync between two managed providers** (Supabase → Neon, etc.) | Any extension column blocks the migrate. Operator manually rewrites schemas. | Both sides have the same extensions installed → opt in, sluice handles the rest. |
| **Cross-engine PG → MySQL with extension columns** | Loud refusal (today's behavior). | Still loud refusal — `--type-override` remains the operator escape hatch. The new flag is intentionally same-engine only. |

## Compatibility

- **No format-breaking changes.** Manifest schema, change-chunk format, CLI surface — all unchanged for existing flows.
- **Default behavior unchanged.** Operators not using `--enable-pg-extension` see no behaviour change — extension column types continue to refuse loudly at schema-read.
- **Drop-in upgrade from v0.25.1.** No DDL migration on `sluice_cdc_state`; no operator action required.
- **MySQL operators unaffected.** MySQL doesn't implement `ir.ExtensionAware`; the structural type-assertion skips cleanly. Cross-engine PG → MySQL with `--enable-pg-extension` enabled still refuses cleanly at the cross-engine retarget step (`--type-override` remains the operator escape hatch).

## Known limitations

- **Extension version skew not detected.** v1 checks extension presence on both source and target, NOT version compatibility. pgvector 0.7 source → 0.5 target may surface subtle behaviour gaps that sluice doesn't see. Documented in ADR-0032's threat model item 2; future refinement could add `--enable-pg-extension vector@>=0.7` syntax if real operator demand surfaces.
- **Operator-class emission scoped to extension AMs.** `hnsw` indexes correctly emit their required operator class. Built-in PG access methods (B-tree, hash, GIN/GiST with built-in opclasses) emit unchanged. Future extensions requiring custom operator classes need a catalog entry update.
- **Tier 3 extensions deferred.** uuid-ossp + pgcrypto are universal across all four surveyed providers (Supabase, Neon, PlanetScale Postgres, ps-extensions.io) but are Tier 3 (function-in-defaults work). Strong v2 candidates after the v1 Tier 1+2 machinery is in place. Tracked in ADR-0032 §"Consequences."
- **Per-extension v1 shortlist not all shipped in v0.26.0.** This release lands pgvector + the framework. pg_trgm, hstore, citext, and PostGIS are planned as catalog-only follow-ons (each likely a single point release). Roadmap item 12 has the implementation order pinned by the research doc.

## Test coverage

- **Unit tests**: `ir.ExtensionType` round-trip; tier classification; PG extension catalog shape (typesByName / typesByOID / emitCol with dimension); cross-engine target refusal (MySQL writer rejects `ir.ExtensionType` with operator-actionable error); preflight refusals (source missing extension; target missing extension); flag-parse refusal of unknown extension names.
- **Integration tests** (gated `//go:build integration`, against `pgvector/pgvector:0.7.4-pg16`):
  - PG → PG migrate of a `vector(384)` column — bulk-copy round-trip + INSERT-with-vector-value
  - Same with an `ivfflat` index — index recreated on target
  - Same with an `hnsw` index
  - Source has pgvector, target doesn't → preflight refuses with operator-actionable error
  - Schema reader on a vector column WITHOUT `--enable-pg-extension vector` → existing loud-failure path preserved (don't silently drop)

## Who needs this

- **AI/ML PG operators using pgvector for embeddings.** Pre-v0.26.0 sluice couldn't migrate or stream vector-bearing tables; this is the unlock.
- **Operators consolidating multi-tenant PG instances** where extensions are part of the schema (not just bytea/text fallbacks).
- **Anyone planning to migrate to/from Supabase/Neon/PlanetScale Postgres** — all three providers ship a curated extension catalog; the v1 shortlist exactly matches the most-deployed Tier 1+2 extensions across them.

## What's next

- **Roadmap item 12 v1 shortlist remainder** — pg_trgm, hstore, citext, PostGIS as catalog-only follow-ons. Each ships as a single point release; the framework is stable.
- **Roadmap item 3 — Mid-stream Phase 2 strict zero-loss correctness.** Closes v0.24.0's best-effort gap (slot-pause path is the lighter implementation).
- **Roadmap item 4 — MySQL Phase 2 mid-stream live add-table.** Different mechanism (table-filter flip vs publication scope).
- **Roadmap item 7 — GEOMETRY/SPATIAL support** — closes Bugs 26/27; PostGIS for both PG-to-PG (parented under item 12) and cross-engine (parented under item 7).
- See `docs/dev/roadmap.md` for the full queue.
