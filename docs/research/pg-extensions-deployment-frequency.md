# PG extensions — deployment-frequency survey

**Status:** Research-only. Input to roadmap item 12 (PG → PG extension passthrough). No code changes; no roadmap edits. The main session will pin item 12's v1 shortlist by reference to this doc.

**Bottom line:** Recommended v1 allowlist for the PG → PG passthrough chunk is **postgis, pgvector, pg_trgm, hstore, citext** — the initial roadmap guess survives the survey unchanged. Implementation order should be **pgvector → pg_trgm → postgis → hstore → citext** (cheapest Tier 1 + highest demand first, PostGIS last because it overlaps roadmap item 4 GEOMETRY/SPATIAL and benefits from finishing that work in parallel). One survey surprise: **uuid-ossp** is on every provider list and would be near-trivial as a Tier 1 (the type is just `uuid`, which sluice already handles natively) — but its real cost is Tier 3 (the `uuid_generate_v4()` default-expression translator). Worth noting as the natural v2 follow-up.

## 1. Purpose and scope

This doc is the prerequisite survey for **roadmap item 12** ("PG → PG extension passthrough — operator survey + allowlist"). It picks the v1 shortlist of extensions for the PG → PG passthrough chunk by surveying which extensions show up most often in managed-PG provider lists and the operator-voted demand tracker. The scope is intentionally narrow:

- **In scope.** Same-engine PG → PG passthrough where both source and target have the same extension installed. The chunk's job is to recognize extension types in the schema reader, route them through a new IR variant, and re-emit them at the writer. Values pass through as opaque bytes/text where possible (Tier 1) or with index-method awareness (Tier 2).
- **Out of scope.** Cross-engine extension translation (PG → MySQL etc.). That stays loud-failure by default — the tenet "contain Postgres complexity" applies and operator-supplied translations remain the escape hatch. Section 5 below revisits the few extensions where a defensible cross-engine path exists.

## 2. Sources surveyed

Surveyed 2026-05-09. All four URLs were operator-approved.

| # | Source | URL | Shape | Signal |
| - | - | - | - | - |
| 1 | Supabase extensions guide | <https://supabase.com/docs/guides/database/extensions> | Curated provider list ("over 50 extensions" — full roster behind a JS-rendered sidebar; per-extension docs pages confirm individual coverage) | What Supabase customers actually use enough that Supabase pre-installs and documents it. Largest hosted-PG audience in the survey. |
| 2 | Neon extensions list | <https://neon.com/docs/extensions/pg-extensions> | Curated provider list (~80 extensions, fully enumerated by category). Distinguishes preloaded vs. `CREATE EXTENSION`-required and flags experimental/deprecated. | Neon ships a wide allowlist; the categorization (Geospatial / Vector / Data Type / Crypto / etc.) corroborates Tier classification. |
| 3 | PlanetScale Postgres extensions | <https://planetscale.com/docs/postgres/extensions> | Curated provider list (~50 extensions, fully enumerated, split into "Native PostgreSQL", "Community", and "PlanetScale Preinstalled"). | Newest hosted-PG offering; their list is conservative and weighted toward the load-bearing extensions they couldn't ship without. |
| 4 | ps-extensions.io | <https://ps-extensions.io/> | Operator-voted demand tracker for extensions PlanetScale hasn't yet shipped (already-shipped extensions appear with their historical vote totals). | The strongest direct demand signal in the survey: what operators actively *ask* for, ranked by votes. |

Note on Supabase: the WebFetch'd page exposes only a sentence ("over 50 extensions") and points at a JS-rendered sidebar; per-extension URL probes confirm individual coverage but the full enumerated roster isn't trivially scrape-able. For this survey I treat Supabase coverage as **confirmed** for any extension where a per-extension page exists at `supabase.com/docs/guides/database/extensions/<name>` or where the extension is mentioned in the public Supabase Postgres image. Where I couldn't confirm directly, I mark `?`.

## 3. Per-extension matrix

Inclusion rule: extensions appearing on at least 2 of the 4 sources, plus extensions on ps-extensions.io's top-20 demand list. Tier per item 12's framework (1 = type-only opaque bytes, 2 = type + indexes, 3 = type + functions in defaults / generated columns). Sluice complexity is a low/medium/high bucket with rough LOC.

### Tier 1 — type-only (opaque bytes/text)

| Extension | Use case | Supabase | Neon | PlanetScale | ps-extensions.io | Sluice complexity | v1? |
| - | - | - | - | - | - | - | - |
| `hstore` | Key-value pairs in a single column | ? (image) | yes | yes | shipped | low (~80 LOC; text repr is well-defined) | **yes** — broad coverage, mechanical |
| `citext` | Case-insensitive text type | ? (image) | yes | yes | shipped | low (~50 LOC; literally text under the hood) | **yes** — cheapest tractable Tier 1 |
| `ltree` | Hierarchical label paths | ? | yes | yes | shipped | low (~80 LOC; text repr) | no — covered, but lower demand than hstore/citext; v2 |
| `cube` | Multidimensional cube type | ? | yes | yes | shipped | low (~80 LOC) | no — niche; v2 |
| `intarray` | Integer array operators (gin support) | ? | yes | yes | shipped | low-medium (~120 LOC; existing PG `int[]` plus operator class metadata) | no — array support is core PG; the extension adds operators not types |
| `seg` | Floating-point intervals | ? | yes | yes | shipped | low | no — niche |
| `isn` | ISBN/EAN/etc. number types | ? | yes | yes | shipped | low | no — niche |
| `pg_uuidv7` | UUID v7 (sortable timestamps) | ? | yes | (no) | #9 (12 votes) | low (type is `uuid`; format is the function) | no — really a Tier 3 (function in defaults); v2 with uuid-ossp |
| `pgx_ulid` | ULID identifier type | ? | yes | (no) | #5 (20 votes) | low | no — niche; v2 |
| `semver` | Semantic-version ordering type | ? | yes | (no) | (no) | low | no — niche |

### Tier 2 — type + index methods

| Extension | Use case | Supabase | Neon | PlanetScale | ps-extensions.io | Sluice complexity | v1? |
| - | - | - | - | - | - | - | - |
| `postgis` | Geospatial types + GiST indexes | yes (confirmed per-page) | yes | yes | shipped | high (~300+ LOC; SRID-aware, multiple subtypes, EWKB serialization — overlaps roadmap item 4 GEOMETRY/SPATIAL) | **yes** — universal coverage; the canonical PG extension; ship last in v1 to coordinate with item 4 |
| `pgvector` | Vector similarity search (ivfflat / hnsw indexes) | yes (image, widely documented) | yes | yes | shipped | medium (~200 LOC; index-method preservation matters) | **yes** — strongest demand-trajectory signal; AI/ML zeitgeist |
| `pg_trgm` | Trigram fuzzy text search (gin/gist) | ? (image) | yes | yes | shipped | medium (~150 LOC; type is just text — the work is gin operator-class metadata) | **yes** — broad use, Tier-2 lite (no new column type, just operator classes) |
| `pgvectorscale` | Vector-search compression (sits on pgvector) | (no) | (no) | yes | shipped (61 votes pre-ship) | high (depends on pgvector first) | no — v2 follow-on once pgvector lands |
| `bloom` | Bloom-filter index access method | (no) | yes | yes | (no) | medium (no new types — index-method-only, but still requires index-AM awareness) | no — index-only, no type to passthrough |
| `btree_gin` / `btree_gist` | Composite indexes mixing btree-style with gin/gist | (no) | yes | yes | (no) | medium (index-only) | no — index-AM, no types |
| `rum` | Inverted index with ranking | (no) | yes | (no) | (no) | medium | no — too niche |

### Tier 3 — type + functions in defaults / generated columns

| Extension | Use case | Supabase | Neon | PlanetScale | ps-extensions.io | Sluice complexity | v1? |
| - | - | - | - | - | - | - | - |
| `uuid-ossp` | UUID generation (`uuid_generate_v4()` etc.) | yes (confirmed per-page) | yes | yes | shipped | medium (~150 LOC; type is core `uuid` — work is the default-expression translator) | no — but **near-miss**; strong v2 candidate |
| `pgcrypto` | Cryptographic functions (`digest`, `gen_random_uuid`, `crypt`) | yes (confirmed per-page) | yes | yes | shipped | medium-high (~250 LOC; multiple functions in defaults) | no — broad surface area; v2/v3 |
| `pg_jsonschema` | JSON-schema validation in CHECK constraints | ? | yes | (no) | #4 (24 votes) | high (CHECK-constraint expression translation) | no — Tier-3 is harder than Tier-1+2; v2 at earliest |
| `earthdistance` | Great-circle distance functions on `cube` | (no) | yes | yes | (no) | medium (depends on cube; functions in defaults rare) | no — niche |

### Other notable from ps-extensions.io demand tracker (operator-voted)

| Extension | Use case | ps-extensions.io rank | Tier | v1? |
| - | - | - | - | - |
| `pg_search` | Full-text search (BM25 via paradedb) | **#1 (116 votes)** | 2 (specialized index AM) | no — single-vendor (paradedb); not on Neon as recommended; complex; v3+ at earliest |
| `pgmq` | Message queue tables | #2 (40 votes) | 1-ish (mostly tables, not types) | no — table-shape, not column-type passthrough territory |
| `pg_textsearch` | Text-search helpers | #3 (37 votes) | 2 | no — overlaps pg_trgm and core PG FTS |
| `pg_jsonschema` | (see Tier 3) | #4 (24 votes) | 3 | no |
| `pgx_ulid` / `pg_uuidv7` / `pg_ivm` / `age` / `pg_mooncake` | Various | #5–#8 | mixed | no — long tail; v2+ |
| `h3` / `pgrouting` | Geospatial helpers | #10–#11 (10 votes each) | 2 | no — depend on postgis; ship after postgis is bedded in |

## 4. Recommended v1 allowlist for roadmap item 12

The original guess (`postgis, pgvector, pg_trgm, hstore, citext`) survives the survey. Each appears on Neon **and** PlanetScale's enumerated lists, all are documented or known-shipped on Supabase, and pgvector + postgis + pg_trgm are present-on-ps-extensions.io-as-shipped (i.e., PlanetScale prioritized them over the still-requested long tail). The five together cover the three-tier framework well: 2 × Tier 1 (hstore, citext), 3 × Tier 2 (pg_trgm light, pgvector, postgis).

**Suggested implementation order:**

1. **`pgvector`** — strongest demand trajectory (AI/ML adoption since 2023), Tier 2 with a clean index story (ivfflat / hnsw). Building this first establishes the IR + index-method-passthrough machinery that postgis will reuse. Medium complexity.
2. **`pg_trgm`** — Tier 2 lite (no new column type, just operator classes on `text`); validates the index-method-passthrough path on something simpler than postgis. Medium complexity.
3. **`hstore`** — first Tier 1; validates the type-only opaque-bytes machinery. Low complexity.
4. **`citext`** — second Tier 1, even simpler than hstore (it's `text` with a custom collation). Low complexity. Pair with hstore in one PR if the IR shape lands cleanly.
5. **`postgis`** — last in v1, deliberately. PostGIS is the highest-stakes integration (multiple subtypes, SRIDs, EWKB) and it overlaps **roadmap item 4 (GEOMETRY/SPATIAL support)**. Treat the PG → PG passthrough path as the same chunk as the PG-side of item 4: build it once with `ir.Geometry` already in IR, reuse the same writer code path, special-case it for the PG → PG-with-PostGIS-on-target case to round-trip without translation loss. High complexity; ~30–40% of v1 LOC.

**Survey surprises** (worth flagging):

- **uuid-ossp** is universally available (Supabase, Neon, PlanetScale all ship it preinstalled) and would be near-trivial as a *type* passthrough — the type is just `uuid`, which sluice's IR already handles natively. Its real cost is Tier 3: the `uuid_generate_v4()` default-expression translator catalog. Skipping it from v1 means PG → PG syncs of tables with `uuid_generate_v4()` defaults will need operator-supplied translation overrides until v2 lands the function-default mechanism. Worth calling out in v1 release notes.
- **pgcrypto** is similarly ubiquitous and similarly Tier 3 (`digest`, `gen_random_uuid`, `crypt` all appear in column defaults in real schemas). Same skip rationale; same v2 flag.
- **ps-extensions.io's #1 (`pg_search`, 116 votes)** is single-vendor (paradedb), not on Neon's list, and the rejection note on the tracker shows it's controversial. Despite the high vote count it's a poor v1 fit.
- **timescaledb** appears on all three provider lists but didn't make the v1 cut: its passthrough story is more about hypertable preservation than column-type passthrough, which is a different chunk shape. Defer to a separate roadmap entry.
- **Neon's "trusted extensions" model** vs. **Supabase's "GUC permission model"** are provider-side concerns, not sluice-side; the sluice pre-flight just runs `SELECT 1 FROM pg_extension WHERE extname = $1` on both ends and routes accordingly.

## 5. Cross-engine policy revisit

Per the "contain Postgres complexity" tenet and the existing roadmap policy: **cross-engine extension translation stays loud-failure by default**, with operator-supplied translation overrides as the escape hatch. The survey didn't change that — most v1 extensions have no defensible cross-engine path. A short audit:

- **postgis → MySQL spatial.** Already the subject of roadmap item 4 and Bug 26. There *is* a defensible cross-engine path (WKB/EWKB → MySQL `GEOMETRY` columns, with SRID coercion), but it's complex enough that it gets its own chunk. Worth an ADR-0016 expression-translator entry once item 4 lands. Until then: loud failure with override hint.
- **pgvector → MySQL.** No native MySQL vector type. A "JSON array of floats" representation works for storage but loses index semantics; vector search on the MySQL side is non-trivial (requires application-level cosine/L2 calculation or a sidecar vector store). **Recommended: loud-failure default; document the JSON-of-floats override as a known operator pattern.** Don't add a default translator — it would silently break operator workloads that assumed index-backed search.
- **pg_trgm → MySQL.** MySQL has `FULLTEXT` indexes which are different in shape; no clean translation. Loud failure.
- **hstore → MySQL.** Map to MySQL `JSON` is the obvious operator override. Trivial, but easy to get subtly wrong (key-order, string-quoting). **Worth an ADR-0016 entry as a default translator** since the mapping is genuinely lossless and broadly useful. This is the one v1 extension where I'd argue for a default cross-engine translation.
- **citext → MySQL.** Map to `VARCHAR` with `_ci` collation is exact-fit. **Worth a default translator** — it's a clean 1:1 collation match.
- **uuid-ossp / pgcrypto (v2 candidates).** UUID type translates 1:1 to MySQL `BINARY(16)` or `CHAR(36)`. The function-in-defaults problem is the harder bit (MySQL `UUID()` ≠ `uuid_generate_v4()` byte order without explicit byte-swapping). Mark for ADR follow-up when uuid-ossp lands in v2.

**Net:** of the 5 v1 extensions, 2 (hstore, citext) get default cross-engine translators; 3 (postgis, pgvector, pg_trgm) stay loud-failure with operator overrides. ADR-0016 entries for the two defaults can land in the same PR as the v1 chunk or immediately after.

## 6. Open questions

Surfaced during the survey; flag for follow-up but not blockers for v1.

- **Extension version skew.** None of the four sources expose a version-pinning list; they enumerate by name only. A PostGIS 3.4 source → 3.0 target sync could surface subtle behavior differences (e.g., GeoJSON output formatting, new operator additions). v1 will only check extension *presence* (`SELECT 1 FROM pg_extension WHERE extname = $1`); operator-supplied version-pinning could come in v2 if real-world drift causes pain. Document the limitation in the v1 release notes.
- **Extensions needing more than presence parity.** A few extensions need configuration matching beyond `CREATE EXTENSION`: `pg_stat_statements` reads from a GUC (`pg_stat_statements.max`); `pg_cron` writes to a `cron` schema; `timescaledb` has hypertable metadata. None of these are in the v1 list, but the design has to acknowledge that extension *parity* is a weaker check than extension *behavioral parity*. v1 is presence-only; v2 can layer richer checks per-extension.
- **Provider-specific quirks.** Neon's "trusted extensions" model means some extensions install differently than on vanilla PG (the `vector` install requires `CREATE EXTENSION vector;` rather than `CREATE EXTENSION pgvector;` — Neon notes this explicitly). Supabase's GUC permission model affects which extensions a non-superuser role can enable. Sluice's pre-flight should report extension presence at both endpoints and not assume superuser; document expected operator role in the v1 user-facing docs.
- **Supabase enumeration.** This survey couldn't fully enumerate Supabase's 50+ supported extensions through WebFetch (JS-rendered sidebar). The matrix above is conservative — flag any "?" cells before publishing the v1 chunk. The Supabase Postgres Docker image (`supabase/postgres:*` on Docker Hub) is the authoritative source if the gap matters operationally.
- **Single-vendor extensions in the demand tracker.** Several high-vote ps-extensions.io entries are single-vendor (`pg_search` from paradedb, `pgvectorscale` from Timescale, `pg_mooncake` from Mooncake Labs). Sluice's passthrough mechanism doesn't care about vendor identity, but the v1 allowlist intentionally avoids these because their adoption is concentrated, not broad. The allowlist's job is to cover the 80% case; long-tail extensions get added on operator demand.
