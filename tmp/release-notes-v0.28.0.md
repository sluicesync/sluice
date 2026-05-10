# sluice v0.28.0

**PostGIS-aware GEOMETRY/SPATIAL translation closes Bug 26 + Bug 27.** Sluice's IR has carried `ir.Geometry` since v0.1.0, but cross-engine PG ↔ MySQL geometry has been refused at the schema-write boundary (PG: `GEOMETRY requires PostGIS`; MySQL: SRID dropped). v0.28.0 lifts the refusal: PostGIS-detected PG targets accept `ir.Geometry` columns and emit `geometry(<subtype>, <srid>)`; MySQL targets emit `<type> SRID <n>` (MySQL 8.0+ syntax) preserving the SRID; cross-engine PG → MySQL round-trip closes Bug 26. The VStream-specific 4-byte SRID prefix on POINT bytes is now stripped + captured (closes Bug 27); vanilla MySQL protocol path unchanged. ADR-0035 documents the design.

## Fixed

- **Bug 26 — PostGIS SRID dropped on cross-engine schema translation.** `ir.Geometry.SRID` field carries the SRID from PG's `geometry_columns` view through translation. PG schema writer emits `geometry(<subtype>, <srid>)` when PostGIS is detected (init-time `SELECT 1 FROM pg_extension WHERE extname = 'postgis'`); without PostGIS, the existing loud refusal is preserved. MySQL `emitColumnDef` appends `SRID <n>` when `ir.Geometry.SRID != 0` (MySQL 8.0+ syntax). WKB → EWKB conversion at PG write-time injects the SRID's 4-byte prefix; EWKB → WKB on PG read-time captures the SRID for cross-engine flow.

- **Bug 27 — VStream POINT bytes mis-parsed (4-byte SRID prefix).** `decodeVStreamCell` now splits `query.Type_GEOMETRY` from the binary fall-through and strips the 4-byte little-endian SRID prefix; under-5-byte payloads pass through. Vanilla MySQL protocol path unaffected (it strips the prefix correctly already). Unit test fixture (`TestDecodeVStreamCellGeometry`) covers SRID 4326, malformed short, SRID 0; full PlanetScale Vitess round-trip remains an operator-run check via `psverify`.

## Features

- **`integration postgis` build tag** for PostGIS-bearing integration tests. CI's Integration job pre-pulls `postgis/postgis:16-3.4` and runs a separate test step against it. Image-pull cost (~500 MB layer) is one-time per cache.

- **Cross-engine PostGIS round-trip integration tests.** `TestMigrate_PostGIS_PGToMySQL` (Phase C reverse direction; closes Bug 26's load-bearing pin) joins the existing `TestMigrate_PostGIS_MySQLToPG`. Both test the full read → translate → write loop with a real PostGIS target (or source).

- **ADR-0035 — PostGIS-aware GEOMETRY/SPATIAL support.** Decision rationale, EWKB ↔ MySQL-prefix conversion detail, 5-scenario threat model (PostGIS-absent target, SRID mismatch, VStream prefix on vanilla MySQL connections, EPSG SRID unrecognized, MySQL → PG cross-engine SRID drift), explanation of why this is parented under roadmap item 6 (cross-engine focus) rather than item 11 (PG → PG extension passthrough — PostGIS PG-to-PG follows naturally now that the writer emit path lands).

## Use cases this unlocks

| Scenario | Before v0.28.0 | With v0.28.0 |
|---|---|---|
| **PG → MySQL migration of mapping/geo apps with PostGIS** (mapping platforms, IoT geofencing, location-aware SaaS) | Schema writer rejected geometry columns with loud error; operator manually rewrote schemas to `bytea`/blob and lost spatial query semantics on the target. | Geometry columns translate cleanly: `geometry(Point, 4326)` source → MySQL `POINT SRID 4326` target with values byte-for-byte preserved. |
| **MySQL → PG migration with PostGIS-installed target** | Sluice never reached the geometry write path because translation refused upstream. | PostGIS-detected PG target accepts `ir.Geometry`; emits the right type + SRID. |
| **PlanetScale Vitess source with POINT columns** (hosted MySQL with VStream protocol) | sluice's WKB decoder bailed with `wkb has unknown byte-order flag 0xe6` because VStream prepends a 4-byte SRID before standard OGC WKB. | VStream-specific decoder strips the prefix, captures the SRID, parses the remainder as standard WKB. |

## Compatibility

- **No format-breaking changes.** Manifest schema, change-chunk format, CLI surface — all unchanged for existing flows.
- **PG targets WITH PostGIS installed: behavior changed.** Previously the schema writer refused all `ir.Geometry` columns. Post-v0.28.0, the writer emits `geometry(<subtype>, <srid>)`. Operators who relied on the loud-failure to bail on geometry columns should review whether they want the new automatic emission (probably yes — that's the load-bearing fix) or set `--type-override` on those columns to `bytea` for the previous opaque-bytes shape.
- **PG targets WITHOUT PostGIS: behavior unchanged** — loud refusal preserved.
- **MySQL targets: behavior unchanged** beyond SRID preservation in the emit shape.
- **Drop-in upgrade from v0.27.0.** No DDL migration on `sluice_cdc_state`; no operator action required.

## Known limitations

- **VStream Phase B end-to-end verification is operator-run via `psverify`.** The unit-test fixture proves the byte-level fix; the full PlanetScale Vitess source → sluice → target round-trip needs the operator's PlanetScale credentials. Operators using PlanetScale Vitess sources with PostGIS-shaped POINT columns should verify the round-trip manually before relying on the v0.28.0 fix in production.

- **EPSG SRID handling is "common subset" only.** PG's `spatial_ref_sys` table has thousands of SRIDs; MySQL has hard-coded mappings for a smaller subset. Sluice doesn't enforce SRID-existence checks at translation time; an unrecognized SRID will surface as a target-side error at INSERT time (the loud-failure tenet). Operators using non-EPSG-common SRIDs should test their workload before committing.

- **PG-to-PG PostGIS passthrough lands by side-effect, not via the v0.26.0 PG extension passthrough framework.** Roadmap item 11 (PG extension passthrough framework) shipped pgvector as the first concrete extension in v0.26.0; PostGIS would naturally fit as a catalog entry there. v0.28.0 ships PostGIS via the cross-engine path under item 6 instead — both PG-to-PG passthrough AND cross-engine PG ↔ MySQL work, but the explicit `--enable-pg-extension postgis` flag isn't yet wired. Future point release will fold PostGIS in as a v0.26.0-framework catalog entry alongside pg_trgm / hstore / citext.

## Test coverage

- **Unit tests**:
  - `TestDecodeVStreamCellGeometry` (3 sub-cases: SRID 4326, malformed short, SRID 0) — Bug 27 byte-level fix.
  - 3 new geometry SRID emission cases in `ddl_emit_test.go` — MySQL 8.0+ `SRID <n>` syntax.
  - 3 cross-engine-supportable tests inverted from "refuses" to "allowed" — confirms the `unsupportablePGtoMySQL` narrowing.
- **Integration tests** (`integration postgis` build tag):
  - `TestMigrate_PostGIS_PGToMySQL` (NEW) — Phase C reverse direction; closes Bug 26's load-bearing pin.
  - `TestMigrate_PostGIS_MySQLToPG` (existing) — forward direction.

## Who needs this

- **Geo / mapping / IoT operators** running PostGIS on PG who've been blocked from cross-engine migrations or backups by the geometry-refusal.
- **PlanetScale Vitess operators** with POINT columns — Bug 27's VStream prefix handling closes the protocol-level mis-parse.
- **Anyone planning Phase 2.5 VStream work** (item 4 follow-on) — v0.28.0 establishes the VStream-specific decode pattern that other binlog-vs-VStream encoding differences will reuse.

## What's next

- **Roadmap item 3 — Mid-stream Phase 2 strict zero-loss correctness.** ADR-0033 documented Path A (slot-pause) doesn't work; awaiting direction on Path B (dual-slot) / Path C (operator quiesce) / Path D (diagnose actual residual loss surface first).
- **Roadmap item 5 — Multi-source aggregation Shape A (sharded → consolidated).** Demand-driven; Vitess shards → PG analytics is the canonical use case.
- **Roadmap item 7 — Backup chunk compression investigation.** Survey klauspost/compress vs stdlib gzip; benchmark + decision doc.
- **Roadmap item 8 — Analytics-friendly source research doc.** Parquet export + DuckDB + Arrow Flight; pre-code research chunk.
- **PG extension passthrough catalog: pg_trgm, hstore, citext, PostGIS as catalog-only follow-ons** to v0.26.0's framework. Each ships as a single point release per the v1 shortlist.
- See `docs/dev/roadmap.md` for the full queue.
