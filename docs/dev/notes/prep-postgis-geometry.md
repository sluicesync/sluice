# Prep: PostGIS-aware GEOMETRY translation

> **Status: SHIPPED in v0.28.0** (initial PG → MySQL geometry round-trip with SRID); refined through v0.33.3 (Bug 51 / 52 / 53 closure on PG `geography(POINT, SRID)` and Z/M dimensional variants). Canonical reference: [ADR-0035](../../adr/adr-0035-postgis-geometry-spatial-support.md).

Roadmap reference: not in the original roadmap. Surfaces from the post-walkthrough conversation about handling sakila's `address.location` (GEOMETRY) cleanly when the PG target has PostGIS enabled. Loosely depends on [prep-mappings-config-wiring.md](prep-mappings-config-wiring.md) being merged for SRID overrides.

## Goal

Today the PG engine declines GEOMETRY emission with:

```
postgres: column "location": postgres: GEOMETRY requires PostGIS;
not supported in this writer version
```

That's the right v1 behavior — silently faking GEOMETRY would lose data. After this chunk:

- **PostGIS-detected**: PG engine accepts `ir.Geometry` columns and emits `geometry(<subtype>, <srid>)` typed columns. WKB on the source converts to EWKB on the target; row values round-trip cleanly.
- **PostGIS-absent**: existing loud-error path is preserved. Operators see a clear message and can either install PostGIS or pre-trim the columns.

Out of scope:

- **Per-row SRID auto-detection.** Reading `ST_SRID(column)` on every row to discover the source's coordinate system is slow and pulls schema-shape concerns into the row pipeline. v1 defaults to SRID 0 (geometry without coordinate reference) and exposes the SRID as a per-column override via `sluice.yaml` mappings.
- **PostGIS extension installation.** Sluice detects PostGIS but doesn't install it. Operators who need PostGIS must install it via `CREATE EXTENSION postgis;` before running sluice.
- **MySQL → MySQL geometry round-trip.** Already works (the IR carries the bytes intact via `ir.Geometry`). This chunk is specifically about cross-engine MySQL → PG with PostGIS on the target.

## PostGIS detection

At engine open time, the PG `OpenSchemaWriter` and `OpenRowWriter` query:

```sql
SELECT 1 FROM pg_extension WHERE extname = 'postgis'
```

If a row exists, set `engine.hasPostGIS = true` and add `ir.ExtGeometry` to the engine's runtime `Capabilities.SupportedTypes`. If absent, the existing rejection path runs.

Detection happens once per engine open, cached on the engine instance. The integration test runs against `postgis/postgis:16-3.4` (the official PostGIS Docker image) which has the extension pre-installed but unloaded by default — the test's setup runs `CREATE EXTENSION postgis;` before opening sluice's writer.

## DDL emission

For an `ir.Geometry{Subtype: GeometryPoint}` column with default SRID 0:

```sql
"location" geometry(POINT, 0) NOT NULL
```

The IR's `ir.Geometry.Subtype` enum maps to PostGIS subtype names:

```go
ir.GeometryPoint           → "POINT"
ir.GeometryLineString      → "LINESTRING"
ir.GeometryPolygon         → "POLYGON"
ir.GeometryMultiPoint      → "MULTIPOINT"
ir.GeometryMultiLineString → "MULTILINESTRING"
ir.GeometryMultiPolygon    → "MULTIPOLYGON"
ir.GeometryCollection      → "GEOMETRYCOLLECTION"
ir.GeometryUnspecified     → "GEOMETRY"  // generic; permits any subtype
```

The `ir.Geometry` IR doesn't carry SRID today. v1 of this chunk reads SRID from a `sluice.yaml` mapping when present and defaults to 0 otherwise. A future revision could extend the IR with `ir.Geometry.SRID int` and have the MySQL reader populate it from `ST_SRID()` on a per-table sample row (cheap; one query per table at schema-read time).

## Mappings override for SRID

Operators with non-zero SRIDs add per-column mappings:

```yaml
mappings:
  - table: address
    column: location
    target_type: postgis_point
    target_type_options:
      srid: 4326          # WGS84 lat/long
```

The registry learns:

```go
"postgis_point"             → ir.Geometry{Subtype: GeometryPoint}
"postgis_linestring"        → ir.Geometry{Subtype: GeometryLineString}
"postgis_polygon"           → ir.Geometry{Subtype: GeometryPolygon}
"postgis_multipoint"        → ir.Geometry{Subtype: GeometryMultiPoint}
"postgis_multilinestring"   → ir.Geometry{Subtype: GeometryMultiLineString}
"postgis_multipolygon"      → ir.Geometry{Subtype: GeometryMultiPolygon}
"postgis_geometrycollection"→ ir.Geometry{Subtype: GeometryCollection}
"postgis_geometry"          → ir.Geometry{Subtype: GeometryUnspecified}
```

The SRID lives in `target_type_options.srid` and gets threaded to the DDL emit via a small extension to `emitColumnType`. The IR doesn't grow a SRID field for v1 — the override-via-mappings path covers the operational need.

## Value translation

MySQL stores geometry as `4-byte little-endian SRID + WKB` (Well-Known Binary). PostGIS stores as EWKB (Extended WKB) — essentially WKB with the SRID encoded into the geometry-type byte's high bits.

Conversion: 30 lines of byte-shuffling. Sketch:

```go
// mysqlBytesToEWKB converts MySQL's geometry bytes (4-byte SRID
// prefix + WKB) to PostGIS EWKB (SRID encoded into the type byte's
// high bits).
func mysqlBytesToEWKB(src []byte) ([]byte, error) {
    if len(src) < 9 {
        return nil, fmt.Errorf("mysql geometry too short (%d bytes; need >=9)", len(src))
    }
    srid := binary.LittleEndian.Uint32(src[:4])
    wkb  := src[4:]                       // WKB starts at byte 4
    // Set the SRID-present flag (0x20000000) on the geometry-type byte
    // and prepend the SRID inside the EWKB framing.
    // ... full implementation in cdc_ewkb.go ...
}
```

Lives in `internal/engines/postgres/value_encode.go` (currently empty / not present — the existing path uses pgx's native serialization). New file, ~50 lines plus tests.

The integration test seed inserts a known POINT (longitude/latitude pair) on the MySQL side; the PG-side assertion uses `ST_AsText(location)` to verify the round-trip preserved the coordinates.

## Files to add / touch

- `internal/engines/postgres/postgis.go` — `detectPostGIS(ctx, db) (bool, error)`, plus the byte-conversion helper. ~80 lines.
- `internal/engines/postgres/postgis_test.go` — unit tests for the byte-shuffler. ~80 lines.
- `internal/engines/postgres/engine.go` — `OpenSchemaWriter` and `OpenRowWriter` call `detectPostGIS` and stash the result on the writer. The runtime capabilities set is augmented with `ir.ExtGeometry` when present. ~30 lines.
- `internal/engines/postgres/ddl_emit.go` — `emitColumnType` learns to emit `geometry(<subtype>, <srid>)` when the writer has PostGIS. ~20 lines.
- `internal/engines/postgres/row_writer.go` — value-prepare path converts `ir.Geometry` source bytes via the new helper. ~10 lines.
- `internal/translate/mappings.go` — registry entries for the eight `postgis_*` aliases. ~20 lines.
- `internal/pipeline/migrate_geometry_integration_test.go` — new, exercises MySQL → PG-with-PostGIS for a POINT column. Uses `postgis/postgis:16-3.4` as the target image. ~150 lines.

~390 lines net.

## Anticipated rough edges

- **PostGIS image weight.** `postgis/postgis:16-3.4` is ~600 MB compared to the ~250 MB `postgres:16` image used elsewhere. The integration test that uses it should be optional / tagged separately so the default test run doesn't pull the bigger image. *Recommendation:* `//go:build integration && postgis` build tag; standard `make test-it` skips the PostGIS test, an explicit `make test-postgis` runs it.
- **`CREATE EXTENSION postgis` permission.** Most managed PG services restrict extension installation to admin users. The detection query (`SELECT FROM pg_extension`) is read-only and works for any role; the integration test pre-creates the extension via the admin role before opening sluice.
- **SRID 0 vs no SRID.** PostGIS treats SRID 0 as "unknown CRS" (no spatial reference). MySQL with no `SRID` argument also produces 0-SRID geometries. The defaults align — non-zero SRIDs are an explicit operator concern.
- **Multi-geometry types in cross-engine.** MySQL has all the same subtypes PostGIS does, but a MySQL `MULTIPOINT` column may contain a `POINT` value (the column constraint isn't enforced at write time on some MySQL versions). PG's `geometry(MULTIPOINT, 0)` IS enforced. Cross-engine could surface this as a "tried to insert POINT into a MULTIPOINT column" error. v1 documents this as a known difference; future work could re-shape values automatically.
- **CDC for geometry columns.** Same conversion path: MySQL binlog row events carry geometry bytes in the same SRID-prefixed-WKB format as `database/sql` does, so the existing converter works for both bulk-copy and CDC. Confirm with an integration test row-update.
- **Reverse direction.** PG → MySQL with a PostGIS source column: read EWKB, strip the SRID encoding back to MySQL's SRID-prefixed format. The reverse conversion is symmetric. v1 of this chunk could ship the forward direction only; reverse is a 30-line follow-up.

## Open questions for the user

1. **Detection at every Open vs once at Engine construction.** Above I'm doing it per-Open call (each Engine is short-lived in sluice's usage today). Centralising at Engine construction would cache more aggressively. *Recommendation:* per-Open is simpler and the query is one round-trip; defer caching until measurable. Confirm?
2. **Default behavior when PostGIS is detected but the operator hasn't opted in.** Should sluice automatically use PostGIS if available, or require an explicit per-column mapping? *Recommendation:* automatic. Operators who specifically want to opt-out can use mappings to route `address.location` to `bytea` or similar. Confirm?
3. **PostGIS image gating in CI.** Above I'm proposing a separate build tag. Alternative: include in the standard integration suite (and accept the ~10s longer first-run pull). *Recommendation:* separate tag — the cost compounds across CI runs and most developers don't need geometry coverage on every run. Confirm?
4. **Forward-direction-only for v1.** Above I'm punting reverse (PG → MySQL with geometry). *Recommendation:* yes. Confirm scope?
5. **SRID via mappings vs IR field.** Above I'm using mappings for v1. The IR-field approach is cleaner but requires schema-read-side support too. *Recommendation:* mappings for v1, IR field as a follow-up if a real use case wants per-table-not-per-column SRID handling. Confirm?

## Suggested first-cut prompt

> "Read CLAUDE.md, docs/dev/notes/prep-postgis-geometry.md, and the existing internal/engines/postgres engine.go + ddl_emit.go. Propose the design before writing: (1) the detectPostGIS query placement and result caching shape, (2) the mysqlBytesToEWKB byte-conversion helper signature with unit-test cases, (3) the geometry(<subtype>, <srid>) emission rule and how SRID flows from mappings, (4) the integration-test layout including the docker-compose addition for postgis/postgis:16-3.4. Note any deviation from the prep doc with a why. Stop after the design for review."
