# sluice v0.33.3 — PostGIS coord_dimension capture (Bug 53)

**v0.33.2 closed Bug 52 only partially.** The structural fix was correct on the IR + writer side, but skipped Phase A: it assumed PostGIS's `geometry_columns.type` view encodes Z and ZM in the type string the same way it encodes the M-only case (LINESTRINGM, POINTM). It doesn't. Bug 53 captures the missed slice; this release closes it and pins the catalog-shape assertion in integration tests so the failure mode can't recur silently.

## Fixed

- **Bug 53: PG `geometry(POINTZ, 4326)` and `geometry(POLYGONZM, 4326)` now actually round-trip end-to-end.** PostGIS uses a two-channel encoding for spatial-column dimensions in its `geometry_columns` / `geography_columns` views:

  - **M-only** (LINESTRINGM, POINTM): puts the "M" suffix in `type` directly AND records `coord_dimension=3`. v0.33.2's `parseGeometrySubtype` upper-case + suffix-strip path caught this case.
  - **Z** (POINTZ, LINESTRINGZ): leaves `type` as the 2D base name (`POINT`, `LINESTRING`) and signals the dimension *only* via `coord_dimension=3`. v0.33.2 didn't read `coord_dimension`, so the Z flag silently dropped at translate-time and the writer emitted `geometry(POINT, 4326)` instead of `geometry(POINTZ, 4326)`. Bulk copy then failed with SQLSTATE 22023 ("Geometry has Z dimension but column does not").
  - **ZM** (POLYGONZM): same as Z — `type` stays as the 2D base name, dimension signaled by `coord_dimension=4`.

  The fix adds `coord_dimension` to the `SELECT` in both `readGeometryColumnInfo` and `readGeographyColumnInfo`, maps it to `HasZ` / `HasM` flags on the per-column `geometryColumnInfo` via a new `dimensionFlagsFromCoordDim` helper that disambiguates the 3D case by inspecting whether the type column ends in "M", and OR-merges the reader-side flags with the existing type-string parsing in `translateType`. Either channel alone may be load-bearing; the OR-merge means neither in isolation is fragile.

- **PostGIS catalog evidence is now first-class in integration tests.** The v0.33.2 cycle filed Bug 53 because the integration tests asserted on `geometry_columns.type='POINTZ'` — a value PostGIS never produces for that column (the view normalises to the base name, even though the column itself accepts Z values). v0.33.3 shifts the ground-truth assertion to `pg_attribute.format_type(atttypid, atttypmod)`, which returns the modifier-bearing form `geometry(PointZ,4326)`. Three new dimensional-variant integration tests (POINTZ, POLYGONZM, LINESTRINGM) all run against real PostgreSQL containers locally (verified) and will run in CI.

## Compatibility

- **No format-breaking changes.** No CLI or engine-interface changes. `geometryColumnInfo` gains two unexported `HasZ` / `HasM` fields (internal to the postgres engine package); `ir.Geometry`'s existing `HasZ` / `HasM` fields from v0.33.2 are unchanged.
- **Drop-in upgrade from v0.33.2.** Operators with plain 2D `geometry` / `geography` columns are unaffected. Operators with Z, M, or ZM dimensional columns now get the end-to-end round-trip v0.33.2 promised.

## Process — Phase A is non-negotiable, even for "small" fixes

The Phase A diagnostic skip in v0.33.2 (assumed PostGIS encoded Z in the type column without verifying against a live catalog) is the same pattern the three-phase debug protocol exists to prevent. The retag cost: one extra release (v0.33.3), one extra CI cycle, one extra subagent cycle. The protocol's overhead: one `docker exec psql -c "SELECT type, coord_dimension FROM geometry_columns WHERE ..."` against a one-shot container. v0.33.3 corrects the catalog-assertion shape (`format_type` over `geometry_columns.type`) so the failure mode can't recur silently.

## Who needs this release

- **Operators running PG → PG with Z, M, or ZM dimensional spatial columns:** **upgrade** — v0.33.2's claimed fix only worked for the M-only case; v0.33.3 closes Z and ZM.
- **Operators on the cross-engine PG → MySQL PostGIS path:** drop-in; no behaviour change (MySQL carries Z / M in WKB, not the column type).
- **Operators not using PostGIS:** drop-in; no behaviour change.

## Verification surface

- **Real-PostgreSQL integration**: `TestMigrate_PG_PostGIS_PointZPassthrough`, `TestMigrate_PG_PostGIS_PolygonZMPassthrough`, `TestMigrate_PG_PostGIS_LineStringMPassthrough`, `TestMigrate_PG_PostGIS_GeographySubtypePreserved` — all pass against `postgis/postgis:16-3.4` locally; CI runs under `integration postgis` tag.
- **Unit**: `TestDimensionFlagsFromCoordDim` (12 cases pinning coord_dimension semantics), `TestTranslateType_GeometryCoordDimensionMerge` (4 cases pinning the OR-merge), plus the pre-existing `TestParseGeometrySubtype` (28 cases) for the type-string-parsing channel.
