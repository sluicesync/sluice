# sluice v0.33.2 — PostGIS subtype-case + Z / M / ZM dimensional variants

Two adjacent PostGIS fidelity gaps surfaced by the v0.33.1 cycle, both closed. Same fix locus (`parseGeometrySubtype`); shipped together as v0.33.2. Neither bug corrupted row data, but both silently widened the column type on the target — operators reading from `geometry_columns` / `geography_columns` on the target would see drift vs the source.

## Fixed

- **Bug 51: PG `geography(Point, 4326)` columns now preserve the subtype on same-engine PG → PG.** PostGIS's `geometry_columns.type` view returns ALL-CAPS strings ("POINT"), but its sibling `geography_columns.type` view returns Mixed-Case ("Point"). `parseGeometrySubtype` did a literal switch on the upper-case forms only, so `geography_columns` inputs fell through to `GeometryUnspecified` and the target column landed as `geography(Geometry, 4326)` instead of `geography(Point, 4326)`. Rows still round-tripped (the wildcard supertype accepts any concrete shape), but the typmod-constrained subtype was lost. The fix upper-cases the input before dispatching; geometry and geography subtype reads now share one code path.

- **Bug 52: PG `geometry(POINTZ, 4326)` and the Z / M / ZM dimensional variants now round-trip on same-engine PG → PG.** Pre-existing — not a v0.33.1 regression. PostGIS extends each 2D subtype with Z (3D elevation), M (linear measure), and ZM (4D) variants — `POINTZ`, `LINESTRINGZM`, `MULTIPOLYGONZ`. The IR's `GeometrySubtype` enum only modeled the seven 2D base subtypes; the dimensional suffix dropped at translate-time and the writer emitted the generic `GEOMETRY` wildcard. Bulk copy then failed upstream with SQLSTATE 22023 ("Geometry has Z dimension but column does not") because the row's WKB carried Z bytes but the target's typmod-constrained column rejected them. The fix adds two appended booleans (`HasZ`, `HasM`) on `ir.Geometry` orthogonal to `Subtype` (28 enum entries collapsed to 7 × 2 flags); `parseGeometrySubtype` strips the dimensional suffix before subtype dispatch; the PG writer's `postgisSubtypeName` reconstructs the suffix on emit (`geometry(POINTZM, 4326)`). Cross-engine PG → MySQL ignores the flags — MySQL carries Z / M in the WKB bytes rather than the column type, so the value round-trip works on MySQL targets without needing the column type to match.

## Compatibility

- **No format-breaking changes.** No CLI or engine-interface changes. `ir.Geometry` gains two appended bool fields (`HasZ`, `HasM`); existing backups deserialise unchanged because the envelope fields use `omitempty`. Zero values produce the pre-v0.33.2 behaviour (2D, no measure).
- **Drop-in upgrade from v0.33.1.** Operators with plain 2D `geometry` / `geography` columns are unaffected.

## Who needs this release

- **Operators running PG → PG with `geography(<Subtype>, <SRID>)` typed columns:** **upgrade** — target column-type fidelity now matches the source byte-for-byte.
- **Operators running PG → PG with Z / M / ZM (3D / measure / 4D) spatial columns:** **upgrade** — pre-v0.33.2 the bulk copy failed with SQLSTATE 22023 because the dimensional suffix was dropped from the target column type.
- **Operators on the cross-engine PG → MySQL PostGIS path:** drop-in; no behaviour change.
- **Operators not using PostGIS:** drop-in; no behaviour change.

## Verification surface

2 new PG → PG integration tests under `integration postgis`: `TestMigrate_PG_PostGIS_GeographySubtypePreserved`, `TestMigrate_PG_PostGIS_PointZPassthrough`. Extended `TestParseGeometrySubtype` with 14 new cases (mixed-case + Z / M / ZM); extended `TestPostgisSubtypeName` for Z / M / ZM emit; 4 new cases in `TestEmitColumnTypeGeometry`; 3 new backup round-trip cases. All unit tests + `gofumpt` + `go vet` + `golangci-lint` clean.
