# sluice v0.33.1 — PostGIS geography + SP-GiST / BRIN passthrough

Two scoped PostGIS gaps from the v0.33.0 cycle, both closed. `--enable-pg-extension postgis` on same-engine PG → PG now lights up the 3 catalog opclasses that previously slipped through unreached (the SP-GiST + BRIN flavours), and `geography(POINT, 4326)` columns no longer trip a misleading "pass the flag" error when the flag is already passed.

## Fixed

- **Bug 49: PG `geography` columns now round-trip on same-engine PG → PG.** The schema reader's special case only covered `udt_name == "geometry"`; geography columns fell through to the user-defined-hint path and refused with "pass `--enable-pg-extension postgis`" even when the flag was passed. The fix adds an `IsGeography bool` to `ir.Geometry` (minimal IR surface — reuses every writer arm and the cross-engine path), a parallel `readGeographyColumnInfo` against PostGIS's `geography_columns` view, and a PG writer arm that emits `geography(<subtype>, <srid>)` when `IsGeography == true`. Cross-engine PG → MySQL flattens geography to MySQL geometry (no equivalent spherical-operator type there); backup envelope round-trips the flag.

- **Bug 50: `ir.IndexKind` now models SP-GiST and BRIN, unlocking 6 of the 9 PostGIS spatial opclasses.** Pre-fix: only the 3 GiST opclasses (`gist_geometry_ops_2d`, `gist_geometry_ops_nd`, `gist_geography_ops`) round-tripped end-to-end. The SP-GiST and BRIN opclasses (`spgist_geometry_ops_2d` / `_3d` / `_nd`, `brin_geometry_inclusion_ops_2d` / `_4d` / `_nd`) were correctly preserved in IR by the v0.33.0 catalog entry, but `indexKindFrom` in the schema reader returned `IndexKindUnspecified` for `spgist` / `brin` access methods, and the writer's `postgresIndexMethod` therefore dropped the AM, falling back to btree — CREATE INDEX then failed on the target because those opclasses aren't btree-compatible. Two new enum values (`IndexKindSPGist`, `IndexKindBRIN`) are appended to the IR enum (preserves uint8 backup stability), schema-reader and writer dispatch arms are extended, and the cross-engine PG → MySQL refusal in `unsupportablePGtoMySQL` was broadened to catch the new kinds.

## Compatibility

- **No format-breaking changes.** No CLI surface changes, no engine-interface changes. The IR's `Geometry` struct gains an `IsGeography bool` field (zero-value = the pre-v0.33.1 geometry shape; existing backups deserialise unchanged because the backup envelope's `is_geography` field is `omitempty`). The IR's `IndexKind` enum gains two appended values; existing manifests with the prior values are unaffected.
- **Cross-engine PG → MySQL with PG SP-GiST or BRIN indexes** now refuses upstream at `checkCrossEngineSupportable` with a clear "PG <kind> index has no MySQL counterpart" message, mirroring the existing GIN / GiST refusals. Pre-fix the IR carried `IndexKindUnspecified` so the refusal didn't fire and the operator hit a downstream CREATE INDEX failure on the MySQL target.

## Who needs this release

- **Operators running PG → PG with `geography` columns:** **upgrade and pass `--enable-pg-extension postgis`** — same-engine geography passthrough now works end-to-end.
- **Operators running PG → PG with SP-GiST or BRIN spatial indexes:** **upgrade and pass `--enable-pg-extension postgis`** — the spatial opclasses now round-trip and the target index strategy matches the source.
- **Operators running PG → PG with GiST-only spatial indexes** (the v0.33.0 path): drop-in; no behaviour change.
- **Operators on the cross-engine PG → MySQL PostGIS path** (the v0.28.0 / ADR-0035 path): drop-in for `geometry`; if your PG source uses SP-GiST / BRIN spatial indexes you now get an actionable refusal at preflight instead of a CREATE INDEX failure on the MySQL target.
- **Operators not using PostGIS:** drop-in; no behaviour change.

## Verification surface

3 new PG → PG integration tests under `integration postgis` build tags: `TestMigrate_PG_PostGIS_GeographyPassthrough`, `TestMigrate_PG_PostGIS_SPGistIndexPassthrough`, `TestMigrate_PG_PostGIS_BRINIndexPassthrough`. Plus 2 cross-engine refusal tests (`TestCheckCrossEngineSupportable_PGtoMySQL_{SPGist,BRIN}KindRefuses`), 1 backup round-trip case for `IsGeography`, 1 translator test for the geography branch, and 3 added cases to the PG writer's `TestEmitColumnTypeGeometry`. All unit + integration tests pass on Linux (CI), Windows + macOS (Test job), and Vultr (manual smoke).
