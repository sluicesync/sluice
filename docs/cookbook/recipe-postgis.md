# Recipe — PostGIS geospatial round-trip (PG ↔ MySQL, SRID preserved)

GIS-aware migration: PG `geometry(POINT, 4326)` lands on MySQL as
`POINT NOT NULL SRID 4326` (not `SRID 0`), and the reverse direction
preserves the SRID too. This was the load-bearing fix for Bug 26 +
Bug 27, shipped in v0.28.0 under [ADR-0035](../dev/adr/adr-0035-postgis-geometry-spatial-support.md).

## When to use this recipe

- You're moving a PG database with PostGIS columns to MySQL — or
  the reverse — and the target needs the same spatial reference
  system the source used.
- You're doing PG → PG geometry migration and want PostGIS
  installed on the target automatically (sluice refuses loudly if
  it isn't; the recipe shows the operator-side install step).
- You're operating against PlanetScale-MySQL via VStream, where the
  VStream POINT/POLYGON wire format has an SRID prefix sluice's
  decoder strips correctly (Bug 27).

## What sluice does for geometry columns

`pg_dump` + `pg_restore` is the canonical PG → PG GIS migration
path; sluice doesn't compete with it for that shape. Where sluice
fits is:

- **PG → MySQL** (and the reverse): sluice's IR carries
  `ir.Geometry{Subtype, SRID}` through cross-engine. The MySQL writer
  emits `geometrytype NOT NULL SRID <srid>` when the source SRID is
  non-zero, instead of the silently-zeroed shape some older
  conversion tools produced.
- **PlanetScale-MySQL via VStream**: the wire format puts a 4-byte
  little-endian SRID prefix on POINT/POLYGON cells; the
  vanilla MySQL driver path doesn't see this prefix. sluice's
  VStream decoder strips it, so downstream consumers see raw WKB
  matching the IR contract.
- **Cross-engine sync** (not just migrate): the same applies in
  CDC mode. A `sluice sync start` against a PG source with
  geometry columns replays each spatial value to the MySQL target
  byte-faithfully.

## What sluice deliberately refuses

- **A PG source with geometry columns but no `--enable-pg-extension postgis` flag.**
  sluice refuses loudly at preflight: "geometry columns require
  --enable-pg-extension postgis." The flag is the opt-in
  acknowledgement that the operator has reviewed the cross-engine
  implications.
- **A MySQL or PG target without PostGIS installed** when the
  source has geometry columns. sluice's writer detects the missing
  PostGIS extension on a PG target via `pg_extension` and refuses
  loudly with the recovery (`CREATE EXTENSION postgis;`). It does
  not silently downgrade to `bytea` — that's the original Bug 35
  silent-loss class sluice's design tenet refuses to reintroduce.
- **Extension-owned opclasses** (e.g. `gist_geometry_ops_2d`) on
  cross-engine targets where the operator class doesn't exist.
  Same refuse-loudly path.

## The shapes that work end-to-end

These are the round-trip shapes pinned by integration tests + the
Bug 26 / Bug 27 / Bug 52 / Bug 53 closure cycle:

- **2-D point / linestring / polygon** with a non-zero SRID.
  `geometry(POINT, 4326)` → MySQL `POINT NOT NULL SRID 4326`.
- **3-D / 4-D variants** — `POINTZ`, `LINESTRINGZM`, `POLYGONZM`
  preserved across same-engine PG → PG (ADR-0035 sub-phase B). The
  reader captures `coord_dimension` from PostGIS's
  `geometry_columns` view; the writer emits the dimensional
  variant.
- **`geography(POINT, 4326)`** — the PG-only spheroid-aware type
  round-trips PG → PG. Cross-engine to MySQL falls through to the
  loud-refusal path (MySQL has no geography equivalent).
- **GiST / SP-GiST / BRIN indexes** on geometry columns with
  PostGIS opclasses (e.g. `gist_geometry_ops_2d`,
  `spgist_geometry_ops_2d`, `brin_geometry_inclusion_ops_2d`)
  preserved end-to-end same-engine.

## The recipe

### Prerequisites

The target needs PostGIS installed. PG side:

```sql
-- On target PG
CREATE EXTENSION postgis;
```

MySQL side: SRID-aware geometry types ship with MySQL 8.x by
default; no extension to install. Older 5.7 doesn't support
`SRID` clauses on geometry columns and sluice refuses loudly at
preflight if it detects 5.7.

### Step 1: preview

```sh
sluice schema preview \
    --source-driver postgres --source ... \
    --target-driver mysql    --target ... \
    --enable-pg-extension postgis
```

Output you want to see:

```sql
-- For each table with a geometry column:
CREATE TABLE locations (
    id BIGINT NOT NULL AUTO_INCREMENT,
    name VARCHAR(255),
    loc POINT NOT NULL SRID 4326,
    PRIMARY KEY (id)
);

-- For spatial indexes:
CREATE SPATIAL INDEX locations_loc_idx ON locations (loc);
```

Things to look for:

- `SRID <n>` on each geometry column, matching the source.
- `SPATIAL INDEX` (not `INDEX`) on GiST-on-geometry indexes.
- No `WARN: geometry column ... downgraded to bytea` — that would
  signal a regression.

### Step 2: migrate

```sh
sluice migrate \
    --source-driver postgres --source ... \
    --target-driver mysql    --target ... \
    --enable-pg-extension postgis
```

Bulk-copy phase encodes each geometry value as raw WKB (binary
format on PG → MySQL; the MySQL writer routes it through
`ST_GeomFromWKB` with the SRID from the IR).

### Step 3: verify

`ST_SRID` should match the source on every row:

```sql
-- On target MySQL
SELECT id, ST_SRID(loc) FROM locations LIMIT 10;
-- Expected: every row's ST_SRID = the source's SRID (e.g. 4326).
```

`sluice verify --depth=sample` covers the geometry value content;
the SRID check above is the MySQL-side proof that the *shape* of
the column carried.

### Step 4: continuous sync (if needed)

The exact same `sluice sync start` command works against PG sources
with geometry columns. The CDC apply path carries WKB the same way
the bulk-copy path does; no separate sync setup.

## Common pitfalls

- **`--enable-pg-extension postgis` missing.** Loud refusal at
  preflight. Add the flag.
- **Target MySQL is 5.7 or older.** Loud refusal at preflight; the
  `SRID <n>` clause requires 8.0. Upgrade the target or drop down to
  unencoded `BLOB` columns (which loses spatial query support — not
  recommended).
- **PostGIS not installed on the target PG.** Loud refusal at
  preflight; the writer detects PostGIS via `pg_extension` before
  any geometry columns are emitted. Run `CREATE EXTENSION postgis;`
  on the target and rerun.
- **Source has `geography` columns and target is MySQL.** Loud
  refusal — MySQL has no `geography` equivalent. Cast to `geometry`
  on the source side first if the spheroid distinction doesn't
  matter for your downstream use.
- **Z/M dimensional variants on cross-engine.** Same-engine PG → PG
  carries them via ADR-0035 sub-phase B; cross-engine to MySQL
  refuses loudly (the MySQL `POINT`/`LINESTRING` grammar doesn't
  surface Z/M in the type signature). Operators with 3D / 4D
  geometries who need MySQL targets should down-cast on the source
  before migrating.

## See also

- [ADR-0035](../dev/adr/adr-0035-postgis-geometry-spatial-support.md)
  — the full design rationale for cross-engine geometry, the SRID
  threading, the VStream Bug 27 decoder fix.
- [`docs/type-mapping.md`](../type-mapping.md) — the per-type
  translation policy, including the geometry-family rows.
- [`docs/postgres-source-prep.md`](../postgres-source-prep.md) — the
  source-side requirements; for PostGIS-using sources, this is
  where the extension-allowlist flag is documented.
