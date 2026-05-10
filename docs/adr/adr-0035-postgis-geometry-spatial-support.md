# ADR-0035: PostGIS-aware GEOMETRY / SPATIAL translation

## Status

Accepted; landing in v0.28.0.

## Context

Two long-standing open bugs and one incomplete cross-engine direction
share root causes in spatial-type handling:

- **Bug 26** — PG `geometry(POINT, 4326)` translated to MySQL emits
  `POINT NOT NULL` (no `SRID 4326`); on the target `ST_SRID(loc)`
  returns 0 instead of 4326. Operators with spatial joins / distance
  queries against other 4326 datasets see silent corruption of
  spatial-reference identifiers.
- **Bug 27** — VStream POINT bytes carry a 4-byte little-endian SRID
  prefix that pre-fix tripped sluice's WKB byte-order-flag check
  (`wkb has unknown byte-order flag 0xe6 (want 0 or 1)`). Vanilla
  MySQL's text protocol path stripped the prefix correctly; only the
  VStream binary protocol delivered raw bytes through.
- **Pre-v0.28.0 cross-engine PG → MySQL refusal** — `pipeline.
  checkCrossEngineSupportable` refused PG `ir.Geometry` columns to
  any MySQL target (chain restore, migrate, or stream) on the basis
  that the SRID metadata didn't round-trip cleanly. v0.10.3 closed
  the MySQL → PG side; the reverse direction stayed refused as a
  conservative v1 default.

The IR (`ir.Geometry{Subtype, SRID}`), the PG schema reader (reads
`geometry_columns`), the PG writer's `geometry(<subtype>, <srid>)`
emit + EWKB framing, the MySQL schema reader's `srs_id` capture, and
the MySQL row writer's prefix wrapping all landed before this ADR.
What was missing: MySQL DDL emission of the `SRID <n>` clause, VStream
prefix stripping, and lifting the cross-engine refusal.

The roadmap entry that scopes this work is item 6 in
`docs/dev/roadmap.md` ("GEOMETRY / SPATIAL support — PostGIS-aware
translation"); the prep doc is
`docs/dev/notes/prep-postgis-geometry.md`.

## Decision

Land the three remaining pieces under the existing PostGIS-aware
machinery. The framework already detects PostGIS via `pg_extension`,
populates `hasPostGIS` on PG SchemaWriter / RowWriter at engine open,
and converts WKB ↔ EWKB at row-write time. This ADR closes the
remaining gaps.

### 1. MySQL DDL emission learns the `SRID <n>` clause

When `ir.Geometry.SRID != 0`, `emitColumnDef` appends `SRID <n>` to
the spatial column's DDL fragment after the `NOT NULL`/nullability
marker:

    `loc` POINT NOT NULL SRID 4326

`SRID 0` (MySQL's "no spatial reference declared" sentinel) emits no
clause — preserves the pre-Bug-26 shape so existing PG `geometry(POINT, 0)`
cross-engine output isn't gratuitously rewritten. Closes **Bug 26**'s
PG → MySQL side.

The clause is MySQL 8.0+; sluice's MySQL baseline is 8.0, so no
version gate is needed.

### 2. VStream cell decoder strips the 4-byte SRID prefix

`decodeVStreamCell`'s `query.Type_GEOMETRY` branch was previously
folded into the binary types' fall-through (`case query.Type_JSON,
query.Type_BLOB, ..., query.Type_GEOMETRY: return copyBytes(raw)`),
which delivered the SRID-prefixed bytes verbatim. The fix splits
`Type_GEOMETRY` out and applies the same prefix-stripping
`decodeMySQLGeometry` already does for the vanilla `database/sql`
driver path. Output is raw WKB matching the IR contract for
`ir.Geometry` values (`docs/value-types.md`).

Malformed inputs (under 5 bytes — shorter than the prefix alone)
pass through untouched so the downstream WKB validator surfaces a
clear error rather than the decoder silently re-shaping garbage.

Closes **Bug 27**.

### 3. PG → MySQL cross-engine geometry no longer refuses

`unsupportablePGtoMySQL` was the gate. Pre-v0.28.0 it refused
`ir.Geometry` blanket; with the SRID round-trip closed (point 1
above + the existing PG schema reader population), the conservatism
is no longer load-bearing. Refusal narrows to `ir.ExtensionType`
(ADR-0032 PG extension passthrough — pgvector, hstore, citext,
etc.) which has no portable MySQL equivalent.

The chain-restore PostGIS-refusal tests (unit + integration
placeholder) become "now-supported" regression guards.

### EWKB ↔ MySQL-prefix conversion details

The framing already in place:

- **MySQL on-wire format**: `<srid uint32 LE><wkb>` (4-byte prefix).
- **PostGIS EWKB format**: standard WKB with the high bit
  (`0x20000000`) of the geometry-type uint32 set, followed by a
  4-byte SRID inserted between the type word and the type-specific
  payload. Byte-order flag (1 byte: 0=BE, 1=LE) preserved from the
  input.
- **IR canonical form**: raw WKB, no prefix, no EWKB framing
  (per `docs/value-types.md`).

Conversion sites:

- **MySQL value decoder (`decodeMySQLGeometry`)** — strip the 4-byte
  prefix on read (existing).
- **MySQL row writer (`prepareValue` Geometry case)** — prepend a
  4-byte LE SRID prefix on write, using `ir.Geometry.SRID` from the
  column type (existing).
- **PG row writer (`prepareValue` Geometry case)** — wrap WKB into
  EWKB by setting the type-byte's SRID-present flag and inserting
  the 4-byte SRID, using `ir.Geometry.SRID` from the column type
  (existing in `wkbToEWKB`).
- **PG schema reader** — lookup `(table, column)` in
  `geometry_columns` view and populate `ir.Geometry.SRID` /
  `ir.Geometry.Subtype` (existing in `readGeometryColumnInfo`).
- **MySQL schema reader** — lookup `srs_id` in
  `information_schema.columns` and populate `ir.Geometry.SRID`
  (existing).

Per-row SRID is intentionally dropped at the value layer — the IR
treats SRID as a per-column property captured at schema-translation
time, not per-row at decode time. This matches both PG's
`geometry(<subtype>, <srid>)` typed-column model and MySQL's
`SRID <n>` column attribute.

## Threat model

Five operator-visible failure modes the design has to either prevent
or handle loudly.

### A. PostGIS-absent target

**Risk**: An operator runs migrate / restore against a PG target
without PostGIS installed; ir.Geometry columns silently downgrade to
`bytea`, losing query semantics.

**Mitigation**: `OpenSchemaWriter` and `OpenRowWriter` query
`pg_extension` at open time. Without PostGIS, the schema writer's
DDL emission refuses with a clear `GEOMETRY requires PostGIS;
install with CREATE EXTENSION postgis; before running sluice` error
before any tables are created. Loud-failure tenet preserved.

### B. SRID mismatch between source and target

**Risk**: Operator's source has SRID 4326 (WGS84 lat/long) but the
mapping override or default produces SRID 0 on the target; spatial
joins silently miss.

**Mitigation**: SRID flows through the IR end-to-end. The PG schema
reader populates from `geometry_columns`; the MySQL schema reader
populates from `srs_id`. The translate-layer registry threads
`target_type_options.srid` for explicit overrides (postgis_*
aliases). Operators wanting to change SRID set it explicitly via
mapping; without an override, the source's SRID rides through.

### C. VStream prefix on vanilla MySQL connections

**Risk**: A code path that mistakenly applies the prefix-stripping to
already-stripped bytes would produce malformed WKB.

**Mitigation**: The strip happens once per source-driver path —
`database/sql` driver in `decodeMySQLGeometry`, VStream in
`decodeVStreamCell`. Each path covers exactly its own raw bytes;
they don't compose. Unit tests pin both shapes.

### D. EPSG SRID unrecognized by the target's spatial_ref_sys

**Risk**: Source declares an exotic SRID (e.g. proprietary CRS or
state-plane variant) the target's spatial_ref_sys table doesn't
recognize. PostGIS rejects `geometry(POINT, <unknown_SRID>)` at
CREATE TABLE; MySQL's behavior is more lenient but spatial functions
fail later.

**Mitigation**: Loud-failure default — the engine's CREATE TABLE
refusal surfaces the unrecognized SRID, and the operator either
installs the missing CRS row in the target or adds a `--type-override`
mapping to coerce the column to a different SRID. v1 supports the
EPSG common subset (4326, 3857, 4269, the typical lat/long + Web
Mercator + NAD83 set); unrecognized SRIDs surface as a clear error
rather than a silent degradation.

### E. MySQL → PG cross-engine SRID drift

**Risk**: MySQL `srs_id` and PostGIS `srid` use the same EPSG
numbering for common cases, but historical PostGIS deployments may
have customized `spatial_ref_sys` rows. A MySQL source declaring
SRID 5070 (NAD83 / Conus Albers) lands on a PG target whose
spatial_ref_sys was customized to assign 5070 to a different CRS.

**Mitigation**: Same as D — the target-side CREATE TABLE / INSERT
surfaces the mismatch as a CRS-not-found or coordinate-out-of-bounds
error; operators with customized spatial_ref_sys tables already
manage this risk in their existing tooling and can use mappings to
normalize.

## Why parented under item 6 (GEOMETRY/SPATIAL) and not item 11 (PG extension passthrough)

PostGIS is unique in the v1 extension landscape because it has
**both** a PG-to-PG passthrough story (item 11's framework, ADR-0032)
and a cross-engine MySQL story (this entry). Cross-engine is the
load-bearing operator demand — sakila's `address.location`, mapping
schemas in Rails/Django/Hasura apps, IoT location tables — and the
machinery (PostGIS detection, EWKB framing, MySQL prefix handling)
is far enough from the generic ADR-0032 catalog that splitting them
across two chunks would have inflated both. The PG-to-PG passthrough
catalog entry follows naturally once Phase A's PG writer emit lands;
the catalog can register PostGIS as another `--enable-pg-extension`
target whenever the demand for PG → PG round-trips fidelity exceeds
the existing geometry-column path.

This split-of-concerns also matches the test-image distinction:
`pgvector/pgvector:0.7.4-pg16` (ADR-0032's reference image) and
`postgis/postgis:16-3.4` (this ADR's image) are independent, and
each chunk gets its own `integration` build-tag layer
(`integration` for pgvector, `integration postgis` for PostGIS) so
the heavier postgis image isn't pulled on every Integration job.

## Consequences

- **Bug 26 closes** — PG → MySQL cross-engine SRID round-trips
  cleanly through schema + row paths.
- **Bug 27 closes** — VStream POINT bulk-copy + CDC paths no longer
  trip on the SRID prefix.
- **PG → MySQL geometry chain restore + sync stream now work** —
  pre-v0.28.0 they refused at preflight. Existing tests asserting
  the refusal are updated to the new behavior (chain restore now
  refuses only on `ir.ExtensionType` columns, the remaining
  unsupportable cross-engine shape).
- **CI Integration job grows ~500 MB image cold-pull** — the
  `postgis/postgis:16-3.4` image is heavier than `pgvector` and
  bigger than the base `postgres:16` image. Mitigation: separate
  test invocation gated on `integration postgis` build tag means
  only the new tests boot the image (~2 minutes total job-time
  growth, well under the 35m outer cap).
- **MySQL emit gains a single column-attribute clause** — small
  diff (one branch in `emitColumnDef`); covered by unit tests.
- **Operator UX**: the same "PostGIS-absent target" loud-failure
  applies. Operators on a target lacking PostGIS still see the
  clear refusal to install the extension — the chunk doesn't
  introduce silent downgrade paths.
