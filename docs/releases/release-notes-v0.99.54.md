# sluice v0.99.54

**PostGIS `geometry` columns now sync over CDC to a PostgreSQL target
(#20).** Geometry was previously un-appliable over continuous sync — the
applier loudly refused it. This release adds a binary geometry codec on the
applier connections, so geometry INSERT/UPDATE/DELETE replicate correctly,
with full per-column SRID fidelity.

## Added

- **Geometry over CDC.** The CDC applier had no codec for PostGIS's
  dynamically-assigned `geometry` type OID, so the EWKB bytes were shipped
  in text format and PostGIS rejected them (`parse error - invalid
  geometry`) — a loud refusal on both the serial and pipelined (ADR-0092)
  apply paths. (Cold-start COPY was unaffected — it ships EWKB in
  COPY-binary.) A binary geometry codec is now registered on the applier
  connections (mirroring the existing pgvector / hstore / timetz codecs) and
  ships EWKB to `geometry_recv`. Pinned against a real PostGIS target across
  every subtype (point / linestring / polygon incl. holes / multipoint /
  multilinestring / multipolygon / geometrycollection), dimension
  (2D / Z / M / ZM), SRID (unset, 4326, 3857), byte order (LE/BE), and NULL.

## Fixed

- **Per-column SRID preserved on geometry CDC apply.** The CDC readers strip
  per-row SRID to raw WKB (the IR carries SRID as a per-column property,
  ADR-0035), and the applier previously defaulted the column SRID to 0 — so a
  replicated value into a constrained `geometry(<type>,<srid>)` column lost
  its SRID. The applier now recovers each geometry column's real SRID (and
  subtype) from `geometry_columns` / `geography_columns`, so the stored
  geometry matches the source byte-for-byte (`ST_AsEWKB` + `ST_SRID`
  src==dst). This was caught by the pre-merge value-fidelity review before it
  could ship.

## Compatibility / notes

- No flag or config change — geometry-over-CDC works by default once the
  PostgreSQL target has PostGIS installed.
- `geography` columns and `geometry[]` (arrays of geometry) remain **loudly
  refused** over CDC apply (never a silent loss) — separate follow-up work.
- A per-row SRID stored in an *unconstrained* `geometry` column (one declared
  without a SRID modifier) is still dropped by design (ADR-0035: SRID is a
  per-column property). Declare the column `geometry(<type>,<srid>)` to
  preserve it.

## Who needs this

- Anyone running **continuous sync into a PostGIS target** with `geometry`
  columns. Cold-start migration of geometry already worked; this closes the
  CDC (ongoing-replication) gap.

## Install

```
# Linux/macOS/Windows binaries + checksums on the release page.
docker pull ghcr.io/sluicesync/sluice:v0.99.54
```
