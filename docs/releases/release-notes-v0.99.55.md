# sluice v0.99.55

**PostGIS `geometry` now syncs over CDC from a PostgreSQL *source* (Bug 147) — completing geometry-over-CDC.** v0.99.54 added the apply-side geometry codec (which made MySQL→PG geometry sync work); this release fixes the read side, so PostgreSQL→PostgreSQL continuous sync of geometry columns works too.

## Fixed

- **PG-source geometry over CDC (Bug 147).** The pgoutput CDC reader's type map had no `geometry` case — PostGIS's `geometry` OID is *dynamic* (assigned at `CREATE EXTENSION postgis`), so it can't be a static entry — so the first geometry change on a PG→PG CDC stream killed it with `unsupported column type OID <n> (geometry)`. (Cold-start COPY migration of geometry already worked; this was the continuous-replication gap, the read-side counterpart of the v0.99.54 apply-side codec, and the same class as the Bug 144 array gap.) The reader now resolves the runtime geometry OID once and decodes the value.
- **Value-decode correctness (caught in review).** pgoutput delivers a column value as its **text** form, so a geometry arrives as hex-EWKB ASCII bytes — the decoder now treats those as hex (not as raw EWKB, which would have silently corrupted the geometry). Verified that the cold-start, CDC-bytes, and raw-EWKB decode paths all produce identical results.
- **Schema-comparison fix (caught in review).** Added geometry to the CDC schema-comparison normalizer so a geometry column no longer false-triggers an "altered column" against the richer cold-start schema (which would have wedged a concurrent legitimate `ADD COLUMN`).

## Compatibility / notes

- No flag or config change. Works by default once the PostgreSQL endpoints have PostGIS installed.
- Pinned across subtypes (point / polygon / multipolygon / geometrycollection), dimensions (2D / Z / M / ZM), SRID 4326 **and** 0, `POINT EMPTY`, NULL, and the UPDATE after-image / DELETE before-image paths.
- `geography` columns over CDC remain **loudly refused** (no silent loss) — the applier has no geography codec yet; geography end-to-end is a tracked follow-up.

## Who needs this

- Anyone running **PostgreSQL→PostgreSQL continuous sync** with PostGIS `geometry` columns. (MySQL→PostgreSQL geometry-over-CDC already worked as of v0.99.54; cold-start migration of geometry worked before that.)

## Install

```
# Linux/macOS/Windows binaries + checksums on the release page.
docker pull ghcr.io/sluicesync/sluice:v0.99.55
```
