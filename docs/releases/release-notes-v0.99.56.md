# sluice v0.99.56

**MySQL `SET` columns now sync correctly over binlog CDC (Bug 148).** A
replicated `SET` value was carried as its raw numeric bitmask instead of its
member labels — the `SET` sibling of the v0.99.52 ENUM fix. Self-hosted
MySQL binlog CDC only; snapshot/copy and PlanetScale (VStream) sync were
already correct.

## Fixed

- **`SET` over binlog CDC.** The MySQL binlog (go-mysql) decoder hands a
  `SET` cell back as its **1-bit-per-member numeric bitmask**, not the member
  text, and sluice's CDC value decoder passed that straight through — so a
  replicated `SET('a','c')` (mask `5`) landed as `["5"]` instead of
  `["a","c"]`. The decoder now maps the bitmask to its member labels via the
  column's value list (bit *i* → the *i*-th declared member, in declaration
  order — matching the text path), errors **loudly** on a bit with no
  declared member rather than dropping it, and treats mask `0` as the empty
  set. The comma-joined label text delivered by the snapshot/copy path and
  the VStream reader still passes through unchanged.

## Compatibility / notes

- No flag or config change.
- Scope: **self-hosted MySQL → (any target) over binlog CDC**. The snapshot
  (initial copy) path and PlanetScale/Vitess (VStream) sync already delivered
  `SET` as its label text and were unaffected.
- This is the `SET` counterpart of the v0.99.52 ENUM ordinal-index fix
  (Bug 145): both types arrive from the binlog as numbers and must be mapped
  back to labels via the column's member list.

## Who needs this

- Anyone running continuous sync from a **self-hosted MySQL source with
  `SET` columns**. (PlanetScale sources and initial-copy migrations were
  already correct.)

## Install

```
# Linux/macOS/Windows binaries + checksums on the release page.
docker pull ghcr.io/sluicesync/sluice:v0.99.56
```
