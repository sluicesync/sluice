# sluice v0.99.60

**PostgreSQL-source `ENUM` columns now replicate over PG→PG continuous sync (catalog Bug 151).** A PostgreSQL table with a user-defined `ENUM` column cold-started fine but wedged the CDC stream on the first enum write; this closes that reader gap, so enum columns sync end-to-end PG→PG. The same operator experience as the v0.99.50 array fix and the v0.99.55 geometry fix — migrate works, then the first production write to the column killed the stream — now resolved for enums too.

## Fixed

- **PG-source `ENUM` over CDC (catalog Bug 151).** The `pgoutput` CDC reader's OID-to-type map had no case for a user-defined `ENUM`. An enum's OID is dynamic (assigned at `CREATE TYPE` time), so the static lookup declined it and the first enum `INSERT`/`UPDATE`/`DELETE` halted the stream with `unsupported column type OID <dyn> (typmod -1)` (loud, no silent loss — the stream errored and exited before any value reached the target). The reader now resolves the source's set of user-defined enum type OIDs (`pg_type.typtype='e'`) at the relation boundary and maps a matching column to an enum, whose value rides `pgoutput` as its text label and applies to the target enum column directly. Resolution is cumulative across relation boundaries, so a mid-stream `CREATE TYPE` + `ADD COLUMN` of a new enum is picked up as well.
- **Scope boundaries stay loud.** A non-enum user-defined type (composite, domain) and enum **array** columns (`enum[]`) remain loudly refused rather than silently mishandled — a tracked follow-up, mirroring how geography is held out of the geometry path.

## Compatibility / notes

- No flag or config change.
- Scope: **PostgreSQL source → PostgreSQL target, `ENUM` columns, over CDC.** The cold-start (initial copy) path already handled PG enums, and MySQL→PostgreSQL enum sync (v0.99.52) was unaffected — this closes the remaining PG-source CDC leg.
- Same class as the closed Bug 144 (arrays, v0.99.50) and Bug 147 (geometry, v0.99.55) reader gaps; fixed with the same dynamic-OID resolution pattern and value-fidelity-reviewed before landing.

## Who needs this

- Anyone running continuous PostgreSQL→PostgreSQL sync of tables that have a user-defined `ENUM` column.

## Install

```
# Linux/macOS/Windows binaries + checksums on the release page.
docker pull ghcr.io/sluicesync/sluice:v0.99.60
```
