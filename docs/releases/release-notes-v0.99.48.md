# sluice v0.99.48

**Completes default-on schema-change forwarding for the last source path: PlanetScale / Vitess (VStream).** If you run a live PlanetScale-source sync and an `ADD COLUMN` (or `DROP COLUMN` / `ALTER COLUMN TYPE`) on the source used to wedge the stream with a `42703` / `1054` "column does not exist" after a cold start, this is the fix.

## Fixed

- **Online schema changes forward on a PlanetScale / Vitess (VStream) source after a cold start (ADR-0091, F7c).** The default-on forwarding shipped in v0.99.45–46 worked for self-hosted MySQL (binlog) and Postgres (pgoutput) sources, but **not** for a VStream source that had cold-started: the VStream cold-start CDC tail (`dispatchCDCEvent`) is a separate dispatch from the standalone reader, and its FIELD branch only cached the field list — it never emitted the `ir.SchemaSnapshot` boundary the forward intercept consumes. So a source DDL after a cold start never forwarded, and the post-DDL row failed to apply (`42703` on PG / `1054` on MySQL). A *warm-resumed* VStream stream was unaffected, which is why it slipped past earlier testing. The cold-start FIELD branch now emits the boundary, exactly as the standalone reader does.
- **Flavor-aware CDC normalization for VStream.** The VStream FIELD projection is lower-fidelity than binlog's `information_schema` re-read — it carries no primary key, no secondary indexes, and not reliably charset/collation. The cold-start seed (a full schema read) now strips those for VStream flavors so a seed-vs-first-CDC comparison doesn't surface a phantom PK-index-drop or ALTER-TYPE and refuse a real ADD COLUMN. Vanilla MySQL binlog is unchanged.

## Compatibility / limitations

- No flag or config change. With this, the ADR-0091 §1d forwarding matrix holds across **all three source paths** — MySQL binlog, Postgres pgoutput, and PlanetScale/Vitess VStream.
- Documented limitations on a **VStream source** (the wire doesn't carry the metadata): `CREATE`/`DROP INDEX` and charset-only `ALTER` cannot be forwarded; `RENAME COLUMN` continues to refuse (no stable column id on a MySQL-family source). Any such change surfaces as a loud apply error, never silent loss.

## Who needs this

- Anyone running a **live PlanetScale (or self-hosted Vitess) source sync** who performs routine column schema changes — the stream now stays online through an ADD/DROP/ALTER COLUMN instead of wedging on a `42703`/`1054`.

## Install

```
# Linux/macOS/Windows binaries + checksums on the release page.
docker pull ghcr.io/sluicesync/sluice:v0.99.48
```
