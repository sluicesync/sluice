# sluice v0.99.50

**Postgres array columns now sync over CDC (Bug 144).** If you run continuous
sync from a Postgres source and a table has an array column (`int4[]`, `text[]`,
`numeric[]`, `timestamptz[]`, …), the stream used to wedge on the first
INSERT/UPDATE/DELETE to that table with `unsupported column type OID
1007/1009/1231…`. Arrays now stream end-to-end.

## Fixed

- **Postgres array columns over CDC (Bug 144).** The pgoutput CDC reader's type
  resolver (`oidToType`) had no array-OID cases, so the first array DML on a
  Postgres source table loudly stopped the sync stream — even though the initial
  bulk copy (cold-start) handled arrays fine. Now:
  - array OIDs resolve to the IR array type by mapping each array OID to its
    element OID and recursing, so an array element decodes **identically** to the
    same scalar column;
  - the array-value decoder also accepts the `[]byte` text form the pgoutput wire
    delivers (it previously handled only the cold-start `[]any` / `string`
    shapes, and silently mis-walked the raw text bytes);
  - **multi-dimensional arrays are preserved (not flattened), NULL elements
    survive as NULLs, `numeric[]` keeps full scale**, and quoted/escaped text
    elements (commas, quotes, braces, backslashes) round-trip.

## Compatibility / notes

- No flag, config, or format change — array CDC just works now.
- This was always a **loud** failure (the stream stopped with a clear error),
  never silent data loss. If a purged/older release wedged your stream on an
  array column, upgrading and resuming picks up cleanly.
- Unchanged by design: `timetz[]`, `bytea[]`, and `json[]`/`jsonb[]` arrays
  still **refuse loudly at apply** rather than risk a lossy translation — no
  silent acceptance.
- A parity guard now pins the CDC array type-registry to the schema reader's so
  the two can't drift (the same class of gap as the earlier Bug 97/118 fix).

## Who needs this

- Anyone running **continuous sync (CDC) from a Postgres source** whose schema
  includes array columns. Migration/backup (cold-start) was unaffected; this is
  specifically the live-replication path.

## Install

```
# Linux/macOS/Windows binaries + checksums on the release page.
docker pull ghcr.io/sluicesync/sluice:v0.99.50
```
