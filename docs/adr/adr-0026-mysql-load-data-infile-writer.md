# ADR-0026: MySQL LOAD DATA LOCAL INFILE row writer

## Status

Accepted. Implemented in `internal/engines/mysql/load_data_writer.go`
(streaming TSV serializer + LOAD DATA dispatch),
`internal/engines/mysql/row_writer.go` (strategy switch on
`Capabilities.BulkLoad`), and `internal/engines/mysql/flavor.go`
(unchanged — vanilla already declared `BulkLoadLoadDataInfile`; this
ADR brings the implementation up to the declaration). Capability
constant `ir.BulkLoadLoadDataInfile` predates this work.

## Context

[ADR-0005](adr-0005-mysql-flavors.md) introduced the Flavor concept and
declared vanilla MySQL's `BulkLoad` as `LoadDataInfile`, but the
`RowWriter` shipped with only the BatchedInsert path —
`BulkLoadLoadDataInfile` and `BulkLoadBatchedInsert` both fell through
to `writeBatched`. The TODO comment in `row_writer.go` named the work
explicitly. Bulk-loading via `LOAD DATA LOCAL INFILE` is typically
5–10× faster than parameter-bound multi-row INSERTs because the server
parses one statement and one stream rather than N quoted-literal
statements; pscale-cli's dumper, mydumper, and pgcopydb's MySQL-side
proof-of-concept all use it for the bulk phase.

The shape of the work has three sub-decisions:

1. **How to feed bytes to the server.** Three options: a real file
   (LOAD DATA INFILE without LOCAL — requires server filesystem
   access; rules out cloud-managed MySQL), `LOAD DATA LOCAL INFILE
   '<file>'` with go-sql-driver's `RegisterLocalFile` /
   `?allowAllFiles=true` (requires writing a temp file), or `LOAD
   DATA LOCAL INFILE 'Reader::<name>'` with the driver's
   `RegisterReaderHandler` mechanism (driver pipes from a Go
   `io.Reader` over the LOCAL INFILE protocol — no real file
   touched). `RegisterReaderHandler` is the only option that doesn't
   require either privileged server filesystem access or
   the security risk of `?allowAllFiles=true`, and it composes with
   the streaming RowReader → channel → RowWriter pipeline without
   buffering an entire table to disk.

2. **How to serialize values.** TSV is the LOAD DATA default and the
   simplest format with documented escape rules: tab field separator,
   newline row separator, backslash escape, `\N` for SQL NULL. CSV
   would need RFC 4180-quoted fields and a `FIELDS ENCLOSED BY '"'`
   clause; gain is nil. The `prepareValue` helper that already shapes
   per-type Go values for the BatchedInsert driver path applies
   verbatim — the LOAD DATA path uses the same shaped values, just
   serializes them differently.

3. **What to do when the server doesn't allow it.** MySQL 8.0+ ships
   with `local_infile=OFF` by default (security hardening — a hostile
   server-side `LOAD DATA LOCAL INFILE` can read arbitrary client
   files). Operators have to set it ON. Some managed services
   (PlanetScale, Vitess-backed clusters) disallow `LOAD DATA LOCAL
   INFILE` entirely, which is why PlanetScale's flavor declares
   `BulkLoadBatchedInsert`. For vanilla flavor, sluice has to handle
   the "operator hasn't enabled local_infile" case without crashing
   mid-stream.

## Decision

**Streaming via `RegisterReaderHandler`.** Each `WriteRows` call mints
a unique handler name (`sluice_loaddata_<8 random hex bytes>`),
registers an `io.Pipe` reader on the package-level driver registry,
runs `LOAD DATA LOCAL INFILE 'Reader::<name>' INTO TABLE …`, then
deregisters on return. The encoder goroutine writes TSV bytes to the
pipe writer; the driver reads them as the server requests data
packets. No real file is created and `?allowAllFiles=true` is not
required (the `Reader::` prefix in the registered name is the
mechanism the driver uses to disambiguate handler-backed paths from
filesystem ones).

**TSV with default escape rules.** The serializer escapes `0x00`
(`\0`), `\t` (`\t`), `\n` (`\n`), `\r` (`\r`), and `\\` (`\\`); other
bytes pass through unchanged. NULL emits the literal `\N`. The four
recognised escapes are MySQL's documented LOAD DATA inverses (under
the default `FIELDS TERMINATED BY '\t' ESCAPED BY '\\'`); declaring
the separators explicitly in the SQL keeps the serializer's escape
table coupled to the statement form in one place.

**`CHARACTER SET binary` plus per-column `SET` clauses.** Two MySQL-isms
forced this:

- Without `CHARACTER SET binary`, the server validates every input
  byte against the connection's `character_set_database` (utf8mb4)
  and rejects on the first non-ASCII byte in a `BLOB` / `VARBINARY`
  column with Error 1300. The default would be silently broken for
  any binary column.
- With `CHARACTER SET binary`, the server flips the other way:
  `JSON` columns reject input as "Cannot create a JSON value from a
  string with CHARACTER SET 'binary'" because JSON requires a
  Unicode-tagged input stream.

The fix is to load every field into a user variable (`@c0`, `@c1`,
…) and assign the real columns via a `SET` clause. JSON, VARCHAR,
TEXT, and SET columns get `CONVERT(@cN USING utf8mb4)` — the bytes
are unchanged but the charset tag is corrected. Numeric, temporal,
BLOB, VARBINARY, and other types take the variable verbatim.
Generated columns are excluded upstream (the row reader doesn't emit
values for them).

**Per-call fallback to BatchedInsert.** Two conditions force a
fallback:

1. `SELECT @@local_infile` returns `0` / `OFF`. Pre-flighting one
   query at WriteRows entry catches the disabled-by-default case;
   the alternative is the server rejecting the statement
   mid-stream after the encoder has started producing bytes,
   which is hard to debug from the client side.
2. The table has an `ir.Geometry` column. MySQL's geometry wire
   format is `<srid uint32 LE><wkb>`; LOAD DATA can't apply
   `ST_GEOMFROMWKB()` or the SRID prefix without the operator
   hand-rolling a SET clause per geometry column. Falling back is
   the conservative move; geometry is an extension type and
   geometry-heavy migrations are rare.

Both fallback paths emit one structured WARN line naming the table
(and the gating condition's hint, e.g. "set local_infile=ON for ~5–10×
faster bulk load") then call `writeBatched(ctx, table, rows)`. The
fallback's exit signal is "rows landed via SELECT" — there's no
silent success-on-zero-rows hazard.

## What this design deliberately does not do (yet)

- **No real-file LOAD DATA path.** `LOAD DATA INFILE` (without LOCAL)
  is faster on bare-metal MySQL because the server reads the file
  directly, but it requires either filesystem access on the server
  (impossible on managed services) or explicit `secure_file_priv`
  configuration. The `Reader::` path covers the full deployment
  matrix at a small streaming-throughput cost.
- **No automatic enabling of `local_infile`.** The flag is global
  and changing it requires `SUPER` / `SYSTEM_VARIABLES_ADMIN`, which
  sluice's connecting account typically lacks (and shouldn't need
  for the bulk-copy use case). The pre-flight + warning + fallback
  is the right shape; operators who want the speedup explicitly opt
  in by setting `local_infile=ON` server-side.
- **No table-level `LOCK` / `INSERT BUFFER` tuning.** `ALTER TABLE …
  DISABLE KEYS` is MyISAM-only; the three-phase apply
  ([ADR-0004](adr-0004-three-phase-apply.md)) already defers
  index/constraint creation, so the LOAD DATA stream lands into a
  bare-table InnoDB tablespace without secondary-index churn. No
  additional bulk-load session variables are toggled.
- **No statement / batch chunking.** Unlike the BatchedInsert path
  which breaks rows into N-row INSERTs, the LOAD DATA path streams
  the entire input channel as one statement. MySQL's
  `max_allowed_packet` matters at the wire level (default 64 MiB on
  8.0+) but the driver's packet framing handles streams larger than
  one packet without operator intervention. Future
  `--max-buffer-bytes` work (ADR-0028, in flight) can introduce a
  byte-driven chunk break if it surfaces as a real bottleneck.
- **No parallel within-table LOAD DATA.** [ADR-0019](adr-0019-parallel-within-table-bulk-copy.md)
  parallelises bulk-copy across N PK ranges; each range's writer is
  one LOAD DATA stream. The orchestrator already handles
  `RowWriter` parallelism table-side; the LOAD DATA path is a
  drop-in replacement at the per-writer level.

## Consequences

**Throughput.** Vanilla MySQL bulk-copy moves from
parameter-bound multi-row INSERTs (typical 50–80 MB/s sustained on
loopback) to LOAD DATA streaming (typical 200–400 MB/s on the same
host). The win is largest on wide-row tables where INSERT statement
parsing dominated CPU; narrow-row throughput is closer to 2–3×.
Numbers will be re-measured against the soak fixture once the
cross-engine (PG → MySQL) integration test is updated to cover
the LOAD DATA path with `local_infile=ON`.

**Operator-facing precondition.** `local_infile=ON` is now a
performance-affecting toggle for vanilla MySQL targets. The fallback
keeps migrations *correct* without it; operators wanting the
throughput win pass `--local-infile=ON` to `mysqld`, set
`local_infile=ON` in `my.cnf`, or run `SET GLOBAL local_infile=1`
before the migration starts. The WARN line surfaces the hint.

**Charset hand-shape wart.** The CHARACTER SET binary + per-column
SET-clause CONVERT() shape is load-bearing — without it either binary
columns or JSON columns fail. The wart has a name (`columnSetExpr`),
a focused unit test (`TestColumnSetExpr`), and a comment in the SQL
builder. Adding a new IR type that needs charset re-tagging is a
one-line addition to `columnSetExpr`'s switch.

**Geometry stays on BatchedInsert.** Operators migrating spatial-heavy
schemas don't see the LOAD DATA speedup. The fallback's WARN line
names the offending column, so the cause is diagnosable from one log
line. A future revision can add geometry-specific SET-clause handling
(`ST_GEOMFROMWKB(@cN, srid)`) but the IR's geometry-SRID story isn't
fully baked yet — the BatchedInsert path's existing little-endian
prefix construction in `prepareValue` already works.

**Driver-registry concurrency.** `RegisterReaderHandler` mutates a
package-level map under a mutex, but the per-call random-name suffix
means concurrent writers on the same engine instance can't collide on
slot. `defer DeregisterReaderHandler(name)` cleans up even on early
return. Per-call cost is one `crypto/rand.Read(8 bytes)` plus the map
write; both are negligible compared to the LOAD DATA round-trip.

**Tested edges.** Unit tests cover TSV escape rules (tab, newline,
CR, backslash, NUL, mixed, empty string, empty bytes, time-tz
normalisation, unsupported types), column SET-expression dispatch,
and the SQL-statement shape (with and without schema qualifier).
Integration tests cover a multi-type happy path (round-tripping
varchar/decimal/JSON/blob/timestamp/enum/set), an escape-edges table
(real tabs, newlines, CRs, backslashes, NULs, NULLs, empty values
in both VARCHAR and VARBINARY), a 5,000-row large batch, and the
local_infile=OFF fallback. The pre-existing `TestRowWriter_RoundTrip`
and `TestRowWriter_LargeBatch` tests now exercise the fallback path
on testcontainers (where setting `local_infile=ON` requires `SUPER`,
which the default `test` user lacks); the LOAD DATA-specific tests
explicitly enable the variable as `root`.
