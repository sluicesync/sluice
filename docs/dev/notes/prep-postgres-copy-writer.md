# Prep: Postgres COPY-protocol writer

> **Status: SHIPPED in v0.1.0** — `chanCopySource` adapter wrapping pgx `CopyFrom` is the canonical PG bulk-load path.

Roadmap reference: [docs/dev/roadmap.md §6](../roadmap.md). Related: [PlanetScale's pgcopydb fork](https://github.com/planetscale/pgcopydb), referenced in [CLAUDE.md](../../../CLAUDE.md) as the tactical inspiration.

## Goal

Replace the Postgres `RowWriter`'s batched-INSERT bulk path with `COPY FROM STDIN`, the native Postgres bulk-load protocol. COPY is documented at 3-5× the throughput of multi-row INSERTs at scale (matches what pgcopydb achieves) and is the canonical way to load Postgres in volume. The current PG writer already declares `BulkLoad: BulkLoadCopy` in its capabilities, but the implementation falls back to batched INSERTs everywhere — this chunk wires the declaration to a real implementation.

Out of scope:

- **Performance benchmarks.** Adding a bench harness alongside the writer is appealing but ships separately; a real-world dataset is needed to make the numbers meaningful, and that's a different chunk.
- **MySQL `LOAD DATA INFILE`.** The MySQL side has the same write-speed problem and the `BulkLoadLoadDataInfile` capability constant exists, but MySQL's LOAD-DATA path has more moving pieces (file vs. inline data, server-side vs. client-side, `LOCAL` permissions). Separate chunk.
- **Binary COPY format.** pgx supports both text and binary; binary is slightly faster but adds per-type encoding complexity. v1 uses text format (the default for `pgx.Conn.CopyFrom` with primitive Go values), revisits binary if benchmarks justify.
- **Concurrent COPY across tables.** The Migrator copies tables sequentially today; per-table parallelism would amplify the COPY win, but it's a separate orchestrator-level chunk.

## Library choice

pgx's high-level `pgx.Conn.CopyFrom` interface, not the lower-level `pgconn.CopyFrom`. The high-level form takes:

```go
func (c *pgx.Conn) CopyFrom(
    ctx context.Context,
    tableName pgx.Identifier,
    columnNames []string,
    rowSrc pgx.CopyFromSource,
) (int64, error)
```

`CopyFromSource` is a small interface (`Next() bool`, `Values() ([]any, error)`, `Err() error`) we can implement on top of the existing `<-chan ir.Row`. pgx handles the wire-format encoding via its `pgtype.Map`, so per-type encoding work we'd otherwise duplicate doesn't appear in this chunk.

## Files to add / touch

New files:

- `internal/engines/postgres/copy_source.go` — the `chanCopySource` adapter that turns the IR's `<-chan ir.Row` into a `pgx.CopyFromSource`. Pure bridging code; the existing `prepareValue` helper handles the IR-canonical → pgx-acceptable conversions (already used by the BatchedInsert path).
- `internal/engines/postgres/copy_source_test.go` — unit tests for the adapter. No Docker; assert Next/Values/Err semantics with a manual channel.

Modify:

- `internal/engines/postgres/row_writer.go` — `RowWriter` gains a `useCopy bool` field; `WriteRows` dispatches to a new `writeViaCopy` path when set, falling back to the existing batched-INSERT path otherwise.
- `internal/engines/postgres/engine.go` — `OpenRowWriter` consults the engine's `Capabilities().BulkLoad` and sets `useCopy = (capability == BulkLoadCopy)`. Vanilla PG always opts into COPY; future flavors can opt out via their capability declaration.
- `internal/engines/postgres/row_writer_test.go` — existing integration tests should pass unchanged (the public WriteRows contract is unchanged); a new test ensures the COPY path actually runs (verifies via test-only flag or by bypassing the dispatch).

## Data flow sketch

```
[Streamer / Migrator]
  rw.WriteRows(ctx, table, rowsCh)
    │
    ▼
[RowWriter.WriteRows]
  if useCopy:
      → writeViaCopy(ctx, table, rowsCh)
            db.Conn(ctx)
              .Raw(func(driverConn) {
                  pgxConn := driverConn.(*stdlib.Conn).Conn()
                  source := newChanCopySource(rowsCh, table)
                  _, err := pgxConn.CopyFrom(
                      ctx,
                      pgx.Identifier{schema, table.Name},
                      columnNames(table),
                      source,
                  )
              })
  else:
      → writeViaBatch(...) (existing path, unchanged)
```

`pgx.Identifier` is pgx's type-safe identifier wrapper that handles its own quoting; we don't need to feed it pre-quoted strings. `columnNames(table)` is a one-liner returning `[]string` from the IR `Table.Columns`.

The `db.Conn(ctx).Raw(...)` dance is necessary because `database/sql` doesn't expose pgx-specific operations directly. We acquire a single pinned conn for the duration of the COPY, run COPY against the underlying `*pgx.Conn`, and release the conn back to the pool when done.

## `chanCopySource` adapter shape

```go
// chanCopySource adapts an ir.Row channel to pgx.CopyFromSource.
// Next blocks on the channel; if ctx (captured in the source) is
// cancelled, Next returns false and Err reports ctx.Err().
type chanCopySource struct {
    ctx     context.Context
    rows    <-chan ir.Row
    table   *ir.Table
    current []any
    err     error
}

func (s *chanCopySource) Next() bool {
    select {
    case row, ok := <-s.rows:
        if !ok {
            return false
        }
        values := make([]any, len(s.table.Columns))
        for i, col := range s.table.Columns {
            v, err := prepareValue(row[col.Name], col.Type)
            if err != nil {
                s.err = err
                return false
            }
            values[i] = v
        }
        s.current = values
        return true
    case <-s.ctx.Done():
        s.err = s.ctx.Err()
        return false
    }
}

func (s *chanCopySource) Values() ([]any, error) { return s.current, nil }
func (s *chanCopySource) Err() error             { return s.err }
```

The existing `prepareValue` already handles the IR-canonical-value → pgx-acceptable conversions (notably array-element retyping). Reusing it keeps the COPY and BatchedInsert paths in lockstep on value semantics — so a value that round-trips correctly through one path also round-trips through the other.

## Open questions for the user

1. **Use COPY by default for vanilla PG.** Above. The capability already declares `BulkLoadCopy`; this just wires the declaration. *Recommendation:* yes. Confirm?
2. **Fallback on COPY error.** If COPY fails mid-stream (network drop, server reject), should the writer retry via BatchedInsert? *Recommendation:* no — surface the error. COPY is the documented fast-path; if it fails, that's a real signal worth surfacing rather than silently slowing down. The simple-mode Migrator's fail-and-restart loop handles transient errors at a higher level. Confirm?
3. **Text format vs binary format COPY.** Text is the high-level pgx default and works for every IR-canonical type via pgx's encoding. Binary is slightly faster for numeric-heavy workloads. *Recommendation:* text in v1. Revisit binary if benchmarks show a meaningful gap; that's a follow-up chunk with measurable justification.
4. **Identity column behavior under COPY.** PG's `GENERATED BY DEFAULT AS IDENTITY` accepts user-supplied values via COPY (we already use this form per the cross-engine compatibility decision in §1). `GENERATED ALWAYS AS IDENTITY` would reject user-supplied values with `ERROR: cannot insert a non-DEFAULT value into column ...`. *Recommendation:* document the constraint; sluice's schema writer already emits `BY DEFAULT`. Confirm no policy change needed?
5. **Per-table COPY transaction shape.** COPY runs as one statement per table; if it errors mid-stream the whole COPY rolls back. The Migrator already runs each table's bulk-copy independently, so this matches the current shape. *Recommendation:* keep one COPY per table (no explicit BEGIN/COMMIT around it; the COPY is atomic by itself). Confirm?

## Anticipated rough edges

- **NULL handling.** pgx's high-level `CopyFrom` handles `nil` in the Values slice as SQL NULL natively. No special encoding needed in `chanCopySource`.
- **Array values.** `prepareValue` already converts `[]any` (the IR canonical form) to typed Go slices that pgx serialises as Postgres arrays. The COPY path inherits this for free.
- **Long-running COPY blocks ctx cancellation on the inner read.** pgx's CopyFrom honors ctx for the network round-trip but the source's `Next()` is what reads from our channel. Adapter handles ctx in its select; pgx aborts the COPY when the source returns false with a non-nil err. Tested by the unit test.
- **Connection-pool semantics.** `db.Conn(ctx)` pins one connection for the COPY's duration. We must release it (via `sqlConn.Close()`) when done so the pool is healthy. `defer sqlConn.Close()` covers it.
- **Schema name quoting.** `pgx.Identifier{"public", "users"}` produces correctly quoted output (`"public"."users"`). We don't need to call our own `quoteIdent` for the COPY path — pgx handles it. The BatchedInsert path keeps its own quoting because it builds raw SQL.
- **Empty channel.** If the input channel closes immediately without a single row, COPY runs with zero rows — pgx handles this fine. WriteRows returns nil.

## Suggested first-cut prompt for Claude Code

> "Read CLAUDE.md, docs/dev/notes/prep-postgres-copy-writer.md, and the existing internal/engines/postgres/row_writer.go. Propose the design before writing code: (1) the exact chanCopySource adapter API, (2) the writeViaCopy implementation including the db.Conn → Raw dance to get *pgx.Conn, (3) the dispatch logic in WriteRows, (4) the integration test that verifies COPY actually runs (not just that rows arrive). Note any deviation from the prep doc with a why. Stop after the design for review."
