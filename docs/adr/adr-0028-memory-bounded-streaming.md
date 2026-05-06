# ADR-0028: Memory-bounded streaming via `--max-buffer-bytes`

## Status

Accepted. Implemented in:

- `internal/ir/bytes.go` (shared `ApproximateRowBytes` /
  `ApproximateChangeBytes` helpers, exported so engines can reuse the
  byte-estimation policy without importing the pipeline package).
- `internal/ir/interfaces.go` (optional `MaxBufferBytesSetter`
  interface — the byte cap is opt-in per engine surface).
- `internal/engines/{mysql,postgres}/row_writer.go` and
  `row_writer_batch.go` (byte-cap flush in the batched-INSERT and
  idempotent-INSERT paths).
- `internal/engines/{mysql,postgres}/change_applier_batch.go`
  (byte-cap flush in the CDC `ApplyBatch` path).
- `internal/pipeline/migrate.go` (`Migrator.MaxBufferBytes` plumbed
  to row writers via `applyMaxBufferBytes`; threaded through to
  `parallelBulkCopyDeps` so per-chunk writers honour the cap).
- `internal/pipeline/streamer.go` (`Streamer.MaxBufferBytes` plumbed
  to applier and cold-start row writer).
- `cmd/sluice/cli.go` (`--max-buffer-bytes` flag on `migrate` and
  `sync start`, default `67108864` = 64 MiB).

## Context

Through v0.6.x the bulk-copy and CDC apply paths bound buffered work
strictly by row count: bulk copy via `--bulk-batch-size` (default
5000) and CDC apply via `--apply-batch-size` (default 1; production
tuning lands at 100–500). For typical OLTP rows that's fine — 5000
narrow rows is well under 10 MB of heap. For columns at MB scale
(TEXT documents, BYTEA blobs, JSON aggregates, EWKB geometry), a
5000-row batch can easily pin hundreds of MB before the writer
flushes; a 500-change CDC apply against a stream that's mostly TEXT
updates can hold a single transaction's parameter slice in the high
hundreds of MB.

The risk is operational: the operator picks a batch size that's
right for narrow rows, hits a wide-row workload, and the tool's RSS
spikes unpredictably. There's no mechanism to say "batch up to N
rows, but don't let any single batch exceed M bytes of buffered
data". That's what `--max-buffer-bytes` is.

PlanetScale's `pscale-cli` dumper had this lesson built in already:
its bulk-INSERT batcher flushes at ~1 MB of statement body rather
than a fixed row count. Per ADR-0017's adopt-the-pscale-defaults
note, this is the same shape we want here.

### Where memory accumulates today

Audit walking the row-streaming and apply paths from source to
target:

- **Row readers.** Both `internal/engines/postgres/row_reader.go`
  and `internal/engines/mysql/row_reader.go` stream
  one-row-at-a-time off `pgx.Rows` / `database/sql.Rows` onto an
  unbuffered channel. At most one row in flight in the reader's
  goroutine and one row in flight on the channel send.
  **Verdict: not the source of accumulation.**

- **The reader→writer channel.** `teeRows` in
  `internal/pipeline/progress.go` returns an unbuffered channel; the
  tee goroutine forwards row-by-row with backpressure. **Verdict:
  not the source of accumulation.**

- **Postgres COPY-protocol writer**
  (`row_writer.go::writeViaCopy`). Uses `pgx.CopyFrom` with our
  `chanCopySource` adapter. pgx frames rows on the wire; each row's
  values pass through the adapter once and are sent to the server
  in protocol-sized chunks. **Verdict: streaming; bounded by the
  driver's wire-frame buffer, not by anything we control.**

- **MySQL `LOAD DATA LOCAL INFILE` writer**
  (`load_data_writer.go`). Streams rows as TSV through an
  `io.Pipe` to the driver. **Verdict: streaming; bounded by the
  pipe's chunk size, not row count.**

- **Postgres batched-INSERT writer**
  (`row_writer.go::writeViaBatch`) and **MySQL batched-INSERT
  writer** (`row_writer.go::writeBatched`). Both accumulate rows
  in a `[]ir.Row` up to `maxRowsPerBatch` (default 500) before
  building a multi-row `INSERT ... VALUES` and `Exec`'ing it.
  **Verdict: this is one of the two real accumulators.** The
  bound is row count only; wide rows aren't penalised.

- **Per-batch idempotent-INSERT writers** (the resume path in
  `row_writer_batch.go` for both engines). Same accumulator shape
  as the batched-INSERT path with a different SQL template. Same
  unboundedness on bytes.

- **Postgres `ApplyBatch`**
  (`change_applier_batch.go::applyOneBatch`) and **MySQL
  `ApplyBatch`** (same file in mysql package). Both keep an open
  target transaction across up to `maxBatchSize` source changes.
  Each `dispatch` call binds the change's row values into an
  `Exec` parameter slice; the driver's parameter buffer for an
  open tx grows linearly with cumulative bound bytes. **Verdict:
  this is the second real accumulator.** Same row-count-only
  bound.

- **Change-applier per-change `Apply` path.** One change per
  target transaction; no buffered accumulation across changes.
  **Verdict: not the source of accumulation; not in scope for the
  cap.**

The fix targets the four real accumulators: bulk-INSERT writer
(both engines), idempotent-INSERT writer (both engines), and
`ApplyBatch` (both engines). The COPY/LOAD DATA streaming writers
and the row-by-row Apply path are unchanged.

### Options considered

1. **Strict per-row size measurement** (serialise each row's binary
   form, sum exactly). Accurate but expensive — every row pays a
   second serialisation cost just to be measured. The progress-
   ticker work in v0.5.0 (ADR-0019) already settled on a cheap
   approximation, and the same approximation is plenty for a flush
   trigger: the cap is a soft bound, not a per-byte invariant.

2. **A separate accounting goroutine** that watches buffered work
   externally. Adds a moving part for no real benefit; the
   accumulators all run single-threaded inside the writer/applier.

3. **Reuse `approximateRowBytes` directly inline.** The helper was
   pipeline-package private through v0.6.x. Hoist it into
   `internal/ir`, expose `ApproximateRowBytes` /
   `ApproximateValueBytes` / `ApproximateChangeBytes`, and have the
   writer/applier loops call them on every row/change. Cheap (no
   allocations, single map walk), already covered by tests, already
   the policy of record for byte-flavoured work.

Option 3 picked.

## Decision

Add `--max-buffer-bytes N` on both `sluice migrate` and
`sluice sync start`, defaulting to `67108864` (64 MiB). Plumbed
through `pipeline.Migrator.MaxBufferBytes` and
`pipeline.Streamer.MaxBufferBytes` to engine-side surfaces that opt
in via the `ir.MaxBufferBytesSetter` interface. Engines that don't
implement the setter retain their pre-v0.7.0 row-count-only
behaviour — the cap is fully opt-in per surface.

Inside each accumulator:

- **Bulk-INSERT writer (both engines).** Track running
  `batchBytes int64`; on each row append, add
  `ir.ApproximateRowBytes(row)`. Flush when either
  `len(batch) >= maxRowsPerBatch` *or*
  `batchBytes >= maxBufferBytes`. Same shape on the idempotent-
  INSERT (resume) path.

- **`ApplyBatch` (both engines).** Track running
  `batchBytes int64`; on each successful `dispatch`, add
  `ir.ApproximateChangeBytes(c)`. After the dispatch, if
  `batchBytes >= maxBufferBytes`, commit the in-flight tx and
  return so the outer loop starts a new batch. The check is *after*
  dispatch so the just-dispatched change is included in the
  committed-batch count.

The cap is a **soft target**, not a hard limit. A single row larger
than the cap still applies — by the time the post-append check
fires, the row is already in the batch slice (and, for the applier,
already dispatched into the open tx). The alternative — refusing
the row — would silently break otherwise-valid migrations of tables
whose normal rows happen to exceed the cap. Operators who want a
hard limit can use the row-count cap; the byte cap exists to bound
*accumulation*, not individual rows.

The `ir.ApproximateRowBytes` helper was hoisted from
`internal/pipeline/progress.go` into a new `internal/ir/bytes.go`
file. The pipeline-package `approximateRowBytes` is retained as a
thin pass-through so the existing pipeline-package tests keep
working unchanged.

## What this design deliberately does not do

- **No byte cap on the COPY / LOAD DATA paths.** Both already
  stream rows to the wire row-by-row through driver-controlled
  buffers; there's no in-process accumulation to bound. Adding a
  cap here would force splitting one table's copy into multiple
  COPY operations, which breaks the COPY-is-atomic-per-table
  invariant documented in `writeViaCopy`'s file-header comment.

- **No byte cap on the per-change `Apply` path.** One change per
  target transaction means no cross-change accumulation.

- **No byte cap on the reader.** Readers stream
  one-row-at-a-time onto unbuffered channels. The per-row buffer
  is the row itself; there's no batching to bound.

- **No precise bytes counting.** `ApproximateRowBytes` is
  intentionally rough (per the v0.5.0 progress-metric work):
  fixed-width types use natural byte width, strings/[]byte use
  length, time.Time uses 24, unknown engine-specific shapes (e.g.
  `pgtype.Numeric`, raw geometry WKB) contribute zero. The cap is
  therefore a lower-bound estimator on real wire bytes, which is
  the safer direction for a memory bound — actual heap usage may
  exceed the estimate slightly, but the *order of magnitude* is
  correct, and an operator setting `--max-buffer-bytes=64M` will
  see flushes at 64 MB of estimated content, not at 200 MB.

- **No interaction with `--apply-batch-size` / `--bulk-batch-size`
  beyond "whichever fires first".** Both caps stay active. Setting
  the byte cap to a tiny value can starve the row-cap; setting the
  byte cap above what the row cap would accumulate makes the byte
  cap a no-op. Both are tunable; the default 64 MiB is generous
  enough that typical workloads see no behavioural change.

## Consequences

The default value (64 MiB) was chosen for two reasons:

1. **Conservative for production hosts.** Most production
   migrations run on hosts with multiple GB of RAM available to
   the Go process. 64 MiB per stream is a small fraction of that;
   parallel-copy with N=8 chunks each at 64 MiB is 512 MiB total,
   still well under what a typical host has spare.

2. **Wide enough that typical workloads don't trip it.**
   `--bulk-batch-size=5000` × ~200-byte average row = 1 MB; the
   default cap is 64× that. The byte cap is invisible until the
   workload starts producing rows in the multi-KB range.

Operators with unusually narrow rows can raise the cap (or
practically, leave it at default — narrow rows don't accumulate to
64 MB anyway). Operators with extreme row sizes (multi-MB BYTEA
blobs) may want to *lower* the cap so a 50-row batch of 1-MB
documents flushes at 64 MB rather than waiting for the 5000-row
default.

The cap interacts well with parallel within-table copy
([ADR-0019](adr-0019-parallel-within-table-bulk-copy.md)): the cap
applies per-chunk, so a parallelism-of-8 copy with `--max-buffer-
bytes=64M` bounds peak heap per chunk at 64 MB. The orchestrator
opens N writers and applies the cap to each via the same setter
helper.

The cap is also forward-compatible with the COPY-protocol writer
work the roadmap envisages: when the COPY path eventually grows a
chunked-send tunable (it currently uses pgx defaults), the same
`MaxBufferBytesSetter` surface can plumb the value in without a new
interface.

## Related ADRs

- [ADR-0017](adr-0017-batched-cdc-apply.md) — the row-count cap
  this ADR layers atop on the CDC path.
- [ADR-0018](adr-0018-per-batch-bulk-copy-checkpointing.md) — the
  per-batch checkpointing path that already paid for byte-bounded
  semantics; this ADR makes the same call on the cold-start
  bulk-copy and CDC paths.
- [ADR-0019](adr-0019-parallel-within-table-bulk-copy.md) — the
  byte-estimation helper originated here; v0.7.0 hoists it from
  pipeline-private into `internal/ir` so engines can reuse it.
