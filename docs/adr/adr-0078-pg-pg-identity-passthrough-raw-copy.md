# ADR-0078: PG→PG identity passthrough — byte-pipe the raw COPY stream

## Status

Accepted. Builds on [ADR-0043](adr-0043-native-bulk-loader-on-parallel-copy-path.md) (the cold-start fast-loader gate this lane is slotted INTO), [ADR-0019](adr-0019-parallel-within-table-bulk-copy.md) (within-table PK-range chunking — each chunk byte-pipes its own range), [ADR-0076](adr-0076-cross-table-copy-worker-pool.md) / [ADR-0077](adr-0077-overlap-index-builds-with-bulk-copy.md) (the cross-table + index-overlap pools this composes with unchanged), and [ADR-0047](adr-0047-verbatim-extension-passthrough.md) (the engine-neutral `source.Name() == target.Name()` same-engine determination precedent). This is **phase (b)** of roadmap item 3b ("PG→PG copy throughput: index-build overlap + identity passthrough"); phase (a) (index-build overlap) shipped as ADR-0077.

This is a **concurrency chunk** (a local `io.Pipe` + two goroutines per chunk, running inside the existing copy pools) — the `-race` integration gate must pass before any tag (the dev box is `CGO_ENABLED=0` and cannot run `-race` locally; the gate is CI-only).

### Implementation notes (what landed)

- **The byte-pipe — `COPY (SELECT …) TO STDOUT` → `COPY tbl (…) FROM STDIN`.** The IR copy path decodes every source row into an `ir.Row` and re-encodes it via `pgx.CopyFrom` — the per-stream gap the at-scale comparison measured (99.5 vs 122.6 MB/s, ~23%; `docs/comparison-pgcopydb.md` "At scale"). The raw lane streams the source server's native COPY bytes straight into the target via `*pgconn.PgConn.CopyTo(w)` → `io.Pipe` → `*pgconn.PgConn.CopyFrom(r)`, escaping `database/sql` through the SAME `Conn.Raw` → `*stdlib.Conn` → `*pgx.Conn` → `PgConn()` path `RowWriter.writeViaCopy` already uses. No `ir.Row` is ever produced.

- **Engine-neutral OPTIONAL IR surfaces (`internal/ir/raw_copy.go`).** All carry OPAQUE bytes, never `ir.Row`:
  - `RawCopyExporter` — `ExportRawCopy(ctx, table, *RawCopyChunk, RawCopyFormat, io.Writer)`: the source streams its native COPY-TO-STDOUT bytes for the table/chunk projection.
  - `RawCopyImporter` — `ImportRawCopy(ctx, table, RawCopyFormat, io.Reader) (rowsCopied int64, err error)`: the target consumes same-engine bytes.
  - `RawCopyChunk{PKColumn, LowerPK, UpperPK any}` — nil ⇒ whole table; single integer PK only in v1 (matches the existing chunk path's `canParallelChunkTable` restriction). Lower exclusive, upper inclusive.
  - `RawCopyVersionProber` (folded into both exporter and importer) — `ServerMajorVersion(ctx) (int, error)`; the orchestrator probes BOTH and compares two ints (never naming an engine).
  - `RawCopyFormat` (`RawCopyText` default / `RawCopyBinary`).
  PG implements all of them (`internal/engines/postgres/raw_copy.go`). MySQL does not, so a MySQL pair never takes the lane.

- **The orchestrator wiring (`internal/pipeline/migrate_raw_copy.go`).** `runRawCopyChunk` connects exporter→importer with an `io.Pipe()` under an `errgroup`: the exporter writes `pw` in one goroutine (closing it on done so the importer sees EOF; `pw.CloseWithError` on failure), the importer reads `pr` in the other (`pr.CloseWithError` on import error so a still-writing exporter unblocks instead of deadlocking on a full pipe). Same-engine detection is `m.Source.Name() == m.Target.Name()` AND both rr/rw type-assert to the raw surfaces AND the gate holds — **never naming "postgres".**

- **THE GATE — one auditable predicate `rawCopyGate(m, schema) (ok, reason)`.** The byte-pipe bypasses the typed IR, i.e. EVERY value transform, so a gate miss would silently skip a transformation (a silent-loss class). The gate is the backstop, checked ONCE at migrate setup (in `runSingleDatabase`, AFTER the IR-mutation steps `ApplyMappings`/`ApplyExpressionOverrides`/`InjectShardColumn`), threaded as a bool into `parallelBulkCopyDeps.rawCopyOK`:
  - **G1 same engine:** `m.Source.Name() == m.Target.Name()` (else "cross-engine").
  - **G2 no redaction:** `m.Redactor == nil || m.Redactor.Empty()`.
  - **G3 no type/expr override:** `len(m.Mappings) == 0 && len(m.ExpressionMappings) == 0`.
  - **G4 no shard injection:** `!m.InjectShardColumn.Engaged()`.
  - **G6 per-table identity projection** (`identityProjection(table)`, re-checked at per-table dispatch so one odd table falls back without disabling the lane): on a same-engine, no-transform run the source-readable projection (generated + `SluiceInjected` columns excluded) and the target's non-generated column list derive from the SAME `*ir.Table`, so names/order/wire-type match by construction (G4 already excludes the only producer of a `SluiceInjected` column). What `identityProjection` adds is the v1 CONSERVATIVE exclusion: a table carrying `ir.ExtensionType` / `ir.VerbatimType` / `ir.Bit` / `ir.Geometry` (OID/wire-format-sensitive — the per-type COPY codecs in `row_writer.go` must run) routes to the IR path. Start strict, widen on evidence.
  ANY condition false → fall back to the existing IR copy path.

  **The CRUX sub-invariant (G6):** the exporter builds `COPY (SELECT <sourceReadableColumns>) TO STDOUT` — the SAME projection as `buildSelect`, generated columns EXCLUDED — NEVER a bare `COPY tbl TO STDOUT` (which would include generated columns and desync the column list). The importer builds `COPY tbl (<nonGeneratedColumns>) FROM STDIN` from the SAME column helper, so the two column lists line up by construction. Pinned by `TestBuildRawCopyToStmt_ProjectionExcludesGenerated` + `TestBuildRawCopyFromStmt_ColumnListMatchesExportProjection` (unit) and `TestRawCopy_GeneratedColumnRecomputed` (integration: the source generated value is NOT copied; the target recomputes it).

- **Scoping — cold-start-only by construction.** The raw lane is slotted INSIDE the ADR-0043 fast-loader branch (already cold-start-only via `useFastLoader`). In `copyChunk`, BEFORE `copyChunkFast`, if `rawCopyOK && identityProjection(table)` and rr/rw implement the raw surfaces → `copyChunkRaw` (mirrors `copyChunkFast`'s terminal-checkpoint/progress shape but calls `runRawCopyChunk`); else `copyChunkFast`. The whole-table single-stream path (`bulkCopyOneTable`, chunk == nil) gets the same check before `copyTable`, additionally guarded by `!resuming && !forceColdStart` (the fast-loader gate's cold-start conditions, which the chunked path already satisfies inside the `useFastLoader` branch). On crash/resume: `useFastLoader` gate (1) fails → the raw lane is unreachable, so resume replays through the idempotent IR path — correct, not a gap (pinned by `TestRawCopy_ResumeFallsBack`). Target is empty on cold-start (Bug 9), so the non-upsert `COPY FROM` can't collide. **`migrate` path ONLY** — `runBulkCopyWithOpts` (sync cold-start) stays on the IR path in v1 (same deferral ADR-0076/0077 made). Progress is incremented by `ImportRawCopy`'s `RowsAffected` at completion (a byte-pipe has no per-row visibility — documented).

- **Format — text default.** `--raw-copy-format=text|binary|auto` (default `text`; `auto` = binary-if-same-major-else-text). Text is cross-PG-major safe (pgcopydb's default); the win is eliminating decode/re-encode, NOT text-vs-binary. `negotiateRawCopyFormat` engages binary ONLY when requested AND both endpoints' server majors match; a mismatch or a probe error downgrades to text LOUDLY (INFO), never silently. Binary stays excluded for the extension/verbatim/bit/geometry tables regardless (those don't take the lane at all under G6).

- **Observability seam.** A test-only package var `rawCopyTakenObserver func(table string)` fires every time a chunk/table is byte-piped, so integration tests assert the lane was actually TAKEN (a green zero-loss test alone can't distinguish the byte-pipe from the IR fallback) — the same disposition as ADR-0077's `onTableCopiedObserver`.

## Context

The 110 GB / 43-table at-scale comparison showed pgcopydb ~1.75× faster end-to-end after item 3 (cross-table) and item 3b(a) (index overlap) closed the structural gaps. The remaining per-stream difference (~23%) is that pgcopydb byte-pipes the raw COPY stream with zero per-value work, while sluice decodes every row into the typed IR and re-encodes it — the price of IR-first generality (cross-engine, redaction, type-overrides, value-fidelity). For a same-engine, no-transform PG→PG copy that price buys nothing: the bytes the source emits are exactly the bytes the target wants. This ADR adds the same-engine fast lane that does what pgcopydb does — falling back to the IR path the moment any transform is present.

## Decision

Add a same-engine raw-copy passthrough fast lane that byte-pipes `COPY … TO STDOUT` → `COPY … FROM STDIN` via pgx's raw `pgconn`, composed with the existing chunk machinery (per-chunk `COPY (SELECT … WHERE pk range) TO STDOUT`). Engage it ONLY behind a single auditable value-fidelity gate that proves there is no transform to skip; fall back to the IR path otherwise. Default to text COPY format (cross-major safe); binary is opt-in on matched majors.

### Why the gate is the load-bearing surface (value-fidelity lens)

The whole correctness argument is: *a byte-pipe is faithful precisely because nothing is supposed to change.* The gate is what makes "nothing is supposed to change" true. Every transform sluice can apply (redaction, type-override, expression-override, shard-injection) lives in the IR row pipeline the byte-pipe skips, so each is a gate condition; a missed condition is a silently-skipped transformation — the silent-loss class the project's tenets exist to prevent. So the gate is a single pure function with an exhaustive negative-test matrix (`TestRawCopyGate`: each transform present → ok=false with the right reason; the all-clear positive → ok=true), and every negative is also proven end-to-end (`TestRawCopy_FallbackOn{Redaction,TypeOverride,ExprOverride,ShardInjection}`: the raw lane is NOT taken AND the transform IS applied on the target).

### Why text is the default (not binary)

Binary COPY is faster on the wire but version/codec-sensitive across PG majors. The measured win is the elimination of the decode→IR→re-encode CPU, which both formats get; text additionally survives a cross-major copy (PG 15 → PG 16). So text is the safe baseline (pgcopydb's default too), with binary an explicit opt-in gated on a matched-major probe of both endpoints — and even then excluded for the OID-sensitive type families.

## Consequences

- A same-engine, no-transform PG→PG cold migrate byte-pipes each table/chunk, closing most of the per-stream rate gap the at-scale benchmark measured. Every other run shape (cross-engine, any transform, resume, an OID-sensitive table) is byte-identical to before — the IR path.
- The gate governs correctness at one auditable chokepoint; per-table identity is re-checked so one odd table falls back without disabling the lane.
- New optional IR surfaces (`RawCopyExporter` / `RawCopyImporter` / `RawCopyVersionProber` / `RawCopyChunk` / `RawCopyFormat`) — additive, type-asserted, no change to the base `RowReader` / `RowWriter` or to engines that don't implement them (MySQL).
- The lane is cold-start + `migrate`-only by construction (slotted in the fast-loader branch); resume and sync cold-start stay on the IR path.
- v1 is conservative on type families (extension/verbatim/bit/geometry excluded) and on chunk PKs (single integer only). Both widen on evidence.
- **Wire encoding is pinned to UTF8 on both raw sessions** (`rawCopyForceUTF8`). The byte-pipe encodes under the source session's `client_encoding` and decodes under the target's; an asymmetric DSN (`client_encoding=LATIN1` on one side only) would otherwise silently corrupt non-ASCII text, since the byte-pipe skips the IR per-value re-encode that would normalize it. Forcing UTF8 on both makes the stream self-consistent by construction — matching the pgx IR path's default UTF8 session. This is the one place the raw lane actively asserts session state rather than passing bytes through untouched, and it exists precisely to keep "nothing is supposed to change" honest at the encoding layer.

## Alternatives considered

- **A runtime guard inside the copy goroutines instead of a setup-time gate.** Rejected — the gate is a pure predicate auditable in one place (the value-fidelity-reviewer lens); a scattered runtime check is harder to prove exhaustive and is exactly the silent-loss risk this ADR is built to remove.
- **Default to binary COPY.** Rejected — version/codec-sensitive across majors for no benefit over text on the decode-elimination win; binary is the matched-major opt-in.
- **`COPY tbl TO STDOUT` (bare table) instead of an explicit SELECT projection.** Rejected — the CRUX bug: it includes generated columns and desyncs the importer's `(non-generated)` column list. The explicit `COPY (SELECT <readable>) TO STDOUT` is mandatory.
- **Widen the type-family allowlist in v1 (carry extension/bit/geometry through the byte-pipe).** Deferred — those have OID/wire-format-sensitive per-type codecs (pgvector, hstore, EWKB) that the IR path runs; carrying them raw needs per-type evidence (the Bug 74 "pin the class" discipline). Start strict.
- **Overlap the `sync` cold-start path too.** Deferred — same snapshot-pinning/durable-watermark surface ADR-0076/0077 deferred.
