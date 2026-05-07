# Design: Apache Arrow integration

**Status:** Proto-ADR / design exploration. Not yet a numbered ADR. Captures the design space for "should sluice integrate with Apache Arrow, and if so where" so a maintainer can decide whether to commit the cycles. This doc is research-driven — there is no operator demand on file. The question is whether Arrow would unlock new use cases that justify the engineering and surface-area cost.

## Context

### What sluice does today

Sluice is a typed-IR-first MySQL ↔ Postgres migration and continuous-sync tool. The data path is:

```
Source DB  →  RowReader  →  chan ir.Row  →  RowWriter  →  Target DB
```

`ir.Row` is `map[string]any` ([internal/ir/change.go](../../internal/ir/change.go)) where the `any` is constrained by the contract in [docs/value-types.md](../value-types.md). Bulk paths today are engine-native: pgx `CopyFrom` for Postgres ([internal/engines/postgres/copy_source.go](../../internal/engines/postgres/copy_source.go), [internal/engines/postgres/row_writer.go](../../internal/engines/postgres/row_writer.go)), `LOAD DATA LOCAL INFILE` for MySQL (ADR-0026). Both feed off the same `<-chan ir.Row` shape — readers are decoupled from writers via the IR.

### Where Arrow could fit

Apache Arrow is a columnar in-memory format with an attached IPC / file / streaming serialisation (Feather, Parquet via the Parquet C++ / Go libs, Flight RPC). The Go binding is `github.com/apache/arrow-go` (formerly `apache/arrow/go/v17`). Arrow's pitch is zero-copy interchange between Arrow-speaking systems: DuckDB, Polars, Spark via Arrow IPC, ClickHouse, pandas, and Parquet on object storage.

Sluice does not currently emit anything other than SQL writes. Every "the operator wants to land sluice data somewhere non-SQL" use case is unsupported.

### Why this came up

Captured as a separate research topic during the v0.10.x → v0.11.x transition. No external operator has asked for it. It surfaces because:

- Logical-backup research (parallel proto-ADR) raised "what's the on-disk format" as a real question — Parquet is one candidate.
- A class of plausible users (analytics / data-platform teams) speak Arrow more naturally than they speak the SQL dialects sluice already targets.
- Sluice's row-stream shape (push-driven `<-chan ir.Row`, batched in the writer) is *almost* the right shape to feed an Arrow record-batch builder. The conversion is mechanically straightforward; the question is whether it's load-bearing.

This doc is about whether to commit. It is not about how to build it well — that's a downstream ADR if the answer here is "yes."

## Use cases

| # | Use case | Asked for? | Strength of fit |
|---|---|---|---|
| 1 | Sync MySQL/PG → Parquet on cloud storage (data-lake offload) | No | Strong: sluice already reads efficiently; Parquet is the de facto data-lake format. |
| 2 | Sync into an Arrow-Flight sink (DuckDB, ClickHouse with Arrow, Spark, Polars) | No | Moderate: Flight is the right protocol but few production sinks are Flight-native; most go via Parquet-on-object-storage anyway. |
| 3 | Cross-process zero-copy IPC ("sluice as an extract step") | No | Weak: niche; "embed sluice as a library" is a different product. |
| 4 | Backups in Arrow/Parquet (folds into logical-backup research) | Implied by sibling research | Strong if logical-backup commits to Parquet. Conditional on that decision. |
| 5 | Query-time analytics directly against the in-flight sync stream | No | Weak: sluice is a flow-regulator (per the name), not a query engine. Out of scope by tenet. |

The **load-bearing** use cases are #1 (data-lake offload) and #4 (backups). #2 is a nice-to-have that piggybacks on #1's machinery. #3 and #5 are tangents.

The pitch for #1: an operator running Postgres-as-OLTP wants a daily refresh of selected tables into S3/GCS Parquet for their analytics warehouse to read. Today they'd reach for AWS DMS, Debezium + Kafka Connect S3 sink, or a custom `pg_dump | parquet-tools` pipeline. Sluice landing this would be a concrete competitive surface.

## Type-system mapping

Sluice's IR types ([internal/ir/types.go](../../internal/ir/types.go), [internal/ir/extension_types.go](../../internal/ir/extension_types.go)) and their Arrow equivalents:

| IR type | Arrow logical type | Notes / lossiness |
|---|---|---|
| `Boolean` | `Boolean` | clean |
| `Integer{8/16/32/64}` (signed) | `Int8`/`Int16`/`Int32`/`Int64` | clean |
| `Integer{8/16/32/64}` (unsigned) | `Uint8`/`Uint16`/`Uint32`/`Uint64` | clean |
| `Integer{Width: 24}` (MySQL MEDIUMINT) | `Int32` | promote; documented elsewhere as a non-issue. |
| `Decimal{P,S}` | `Decimal128(P,S)` (or `Decimal256` for P>38) | clean if P ≤ 38; needs Decimal256 above. Sluice's Row carries `string` for Decimal — converting to Arrow's fixed-width decimal needs a parse step. Errors are loud. |
| `Float{Single}` | `Float32` | clean |
| `Float{Double}` | `Float64` | clean |
| `Char(N)` / `Varchar(N)` / `Text` | `Utf8` (or `LargeUtf8` for `TextLong`) | charset / collation are dropped (Arrow has no concept of either). Document as "lossy on metadata, not on bytes." |
| `Binary(N)` / `Varbinary(N)` / `Blob` | `Binary` (or `LargeBinary`) | clean. Length cap on `Binary(N)` is Arrow-side unenforced. |
| `Date` | `Date32` (days since epoch) | clean |
| `Time(P)` | `Time64[ns]` or `Time32[s/ms]` per precision | sluice's Row carries `string` for Time; must parse. Out-of-`time.Duration`-range values (which is *why* sluice uses string) need a guard before they land in Arrow. |
| `DateTime(P)` (no TZ) | `Timestamp[*, tz=null]` | clean. The "no timezone" semantic is preserved by Arrow's null-tz timestamp. |
| `Timestamp(P, WithTimeZone=true)` | `Timestamp[*, tz="UTC"]` | sluice writes UTC by contract; Arrow stores the tz string. clean. |
| `Timestamp(P, WithTimeZone=false)` | `Timestamp[*, tz=null]` | clean. |
| `JSON` | `Utf8` (with Arrow `extension_type = arrow.json`, available since Arrow 17) | preferred path uses the JSON extension type; pre-17 readers fall back to plain Utf8 + a metadata field. |
| `Enum` | `Dictionary<Int32, Utf8>` | natural fit; preserves the value list as the dictionary, the row carries indices. PG-side this rebuilds; MySQL-side same. |
| `Set` | `List<Utf8>` | clean. (Not `Dictionary` — Arrow Sets aren't a thing; List preserves order/multiplicity.) |
| `UUID` | `FixedSizeBinary(16)` (with Arrow `extension_type = arrow.uuid` if 17+) | sluice's Row carries the canonical hyphenated string; must parse to 16 bytes. Loud errors on bad input. |
| `Array<E>` | `List<arrow_type_of_E>` | nests cleanly. Multidim PG arrays nest as `List<List<...>>`. |
| `Geometry` | `Binary` (WKB raw bytes) + metadata for SRID/subtype | Arrow has no native geometry type; GeoParquet (a GeoParquet-spec metadata convention) is the de facto standard for storing WKB in a Parquet column. Lossy on metadata-not-in-WKB unless we follow GeoParquet. |
| `Inet` | `Utf8` | Arrow has no inet type. Document as "string by convention." |
| `Cidr` | `Utf8` | same as Inet. |
| `Macaddr` | `Utf8` | same. |

**Lossiness summary:**

- **Clean translations** (no information loss): all numeric and boolean types, dates, timestamps with/without TZ, JSON (via extension type), Enum (via Dictionary), Array (via List), Set (via List), Char/Varchar/Text bytes, all binary types.
- **Metadata-only loss** (data preserved, only annotations dropped): charset/collation on Char/Varchar/Text, length caps on bounded types, Geometry SRID without GeoParquet metadata, network-type semantics on Inet/Cidr/Macaddr.
- **Genuine round-trip blockers**: none in the source → Arrow direction. In the Arrow → source direction (if Arrow is ever a *source*), recovering a column's original engine-specific shape from a `Utf8`-stored Inet would require operator schema hints — same shape as sluice's existing `--type-override` mechanism.

The Decimal-256 / Time-out-of-range / UUID-string-parse boundaries are the three places where "loud failure beats silent corruption" is most at risk. Each needs an explicit error branch the moment the conversion is implemented.

## Where Arrow would sit

Three candidate shapes, ordered from smallest to largest:

### Shape A — Arrow as a target writer

A new engine package `internal/engines/arrow/` that implements `ir.SchemaWriter` + `ir.RowWriter`, registered as engine name `arrow` (or `parquet`, with Arrow IPC and Parquet selectable via subcommand flag). Source can be any existing engine.

```
MySQL/PG  →  Reader  →  ir.Row  →  Arrow Writer  →  Parquet on disk / S3 / GCS
```

- `SchemaWriter.CreateTablesWithoutConstraints` becomes "create the Arrow schema for each table" (no constraints concept on Arrow side).
- `SchemaWriter.CreateIndexes` / `CreateConstraints` are no-ops; `Capabilities` declares no support for indexes / FK / CHECK.
- `RowWriter.WriteRows` builds Arrow record batches from the `<-chan ir.Row` and writes them either as Arrow IPC streams or Parquet files. Per-table file output, with a configurable max-rows-per-file or max-bytes-per-file (mirrors the existing `--max-buffer-bytes` shape).
- New CLI surface: `--target-driver=arrow --target=s3://bucket/prefix/` or `--target=file:///path/`. Object-storage URL parsing is a new dependency.
- `RowReader` and `CDCReader` not implemented in v1 — Arrow is target-only. Operators rehydrate from Parquet via existing tools (DuckDB / Polars / Spark) if they need the reverse direction.

This is the smallest useful slice. Engineering cost is bounded: the engine-package pattern is well-trodden ([docs/architecture.md#adding-a-new-engine](../architecture.md#adding-a-new-engine)), the IR-to-Arrow type mapping is the bulk of the work, and the orchestrator doesn't need to know anything about Arrow.

### Shape B — Arrow as a source reader

Symmetric to Shape A: read existing Parquet/Arrow files as the source. Useful for "rehydrate from cloud-storage backup" or "load this Parquet snapshot into Postgres."

```
Parquet on disk / S3 / GCS  →  Arrow Reader  →  ir.Row  →  MySQL/PG Writer
```

This is the harder direction because the Arrow → IR mapping has to recover semantics that aren't in Arrow (Inet, Cidr, charset, length caps). Operator schema hints fill the gap, but the UX surface (`--type-override`-on-steroids) is real engineering. Should not ship in v1; revisit once Shape A has real usage.

### Shape C — Arrow as an interchange in the row pipeline

Replace (or supplement) `chan ir.Row` with Arrow record batches as the in-flight format. The orchestrator becomes Arrow-aware; readers and writers convert at their boundaries.

Cost: very high. `ir.Row` and the value contract in [docs/value-types.md](../value-types.md) are load-bearing across every engine, every test, and the entire CDC path. Replacing them with Arrow record batches is a multi-month rewrite that touches every line of every reader and writer. Benefit: zero-copy interop with other Arrow tools *if anyone is consuming sluice's in-flight stream as a library*, which nobody is.

This shape is a no — it bends sluice's IR-first tenet (Arrow's type system would compete with the IR), the engineering cost dwarfs the benefit, and the use case ("embed sluice as a library") is a different product. **Reject.**

## Engineering shape

**Library:** `github.com/apache/arrow-go/v18` (current). Pure Go for the in-memory + IPC code path. Parquet support comes via `github.com/apache/arrow-go/v18/parquet`, also pure Go. No CGO required for either, which preserves sluice's cross-compile story (goreleaser produces Linux/macOS/Windows × amd64/arm64 binaries today).

**Dependency footprint:** non-trivial. The Arrow Go module is large (≈100k LOC). Pulling it in roughly doubles sluice's binary size — current binaries are ~20–25 MB, with Arrow they'd be ~40–50 MB. Not disqualifying but worth noting; the OSS-hygiene README pass mentions binary size as an operator-visible thing.

**Build-tag layering:** mirror the `integration` / `psverify` pattern. New build tag `arrow` gates the engine package compilation. Operators who don't need Arrow get the slimmer binary; goreleaser produces both an `arrow`-tagged and untagged variant per platform. The canonical CI build stays untagged; an additional `arrow` job in `.github/workflows/ci.yml` smoke-builds the tagged variant on Linux.

**Object-storage credentials:** out of scope for the integration itself, but a real ergonomic question. Operators expect `s3://`, `gs://`, `azblob://` URLs to "just work" with ambient credentials (instance-profile, gcloud ADC, Azure managed identity). The `gocloud.dev/blob` SDK abstracts these well and is already used by other Go data tools. Adds another ~50k LOC; could land it staged behind the `arrow` build tag.

## Tenet check

| Tenet | Interaction | Verdict |
|---|---|---|
| **IR-first** | Shape A keeps the IR central — Arrow is just another writer. Shape C inverts it. | Shape A clean; Shape C violates. |
| **Contain Postgres complexity** | Arrow brings its own ecosystem complexity (Parquet versions, Flight protocol, GeoParquet conventions, Decimal256 vs Decimal128 picking) that's not Postgres-specific but is structurally similar — a sprawl of optional sub-features each with their own gotchas. | Shape A contains the complexity inside one engine package. Acceptable. |
| **Validate end-to-end** | Cross-engine integration tests for sluice today are MySQL/PG round-trips. An Arrow target adds a new axis — would need tests that emit Parquet, read it back via DuckDB / arrow-go reader, and assert value-level fidelity. Doable; the test surface roughly doubles for the engine. | Plan for it; not a blocker. |
| **Loud failure beats silent corruption** | The Decimal precision / Time range / UUID parse / charset metadata loss are all "loud failure" sites in the proposed mapping. Arrow's nullability semantics differ from SQL NULL (Arrow has both null *and* zero-length-string-as-distinct), and Arrow timestamps with tz="" vs tz=null differ from sluice's `WithTimeZone` flag in subtle ways — the conversion table above pins each. | Each conversion site is a test surface. Plan ≥3 tests per type-mapping row in the table above. |

## Effort estimate

Order of magnitude per shape:

- **Shape A (target writer, Parquet + Arrow IPC, no object-storage):** 3–5 weeks. Type mapping + record-batch builder + file writer + integration tests + ADR. Mostly mechanical once the type-mapping decisions are pinned.
- **Shape A + object-storage backends:** +1–2 weeks. `gocloud.dev/blob` integration, credential plumbing, multipart upload for large files.
- **Shape B (source reader):** 4–6 weeks on top of Shape A. The Arrow → IR direction needs the schema-hint UX which is its own design exercise.
- **Shape C (Arrow as in-flight format):** 3+ months. Effectively a rewrite of every reader and writer. **Do not pursue.**

**MVP slice:** Shape A, Parquet output only, local filesystem only, no Arrow IPC, no object-storage. ~2 weeks. Tables emit one Parquet file each; CLI is `--target-driver=parquet --target=file:///path/`. Operators with S3 needs can run sluice locally + `aws s3 cp` afterwards. This proves the value-translation layer is correct without committing to the full ergonomic surface. If real users adopt this for their data-lake pipeline, expand to object-storage and Arrow IPC; if nobody does, the cost is bounded.

## Decision

**Conditional yes**, gated on the parallel logical-backup research outcome.

- If logical-backup picks Parquet as its on-disk format, Shape A subsumes the backup writer and the combined effort is justified — sluice gets data-lake offload + a credible backup format in one engineering cycle.
- If logical-backup picks something else (custom binary, SQL `pg_dump`-style, Arrow IPC sans Parquet), Arrow's standalone case is weaker. Defer until an operator asks for it concretely. The proto-ADR remains as the path-when-needed.

The reason for the conditionality: Arrow's standalone value to sluice's current operator base (MySQL/PG migration, MySQL/PG continuous sync) is real but unverified. The combined value (backup + offload) is a stronger story than either alone. Coupling the decision to the logical-backup decision concentrates the engineering risk into one cycle rather than spreading it across two speculative-feature tracks.

## Consequences

**If pursued (Shape A MVP):**

- New engine package `internal/engines/arrow/` (gated by `arrow` build tag).
- New CLI flag combinations: `--target-driver=parquet`, `--target=file:///...`. Documented in the README's "supported targets" matrix.
- Binary size grows ~2× under the `arrow` tag; default build stays untagged and unaffected.
- CI Integration job adds an `arrow` matrix axis: emit Parquet → read back via `arrow-go` Parquet reader in a test → assert value-level fidelity per the type-mapping table.
- Type-mapping table above becomes the normative spec; lives in `docs/type-mapping.md` alongside the existing engine-pair tables.
- Two new ADRs: one for the engine-package + capability declaration shape, one for the type-mapping decisions.

**If not pursued:**

- Operators wanting MySQL/PG → Parquet continue to use AWS DMS / Debezium / custom pipelines. Sluice cedes that surface to dedicated tools.
- Logical-backup picks a non-Arrow on-disk format and documents the rationale.
- This proto-ADR stays as the "path when asked for."

## Open questions

1. **Parquet-only or Arrow IPC too?** Parquet is the data-lake format; Arrow IPC is the cross-process / streaming format. Different operator audiences. MVP is Parquet; Arrow IPC is a follow-on if Flight-sink users emerge.
2. **GeoParquet for the Geometry type?** GeoParquet (https://geoparquet.org/) is a metadata convention layered on Parquet. Following it preserves SRID + subtype on round-trip. Adds a small-spec dependency (just metadata keys, no new library) but commits sluice to tracking the GeoParquet spec.
3. **Per-table file or one consolidated file?** Today's per-table parallelism + per-table file is the natural shape. Some warehouses (BigQuery, Snowflake) ingest faster from one large file; others (Athena, Spark) prefer many small files. Operator-configurable, default per-table.
4. **CDC into Parquet?** Continuous-sync into Parquet is a different problem — Parquet is immutable per-file, so CDC events become append-only deltas (or the operator runs periodic compaction). Out of scope for v1; revisit if the use case shows up.
5. **Schema evolution.** Parquet has its own schema-evolution rules; sluice's source-side ADD COLUMN / DROP COLUMN should map to "new file with new schema" rather than "rewrite history." Aligns with the per-table-file decision.
6. **Compression codec.** Snappy (default), Zstd, Gzip. Operator-configurable; default Snappy for compatibility with the broadest reader ecosystem.

## Why not now

Two reasons:

- No operator demand. Every other v0.x shipping decision has been driven by real-world testing reports; Arrow integration would be the first speculative-feature commit. The "validate end-to-end before building more" tenet pushes back on that.
- The conditional-yes structure: the right time to commit is when the logical-backup research has decided whether Parquet is also its format. Building Arrow alone is ~2 weeks for the MVP; building Arrow + backup together is ~3–4 weeks combined and yields two operator-facing surfaces from one investment. Sequencing matters.

When real-world testing reports a "we want sluice to land in our data lake" need, this design is the starting point. Until then, the proto-ADR is reference material.

## See also

- [docs/architecture.md](../architecture.md) — IR / engine pattern this design assumes.
- [docs/value-types.md](../value-types.md) — runtime contract Arrow conversion has to honour.
- [docs/type-mapping.md](../type-mapping.md) — engine-pair type tables; an Arrow column would slot alongside.
- [docs/dev/roadmap.md](roadmap.md) — slots after the heavier design-first items currently in flight.
- [internal/ir/types.go](../../internal/ir/types.go) and [internal/ir/extension_types.go](../../internal/ir/extension_types.go) — IR types Arrow has to map.
- [internal/engines/postgres/copy_source.go](../../internal/engines/postgres/copy_source.go) — example of how a writer adapts a `chan ir.Row` to an engine-native bulk path; an Arrow writer would mirror this shape.
