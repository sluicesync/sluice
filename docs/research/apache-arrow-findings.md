# Apache Arrow / Parquet integration — research findings

**Status:** Research-only. Written to inform an operator decision; no code changes proposed and no dependencies added. Companion to the existing proto-ADR at [`docs/dev/design/apache-arrow-integration.md`](../dev/design/apache-arrow-integration.md), which already laid out the three integration shapes and a tenet-check. This doc revisits the question after Phase 1 logical-backup landed, validates the proto-ADR's library + dep assumptions against current upstream state, and tightens the recommendation.

**Bottom line:** Defer Shape A. The conditional-yes in the proto-ADR was gated on Phase 1 logical-backup picking Parquet — it didn't (it picked gzipped JSON-Lines, [`internal/pipeline/backup_chunk.go`](../../internal/pipeline/backup_chunk.go)), which removes the combined-cycle justification. Shape A standalone for data-lake offload is a real but unverified use case; a small operator-facing feature flag and a public note ("we'd build this if asked") is a cheaper signal-collection move than building it pre-emptively. Shape B and Shape C remain rejected for the same reasons the proto-ADR gave.

## What changed since the proto-ADR

The proto-ADR (`docs/dev/design/apache-arrow-integration.md`, ~v0.10.x → v0.11.x window) made a **conditional yes** call: ship Shape A *if* logical-backup picked Parquet, defer otherwise. Two things have moved since:

1. **Logical-backup Phase 1 shipped with non-Parquet chunks.** The current chunk format is gzip-compressed JSON-Lines with a tagged-value envelope ([`internal/pipeline/backup_chunk.go`](../../internal/pipeline/backup_chunk.go)). The format-version is pinned (`chunkHeaderVersion = 1`) and the rationale ("debuggable; engine-portable; forward-compat through new tag kinds; gzip is stdlib") is documented in the file's preamble. Phase 2 considered swapping gzip → zstd; Parquet was not adopted. The combined-cycle argument the proto-ADR rested on is therefore no longer available.
2. **`apache/arrow-go` matured but did not consolidate.** Latest is `v18.6.0` (2026-04-28), continuing a quarterly minor-release cadence ([github.com/apache/arrow-go/releases](https://github.com/apache/arrow-go/releases)). Module path is `github.com/apache/arrow-go/v18`. The dependency graph in the current `go.mod` is broader than the proto-ADR estimated — see "Dependency footprint" below.

The roadmap entry ([`docs/dev/roadmap.md`](../dev/roadmap.md) §2 "Apache Arrow integration (conditional)") still flags the conditional-yes; this doc is the input for moving it to a definitive call.

## Library landscape

### `github.com/apache/arrow-go/v18` (the canonical option)

- **Maintainer:** Apache Foundation; 1 maintainer org, multiple contributors. 360 stars, 22 published releases, 1.1k commits on `main`.
- **Pure Go?** *Mostly.* The README says "uses c2goasm to leverage LLVM's advanced optimizer and generate PLAN9 assembly functions from C/C++ code"; can be compiled without those optimisations using the `noasm` build tag. **No CGO required** in either mode — sluice's cross-compile story (goreleaser produces Linux/macOS/Windows × amd64/arm64 binaries) is preserved as long as `noasm` works on the target platforms it falls back on.
- **API stability:** v18 is the current major; the project bumps majors infrequently (the proto-ADR was written when v17 was current; the upgrade to v18 happened upstream without a Go-API earthquake).
- **Release cadence:** quarterly minor releases with 1–2 RCs preceding each, plus periodic patch releases. Reasonable for a backed-OSS project.
- **Activity signals:** 59 open issues, 17 open PRs, monthly+ commit activity. Healthy but not high-volume — this is a "mature library" tempo, not a "young library racing to v1" tempo. Apache Foundation backing is the real durability signal.
- **License:** Apache-2.0. Compatible with sluice's Apache-2.0 licensing.
- **Sub-modules used by the integration:** `github.com/apache/arrow-go/v18/arrow` (in-memory + IPC) and `github.com/apache/arrow-go/v18/parquet` (Parquet writer/reader). Both pure Go.

### `parquet-go/parquet-go` (formerly Twilio Segment's `segmentio/parquet-go`)

- **Maintainer:** community-led after being open-sourced from Twilio Segment. 713 stars.
- **Latest:** v0.29.0 (2026-03-09); pre-v1, breaking API changes are explicitly allowed.
- **Pure Go?** Yes (94.2% Go, 5.8% assembly for hot loops).
- **Strengths:** Smaller dep footprint than `arrow-go`; strict focus on Parquet (no Arrow IPC / Flight / Substrate / SQLite-via-modernc baggage); on-disk page buffering for files larger than RAM; supports Parquet VARIANT, bloom filters, schema evolution.
- **Weaknesses:** No first-class Arrow array integration — if sluice ever wanted in-memory Arrow batches (Shape C territory, currently rejected), this library wouldn't help. Pre-v1 stability caveat.
- **Tradeoff for sluice:** If the integration is *Parquet-only-as-a-target* (Shape A scope), this library is the smaller, more focused choice and avoids the `arrow-go` dependency tail. If the integration ever extends into Arrow IPC streams or Flight (open question #1 in the proto-ADR), `arrow-go` becomes necessary.

### `xitongsys/parquet-go` (older, less-maintained)

- 1.4k stars but **last release v1.6.2 in December 2021** — effectively abandoned for sluice's purposes. Mention only to document that this is *not* the same project as `parquet-go/parquet-go` despite the colliding name. Skip.

### Library recommendation if the work happens

- **Default to `parquet-go/parquet-go`** for an MVP-Shape-A landing if and only if the scope stays "Parquet files written to local FS or operator-owned object storage." Smaller deps, focused API, no implicit commitment to the Arrow IPC ecosystem.
- **Switch to `apache/arrow-go/v18`** if and when an Arrow IPC / Flight integration becomes the goal (open question #1 in the proto-ADR — "Parquet-only or Arrow IPC too?"). Trying to use both simultaneously would be the worst of both worlds.

The proto-ADR assumed `arrow-go` by default. This finding nudges that assumption: **for the MVP slice, `parquet-go/parquet-go` is the better starting point** unless the operator wants Arrow IPC in scope from day one.

## Dependency footprint

The proto-ADR estimated "≈100k LOC; doubles binary size; pure Go." The first two numbers are roughly right; the third is true (no CGO) but understates the *transitive* dep surface.

`apache/arrow-go/v18` direct deps observed in upstream `go.mod`:

- `github.com/andybalholm/brotli`, `github.com/klauspost/compress`, `github.com/pierrec/lz4/v4` — compression codecs (Parquet + Arrow IPC support these). `klauspost/compress` is **already in sluice's transitive deps** via gocloud.
- `github.com/apache/thrift` — Parquet metadata is Thrift-encoded.
- `github.com/google/flatbuffers` — Arrow IPC schemas are FlatBuffers.
- `github.com/cespare/xxhash/v2`, `github.com/zeebo/xxh3` — bloom-filter / hashing.
- `github.com/goccy/go-json` — JSON marshalling. **Already in sluice's transitive deps.**
- `github.com/hamba/avro/v2` — Avro support (used by Arrow's optional Avro adapter).
- `github.com/substrait-io/substrait-go/v8`, `github.com/substrait-io/substrait-protobuf/go` — Substrait (logical-plan IR) integration. **Almost certainly unused by sluice.**
- `github.com/pterm/pterm` — pretty terminal output. **Almost certainly unused by sluice.**
- `gonum.org/v1/gonum` — numerical computing. **Almost certainly unused by sluice.**
- `modernc.org/sqlite` — pure-Go SQLite, used by ADBC drivers. **Almost certainly unused.**
- `google.golang.org/grpc` v1.81.0 — Arrow Flight RPC. sluice already requires `grpc` v1.81.0, so this is consistent.
- `google.golang.org/protobuf` — already a sluice transitive dep.

The Substrait, Avro, gonum, modernc/sqlite, and pterm tail is the surprise. Even if sluice's code only imports `.../parquet` and `.../arrow/array`, Go's module resolution still pulls these into `go.sum` and the lock graph (binary impact depends on whether the linker dead-strips the unused symbols, which it usually does, but Go's linker doesn't strip *modules*). Builds will be slower; `go.sum` will grow ~30%.

`parquet-go/parquet-go` direct deps:

- `andybalholm/brotli`, `klauspost/compress`, `pierrec/lz4` — same compression set.
- `google/uuid` — already in sluice transitive deps.
- `parquet-go/bitpack`, `parquet-go/jsonlite` — small in-house companion modules.
- `twpayne/go-geom` — geometry support (relevant if sluice ever maps `Geometry` IR type to GeoParquet).
- `golang.org/x/sys`, `google.golang.org/protobuf` — already in sluice transitive deps.

Total dep growth from `parquet-go/parquet-go`: ~5 new direct/indirect modules vs `arrow-go`'s ~15+. Binary size impact: `arrow-go`-tagged build is the proto-ADR's "~2×" estimate; `parquet-go/parquet-go` build is closer to ~1.3× of the untagged baseline based on rough comparison of similar-shape Go binaries (no measurement done — operator should re-check before committing).

**Build-tag gating remains the right answer either way.** `arrow` (or `parquet`, see "Naming" below) build tag, default-untagged builds untouched. Mirrors the existing `integration` / `psverify` / `vstream` pattern documented in `CLAUDE.md`.

## Three integration shapes

The proto-ADR identified Shape A / B / C. This finding doesn't reopen Shape B or C — it tightens the Shape A scope and offers a Shape A-prime variant.

### Shape A — Parquet as a target-engine option (recommended *if* anything ships)

```
MySQL/PG → Reader → ir.Row → Parquet Writer → Parquet on disk / S3 / GCS
```

New engine package `internal/engines/parquet/` (or `internal/engines/arrow/` if Arrow IPC is also in scope; see naming below). Behind `parquet` build tag. Implements `ir.SchemaWriter` + `ir.RowWriter`. Capability declares no FK / index / CHECK support. CLI: `sluice migrate --target-driver=parquet --target=file:///path/`.

This is the proto-ADR's Shape A unchanged. The MVP slice the proto-ADR specified — Parquet output only, local FS only, no object storage, no Arrow IPC — remains the correct starting point if the work happens.

**LOC estimate:** ~1500–2500 lines for the engine package + tests, scoped as:

- ~400 LoC: type-mapping table (one function per IR type → Arrow/Parquet type, with the loud-failure branches at Decimal precision / Time range / UUID parse / etc.).
- ~300 LoC: `RowWriter.WriteRows` — record-batch builder consuming `<-chan ir.Row`, flushing to `.parquet` files at a configurable size cap.
- ~200 LoC: `SchemaWriter.CreateTablesWithoutConstraints` — IR schema → Parquet schema translation. `CreateIndexes` / `CreateConstraints` are no-ops.
- ~200 LoC: engine registration + `Capabilities` declaration + URL parsing for `file://` targets.
- ~600–1300 LoC: tests — unit tests for type-mapping table (one test per row), one integration test that emits Parquet and reads it back via the same library to assert value-level fidelity, possibly a DuckDB-via-CLI smoke test if DuckDB CLI ends up in CI.

The proto-ADR's "3–5 weeks" remains the right order of magnitude; the lower bound (~2 weeks) is plausible if scope holds firm to "local FS + Parquet" with zero ergonomic frosting.

### Shape A-prime — chunk-format option for backups (deprioritised)

The original Shape A in this finding's framing was "Parquet as a chunk-format option for backups" — replace `backup_chunk.go`'s gzip+JSON-Lines with Parquet for the operators who want to grep their backups with DuckDB. **Do not pursue this.** Reasoning:

- Phase 1 logical-backup just shipped with the JSON-Lines format; it's the public chunk format contract operators are reading and tooling is being built against. Adding a second chunk format mid-flight is a UX regression.
- The Phase 2 / Phase 3 / Phase 4–6 design docs ([`docs/dev/design/logical-backups-phase-2.md`](../dev/design/logical-backups-phase-2.md) onward) are still in flight; introducing a chunk-format choice axis to those phases multiplies the test surface for no operator-asked benefit.
- If an operator genuinely wants to query backup chunks with DuckDB, the path of less resistance is `sluice backup export-as-parquet <backup-id>` — a one-shot transcoding command — not a chunk-format choice the user has to decide at backup time. The transcoding command is also strictly cheaper to implement (one direction, manifest-driven, no orchestrator changes) than mid-pipeline format switching.

**Defer this until an operator asks for it concretely**, then ship the export-as-parquet command rather than a backup-time chunk format. Cost of waiting: zero (no decision blocks on this).

### Shape B — Parquet as a source-engine option (rejected for now)

```
Parquet on disk / S3 / GCS → Parquet Reader → ir.Row → MySQL/PG Writer
```

Symmetric to Shape A. The proto-ADR's verdict ("4–6 weeks on top of Shape A; the Arrow → IR direction needs the schema-hint UX which is its own design exercise; should not ship in v1") stands. The schema-recovery problem (Inet, Cidr, Macaddr, charset metadata, length caps — none of which round-trip through Parquet without operator hints) is the load-bearing reason; Arrow's type system can't carry the per-engine SQL semantics sluice's IR carries. **Reject for v1.**

### Shape C — Arrow as the in-flight row-pipeline format (rejected, IR-violating)

```
chan ir.Row → (replaced by) chan Arrow.RecordBatch → ...
```

The proto-ADR rejected this for tenet reasons ("IR-first" — Arrow's type system would compete with sluice's IR). That call holds and the reasoning is unchanged:

- `ir.Row` is `map[string]any` constrained by [`docs/value-types.md`](../value-types.md). Replacing it with Arrow record batches means every reader, every writer, every CDC path, the entire applier, and every test moves from value-by-value Go-native types to schema-bound Arrow columnar batches.
- The benefit of Shape C is *zero-copy interchange with downstream Arrow consumers*. sluice has no downstream Arrow consumers — it terminates at SQL writes (or, post-Phase-1, at backup-store writes). Zero-copy for nobody is no benefit.
- The cost is a rewrite of every line of every reader and writer, plus the value contract becomes Arrow's instead of sluice's. Arrow's nullability semantics differ from SQL NULL in subtle ways (Arrow has both null *and* zero-length-string-as-distinct), Arrow timestamps with `tz=""` vs `tz=null` differ from sluice's `WithTimeZone` flag, and Arrow's decimal precision is fixed at type-creation time vs sluice's per-value Decimal-as-string carrying. The proto-ADR's loud-failure-tenet-check work would need to be done at every conversion site, not just at the writer boundary.
- **Strategic note:** Shape C is what you'd build if "embed sluice as a library inside another data tool" was the product. It is not the product. sluice is a flow-regulator at the SQL boundary, per the name.

**Reject indefinitely.** This shape is documented for completeness; reopening would require a different sluice product identity.

## Type fidelity

The proto-ADR included a complete IR-to-Arrow type table. Reproducing it here would be redundant; instead, this finding flags the rows that have *moved* since the proto-ADR was written, and adds a couple the proto-ADR didn't fully address.

### Confirmed since proto-ADR (Arrow canonical extension types)

The proto-ADR called out `arrow.json` and `arrow.uuid` as preferred encodings. Both are now confirmed as canonical extension types per the upstream spec ([Apache Arrow Canonical Extensions](https://arrow.apache.org/docs/format/CanonicalExtensions.html)):

- **`arrow.json`** — storage type is `String` / `LargeString` / `StringView`; UTF-8 RFC8259-encoded JSON. Maps cleanly to sluice's `ir.JSON` type. Readers without extension-type support fall back to `Utf8` (lossy on the type-tag, not on bytes).
- **`arrow.uuid`** — storage type is `FixedSizeBinary[16]`, big-endian. sluice's `ir.UUID` value is a canonical hyphenated string post-Bug 41 / per [`docs/value-types.md`](../value-types.md). Writer parses string → 16 bytes; loud failure on bad input. Same shape the proto-ADR specified.
- **`arrow.opaque`** is also now canonical — useful as a "we know this came from a SQL type Arrow doesn't have a name for" wrapper. Possible home for `Inet` / `Cidr` / `Macaddr` instead of bare `Utf8`, **if** the round-trip story matters (Shape B). For Shape A target-only it's not load-bearing; bare `Utf8` plus a metadata key naming the original SQL type is sufficient.

### Newly clarified rows

| sluice IR type | Arrow target | Notes |
|---|---|---|
| `ir.JSON{Binary: true}` | `Utf8` + extension `arrow.json` | Per spec, RFC8259 UTF-8. sluice's MySQL `JSON` and PG `jsonb` both round-trip text-form by contract. Clean. |
| `ir.JSON{Binary: false}` | `Utf8` + extension `arrow.json` | Same as above. The `Binary` flag carries no Arrow-side distinction. |
| `ir.UUID` | `FixedSizeBinary[16]` + extension `arrow.uuid` | Big-endian. Loud failure on parse. Consider also offering a `--uuid-as-string` flag for Parquet readers that don't honour the extension type — many downstream tools (older DuckDB, older Spark) fall back to raw bytes which are less useful. |
| `ir.Inet` / `ir.Cidr` / `ir.Macaddr` | `Utf8` (with metadata key naming the original SQL type) | Cross-engine retargeting in sluice already maps these to VARCHAR per the existing PG-native type-retargeting policy ([`docs/type-mapping.md`](../type-mapping.md)). Arrow target should follow that policy — `Utf8` is the right default. Consider `arrow.opaque` extension-tagging if Shape B ever happens. |
| `ir.Geometry` | `Binary` (WKB) + GeoParquet metadata | GeoParquet (https://geoparquet.org/) is a metadata-only convention; no new library required. Following it preserves SRID + subtype on round-trip. **Open question.** |
| `ir.Set{Values: ...}` | `List<Utf8>` | The proto-ADR called out that Arrow has no `Set` type and `List` preserves order/multiplicity. Confirmed correct; cross-engine `Set` → PG already degrades to `text[]` in sluice today, so the Arrow target is consistent. |
| `ir.Enum{Values: ...}` | `Dictionary<Int32, Utf8>` | Arrow `Dictionary` natively expresses "value drawn from a fixed set." Preserves the value list as the dictionary, the column carries indices. Best choice over plain `Utf8`. |
| `ir.Decimal{P, S}` | `Decimal128(P, S)` if P ≤ 38 else `Decimal256(P, S)` | sluice's `Row` value is a Go `string` for Decimals (per value-types.md). Writer parses string → fixed-width decimal; out-of-precision values raise loud errors. The P=38 boundary is the load-bearing failure mode. |
| `ir.Time{Precision: P}` | `Time32[s/ms]` for P≤3, `Time64[us/ns]` for P≥4 | sluice carries Time as `string` (because some MySQL TIME values are out of `time.Duration` range — that's *why* Time is string-typed in the IR). Writer must parse string → microseconds-since-midnight, with a guard for the out-of-range cases. **This is the trickiest type-mapping site.** |
| `ir.DateTime{P}` | `Timestamp[*, tz=null]` | Clean. The "no timezone" semantic is preserved by Arrow's null-tz timestamp. |
| `ir.Timestamp{P, WithTimeZone: true}` | `Timestamp[*, tz="UTC"]` | sluice writes UTC by contract. Arrow stores the tz string. Clean. |
| `ir.Array<E>` | `List<arrow_type_of_E>` | Multidim PG arrays nest as `List<List<...>>`. Element-type translation recurses through the same table. |

The proto-ADR's "loud failure beats silent corruption" boundaries (Decimal-256 / Time out-of-range / UUID-string-parse) remain the three highest-risk sites. Each should be a test row in the type-mapping integration test, not just a code path.

### Types that map dirty (require operator decisions, not blockers)

- **Charset/collation on Char/Varchar/Text.** Arrow has no concept. Drop on write. Document as "lossy on metadata, not on bytes." If round-trip matters (Shape B), persist via Arrow field metadata (`field.Metadata.Set("sluice.charset", "utf8mb4")`).
- **Length caps on Varchar(N) / Binary(N).** Arrow doesn't enforce. Drop or persist via field metadata. Same treatment as charset.
- **Postgres `point`, `line`, `lseg`, `box`, `path`, `polygon`, `circle`** (geometric types not in the proto-ADR table). sluice doesn't model these as first-class IR types yet (degrade to `Text` per existing PG type-mapping). Arrow target follows the same path: `Utf8`. No new fidelity to lose.

## Cost / benefit

### Binary growth

- **`apache/arrow-go/v18` under `arrow` tag:** ~2× untagged binary baseline (proto-ADR estimate; not re-measured; consistent with the `arrow-go` dep tail observed). Default untagged builds unaffected — the build-tag gate is load-bearing.
- **`parquet-go/parquet-go` under `parquet` tag:** ~1.3× untagged baseline (rough estimate; recommend measuring before committing). Smaller dep set is the primary driver.
- **No CGO either way** — sluice's cross-compile matrix preserved.

### Maintenance surface

Per shape, rough additional test/code surface:

| Shape | New code | New tests | New CI axis | Ongoing maintenance |
|---|---|---|---|---|
| A (Parquet target, local FS) | ~1500–2500 LOC | type-mapping table tests + 1 integration roundtrip | One: build-tagged variant smoke-build | Low. Type mapping is mechanical; once correct, rarely changes. |
| A + object storage | +500–1000 LOC | +cloud-storage integration tests (gocloud already in deps; relatively contained) | Same axis, more credentialed-mock fixtures | Medium. Operator-credential ergonomics drive recurring issues. |
| A-prime (chunk-format option) | ~800–1500 LOC inside `internal/pipeline/` | every existing backup test re-runs in two formats | Same axis, doubles backup test matrix | High — chunk format is the public Phase-1 contract; format-switching mid-pipeline is a long-tail bug source. **This is why A-prime is deprioritised.** |
| B (Parquet source) | +1500–2500 LOC | type-recovery tests, schema-hint UX tests | Same axis | Medium-high. Schema-hint UX is its own design problem. |
| C (in-flight Arrow) | rewrite | rewrite | Replaces existing axes | Very high. Effectively a different product. |

### Operator demand signals

Re-checking the proto-ADR's "no operator demand" claim against what's visible at v0.10.x → current:

- **No new GitHub issues or testing reports** asking for Parquet output. The "operator demand" status is unchanged: zero asks on file.
- **Logical-backup users** (who genuinely exist now post-Phase-1) are reading the gzipped JSON-Lines chunks with `zcat | jq` per the format's documented entry point — none have asked for an alternative chunk format. Nobody has yet requested `sluice backup export-as-parquet`, but the path exists if asked.
- **Data-lake users (hypothetical persona).** The proto-ADR's strongest pitch was "PG-as-OLTP operator wants daily Parquet refresh into S3 for warehouse consumption." This persona is real in the industry (AWS DMS, Debezium + Kafka Connect S3 sink, custom `pg_dump | parquet-tools` pipelines all serve it), but no operator using sluice today has surfaced this need.
- **DuckDB / Polars / Spark interop persona.** Even more speculative; no signal.

The "validate end-to-end before building more" tenet specifically calls out unverified speculative-feature commits. Building Arrow without a single asker would be the first such commit in sluice's v0.x trajectory.

### Strategic fit

Two readings:

1. **Extends sluice's identity.** sluice is "data flow regulator at the SQL boundary"; Parquet on object storage is *also* a flow boundary. Adding it makes sluice a multi-target tool — MySQL ↔ PG ↔ Parquet — without violating the IR-first tenet (Shape A keeps the IR central). The same per-engine code structure, the same `Capabilities`-based dispatch.
2. **Different product.** sluice today is a *database* migration / sync tool. Parquet output makes it (also) a *data-lake export* tool. Those are different operator audiences, different selling points, different competitive sets (vs AWS DMS / Debezium for migration; vs Fivetran / Airbyte / dlt for data-lake pipelines). Building a foot in both camps risks identity dilution.

The honest answer: **(1) is the kinder reading, (2) is the more disciplined reading, and the project hasn't yet had to choose because no operator has forced the question.** When/if a real operator asks "can sluice land my MySQL data in S3 Parquet for our analytics warehouse," the answer should be a confident yes (Shape A is the path) — but proactively building it is choosing the (1) reading on speculation.

## Recommendation

**Defer Shape A. Document the path-when-asked-for. Do not build now.**

Specifically:

1. **Update `docs/dev/roadmap.md`** §2 from "conditional yes (gated on logical-backup)" to "deferred — no operator demand; build when asked." The roadmap entry already captures most of the framing; this is a one-line status flip.
2. **Move the proto-ADR to "deferred"** in its own status header. Don't delete it — it remains the path-when-asked-for, which is genuinely useful future-self / future-contributor context.
3. **Add a "Supported targets" matrix entry to the README** noting that Parquet/Arrow output is "on the roadmap, ask if you need it." Cheap operator-facing signal-collection mechanism. If three operators ask in six months, that's the demand signal that flips the call.
4. **If/when the call flips:** ship Shape A MVP per the proto-ADR's spec — Parquet only, local FS only, no Arrow IPC, behind `parquet` build tag, using `parquet-go/parquet-go` (not `apache/arrow-go/v18`) unless Arrow IPC is in scope from day one. ~2-week implementation, plus 1 week for the type-mapping table tests.

**Why not ship a stub.** A "Parquet target that almost works" is worse than no Parquet target — the type-mapping table is the entire load-bearing claim, and getting it wrong silently corrupts data (loud-failure tenet violation). The cost of doing it well is not significantly higher than doing it badly; either ship it correctly or wait for demand.

**Confidence in the deferral:** medium-high. The reasoning is structural (no demand + the combined-cycle gating dissolved when logical-backup picked JSON-Lines), not aesthetic. If a single operator with a credible data-lake use case surfaces, this call should flip immediately — the engineering preparation is done in the proto-ADR and this finding.

## Open questions for operator review

These are the design questions where the operator's preference would materially change the Shape A scope. Listed in priority order.

1. **Arrow IPC in scope, or Parquet-only?** This determines library choice (`arrow-go` vs `parquet-go/parquet-go`). Parquet-only is the cheaper, more focused option and is what the data-lake persona actually wants. Arrow IPC matters only for the Flight-RPC streaming-sink persona, which has zero current asks. **Suggested default: Parquet-only.**
2. **GeoParquet adoption for `ir.Geometry`?** Adds a small spec dependency (no new library, just metadata key conventions) but commits sluice to tracking the GeoParquet spec. Without it, Geometry round-trips lose SRID and subtype. **Suggested default: yes, follow GeoParquet.**
3. **Object-storage from day one or staged?** Local-FS only is the proto-ADR's MVP slice; object storage adds ~1 week and uses gocloud (already in sluice deps for blob_store). Operators asking for Parquet output very likely also want it on S3/GCS/Azure. **Suggested default: stage — local-FS first, object storage in v2 of the engine after the type-mapping is validated.**
4. **Per-table file or one consolidated file?** Per-table is the natural shape (matches sluice's existing per-table parallelism). Some warehouses (BigQuery, Snowflake) prefer fewer larger files; Athena, Spark prefer many smaller. **Suggested default: per-table, with `--max-rows-per-file` and `--max-bytes-per-file` knobs mirroring `--max-buffer-bytes`.**
5. **Compression codec?** Snappy (Parquet default), Zstd, Gzip. Zstd is increasingly common in modern data-lake stacks; Snappy is the safest cross-reader compatibility default. **Suggested default: Snappy.**
6. **CDC into Parquet (sync-mode target)?** Out of scope per the proto-ADR ("CDC events become append-only deltas; revisit if the use case shows up"). Worth re-confirming this stays out of scope for the Shape A MVP. **Suggested default: yes, out of scope for v1.**
7. **`--uuid-as-string` escape hatch?** Many downstream Parquet readers don't honour the `arrow.uuid` extension and surface `FixedSizeBinary[16]` as raw bytes (less useful for downstream queries). A flag to write UUIDs as `Utf8` instead would help compat. Cheap to add. **Suggested default: include the flag.**

## See also

- [`docs/dev/design/apache-arrow-integration.md`](../dev/design/apache-arrow-integration.md) — the original proto-ADR; full type-mapping table; tenet-check; the conditional-yes rationale this finding reverses on the gating dissolving.
- [`docs/dev/roadmap.md`](../dev/roadmap.md) §2 — current roadmap entry, to be updated to "deferred" per the recommendation.
- [`docs/dev/design/logical-backups.md`](../dev/design/logical-backups.md) and the Phase 2/3/4–6 follow-ons — the logical-backup work whose chunk-format choice (JSON-Lines, not Parquet) dissolved the Arrow conditional-yes gate.
- [`internal/pipeline/backup_chunk.go`](../../internal/pipeline/backup_chunk.go) — the Phase 1 chunk format Shape A-prime would have replaced; the file's preamble documents the JSON-Lines + gzip rationale.
- [`docs/value-types.md`](../value-types.md) — the runtime contract for `ir.Row` values that any Arrow writer has to honour.
- [`docs/type-mapping.md`](../type-mapping.md) — the engine-pair type tables; an Arrow column would slot alongside if the work happens.
- [`internal/ir/types.go`](../../internal/ir/types.go) and [`internal/ir/extension_types.go`](../../internal/ir/extension_types.go) — the IR types Arrow has to map.
- [`internal/engines/postgres/copy_source.go`](../../internal/engines/postgres/copy_source.go) — example of how a writer adapts a `chan ir.Row` to an engine-native bulk path; an Arrow writer would mirror this shape.
- [github.com/apache/arrow-go](https://github.com/apache/arrow-go) — official Apache Arrow Go bindings; v18.6.0 as of 2026-04-28.
- [github.com/parquet-go/parquet-go](https://github.com/parquet-go/parquet-go) — community-maintained pure-Go Parquet library; v0.29.0 as of 2026-03-09. Recommended over `arrow-go` for Shape A MVP unless Arrow IPC is in scope.
- [Apache Arrow Canonical Extension Types](https://arrow.apache.org/docs/format/CanonicalExtensions.html) — `arrow.json`, `arrow.uuid`, `arrow.opaque`, etc.
- [GeoParquet specification](https://geoparquet.org/) — metadata-only convention for geospatial columns in Parquet; relevant if `ir.Geometry` round-trip matters.
