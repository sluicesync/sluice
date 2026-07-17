# ADR-0168: `mariadb` flavor — Phase-1 bulk source+target with honest capabilities, CDC refused loudly

## Status

**Accepted (2026-07-16).** Roadmap item 73 Phase 1. Scoped by the 2026-07-16 live probe (operator's sluice-testing `workspace/mariadb/scoping-probe.md`: mariadb:11.4.12 + 10.11.18 side by side with mysql:8.4, shipped v0.99.263 binary); every convention this ADR encodes was re-ground-truthed on live containers during implementation and is pinned by a unit parity matrix plus an integration matrix on both LTS lines.

## Context

MariaDB is the largest MySQL-adjacent install base sluice didn't serve. The probe found the gap smaller and better-shaped than feared: every mariadb-as-source surface failed loudly at one choke point (two MySQL-8-only information_schema columns), every write-as-target surface at another (the MySQL 8.0.20+ row-alias upsert), and **no silent-loss path was reachable** — but only *accidentally*: behind the catalog wall sat a defaults-convention divergence that a naive "just fix the query" patch would have turned into a silent default-corruption class. The flavor pattern (Vanilla / PlanetScale / Vitess precedent, flavor.go's "Be honest — declared capabilities drive runtime strategy") exists for exactly this.

## Decision

Register `FlavorMariaDB` as engine name `mariadb` (supported floor: **MariaDB 10.11 LTS**; integration matrix runs 10.11 and 11.4). Phase-1 capabilities are deliberately narrow and honest:

- **Bulk migrate source + target, backup/restore/verify.** `BulkLoad: LoadDataInfile` (the probe's restore-into-11.4 leg landed the full corpus byte-identically through LOAD DATA LOCAL; MariaDB ships `local_infile=ON`); the per-call BatchedInsert fallback carries the flavor's upsert spelling.
- **CDC: CDCNone, refused loudly and coded.** MariaDB replicates with domain-based GTIDs (`0-100-38`) the MySQL binlog reader's position codec cannot parse or resume. `OpenCDCReader`/`OpenServerCDCReader` and the pipeline's CDC preflights (sync start, backup stream/incremental, add-table) refuse with the new `SLUICE-E-CDC-MARIADB-UNSUPPORTED`, naming Phase 3 and the trigger-less alternatives (bulk migrate + cutover, backup/restore). A new optional interface, `ir.CDCUnsupportedExplainer`, lets an engine flavor supply that refusal to the otherwise-generic orchestrator preflights without any engine import leaking into the pipeline.
- **JSONSupport: JSONText** (MariaDB JSON is a LONGTEXT alias; identity recovery via the auto `json_valid` CHECK is Phase 2). **ExtGeometry excluded** (MariaDB spells the column SRID attribute `REF_SYSTEM_ID=n` and has no `srs_id` catalog column to read one back from — carrying geometry today would silently drop SRIDs on read and emit unparseable DDL on write; Phase 2).

### The four MariaDB leaves

1. **Catalog queries** (`schema_reader.go`): `columns.srs_id` (MySQL 8.0) and `statistics.expression` (8.0.13 functional indexes) don't exist on MariaDB — the flavor's query variants select constants (`0` / `''`) in their place; the MySQL-8 query text is pinned byte-identical to the historical constants. MariaDB functional indexes are generated columns, which arrive through the ordinary generated-column path — the `''` substitution is faithful, not a placeholder.

2. **The COLUMN_DEFAULT shim** (`translateMariaDBDefault`) — ATOMIC with (1), the probe's hidden-hazard finding. MariaDB ≥ 10.2.7 reports the DEFAULT *expression* text: string literals **with quotes** (`'abc'`, `''` doubling, and the same `\0 \n \r \\` schema-metadata escape set MySQL uses for ENUM labels — decoded by the existing `scanMySQLQuotedString`, including NUL-bearing binary defaults, which MariaDB escape-encodes rather than C-truncating); the bare keyword `NULL` for defaultless-nullable/`DEFAULT NULL`; `current_timestamp()` lowercase-with-parens and **empty `extra`** (no DEFAULT_GENERATED token exists); evaluated bare numerics; everything else bare expression text. The shim classifies on the surface form, re-encodes binary-family literals into MySQL's `0x…` hex form, and canonicalizes the CURRENT_TIMESTAMP family — so the IR from a MariaDB read is **byte-identical** to what the same logical schema produces via a MySQL 8 read (unit parity matrix + live three-server integration matrix, every shape × family). Malformed shapes carry verbatim and fail loudly on the target.

3. **Upsert spelling** (`upsertSpelling`): MariaDB never implemented the 8.0.19+ row alias (`AS new` is Error 1064 on all versions) and keeps the legacy `VALUES(col)` function MySQL 8 deprecates. The spelling is flavor-derived and threaded through the **whole class** of `AS new` emission sites — the change applier (single-row, ADR-0139 multi-row, ADR-0007 position write, schema history), the migrate-state store, and the batched-insert row writer — each pinned byte-exactly in both spellings, and executed live against MariaDB. Zero value is the row alias, so every existing construction is byte-identical.

4. **Cross-family collation remap** (emitter-level, surfaced never silent): a MySQL 8 source's default `utf8mb4_0900_*` collations don't exist on MariaDB 10.11 (Error 1273; 11.4 aliases them), and a MariaDB 11.4 source's default `utf8mb4_uca1400_ai_ci` doesn't exist on any MySQL 8. The mariadb-flavor writer maps `utf8mb4_0900_{ai_ci,as_ci,as_cs}` → `utf8mb4_uca1400_*` and `0900_bin` → `utf8mb4_nopad_bin` (all present on both LTS lines — mapping unconditionally avoids a version-gated emit); the MySQL-family writers apply the mirror map. One WARN per table on CREATE, one per column on the ALTER paths, naming each swap and the known deltas (UCA 9.0 vs 14.0 edge weights; PAD semantics). Language-specific variants are deliberately unmapped — loud 1273 beats guessing a language table.

### Guards

- **Server-fingerprint guard** at `OpenSchemaReader`/`OpenSchemaWriter` (one `SELECT VERSION()`): the mariadb flavor **refuses** a non-MariaDB server (coded `SLUICE-E-DRIVER-HOST-MISMATCH` — the shim would actively mis-read MySQL conventions, e.g. a bare `abc` default classifies as an expression); the plain `mysql` flavor **WARNs** toward `--source-driver/--target-driver mariadb` when the server fingerprints as MariaDB (`-MariaDB` in VERSION()) — deliberately not a hard refusal; the loud `srs_id` wall still follows. Below the 10.11 floor: WARN, proceed.
- **Invisible-object census**: MariaDB reports `SEQUENCE` and `SYSTEM VERSIONED` as distinct table_types the `BASE TABLE` filter silently misses (the Bug-100/INHERITS silent-loss class). The mariadb reader censuses both and refuses loudly naming every object; Phase 2 carries them.

## Alternatives considered

- **Blanket VALUES() spelling for all flavors** — rejected: MySQL 8 deprecates VALUES() with a warning per statement; flavor-gating keeps the modern form everywhere it's supported.
- **Version-gated collation passthrough (0900 verbatim on 11.4)** — rejected for determinism: the uca1400 names are valid on both LTS lines and are what 11.4's own aliases resolve to; one emit shape for the whole floor beats a probe-dependent one.
- **Hard-refusing plain `mysql` against a MariaDB server** — rejected per the roadmap: the flavor is the honest path and the WARN steers; the existing loud failure already prevents any silent read.

## Consequences

- mariadb ↔ {mysql, postgres} bulk migrate, backup→restore, and verify (both roles) work and are integration-pinned on 11.4 and 10.11, corpus-ground-truthed row-for-row against MySQL 8 anchors.
- `sync start` / `backup stream` / `backup incremental` from MariaDB exit with a coded, remedied refusal instead of a raw catalog error.
- Phase 2 (uuid/inet6/inet4 native types, JSON identity via json_valid, sequences + system-versioned tables, `POINT SRID`→`REF_SYSTEM_ID`) and Phase 3 (domain-GTID CDC — the go-mysql `MariadbGTIDSet` support is already vendored) remain roadmap item 73.
- The `SLUICE-E-CDC-MARIADB-UNSUPPORTED` code and the `mariadb` engine name are frozen compatibility surfaces from first release.
