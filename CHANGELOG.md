# Changelog

All notable changes to sluice are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the
project follows [Semantic Versioning](https://semver.org/).

## [Unreleased]

**v0.32.1 — hstore PG → PG COPY binary codec completes the v0.31.0 Tier 1 deferred work.** The v0.31.0 release shipped hstore as an ADR-0032 Tier 1 extension but deferred PG → PG passthrough because the IR carries text-form hstore bytes (`"k"=>"v"`) and PG's COPY binary protocol expects hstore's pair-array wire format. The preflight refusal in `validateEnabledPGExtensions` surfaced an actionable workaround hint until the codec landed. This release implements the codec and removes the refusal.

### Fixed

- **`--enable-pg-extension hstore` on PG → PG bulk-copy now succeeds.** Pre-fix: `pipeline.validateEnabledPGExtensions` refused at preflight with a `--type-override hstore_column=text` workaround hint; the bulk-copy path could not encode hstore values through pgx's binary COPY protocol because no per-conn `pgtype.Codec` translated the IR's text-form hstore bytes into hstore's pair-array wire format. The refusal branch is gone; PG → PG hstore now round-trips byte-for-byte the same way the existing pgvector / pg_trgm / citext same-engine paths do.
- **New codec at `internal/engines/postgres/hstore_codec.go`** — `pgHstoreBinaryCodec` mirrors `pgvectorBinaryCodec`'s structural pattern (v0.26.0). Accepts `string` and `[]byte` text-form hstore inputs; emits PG's binary hstore wire format (`int32 BE pair-count + per-pair int32 BE keylen + key bytes + int32 BE vallen + value bytes`, with `vallen = -1` signalling SQL NULL on the value side). Decoder is symmetric for unit-test round-trip and future typed-scan paths.
- **Two-codec framework now documented as the pattern for future Tier 2+ extensions with their own wire formats** (e.g. PostGIS EWKB). The COPY path's per-conn codec registration gate consults both `tableHasPGVectorColumn` and the new `tableHasHstoreColumn` helper; future extensions add a sibling helper + a `registerPG<Ext>Codec` function + an entry in the same gate.
- **`TestMigrate_PG_Hstore_Passthrough` (un-skipped) now pins the end-to-end PG → PG hstore round-trip** with 3 rows including the canonical multi-pair shape (`"a"=>"1", "b"=>"2"`), a single-pair shape, and `pg_attribute` verification that the target column's type is `hstore` (not a text column with hstore-shaped content). Same-engine and cross-engine pgvector / pg_trgm / citext tests are regression-clean.

### Migration / Compatibility

- **Bug fix only; no operator surface change.** `sluice migrate --enable-pg-extension hstore` against PG sources/targets now works end-to-end without `--type-override`. Operators who deployed the v0.31.0 workaround (`--type-override hstore_column=text`) can drop the override; the hstore-as-text shape they got is byte-equivalent to what the codec now ships through binary COPY.
- **Cross-engine PG → MySQL hstore path is unaffected.** That branch uses the value translator (`hstore` → JSON via `prepareHstoreToJSON` in `internal/engines/mysql/row_writer.go`); the v0.31.0 codepath had no preflight refusal there and the v0.32.1 codec is not consulted on a non-PG target.

### Who needs this release

- **Operators running PG → PG migrations or syncs with hstore columns:** **upgrade** — the v0.31.0 preflight refusal is closed and the migration now proceeds without the `--type-override` escape hatch.
- **Operators on the cross-engine PG → MySQL hstore path:** drop-in; no behavior change (v0.31.0 + v0.31.1 already worked end-to-end via the JSON translator).
- **Operators not using hstore:** drop-in; no behavior change.

## [0.32.0]

**Mid-stream live add-table strict zero-loss closes the v0.24.0 best-effort gap (Item 3, ADR-0036 Phase B).** The four-round Phase A diagnose campaign on the v0.24.0 residual loss surface — Phase A.1 ruled out M1/M2/M4 and reframed M3; Phase A.2 falsified reframed M3 (10/10 runs); Phase A.3 confirmed M5c (applier-side drop, 10/10 runs); Phase B identified the precise drop site via code-reading — pinned the failure to a single load-bearing line in the PG applier's dispatch path. The fix is a one-line orchestration reorder plus a small idempotent-bulk-copy helper.

### Fixed

- **PG mid-stream live add-table (`sluice schema add-table --no-drain`) now delivers every in-flight CDC event on the new table exactly-once-effectively.** Pre-fix: events on the new table at LSN > publication-add LSN that reached the active stream's applier in the window between `ALTER PUBLICATION ADD TABLE` and `runBulkCopy`'s `CREATE TABLE ... IF NOT EXISTS` were silently skipped by the applier's `errUnknownTable` defence-in-depth branch (Bug 13 path), then never re-delivered (pgoutput doesn't replay). v0.24.0 documented this as a "best-effort" property; CI's under-load test reported ~36% loss at sub-second burst rates with no shipped fix. Phase A.3 confirmed the drop site (applier receives, then drops); Phase B fixes it.
- **Drop site (code-reading evidence):** `internal/engines/postgres/change_applier.go::dispatch`'s Insert case — when `colTypesFor` returns `errUnknownTable` because `information_schema.columns` is empty (target table doesn't exist), the dispatch logs a warning and `return nil`. Bug 13's defence-in-depth path stays in place for genuinely-drifted publications; the v0.24.0 race is closed at the orchestration layer instead.
- **Fix shape (~150 LOC):**
  - `internal/pipeline/add_table.go::AddTable.Run` step 3a (NEW): `sw.CreateTablesWithoutConstraints(ctx, scoped)` runs BEFORE publication-add. By the time pgoutput's per-LSN catalog snapshot includes the new table, the target table is already in `information_schema.columns` and the applier's dispatch path succeeds.
  - `internal/pipeline/migrate.go::runBulkCopyForAddTable` (NEW): mid-stream-live-add variant of `runBulkCopy` that routes the row copy through `ir.IdempotentRowWriter.WriteRowsIdempotent` (INSERT … ON CONFLICT (pk) DO UPDATE) when the writer exposes it. With the target table pre-created, a small number of CDC events on the new table may have already been applied by the active stream's applier by the time bulk-copy reaches those rows (events in the [publication-add, snapshot-open] window); the idempotent path absorbs the overlap. Engines without the surface (none today — both PG and MySQL implement it) fall back to plain `WriteRows` with a debug log.
- **Engine-symmetric.** Both PG and MySQL schema writers use `CREATE TABLE IF NOT EXISTS`; both engines implement `ir.IdempotentRowWriter`. MySQL's filter-flip mechanism (ADR-0034) didn't manifest the race in v0.24.0 (it gates dispatch on `live_added_tables` membership rather than table existence), but the early-create + idempotent-copy shape removes engine-specific timing assumptions from the orchestrator.
- **Regression artifact.** `TestAddTable_LiveMode_PG_UnderLoad` tightened from best-effort logging (`if got < maxTotal { t.Logf(...) }`) to strict zero-loss assertion (`if got != maxTotal { t.Errorf(...) }`). `TestAddTable_LiveMode_PG_DiagnoseLossSurface` and the Phase A.3 applier-side capture probe in `change_applier.go::dispatch` stay as permanent diagnostic artifacts (mirrors how ADR-0033's slot-pause verify test stays as proof-of-falsification).

### Migration / Compatibility

- **Bug fix only; no operator surface change.** `sluice schema add-table --no-drain TABLE` runs the same way; the in-flight-loss caveat from ADR-0030 § "What could go wrong" item 3 is closed without operator intervention.
- **Operators with high write-rate workloads who deferred live add-table** in favour of the drained flow (or operator-coordinated quiesce per Path C) can switch to `--no-drain` without the small residual loss caveat.
- **Path B (dual-slot) and Path C (operator quiesce) are no longer needed** for the v0.24.0 loss surface. They remain available as forward options for unrelated edge cases (e.g. M1 long-txn straddling under workloads where Phase A.1's 1-in-6 rate becomes operator-relevant).

### Who needs this release

- **Operators running mid-stream live add-table (`sluice schema add-table --no-drain`) on PG → PG or PG → MySQL with sustained write rates on the new table at the moment of live-add:** **upgrade** — the residual loss surface that was best-effort in v0.24.0 through v0.31.1 is closed.
- **Operators on the drained add-table flow:** drop-in; no behavior change (the drained flow already had zero-loss semantics by construction).
- **MySQL → MySQL operators using `--no-drain`:** drop-in; the ADR-0034 filter-flip path didn't manifest the same race, but the orchestration reorder removes engine-specific timing assumptions from the flow.

## [0.31.1]

**Bug 48 fix: hstore PG → MySQL cross-engine works under LOAD DATA path.** v0.31.0's headline feature succeeded on the batched-INSERT path (`local_infile=OFF`) but failed cryptically on the LOAD DATA path (`local_infile=ON`) with `Error 3144 (22032): Cannot create a JSON value from a string with CHARACTER SET 'binary'`. Surfaced by the v0.31.0 cycle on a MySQL target with the recommended `local_infile=ON` setting. Real-world operator UX bug; not a regression.

### Fixed

- **Bug 48 — `columnSetExpr` in `load_data_writer.go` now wraps `ir.ExtensionType` columns in `CONVERT(@var USING utf8mb4)`.** Pre-fix: only `ir.JSON` / `ir.Varchar` / `ir.Text` / `ir.Set` got the wrapper; hstore (which the cross-engine path translates to MySQL JSON via `prepareHstoreToJSON`) was sent as charset=binary and rejected by MySQL's JSON parser. Citext also benefits (it lands as VARCHAR with `_ci` collation; the same charset reinterpretation applies). Other ExtensionType arms (vector / pg_trgm / postgis) don't reach `columnSetExpr` — cross-engine preflight refuses them. New unit-test pins for both hstore and citext in `TestColumnSetExpr`.

### Migration / Compatibility

- **Bug fix only; no operator-visible behavior change beyond the failure path closing.** Drop-in upgrade from v0.31.0.
- **Operators who hit Bug 48 on v0.31.0** (cross-engine PG → MySQL with hstore source columns and `local_infile=ON` on the MySQL target) should retry the migration after upgrading. The fix is targeted to the LOAD DATA path; batched-INSERT (the v0.31.0 success path) is unaffected.

### Who needs this release

- **Cross-engine PG → MySQL operators with hstore (or future translator-bearing extension) columns AND `local_infile=ON` on the MySQL target:** **upgrade** — v0.31.0's headline feature only worked under the slower batched-INSERT path.
- **Operators on `local_infile=OFF`:** drop-in; no behavior change (v0.31.0 already worked for you).

**PG → PG `hstore` (cross-engine only this release) and `citext` extension passthrough land as the v1 shortlist's Tier 1 entries (ADR-0032).** Both are type-only opaque-bytes extensions: hstore is the key/value-map type, citext is `text` with a case-insensitive collation. Cross-engine PG → MySQL gets built-in default translators for both (hstore → MySQL JSON with value-shape conversion; citext → MySQL VARCHAR with `utf8mb4_0900_ai_ci` collation). citext also ships PG → PG passthrough. **hstore PG → PG is deferred to v0.31.1** — the COPY-protocol binary codec for hstore (mirroring pgvector's pattern from v0.26.0) hasn't landed yet; preflight refuses with an actionable workaround hint (`--type-override hstore_col=text`). This is the first ADR-0032 entry to ship a cross-engine translator alongside same-engine passthrough — the framework formalises the surface (`ir.CrossEngineExtensionTranslator` optional engine interface; per-extension `crossEngineDefaultTranslatedExtensions` catalog declaration) so future Tier 1 entries with defensible cross-engine mappings can follow the same pattern.

### Deferred

- **hstore PG → PG passthrough deferred to v0.31.1.** Preflight refuses `--enable-pg-extension hstore` when target is PG with an actionable hint. Workaround until the binary codec lands: `--type-override hstore_col=text` per column (preserves text form on PG target). Cross-engine PG → MySQL hstore works as advertised — the value translator handles the wire format. Tracked: the COPY binary codec needs to mirror `pgvectorBinaryCodec`'s shape, accepting `[]byte` text-form hstore input and emitting PG's binary hstore wire format (int32 BE pair-count + length-prefixed key/value pairs).

### Added

- **`hstore` PG extension catalog entry (`internal/engines/postgres/extension_catalog.go`).** Declares the `hstore` udt under `typesByName`; emits the bareword `hstore` in the PG column-DDL position. No `indexAccessMethods` / `indexOperatorClasses` — the hstore GIN / GiST operator classes are out-of-v1-scope per the research doc and ship in a future PR if operator demand surfaces.

- **`citext` PG extension catalog entry.** Declares the `citext` udt; emits bareword `citext`. citext rides on core PG btree / gin / gist (no extension-owned AMs / opclasses), so the catalog entry is the minimum-viable shape: typesByName + build + emit, both AM and opclass sets empty.

- **Cross-engine PG → MySQL default translators (ADR-0032 § "Cross-engine policy").** MySQL writer's `emitColumnType` rewrites `ir.ExtensionType{Extension:"hstore"}` to `JSON` and `ir.ExtensionType{Extension:"citext"}` to `VARCHAR(255) COLLATE utf8mb4_0900_ai_ci`. Value-side conversion for hstore lives in `mysql/row_writer.go::prepareValue`: a new `prepareHstoreToJSON` + `parseHstoreText` pair reparses the PG hstore canonical wire form (`"k"=>"v"`) into a JSON object string the MySQL JSON column accepts. citext value translation is identity (the case-insensitive comparison is a server-side property of the collation, not the wire format).

- **`ir.CrossEngineExtensionTranslator` optional engine surface (`internal/ir/interfaces.go`).** New interface `HasCrossEngineDefaultTranslator(name string) bool` lets an engine declare which of its extensions can cross-engine-translate without operator intervention. PG's `Engine` implements it backed by the catalog's `crossEngineDefaultTranslatedExtensions` registry (today: hstore, citext). The pipeline's `validateEnabledPGExtensions` consults this on the target-not-PG branch: extensions with declared translators pass through; others keep the loud-failure refusal.

- **`extensionOwningType` helper + actionable schema-reader hint.** The PG schema reader's user-defined-fallthrough error now names the owning extension when udt_name matches a known catalog entry the operator didn't enable — e.g. `user-defined type "hstore" is owned by extension "hstore"; pass --enable-pg-extension hstore to enable passthrough (ADR-0032)`. Replaces the v0.26.0+ vague "not a recognised enum" wording for the load-bearing case.

- **`translate.RetargetForEngine` extended for hstore + citext.** The `sluice schema diff` cross-engine path's expected-shape pass now rewrites hstore and citext to their MySQL forms so target-side diff comparison sees the same shape the migrate path lands on. Other extension types (vector, pg_trgm, postgis) continue to fall through unchanged so the cross-engine refusal in `checkCrossEngineSupportable` fires.

- **Integration tests (`internal/pipeline/migrate_hstore_citext_integration_test.go`, `integration` build tag).** Four cases: `TestMigrate_PG_Hstore_Passthrough` + `TestMigrate_PG_CiText_Passthrough` (same-engine PG → PG byte-for-byte round-trip with type + value + functional-query assertions); `TestMigrate_PG_Hstore_CrossEngineToMySQL` + `TestMigrate_PG_CiText_CrossEngineToMySQL` (cross-engine, with MySQL-side `JSON_TYPE`/collation/case-insensitive-lookup ground-truth queries on the target). Boots stock `postgres:16` (both extensions ship in standard contrib bundle — no special image required).

- **Unit tests across catalog, retarget, value-decode, prepareValue, ddl-emit surfaces.** Pin both extensions' catalog shapes (typesByName / emit / build / AM / opclass sets), the cross-engine translator policy declaration, the retarget rules, the value-decode passthrough, the MySQL writer's hstore-to-JSON conversion (including the standalone `parseHstoreText` parser's positive and negative cases), the citext identity value translation, and the MySQL emit forms.

### Changed

- **`--enable-pg-extension` help text refreshed** on `migrate`, `sync start`, `schema preview`, and `schema diff` to list all four shipped extensions and call out the cross-engine translator carve-out for hstore + citext.

- **`validateEnabledPGExtensions` no longer refuses non-PG targets unconditionally.** Per-extension policy: extensions whose source engine declares `HasCrossEngineDefaultTranslator` may pass against a non-PG target; the refusal message names the specific non-translatable extension when a mixed list is given.

- **`unsupportablePGtoMySQL` carves out hstore and citext** from the blanket ExtensionType refusal — both have default translators wired into the MySQL writer directly. The carve-out is data-driven via the `isCrossEngineTranslatablePGExtension` helper; vector / pg_trgm / postgis continue to refuse loudly.

### Migration / Compatibility

- **No format-breaking changes.** Manifest schema, change-chunk format, CLI surface — all unchanged for existing flows. No new required CLI flags; existing `--enable-pg-extension` flag picks up the two new recognised names.
- **Default behavior unchanged.** Operators not using `--enable-pg-extension hstore` or `citext` see no behaviour change — hstore/citext columns continue to surface as a loud failure at type-resolution time on PG sources (now with a clearer message naming the missing flag).
- **Drop-in upgrade from v0.30.3.** No DDL migration; no operator action required unless you're explicitly opting into the new extensions.
- **Cross-engine PG → MySQL operators with hstore or citext columns:** **upgrade is the migration**. Pre-v0.31.0 hstore/citext columns refused at schema-read time even with `--type-override`; now `--enable-pg-extension hstore` (or citext) opts into the built-in default translator without a per-column override.

### Known limitations

- **hstore GIN / GiST operator classes are not preserved.** The catalog entry declares no `indexOperatorClasses`, so a `CREATE INDEX ... USING GIN (col)` on an hstore column will round-trip the AM but drop the opclass — and PG's `gin` has no default opclass for hstore, so the target's CREATE INDEX will fail loudly. Future PRs may add the opclass round-trip if operator demand surfaces (the pattern is mechanical — mirror pg_trgm's catalog shape).
- **No length-aware citext.** PG citext is unbounded text; the cross-engine MySQL translator picks `VARCHAR(255)` as a reasonable default. Operators with longer values supply `--type-override TABLE.COL=text` (or `varchar:length=N`) per column.
- **No MySQL → PG path.** MySQL has no hstore source; citext source is "VARCHAR with `_ci` collation" but the reverse translator (MySQL `VARCHAR _ci` → PG citext) is out of scope. MySQL → PG operators who want PG citext on the target supply `--type-override TABLE.COL=citext` explicitly (the catalog's emit side handles it; the schema reader just doesn't auto-recognise the MySQL collation as citext-shaped).
- **Cross-engine hstore wire-format strictness.** The MySQL writer's `parseHstoreText` rejects malformed input loudly (unterminated quoted strings, missing arrows, etc.) and falls through to driver-side "Invalid JSON text" errors rather than emitting partial JSON. Real-world hstore values produced by PG always pass; pathological hand-constructed shapes surface clear errors.

### Who needs this release

- **Operators with PG sources using hstore for tags / attributes / configuration maps:** **upgrade**. Same-engine PG → PG previously refused at schema-read; now passthrough works with `--enable-pg-extension hstore`.
- **Operators with PG sources using citext for emails / case-insensitive identifiers:** **upgrade**. Same-engine PG → PG passthrough preserves the type; cross-engine MySQL targets land as VARCHAR with case-insensitive collation so application-side equality queries continue to work.
- **Cross-engine PG → MySQL operators with hstore or citext columns:** **upgrade**. Previously required `--type-override` per column; now `--enable-pg-extension` opts into a sensible default. Operators wanting a non-default shape still use `--type-override`.
- **Everyone else:** drop-in; no behavior change.

## [0.30.3]

**One-line test fix: `TestChunkEncryptedRoundTrip` no longer flakes on the 2-byte `"id"` substring check.** Hit once on the v0.30.2 main CI run. Pre-existing latent flake; not a regression.

### Fixed

- **`TestChunkEncryptedRoundTrip` 2-byte substring false-positive.** `backup_chunk_test.go:194` checked that encrypted chunk bytes don't contain banned plaintext substrings — including `"id"` (2 bytes). `"id"` appears in random ciphertext ~certainly at typical chunk sizes (`P("id" in 1KB random) ≈ 1024/65536 = ~1.5% per byte position × ~1024 positions`). v0.30.3 drops `"id"` from the banned list; the remaining 4–5-byte strings (`"alpha"`, `"beta"`, `"name"`) have `P(banned in 1KB random) ≈ 1 in 10^12` — effectively zero false positives. Encryption-correctness coverage unchanged.

### Migration / Compatibility

- **Test-only fix; no operator-visible behavior change.** Drop-in upgrade from v0.30.2.

### Who needs this release

- **Project maintainers + contributors:** another release-flow flake permanently closed. No operator impact.

## [0.30.2]

**Test-stability + CLI help-text patch.** Two operator-invisible nits surfaced via the v0.29.x / v0.30.x release flow. Neither affects production behavior.

### Fixed

- **`TestSyncFromBackup_FanOut` flake closed.** The fan-out integration test runs two brokers in the same Go process against the same chain root; both defaulted to `manifests/broker_state.json` for their state file. Concurrent `LocalStore.Put` to the same path occasionally hung one broker at startup — symptom: target N stayed at the seed row for the test's 90s wait window. Hit 3 of the last 4 main-CI release cycles (v0.29.1, v0.30.1 twice). Phase A diagnose (3 instrumentation passes) narrowed the hang to `writeBrokerState` between `coldStartAtChainID`'s return and the "broker: started" log; the test-side fix is to give each broker a distinct state path. New `brokerOpts.StatePath` field; `TestSyncFromBackup_FanOut` sets distinct paths per broker. Production unaffected — one broker process per chain. 30/30 PASS on Vultr under race detector (vs 15-20% baseline failure rate). Closes the persistent re-run cost on every other release flow.
- **`--enable-pg-extension` help text refreshed** to list pg_trgm alongside pgvector. v0.30.0's pg_trgm extension passthrough shipped without updating the four flag declarations (`migrate`, `sync start`, `schema preview`, `schema diff`) which still said "Recognised in v0.26.0: vector (pgvector)". Now: "Recognised: vector (pgvector), pg_trgm" (drops the version pin — CHANGELOG is the canonical version-pinning source). Surfaced by the v0.30.1 cycle.

### Migration / Compatibility

- **No format-breaking changes.** No CLI flag changes (only help-text refresh). No on-disk format changes. No engine-interface changes.
- **Drop-in upgrade from v0.30.1.** No DDL migration; no operator action required.

### Who needs this release

- **Operators tracking sluice's release cadence:** drop-in upgrade; no behavior change. Take it for the next release window.
- **Operators frustrated by `--enable-pg-extension` help text not mentioning pg_trgm:** the discoverability improvement is here.
- **Project maintainers + contributors:** every other release CI flow now skips the broker-fanout flake retry cost.

## [0.30.1]

**Two operator-UX gaps surfaced by the v0.30.0 cycle, both closed.** Neither is a regression or correctness issue; both are cleaner failure modes for cross-engine + extension scenarios.

### Fixed

- **Cross-engine PG → MySQL preflight refusal now fires on `migrate` path.** v0.30.0 added `unsupportablePGIndexToMySQL` and wired it into `chain_restore` only — `Migrator.Run` skipped the helper entirely, so a PG source with a pg_trgm-indexed table targeting MySQL bulk-copied successfully and then died at MySQL Error 1170 during the indexes phase (no recovery path; data already partially migrated). v0.30.1 calls `checkCrossEngineSupportable` from `Migrator.Run` right after `translate.ApplyMappings`, mirroring the chain_restore wire-up. The refusal now fires before any data moves.

- **`unsupportablePGIndexToMySQL` broadened to catch extension AMs without an opclass.** The v0.30.0 helper only checked `IndexColumn.OperatorClass`. That misses the no-flag scenario: when the operator runs cross-engine PG → MySQL without `--enable-pg-extension pg_trgm`, the schema reader strips `OperatorClass` from IR (loud-failure default) but `idx.Kind` stays `IndexKindGIN`. The opclass-only refusal returned empty and bulk-copy proceeded. The helper now also flags `IndexKindGIN` / `IndexKindGIST` indexes for non-PG targets. MySQL's FULLTEXT (`IndexKindFullText`) and SPATIAL (`IndexKindSpatial`) stay portable.

- **WARN log when extension-owned opclass is stripped due to missing `--enable-pg-extension` flag.** The opt-in gate in the schema reader was silent: operators saw the downstream raw PG error 42704 (`data type text has no default operator class for access method "gin"`) on the target with no sluice-side attribution. v0.30.1 emits a WARN at strip-time naming the index, column, opclass, owning extension, and the exact `--enable-pg-extension <name>` flag the operator probably wanted. New helper: `extensionOwningOperatorClass(opclass) string` in the postgres engine's catalog.

### Migration / Compatibility

- **No format-breaking changes.** No CLI changes, no on-disk format changes, no engine-interface changes.
- **Drop-in upgrade from v0.30.0.** No DDL migration; no operator action required.
- **Cross-engine PG → MySQL operators previously running migrations through silent failure:** the new refusal surfaces an actionable error message at preflight instead of leaving a partially-migrated target. If you previously worked around the failure by dropping pg_trgm indexes on source pre-migrate, that workaround is now sluice's first suggestion in the refusal message.

### Who needs this release

- **Cross-engine PG → MySQL operators with pg_trgm (or any GIN/GiST) indexes on the source:** **upgrade**. The refusal now fires at preflight; previously bulk-copied succeeded then indexes phase failed cryptically.
- **PG → PG operators using `--enable-pg-extension pg_trgm`:** drop-in; no behavior change.
- **Operators forgetting `--enable-pg-extension pg_trgm` on a PG → PG with pg_trgm-indexed source:** the new WARN at schema-read time tells you which flag to pass; the resulting CREATE INDEX failure path is otherwise unchanged.

## [0.30.0]

**PG → PG `pg_trgm` extension passthrough lands as the v1 shortlist's second concrete entry (ADR-0032).** pg_trgm is the "operator-class only" extension — no new column types, just `gin_trgm_ops` / `gist_trgm_ops` operator classes that ride on core PG `gin` / `gist` access methods. Sluice now recognises and round-trips trigram-indexed columns when the operator passes `--enable-pg-extension pg_trgm` on `migrate` / `sync start` / `schema preview` / `schema diff`. Validates the index-method-passthrough framework on a simpler shape than pgvector (Tier 2 lite) and clears the path for `hstore` / `citext` / `postgis` to follow as additional catalog entries.

### Added

- **`pg_trgm` PG extension catalog entry (`internal/engines/postgres/extension_catalog.go`).** Declares the two operator classes (`gin_trgm_ops`, `gist_trgm_ops`) the schema reader's index-population path now recognises. No `typesByName` entries (pg_trgm has no column types); `indexAccessMethods` empty (rides on core `gin` / `gist`); both `build` and `emitColumn` return loud sentinel refusals (defensive — never reached on the read or write path for a well-formed IR).

- **`extensionDef.indexOperatorClasses` now `map[string]struct{}` (was `[]string`).** Promoted from "metadata" to a queryable set so the schema reader can ask "is this opclass extension-owned?" in O(1). Existing `pgVectorDef` rewritten to the new shape (additive — same opclass coverage, just lookup-friendly).

- **`extensionOperatorClassEnabled(opclass, enabled)` helper.** Mirrors `extensionAccessMethodEnabled`; gates per-opclass passthrough on the operator's `--enable-pg-extension` allowlist. The schema reader's `populateIndexes` now consults both gates so a pg_trgm `gin (col gin_trgm_ops)` index on core PG `gin` AM survives the round-trip — the previous `idx.Method != ""` gate only fired for extension-introduced AMs (pgvector's ivfflat / hnsw) and dropped the pg_trgm opclass.

- **Cross-engine refusal for extension-owned operator classes (`internal/pipeline/cross_engine_supportable.go`).** PG → MySQL with a pg_trgm-indexed table now refuses loudly at the existing `checkCrossEngineSupportable` pre-flight (was silently passed through, would have failed at MySQL CREATE INDEX with an opaque opclass-unknown error). The refusal rides on `ir.IndexColumn.OperatorClass` non-empty — sluice never populates that field for core-PG opclasses (Bug 47 design), so the field's presence is an honest "extension-owned opclass" marker without re-importing the postgres engine catalog into the pipeline package.

- **Integration tests (`internal/pipeline/migrate_pgtrgm_integration_test.go`, `integration` build tag).** Four cases: GIN + GiST round-trip (ground-truth pg_index/pg_opclass query on the target verifies opclass survives), opt-in-skipped failure (operator forgot `--enable-pg-extension pg_trgm`; loud failure at index-create time), target-missing-extension preflight refusal (mirrors pgvector). Boots stock `postgres:16` (pg_trgm ships in standard contrib bundle — no special image needed; CI already pre-pulls).

- **Cross-engine refusal unit tests (`internal/pipeline/cross_engine_supportable_test.go`).** PG → MySQL `gin_trgm_ops` and `gist_trgm_ops` index refusals; PG → PG passthrough not refused (regression guard).

### Migration / Compatibility

- **No format-breaking changes.** Manifest schema, change-chunk format, CLI surface — all unchanged for existing flows.
- **Default behavior unchanged.** Operators not using `--enable-pg-extension pg_trgm` see no behaviour change — pg_trgm-indexed columns continue to surface as a loud failure at index-create time on the target (the existing pattern). The opt-in is the same shape as pgvector's v0.26.0 surface.
- **Drop-in upgrade from v0.29.1.** No DDL migration on `sluice_cdc_state`; no operator action required.
- **Cross-engine PG → MySQL operators:** if a previously silently-broken PG source with a pg_trgm index was being migrated to MySQL (and failing at MySQL CREATE INDEX with an opaque error), the new pre-flight refusal surfaces a clear operator-actionable message instead. The fix recommendation in the error message: drop the index on source before migrating, exclude the table, or supply a `--type-override` / `--index-override` mapping.

### Known limitations

- **No MySQL counterpart.** pg_trgm has no MySQL equivalent (MySQL's `FULLTEXT` indexes are different in shape — ranking, stemming, stop-words). Cross-engine PG → MySQL refuses; operators wanting fuzzy text search on the MySQL side need to design for MySQL's `FULLTEXT` ahead of time.
- **No version pinning.** pg_trgm 1.5 source → 1.4 target may surface subtle behaviour gaps that sluice doesn't see (per ADR-0032's Tier-2 framework caveat). `pg_extension` presence check is the only pre-flight; version-pinning syntax is a future refinement if real operator demand surfaces.
- **No new column types means no new IR variant.** Unlike pgvector (which introduced `ir.ExtensionType{Extension: "vector", ...}`), pg_trgm columns flow through as plain `ir.Text` / `ir.Varchar`. The catalog entry's surface is the operator-class round-trip and the cross-engine refusal — operators reading the IR will not see a `pg_trgm`-flavoured column type.

## [0.29.1]

**Closes Bug 47 — MySQL writer corrupts empty JSON object `{}` to empty JSON array `[]` on bulk copy.** Pre-existing latent bug reproducing back to v0.20.0; surfaced in v0.29.0 cycle. A simple "preserve `{}` bytes" fix in `convertArrayLikeToJSON` was attempted, rolled back when it broke `TestMigrate_PostgresToMySQL_ArrayToJSONOverride` (Bug 14's load-bearing test) — the two paths converge irreducibly at `prepareValue([]byte("{}"), ir.JSON{...})` with no signal from the bytes + target type alone. The proper fix carries pre-override context: `ir.Column` gains an optional `SourceColumnType` field that `translate.ApplyMappings` populates when an override fires. The MySQL writer's `prepareValue` now consults this to disambiguate the two surfaces: `SourceColumnType = ir.Array{...}` → `[]` (Bug 14 override path); nil or non-array → `{}` (Bug 47 round-trip path).

### Fixed

- **Bug 47 — empty JSON object `{}` round-trip on MySQL targets.** MySQL JSON source columns carrying `{}` now land on MySQL targets as `{}` (`JSON_TYPE = OBJECT`), not `[]` (`JSON_TYPE = ARRAY`). PG → MySQL also fixed (same writer-side path). Bug 14's PG `text[]` override to `jsonb` continues to land empty arrays as `[]` (load-bearing integration test still green).

### Migration / Compatibility

- **No format-breaking changes.** No CLI changes, no on-disk format changes, no engine-interface changes.
- **Drop-in upgrade from v0.29.0.** No operator action required.
- **`ir.Column.SourceColumnType` is an internal IR field.** Engines and external consumers reading `ir.Column` see it as nil unless `translate.ApplyMappings` has run with an explicit per-column override. Field is informational — writers consult it; readers do not populate it.

### Who needs this release

- **Operators running MySQL → MySQL or PG → MySQL `migrate` / `sync` on workloads that use `'{}'` as a JSON value** (e.g. as a default for not-yet-populated `attrs` / `metadata` / `config` columns). Pre-fix: those values silently flipped to `'[]'` on the target. Detection: `SELECT JSON_TYPE(col) FROM table` on src vs dest after migrate; mismatched OBJECT-vs-ARRAY counts confirm the bug fired. Workaround in older versions was changing source defaults from `'{}'` to a placeholder like `'{"_": null}'` — no longer needed.
- **Everyone else:** drop-in; no behavior change.

## [0.29.0]

**CI structural improvements + Path D Phase A diagnostic infrastructure for mid-stream live add-table loss.** Two operator-visible threads land together: (a) the `Integration (PostGIS)` job is now a separate parallel CI gate (cuts wall-clock from ~45 min to ~25 min on tag pushes); (b) sluice now runs `integration vstream` (vttestserver-based VStream coverage) on an always-on Vultr instance during pre-release validation, closing the gap CI explicitly skipped for cost reasons. Internal: new ADR-0036 documents the empirical Phase A.1 characterization of the mid-stream live add-table residual loss surface — diagnostic-only branch, no behavior change to the v0.24.0 best-effort flow yet.

### Added

- **Parallel `Integration (PostGIS)` CI job** in `.github/workflows/ci.yml`. Splits the postgis-tagged tests off from the main Integration job into a sibling runner so the two run concurrently. CI wall-clock for a tag push drops from ~45 min sequential to ~25 min parallel. The new job is required-to-pass-before-merge on `main` (see `docs/dev/branch-protection.md` for the updated `gh api PUT` snippet).

- **VStream coverage on the Vultr pre-release validation box.** `integration vstream` (vttestserver-backed Vitess coverage) now runs on the always-on Vultr instance as part of the pre-release runbook; reference timing 3m43s on a vhf-3c-8gb. Roadmap item 10 records this as "Path C — Vultr-box pre-release validation (LANDED)" alongside the original Path A (operator-run checklist) and Path B (CI-integrated coverage). Documented in `docs/dev/notes/release-validation-on-vultr.md`.

- **ADR-0036 — Mid-stream live add-table residual loss surface (Phase A.1).** Empirical characterization of v0.24.0's documented best-effort gap. Six-run multi-iteration on Vultr with targeted DEBUG-level slog instrumentation: M1 (long-txn) contributes rarely (1/6 runs, 1 row); M2 (snapshot consistent-point race) ruled out conclusively; M3 (catalog-snapshot lag) reframed and INCONCLUSIVE pending Phase A.2 with per-row source-side LSN cross-reference; M4 (counter artifact) ruled out — loss is real (~5–9% in the diagnostic test). New diagnostic test `TestAddTable_LiveMode_PG_DiagnoseLossSurface` ships as a permanent regression artifact for the next iteration. No production fix lands in v0.29.0.

- **`Engine.ReadCurrentWALPosition` optional engine surface** (PG-only). Returns the current WAL position via `pg_current_wal_lsn()`; mirrors the existing `ReadSlotPosition` pattern. Used by ADR-0036's diagnostic instrumentation; available to other observability use cases.

### Changed

- **CI Integration job timeout bumped** from 50 min to 60 min on the outer envelope; postgis step's individual timeout from 15 min to 25 min. Defensive headroom; the parallel split makes both unlikely to be hit in normal operation.

### Migration / Compatibility

- **No format-breaking changes.** No CLI changes, no on-disk format changes, no engine-interface changes that affect external implementations.
- **CI required-checks list grew by one** — `Integration (PostGIS)`. Operators using their own fork's branch-protection rules should add this to their required-checks list (snippet in `docs/dev/branch-protection.md`).
- **Drop-in upgrade from v0.28.0.** No DDL migration on `sluice_cdc_state`; no operator action required.

### Who needs this release

- **Operators tracking sluice's release cadence:** drop-in upgrade; no behavior change. Take it for the next release window.
- **Operators on PostgreSQL with `--no-drain` mid-stream live add-table:** no functional change in v0.29.0. The instrumentation lands but is gated behind `--log-level=debug`. The actual mitigation (Path B dual-slot or Path C operator quiesce) is queued behind ADR-0036's Phase A.2 verdicts.
- **Operators contributing to sluice:** the parallel CI job + faster wall-clock cuts the PR-feedback loop. The Vultr-box pre-release validation runbook is documented in `docs/dev/notes/release-validation-on-vultr.md`.

## [0.28.0]

**PostGIS-aware GEOMETRY/SPATIAL translation closes Bug 26 + Bug 27.** Sluice's IR has carried `ir.Geometry` since the beginning, but cross-engine PG ↔ MySQL geometry has been refused at the schema-write boundary (PG: "GEOMETRY requires PostGIS"; MySQL: SRID dropped). v0.28.0 lifts the refusal: PostGIS-detected PG targets accept `ir.Geometry` columns and emit `geometry(<subtype>, <srid>)`; MySQL targets emit `<type> SRID <n>` (MySQL 8.0+ syntax) preserving the SRID; cross-engine PG → MySQL round-trip closes Bug 26. The VStream-specific 4-byte SRID prefix on POINT bytes is now stripped + captured (closes Bug 27); vanilla MySQL protocol path unchanged. ADR-0035 documents the design.

### Fixed

- **Bug 26 — PostGIS SRID dropped on cross-engine schema translation.** `ir.Geometry.SRID` field carries the SRID from PG's `geometry_columns` view through translation. PG schema writer emits `geometry(<subtype>, <srid>)` when PostGIS is detected (init-time `SELECT 1 FROM pg_extension WHERE extname = 'postgis'`); without PostGIS, the existing loud refusal is preserved. MySQL `emitColumnDef` appends `SRID <n>` when `ir.Geometry.SRID != 0` (MySQL 8.0+ syntax); SRID 0 still emits the bare type. WKB → EWKB conversion at PG write-time injects the SRID's 4-byte prefix; EWKB → WKB on PG read-time captures the SRID for cross-engine flow.

- **Bug 27 — VStream POINT bytes mis-parsed (4-byte SRID prefix).** `decodeVStreamCell` in `internal/engines/mysql/cdc_vstream.go` splits `query.Type_GEOMETRY` from the binary fall-through and strips the 4-byte little-endian SRID prefix; under-5-byte payloads pass through as-is. Vanilla MySQL protocol path unaffected (it strips the prefix correctly already). The `psverify`-build-tag end-to-end verification against a real PlanetScale source remains operator-run; the unit-test fixture (`TestDecodeVStreamCellGeometry` with 3 sub-cases — SRID 4326, malformed short, SRID 0) proves the byte-level fix.

### Added

- **ADR-0035 — PostGIS-aware GEOMETRY/SPATIAL support.** Decision rationale, EWKB ↔ MySQL-prefix conversion detail, 5-scenario threat model (PostGIS-absent target, SRID mismatch, VStream prefix on vanilla MySQL connections, EPSG SRID unrecognized, MySQL → PG cross-engine SRID drift), explanation of why this is parented under roadmap item 6 (cross-engine focus) rather than item 11 (PG → PG extension passthrough — the PostGIS PG-to-PG case follows naturally now that the writer emit path lands).

- **`postgis/postgis:16-3.4` pre-pulled in CI's Integration job.** `.github/workflows/ci.yml` adds the image to the pre-pull list and runs `go test -tags="integration postgis" -timeout=15m ./internal/pipeline/...` as a separate Integration step. The image-pull cost (~500 MB layer) is one-time per cache; subsequent runs hit the warm cache.

- **`integration postgis` build tag** for cross-engine geometry round-trip integration tests. New tests under `migrate_postgis_integration_test.go`: `TestMigrate_PostGIS_PGToMySQL` (Phase C reverse direction; closes Bug 26's load-bearing pin); existing `TestMigrate_PostGIS_MySQLToPG` (already shipped) still covers the forward direction.

### Changed

- **`unsupportablePGtoMySQL` in `internal/pipeline/cross_engine_supportable.go`** no longer refuses `ir.Geometry`; the refusal narrows to `ir.ExtensionType` (the v0.26.0 PG extension passthrough framework's IR variant). Cross-engine PG → MySQL geometry now works as long as both source and target have their respective spatial subsystems (PostGIS on PG, MySQL spatial types).

- **Roadmap item 6 (GEOMETRY/SPATIAL)** moved from "Next up" to "Recently landed" with one-line summary. The MySQL/PostgreSQL parity tracker subsection now shows GEOMETRY/SPATIAL as "PG-to-PG + cross-engine v0.28.0; VStream Phase B v0.28.0 (operator-run via psverify)".

### Migration / Compatibility

- **No format-breaking changes.** Manifest schema, change-chunk format, CLI surface — all unchanged for existing flows.
- **Default behavior changed for PG targets WITH PostGIS installed.** Previously the schema writer refused all `ir.Geometry` columns with `GEOMETRY requires PostGIS; not supported in this writer version`. Post-v0.28.0, the writer emits `geometry(<subtype>, <srid>)` for those columns. Operators with PostGIS-bearing PG targets who relied on the loud-failure to bail on geometry columns should review whether they want the new automatic emission (they probably do — that's the load-bearing fix) or set `--type-override` on those columns to `bytea` for the previous opaque-bytes shape.
- **Default behavior unchanged for PG targets WITHOUT PostGIS.** The loud refusal is preserved.
- **Default behavior unchanged for MySQL targets** — already accepted geometry columns; v0.28.0 just adds SRID preservation in the emit shape.
- **Drop-in upgrade from v0.27.0.** No DDL migration on `sluice_cdc_state`; no operator action required.

### Known limitations

- **VStream Phase B end-to-end verification is operator-run via `psverify`.** The unit-test fixture proves the byte-level fix; the full PlanetScale source → sluice → target round-trip needs the operator's PlanetScale credentials (per `feedback_planetscale_creds.md`). Operators using PlanetScale Vitess sources with PostGIS-shaped POINT columns should verify the round-trip manually before relying on the v0.28.0 fix in production.

- **EPSG SRID handling is "common subset" only.** PG's `spatial_ref_sys` table has thousands of SRIDs; MySQL has hard-coded mappings for a smaller subset. Sluice doesn't enforce SRID-existence checks at translation time; an unrecognized SRID will surface as a target-side error at INSERT time (the loud-failure tenet). v1 doesn't enumerate the supported SRID set — operators using non-EPSG-common SRIDs should test their workload before committing.

- **PG-to-PG PostGIS passthrough lands by side-effect, not by explicit framework integration.** Roadmap item 11 (PG extension passthrough framework) shipped pgvector as the first concrete extension in v0.26.0; PostGIS would naturally fit as a catalog entry there. v0.28.0 ships PostGIS via the cross-engine path under item 6 instead — both PG-to-PG passthrough AND cross-engine PG ↔ MySQL work, but the explicit `--enable-pg-extension postgis` flag isn't yet wired (the v0.26.0 framework will fold PostGIS in as a future catalog entry, parallel to pg_trgm / hstore / citext).

## [0.27.0]

**MySQL Phase 2 mid-stream live add-table** (parity for v0.24.0's PG-only `--no-drain`). Operators with high-availability MySQL workloads can now bring a new source table into an active CDC stream's scope without the `sluice sync stop --wait` drain that Phase 1 required. Different mechanism from PG (binlog auto-includes every table; the gate is in the streamer's table-filter, not in a publication): sluice persists the live-added table into a new `live_added_tables` column on `sluice_cdc_state`, and the running streamer polls the column on the same cadence as `stop_requested_at` and atomically extends `applyTableFilter`'s scope. ADR-0034 documents the design. Same operator UX as PG (`sluice schema add-table TABLE --no-drain`); same best-effort caveat (events on the new table during the bulk-copy + filter-flip window may not be delivered — operators with high write rates should use the drained flow or quiesce briefly).

### Added

- **MySQL `--no-drain` support on `sluice schema add-table`.** Same flag, same UX as PG (v0.24.0). Orchestrator dispatches by source engine: PG → existing publication-add path; MySQL → new filter-flip path. Mixed-engine refusals stay clean with operator-actionable messages.

- **`live_added_tables TEXT NULL` column on `sluice_cdc_state` (MySQL).** Idempotent migration (mirrors v0.24.0's `slot_name`, v0.25.0's `source_dsn_fingerprint`, v0.25.1's `target_schema`). Comma-separated list of newly-added table names; the streamer polls and merges on each tick.

- **`tableFilterFlipper` optional engine surface.** PG doesn't implement (uses publication scope instead); MySQL implements via `RecordLiveAddedTable` / `ReadLiveAddedTables` on `ChangeApplier`. Discovered structurally so the orchestrator stays engine-neutral.

- **Streamer `liveAddedFilter` (atomic.Pointer-backed).** New `streamer_filter_flip.go` plumbs the polled-from-cdc-state additions into `applyTableFilter`'s scope mid-run. The filter is additive: existing operator-supplied `--include-table` / `--exclude-table` rules continue to apply; the live-added table joins the include list at the next poll tick.

- **ADR-0034 — MySQL Phase 2 mid-stream live add-table.** Design rationale (filter-flip vs accept-no-filter), threat model (4 scenarios), best-effort caveat documentation, parity with PG `--no-drain` operator UX.

### Changed

- **Roadmap item 4 (MySQL Phase 2)** moved from "Next up" to "Recently landed" with one-line summary. Items 5-14 renumbered down by 1. The `MySQL & PlanetScale parity tracker` subsection updated: mid-stream live add-table now shows "Both engines, v0.27.0".

- **ADR-0030 (PG Phase 2)** cross-references ADR-0034 in its `MySQL deferred (resolved)` section. The "deferred" caveat from v0.24.0 is now closed.

### Migration / Compatibility

- **No format-breaking changes.** Manifest schema, change-chunk format, CLI surface — all unchanged for existing flows.
- **Default behavior unchanged.** Operators not using `--no-drain` see no behaviour change — the v0.24.0 + earlier add-table flow continues to require a drained stream.
- **Drop-in upgrade from v0.26.0.** No DDL migration on `sluice_cdc_state`; the new `live_added_tables` column lands on first `EnsureControlTable` call.
- **PG operators unaffected.** PG `--no-drain` continues to use the publication-add mechanism from ADR-0030; the MySQL filter-flip path is engine-gated.
- **Existing `--include-table` / `--exclude-table` semantics preserved.** Live-added tables are additive to the operator-supplied filter; explicit exclusions still apply.

### Known limitations

- **Best-effort during the filter-flip window** (parallel to PG Phase 2's documented gap, ADR-0030 item 3). Events on the new table that arrive between the bulk-copy snapshot's binlog position and the streamer's filter-flip observation (~5s poll interval) may not be delivered. Under-load test observed ~3 events lost out of 59 in CI's worst-case sustained-INSERT scenario. Operators with high write rates on the new table at the moment of live-add should use the drained add-table flow (zero-loss by construction) or quiesce writes for the seconds-long window. The strict-correctness mechanism (ADR-0033) is open for both engines pending further design work.

- **Filter-flip poll cadence is 5 seconds** (matches the existing `stop_requested_at` poll). A future refinement could shorten the cadence or add a notification mechanism (LISTEN/NOTIFY on PG, but MySQL has no equivalent — would need a polling-rate trade-off).

- **VStream / PlanetScale not in scope** for Phase 2. Different binlog source surface; Phase 2.5 follow-on if real demand surfaces.

## [0.26.0]

**PG → PG extension passthrough framework + pgvector.** Sluice's IR has been engine-neutral by design — column types are categorized by core SQL kinds, and PG's extensible type system has been treated as hostile (loud-failure refusal at schema-read time). v0.26.0 lands the framework that flips this for same-engine PG → PG syncs where the operator opts in: `--enable-pg-extension EXT` (repeatable) tells sluice to recognize and round-trip column types defined by named extensions, with native fidelity. Cross-engine targets (PG → MySQL) keep the loud-failure default; explicit operator translations (`--type-override`) stay the escape hatch. v1 ships with **pgvector** as the first concrete extension, exercising both the type-only path (Tier 1 mechanics) and the index-method path (Tier 2 mechanics — `ivfflat` and `hnsw`). Subsequent extensions from the v1 shortlist (pg_trgm, hstore, citext, PostGIS) ship as catalog-only follow-ons; the framework stays put. Implementation supplement: `docs/adr/adr-0032-pg-extension-passthrough.md`. Decision input: `docs/research/pg-extensions-deployment-frequency.md` (the survey that pinned the v1 shortlist).

### Added

- **`--enable-pg-extension EXT` flag (repeatable) on `migrate` / `sync start` / `schema preview` / `schema diff`.** Default empty (today's behavior — extension types refuse loudly at schema-read). When set, sluice validates each name against the recognized-extensions catalog (refuses unknown names at flag-parse with the recognized set in the error), then preflights against the source DB (`SELECT extname FROM pg_extension WHERE extname = ANY(...)`) to ensure the extension is actually installed. Same preflight runs on the target. Same-engine PG → PG only — cross-engine targets refuse with operator-actionable error pointing at `--type-override`.

- **`ir.ExtensionType` IR variant.** Engine-neutral by name (Extension + Name); modifiers carry per-type metadata (e.g. `vector(384)` → Modifiers=[]int{384}). Schema reader emits this variant when the column's PG type OID matches a catalog entry; schema writer renders it via the catalog's `emitCol` function on same-engine targets, refuses on cross-engine.

- **`ir.ExtensionAware` optional engine surface.** `EnableExtensions(ctx, names)` activates allowlisted extensions on the engine. PG implements; MySQL does not (no extension concept in the same shape — MySQL's "feature flags" are server-level, not type-defining). The structural type-assertion skips cleanly on engines that don't implement.

- **`ir.Index.Method` field.** Carries verbatim extension-introduced index access-method names (`ivfflat`, `hnsw` for pgvector; `gin` / `gist` for pg_trgm / PostGIS in the future). Bareword fallback for `IndexKindUnspecified` preserves the existing engine-neutral `IndexKind` enum while letting catalog entries register their own access methods without expanding the enum.

- **`ir.IndexColumn.OperatorClass` field + emission.** Captured by a `pg_index/pg_opclass` join in `populateIndexes`, emitted by `emitIndexColumnList` for indexes whose access method is extension-introduced and requires it (e.g. `hnsw` requires `vector_l2_ops` / `vector_ip_ops` / `vector_cosine_ops` / `vector_l1_ops`). Default-PG indexes (B-tree, hash, GiST, GIN with built-in opclasses) emit unchanged.

- **pgvector binary COPY codec (`internal/engines/postgres/pgvector_codec.go`).** Bulk-copy uses pgvector's binary wire format (`int16 dim, int16 unused, dim × BE float32` per pgvector/src/vector.c). The naive text-passthrough approach fails because pgx's binary COPY protocol parses the first two bytes of the value as a dimension count — text representation `[0.1,0.2...` would be interpreted as a 23344-dimension vector and trip pgvector's 16000-dim ceiling. The codec is registered per-connection in `writeViaCopy` when the table has any vector column; the OID is resolved from `pg_type` at registration time.

- **PG extension catalog (`internal/engines/postgres/extension_catalog.go`).** Registry mapping extension name → recognized type OIDs + emit functions + index access methods. Adding a new extension is "add a catalog entry," not "extend interfaces." pgvector ships as the first entry; catalog stub entries for pg_trgm / hstore / citext / PostGIS follow in subsequent point releases per the v1 shortlist (pinned by `docs/research/pg-extensions-deployment-frequency.md`).

- **ADR-0032 — PG extension passthrough.** Decision rationale (allowlist over auto-detect), three-tier classification framework (Tier 1 type-only / Tier 2 type+index / Tier 3 type+functions), threat model (5 scenarios — target missing extension, version skew, cross-engine refusal, operator typo, no-columns no-op), why pgvector first.

### Migration / Compatibility

- **No format-breaking changes.** Manifest schema, change-chunk format, CLI surface — all unchanged for existing flows.
- **Default behavior unchanged.** Operators not using `--enable-pg-extension` see no behaviour change — extension column types continue to refuse loudly at schema-read (the existing pattern).
- **Drop-in upgrade from v0.25.1.** No DDL migration on `sluice_cdc_state`; no operator action required.
- **MySQL operators unaffected.** MySQL doesn't implement `ir.ExtensionAware`; the structural type-assertion skips cleanly. Cross-engine PG → MySQL with `--enable-pg-extension` enabled still refuses cleanly at the cross-engine retarget step (`--type-override` remains the operator escape hatch).

### Known limitations

- **Extension version skew not detected.** v1 checks extension presence on both source and target, NOT version compatibility (pgvector 0.7 source → 0.5 target may surface subtle behaviour gaps that sluice doesn't see). Documented in ADR-0032's threat model item 2; future refinement could add `--enable-pg-extension vector@>=0.7` syntax if real operator demand surfaces.
- **Operator-class emission scoped to extension AMs.** `hnsw` indexes correctly emit their required operator class (`vector_l2_ops` etc.); built-in PG access methods (B-tree, hash, GIN, GiST with built-in opclasses) emit unchanged. If a future extension requires custom operator classes that aren't in pgvector's recognized set, the catalog needs an entry update.
- **Tier 3 extensions deferred.** uuid-ossp + pgcrypto are universal across all four surveyed providers (Supabase, Neon, PlanetScale Postgres, ps-extensions.io) but are Tier 3 (function-in-defaults expression-translator work). Strong v2 candidates after the v1 Tier 1+2 machinery is in place. Tracked in ADR-0032 §"Consequences."

## [0.25.1]

Two-bug patch from the v0.25.0 cycle. Both bugs were introduced by v0.25.0's `--target-schema` flag and surfaced in the load-bearing happy-path scenario + the v0.24.0 live-add-table interaction.

### Fixed

- **Bug 45 — `--target-schema=NAME` against a PG source with enum-typed columns failed at CREATE TABLE with `ERROR: type "<table>_<column>_enum" does not exist (SQLSTATE 42704)`.** The PG schema writer schema-qualified the `CREATE TYPE` statement correctly (`CREATE TYPE "customer_svc"."orders_status_enum" AS ENUM (...)`), but the column-type ident inside `CREATE TABLE` and the `::cast` in column DEFAULT expressions were emitted unqualified — PG's parser with default `search_path` couldn't find the unqualified type and bailed. Fix: new `qualifiedEnumTypeRef` helper in PG ddl_emit; `emitColumnDef` qualifies enum column-type idents and the `::cast` suffix on DEFAULT expressions when TargetSchema is non-empty. Default-public operators (no `--target-schema`) see no behaviour change — the new `schemaExplicit` flag on the SchemaWriter only triggers qualifying emission when `SetSchema` was called from operator override.

- **Bug 46 — `schema add-table --no-drain` against a stream started with `--target-schema=NAME` silently dropped CDC events on the new table.** The new table was created in `public.<table>` while the active stream's CDC applier (still running with `--target-schema=NAME`) routed new-table CDC events to `<NAME>.<table>` — which didn't exist. Events emitted a single WARN and silently dropped. Two-part fix: (a) added `--target-schema=NAME` flag to `schema add-table` (mirroring `migrate` / `sync start` / `schema preview`); (b) persist `target_schema` to `sluice_cdc_state` at sync start via the new `targetSchemaSetter` engine surface (PG implements; MySQL doesn't — same shape as v0.24.0's slot_name plumbing); (c) `AddTable.preflightStream` resolves the target schema from operator-supplied flag → recorded cdc-state value → (legacy) empty, with a 5-case resolution table covering inherit-from-recorded, operator-override, mismatch-refusal, agreement, and legacy-row-back-fill behaviors.

### Added

- **`--target-schema=NAME` flag on `sluice schema add-table`.** When non-empty, both bulk-copy DDL and any subsequent CDC events on the new table land in the named PG schema. Auto-inherits the active stream's recorded value when omitted; refuses with a clear error when the operator-supplied value disagrees with the recorded value (the latter close ADR-0031's previously-documented "mid-flight `--target-schema` change is NOT detected" caveat).
- **`target_schema TEXT NULL` column on `sluice_cdc_state` (PG).** Idempotent migration via `ADD COLUMN IF NOT EXISTS` (mirrors v0.24.0's `slot_name` and v0.25.0's `source_dsn_fingerprint`). Streamer records the resolved target schema on every position-write via the new `targetSchemaSetter` interface; `ListStreams` returns it as `StreamStatus.TargetSchema`.

### Changed

- **ADR-0031 threat-model entry 5** (mid-flight `--target-schema` change on warm-resume not detected) moved from "deferred until a real operator surfaces the gap" to **"closed in v0.25.1"** via the recorded `target_schema` column + add-table mismatch refusal. A future warm-resume `sync start` with a different `--target-schema` against an existing stream-id refuses the same way as add-table's mismatch path; the operator either matches the recorded value or runs `--reset-target-data`.

### Migration / Compatibility

- **No format-breaking changes.** Manifest schema, change-chunk format, CLI surface — all unchanged for existing flows. The new `target_schema` column on `sluice_cdc_state` is additive (idempotent `ADD COLUMN IF NOT EXISTS`); legacy rows surface as empty TargetSchema via `COALESCE` and skip the resolution check (preserves backward-compat).
- **Drop-in upgrade from v0.25.0.** No DDL migration needed; the new column lands on first `EnsureControlTable` call.
- **Default behavior unchanged.** Operators not using `--target-schema` see no emission style changes — the `schemaExplicit` flag on the SchemaWriter only triggers qualifying emission when `SetSchema` was called from operator override.

## [0.25.0]

Multi-source aggregation Phase 1 + Phase 2: **`--target-schema` (PG-only) + stream-id collision detection.** Operators with N source databases landing in one target Postgres can now namespace each source's tables into its own schema (`customer_svc.users`, `billing_svc.users`) with a single CLI flag — N independent `sluice sync start` processes, one per source, each with its own `--target-schema NAME` + `--stream-id`. The `sluice_cdc_state` control table picks up a `source_dsn_fingerprint` column and refuses on stream-id collision (operator typo'd `--stream-id`; would silently overwrite another stream's position). ADR-0031 formalises the design (Shape B per `docs/dev/design-multi-source-aggregation.md`); Shape A (sharded → consolidated) is queued as a long-term roadmap entry, and MySQL native parity is the documented follow-up (today MySQL operators get equivalent coverage via `--target` DSN choice).

### Added

- **`--target-schema NAME` flag on `migrate`, `sync start`, `schema preview`, `schema diff`.** Default empty (use the target DSN's default schema, today's behavior). When set, every emitted CREATE TABLE / ALTER TABLE prefixes the table reference with the schema name; PG enums get schema-namespaced (`customer_svc.accounts_status_enum`) so two sources with same-named tables don't collide on type names. PG schema reader / writer / row reader / row writer / change applier all thread the schema through via the new optional `ir.SchemaSetter` surface. Schema is auto-created on first emit via `CREATE SCHEMA IF NOT EXISTS`.

- **MySQL `--target-schema` refusal.** MySQL has no schema concept distinct from databases; the flag refuses cleanly at validate time with an operator-actionable message directing them at the `--target` DSN-choice pattern (different MySQL databases on the same server). Pinned via test that asserts MySQL doesn't implement `ir.SchemaSetter`.

- **Stream-id collision detection.** `sluice_cdc_state` gains a `source_dsn_fingerprint TEXT NULL` column (idempotent migration). The streamer records a SHA-256-truncated fingerprint of the normalized source DSN (host + port + database; user/password excluded so password rotation doesn't break collision detection) on every position-write. On `sync start`, sluice queries the existing fingerprint for the stream-id; if it differs from the new source's fingerprint, refuses with `stream "X" exists on target with a different source DSN — pick a different --stream-id or --reset-target-data to wipe and start fresh`. Catches the operator-typo case where two streams accidentally share a stream-id and would silently overwrite each other's position.

- **ADR-0031 — Multi-source aggregation: --target-schema + stream-id collision detection.** Decision rationale (Shape B + N-processes + PG-only first), threat model with 5 scenarios, type-name derivation (PG enums namespaced through the schema), and impl summary. References the proto-ADR at `docs/dev/design-multi-source-aggregation.md`.

### Migration / Compatibility

- **No format-breaking changes.** Manifest schema, change-chunk format, CLI surface — all unchanged for existing flows. The new `source_dsn_fingerprint` column on `sluice_cdc_state` is additive (idempotent `ADD COLUMN IF NOT EXISTS`); legacy rows surface as empty fingerprint via `COALESCE` and skip the collision check (preserves backward-compat).
- **Drop-in upgrade from v0.24.0.** No DDL migration needed; the new column lands on first `EnsureControlTable` call.
- **Default behavior unchanged.** Without `--target-schema`, every existing migrate / sync-start invocation lands tables in the target DSN's default schema exactly as before.
- **MySQL operators unaffected** — `--target-schema` refuses cleanly with the DSN-choice-workaround error message. MySQL native parity (per-table-rename mechanism) is a future chunk if real demand surfaces.

### Known limitations

- **Mid-flight `--target-schema` change on warm-resume not detected.** If the operator changes the `--target-schema` value between `sync start` invocations, sluice doesn't refuse — both schemas would receive the stream's new writes. Documented in ADR-0031's threat model (item 5) as a known caveat; same-shape future refinement could add `target_schema TEXT` to `sluice_cdc_state` and refuse on mismatch.

## [0.24.0]

Mid-stream live add-table Phase 2: **`sluice schema add-table TABLE --no-drain`** for PG sources. Operators with high-availability workloads no longer need the `sluice sync stop --wait` drain that Phase 1 required to bring a new source table into an active CDC stream's scope. ADR-0030 formalises the design (Strategy C variant c per `docs/dev/design-mid-stream-add-table.md`); the heavy lifting was already in Phase 1 (publication-add-then-snapshot ordering, idempotent applier overlap handling) — Phase 2 lifts the conservative active-stream refusal, adds an explicit LSN-floor invariant check, and plumbs the active stream's slot name through the per-target `sluice_cdc_state` control table so live-add picks the right slot when the operator uses `--slot-name`. PG-only in this release; MySQL Phase 2 has a meaningfully different design space (table-filter flip vs publication scope) and is queued as a separate chunk.

### Added

- **`--no-drain` flag on `sluice schema add-table`.** Default off (preserves Phase 1's drained-stream refusal as the conservative default). When set, the orchestrator captures the active stream's slot `confirmed_flush_lsn`, runs `ALTER PUBLICATION ... ADD TABLE`, opens a temp-slot snapshot at LSN ≥ confirmed_flush_lsn, bulk-copies, and verifies the snapshot-LSN ≥ slot-LSN invariant. CDC keeps streaming throughout — no stream restart needed.
- **Slot-name plumbing through `sluice_cdc_state`.** New `slot_name TEXT NULL` column (idempotent migration). The streamer records its resolved slot name on every position-write via the `SetSlotName` applier hook (Phase 1.5 follow-up shipped in the same release). Operators running multiple concurrent streams against the same source via `--slot-name=shard_a` get the right slot's confirmed_flush_lsn queried automatically by live add-table.
- **New optional engine surfaces** (in `internal/pipeline/add_table.go`): `slotPositionReader`, `snapshotLSNExtractor`, `lsnComparer`, `slotNameSetter`. PG implements all four; MySQL implements none (no slot concept); the structural type-assertions skip cleanly. The orchestrator stays engine-neutral.
- **ADR-0030 — Mid-stream live add-table.** Formalises the correctness story (pgoutput evaluates publication membership at decode time; snapshot-after-publication-add ordering + idempotent applier covers all rows on the new table exactly-once-effectively for the load-bearing cases), threat model (six hazards, all mitigated), why Strategy B (dual-slot) was deferred, and why MySQL's Phase 2 is a separate chunk.

### Known limitations

- **Best-effort for in-flight inserts during the publication-add window.** Discovered during v0.24.0's CI under-load testing: events on the new table inserted DURING the brief publication-add window may not be delivered (~1–3 events lost per sub-second window in the worst case). pgoutput evaluates publication membership per WAL record at decode time; events the slot decoded-and-filtered BEFORE publication-add commit took effect are gone. **Snapshot rows + post-publication-add events are delivered exactly-once-effectively** (proven by `TestAddTable_LiveMode_PG` happy-path test + the post-add sentinel pin in `TestAddTable_LiveMode_PG_UnderLoad`). Operators with high write rates on the new table at the moment of live-add should use the drained add-table flow (zero-loss by construction) or quiesce writes for the seconds-long window. Strict-correctness fix queued as a follow-up roadmap entry (Path A slot-pause / Path B Strategy B dual-slot). ADR-0030's "What could go wrong" section documents the gap in full.

### Migration / Compatibility

- **No format-breaking changes.** Manifest schema, change-chunk format, CLI surface — all unchanged for existing flows. The new `slot_name` column on `sluice_cdc_state` is additive (idempotent `ADD COLUMN IF NOT EXISTS`); legacy rows surface as empty `SlotName` via `COALESCE` and fall back to the engine default `sluice_slot` for live-add lookup.
- **No CLI breaking changes.** `--no-drain` is opt-in; existing `sluice schema add-table` invocations without it continue to require a drained stream identically.
- **Drop-in upgrade from v0.23.2.** No DDL migration needed; the new column lands on first `EnsureControlTable` call.
- **MySQL operators unaffected** — `--no-drain` refuses cleanly with a PG-only error directing them at the drained-stream flow.

## [0.23.2]

Single-bug patch: closes Bug 44 — same-engine MySQL → MySQL migrate of any column with `DEFAULT (UUID())` or `DEFAULT (RAND())` failed at CREATE TABLE with MySQL Error 1064. The MySQL writer's `emitDefault` was emitting `DEFAULT uuid()` (without outer parens) because MySQL's INFORMATION_SCHEMA returns the default expression with parens stripped (`column_default = 'uuid()'`). MySQL 8.0.13+ requires `DEFAULT (uuid())` for function-call expression defaults — only special temporal keywords (CURRENT_TIMESTAMP family, NOW(), LOCALTIME, etc.) are accepted bare. Symmetric writer-side counterpart to v0.11.3's Bug 28/29 fix (which were reader-side translation gaps for the cross-engine direction); together with v0.23.1's Bug 42 fix and v0.21.2's Bug 41 fix, all known UUID-default migration paths (PG → MySQL cross-engine, MySQL → MySQL same-engine, MySQL → PG cross-engine, PG → PG same-engine) now work end-to-end.

### Fixed

- **Bug 44 — MySQL → MySQL same-engine migrate of columns with `(UUID())` / `(RAND())` expression defaults fails with Error 1064.** Pre-existing on v0.23.1 and earlier — surfaced by the v0.23.1 cycle when it exercised the same-engine MySQL → MySQL Scenario 4 path for the first time. **Not introduced by v0.23.1**; the v0.23.1 translator entries (`gen_random_uuid()` / `random()`) only match PG-canonical names that never appear in MySQL's IR. Root cause: MySQL's INFORMATION_SCHEMA stores `DEFAULT (UUID())` as `column_default = 'uuid()'` (parens stripped, lowercased); the MySQL schema reader stores this in the IR as `ir.DefaultExpression{Expr: "uuid()", Dialect: "mysql"}`; the MySQL writer's `emitDefault` then emitted `DEFAULT uuid()` verbatim. MySQL 8.0+ rejects this because the expression-default grammar treats function calls and the temporal-keyword family as separate productions — function calls require outer parens. Fix: a new `wrapMySQLExpressionDefault` helper runs after the existing `pgToMySQLDefaultExpr` lookup and `matchTimestampDefaultPrecision` pass. It detects three cases: (a) already outer-wrapped (`(UUID())` from Bug 42's translation, or operator-supplied `(coalesce(x, 0))`) — pass through; (b) bare temporal keyword (`CURRENT_TIMESTAMP[(N)]`, `LOCALTIME[(N)]`, `LOCALTIMESTAMP[(N)]`, `NOW[()]`, `CURRENT_DATE[()]`, `CURRENT_TIME[(N)]`) — pass through bare (wrapping these is itself a syntax error); (c) anything else (function-call shape) — wrap in outer parens. The bare-keyword detector strips a trailing precision suffix (`(N)` or empty `()`) before matching the case-insensitive keyword set, so MySQL's various capitalisations and the parens-vs-no-parens synonyms (`CURRENT_TIMESTAMP` vs `CURRENT_TIMESTAMP()`) are all recognised.

### Migration / Compatibility

- **No format changes.** Manifest schema, control-table schema, change-chunk format, CLI surface — all unchanged.
- **No CLI breaking changes.** All existing `sluice` subcommands keep their flag surfaces verbatim.
- **Drop-in upgrade from v0.23.1.** No DDL migration on `sluice_cdc_state`; no operator action required.
- **MySQL 8.0+ baseline preserved.** The wrap-in-outer-parens emission is exactly what MySQL 8.0.13+ requires for function-call expression defaults; the project already declares MySQL 8.0+ as the supported baseline.
- **No behaviour change for existing PG → MySQL cross-engine paths.** The Bug 42 translation entries (`gen_random_uuid() → (UUID())`, `random() → (RAND())`) already emit pre-wrapped expressions; the wrap helper is a no-op on those. PG → PG and same-engine PG paths are not touched (this is in the MySQL writer).
- **Existing temporal-default behaviour preserved.** The `matchTimestampDefaultPrecision` precision-promotion path runs before the wrap helper; bare `CURRENT_TIMESTAMP` on a TIMESTAMP(6) column still emits `CURRENT_TIMESTAMP(6)` (not `(CURRENT_TIMESTAMP(6))`) — matching the bare-keyword passthrough rule.

## [0.23.1]

Single-bug patch: closes Bug 42 — cross-engine PG → MySQL restore of a column with `DEFAULT gen_random_uuid()` failed at CREATE TABLE with MySQL Error 1064 (PG's UUID-generator function name lands verbatim in the MySQL DDL). Symmetric reverse of v0.11.3's Bugs 28/29 fix (which translated MySQL's `UUID()` / `RAND()` → PG's `gen_random_uuid()` / `random()`); the MySQL-side default-translator catalog now covers the opposite direction. Together with v0.21.2's Bug 41 (CDC value-decode for UUID columns), this completes "first-class UUID support in cross-engine restore" — Bug 41 fixed the CDC value-decode side; Bug 42 fixes the schema-side default-translation gap. Common pattern in modern PG schemas (Rails, Django, Hasura, Supabase all default-emit `gen_random_uuid()` for UUID PKs); pre-fix, those tables couldn't be migrated to MySQL without operator-side schema munging.

### Fixed

- **Bug 42 — cross-engine PG → MySQL restore of `DEFAULT gen_random_uuid()` columns fails with MySQL Error 1064.** `internal/translate.RetargetForEngine` correctly rewrote `Column.Type` (UUID → CHAR(36)) but didn't rewrite `Column.DefaultValue` of kind `DefaultExpression`. The PG-flavored expression `gen_random_uuid()` flowed through to the MySQL writer's `emitDefault`, where the `pgToMySQLDefaultExpr` translation table didn't have an entry for it, so it fell through to verbatim emission. The resulting CREATE TABLE statement (`uuid_col CHAR(36) DEFAULT gen_random_uuid()`) was rejected by MySQL — `gen_random_uuid` doesn't exist there. Fix: extend `pgToMySQLDefaultExpr` with `gen_random_uuid()` → `(UUID())` (MySQL's canonical UUID-generator function, wrapped in the outer parens MySQL 8.0+ requires for function-call expression defaults). Both PG's `gen_random_uuid()::text` and MySQL's `UUID()` return canonical hyphenated 36-char form, so the column's stored values are semantically equivalent. Also added the symmetric reverse of v0.11.3's Bug 29 fix: `random()` → `(RAND())` (same root cause; both functions return `[0, 1)` doubles). The MySQL writer's existing `emitDefault` precision-matching path is preserved; UUID and RAND defaults don't carry temporal precision so the matchTimestamp branch is a no-op for them.

### Migration / Compatibility

- **No format changes.** Manifest schema, control-table schema, change-chunk format, CLI surface — all unchanged.
- **No CLI breaking changes.** All existing `sluice` subcommands keep their flag surfaces verbatim.
- **Drop-in upgrade from v0.23.0.** No DDL migration on `sluice_cdc_state`; no operator action required.
- **MySQL 8.0+ baseline preserved.** `DEFAULT (UUID())` and `DEFAULT (RAND())` require MySQL 8.0.13+ for the function-call-as-default expression-default syntax; the project already declares MySQL 8.0+ as the supported baseline.
- **No behaviour change for same-engine paths.** PG → PG and MySQL → MySQL migrations of UUID-bearing tables are unaffected; the new translation only fires when the IR's expression default carries the canonical PG function name and the target engine is MySQL.

## [0.23.0]

Logical backups Phase 6.2 lands: **AWS KMS-backed envelope encryption.** Operators who already manage encryption keys via AWS KMS (the common compliance posture for HIPAA / PCI / SOC 2 shops, and the common BYOK posture for multi-tenant SaaS) can now hand sluice a key ARN and skip the passphrase plumbing entirely. The manifest's per-chain CEK is wrapped via `kms.Encrypt`; restore unwraps via `kms.Decrypt` once at the start and caches the CEK in-memory for the rest of the chain. Phase 6.1 (passphrase mode) keeps working unchanged; the two modes are mutually exclusive per backup but pluggable behind the same `EnvelopeEncryption` interface. Implementation supplement: `docs/dev/design-logical-backups-phase-6.md`. Operator guide: `docs/operator/encryption.md` ("AWS KMS setup" section).

### Added

- **`--kms-key-arn` + `--kms-region` on backup full / incremental / stream / restore / sync from-backup.** Operator passes a KMS key ARN (or alias ARN, or alias name); sluice loads the default AWS config (env vars, IAM role, profile, SSO), pre-flights the key with a `DescribeKey` call (auth/region/key-not-found errors surface at construction time, not mid-backup), then wraps every chain's CEK via `kms.Encrypt`. Restore mirrors the path: build the envelope, unwrap once, decrypt every chunk in the chain with the cached CEK. Per-chain CEK caching is the load-bearing performance choice — a 100-chunk restore makes ≤1 KMS Decrypt call regardless of chunk count, so KMS API charges stay flat against chain length.

- **`internal/crypto.KMSEnvelope` implementing `EnvelopeEncryption`.** Drops in alongside Phase 6.1's `PassphraseEnvelope` behind the same interface; the chunk writer/reader paths don't change. Manifest schema is unchanged: `ChainEncryption.KEKMode = "aws-kms"`, `KEKRef = <arn>`, `Argon2id` omitted (KMS doesn't use it), `WrappedCEK` is the KMS CiphertextBlob.

- **Operator-actionable KMS error translation.** `AccessDeniedException` surfaces as "AWS IAM principal lacks kms:Encrypt/Decrypt/DescribeKey on the key (verify key policy + role policy grants the action)"; `NotFoundException` as "key not found (verify the ARN/alias)"; `KMSInvalidStateException` / `DisabledException` as state-specific recovery hints; `IncorrectKeyException` as "ciphertext was wrapped under a different key" (the wrong-key-on-restore path). Generic SDK errors fall through with the key ARN preserved for support correlation.

- **AWS KMS setup section in `docs/operator/encryption.md`.** Covers IAM policy template (least-privilege grant of kms:Encrypt + kms:Decrypt + kms:DescribeKey on a specific key ARN), key creation (CLI + console), key alias usage, and key-rotation handling (KMS rotates the root key transparently; wrapped CEKs reference the key ID — old chains stay decryptable).

### Changed

- **`EncryptionFlags.buildBackupEncryption` + `buildReadEnvelope` route through a single key-source validator.** `--encrypt` now requires exactly one of the passphrase-flag family OR `--kms-key-arn`; mixing them errors with "mutually exclusive" before any envelope work happens. Sets `pipeline.BackupEncryption.KEKRef` from the operator-supplied ARN so the manifest records what the operator passed verbatim.

### Migration / Compatibility

- **No format changes.** Manifest schema, control-table schema, change-chunk format, CLI surface — all unchanged. v0.22.x chains taken under passphrase mode restore unchanged under v0.23.0. KMS-mode chains are taken with v0.23.0+ binaries; pre-v0.23.0 binaries refuse them at preflight (the chain root's `KEKMode = "aws-kms"` doesn't match a `PassphraseEnvelope.Mode()`).

- **No CLI breaking changes.** All existing `sluice` subcommands keep their flag surfaces verbatim. The new flags are additive.

- **AWS SDK pulled in via `github.com/aws/aws-sdk-go-v2/service/kms`.** Already an indirect dependency for the S3 backup target; v0.23.0 promotes it to direct. Build size change is negligible (KMS service module is ~200KB compiled).

### Test coverage

- Unit tests for the KMS envelope: round-trip, wrong-key, missing-key, access-denied, disabled-key, invalid-state, generic SDK error fall-through, length-validation, per-chain caching pattern (100-chunk read = 1 Decrypt call).
- Pipeline-level integration: end-to-end manifest stamping (KEKMode/KEKRef/no-Argon2id), restore preflight, mode-mismatch refusal, per-chain caching across the chunk-CEK resolver.
- CLI flag-level tests: KMS-vs-passphrase mutual exclusion, KMS-without-encrypt sanity check, encrypt-without-any-key shape.
- A `kmsverify` build-tag harness skeleton sits in `internal/pipeline/backup_kms_localstack_integration_test.go` for operator-run localstack verification; the main `integration` build tag stays focused on real-database scenarios so CI throughput doesn't regress on the localstack pull/boot cost.

## [0.22.1]

Single-bug patch from the v0.22.0 cycle. v0.22.0's load-bearing encryption pieces (encrypted full backup + restore, wrong-passphrase / missing-key refusals, per-chunk mode, plaintext backward-compat) all shipped clean — but the write-side envelope builder minted a fresh Argon2id salt every call, so any **chain extension** of an encrypted chain (`backup incremental --encrypt`, `backup stream run --encrypt`, or resume of a partial encrypted full) crashed at startup with `aes-gcm open: cipher: message authentication failed`. Restore-side already mirrored the chain's recorded salt; this patch brings the write-side in line. Fix is local to the encryption-builder + the orchestrator's chain-alignment paths; no schema or CLI changes.

### Fixed

- **Bug 43 — encrypted-chain extension fails at startup with `aes-gcm open: cipher: message authentication failed`.** `cmd/sluice/backup.go`'s write-side `buildBackupEncryption` called `crypto.DefaultArgon2idParams()`, which mints a fresh random salt every call. For `backup full` (no parent chain) this was correct — it sets the chain's salt for the first time. For `backup incremental --encrypt`, `backup stream run --encrypt` extending an existing encrypted chain, or resuming a partial encrypted full, the resulting envelope's KEK was derived against a different salt than the chain's recorded salt, so `Envelope.UnwrapCEK(parent.WrappedCEK)` failed with auth-tag mismatch. The read side (`buildReadEnvelope`) already loaded `rootManifest.ChainEncryption.Argon2id` and re-derived the KEK against the chain's recorded salt — that's why restore worked. Fix mirrors the read-side pattern on the write side: `pipeline.BackupEncryption` gains a `RebuildForChain func(*ir.Argon2idParams) (crypto.EnvelopeEncryption, error)` hook the CLI populates with a closure over the operator's passphrase. The orchestrator's chain-alignment paths (`Backup.setupChainEncryption`, `IncrementalBackup.alignEncryption`, `BackupStream.alignEncryption`) now read the chain root's recorded `Argon2id` and call `RebuildForChain` with those params before any CEK unwrap; the rebuilt envelope's KEK derives against the chain's salt and the unwrap succeeds. Cold-start full backups are unchanged — `RebuildForChain` is a no-op when the parent chain has no recorded params, so the freshly-minted Envelope's salt becomes the chain's salt as before.

- **Latent: `Restore` discarded `Envelope` when chain shape was detected.** `Restore.Run` dispatches to `ChainRestore` for chains with incrementals, but the `ChainRestore{}` literal it built omitted the `Envelope` field — so encrypted chains restored via `sluice restore` (the public CLI surface) silently lost the operator's envelope and refused with the missing-key error. Surfaced as a follow-on while landing the Bug 43 integration test (encrypted full + encrypted incremental → chain restore). Fix is one-line; tests pin the propagation. Standalone full restores were unaffected because the chain-detection branch was skipped.

### Added

- **`pipeline.BackupEncryption.RebuildForChain` field.** Optional builder hook the orchestrator calls when extending an existing encrypted chain. Phase 6.1 (passphrase mode) populates it from the CLI; Phase 6.2/6.3 (KMS modes) leave it nil — KMS unwrap doesn't depend on a chain-recorded salt.

- **Integration coverage for encrypted chain extension.** `TestBackup_EncryptedChainExtension_Incremental_PG` runs the full + encrypted-incremental + chain-restore round-trip with two distinct cold-start salts on the writer envelopes, mirroring the CLI shape that exposed Bug 43; the rebind pulls the chain's recorded salt into the second envelope before unwrap. Companion test `TestBackup_EncryptedChainExtension_NoRebuildHook_Fails` pins the pre-fix failure mode (no `RebuildForChain` → auth-tag mismatch) so a future regression of the wiring surfaces here rather than at the next cycle's S5 attempt.

### Migration / Compatibility

- **No format changes.** Manifest schema, control-table schema, change-chunk format, CLI surface — all unchanged.
- **No CLI breaking changes.** All existing `sluice` subcommands keep their flag surfaces verbatim.
- **Drop-in upgrade from v0.22.0.** A v0.22.0 chain that took its initial encrypted full cleanly is fully extensible under v0.22.1 (the chain's recorded `Argon2id` params let v0.22.1 derive the right KEK on every subsequent extension).
- **Cold-start full backups unchanged.** `backup full --encrypt` against an empty destination mints a fresh salt as it always did; the chain root's recorded params are what subsequent extensions key off.
- **No new dependencies.** Pure orchestration plumbing within the existing `internal/crypto` + `internal/pipeline` surfaces.

## [0.22.0]

Logical backups Phase 6.1 lands: **client-side passphrase-mode encryption.** Chunks now land in cloud storage as AES-256-GCM ciphertext when `--encrypt` is set; only an operator with the right passphrase can recover the underlying rows. Closes the v0.16.0 / v0.17.2 release-notes-disclosed gap that sluice currently writes plaintext chunks; unlocks compliance-driven adoption (HIPAA, PCI-DSS, SOC 2 Type II, GDPR with customer-controlled keys) + air-gapped DR workflows where bucket-SSE doesn't follow the bytes. Implementation supplement: `docs/dev/design-logical-backups-phase-6.md`.

### Added

- **`--encrypt` + `--encryption-passphrase{,-env,-file}` + `--encrypt-mode={per-chain,per-chunk}` on backup full / incremental / stream / restore / sync from-backup.** Operator passes a passphrase; sluice derives a Key Encryption Key via Argon2id (default 64 MiB / 3 iterations / 4 parallelism, NIST-recommended starting point), generates an AES-256 Content Encryption Key, encrypts every chunk with AES-256-GCM under the CEK, wraps the CEK with the KEK, and records the wrapped CEK + Argon2id params on the chain manifest. Restore re-derives the KEK from the operator's passphrase + the recorded salt, unwraps the CEK, decrypts every chunk on the fly. `--encryption-passphrase-env` and `--encryption-passphrase-file` are recommended over inline `--encryption-passphrase` for production (the inline form shows up in shell history).

- **Per-chain CEK by default; per-chunk CEK opt-in.** Per-chain wraps a single CEK at chain root; every chunk reuses the same CEK with its own random 12-byte nonce. Argon2id (the expensive op) runs **once per restore**, not once per chunk. Per-chunk mode (`--encrypt-mode=per-chunk`) wraps a fresh CEK per chunk for defense-in-depth at the cost of per-chunk Argon2id derives during restore.

- **`internal/crypto/envelope.go` package.** New `EnvelopeEncryption` interface abstracts CEK wrap/unwrap so Phase 6.2 (AWS KMS) and Phase 6.3 (GCP Cloud KMS / Azure Key Vault) modes plug in without changing the chunk writer/reader. Phase 6.1 ships `PassphraseEnvelope` (Argon2id-derived KEK).

- **Manifest schema additions.** `Manifest.ChainEncryption` (`{algorithm, mode, kek_mode, kek_ref, wrapped_cek, argon2id}`), `ChunkInfo.Encryption` (`{algorithm, nonce_len, auth_tag_len, wrapped_cek}`). All fields use `omitempty` so pre-Phase-6 manifests round-trip bit-identically post-Phase-6 readers.

- **`sluice backup verify` runs without keys.** SHA-256 verification covers ciphertext bytes (post-encryption), so cron-probe verification of archived encrypted backups doesn't need the passphrase distributed to the verification host.

- **Mixed-mode chain refusal.** A chain whose full is encrypted but an incremental isn't (or vice versa) is rejected at chain-restore time with a clear error. Encryption is per-chain, not per-chunk; chains are atomic.

- **Operator-actionable refusals on restore.** Encrypted chain restored without `--encrypt` → refusal naming `algorithm` / `kek_mode` / `kek_ref`. Wrong passphrase → AES-GCM auth-tag-mismatch error before any data lands on target. No partial data lands on the target on either failure mode.

- **`docs/operator/encryption.md`** — operator-facing guide on passphrase storage best practices, examples integrating with 1Password CLI / AWS Secrets Manager / env-injection patterns, the "lose the passphrase = lose the data" warning, recovery posture, mixed-mode-chain semantics, passphrase rotation workflow.

### Changed

- **`pipeline.Backup` / `IncrementalBackup` / `BackupStream` gain an `Encryption *BackupEncryption` field.** Plaintext (the v0.16.x..v0.21.x default) preserved by leaving it nil. Construction ergonomics unchanged for existing callers.

- **`pipeline.Restore` / `ChainRestore` / `SyncFromBackup` gain an `Envelope crypto.EnvelopeEncryption` field.** Encryption preflight at chain-walk time fails fast on key mismatch; chain-level CEK is unwrapped once per Run() in per-chain mode (Argon2id pays a single derivation cost per restore process).

- **`pipeline.ReadRootManifest` exported helper.** Reads the chain root manifest at `manifest.json` for CLI-side encryption preflight (extracting recorded Argon2id params before constructing the read-side envelope).

### Migration / Compatibility

- **No CLI breaking changes.** `--encrypt` is opt-in; existing backup / restore / sync from-backup invocations without it continue to write / read plaintext chunks identically.
- **Pre-v0.22.0 chains restore unchanged.** Plaintext chains stay plaintext on restore; the manifest's `ChainEncryption` field is absent (omitempty); the chunk reader takes the existing plaintext path.
- **Manifest schema additive.** New encryption fields use `omitempty`; older sluice readers (v0.21.x and earlier) ignore them gracefully — a v0.21.x sluice reading a v0.22.0 plaintext manifest sees the same shape it always did. **An older sluice cannot restore a v0.22.0 encrypted chain** (no decryption code path), but the manifest is human-readable enough that operators can recognize the `chain_encryption` field's presence and upgrade.
- **`backup verify` continues to work without keys.** Existing cron probes that hash chunks need no changes; encrypted chunks hash ciphertext, which matches what's recorded in the manifest.
- **No new heavy dependencies.** AES-GCM uses stdlib `crypto/aes` + `crypto/cipher`; Argon2id uses `golang.org/x/crypto/argon2` (already an indirect dependency, now promoted to direct).

### Deferred

- **Phase 6.2 (AWS KMS) + Phase 6.3 (GCP Cloud KMS / Azure Key Vault).** The `EnvelopeEncryption` interface is the seam those modes plug into; CLI flags will follow the same `--encryption-*` shape with `--kms-key-arn` / `--kms-key-resource` / `--azure-key-vault-id` keys.
- **`--decrypt-verify` for `backup verify`.** v0.22.0 is sha256-only (covers integrity but not "the ciphertext decrypts to something parseable"); a future enhancement will add a deeper verify mode that decrypts + re-hashes plaintext.
- **Passphrase rotation tooling.** v0.22.0's rotation workflow is "fresh full + new chain"; re-encrypting existing chunks under a new passphrase is out of scope. KMS-mode key rotation in Phase 6.2/6.3 will be transparent via cloud-provider key-version chains.
- **Encrypted manifests.** The manifest itself stays plaintext (carries chunk paths, sha256s, and the wrapped CEK — none of which leak rows). Operators wanting "encrypt everything including manifests" have a future-phase option.

## [0.21.2]

Single-bug patch from the v0.21.0 cycle. CDC streams against any Postgres source carrying a `UUID`-typed column crashed on the first INSERT/UPDATE — pre-existing bug, not a v0.21 regression, surfaced when the v0.21 cross-engine cycle expanded UUID coverage. Fix is local to the PG CDC value-decoder; no protocol or schema changes.

### Fixed

- **Bug 41 — PG CDC decode of UUID columns crashes the stream with `UUID byte slice has length 36; want 16`.** Pgoutput's TupleData carries every column value with format byte `'t'` (text); the `'b'` (binary) branch in `decodeTuple` is already a hard refusal, so for the CDC path UUID values arrive at `decodeUUID` as the 36-byte ASCII canonical hyphenated string. The previous code path required `len([]byte) == 16` (binary form, the shape pgx returns for non-CDC reads) and bailed loudly on anything else — including the CDC text-format payload. Net effect: the stream exited with the catalog error message on the first INSERT against any UUID-bearing CDC-streamed table. Workarounds were `--exclude-table` or dropping UUID columns. Fix: `decodeUUID`'s `[]byte` branch now switches on length — 16 routes to the existing binary path (`formatUUIDBytes`), 36 routes through a new `canonicalizeUUIDText` helper that validates the 8-4-4-4-12 hyphenated shape and lowercases to the IR's UUID-as-string contract; any other length surfaces a clear error naming both the length and the supported alternatives. The string-passthrough case now also routes through the same canonicalisation so the IR contract holds whichever shape pgx returns. New helper is a small ASCII validator (no new dependency on `github.com/google/uuid`). Unit tests cover all three positive shapes (16-byte binary, 36-byte text, string passthrough), case folding, and five malformed-input negatives. Integration test `TestCDCReader_UUIDColumnRoundTrip` boots PG 16 with REPLICA IDENTITY FULL on a UUID-bearing table, drives INSERT + UPDATE with known UUIDs, and asserts both Before and After images carry the canonical lowercase string on the CDC channel — without the fix, the stream errored before draining a single event.

### Migration / Compatibility

- **No format changes.** Manifest schema, control-table schema, change-chunk format, CLI surface — all unchanged.
- **No CLI breaking changes.** All existing `sluice` subcommands keep their flag surfaces verbatim.
- **Drop-in upgrade from v0.21.x.** No DDL migration on `sluice_cdc_state`; no operator action required.
- **No behaviour change for non-CDC paths.** Bulk-copy migrate (`sluice migrate`) of UUID-bearing tables continues to work identically — bulk-copy uses `TableReader`, a different code path that already handled `[16]byte` from pgx's binary mode. The new canonicalisation surface only sees the CDC-text shape it didn't before.

## [0.21.1]

Two-item housekeeping release. No functional changes; documentation polish + a CI-flagged test-cleanup race fix.

### Fixed

- **`TestCDCReader_TimestampNonUTCHost` cleanup-race under `-race` on CI.** The test's `t.Cleanup` callback restored `time.Local` while the CDC pump goroutine — which calls go-mysql's binlog decoder, which builds `time.Time` via `time.Unix` (reads `time.Local`) — was still active. The original ordering relied on `defer rdr.Close()` running before the `t.Cleanup`, but `syncer.Close()` does not synchronously wait for the pump goroutine to exit, so the race detector caught reads from a still-running pump. Fix is test-side only: `defer rdr.Close()` becomes a `t.Cleanup` that closes AND drains the `changes` channel to completion. The pump's deferred `close(out)` runs as its last act, so observing the channel close gives a happens-before edge against any further pump-side reads of `time.Local`. Cleanup ordering (LIFO) is now: (1) close rdr + drain changes — registered second, runs first; (2) restore `time.Local` — registered first, runs last. Production CDC reader code is unchanged.

### Documentation

- **`docs/value-types.md` — MySQL binlog-event-volume sizing rule for `--rollover-max-changes`.** New section formalises an operator rule of thumb that surfaced during the v0.20.0 broker cycle: MySQL emits ~3 events per autocommit `INSERT` (`BEGIN` `QueryEvent` → `WRITE_ROWS_EVENTv2` → `XID/TxCommit`), plus a spurious empty `BEGIN/COMMIT` pair on the first DML of any new connection. Operators sizing `--rollover-max-changes` against naive INSERT counts under-size the bound by 3-4×. The new section documents the per-INSERT shape, the multi-row `INSERT` collapsed shape (`2 + N` events), the spurious pair, and the **4× expected-INSERT-count** rule of thumb. Includes a brief contrast against PostgreSQL's `pgoutput` (one event per row change, no in-band BEGIN/COMMIT inflation in the consumer's view) so PG operators don't apply the multiplier where it doesn't belong.

### Migration / Compatibility

- **No format changes.** Manifest schema, control-table schema, change-chunk format, CLI surface — all unchanged.
- **No CLI breaking changes.** All existing `sluice` subcommands keep their flag surfaces verbatim.
- **Drop-in upgrade from v0.21.0.** No DDL migration on `sluice_cdc_state`; no operator action required.

## [0.21.0]

Logical backups Phase 5 lands: **cross-engine chain restore.** A PG-rooted backup chain can now restore (and stream-apply via `sync from-backup`) into a MySQL target, and vice versa. Closes the loud refusal at `chain_restore.go:99` (`"cross-engine chain restore is a Phase 5+ topic"`) that v0.17.0 through v0.20.x raised when the chain's source engine differed from the target. Implementation supplement: `docs/dev/design-logical-backups-phase-5.md`.

### Added

- **Cross-engine `sluice restore --from=<chain-url> --target-driver=<engine>`** — was supported for full-only chains since v0.16.x; v0.21.0 extends it to chains with incrementals. Schema deltas in incremental manifests now route through `internal/translate.RetargetForEngine` before invoking `ir.SchemaDeltaApplier.AlterAddColumn` on the target. PG-source `ADD COLUMN UUID` lands as MySQL `CHAR(36)`; PG-source `ADD COLUMN INET` lands as `VARCHAR(45)`; PG-source `ADD COLUMN <Array>` lands as MySQL `JSON`. Existing `RetargetForEngine` rules are reused verbatim — no new translation surface.

- **Cross-engine `sluice sync from-backup run --target-driver=<engine>`** — the broker variant. Same delta-translation pass on each tick's incremental. Detects cross-engine at startup and logs `INFO broker: cross-engine chain — chain's EndPosition not written to sluice_cdc_state; use --at-chain-id for cross-engine resumption assertions`. The broker still writes its own `_engine="backup-broker"` envelope to `sluice_cdc_state` (warm resume works); the chain's source-engine-flavored terminal `EndPosition` is intentionally omitted because PG LSN ↔ MySQL GTID is not a meaningful translation.

- **Change-event value translation reuses live-CDC machinery.** Cross-engine row payloads in change chunks land at the engine appliers' existing live-CDC value-translation path: each applier looks up its own *target* column types and routes every value through `prepareValue` for target-shape preparation. PG → MySQL: UUID strings bind to `CHAR(36)` natively; JSONB `[]byte` is shaped to a string for MySQL JSON columns (no `_binary` charset prefix). MySQL → PG: TINYINT(1) → `bool` (the cross-engine MySQL → PG bool path) is handled at the CDC reader's decode layer; `pgx` accepts `bool` natively for `BOOLEAN`.

- **Loud refusal for unsupportable types.** PG-source PostGIS `Geometry` columns refuse cross-engine restore to MySQL with an operator-actionable message naming the offending table + column + recovery hint (`--exclude-table` to skip, or `--type-override` for a portable IR type). Pre-flighted at chain start so the operator gets a clear failure before any work happens. Same refusal pattern as full cross-engine restore, extended to cover incremental schema deltas (a delta that introduces a PostGIS column refuses with the incremental's BackupID named).

- **`pipeline.checkCrossEngineSupportable` / `pipeline.checkCrossEngineDeltaSupportable`** — internal helpers driving the refusals. Both return nil for same-engine pairs and unknown engine pairs; PG → MySQL is the loaded direction in v0.21.0. Future engine pairs add their entries here.

### Changed

- **`pipeline.ChainRestore.Run`** — the cross-engine refusal at lines 94-103 is replaced with a routing branch: when `manifest.SourceEngine != Target.Name()`, the supportability pre-flight runs, then schema deltas + change events route through their respective translation paths during apply.

- **`pipeline.SyncFromBackup.applySchemaDeltas`** — also routes deltas through `translate.RetargetForEngine` (mirrors the chain-restore path; the broker's apply intentionally duplicates the chain-restore logic per the Phase 4.5 tenet of "don't refactor across surfaces").

### Migration / Compatibility

- **No format changes.** Manifest schema, control-table schema, change-chunk format are unchanged. Pre-v0.21.0 chains restore identically across same-engine and cross-engine targets.
- **No CLI breaking changes.** All existing `sluice` subcommands keep their flag surfaces.
- **Same-engine paths regression-clean.** Existing same-engine chain-restore tests pass unchanged; same-engine broker happy paths unchanged.
- **`backup verify` unchanged.** Verify is read-only and engine-agnostic; integrity checks pass on cross-engine-target-bound chains identically.

### Deferred

- **Cross-engine CDC handoff with engine-translated `EndPosition`** — translating PG LSN to MySQL GTID set isn't meaningful (different change-log shapes). Operators wanting cross-engine continuous CDC after restore set up a fresh `sluice sync start` against the source's native engine; the chain restore lands the data, sluice sync handles ongoing replication separately.
- **PG-only types not yet in `RetargetForEngine`'s table** (PostGIS geometry, hstore, custom enums beyond the existing PG enum support) — refuse loudly with the offending column named; operator can use `--exclude-table` or `--type-override` per existing escape hatches. Adding new types to the rewrite table is a separate minor.
- **Phase 6 (KMS encryption)** stays unimplemented through Phase 5.

## [0.20.1]

Three-bug patch from the v0.20.0 Phase 4.5 broker cycle. v0.20.0 shipped the consumer-side `sluice sync from-backup` orchestrator; cycle testing surfaced three independent failures along the broker's restart, schema-evolution, and cold-start-recovery paths. None affects chain correctness, the source-side `backup stream`, or the read-only-consumer contract — broker-driven targets stayed safe at all times. Each fix is local to its surface and ships behind the same `--reset-target-data` / `--at-chain-id` operational guardrails as v0.20.0.

### Fixed

- **Bug 38 — `backup stream` does not refresh source schema mid-stream; ALTER TABLE on source while stream is running causes broker apply to fail with `column "<new>" does not exist`.** Pre-fix: the stream baked `parent.Schema` into every rollover's manifest without ever re-reading, so an `ALTER TABLE customers ADD COLUMN tag` on source produced subsequent manifests with the original 3-column schema and the broker's apply hit `SQLSTATE 42703` the moment a CDC change carried the new column. Fix is option (a) from the catalog: at each rollover boundary in `pipeline.BackupStream.runRollover`, re-read the source schema via the engine's existing `SchemaReader`, diff against the parent's recorded schema (using the existing Phase 3 `diffSchemas` helper), and emit `ir.SchemaDeltaEntry` entries into the rollover's manifest. Same shape Phase 3.2's `IncrementalBackup` already produces; the broker's `ApplyChain` consumes it via the existing `SchemaDeltaApplier.AlterAddColumn` path. Engine-neutral — works on PG and MySQL identically. The stream now records `INFO stream: schema delta detected at rollover` whenever the diff produces entries.

- **Bug 39 — `sluice sync from-backup run` warm-resume after restart fails: PG / MySQL `ChangeApplier.ReadPosition` discards the broker's engine sentinel, restart errors with `stream "<id>" is owned by a non-broker writer (position engine "postgres")`.** Pre-fix: the broker writes its position with `Engine="backup-broker"` but the engine appliers' `ReadPosition` hard-codes its own engine name into the returned `ir.Position.Engine` (`engineNamePostgres` / `engineNameMySQL`), discarding the broker's sentinel on the round-trip. The `sluice_cdc_state` table has no `position_engine` column, so the engine sentinel had nowhere durable to live. Fix is option (b) from the catalog: encode `_engine` into the broker's token JSON envelope (`{"_engine":"backup-broker","chain_url":...,"last_applied_backup_id":...}`). On restart, the broker reads its own row's token and discriminates via the embedded `_engine` field rather than `Position.Engine`. Backward-compatible: legacy v0.19.x rows without `_engine` parse as non-broker (current behaviour preserved). No DDL migration on `sluice_cdc_state`. The discriminator helper `isBrokerToken` is the new canonical "is this a broker row?" predicate. Unit tests pin round-trip + legacy-shape behaviour.

- **Bug 40 — `sync from-backup run --reset-target-data` does not drop pre-existing target tables; ChainRestore CREATE TABLE IF NOT EXISTS no-ops; broker silently hangs in restore phase.** Two compounding root causes; fixed in two layers:
  - **40a (drop tables before chain restore).** Pre-fix: the broker's `--reset-target-data` branch invoked `pipeline.ChainRestore` directly, whose schema-application path uses `CREATE TABLE IF NOT EXISTS` — a no-op against pre-existing tables. A target carrying a stale `(id, email)` shape from a prior cycle would keep its old columns, and the chain's bulk-copy COPY would reference columns the table didn't have. Fix: `SyncFromBackup.coldStartReset` now enumerates the chain's terminal manifest schema and drops every named table via the existing `ir.TableDropper` / `ir.BulkTableDropper` surfaces (mirrors `migrate --reset-target-data`'s drop-loop in `reset.go`) before invoking ChainRestore. Idempotent across retries via `DROP TABLE IF EXISTS`.
  - **40b (surface bulk-copy errors loudly).** Pre-fix: when the COPY errored, the producer goroutine in `restoreTable` was still trying to push rows into an unbuffered channel that the writer no longer drained, so the producer blocked forever on `rowCh <- row` and `<-errCh` deadlocked. Net effect: silent hang, idle PG connections in `ClientRead` state, no log lines, operator must `Stop-Process` to recover. Fix: `restoreTable` now derives a `streamCtx` for the producer and calls `streamCancel()` on writer failure; the producer's `<-ctx.Done()` arm in `streamChunkRows` unblocks the goroutine cleanly so the error propagates (loud-failure tenet preserved). Defense-in-depth fix that prevents future silent-hang regressions in the chain-restore bulk-copy path.

  The two fixes compose: 40a closes the primary failure (stale schema is gone before restore runs), 40b ensures any future variant of "writer fails mid-restore" surfaces as a real error rather than a hang. New `INFO broker: --reset-target-data: target tables dropped before chain restore` and `ERROR restore: write rows failed; cancelling chunk producer` log lines mark the new code paths. Integration test `TestSyncFromBackup_ColdStartWithReset_StaleSchema` pre-seeds the target with a deliberately-stale `users` table + a sentinel row and confirms the chain restore lands cleanly with no stale data surviving.

### Migration / Compatibility

- **No format changes.** Manifest schema, control-table schema, change-chunk format are unchanged.
- **Backward-compatible token shape (Bug 39).** Legacy v0.19.x rows without `_engine` parse as non-broker; v0.20.1+ broker writes both the embedded sentinel AND the deprecated `Position.Engine` field for forward compatibility.
- **No CLI breaking changes.** All existing `sluice` subcommands keep their flag surfaces.

### Fixed (post-tag, drain-commit regression)

- **Bug 38 fix follow-up — graceful-drain rollover dropped its in-flight chunks.** The Bug 38 fix ran `refreshSchemaAndAttachDelta` after `captureWindow` returned but before setting `out.Manifest`; on `ctx`-cancel mid-rollover (the SIGTERM / drain path), the schema refresh dialed a new connection on the already-cancelled `ctx`, failed immediately with `ctx.Err()`, and left `out.Manifest=nil` — so the outer drain-commit branch in `BackupStream.Run` saw "nothing to commit" and silently dropped the chunk that `captureWindow`'s own drain path had already flushed to the store. Symptom: `TestBackupStream_*RolloverByMaxChanges` deterministically missed `user24` (the last event captured before cancel) on both engines. Fix: skip the schema-refresh on `ctx.Canceled` / `context.DeadlineExceeded` so the drain-commit rollover commits with the parent's schema. Functional behavior of the Bug 38 fix is unchanged for normal rollovers — `SchemaDelta` is still emitted at rollover end whenever the source schema diff is non-empty; only the cancelled-rollover edge case skips the refresh, and any DDL that occurred during the drained window is captured on the next stream's first rollover (diffed against the drain-commit's terminal manifest).

## [0.20.0]

Logical backups Phase 4.5 lands: `sluice sync from-backup` is the consumer-side companion to v0.19.0's `sluice backup stream`. The headline operator outcome: **decouple source and target via the backup chain as the message log.** Source-side `backup stream` writes incrementals to S3/GCS/Azure/local-FS; target-side `sync from-backup` polls the same destination and replays incrementals into its own database — log-based ETL without direct source-target connectivity. Implementation supplement: `docs/dev/design-logical-backups-phase-4-5.md`.

### Added

- **`sluice sync from-backup run --backup-target=<url> --target-driver --target --stream-id=ID`** — new long-running broker subcommand. Drives a `for { tick(); replay(); commit(); }` loop at the configured `--poll-interval=DURATION` cadence (default `30s`). Each tick lists manifests at the chain root, filters to incrementals NOT yet applied (via the persisted `last_applied_backup_id` in `sluice_cdc_state`), and replays each in chain order — schema deltas first (via `ir.SchemaDeltaApplier.AlterAddColumn` from Phase 3.2), then change chunks through the engine's batched `ChangeApplier.ApplyBatch` (reusing the existing applier verbatim per ADR-0010 idempotent-apply).

- **`sluice sync from-backup stop --backup-target=<url>`** — companion stop command. Writes `stop_requested_at` to the chain destination's `manifests/broker_state.json`; the running broker observes the request on its next tick poll and exits cleanly. Cross-machine: an operator on machine B can stop a broker running on machine A without process access — both sides agree on the chain destination. Mirrors the `backup stream stop` pattern.

- **`pipeline.SyncFromBackup` orchestrator** — opens the target's `ChangeApplier` ONCE for the broker's lifetime; each tick reuses the connection. Per-tick INFO log line records new applied + total bytes + elapsed for monitoring. Integration tests use `--poll-interval=2s` for ~30-60s test scenarios.

- **`pipeline.RequestSyncFromBackupStop(ctx, store, now)`** — exported helper for downstream tooling that wants to stop a running broker without going through the CLI. Idempotent: re-issuing stop preserves the original `stop_requested_at` timestamp so drain-completion watchers don't see the clock reset. In-process channel registry (`broker_stop_registry.go`) closes same-process stops with zero file I/O, mirroring the v0.19.1 stream-stop fix.

- **`manifests/broker_state.json`** — new informational liveness file. Mirrors Phase 4's `stream_state.json` shape at the consumer side: `{pid, host, stream_id, started_at, last_apply_at, stop_requested_at}`. Coexists with `stream_state.json` when a stream + broker run against the same destination — one is producer-side, the other consumer-side; neither gates the other's concurrent-writer check.

- **Cold-start safeguards.** First-start refusal when `sluice_cdc_state` has no row for the supplied `--stream-id` (mirrors `migrate --force-cold-start` friction tier from Bug 9). Two override flags:
  - `--reset-target-data` — the broker first runs `pipeline.ChainRestore` internally to land the full + every incremental up to current, THEN transitions to live polling.
  - `--at-chain-id=<BACKUP-ID>` — operator's assertion that the target is currently at chain ID `<BACKUP-ID>` (typical workflow: manual `sluice restore --from=<chain-url>` followed by `sync from-backup`); broker writes a fresh `sluice_cdc_state` row and transitions to live polling.

- **`ir.PositionWriter` optional surface** — engine appliers that implement it allow the broker to record cold-start positions and schema-delta-only-incremental positions without an accompanying data write. Postgres + MySQL appliers implement it via the same `writePositionTx` helper the apply path uses, so the row shape and idempotency contract are identical.

- **Replay state via existing `sluice_cdc_state`** — no schema change. New position-shape sentinel: `position_engine = "backup-broker"`, `position_token = '{"chain_url":"...","last_applied_backup_id":"<id>"}'`. Distinct from `sync start`'s positions (CDC LSN/GTID); the broker's positions reference chain state. ADR-0007 transactional position-and-data atomicity makes broker crashes mid-replay safe to re-apply (ADR-0010 idempotent applier).

### Changed

- **`cmd/sluice/cli.go` grows the `sync from-backup` subtree.** New `SyncCmd.FromBackup` field of type `SyncFromBackupCmdGrp` (kong-grouped `Run` + `Stop` subcommands). Existing `sync start` / `sync stop` / `sync status` / `sync health` flag surfaces are unchanged.

### Fixed (post-tag, integration-test stability)

- **MySQL broker happy-path test (`TestSyncFromBackup_MySQL_HappyPath`)** — bumped the per-test `IncrementalBackup.MaxChanges` cap from 10 to 50. MySQL's binlog emits ~3 events per autocommit `INSERT` (`BEGIN` `QueryEvent` → `WRITE_ROWS_EVENTv2` → `XID/TxCommit`) and the session's first connection-time interaction commonly flushes a short empty `BEGIN/COMMIT` pair into the binlog ahead of any DML, so 5 INSERTs produced 17 captured events in CI. The original cap of 10 stopped the incremental window after only 3 INSERTs, leaving `user3`/`user4` missing from the chain. No code-path change — the broker, applier, and incremental-backup orchestrator all behaved correctly; the test bound was simply too tight for MySQL's event volume.
- **PG broker cold-start-with-reset test (`TestSyncFromBackup_ColdStartWithReset`)** — the test's poll loop used `pgQueryEmails`, which fatal-fails on any query error including the expected `users does not exist` window between launching the broker goroutine and the broker's inline `ChainRestore` recreating the schema. Added `pgQueryEmailsTolerant` (returns `nil` on `SQLSTATE 42P01` instead of fatal) for poll-loop callers; production code paths are unchanged.

### Migration / Compatibility

- **No format changes.** Manifest schema, control-table schema, change-chunk format are unchanged. Pre-v0.20.0 chains restore + verify identically.
- **No CLI breaking changes.** All existing `sluice` subcommands keep their flag surfaces.
- **Brokers are read-only consumers of the chain.** They never modify manifests; the chain itself stays the source of truth for restore + verify regardless of broker activity.
- **Same-engine only in v1.** Cross-engine `sync from-backup` (PG-source-chain → MySQL target) is deferred to Phase 5 alongside cross-engine chain restore. The broker today refuses cross-engine chains the same way `chain restore` does.

### Deferred

- **Phase 5 (cross-engine chain restore)** — the partner feature. Cross-engine `sync from-backup` waits for the SELECT-grammar translator + `RetargetForEngine` extensions in Phase 5.
- **Phase 6 (KMS encryption)** — encrypted-chain consumption (the broker reads the same chunks that decrypt-on-restore would handle).
- **Phase 7+ — operationally-mature features**: multi-source aggregation (one target consuming N source chains), selective-table replay (`--include-table` on the broker), time-shifted replay (`--lag-window=DURATION`).

## [0.19.1] - 2026-05-08

Single-bug patch from the v0.19.0 test cycle. v0.19.0 shipped logical-backups Phase 4 (`sluice backup stream`) with the `TestBackupStream_Postgres_StopCommandRequestsExit` cooperative-stop integration test skipped on CI; cycle testing surfaced that the `sluice backup stream stop` companion command does not fire reliably under `-race` + heavy goroutine contention. The CDC pump, chain correctness, rollover policy, and SIGTERM-based shutdown were unaffected — only the cross-machine convenience surface was unreliable. Fix is option (b) from the BUG-CATALOG analysis: in-process channel notification for the same-process case + a heartbeat read-modify-write that closes the file-poll clobber race.

### Fixed

- **Bug 37 — `backup stream` stop-signal observation does not fire reliably under `-race` + heavy goroutine contention; `sluice backup stream stop` may not trigger graceful drain on contended systems.** Phase A instrumentation pinned the actual root cause to **hypothesis (c)** from the catalog analysis: the `captureWindow` stop-poll fires on time and `LocalStore.Get` returns sub-millisecond, but the state file returned by `Get` is missing `stop_requested_at` because the running stream's per-rollover heartbeat write at the rollover boundary CLOBBERED it. The stream's in-memory `state` struct never carried `StopRequestedAt`, so its last-writer-wins overwrite of `manifests/stream_state.json` at every rollover boundary silently erased the operator's stop request whenever the heartbeat write landed after `RequestStreamStop`'s write within the same rollover-cycle. Once clobbered, neither the inner stop-poll nor the outer-loop `readStreamStopRequested` could recover — the field was gone from disk. The race fired more reliably on CI under `-race` because rollover-window deadline + heartbeat-write timing is sensitive to scheduler overhead; local development happened to land in the safe ordering on most runs.

  **Pre-fix shape (v0.19.0):** `TestBackupStream_Postgres_StopCommandRequestsExit` consistently exceeds even a 60-second post-stop budget on CI (25.71s → 65.69s when budget bumped 20s → 60s — proportional scaling indicating the stream NEVER observes the stop, not just slow). `t.Skip` guard added so the test runs locally but not on CI; v0.19.0 ships with a workaround documented for operators (use SIGTERM directly instead of `backup stream stop`).

  **v0.19.1 fix (two layered closes):**
  1. **Heartbeat read-modify-write** (the actual correctness bug). New `writeStreamStateMergeHeartbeat` helper in `stream_state.go`: at every per-rollover heartbeat boundary, read the current state file first, copy any concurrent `StopRequestedAt` forward into the new payload, then write. When the merge observes a concurrent stop, the outer loop's heartbeat call returns `stopObserved=true` and the stream exits cleanly without starting a fresh rollover. Closes the clobber-race window across all backends (`LocalStore` and the `s3://` / `gs://` / `azblob://` `BlobStore` variants).
  2. **In-process channel notification** (option (b) from BUG-CATALOG; structural reliability win). New `stream_stop_registry.go` maintains a process-local map of `[ir.BackupStore]→chan struct{}`; `BackupStream.Run` registers its store at startup and deregisters on return. `RequestStreamStop` closes the registered channel alongside the file write. `captureWindow`'s select grows a `case <-stopCh` that fires instantaneously when same-process — no file I/O, no select-loop starvation, no clobber-race window. Cross-process operators (`sluice backup stream stop --target=<url>` on a different machine) still go through the file; the channel is process-local and `notifyStreamStop` is a no-op for them. Both paths land at the same eager-exit code path, so the chain-correctness contract is unchanged.

  **Verification (v0.19.1 cycle):** `TestBackupStream_Postgres_StopCommandRequestsExit` re-enabled on CI (no skip); local runs pass in ~6s with logs showing `in_process_stop` fires within ~5ms of `RequestStreamStop`'s write. All 6 BackupStream Postgres + MySQL integration tests pass. Phase 3 chain-restore + Phase 3.3 `--position-from-manifest` tests pass clean. Full `./internal/pipeline` integration suite green (834s, 0 failures). New unit tests `TestWriteStreamStateMergeHeartbeat_PreservesStop`, `TestWriteStreamStateMergeHeartbeat_NoStopReturnsFalse`, and `TestStreamStopRegistry_*` pin the contracts.



Logical backups Phase 4 lands: `sluice backup stream` is a single long-running process that produces rolling incrementals at a configured cadence, no per-incremental cron orchestration. Fits k8s "always-on protection" deployments naturally; pairs with continuous CDC + chain-restore for full DR coverage. Implementation supplement: `docs/dev/design-logical-backups-phase-4.md`.

### Added

- **`sluice backup stream run`** — new long-running stream subcommand. Drives a `for { rollover() }` loop where each rollover is a bounded window producing one new manifest at `manifests/incr-<unix-millis>-<seq>.json`. Three rollover ceilings active in parallel, first-fired wins:
  - `--rollover-window=DURATION` (default `5m`): wall-clock cadence.
  - `--rollover-max-changes=N` (default `100000`): change-count ceiling.
  - `--rollover-max-bytes=BYTES` (default `64Mi`, mirrors `--max-buffer-bytes` from Phase 2 backup writer): buffered-bytes ceiling.

  Window extends to the next `TxCommit` so the chain doesn't end mid-tx (mirrors Phase 3.1's incremental orchestrator). Empty rollovers skipped by default; `--rollover-include-empty` opts in for heartbeat-shape monitoring.

- **`sluice backup stream stop`** — companion stop command. Writes `stop_requested_at` to the destination's `stream_state.json`; the running stream observes the request on its next rollover-tick poll and exits cleanly. Cross-machine: an operator on machine B can stop a stream running on machine A without process access — both sides agree on the destination. Mirrors the `sync stop` pattern (ADR-0025).
- **`pipeline.BackupStream` orchestrator** — opens the engine's CDC pump ONCE for the lifetime of the stream and reuses it across rollovers (load-bearing efficiency win over a tight `for { incremental.Run() }` loop, which would re-open the slot every iteration). `manifests/stream_state.json` carries `{pid, host, started_at, last_rollover_at, stop_requested_at}` for liveness + cross-machine signalling. Concurrent-writer protection: refuses to start a second stream when the file shows a recent (`< 2 × rollover-window`) `last_rollover_at` from a different (pid, host); `--force` bypasses with a WARN.
- **`pipeline.RequestStreamStop(ctx, store, now)`** — exported helper for downstream tooling that wants to stop a running stream without going through the CLI. Idempotent: re-issuing stop preserves the original `stop_requested_at` timestamp so drain-completion watchers don't see the clock reset.
- **Rollover hooks** — `--rollover-hook=<cmd>` runs a shell command after each rollover commits successfully. Receives env vars `SLUICE_ROLLOVER_MANIFEST_PATH`, `SLUICE_ROLLOVER_PARENT_BACKUP_ID`, `SLUICE_ROLLOVER_BACKUP_ID`, `SLUICE_ROLLOVER_CHANGES`, `SLUICE_ROLLOVER_BYTES`, `SLUICE_ROLLOVER_ELAPSED_MS`. 30 s timeout. Hook errors WARN-log but do NOT fail the stream — the rollover already committed. Examples in docs: push to Prometheus pushgateway / send Slack notification / write to monitoring datastore.
- **Signal handling** — SIGINT / SIGTERM via the existing `kongContext` notifier propagates as ctx.Done through the rollover loop; mid-rollover cancel surfaces as a clean nil exit. The rollover's chunks may be partially-written but the manifest never finalises; on restart the stream picks up at the previous rollover's EndPosition. Operator-visible warning + `sluice backup verify` recommendation for orphan-chunk cleanup mid-rollover-crash.

### Changed

- **`cmd/sluice/backup.go` grows the `BackupStream` subtree.** New `BackupCmd.Stream` field of type `BackupStreamCmdGroup` (kong-grouped `Run` + `Stop` subcommands). Existing `backup full` / `backup incremental` / `backup verify` flags unchanged.
- **Stop-signal polling cadence on `backup stream` decoupled from rollover-window cadence (1s polling).** Inside the rollover capture loop a dedicated 1-second ticker reads `stream_state.json`'s `stop_requested_at` field, so an operator's `sluice backup stream stop` is observed within ~1 s regardless of the (typically minutes-long) rollover-window setting. On observation, the in-flight rollover flushes and commits, then the stream exits without a final state-file write that would clobber the operator's stop request.
- **Stop-signal observation in `backup stream` now triggers an eager exit** (commit any in-flight changes immediately + return) rather than waiting for the next TxCommit boundary, so quiet-source streams exit within seconds of `sluice backup stream stop` regardless of source activity.
- **Chain-walker manifest discovery filters by entry shape (`manifest.json` + `manifests/incr-*.json`)** so non-manifest state files like Phase 4's `manifests/stream_state.json` aren't mistaken for chain entries. Restore + verify against any stream-written destination now works regardless of how many state files coexist with the chain manifests.
- **`backup stream` ctx-cancel now performs a graceful drain of the in-flight rollover** (chunks flushed + manifest written within `stopDrainTimeout`), matching the design doc's SIGTERM contract. Previously, ctx-cancel mid-rollover dropped the rollover entirely, losing any changes captured since the last commit boundary.

### Migration / Compatibility

- **No format changes.** Manifest schema is unchanged; stream rollovers write Phase-3 shape manifests at the same `manifests/incr-…json` path. Pre-v0.19.0 chains (single-shot incrementals + fulls) remain compatible — `restore` and `verify` walk stream-written chains identically.
- **`stream_state.json` is new and informational-only.** The chain itself remains the source of truth for restore + verify. Losing the state file (operator deletes it, object-store eventual-consistency lag) doesn't break the chain — only the concurrent-writer / cross-machine-stop signalling falls back to ctx-cancel and process signals.
- **No CLI breaking changes.** `sluice backup full` / `sluice backup incremental` / `sluice backup verify` / `sluice restore` flag surfaces are unchanged.

### Deferred

- **Phase 4.5 (backup-as-broker / `sync from-backup`)** — the watcher-side feature that polls a chain and replays incrementals into a target. Stream is the producer; `sync from-backup` is the future consumer. Out of scope here; tracked separately on the roadmap.
- **Cross-engine chain restore** — Phase 5+ topic. Phase 4 streams produce same-engine chains; cross-engine replay needs the existing translate machinery extended for replay-of-changes-with-translation.
- **KMS encryption** — Phase 6.

## [0.18.0] - 2026-05-07

Closes the v0.17.2-documented "during-backup write window" gap. v0.17.x full backups recorded `EndPosition` at end-of-backup with no shared snapshot across tables — writes that landed on already-read tables before the position capture were missing from BOTH the row chunks AND the first incremental's `--since=<full>.EndPosition` window. v0.18.0 wires the full-backup row sweep into a snapshot-anchored consistent view and captures `EndPosition` at snapshot START, so the chain's next link's CDC stream from `EndPosition` forward picks up every write after the snapshot. Backup-only DR (no continuous `sluice sync start` paired) is now byte-perfect under heavy write load.

### Added

- **`ir.BackupSnapshotOpener` optional engine surface.** Returns an `ir.BackupSnapshot` bundle: snapshot-anchor `Position` + a snapshot-pinned `RowReader` + a cleanup closure. Engines that implement it get cross-table snapshot consistency plus a snapshot-anchored `EndPosition` for free; engines that don't fall back to the v0.17.x `BackupPositionCapturer` path with a soft `WARN` log line surfacing the during-backup window gap.
- **Postgres `OpenBackupSnapshot` implementation.** Creates a temporary `EXPORT_SNAPSHOT`-shape replication slot (named `sluice_backup_anchor_<unix-nanos>`) to anchor the snapshot LSN, opens a `*sql.Conn` that imports the snapshot via `SET TRANSACTION SNAPSHOT '<name>'`, and returns a `RowReader` bound to the conn. The temporary anchor slot is dropped on close (the `consistent_point` LSN is preserved on the manifest's `EndPosition` for chain handoff against the operator's chain-handoff slot, which is recorded on the position alongside the LSN). Reuses `createLogicalReplicationSlot`'s PG-version-adaptive helper (FAILOVER on PG 17+).
- **MySQL `OpenBackupSnapshot` implementation.** Pins a single `*sql.Conn`, runs `SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ` + `START TRANSACTION WITH CONSISTENT SNAPSHOT`, captures `@@global.gtid_executed` (or `(file, pos)` in non-GTID mode) inside the same transaction so the recorded position refers to the snapshot's logical clock. All table reads run on this one connection sequentially. **Trade-off vs PG**: MySQL's REPEATABLE READ snapshot is per-session and not shareable across connections (ADR-0019), so multi-conn parallel reads aren't available on this path; PG's snapshot can fan out to N readers via the existing `SnapshotImporter` machinery in a future revision. MySQL operators running backups under high read parallelism configurations should expect single-conn throughput on the row sweep.

### Changed

- **`pipeline.Backup` orchestrator now prefers the snapshot path.** `Backup.Run` type-asserts on `ir.BackupSnapshotOpener` at start of run; when implemented, the captured snapshot position becomes `Manifest.EndPosition` immediately and the post-sweep `BackupPositionCapturer` fallback is skipped. When the engine doesn't implement the new surface, falls through to the v0.17.x shape with a `WARN` line citing the during-backup window gap and pointing at the v0.17.2 release notes / `docs/dev/design-logical-backups-phase-3.md` for context. Snapshot-open errors fall back to the v0.17.x `OpenRowReader` + post-sweep `BackupPositionCapturer` path with a clear `WARN`, so backups work on PG environments without `wal_level=logical` (no-CDC scenarios). Chain-correctness still requires the snapshot path; the WARN names the operational implication.
- **`Manifest.EndPosition` semantic.** For full manifests written by v0.18.0+ this is the snapshot-anchor LSN/GTID (captured AT snapshot start); for fulls written by v0.17.0–v0.17.3 it remains the post-sweep position (captured at end-of-backup). The chain-walker treats both identically — the field is a CDC resume cursor — so existing chains restore unchanged. The wire shape of `EndPosition` is unchanged; only its capture timing semantics shift, so old chains and new chains can coexist in a chain history without operator action.
- **`docs/dev/design-logical-backups-phase-3.md`.** The "Implementation note: deviation from snapshot-anchored EndPosition" section is rewritten — flips from "deviation in v0.17.2" to "deviation closed in v0.18.0" with the post-fix shape documented.

### Closed

- **v0.17.2's documented "during-backup write window" caveat.** The release notes for v0.17.2 surfaced this as a known limitation with the workaround "pair backups with continuous `sluice sync start`." v0.18.0 closes it — backup-only DR works correctly even under heavy write load on the source. The mitigation pattern is still recommended for the "fresher than the most recent incremental window" use case, but is no longer load-bearing for chain correctness.

### Migration / Compatibility

- Pre-v0.18.0 chains restore unchanged; the chain-walker handles both old (post-sweep) and new (snapshot-anchored) `EndPosition` semantics identically.
- Operators running PG with `wal_keep_size` tuned for the chain's incremental cadence don't need to revisit settings — the snapshot anchor is short-lived (dropped at end of full) and doesn't change the chain's WAL footprint at the chain-handoff slot.
- MySQL operators running backups against high-throughput sources should expect the row sweep to be single-conn (per-session snapshot constraint); throughput-sensitive backups that previously ran with multiple workers via OpenRowReader will lose that parallelism. Document the trade-off; mitigation is to run backups during slightly slower windows or accept the consistent-view trade.

## [0.17.3] - 2026-05-07

Single-bug patch from the v0.17.2 test cycle. v0.17.2 shipped Phase 3.3's PG soft-warning preflights (including Patroni / HA-managed source detection); the cycle surfaced that the three v0.17.2 detection signals all systematically miss on tenant-isolated managed PG services like PlanetScale Postgres. The operators who most need the idle-slot trap warning (managed-PG users who can't tune their own slot retention) got nothing.

### Fixed

- **Bug 36 — Patroni / managed-PG idle-slot trap warning does not fire on PlanetScale Postgres (or other tenant-isolated managed PG services).** The v0.17.2 `detectPatroniSource` heuristic checked three signals: (1) `pg_settings WHERE name ILIKE '%patroni%'`, (2) `pg_stat_replication.application_name ILIKE 'patroni%'`, (3) `pg_roles WHERE rolname IN ('patroni', 'replicator')`. All three miss on PS-PG: Patroni sets standard PG GUCs via `ALTER SYSTEM` (not Patroni-prefixed ones, so `name ILIKE '%patroni%'` returns 0 rows); `pg_stat_replication` is permission-restricted on per-tenant roles (returns 0 rows even when Patroni is using it); PS creates tenant-prefixed roles like `hzi_xgsa060j2bbb_role` (so `rolname IN ('patroni', 'replicator')` doesn't match). Net effect: managed-PG operators got no warning when pointing `--position-from-manifest` at their cluster. Fix lands as option (c) from the BUG-CATALOG analysis: broader heuristics + an explicit override flag.

  **Broader engine-side heuristics (v0.17.3 adds Signals 4–5):**
  1. **Non-temporary physical replication slots present.** `SELECT count(*) FROM pg_replication_slots WHERE slot_type = 'physical' AND temporary = false`. Standby physical slots are a strong HA-cluster signal — most non-HA PG deployments don't carry them. Permission-denied on `pg_replication_slots` (some managed services restrict it) gracefully degrades to skipping the signal.
  2. **`cluster_name` GUC populated.** Patroni convention sets this; many managed services follow suit. Empty string = no signal; permission-denied / sql.ErrNoRows on `pg_settings WHERE name = 'cluster_name'` gracefully degrades.

  **DSN hostname-pattern signal (streamer layer, layered on top of the engine's six SQL signals):** known managed-PG suffixes — `*.psdb.cloud` (PlanetScale Postgres), `*.aws.prod.archil.com` / `*.gcp.prod.archil.com` (Archil), `*.cluster*.rds.amazonaws.com` (Aurora cluster endpoints; vanilla RDS instances are excluded because they're not always HA), `*.postgres.database.azure.com` (Azure Database for PostgreSQL), `*.cloudsql.google.internal` (Cloud SQL via private IP). Patterns are intentionally narrow — false positives on non-HA setups would erode the warning's signal value. The signal lives at the streamer layer because the IR `PositionFromManifestPreflight` interface deliberately doesn't carry the DSN (engines without network awareness can implement it cleanly).

- **Closes Bug 36.** Verified via integration tests on testcontainer-shaped HA signals: `TestPreflight_PG_DetectsPhysicalSlot` (a non-temporary physical replication slot trips Signal 4, citing the slot signal in the warning); `TestPreflight_PG_DetectsClusterNameGUC` (cluster_name set at server start trips Signal 5, citing the GUC value); `TestSyncStart_PatroniMode_Off_SuppressesWarning` (with a physical slot present, `--patroni-mode=off` strips the warning while keeping wal_keep_size warnings intact); `TestSyncStart_PatroniMode_On_FiresOnVanilla` (on vanilla PG with no Patroni signals, `--patroni-mode=on` still emits the operator-forced warning). DSN hostname-pattern detection is unit-tested via direct DSN-string parsing across all six patterns (PlanetScale, Archil aws/gcp, Aurora, Azure, Cloud SQL) plus negative cases (vanilla RDS instance, self-hosted host, localhost, empty DSN).

### Added

- **`sluice sync start --patroni-mode=auto|on|off`.** New flag pairing with the broader heuristics. `auto` (default) runs the engine heuristics + DSN hostname-pattern check and warns if any of the six signals fires; `on` skips the heuristics and forces the warning (operator opts in regardless of detection — the canonical override for tenant-isolated managed PG where the heuristics still miss); `off` skips the heuristics and suppresses the Patroni warning entirely (operator confirmed self-hosted single-node PG without HA, doesn't want the noise). Combine `--patroni-mode=on` with `--strict-preflight=true` to make the warning a hard refusal. The slot-existence / `wal_status='lost'` refusal is unaffected by `--patroni-mode` (those are always refusals — the slot can't deliver what's needed). Validation: unknown values are rejected at flag parse time with a clear error naming the accepted set.

## [0.17.2] - 2026-05-07

Logical backups Phase 3.3 lands: full-backup `EndPosition` recording, the `--position-from-manifest` CDC handoff flag, and PG soft-warning pre-flights. Closes the v0.17.0 known-limitation list — chains rooted in v0.17.2+ fulls work end-to-end without manually patching the manifest's terminal position, and a freshly-restored target resumes CDC from the chain's tail without re-bulking from source. Implementation supplement: `docs/dev/design-logical-backups-phase-3.md` (Phase 3.3 row in the sub-phasing table).

### Added

- **Full-backup `EndPosition` recording (Phase 3.3.A).** `sluice backup full` now captures the source's CDC position at end-of-backup and writes it onto the manifest's `EndPosition` field. PG records `pg_current_wal_lsn()` paired with the configured slot name (default `sluice_slot`; override via the new `--slot-name` flag); MySQL records `@@global.gtid_executed` (or `(file, position)` when GTID mode is off) via the existing master-status helpers. Engines opt in by implementing the new `ir.BackupPositionCapturer` optional interface on their `SchemaReader`; engines without CDC support skip silently. Closes the v0.17.0 known limitation: incrementals chained off v0.17.2-rooted fulls no longer fire the "parent has no EndPosition; chain will start from CDC's current position" warning.
- **`sluice sync start --position-from-manifest=<chain-url>`.** New CLI flag that loads the chain's terminal manifest's `EndPosition` and uses it as the resume position, bypassing the per-target `sluice_cdc_state` lookup. Use after `sluice restore --from=<chain-url>` to resume CDC from the chain's tail without re-bulking from source. Mutually exclusive with `--reset-target-data` (different recovery shapes; both override the persisted position). The slot-missing fall-through (ADR-0022) is suppressed when chain handoff is requested — silently re-bulking would defeat the chain's purpose. Accepts the same `s3://` / `gs://` / `azblob://` / `file:///` URL schemes as `sluice backup`, with companion `--backup-endpoint` / `--backup-region` / `--backup-path-style` flags for S3-compatible providers.
- **PG soft-warning pre-flights (Phase 3.3.C) for `--position-from-manifest`.** New `ir.PositionFromManifestPreflight` optional engine surface; PG implements three checks against the source before CDC opens:
   1. `wal_keep_size` sufficiency — soft warning when configured below PG's 64 MB default (so only setups that explicitly dialed it down trigger), with an operator-facing pointer to `docs/postgres-source-prep.md`.
   2. Patroni / HA-managed source detection — soft warning about the idle-slot failover trap (the user's 2026-05-07 production finding). Three signals checked in order: Patroni-set GUCs in `pg_settings` (most specific), `pg_stat_replication.application_name` LIKE 'patroni%' (catches standby connections; gracefully degrades on permission denied), role names `patroni` / `replicator` (loosest).
   3. Slot existence + health — fatal refusal for missing or `wal_status='lost'` / `'unreserved'`. Always a refusal regardless of `--strict-preflight` because the slot can't deliver what's needed.

   MySQL intentionally has no preflight surface — its CDC reader's existing `verifyPositionResumable` already covers binlog purge.

- **`sluice sync start --strict-preflight` (Phase 3.3.D).** New flag that promotes the soft warnings emitted by Phase 3.3.C to hard refusals before CDC starts. Default off: warnings log via slog and the run proceeds. Use in CI gates, scripted runbooks, or post-incident audits where the operator wants a strict "fail loudly on any preflight signal" posture.
- **`sluice backup full --slot-name`.** Labels the recorded `EndPosition` on engines with a slot concept (Postgres) so a Phase 3 incremental opens CDC against a slot of the same name. Engines without slots (MySQL: binlog stream is the slot) ignore the flag. Default `sluice_slot`.
- **`pipeline.LoadChainTerminalPosition(ctx, store)` exported helper.** Reads every manifest in a backup store, validates the chain shape via the existing `buildChain` helper, and returns the terminal manifest's `EndPosition`. Used by the streamer's `--position-from-manifest` path; exposed for downstream tooling that wants to inspect a chain's tail position without standing up a sync.

### Changed

- **`ir.PositionFromManifestPreflight` and `ir.PreflightReport` live in the `ir` package** (initially scoped to `pipeline`). The cycle-break is necessary because engine packages implement the interface and integration tests in pipeline import engines. The `pipeline` package keeps type aliases so existing call sites compile unchanged.
- **`pipeline.ResolveSlotName` exported** so CLI commands outside the pipeline package (today: `sluice backup full --slot-name`) can apply the sluice-prefix convention without re-implementing it.

### Phase 3 known limitations (closed)

The v0.17.0 release notes flagged three Phase 3.3 follow-ups; all three are addressed in v0.17.2:

- ✅ Full backups record `EndPosition` automatically (Phase 3.3.A).
- ✅ `sluice sync start --position-from-manifest` is implemented (Phase 3.3.B).
- ✅ PG `wal_keep_size` soft-warning + Patroni-detection pre-flights are implemented (Phase 3.3.C).

## [0.17.1] - 2026-05-07

Single-bug patch from the v0.17.0 test cycle. v0.17.0 shipped logical-backups Phase 3.1 + 3.2 (incrementals + chain restore); the cycle surfaced a writer-side path collision that broke any chain with two or more incrementals into the same destination. Single-incremental chains and the schema-evolution path were unaffected.

### Fixed

- **Bug 35 — Incremental change-chunk filename collision; second incremental clobbers the first's chunk on disk.** v0.17.0's change-chunk writer constructed paths as `chunks/_changes/changes-<idx>.jsonl.gz` with the chunk index reset to 0 per-Run. Two incrementals taken into the same `--output-dir` / `--target` therefore both wrote to `chunks/_changes/changes-0.jsonl.gz` — the second overwrote the first's bytes while each manifest still recorded its own (now-divergent) SHA-256. `backup verify` exited 1 with `1 of N chunk(s) failed SHA-256 check`; chain restore exited 1 at `chain restore: incremental <id1>: stream chunks: chunk 0 (chunks/_changes/changes-0.jsonl.gz): backup: chunk SHA-256 mismatch`. Engine-agnostic (verified on both PG and MySQL in the v0.17.0 cycle) and backend-agnostic (writer code is shared between local-FS and S3/cloud). Fix: namespace each incremental's chunks under a per-Run subdirectory derived from the manifest's `CreatedAt` (`chunks/_changes/<unix_millis>/changes-<idx>.jsonl.gz`). `CreatedAt` is preferred over `BackupID` because `BackupID` depends on `EndPosition`, which is only known after the window closes — chunks need a stable namespace before the first write. The manifest's recorded `change_chunks[].file` path is the source of truth for reads, so chain restore + `backup verify` pick up the new shape with no other changes. Single-incremental chains written by v0.17.0 still restore cleanly post-fix because the readers follow whatever path the manifest recorded. Two new unit tests (`TestIncrementalBackup_TwoIncrementals_NoChunkCollision` pins distinct paths + SHA-256 fidelity across two Run calls; `TestChangeChunkPath_RunNamespaceShape` pins the path shape) plus a new PG integration test (`TestIncrementalBackup_PostgresChainRestore_TwoIncrementals` drives the full repro: full → inserts → incr1 → inserts → incr2 → verify chain → chain restore → confirm every row arrives on the target).

## [0.17.0] - 2026-05-07

Logical backups Phase 3.1 + 3.2 lands: incremental backups + chain-aware restore. Phase 3.3 (CDC handoff via `--position-from-manifest`) is the v0.17.1 follow-up; that release closes the "auto-resume CDC from a chain's terminal position" UX gap. v0.17.0 is the storage + restore plumbing, usable today via `sluice backup full → backup incremental → restore --from=<chain-url>` with a manual `sluice sync start --resume` to continue replication. Implementation supplement: `docs/dev/design-logical-backups-phase-3.md`.

### Added

- **Logical backups Phase 3.1 + 3.2: chained backups (`sluice backup incremental --since=<backup-id>`) + chain-aware restore.** The chunk that closes the resync-avoidance story for irrecoverable position loss. New CLI subcommand `sluice backup incremental` opens the source's CDC pump at the parent manifest's terminal position, streams events for a bounded window (`--window` time-bound + `--max-changes` count-bound, first-fired wins; window extends to the next TxCommit so the chain doesn't end mid-tx), and writes a chain-linked manifest under `manifests/incr-…json` plus serialised change chunks under `chunks/_changes/`. Manifest gains `Kind`, `BackupID`, `ParentBackupID`, `StartPosition`, `EndPosition`, `SchemaHash`, and `SchemaDelta` fields; pre-Phase-3 manifests treated as orphan fulls under the canonicaliser. Implementation supplement: `docs/dev/design-logical-backups-phase-3.md`.
- **`sluice restore --from=<chain-url>` chain detection.** The existing single-manifest restore path now lists `manifests/incr-…json` at the supplied URL; when any incremental manifests are found, dispatches to a new chain-aware orchestrator that walks `[full, incr_1, …, incr_N]` in order. Builds the chain via `ParentBackupID` linkage, validates: single full root, no branching, no cycles, no orphans, every incremental's `StartPosition` matches its parent's `EndPosition`. Cross-engine chain restore is refused loudly per the design doc's Phase 5+ deferral; same-engine chain restore applies schema deltas (AddTable creates the table, AlterTable replays ADD COLUMN via the new `ir.SchemaDeltaApplier` surface implemented on both PG and MySQL) then streams change chunks through the engine's idempotent `ChangeApplier.ApplyBatch` per ADR-0010.
- **`sluice backup verify` walks chains.** Re-checksums every chunk in the store across all manifests (the full's row chunks and every incremental's change chunks), so cron-style integrity probes cover the whole chain in one call.
- **`ir.SchemaDeltaApplier` interface** with `AlterAddColumn(ctx, table, cols)` — implemented on PG (`ALTER TABLE … ADD COLUMN IF NOT EXISTS …`) and MySQL (information-schema probe + `ALTER TABLE … ADD COLUMN`); same-engine column-add deltas apply cleanly during chain restore.
- **Schema-evolution capture within an incremental's window.** Source's recorded schema (parent manifest) vs end-of-window source schema is diff'd into typed `ir.SchemaDeltaEntry` slice on the incremental manifest; AddTable / DropTable / AlterTable kinds covered. Rename-shaped deltas (single drop + single add per table) are flagged as ambiguous and surface a "force fresh full + new chain" recovery message.

### Changed

- **`ir.Manifest.FormatVersion` stays at 1.** All Phase 3 fields are forward-compatible additions (older sluice ignores `Kind` / `ParentBackupID` / etc.; those manifests appear as orphan fulls when read by an older binary, which is the right degraded behaviour for incrementals nobody can chain anyway). The version bumps when a future change would break older readers.

### Phase 3 known limitations (Phase 3.3 follow-up)

- The full-backup writer doesn't yet record an `EndPosition` (= snapshot LSN/GTID) on its manifest; integration tests patch this in manually, and the first incremental against a v0.16.x full surfaces a clear "parent has no EndPosition; chain will start from CDC's current position" warning. Phase 3.3 will close this gap so chains rooted in v0.17.0+ fulls get position-from-snapshot for free.
- `sluice sync start --position-from-manifest=<chain-url>` (the CDC handoff flag that lets a sync stream resume from the chain's terminal position without re-bulking) is not in 3.1+3.2 — it's the next subagent's job, gated on a test cycle for the writer + restore work.
- PG `wal_keep_size` soft-warning pre-flight checks are not in 3.1+3.2 — Phase 3.3 territory.

## [0.16.1] - 2026-05-07

Two-bug patch from the v0.16.0 test cycle. v0.16.0 shipped logical-backups Phase 2 (cloud backends + resumable writer); both findings are operational papercuts on top of an otherwise-clean cloud-roundtrip surface — not data-correctness bugs, but they break the workflow shape v0.16.0 promised.

### Fixed

- **Bug 33 — `--target=s3://bucket/prefix` silently drops the path; chunks land at bucket root.** `gocloud.dev/blob.OpenBucket` consumes only the bucket name from the URL it receives — the path component after the bucket name is dropped without warning. v0.16.0's `BlobStore` therefore wrote every key to bucket root regardless of the URL's path. Multiple backups in one bucket collided at root and tripped the "completed backup already exists" guard. Fix: `BlobStore` now extracts the path-after-bucket at construction time, stashes it on the struct, and prepends it to every key in Put / Get / List / Exists / Delete; List results are stripped of the prefix so callers see paths relative to it (matching `LocalStore`'s contract). `file://` URLs are exempt — gocloud's fileblob driver treats the whole path as the bucket root, so no double-prefix needed. Affects all URL schemes (`s3://`, `gs://`, `azblob://`); pre-fix shape, manifest at `s3://bucket/backup-v0160` lands at `<bucket>/manifest.json`, post-fix it lands at `<bucket>/backup-v0160/manifest.json`. Three new unit tests pin the prefix join + List-strip + empty-prefix shapes via the `mem://` driver; integration test extended with a Bug-33-regression check that HEADs the expected key directly via the AWS SDK.

- **Bug 34a — No "resuming" log line emitted on resume detection.** v0.16.0's resume code ran silently; operators couldn't tell from the log whether a re-run had started fresh or picked up a partial. Fix: emit `INFO resuming from partial backup` with the destination descriptor + the prior-manifest's table count + the prior `created_at` timestamp at the point the orchestrator detects an `in_progress` manifest; emit `INFO resume plan` with the per-table fan-out (`tables_already_complete`, `tables_to_resume`) once the schema is in hand; emit `INFO skipping table — already complete in partial backup` per skipped table during iteration.

- **Bug 34b — Resumable writer's per-chunk skip wasn't actually wired up.** The Phase 2 design + v0.16.0 CHANGELOG promised "per-chunk skip via `BackupStore.Exists` + manifest's recorded SHA-256" — but the v0.16.0 implementation only checkpointed the manifest at table boundaries, so a kill mid-table forced the entire table to be re-bulked from scratch on resume. Fix: per-chunk manifest checkpointing (manifest is now committed after every chunk, not just every table), plus a pre-write skip path in `backupTable` that consults the prior manifest's `ChunkInfo` for the in-flight chunk index — if `BackupStore.Exists` reports the chunk path is still on the store and `chunkAlreadyMatches` confirms the recorded SHA-256, the orchestrator advances the row cursor over that chunk's rows without opening a writer or issuing a Put. Mid-table kills now leave a manifest with `Partial=true` on the in-flight table; the resume picks up at the next un-completed chunk. New `TableManifest.Partial bool` field signals partial-state to resume code (omitted on fully-complete entries; pre-v0.16.1 manifests treated as complete-by-default for backward compat). New `TestBackup_ResumePerChunkSkipsAlreadyUploadedChunks` unit test stages a partial manifest and verifies chunks 0..N (already uploaded with matching SHA-256) are skipped while chunk N+1 onwards is written. Two existing tests updated to account for the per-chunk Put; `TestBackup_ResumeSkipsAlreadyCompletedTables` injection point shifted from `failOn=3` → `failOn=4` (per-chunk + per-table checkpoints add one Put each).

## [0.16.0] - 2026-05-07

Logical backups Phase 2 lands: cloud backends (S3 + S3-compatible providers + GCS + Azure) atop the `BackupStore` interface that shipped in v0.15.0's Phase 1. Plus a resumable backup writer that picks up a partially-completed job at the next un-finished table. Implementation supplement: `docs/dev/design-logical-backups-phase-2.md`.

### Added

- **Cloud backends for `sluice backup` / `restore` / `backup verify` (Phase 2 of the logical-backups proto-ADR).** New `internal/pipeline.BlobStore` wraps `gocloud.dev/blob` and implements the same `ir.BackupStore` contract as Phase 1's `LocalStore`. CLI: pass `--target=s3://bucket/prefix/` to `backup full`, or `--from=s3://...` to `restore` / `backup verify`. URL schemes wired: `s3://` (AWS S3 + S3-compatible via `--backup-endpoint`), `gs://` (Google Cloud Storage, ADC creds), `azblob://` (Azure Blob, managed-identity / connection-string), and `file:///` (kept for parity with `--output-dir`/`--from-dir`). Multipart upload for large chunks is automatic in `gocloud.dev/blob`'s `s3blob` driver. Per-chunk SHA-256 integrity carries through unchanged from Phase 1; backups are stored gzipped (`*.jsonl.gz`). **Client-side encryption is NOT shipped — chunks land unencrypted at rest. Operators relying on at-rest encryption should use bucket-level SSE on the cloud side or filesystem-level encryption (LUKS / BitLocker / FileVault) on the local-FS side. Sluice-managed client-side AES-256-GCM remains a Phase 6 (KMS) deliverable per the original proto-ADR.**
- **`--backup-endpoint`, `--backup-region`, `--backup-path-style` flags** for S3-compatible providers — MinIO, Cloudflare R2, Backblaze B2, Wasabi, Tigris, DigitalOcean Spaces. Same flags also enable Archil's read-only S3 API for cross-environment restore-from-Archil flows. Each flag is rejected with a clear error if combined with a non-`s3://` URL scheme.
- **Resumable backup writer (NEW in Phase 2 scope).** Two complementary mechanisms so a partially-completed backup picks up where it left off rather than re-doing everything: per-chunk skip via `BackupStore.Exists` + manifest's recorded SHA-256 (skip if present and checksum-matches; overwrite if checksum-mismatches), and per-table progress checkpoints (manifest is updated atomically after each table completes). On a re-run against the same `--output-dir` / `--target`, the orchestrator detects the partial-state manifest and resumes from the next un-completed table. New `--force-overwrite` flag mirrors `--reset-target-data`'s friction tier for the case where the operator wants to discard the partial backup and start fresh.
- **`BackupStore.Exists(ctx, path) (bool, error)` interface method** added to `ir.BackupStore`, both engines (`LocalStore`, `BlobStore`) implement it. Internal — no operator-visible surface change beyond the resumable writer.

### Changed

- **`gocloud.dev/blob` added as a dependency.** Pre-implementation estimate was `~3-5 MB` binary growth even on local-FS-only builds; real measurement after wiring all four side-effect imports (`s3blob`, `gcsblob`, `azureblob`, `fileblob`) is `~46 MB` total binary growth (~38.5 MB → ~84.2 MB on linux/windows amd64). Larger than the optimistic estimate but in line with the pessimistic envelope. Acceptable for v1 — operators consuming sluice via container images won't notice the layer-cache cost; binary distribution sizes remain in the same order of magnitude as `kubectl` (~50 MB) and `terraform` (~110 MB). If footprint becomes a real concern (e.g. embedding sluice in a small operator binary), the path is build-tag gating per cloud or moving to native SDKs per backend; the `BackupStore` interface is unchanged either way.

### Documentation

- **`docs/dev/design-logical-backups-phase-2.md`** — implementation supplement to the original proto-ADR. Captures: the `gocloud.dev/blob` library pivot (revised from native `aws-sdk-go-v2`); Archil integration findings (their S3 API is read-only — `PutObject` / multipart return `MethodNotAllowed`; clean split between POSIX-mount writes via Phase 1's `LocalStore` and S3-API restore reads via Phase 2's `BlobStore`); resumable backup writer addition; backup-chain → CDC handoff design as Phase 3 acceptance criterion (MySQL `gtid_purged` is clean; PG needs maintained slot or generous `wal_keep_size`); and the backup-as-broker pattern as a Phase 4.5+ direction (decoupled source-target sync with backup storage as the message log — unlocks no-direct-connectivity, multi-region-no-VPN, fan-out, time-shifted-sync, and air-gapped scenarios).

## [0.15.1] - 2026-05-08

Single-bug patch from the v0.15.0 test cycle. v0.15.0's PG → PG sync-health source-side probe was non-functional — `lag_bytes` always reported "unavailable" and `--max-lag-bytes` thresholds never tripped on the very engine pair the feature was designed for.

### Fixed

- **Bug 32 — `sync health --max-lag-bytes` non-functional on PG → PG.** The lag-bytes calculation passed the persisted target Position's Token verbatim into `pg_wal_lsn_diff($1::pg_lsn, ...)`. PG positions are JSON envelopes (`{"slot":"...","lsn":"X/Y"}`), not bare LSN strings — PG rejected with `SQLSTATE 22P02`. The error landed in `source_probe_reason` (good loud-failure shape) but the headline alerting feature was unusable. The orchestrator was also reconstructing the target token from the truncated-for-display string rather than passing the full position through. Two-part fix: PG engine's `LagBytes` now extracts LSN transparently from either bare-LSN or JSON-envelope Token shapes via a new `extractPGLSN` helper; the orchestrator passes the full `StreamStatus.Position` through to `probeSource` instead of reconstructing it. 5 new regression tests cover bare LSN, JSON envelope, leading-whitespace tolerance, empty token, and malformed JSON. Surfaced by sluice-testing's v0.15.0 cycle.

## [0.15.0] - 2026-05-08

Three roadmap items + a verify analysis pass land together. The user's morning brief asked me to pick "the next 3 items on the roadmap" and tackle them — these are the result.

### Added

- **`sluice sync health` source-side position probe (Phase 2 of sync-health proto-ADR).** New optional `--source-driver` + `--source` flags on the health probe; when supplied, sluice opens a `SchemaReader` against the source, type-asserts to the new `ir.HealthReporter` interface, calls `SourceCurrentPosition()`, and surfaces source/target tokens + (for PG-only pairs) a byte-distance lag metric via `ir.BytesLagReporter` + `pg_wal_lsn_diff()`. New `--max-lag-bytes` threshold flag (PG-only, exit 1 on breach). Source-probe errors don't fail the target-side check — an unreachable source shouldn't break cron probes monitoring the target. MySQL `SourceCurrentPosition` returns `gtid_executed`; MySQL doesn't implement `BytesLagReporter` (GTID sets aren't byte-distance comparable).
- **Mid-stream add-table (Phase 1 MVP) — `sluice schema add-table TABLE --stream-id ID`.** Brings a new source table into an active CDC stream's scope without forcing a destructive `--reset-target-data` cycle. Phase 1 requires the stream to be drained first (`sluice sync stop --wait`); the orchestrator refuses cleanly if the stream's `stop_requested_at` is still set. After successful add the operator restarts via `sluice sync start --resume` and CDC picks up the new table from the stream's existing position — the applier's idempotent upsert handles the [persisted_LSN, snapshot_LSN] overlap on the new table. Implements `docs/dev/design-mid-stream-add-table.md`. New `pipeline.AddTable` orchestrator (mirrors `Migrator` shape); new `Postgres.Engine.AddPublicationTables` issues `ALTER PUBLICATION ... ADD TABLE` (additive; existing scope untouched; idempotent on partial-add re-run); MySQL participates with no engine surface change — the binlog already covers every table. Pipeline-side optional interfaces `publicationAdder` / `snapshotSlotOpener` / `slotDropper` so engines opt in structurally. Operator safeguards: refuses if no row exists for the supplied `--stream-id`, if the target table already has rows (`TableEmptyChecker` preflight, same shape as cold-start), or if the named table doesn't exist on the source. Typed-confirmation prompt mirroring `--reset-target-data`'s friction tier; `--yes` bypasses; `--dry-run` prints the plan without touching anything. Phase 2 (live add-table without the drain) is roadmap'd; Phase 1 covers the routine "developer ran `CREATE TABLE` and forgot to tell ops" case.
- **Logical backups, Phase 1 — full snapshot to local filesystem.** New `sluice backup full`, `sluice backup verify`, and `sluice restore` commands implement the MVP slice from `docs/dev/design-logical-backups.md`. Backup writes a JSON `manifest.json` plus one or more gzipped JSON-Lines chunk files under `--output-dir`; manifest carries the full IR schema, per-table row counts, and a per-chunk SHA-256. Restore reads the manifest, runs `translate.RetargetForEngine` so cross-engine restore (PG backup → MySQL target, etc.) succeeds, applies the schema and bulk-copies rows back through the existing `RowWriter` path, verifying each chunk's SHA-256 and row count along the way. `sluice backup verify` rehashes every chunk against the manifest without restoring — useful as a cron probe against archived backups. New types: `ir.BackupStore` (interface, designed for Phase 2 cloud backends from day one), `ir.Manifest`, `ir.TableManifest`, `ir.ChunkInfo`, plus tagged-union JSON envelopes (`ir.MarshalType` / `ir.UnmarshalType` / custom `Column.MarshalJSON`) so the IR's sealed `Type` / `DefaultValue` interfaces round-trip through standard `encoding/json`. New pipeline orchestrators: `pipeline.Backup`, `pipeline.Restore`, `pipeline.LocalStore`, `pipeline.VerifyBackup`. Per the proto-ADR, Phase 1 is intentionally local-FS only — cloud backends (S3 / GCS / Azure) are Phase 2 (`BackupStore` interface is ready), incremental backups are Phase 3, encryption is Phase 6.
- **`sluice verify --strict-hash` opt-in for SHA-256 sample-mode hashing.** Default stays MD5 (statistically sufficient for honest-data scenarios at any practical row count — see `docs/verify-vs-vitess-vdiff.md` for the collision math). `--strict-hash` switches sample-mode to SHA-256 for operators wanting an extra confidence margin or matching a compliance posture that requires SHA-256. New `ir.HashAlgorithm` enum on the `ir.SampleVerifier` interface; PG implements via the built-in `sha256()` (PG 11+ core, no pgcrypto needed); MySQL via `SHA2(..., 256)`. ~2× server-side hashing time vs MD5; difference is sub-second at sample-mode's typical sizes.

### Documentation

- **`docs/verify-vs-vitess-vdiff.md`** — operator-facing comparison of sluice's verify approach to Vitess's vdiff workflow. Covers what each tool does, when to reach for which, and the MD5 collision math (P(collision) ≈ 10⁻²¹ at 1B rows; effectively zero for honest data). Sourced via WebFetch against vitess.io docs and the vdiff `table_differ.go` source — vdiff uses direct value comparison (not hashing), streams every row in PK order, full-fidelity, heavy on multi-TB tables. Sluice verify offers count + statistical-sample comparison cheaper but less exhaustive; full-fidelity mode is planned (proto-ADR phase 3).

## [0.14.1] - 2026-05-08

Single-bug patch from the v0.14.0 test cycle (`session-reports/v0.14.0.md` in the sluice-testing companion repo). 5 of 6 focus areas passed clean — including the headline PlanetScale Oregon → Virginia online migration with verify-mode-as-accuracy-proof — but the v0.14.0 view-support Phase 1 had one emission bug that blocked PG materialized-view round-trip.

### Fixed

- **Bug 31 — PG materialized view emit produces `... ; WITH DATA;` syntax error.** PG's `pg_matviews.definition` catalog column returns the SELECT body with a trailing `;`. The v0.14.0 `emitCreateView` matview branch appended ` WITH DATA;` directly, producing `CREATE MATERIALIZED VIEW ... AS SELECT ...; WITH DATA;` which PG rejects with SQLSTATE 42601. Migrate exited with the orchestrator's "view dependency budget exhausted" error after 2 retries, leaving the matview uncreated. Regular views happened to "work" because PG silently tolerates the resulting `;;` (parses as no-op-then-empty-statement); same code path; same fix. The shared `trimTrailingSemicolon` helper now also strips trailing whitespace before/after the semicolon. Workaround pre-fix: `--exclude-view <matview-name>` or `--skip-views`. Four new regression tests pin the trim behavior.

## [0.14.0] - 2026-05-08

Three feature tracks land together: **sample-mode verify** (Phase 2 of the verify proto-ADR), **Prometheus `/metrics` listener** (Phase 2 of the sync-health monitoring proto-ADR), and **view support Phase 1** (schema-only round-trip from the schema-completeness proto-ADR). Plus a chunked-count fallback that makes `sluice verify` work cleanly against PlanetScale-MySQL tables larger than its per-query row-read budget. Validated end-to-end including PlanetScale Oregon → Virginia online migration with verify confirming 100% accuracy.

### Added

- **View support Phase 1 — schema-only round-trip for regular and materialized views.** Both schema readers populate `Schema.Views`: MySQL via `information_schema.views`, Postgres via `pg_views` (regular) and `pg_matviews` (materialized; tagged `Materialized=true`). Both schema writers emit views as a new Phase 6 of the simple-mode orchestrator (after constraints): `CREATE OR REPLACE VIEW` for regular views, `CREATE MATERIALIZED VIEW ... WITH DATA` for matviews so the matview is populated from the just-loaded target tables on cold-start. View-to-view dependency ordering uses a single-pass-with-up-to-2-retries policy — no SQL parser, surface a clear error if a view still fails after the retry budget. New CLI flags on `migrate` / `sync start` / `schema preview` / `schema diff`: `--include-view PATTERN`, `--exclude-view PATTERN`, `--skip-views`. New `ir.SchemaWriter.CreateViews` interface method (additive, all shipping engines implement). New `ir.SchemaDiff.ViewsMissing` / `ViewsExtra` / `ViewsMismatched` for view-level drift detection. Cross-engine view-body translation is deferred to Phase 3 — Phase 1 emits the source-dialect definition verbatim and relies on the loud-failure tenet for non-portable definitions. Materialized view CDC refresh is a Phase 2 follow-up.
- **`sluice sync start --metrics-listen ADDR` (Phase 2 of the sync-health monitoring proto-ADR).** Optional Prometheus-format `/metrics` endpoint runs alongside an active stream; companion to the existing one-shot `sluice sync health` probe. Off by default; opt-in. Hand-written exposition encoder (no new dependency on `prometheus/client_golang`); reads scrape-time data from `ListStreams` so the apply hot path is untouched. Metric set: `sluice_seconds_since_last_apply`, `sluice_stream_known`, `sluice_metrics_scrape_unix_seconds` — all gauges, labelled by `stream_id`. Plus a `/healthz` endpoint that returns 200 OK so monitoring stacks can distinguish "scrape target gone" from "scrape target up but reports zero streams." Bind failure at startup is fatal (operator misconfig shouldn't be silent).
- **`sluice verify --depth sample` (Phase 2 of the verify proto-ADR).** Per-table sampled-row content hashes alongside count comparison; closes the "did the row data come across, not just the count" gap that count-mode alone can't catch. Default 100 rows per table; `--sample-rows-per-table N` to raise; deterministic `--sample-seed` so source + target select the same row subset run-to-run. Server-side hashing via `MD5(CONCAT_WS('|', col::text, ...))`; merge-walk in the orchestrator detects three drift shapes: PK on source only (target-missing-row), PK on target only (target-extra-row), and PK on both with hash difference (content drift). New `ir.SampleVerifier` optional interface; both core engines implement.
- **Same-engine constraint on sample mode** — sample-mode requires `source.Name() == target.Name()` since server-side text rendering of values differs cross-engine (MySQL TINYINT(1) → 0/1 vs PG BOOLEAN → t/f, etc.). Cross-engine sample is deferred to a future phase that adds client-side canonicalization. The orchestrator surfaces a clear error pointing operators at `--depth count` for cross-engine verification.
- **MySQL chunked-count fallback for PlanetScale large tables.** Pre-fix, `sluice verify` against a PlanetScale-MySQL source with > ~100K rows could hit the per-query row-read budget and fail. Now: when the table has a single integer PK, MySQL `ExactRowCount` splits the count across PK ranges of 50K rows each (default), summing partial counts. Cost: `⌈rows / 50000⌉ + 1` queries, well under PS's per-query budget. Tables without a single-int PK fall back to single-shot `SELECT COUNT(*)`; documented limitation.

### Fixed

- **`--output FILE` error messages** previously prefixed "preview:" regardless of which command (`schema preview`, `schema diff`, `verify`, `sync health`) invoked the shared atomic-write helper. Renamed prefix to "atomic output:" which describes the helper's actual responsibility and is correct regardless of caller. Cosmetic; surfaced by the v0.12.0 + v0.13.0 test cycles.

### Changed

- **`ir.SchemaWriter` interface gained `CreateViews(ctx, *Schema) error`** as part of view-support Phase 1. Additive change — both shipping engines (MySQL, Postgres) implement; out-of-tree engines that satisfy this interface need to add a method (no-op for engines without view support is acceptable).
- **`ir.View` type pre-staged in v0.13.x is now wired end-to-end** through readers, writers, pipeline, CLI, schema diff, and schema preview. `Schema.Views` is populated by both shipping engines.

## [0.13.0] - 2026-05-07

Companion to v0.12.0's `sluice verify` count-mode MVP — adds the **liveness side** of the user's "100% confidence" goal. Where verify covers data-integrity, `sluice sync health` covers liveness (is the sync still ticking?). Together they close the no-Fivetran-silent-stop pain shape on both axes.

Plus operator-facing polish: extra-on-target reporting on verify, integration tests for verify, the README troubleshooting matrix, the Vitess VStream troubleshooting runbook, and 6 new proto-ADRs capturing the design space for the next round of substantive feature work.

### Added

- **`sluice sync health` command (probe MVP).** Companion to `sluice verify` from the sync-health monitoring proto-ADR (`docs/dev/design-sync-health-monitoring.md`). Probes a target's `sluice_cdc_state` for the supplied `--stream-id` and computes wall-clock seconds-since-last-apply; compares against `--max-stale-seconds` threshold; structured exit code (0 healthy / 1 stale / 2 op error) integrates with cron / alertmanager / blackbox-exporter / GitHub-Actions-CI pipelines. `--format text|json`; `--output FILE` for atomic write. **MVP scope** — exposes only target-side state (what `ListStreams` already carries); source-side position comparison + true lag-events / lag-seconds metrics follow with the new `ir.HealthReporter` interface. Closes the cron-friendly "is the target still ticking?" probe gap, which is the load-bearing operator concern (Fivetran-stops-silently shape).
- **`sluice verify` reports tables present on target but absent from source.** Surfaced informationally in the new `VerifyResult.ExtraOnTarget` slice + the `TablesExtraOnTarget` summary count + a section in the text output. Does NOT count as mismatch (operators with shared targets often have other-app tables alongside sluice-managed ones; flagging would produce false-positive alerts). Text output nudges to `sluice schema diff` for structural-drift reconciliation.
- **Integration tests for verify** (`internal/pipeline/verify_integration_test.go`) — four real-DB tests cover happy path (PG→PG), intentional drift on target, extra-on-target reporting, and MySQL→MySQL clean. Run under `-tags=integration` in CI on every push.
- **FK edge-case test coverage** — six new unit tests across both engines (`TestEmitAddForeignKey_SelfReferential`, `TestEmitAddForeignKey_CompositePK`, `TestEmitAddForeignKey_AllOnDeleteActions` for both PG and MySQL). Pin self-ref FK shape, composite-PK FK shape, and every supported `ir.FKAction` keyword. No code changes; tests just pin behaviors per `design-schema-completeness.md`.

### Documentation

- **`docs/vitess-vstream-troubleshooting.md`** — operator runbook for sluice users running against PlanetScale MySQL when their sync is showing lag or has stopped advancing. Top three VStream delay causes characterized with code-path citations (replica-tablet replication lag; tablet throttler indirect impact; internal Vitess operations including failovers, reshards, PS deploy requests). Plus what's new in Vitess 24's binlog streaming surface and an honest "PS exposure timeline unclear" assessment.
- **Public README rewritten** for an operator scanning to decide "does this fit my use case" in 30 seconds. Engine matrix, "vs alternatives" comparison, decision-tree table for command selection, links to operator-facing docs first.
- **README troubleshooting matrix** — quick-look for the most common operator symptoms (migrate failed mid-phase, sync slot lost, verify reports mismatch, sync health stale, etc.) and the first-look response.

### Design / planning

- **Six new proto-ADRs** capturing the design space for the user's "100% confidence" goal:
  - `design-sluice-verify.md` — count / sample / full data-integrity verification (count MVP shipped in v0.12.0; sample + full follow).
  - `design-sync-health-monitoring.md` — probe MVP + Prometheus listener + per-table metrics phases (probe MVP shipped in v0.13.0).
  - `design-logical-backups.md` — full + incremental backups to local-FS + cloud storage, with restore tooling. MVP recommendation: local-FS Phase 1.
  - `design-apache-arrow-integration.md` — Parquet writer engine + format interop. Conditional yes, gated on logical-backup format choice.
  - `design-schema-completeness.md` — FK edge-case test coverage + view support Phase 1 (read+emit). FK tests landed in v0.13.0; view support is a future implementation chunk.

### Compatibility

- **No breaking IR changes.** `ir.Verifier`'s new `ExtraOnTarget` field on `VerifyResult` is additive; existing JSON consumers ignore unknown fields.
- **No CLI-breaking changes.** New subcommand (`sluice sync health`) only.
- **Behaviour change on `sluice verify`** — extra-on-target tables now surface in output as informational entries. Operators piping the JSON output will see a new top-level `extra_on_target` array (empty when no extras exist).

## [0.12.0] - 2026-05-07

`sluice verify` lands as a first-class operator surface — the count-mode MVP from the proto-ADR (`docs/dev/design-sluice-verify.md`). Direct delivery on the user's overarching "100% confidence that all data has been copied + synced" goal: operators can now ask "is the target row-count-equal to the source?" without writing the SQL themselves, integrate with cron / alertmanager / CI gates via the structured exit code, and machine-consume the JSON output for monitoring pipelines.

Sample-mode and full-mode (proto-ADR phases 2 + 3) follow per the sequencing in the design doc; count-mode is the cron-friendly probe operators run most frequently.

### Added

- **`sluice verify` command (count-mode MVP).** New CLI subcommand. Runs `SELECT COUNT(*)` per table on both sides, compares, surfaces mismatches with deltas. Exit-code shape mirrors `schema diff`: 0 clean, 1 mismatch, 2 operational error. Same flag surface as `migrate` / `sync start` (DSN + driver + filters); reusable against the operator's existing `sluice.yaml`. `--depth count` (only supported value in v0.12.0; `sample` and `full` planned). `--format text|json` for machine consumption. `--output FILE` for atomic write.

- **`ir.Verifier` optional engine interface.** Engines opt-in by implementing `ExactRowCount(ctx, table) (int64, error)`. Distinguished from existing `RowCounter` (which returns approximate counts via `pg_class.reltuples` / `information_schema.tables.table_rows` for ETA hints) — verify needs authoritative counts, so we pay the full-table-scan cost. MySQL and Postgres engines both implement on their `SchemaReader` (which already holds the DB connection). Engines without `Verifier` cause `sluice verify` to fail loud with a clear "not supported" operational error.

- **`pipeline.Verifier` orchestrator.** Mirrors `Differ` shape. Reads both schemas via `SchemaReader`, type-asserts to `Verifier`, runs `ExactRowCount` per table, builds `VerifyResult` with per-table outcomes + summary counters. Renders text or JSON. Tables present on source but absent on target surface as SKIPPED (reported in the result; not flagged as mismatches — they're a structural concern that `schema diff` covers).

### Notes

- **Why not include `sample`/`full` in v0.12.0?** Per the proto-ADR's sequencing — count-mode alone closes the most common Fivetran-style "did I lose rows?" probe at the cheapest cost. Sample mode adds N random rows × content hashing per table; full mode adds full-table content-hash + bisection on mismatch. Each has its own engineering surface (sampling determinism, cross-engine value canonicalization, bisection chunk size). Shipping count-mode as the MVP gets operators the cron-friendly probe immediately while sample/full follow on real-world demand signal.
- **CDC-position-aware verification deferred.** When sluice is verifying a continuously-syncing target, the source can have new rows the target hasn't applied yet — count mismatch is expected, not an error. The proto-ADR's open question #1 covers the design (verify against the target's tracked source position). Out of scope for v0.12.0; the MVP is best run against migration-completed targets, not in the middle of CDC catch-up.

### Compatibility

- **No breaking IR changes.** `ir.Verifier` is purely additive (new optional interface). Existing engine surfaces unchanged.
- **No CLI-breaking changes.** New subcommand only.
- **Behaviour change: none.** Verify is a read-only inspection tool; it doesn't modify either side.

## [0.11.3] - 2026-05-07

Three-bug patch from the v0.11.2 real-world test cycle (see sluice-testing's `session-reports/v0.11.2.md`). All three bugs were in v0.11.x's translator emission paths — places where the catalog work was supposed to cover but a code path bypassed the translator or matched the wrong syntactic form.

### Fixed

- **Bug 28 — `DEFAULT (UUID())` now translates to `DEFAULT gen_random_uuid()` on cross-engine MySQL→PG migrate.** Pre-fix the DEFAULT-expression emit path bypassed the translator entirely (generated columns / CHECK constraints / index expressions all ran through it; only DEFAULTs didn't). Cross-engine migrates against schemas with `DEFAULT (UUID())`, `DEFAULT (RAND() * 100)`, or `DEFAULT (DATE_ADD(...))` failed loud on PG with `function uuid() does not exist` / `function rand() does not exist` / etc. Operator workaround was `--expr-override` (v0.10.0); the translator now handles it without intervention.

- **Bug 29 — `DEFAULT (RAND() * 100)` now translates to `DEFAULT (RANDOM() * 100)`.** Same root cause as Bug 28; same fix. Both bugs surfaced in the same test cycle because they're the same code path.

- **Bug 30 — `DATE_ADD(d, INTERVAL N DAY)` in a generated column now translates to PG's `(d + INTERVAL 'N day')` quoted-magnitude form.** MySQL's `information_schema.generation_expression` canonicalizes `DATE_ADD(...)` to the operator form `(d + interval N day)` when read back — the function-call rewrite added in v0.11.1 never fired on the canonicalized text because the function call was gone. Pre-fix the unquoted operator form emitted verbatim and failed loud with `syntax error at or near "7"`.

### Implementation notes

- New `Dialect` field on `ir.DefaultExpression` mirrors the existing `Column.GeneratedExprDialect` / `CheckConstraint.ExprDialect` fields. MySQL schema reader sets `Dialect="mysql"`; PG schema reader sets `Dialect="postgres"`. PG writer's `emitDefault` now routes `DefaultExpression` through `translateDefaultExpr` (same dialect-gating shape as `translateGeneratedExpr` / `translateCheckExpr`). The `ExprContext` passed on the DEFAULT path is the zero value — bool-idiom rewrites are no-ops here because DEFAULT expressions are evaluated per-row at INSERT time, not over other column values.
- New `rewriteIntervalLiteral` operates on the operator-form `INTERVAL <int> <unit>` directly (vs. the function-form `DATE_ADD(...)` rewrite from v0.11.1). Same supported singular-unit set: MICROSECOND, SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, YEAR. QUARTER, compound units (`HOUR_MINUTE` etc.), and non-literal magnitudes pass through under the loud-failure tenet.
- ADR-0016's cumulative-scope table extended with the new operator-form INTERVAL row and a DEFAULT-expression-scope row noting the gate. v0.11.3 caveats section documents the per-rule reasoning.

### Compatibility

- **No CLI-breaking changes.** Same flags, same defaults.
- **IR change**: `ir.DefaultExpression` gained a `Dialect string` field. Existing callers constructing `ir.DefaultExpression{Expr: ...}` with named fields continue to compile (zero value = "" = "verbatim" — same as pre-fix behaviour for that single field). Positional struct literals (rare; not present in the codebase) would need `, ""` appended.
- **Behaviour change for cross-engine MySQL→PG migrate.** The three bug repros in sluice-testing now translate cleanly. Operators using `--expr-override` to work around these specific defaults can drop those overrides.

## [0.11.2] - 2026-05-07

Single-bug patch from CI integration. v0.11.0's CHARSET/COLLATION cross-engine diff regressed `TestDiff_PostgresToMySQL` — three subtests started failing because the diff began surfacing bogus drift on every PG→MySQL retargeted column (UUID/Inet/Macaddr/Array). The Integration job has been red on every push since v0.11.0; the failure was visible in CI but not gated on (Integration is one of the required checks but the existing PR-merge flow had been bypassing it for tag-driven release commits).

### Fixed

- **Cross-engine retargeted columns no longer surface as charset/collation drift in `sluice schema diff`.** The diff comparison now treats empty source-side charset/collation as "no opinion" rather than as a sentinel value to compare against. Three legitimate cases are covered: source is Postgres (PG doesn't expose per-column charset via `information_schema`), source column is non-character (Integer/JSON/etc.), and source column was retargeted from a PG-native type by `internal/translate.RetargetForEngine` (the retarget rewrites UUID→Char(36), Inet/Cidr→Varchar(45), Macaddr→Varchar(30) but doesn't carry charset/collation since the source type never had one).

  When the source DOES declare a charset/collation, the comparison stays strict: a target missing the source's declared charset still surfaces as drift. The asymmetry is intentional — source/expected is authoritative, so empty-source means "any actual is fine," populated-source means "actual must match." Operators wanting strict bidirectional compare can suppress with `--ignore-charset-collation` (already plumbed) or rely on the matched-pair behaviour, which is unchanged.

  Pre-fix: every PG→MySQL diff on retargeted columns flagged drift like `accounts.account_uuid: expected="" actual=""` (the ColumnDiff's type strings were empty because the types matched post-retarget; only the new charset/collation comparison was triggering the mismatch). Three integration subtests caught it: `TestDiff_PostgresToMySQL/json_captures_only_in-band_drift_after_retarget`, `text_reports_drift_sections`, `ignore-extras_suppresses_extra-table_diff`. New unit test `TestDiffSchemas_EmptySourceCharsetCollationNoDrift` pins the empty-as-no-opinion behaviour so the regression can't return.

## [0.11.1] - 2026-05-07

Continuation of the proactive-translator cycle started in v0.11.0. Eight more rewrites across the catalog's high- and medium-priority tiers, picked for "biggest leverage per LOC, fewest gotchas." All additive, no IR or interface changes, no operator-facing flags. Same loud-failure-on-unrecognized-shape policy as v0.11.0.

### Added

- **Translator catalog second batch (MySQL → PG).** Eight new rewrites across eight rule families:
  - `RAND()` (argless) → `RANDOM()`. Direct rename. The seed form `RAND(seed)` has no single-call PG equivalent and falls through.
  - `UUID()` (argless) → `gen_random_uuid()`. PG 13+ baseline assumed (matches sluice's existing baseline). The MySQL schema reader's UUID type-canonicalization path may already cover most real-world cases via the type mapping; this rule covers expression-level uses (CHECK constraints, text columns with UUID-shaped defaults, etc.).
  - `ISNULL(x)` → `(x IS NULL)`. MySQL's function form returns int (1 or 0); PG's IS NULL operator returns boolean. For `COALESCE(ISNULL(x), 0)` patterns the existing v0.10.1 aggressive `::int` cast picks up the bool result automatically once this rewrite has fired. Standalone `ISNULL` in an integer-typed generated column body still needs `--expr-override`.
  - `REGEXP_REPLACE(x, pat, repl)` (3-arg) → `REGEXP_REPLACE(x, pat, repl, 'g')`. PG defaults to first-match-only; MySQL defaults to all-match. Without the global flag, generated columns and CHECK constraints would silently produce different output. The 4-arg MySQL form (with position) has different semantics from PG's 4-arg form (with flags) and falls through verbatim. Regex-dialect divergence (ICU vs POSIX) is the operator's responsibility under the loud-failure tenet.
  - `INSTR(s, sub)` → `STRPOS(s, sub)`. Same arg order, direct rename.
  - `LOCATE(sub, s)` → `STRPOS(s, sub)`. **Argument order is FLIPPED** between the two functions (MySQL `LOCATE` takes needle-then-haystack; PG `STRPOS` takes haystack-then-needle). The arg-swap is load-bearing — getting it wrong silently searches the haystack inside the needle. The 3-arg form `LOCATE(sub, s, start)` has no clean single-call PG equivalent and falls through.
  - `DATE_ADD(d, INTERVAL n unit)` / `DATE_SUB(d, INTERVAL n unit)` → `(d + INTERVAL 'n unit')` / `(d - INTERVAL 'n unit')`. Common in TTL / `expires_at` patterns. Singular MySQL units only — `MICROSECOND`, `SECOND`, `MINUTE`, `HOUR`, `DAY`, `WEEK`, `MONTH`, `YEAR`. Compound units (`HOUR_MINUTE`, `DAY_HOUR`, etc.), `QUARTER` (no PG equivalent), and non-literal counts fall through verbatim.
  - `DATE_FORMAT(x, '<fmt>')` → `TO_CHAR(x, '<pg_fmt>')`. Format-string token mapping covers the common `%Y/%m/%d/%H/%i/%s` family and friends (24 token mappings total). Literal text in the format gets PG's `"..."` double-quote wrapping; punctuation passes through. **Strict mode**: any `%X` token outside the supported set causes the entire DATE_FORMAT call to fall through verbatim — silent partial translation would produce wrong output without raising an error. **Immutability caveat**: PG's `TO_CHAR` is `STABLE` not `IMMUTABLE`, blocking STORED generated columns; CHECK / DEFAULT / VIRTUAL bodies still benefit.

  ADR-0016 cumulative-scope table extended with the new rows; v0.11.1 caveats section captures per-rule gotchas (PG version baseline for UUID, regex-dialect divergence, DATE_ADD compound-units, DATE_FORMAT immutability + strict-mode token policy, etc.).

## [0.11.0] - 2026-05-07

Closes the v0.10.x reactive-bug cycle and opens the proactive-translator cycle. v0.10.x's bug bundle was driven by real-world testing surfacing translation gaps one at a time; v0.11.0 inverts the loop by mining sqlglot, pgloader, and dolt's function registry for the next batch of likely culprits and landing the highest-priority rewrites pre-emptively. CHARSET/COLLATION cross-engine diff finishes the schema-diff feature surface (the `--ignore-charset-collation` flag was plumbed but inert since v0.8.0). Two design docs capture the design space for the heavier roadmap items (mid-stream add-table, multi-source aggregation) so the implementation pass starts from a structured doc, not a blank page.

### Added

- **Translator catalog top-5 rewrites (MySQL → PG).** Eight rules across five families, all sourced from `docs/dev/translator-coverage.md`'s high-priority tier:
  - `NOW()` / `CURRENT_TIMESTAMP()` / `LOCALTIMESTAMP()` / `LOCALTIME()` (argless) → bare `CURRENT_TIMESTAMP` / `LOCALTIMESTAMP` keyword. PG accepts the keyword form (no parens) and rejects `NOW()` outright; bare-keyword is also what PG emits when reading back its own DEFAULTs, so the rewrite normalises round-trips. The `NOW(6)` precision form falls through verbatim — the bare-keyword form doesn't accept precision at parse time and the operator escape (`--expr-override` from v0.10.0) covers the rare case.
  - `UNIX_TIMESTAMP(x)` → `EXTRACT(EPOCH FROM x)::bigint`. The explicit `::bigint` cast preserves MySQL's storable-as-integer semantics; PG's `EXTRACT(EPOCH FROM …)` returns `double precision` natively. Argless `UNIX_TIMESTAMP()` expands to `EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::bigint`. Two-arg / fractional-precision forms fall through verbatim. **Caveat:** PG treats `extract(epoch from timestamp)` as `STABLE`, not `IMMUTABLE`, which blocks STORED generated columns; the rewrite still helps for CHECK / DEFAULT / VIRTUAL bodies, and STORED bodies fall back to `--expr-override`.
  - `FROM_UNIXTIME(x)` (single-arg) → `TO_TIMESTAMP(x)`. The two-arg form `FROM_UNIXTIME(epoch, fmt)` returns a formatted string in MySQL and has no clean PG equivalent — falls through verbatim under the loud-failure tenet.
  - `CHAR_LENGTH(x)` / `CHARACTER_LENGTH(x)` → `LENGTH(x)`. PG's `LENGTH(text)` counts characters, matching MySQL's `CHAR_LENGTH`. The reverse direction (MySQL `LENGTH(x)` byte length → PG `OCTET_LENGTH(x)`) is a separate rule with different semantics and not part of this batch — it requires column-type context to fire safely.
  - `LCASE(x)` → `LOWER(x)` and `UCASE(x)` → `UPPER(x)`. Direct synonyms.
  - `SUBSTR(x, …)` / `MID(x, …)` → `SUBSTRING(x, …)`. PG accepts the comma form `SUBSTRING(x, start, length)`; both 2-arg and 3-arg shapes round-trip. The single-arg `SUBSTR(x)` form (which PG's `SUBSTRING` doesn't accept) falls through verbatim.

  ADR-0016's cumulative-scope table extended with the new rows; v0.11.0 caveats section captures the immutability + format-string + precision-form gotchas in one place.

- **CHARSET/COLLATION cross-engine diff.** PG schema reader now reads per-column collation via `pg_attribute.attcollation` (joined to `pg_collation` for the name); `ir.DiffOptions.IgnoreCharsetCollation` becomes load-bearing instead of inert; `diffColumn` compares charset/collation as separate `ColumnDiff` fields (`ExpectedCharset`/`ActualCharset`, `ExpectedCollation`/`ActualCollation`); `stripCharsetCollation` suppresses the drift at compare time when the flag is set, dropping columns whose only drift was charset/collation. Renderer emits MySQL `MODIFY COLUMN` and PG `ALTER COLUMN` suggestions.

- **`docs/dev/translator-coverage.md`** — research catalog with 30 candidate MySQL→PG rewrite rules from sqlglot's parser/generator, pgloader, and dolt's function registry. Each entry carries the MySQL form, the PG equivalent, semantic notes, citation, and an importance rating measured by how often the construct appears in real-world DDL bodies (not general usefulness). The "How to land a rule" section at the bottom documents the existing implementation pattern. Closes the "what about idioms we haven't seen?" thread.

- **`docs/dev/design-mid-stream-add-table.md`** (proto-ADR). Lays out the design space for handling `CREATE TABLE` on a CDC source mid-stream: trigger options (manual subcommand vs. auto-detect from DDL events), snapshot-LSN coordination strategies, per-engine differences, four-phase implementation plan. Reference for when real-world testing surfaces the need.

- **`docs/dev/design-multi-source-aggregation.md`** (proto-ADR). N-sources → one-target. Identifies three shapes (sharded, microservices, multi-master), scopes out multi-master, recommends N-processes with `--target-schema` for collision handling. Reference for the same reason.

### Changed

- **`docs/dev/roadmap.md` swept.** "Next up" #1 (CHARSET/COLLATION) moved to "Recently landed" since v0.11.0 closes it. OSS-hygiene goreleaser entry dropped — `.goreleaser.yaml` + `release.yml` have been live since earlier in the cycle. v0.10.x feature-wave summary added to "Recently landed" with the eight tagged + untagged commits between v0.9.x and v0.11.0. New "Next up" #1 reframed around continuing through the catalog's remaining high-priority rules.

## [0.10.4] - 2026-05-06

CI workflow cost optimization. No sluice runtime change; no IR or interface change. Tagged separately so the workflow shift has a versioned anchor and the corresponding `branch-protection.md` doc update has a clear "applies as of" reference.

### Changed

- **CI matrix is conditional on trigger.** The `test` and `build` jobs ran on `[ubuntu-latest, macos-latest, windows-latest]` for every push and PR. macOS-latest costs ~10× Linux per-minute and Windows ~1.7× Linux; on a frequent-push cadence those two platforms drove the bulk of the daily Actions bill. New shape: push to main / pull_request runs Linux-only; push of a `v*` tag or a manual `workflow_dispatch` from the GitHub UI's "Run workflow" button runs the full 3-OS matrix. Implementation uses a single workflow file with `fromJSON()`-conditional matrix selected at workflow-parse time. Operators wanting cross-platform verification before merging a sensitive PR can dispatch the workflow manually.
- **Branch-protection required-checks list trimmed.** `docs/dev/branch-protection.md` updated to drop `Test (macos-latest)` / `Test (windows-latest)` / `Build (macos-latest)` / `Build (windows-latest)` from the required set — they no longer run on PRs and would otherwise permanently block merges. Operators with existing branch protection per the older doc need to remove those four checks before further PR merges.

## [0.10.3] - 2026-05-06

Single-bug patch from PostGIS testing. Bug 27 (VStream POINT mis-parse) defers to a later release because it needs VStream test infrastructure.

### Fixed

- **Bug 26 — MySQL geometry SRID is now preserved on cross-engine emit.** The MySQL schema reader didn't extract `information_schema.columns.srs_id`, so a `POINT NOT NULL /*!80003 SRID 4326 */` source column landed on PG as `geometry(POINT, 0)` — the SRID silently dropped. Any spatial query on the target that depended on the coordinate system (distance, contains, etc.) returned wrong results.

  Fix: read `srs_id` from `information_schema.columns` and thread it through `columnMeta.SrsID` into `ir.Geometry.SRID`. The PG schema writer already honoured `Geometry.SRID` (no change needed), so the cross-engine emit path now produces `geometry(POINT, 4326)` on PG matching the MySQL source.

  **Schema diff coverage extends automatically:** `ir.Geometry.String()` already includes the SRID in its rendering (`Geometry[POINT,SRID=4326]`), so the diff's existing type-string comparison surfaces SRID mismatches as drift once both sides carry SRID consistently. No separate diff change needed.

  MySQL 8.0+ baseline assumed. Pre-8.0 MySQL servers don't expose `srs_id` in `information_schema.columns`; sluice's supported MySQL baseline is already 8.0.

### Deferred

- **Bug 27 — VStream POINT bytes mis-parsed.** MySQL's internal storage prepends a 4-byte SRID prefix before OGC WKB; the vanilla MySQL protocol strips this, but VStream doesn't. Sluice's WKB decoder reads `0xE6` (low byte of SRID 4326) as the byte-order flag and fails. The fix needs VStream test infrastructure (the `integration vstream` build tag); deferred to a later patch where it can land with the test that demonstrates it.

## [0.10.2] - 2026-05-06

Two test-unblocking surface additions from `FUTURE-TESTS.md`. Both small and well-bounded; no behaviour change for operators not opting in.

### Added

- **`--slot-name NAME` flag on `sluice sync start`** (Item C). Operator-supplied replication-slot name for engines that have a slot concept (Postgres). Default unchanged (`sluice_slot`); operators set per-instance to run multiple concurrent sluice instances against the same source — without distinct slot names they'd collide on the hard-coded default. Engines without slots (MySQL: binlog stream is the slot) silently ignore the flag.

  **Sluice-prefix convention:** sluice prepends `sluice_` if the supplied name doesn't already start with it. `--slot-name shard_a` creates `sluice_shard_a`; `--slot-name sluice_shard_a` is idempotent. The convention lets operators find every sluice slot with `pg_replication_slots WHERE slot_name LIKE 'sluice\_%'` for cleanup, audits, and disambiguation from other tools' slots (Debezium, native logical replication subscribers, etc.). The resolved name surfaces in the orchestrator's INFO log so operators can correlate against `pg_replication_slots`.

  Implementation: two new optional engine surfaces — `ir.CDCReaderWithSlotOpener` and `ir.SnapshotStreamWithSlotOpener` — let engines accept a slot-name parameter without breaking the existing `OpenCDCReader` / `OpenSnapshotStream` signatures. The orchestrator type-asserts on these and falls back to the default methods when the engine doesn't implement them. Postgres implements both.

- **`migrate --dry-run` now reports per-table row counts** (Item H). The dry-run output's per-table line gains a `row_count` attribute populated via the existing `ir.RowCounter` interface (MySQL: `information_schema.tables.table_rows` / `SHOW TABLE STATUS`; Postgres: `pg_class.reltuples`). Best-effort: engines that don't implement `RowCounter`, or per-table counts that fail (permissions, etc.), surface as `row_count=-1` with a Warn-level log line so operators can distinguish "unavailable" from "empty". The count uses the throwaway dry-run-only RowReader handle and doesn't touch the bulk-copy path.

## [0.10.1] - 2026-05-06

Two narrow patches from v0.10.0 real-world testing. Bug 23's enum-cast placement fix uncovered Bug 25 — the cast itself triggers PG's "generation expression is not immutable" error because `enum_in()` is STABLE not IMMUTABLE. Bug 17's hand-coded bool-returning detector kept missing real-world expression shapes; v0.10.1 drops the detector and trusts the column-type signal instead.

### Fixed

- **Bug 25 — enum-typed STORED generated columns now emit as `TEXT` + table-level `CHECK`** instead of `(body)::"enum_type"`. PG rejects the cast inside a generated-column body because `enum_in()` is STABLE not IMMUTABLE, and STORED generated bodies must be IMMUTABLE. VIRTUAL doesn't help (PG 18+ forbids user-defined types in VIRTUAL gen cols). Sluice sidesteps by emitting the column as TEXT (no enum type, no cast) and adding a table-level `CHECK ("col" IN ('a','b','c'))` constraint that enforces the value-list. Mirrors the existing SET → TEXT[] + CHECK fallback. CREATE TYPE is skipped for these columns; non-generated enum columns still use the native PG enum type. Loses the named enum type on the target side but always works — matches sluice's "translate, don't wrap in target-side functions" philosophy.

- **Bug 17 — int-context COALESCE rewrite drops the bool-detector gate.** v0.9.1 / v0.9.2 gated the `::int` cast on a hand-coded `isBoolReturning` detector that recognised bare bool idents, comparisons, `IS NULL`/`IS NOT NULL`, keyword forms, and parenthesised wrappers. v0.10.0 real-world testing surfaced expression shapes the detector missed (function calls returning bool, `AND`/`OR` chains, `NOT` prefixes, `EXISTS` subqueries) and each produced a fresh bug report. v0.10.1 drops the detector entirely: when the outer column is integer-typed, the non-literal side of `COALESCE(<expr>, <int_lit>)` is cast to `::int` unconditionally. Safe because the column must produce int — the cast either helps (bool→int), is a no-op (already int), or fails loudly at apply on a non-numeric expression (loud-failure tenet preserved). Cost: one extra `::int` token in the emitted DDL on already-int columns. Benefit: every bool-returning shape now translates correctly without operator intervention. ADR-0016 updated.

## [0.10.0] - 2026-05-06

The expression-translator escape hatch. v0.8.x / v0.9.x's reactive cycle (operator hits a bug → we add a rule) reaches its planned next stage: instead of dropping a column when sluice's translator doesn't recognise an idiom, the operator can supply target-dialect expression text directly via `--expr-override` (CLI) or `expression_mappings:` (YAML). Sluice emits the override verbatim and the translator stays out of the way. The pattern-matching rule set keeps growing for the common cases; `--expr-override` covers everything else.

### Added

- **`--expr-override TABLE.COLUMN=EXPRESSION` flag** on `migrate`, `sync start`, `schema preview`, and `schema diff`. YAML form: `expression_mappings: [{table:, column:, expression:}]`. CLI flags wholesale-replace the YAML config when both are supplied (same precedence as `--type-override`). The expression part can contain arbitrary characters including additional `=` signs, single quotes, parens — only the first `=` after the column name is the separator.

  Strict validation at config-load time: overrides referencing unknown tables, unknown columns, or columns that aren't generated columns surface as clear errors before any DSN is dialed. Silent passthrough would mask the operator-typo case ("why didn't my override fire?"); the strict check makes those typos visible immediately.

  The override applies via a new `internal/translate.ApplyExpressionOverrides` pass that runs alongside `ApplyMappings`. Mechanism: replace `Column.GeneratedExpr` with the override text and clear `Column.GeneratedExprDialect`. The cleared dialect tag tells the writer-side translator that no rewrite is needed — the column flows through the same code path same-dialect expressions take. No special override-aware code paths anywhere downstream.

  v0.10.0 scope: generated-column bodies only. CHECK constraints, index expressions, and DEFAULT expressions don't have an override surface yet; if real-world testing surfaces the need, each gets its own override type with the same shape. ADR-0016 extended with an "Added in v0.10.0" subsection covering the design.

### Changed

- `pipeline.Migrator`, `pipeline.Streamer`, `pipeline.Previewer`, `pipeline.Differ` all gain an `ExpressionMappings []config.ExpressionMapping` field. Existing callers that don't set it keep working — the field defaults to nil and the override pass is a no-op on nil/empty input.
- `internal/config.Config` gains an `ExpressionMappings []ExpressionMapping` field. Existing YAML configs without an `expression_mappings:` key are unchanged.

## [0.9.2] - 2026-05-06

Two narrow patches surfaced by v0.9.1 real-world testing. Bug 23's enum-cast emit had a placement error — the cast landed outside the GENERATED parens where PG's grammar rejects it; Bug 17's bool-returning detector had been too narrow, missing the comparison operators (`<`, `>`, `<=`, `>=`) and keyword forms (`LIKE`, `BETWEEN`, `IN`) that real-world generated-column bodies use. Both fixes are localised and additive; the rest of v0.9.1 stands.

### Fixed

- **Bug 23 placement — enum cast moves inside the GENERATED parens.** v0.9.1 emitted `GENERATED ALWAYS AS (body)::"X_enum" STORED`, which PG rejects because `::` binds tighter than the AS clause's parens. The cast now lands as `GENERATED ALWAYS AS ((body)::"X_enum") STORED` — wrapping the body in inner parens before the cast and keeping the whole thing inside the outer GENERATED parens. Schema-writer change only; the translation logic is unchanged.

- **Bug 17 detector breadth — `coalesce(<bool>, 0)` now recognises more bool-returning shapes.** v0.9.1's `hasTopLevelCompareOp` only handled `=`, `!=`, `<>` — equality and inequality. Real-world generated-column bodies also use `<`, `>`, `<=`, `>=`, `LIKE`, `BETWEEN`, and `IN`, all of which return bool. v0.9.2's detector recognises every operator in that set plus the `IS [NOT] NULL` form (already covered) and `IS DISTINCT FROM`. Each is matched with appropriate token-boundary discipline so identifier substrings (e.g. a column named `between_us`) don't trigger false positives.

## [0.9.1] - 2026-05-06

Patch release closing the three remaining ADR-0016 translator gaps that v0.9.0 testing surfaced. All three are residuals of bugs the v0.8.0 / v0.9.0 batches partially closed; together they unblock end-to-end migration on the two real-world schemas (`schema_example_01` 555 tables, `schema_example_02` 138 tables) the sluice-testing companion repo uses for stretch validation.

### Fixed

- **Bug 16 residual — `CAST(x AS CHAR(N) [CHARSET y] [COLLATE z])` translates on cross-engine emit.** v0.9.0 routed index expressions through the ADR-0016 translator but the translator itself didn't yet recognise MySQL's CAST-to-CHAR form with charset/collate decorations. PG's grammar rejects both decorations and the CHAR(N) target's blank-padding semantics differ from MySQL's. The new `rewriteCASTCharCharset` rule rewrites `CAST(x AS CHAR(N) [...])` to `CAST(x AS VARCHAR(N))` (matching MySQL's no-padding semantics) and `CAST(x AS CHAR)` (no length) to `CAST(x AS TEXT)`. Other cast targets (DECIMAL, DATE, etc.) pass through verbatim.

- **Bug 17 residual — `coalesce(<bool_returning>, <int_lit>)` for integer-typed columns.** v0.9.0 expanded the COALESCE rewrite to recognise bool-returning sub-expressions; that path converted the int literal to a bool, which is the right answer when the outer column is BOOLEAN. For an integer-typed generated column (e.g. a MySQL `tinyint(1)` source widened to `smallint` via `--type-override`), the int literal is the right answer and the bool side needs to cast to int instead. New `ExprContext.OuterColumnIsInteger` flag flips the rewrite direction; `translateGeneratedExpr` sets it based on the column's IR type (`ir.Integer` → flag set). Comparison rewrites (the other half of the bool-idiom pass) stay bool-context only since the int-context comparisons (`<int_lit> = <bool_ident>`) already work via PG's implicit-cast handling.

- **Bug 23 — STORED GENERATED column body returning text into an enum-typed target gets the enum cast.** The original v0.8.1 framing was about column DEFAULT casting; real-world testing refined the diagnosis: the failing case is a STORED GENERATED column with a `CASE` expression returning enum-valued text literals. The PG enum-cast emit now also wraps generated-column bodies for enum-typed columns: `GENERATED ALWAYS AS (CASE … END)::"<enum_type>" STORED`. Works for any text-returning shape (`CASE`, `COALESCE`, simple literal), not just `CASE`. Mirrors the `DEFAULT 'value'::"<enum_type>"` cast already emitted for non-generated columns.

## [0.9.0] - 2026-05-06

Operator quality-of-life + cross-engine type-edge audit + OSS-hygiene starter + four follow-ups from v0.8.1 real-world testing. `sync stop --wait` closes the operator-coordination gap surfaced by v0.8.0's stretch-testing of ALTER windows; new TIMESTAMP-precision integration tests audit the cross-engine boundary that Bug 19's TZ fix opened to scrutiny; `CONTRIBUTING.md` and `docs/dev/release-template.md` formalise the conventions that have been carried in conversation memory across the v0.x ramp. The follow-ups close Bug 16 (index-expression translation), Bug 17 (bool-returning sub-expressions in COALESCE), Bug 22 (`schema preview` and `schema diff` now also auto-exclude PlanetScale `_vt_*` tables), plus a new Bug 23 (MySQL `DEFAULT ('value')` parens form not getting the PG enum cast).

### Added

- **`sluice sync stop --wait`** (extends ADR-0025). Blocks the CLI until the running streamer confirms it's drained gracefully; `--timeout` (default 5 minutes) bounds the wait. Useful for ALTER coordination — `sync stop --wait && alter-source.sh && sync start` now runs the ALTER only after the streamer has confirmed it drained, instead of operators polling `sync status` or `pgrep`-ing the streamer process.

  Implementation rests on a flag-clearing convention: the streamer already calls `applier.ClearStopRequested(streamID)` at startup (Bug 11 fix from v0.3.2). v0.9.0 adds a second clear point — after a stop-signal-driven graceful drain, the streamer clears the flag again as the very last step of `Streamer.Run`. The CLI's `--wait` polls `ReadStopRequested` (1s cadence) until the flag clears and exits success; on timeout it exits non-zero with a clear message and the stop request remains in place so the streamer continues draining in the background.

  The streamer only clears the flag on stop-signal-driven exits, not on Ctrl-C / outer-ctx cancels — `pollStopSignal` now exposes an optional `*atomic.Bool` that the streamer reads after `dispatchApply` returns to decide whether the exit was the operator's stop request or something else. Without `--wait` the behaviour is unchanged; against an older streamer that doesn't clear the flag, `--wait` blocks until `--timeout` and then surfaces a clear "did not complete drain" message.

- **TIMESTAMP / DATETIME precision integration tests**
  (`internal/pipeline/migrate_temporal_precision_integration_test.go`).
  Bug 19 (v0.8.0) closed the silent-corruption hole on the TZ axis;
  the precision axis was previously covered only by unit tests on the
  IR's `Precision` field. The new integration tests exercise
  end-to-end behaviour across `DATETIME(0/3/6)` /
  `TIMESTAMP(0/3/6)` (MySQL→PG) and `TIMESTAMP(0/3/6)` /
  `TIMESTAMPTZ(0/3/6)` (PG→MySQL), seeded with
  `12:34:56.123456` so each precision tier surfaces a distinct
  truncated value. Round-trips assert wall-clock equivalence within
  the column's declared precision; the PG→MySQL case also pins the
  expected target column types (`TIMESTAMP` → `DATETIME`,
  `TIMESTAMPTZ` → `TIMESTAMP`) so a future schema-emit rewire would
  surface as a schema-shape failure rather than silently passing on
  equivalent values.

- **`CONTRIBUTING.md` release-process section + `docs/dev/release-template.md`** — formalise the GitHub release-notes structure (Highlights / Fixed / Compatibility / Who-needs-this) that's been carried in conversation memory across the v0.x ramp, plus the `chore: cut vX.Y.Z` commit + annotated-tag pattern. The release-template doc carries section-by-section guidance with examples drawn from the v0.7.0 / v0.8.0 release notes.

### Fixed

- **Bug 16 follow-up — MySQL functional/expression index bodies translate cross-engine.** v0.8.0 unwalled the schema reader for functional indexes; v0.9.0 closes the emit-side gap. The MySQL schema reader now tags each index expression with its source dialect (mirroring the existing tags on generated columns and CHECK constraints), and the PG DDL writer routes index expressions through the ADR-0016 translator on emit. A MySQL `CREATE INDEX ... ((json_unquote(json_extract(meta, '$.k'))))` now lands on PG as `CREATE INDEX ... (((meta->>'k')))` instead of failing at apply time with "function json_unquote(json) does not exist". Same-dialect and untagged expressions pass through verbatim.

- **Bug 17 follow-up — COALESCE with a bool-returning sub-expression rewrites correctly.** v0.8.0 handled `COALESCE(<bool_ident>, 0)` where the bool side was a bare column reference. v0.9.0 extends the rewrite to recognise bool-returning sub-expressions: comparisons (`a = b`, `a <> b`, `a != b`), `IS NULL` / `IS NOT NULL` tests, and parenthesised wrappers around them. Real-world report: a generated column whose body included `coalesce((some_bool_returning_expr), 0)` failed to land on PG even though every direct bool-column case was handled. Arithmetic and other non-bool sub-expressions are still left alone (loud-failure tenet preserved).

- **Bug 22 follow-up — engine-default exclusions now apply to `schema preview` and `schema diff` too.** v0.8.1's Bug 22 fix wired the `_vt_*` Vitess shadow-table auto-exclude into `Migrator.Run` and `Streamer.Run`, but the merge step was missing from `Previewer.Run` and `Differ.Run`. Both now run the same merge before invoking `applyTableFilter` — so `sluice schema preview` and `sluice schema diff` against a PlanetScale source no longer surface `_vt_HOLD_*` / `_vt_hld_*` tables in the output. Operator-supplied `--include-table` short-circuits the merge as in the migrate/sync path.

- **Bug 23 — MySQL `DEFAULT ('value')` parens-form enum default now gets the PG enum cast.** MySQL 8.0+ stores `DEFAULT ('pending')` (with parens) as an expression default — `information_schema.columns.extra` carries the `DEFAULT_GENERATED` flag, which the schema reader translates to `ir.DefaultExpression` rather than `ir.DefaultLiteral`. The PG enum-cast emit was gated only on `DefaultLiteral`, so the parens form skipped the cast and PG rejected with "column X is of type Y_enum but default expression is of type text". The cast now also fires on `DefaultExpression` whose body is shape-equivalent to a single-quoted string literal (the parens form's only legal content for an enum default); true-expression defaults like `current_setting(...)` are still left uncast (the cast wouldn't be safe).

## [0.8.1] - 2026-05-06

Patch release. Closes a CI integration test regression introduced in v0.8.0 (test-only, no behaviour change for users), and finishes the Bug 22 auto-exclude story for vanilla-MySQL connections to PlanetScale endpoints.

### Added

- **PlanetScale Vitess hostname auto-detect for the vanilla MySQL driver.** v0.8.0's Bug 22 fix auto-excluded `_vt_*` Vitess shadow tables when `--source-driver=planetscale`. A vanilla MySQL operator pointing at a PlanetScale endpoint with `--source-driver=mysql` (a legitimate configuration — they get binlog CDC instead of VStream) still had to add `--exclude-table='_vt_*'` manually. v0.8.1 closes that gap with a DSN-keyed hostname sniff at orchestrator startup. The two PlanetScale MySQL hostname suffixes are recognised:

  - `*.connect.psdb.cloud` (public PlanetScale MySQL)
  - `*.private-connect.psdb.cloud` (AWS PrivateLink)

  When matched, the engine merges `_vt_*` into the orchestrator's exclude list — same shape as the existing Bug 22 path. Operator-supplied `--include-table` short-circuits the merge; operators who explicitly want `_vt_*` tables override that way. A structured INFO log surfaces the merged exclusion list at startup so the new behaviour is visible.

  No connection round-trip is involved — the sniff parses the DSN string and matches against the documented hostname suffixes, avoiding the auth/network failure modes that an `@@version_comment` probe would introduce. Non-PlanetScale Vitess deployments (Slack-style, custom domains) still need a manual `--exclude-table='_vt_*'`; if a non-PlanetScale Vitess user reports the gap, the connection-probe path can be added then.

  PG-side PlanetScale hostname suffixes (`*.pg.psdb.cloud`, `*.private-pg.psdb.cloud`) are documented in code for future symmetry but no-op today — PlanetScale Postgres isn't Vitess-backed and has no `_vt_*` shadow tables. The same hostname-sniff machinery would slot into the PG engine's own `DefaultTableExcluder` if that need ever surfaces.

### Changed

- `ir.DefaultTableExcluder.DefaultExcludePatterns` signature gained a `dsn string` parameter so engines can return DSN-derived defaults in addition to flag-derived ones. Out-of-tree engines implementing the optional surface (none expected at this stage of the project) need to update the method signature.

### Fixed

- **CI: `TestMigrate_MySQLToPostgres_CheckBoolIdiom` referenced columns the test schema didn't have.** v0.8.0's bool-idiom integration test (Bug 17) ended with three stray INSERT validations on `email` / `status` columns left over from a sibling test. The bool-idiom test's schema only has `id` + `is_active`, so those INSERTs failed with `column "email" of relation "accounts" does not exist`. Removed the stray block; the test now ends after the bool-CHECK enforcement assertions where it was meant to. Test-only fix, no behaviour change.

## [0.8.0] - 2026-05-06

Schema-diff release plus seven real-world bug fixes from v0.7.0 testing. Headline addition is `sluice schema diff` (ADR-0029): drift detection between sluice's expected target shape and the schema actually present, with text + JSON output, copy-paste-ready ALTER suggestions, and CI-friendly exit codes. The diff round picked up cross-engine type retargeting plus default / generated-expression / CHECK comparison along the way. Seven bug fixes — including Bug 19's silent TIMESTAMP corruption on non-UTC hosts, Bug 20's cross-engine resume dispatch, Bug 21's `idle in transaction` snapshot tx blocking source ALTERs, and Bug 22's auto-exclusion of Vitess `_vt_*` shadow tables — closed the remaining real-world gaps the v0.7.0 stretch testing surfaced.

### Added

- **`sluice schema diff` (ADR-0029).** Drift detection between what
  sluice would produce on a target (source schema → translation
  pipeline → expected target shape) and the schema that's actually
  there. Reads both sides through the existing `SchemaReader`
  surface — no new engine API; every engine that already implements
  `SchemaReader` (today: PG, MySQL) gets diff support immediately.
  Renders text (default; per-table sections with copy-paste
  ALTER/DROP suggestions and a preamble noting they're starting
  points, not verified migration scripts) or JSON (stable shape for
  CI consumers) and supports `--output FILE` with the same atomic
  temp+rename semantics as `schema preview`. Filter and mapping
  flags mirror `schema preview` so the diff and preview pipelines
  stay aligned. CI-friendly exit codes: 0 on no drift, 1 on drift
  detected (suitable for failing a `schema-drift.yml` job), 2 on
  operational error like a bad DSN — distinct so CI scripts don't
  conflate "the gate failed" with "we couldn't run the gate."
  `--ignore-extras` suppresses extra-on-target entries (useful when
  the target hosts other applications' tables); `--ignore-charset-
  collation` is plumbed for the v1.x extension when those fields
  land in the IR. Out of scope per the ADR: column reordering,
  index column ordering, FK constraint name normalisation, and
  trigger/function/view comparison — surfacing those as drift
  produces too much noise for too little operator value, and
  reconciliation is a different tool's job (Atlas, sqitch).

- **Schema diff: defaults, generated expressions, CHECK constraints,
  per-column ALTER rendering.** Three categories originally listed
  as out-of-scope in ADR-0029 are now compared because the IR
  already carries the underlying fields and the comparison shape is
  additive on `ColumnDiff` / `TableDiff`: column defaults
  (`ExpectedDefault` / `ActualDefault`, with a small cross-engine
  equivalence map for the common pairs like `now()` ↔
  `CURRENT_TIMESTAMP`; mismatches outside the map are flagged
  low-confidence rather than silently equated), generated-column
  expressions (verbatim string comparison after trim — engines don't
  support in-place generated-expr ALTERs, so the renderer emits a
  comment plus a DROP+ADD reconciliation hint), and table-level
  CHECK constraints (matched by name; unnamed CHECKs are dropped
  from the comparison to avoid cross-engine spelling false
  positives). Renderer fills the actual column type, default, and
  generated expression on `ALTER TABLE ... ADD COLUMN` suggestions
  for missing-on-target columns via a new optional
  `ir.ColumnDDLPreviewer` interface (implemented on both PG and
  MySQL); the prior `-- TYPE` placeholder remains as a defensive
  fallback for engines that don't implement it.

- **Cross-engine type-policy retarget on schema diff.** New
  `internal/translate.RetargetForEngine` rewrites the source-side
  schema's PG-native IR types (`UUID`, `Inet`, `Cidr`, `Macaddr`,
  `Array`) to the MySQL-storage IR shapes (`Char(36)`, `Varchar(45)`,
  `Varchar(30)`, `JSON[binary]`) the target engine's DDL writer
  would land them on. Wired into `pipeline.Differ.Run` between
  `ApplyMappings` and the target schema read so cross-engine
  `sluice schema diff` no longer flags every translated column as
  drift when the target storage is exactly what sluice would
  produce. Same-engine pairs and unknown engine pairs return the
  schema unchanged. Operator-supplied `--type-override` mappings
  take precedence (override replaces the IR type via
  `ApplyMappings`; the retarget pass only fires on still-source-
  native types). v0.8.0 scope is the PG→MySQL direction.

### Tests

- Cross-engine integration test for `sluice schema diff`
  (`internal/pipeline/diff_cross_engine_integration_test.go`) booting
  a PG source + MySQL target. Asserts the retarget pass collapses
  the noisy cross-engine type drift so only the deliberately
  injected drift surfaces (narrowed VARCHAR, missing column, extra
  table on target). Also covers JSON / text rendering and
  `IgnoreExtras` semantics on the cross-engine path.

### Fixed

- **Bug 16 — MySQL functional / expression indexes wall the schema
  reader.** `information_schema.statistics` rows for
  functional/expression indexes (MySQL 8.0.13+) carry
  `COLUMN_NAME = NULL` and put the actual expression in the
  `EXPRESSION` column. The reader scanned `column_name` into a plain
  `string`, so the first such index produced
  `converting NULL to string is unsupported` and aborted the
  schema-read for the whole database — a hard wall blocking every
  operation against production schemas that use the feature.

  Fix: scan into `sql.NullString`, add `EXPRESSION` to the SELECT,
  and route NULL-column rows into a new `ir.IndexColumn.Expression`
  field (run through the same `normalizeMySQLExpressionText`
  identifier-quote scrubbing the reader applies to generated columns
  and CHECKs). MySQL and Postgres DDL writers render expression
  entries as parenthesised expression text. Cross-engine MySQL→PG
  emit is best-effort: portable expressions round-trip; non-portable
  ones still fail loudly on `CREATE INDEX`. Regression guards:
  `TestEmitCreateIndex/expression_entry`,
  `TestEmitCreateIndex/mixed_plain_and_expression_entries` (unit) and
  `TestSchemaReader_FunctionalIndex` (integration).

- **Bug 17 — MySQL bool-idiom CHECK / generated expressions reject
  on PG (ADR-0016 addition).** MySQL's tinyint(1)→PG BOOLEAN mapping
  silently broke CHECK constraints and generated columns that compared
  the column against an integer literal — `0 <> is_active`,
  `is_active = 1`, `coalesce(is_active, 0)` — because PG's strict
  typing rejects integer↔boolean comparisons that MySQL accepts via
  implicit coercion. Real-world report: 3 of 138 tables on
  `schema_example_02` blocked by this until columns were dropped
  manually.

  Fix extends the writer-side translator (`translateExprForPG`) with
  an `ExprContext` carrying the table's bool-mapped column names.
  When the rewrite recognises `<int_lit> <op> <bool_ident>` /
  `<bool_ident> <op> <int_lit>` (op ∈ `=`, `!=`, `<>`; lit ∈ `0`, `1`)
  or `COALESCE(<bool_ident>, <int_lit>)` and the symmetric form, the
  int literal is replaced with `false` / `true`. `IFNULL` is renamed
  to `COALESCE` by an earlier pass so it falls in too. Anything else
  passes through verbatim — same loud-failure tenet as the rest of
  ADR-0016. Same-engine emits unaffected (the translator only fires
  when the IR's dialect tag differs from the writer's). New
  integration test `TestMigrate_MySQLToPostgres_CheckBoolIdiom`
  verifies a real `CHECK (0 <> is_active)` lands on PG and enforces
  correctly. ADR-0016 updated with an "Added in v0.8.0" subsection.

- **Bug 18 — `--reset-target-data` left orphaned PG enum types.**
  The destructive-recovery path (ADR-0023) dropped tables and the
  bookkeeping row; enum types created during a partially-failed
  cold-start survived and caused the next reset's `CREATE TYPE` to
  fail with "type X already exists" until operators manually
  `DROP TYPE`d. Fix extends the reset path with a
  `dropSchemaTypes` pass that runs after the table drops, walking
  the source schema for `ir.Enum` columns and emitting
  `DROP TYPE IF EXISTS "schema"."<table>_<col>_enum" CASCADE`. PG-
  only via the new optional `ir.SchemaTypeDropper` interface; MySQL
  embeds enum values inline and is unaffected. Idempotent across
  partial failures. New integration test
  `TestMigrate_ResetTargetData_DropsOrphanEnumTypes` simulates the
  stuck state, runs reset, and asserts the next migrate succeeds
  with rows landing.

- **Bug 19 — silent TIMESTAMP corruption in MySQL→PG CDC on non-UTC
  hosts.** TIMESTAMP values delivered through CDC drifted by the host
  process's local UTC offset (e.g. seven hours early on a US/Pacific
  host during DST). Cold-start bulk copy was correct, CDC was not, so
  the destination silently held the wrong instant for every row
  updated post-cold-start until an operator happened to compare
  source and target epochs. Loud failures beat silent corruption;
  this one snuck past v0.7.x.

  Two distinct corruption surfaces landed under the same symptom:

  - **CDC binlog path.** MySQL's binlog wire format encodes
    TIMESTAMP as a UTC seconds-since-epoch integer, but go-mysql's
    `decodeTimestamp2` builds the resulting `time.Time` via
    `time.Unix(sec, ...)` whose `Location` defaults to `time.Local`.
    With the parser's `ParseTime=false` setting (sluice's configured
    path), `fracTime.String()` then formats that instant in
    process-local TZ unless
    `BinlogSyncerConfig.TimestampStringLocation` is pinned. The
    formatted wall-clock string flowed into sluice's `decodeTime`,
    which parses naked datetime strings as UTC — silently
    re-interpreting a PT wall clock as a UTC instant.

  - **Cold-start / database/sql path.** A second, latent surface:
    if the MySQL session's `time_zone` inherits the server's
    `default_time_zone` (often `SYSTEM`, which follows the host),
    MySQL converts the column's UTC-stored TIMESTAMP into the
    session TZ for the wire format. The driver — running with
    `cfg.Loc=UTC` — re-interprets that wall-clock as UTC, producing
    the same offset. This wasn't observed because test containers
    default to UTC; production deployments against MySQL servers
    with non-UTC `default_time_zone` would have hit it.

  Fix lives at the connection-protocol layer in two places — no
  Go-side runtime-TZ conversion that could drift with deployment
  changes: the binlog client sets
  `BinlogSyncerConfig.TimestampStringLocation = time.UTC`, and
  every database/sql connection injects `time_zone='+00:00'` into
  `cfg.Params` so the driver issues `SET time_zone='+00:00'`
  immediately after handshake (covers schema reader, row reader,
  row writer, CDC schema cache, change applier, migration-state
  store). DATETIME is unaffected (its binlog encoding is the
  broken-down date/time directly with no TZ conversion).
  Regression guard: `TestCDCReader_TimestampNonUTCHost`
  (integration tag) pins `time.Local` to America/Los_Angeles,
  inserts a TIMESTAMP, and asserts the value comes back as the
  same UTC instant from both the cold-start `RowReader` and the
  CDC stream's update event.

- **Bug 21 — PG snapshot transaction held source-table locks for the
  entire CDC lifetime, blocking ALTER on the source.** The PG cold-
  start path opens a snapshot transaction (`SET TRANSACTION SNAPSHOT
  '<name>'`) on a pinned SQL connection so bulk-copy reads see a
  consistent view. Pre-fix, that transaction stayed open as `idle in
  transaction` for as long as the SnapshotStream was alive — i.e.
  for the entire CDC streaming phase, which on a long-running sync
  is hours or days. Every snapshotted table held an
  `AccessShareLock`, blocking any concurrent `ALTER TABLE` on the
  source. Real-world report: a 310-second `idle in transaction` queue,
  ALTER waiting behind it, both unblocked the moment sluice exited.

  Fix splits the SnapshotStream cleanup into two phases via a new
  `ir.SnapshotStream.ReleaseRowsFn` (and the corresponding
  `ReleaseRows()` method): the streamer calls `ReleaseRows` after
  bulk-copy completes, which COMMITs the snapshot transaction and
  closes the import-side connections (the pinned SQL conn + the
  slot-creation replication conn) without disturbing the CDC reader.
  The CDC reader runs on its own connection, and the slot's logical
  position is independent of the exporting transaction, so CDC
  continues seamlessly. `Close()` remains the catch-all cleanup and
  is idempotent with `ReleaseRows` — calling both is safe; calling
  only `Close()` still works (it invokes the release path internally
  if not already done). MySQL implementations don't need this surface
  (per-session snapshot, no shared exporter), and the field is
  optional. Regression guard:
  `TestSnapshotStream_ReleaseRowsClosesSnapshotTx` (integration
  tag) asserts `pg_stat_activity` shows zero `idle in transaction`
  sessions after release, that an ALTER TABLE on the source
  succeeds without blocking, and that CDC continues delivering
  events post-release.

- **Bug 22 — Vitess `_vt_*` shadow tables included by default.**
  Vitess maintains internal lifecycle tables (`_vt_HOLD_*`,
  `_vt_PURGE_*`, `_vt_EVAC_*`, `_vt_DROP_*` in legacy naming;
  `_vt_hld_*` / `_vt_prg_*` / `_vt_evc_*` / `_vt_drp_*` plus a
  trailing underscore in the post-PR-14613 scheme) that aren't user
  data and shouldn't appear in publication or bulk-copy. v0.7.0
  silently included them, generating quiet write churn against the
  target with no operator-visible signal. Workaround was a manual
  `--exclude-table='_vt_*'`.

  Fix: new optional `ir.DefaultTableExcluder` engine surface lets
  engines declare baseline exclusion patterns; the orchestrator
  merges them into the operator's filter at the start of `Migrator`
  / `Streamer` `Run`. The PlanetScale flavor opts in with the
  `_vt_*` pattern (covers both legacy and post-PR-14613 naming).
  Operator-supplied `--include-table` short-circuits the merge —
  if the operator explicitly opts into a precise table list, engine
  defaults don't override it. Vanilla MySQL returns no defaults
  (`_vt_*` is a Vitess namespace, not an upstream MySQL one;
  vanilla MySQL operators on Vitess-backed servers can still
  pass `--exclude-table='_vt_*'` manually — auto-detect of the
  underlying server flavor is out of scope for v0.8.0). The merged
  exclusions are surfaced via a structured INFO log at
  orchestrator startup so operators see what's being filtered.
  Regression guards:
  `TestEffectiveTableFilter_MergesEngineDefaults` (covers all four
  merge paths: empty, exclude-mode, include-mode short-circuit,
  duplicate-pattern dedup) and
  `TestDefaultExcludePatterns_PlanetScale` (pins the flavor's
  declared default).

- **Bug 20 — cross-engine warm-resume dispatch on the wrong driver.**
  `sluice sync start --resume` failed on
  `--source-driver=planetscale --target-driver=postgres` because the
  persisted CDC position came back from the target's
  `sluice_cdc_state` tagged with the applier's (target's) engine
  name, so the source CDC reader's decoder rejected it as belonging
  to the wrong engine. v0.1.0's Bug 2 fix patched the symmetric
  same-family PS↔MySQL pair by widening MySQL's decoder; it didn't
  generalise to truly cross-engine pairs. Fix is a re-stamp at the
  streamer level: every persisted position picked up via
  `applier.ReadPosition` has its `Engine` field set to
  `s.Source.Name()` before reaching the source CDC reader. All four
  pairs (MySQL↔MySQL, MySQL↔PG, PG↔PG, PG↔MySQL, plus the
  PlanetScale flavor) round-trip cleanly without per-pair special-
  casing. The from-now sentinel (`Engine="" Token=""`) is preserved.
  The `--reset-target-data --yes` workaround is no longer needed for
  cross-engine zero-downtime resumes. New unit tests
  `TestRetagPositionForSource_*` (helper-level pinning across the
  four pairs) and `TestStreamer_WarmResume_CrossEngine_Retag`
  (end-to-end-shape pin via recording reader/applier).

## [0.7.0] - 2026-05-05

Performance round 2 + ergonomics + reliability follow-ups. Four new ADRs (0025 graceful-drain stop, 0026 LOAD DATA INFILE writer, 0027 source-tx CDC batching, 0028 memory-bounded streaming). Closes Bug 12 (MySQL CDC silent-stall on temporal columns) and Bug 15 (CLI sync-stop drain in the warm-up window) — both classified during v0.6.0 testing as the remaining reliability gaps from the v0.4.0 night soak.

### Added

- **MySQL `LOAD DATA LOCAL INFILE` row-writer (ADR-0026).** Vanilla
  MySQL bulk-copy now streams TSV over `LOAD DATA LOCAL INFILE` via
  go-sql-driver's `RegisterReaderHandler` mechanism (no real file
  written, no `?allowAllFiles=true` needed). Typically 5–10× faster
  than the parameter-bound multi-row `INSERT` path on wide-row
  tables. The `BulkLoadLoadDataInfile` capability constant has been
  declared on vanilla MySQL since v0.1; this release brings the
  implementation up to the declaration. PlanetScale stays on
  BatchedInsert (the flavor doesn't allow `LOAD DATA LOCAL INFILE`).

  Per-call fallback to BatchedInsert when (a) the server has
  `local_infile=OFF` (default on MySQL 8.0+) — one structured WARN
  surfaces the speedup-pending hint, and (b) the table contains a
  geometry column (the SRID-prefixed WKB wire format isn't
  expressible in a column-only LOAD DATA). The TSV serializer
  escapes the four MySQL LOAD DATA defaults
  (tab/newline/CR/backslash/NUL) and emits `\N` for NULL. Statement
  uses `CHARACTER SET binary` plus per-column `SET col = CONVERT(@cN
  USING utf8mb4)` for VARCHAR/TEXT/SET/JSON columns to round-trip
  binary blobs and JSON cleanly in the same statement.

- **Source-transaction-boundary aware CDC batching (ADR-0027).** New
  `ir.TxBegin` / `ir.TxCommit` change variants surface source-side
  transaction boundaries to the applier. Postgres emits from
  `BeginMessage` / `CommitMessage` (with `StreamStart` / `StreamStop`
  mapping to boundaries for the streaming-in-progress chunked path);
  MySQL emits from `BEGIN` QueryEvent / `XIDEvent`. The batched
  applier (`ApplyBatch`) flushes on `TxCommit` so a 5000-row source
  transaction commits as one 5000-row target transaction instead of
  being split by the row-count cap. The cap remains the upper bound;
  idle flush, channel close, and Truncate flush behave as before.
  Empty source transactions produce no target commits (lazy-tx-open
  absorbs them). Per-change `Apply` treats boundary events as
  no-ops; the table filter explicitly bypasses them so a filter
  never drops a boundary signal. Position-and-data atomicity
  (ADR-0007) and idempotency (ADR-0010) preserved. Closes the
  follow-up explicitly deferred from ADR-0017.

- **`--max-buffer-bytes N` (ADR-0028).** Default `67108864` = 64 MiB,
  on `sluice migrate` and `sluice sync start`. Bounds per-batch
  buffered memory by total byte size in addition to the existing
  row-count caps. Wide-row workloads (TEXT / BYTEA / JSON at MB
  scale) no longer have to manually retune `--bulk-batch-size` /
  `--apply-batch-size` to control heap usage; the byte cap fires
  whichever way is tighter. The cap is a soft target — a single row
  larger than the cap still applies. Implemented in the bulk-INSERT
  writer, idempotent-INSERT writer, and CDC `ApplyBatch` paths for
  both engines via the new `ir.MaxBufferBytesSetter` optional
  surface; the COPY-protocol and LOAD DATA paths are streaming and
  unaffected. The byte-counting helper (`approximateRowBytes`) was
  hoisted from the pipeline to `internal/ir/bytes.go` so engine
  packages can reuse it.

- **PG-native types auto-emit on MySQL targets.** `Inet` / `Cidr`
  (PG → MySQL) auto-emit as `VARCHAR(45)`; `Macaddr` as
  `VARCHAR(30)`; `Array` as `JSON` (matches the v0.5.0 Bug 14 fix
  where array values are serialized as JSON for the writer).
  Pre-v0.7.0 these returned an error pointing operators at
  `--type-override`; the auto-emit removes the toil for every
  PG→MySQL migration that touches these types. Operators wanting
  strict syntactic validation still use `--type-override` to a
  custom shape with their own CHECK constraint; the schema-preview
  command (ADR-0024) surfaces the auto-emit choice so it isn't
  silent. Closes roadmap §6.

- **Throughput tuning guide** (`docs/throughput-tuning.md`).
  Operator reference for the knobs that matter at scale —
  `--apply-batch-size`, `--bulk-parallelism`, network compression
  (MySQL `compress=true`, PG TLS+gss settings), and
  `--max-buffer-bytes`. Cross-references the relevant ADRs.

- **`migrate --dry-run` cross-reference to schema preview.** The
  dry-run plan output now includes a one-line pointer to
  `sluice schema preview` for full DDL inspection with translation
  notes and advisory hints. Closes roadmap §10.

### Fixed

- **Bug 12 — MySQL CDC silently dropped events with TIMESTAMP /
  DATETIME / DATE columns.** The decoder for binlog row events
  (`decodeTime` in `internal/engines/mysql/value_decode.go`) only
  accepted `time.Time` directly — but the binlog protocol hands
  temporal values back as their raw string form ("YYYY-MM-DD
  HH:MM:SS[.ffffff]" / "YYYY-MM-DD") regardless of the schema-cache
  DSN's `parseTime=true` setting. The first row event on any table
  with a temporal column raised `cannot decode string as time.Time
  (parseTime=true should be set)`; the binlog pump exited with that
  error stored on the reader (only surfaced via `Err()`, not logged),
  the change channel closed, and the applier saw zero events.
  Symptom: cold-start bulk-copy completed cleanly, then CDC mode
  produced no further inserts on the destination — looked exactly
  like a network/heartbeat issue, which sent the original Bug 12
  hypothesis chasing port-forwarding ghosts.

  Fix: `decodeTime` now parses MySQL's canonical temporal string
  formats — second-precision, microsecond-precision, date-only —
  plus byte-slice equivalents and the `0000-00-00` zero-value (maps
  to `time.Time{}` for clean cross-engine round-trip). Regression
  guard: `TestDecodeTimeFromString` covers all five shapes; the
  pre-existing `TestDecodeValueErrors/timestamp_from_string` case
  was inverted to test the unparseable-string error path instead
  (parseable strings now succeed).

  Empirical confirmation against `bug12_repro_dev.sh` (local mysql:8.0
  containers, table with `t TIMESTAMP DEFAULT CURRENT_TIMESTAMP`):
  pre-fix dropped 100% of CDC events on tables with a temporal
  column; post-fix all events flow.

- **Bug 15 CLI sync-stop drain (data loss in warm-up window,
  ADR-0025).** The v0.5.0 slot-ack-after-apply work (ADR-0020)
  closed the post-restart wedge but left a residual data-loss path
  in the warm-up window between stream start and the first applied
  commit. Pre-fix, `ackLSN` returned `streamedLSN` (the highest
  commit-LSN parsed off the WAL) when the applier-feedback tracker
  was still at zero; the keepalive routine ack'd that to the slot,
  advancing `confirmed_flush_lsn` past events that hadn't been
  durably applied. A subsequent `sync stop` mid-batch then lost
  the events between persisted_position and confirmed_flush_lsn —
  warm-resume's slot stream started past them and the rows never
  landed. Empirical repro on local docker: 25-42 row gap with
  `--apply-batch-size=50` and a sustained 10/sec writer.

  Fix has two layers:

  1. **`ackLSN` anchors at startLSN until first apply commit.** The
     load-bearing data-correctness fix. When the tracker is fresh
     (`applied=0`), ack returns the LSN the pump started from
     (cold-start: snapshot LSN; warm-resume: persisted_position's
     LSN). The slot can't advance past that point until the applier
     reports a higher value via the tracker. One-line, one-parameter
     change.

  2. **Graceful-drain shape for `sync stop`.** The pre-fix
     `pollStopSignal` cancelled `applyCtx`, rolling back the open
     batch — relying on warm-resume to redeliver. With the ackLSN
     fix that worked correctly but produced unnecessary redelivery
     storms. Stop-signal now cancels a separate `streamCtx` (which
     scopes the CDC reader's pump); the channel closes cleanly,
     the applier's existing `channelClosed` branch commits the
     in-flight partial batch, position writes naturally. A
     30-second watchdog escalates to hard-cancelling `applyCtx` if
     the drain wedges.

  Unit-level regression guard: `TestAckLSN_AnchorsAtStartLSNUntilFirstApply`
  pins the contract. Empirical integration repro lives at
  `C:\code\sluice-testing\workspace\bug15_repro_dev.sh` (sustained
  writer, mid-stream `sync stop`): pre-fix dropped 25-42 rows;
  post-fix drops 0. The existing programmatic-RequestStop integration
  test (`TestStreamer_PostgresToPostgres_StopRestartNoLoss`) still
  passes — it happened to time RequestStop past first-batch commit,
  masking the warm-up window. See ADR-0025.

- **Windows CI: `TestPreviewer_Golden_Text` fails with CRLF/LF
  mismatch.** The test compared `bytes.Equal(buf.Bytes(), want)` —
  buffer with LF newlines (Go's native `\n`) vs. file content that
  git's default `core.autocrlf=true` had converted to CRLF on
  Windows checkouts. The diff showed visually identical content;
  byte comparison failed.

  Two-part fix:
  1. New `.gitattributes` enforces `eol=lf` on text files so
     Windows checkouts no longer get CRLF on golden fixtures.
  2. The test normalises CRLF→LF on the read side before comparing
     — belt-and-suspenders against any future checkout that
     bypasses the attribute (e.g. zip-download, alternate clones).

  No behavioural change to runtime code; CI-only fix.

## [0.6.0] - 2026-05-05

Feature release. Headline additions are `sluice schema preview` (operator-facing target-DDL inspection with translation notes and advisory hints) and `--reset-target-data` (one-command destructive recovery on top of v0.5.2's slot-missing fall-through). Plus four reliability items uncovered during v0.5.x testing: a CI-only data race in the parallel-copy state-write path, batched-apply idle flush on quiet streams, MySQL binlog-purged fall-through (extends ADR-0022 to the MySQL side), and two parallel-copy hygiene follow-ups. Two new ADRs (0023 schema preview, 0024 reset-target-data); ADR-0022 extended for MySQL.

### Fixed

- **Data race in parallel-copy state-write path.** v0.5.0's
  `migrate_parallel.go::copyChunk` checkpoint sites took `stateMu`,
  mutated their slot in `state.TableProgress`, then did a shallow
  copy `stateCopy := *state` and released the lock before calling
  `writeState`. The shallow copy left `stateCopy.TableProgress`
  pointing at the same map backing storage as `state`, so the JSON
  encoder iterating outside the lock raced peer chunk goroutines
  taking the lock to mutate their own slots. Surfaced as a CI -race
  failure in `TestMigrate_PG_ParallelCopy_Resume` for the v0.5.x
  releases.

  Fix: a `cloneStateForWrite` helper re-allocates the
  `TableProgress` map and each entry's `Chunks` slice under the
  lock; the encoder gets a fully independent snapshot. Per-chunk
  reference fields (`LowerPK`/`UpperPK`/`LastPK`) are not deep-
  cloned because they're either written once at resolution time or
  replaced wholesale (not mutated in place) on each checkpoint.
  Pre-existing behaviour preserved bit-for-bit; the fix is sync-
  primitive-only.

- **Two parallel-copy hygiene follow-ups.** `progressTicker.startedAt`
  swaps the `Load → Store` check-then-set for an `atomic.CompareAndSwap`
  so the contract stays correct if `loop` ever runs from multiple
  goroutines (single-goroutine today; one-line future-proofing).
  `kickOffRowCount` now suppresses the `row-count probe failed`
  WARN when the parent context was already cancelled, and skips
  the `setTotalRows` store when the ticker is already stopped —
  removes interleaved teardown-time noise during test cleanup.

### Added

- **`sluice schema preview` subcommand.** Reads the source schema,
  applies the translation pipeline (mappings + cross-engine type
  policy), and emits the target DDL with inline cross-engine
  translation notes and advisory hints — without touching either
  database's data. Operators see exactly what the target schema will
  look like before any migration runs, including the `--type-override`
  invocation for known operator-preferable alternatives (e.g. PG
  `uuid` → MySQL `BINARY(16)` instead of the default `CHAR(36)`).
  Supports `--format text|json`, `--include-table`/`--exclude-table`,
  `--type-override`, and `--output FILE` (atomic temp-file +
  rename, so a Ctrl-C mid-write never corrupts the destination).
  New `ir.DDLPreviewer` engine surface; both Postgres and MySQL
  implement it on the same struct as their `SchemaWriter` (the
  emitTableDef/emitCreateIndex/emitAddForeignKey helpers are now
  shared between the execute and preview paths). Initial advisory-
  hints registry seeds five high-traffic surprises from real-world
  testing reports (UUID, large-TEXT, JSON-vs-JSONB note, DATETIME
  timezone, unbounded numeric). Translate package gains
  `binary_uuid`, `mediumtext`, `timestamptz`, and parameterised
  `decimal` aliases to support the suggested overrides. See
  ADR-0024.

- **`--reset-target-data` for destructive recovery.** New flag on
  `sluice migrate` and `sluice sync start` that DELETEs the
  bookkeeping row (`sluice_migrate_state` / `sluice_cdc_state`),
  DROPs every source-schema table on the target, then proceeds with
  cold-start. Collapses the post-`slot drop` recovery flow to a
  single command (no more enumerating tables for `DROP TABLE`).
  Confirmation prompt requires the operator to type `reset`
  verbatim — bypassed by `--yes` for non-interactive use. Mutually
  exclusive with `--resume` at parse time. New optional engine
  surfaces: `ir.TableDropper`, `ir.StreamCleaner`, and
  `ir.MigrationStateStore.ClearMigration`. See ADR-0023.

  An additional optional surface, `ir.BulkTableDropper`, lets
  engines collapse the per-table DROP loop into one statement —
  the recovery flow on a 500-table source pays one network round-
  trip instead of 500. Both Postgres (`DROP TABLE … CASCADE`) and
  MySQL (`DROP TABLE …`) implement the bulk path; engines without
  it fall back to per-table `DropTable` automatically. Audit log
  lines name every dropped table on either path.

  `docs/postgres-source-prep.md` cross-references the flag from the
  `wal_status='lost'` recovery section so the doc trail through the
  destructive-recovery flow stays connected.

- **Batched-apply idle flush on quiet streams.** Closes the trailing-
  row latency footnote from ADR-0020. The batched applier now commits
  a partial in-flight batch (n < `--apply-batch-size`) within
  `defaultIdleFlushPeriod` (5s) when no further change arrives. On
  Postgres this lets the slot's `confirmed_flush_lsn` advance past
  in-flight work on idle streams, so warm-resume from a quiet stream
  starts at the most recent commit rather than the previous full
  batch boundary; on MySQL the same logic keeps `source_position`
  current so the replay window on warm-resume stays bounded. Both
  engines use the same 5s default for symmetry. Existing flush
  triggers (channel close, Truncate, ctx cancel) are unchanged; idle
  flush is purely additive. Integration test:
  `TestChangeApplier_ApplyBatch_IdleFlushCommitsPartial` (PG;
  partial-batch persistence on MySQL was already covered by
  `TestChangeApplier_ApplyBatch_PartialFlushPersistsPosition`).

- **MySQL binlog-purged fall-through to cold-start.** Extends the
  v0.5.2 PG slot-missing recovery to the MySQL side. The MySQL CDC
  reader's `resolveStartPosition` now pre-flights the persisted
  position before handing off to go-mysql's binlog syncer:
  - **File/pos mode**: queries `SHOW BINARY LOGS` and checks the
    persisted file is still present. If missing (typical when
    `expire_logs_seconds` rolled it off, or an operator ran
    `PURGE BINARY LOGS`), returns
    `mysql: binlog file %q is no longer available on the source
    (purged); cannot resume: ir: persisted position is no longer
    valid`.
  - **GTID mode**: runs `SELECT GTID_SUBSET(@@gtid_purged, ?)` with
    the resume set. Returns 0 when the source has purged GTIDs the
    resume set hasn't consumed — meaning we'd be missing data on
    resume — and surfaces `mysql: source has purged GTIDs not
    present in resume set; cannot resume`.

  Both branches wrap with `ir.ErrPositionInvalid`; the streamer's
  existing v0.5.2 fall-through (added engine-neutrally) detects the
  sentinel and re-enters `coldStart` with the same `lsnTracker`.
  No new code in the pipeline package; the engine-neutrality of the
  v0.5.2 design pays off here. ADR-0022 extended.

  Pre-fix shape: a sluice stream restarted after the source's
  binlog had rotated past the persisted file would surface
  go-mysql's raw "Could not find first log file name in binary log
  index file" error mid-stream. Post-fix: the WARN fires at startup,
  cold-start runs, dest is reseeded.

  Integration test:
  `TestStreamer_MySQLToMySQL_BinlogPurgedFallsThroughToColdStart`
  exercises the file/pos branch end-to-end. GTID branch is covered
  by the same `verifyPositionResumable` dispatch and the SQL-side
  semantics of `GTID_SUBSET` (no separate integration test;
  GTID-mode setups are tested elsewhere in the resume coverage).

## [0.5.2] - 2026-05-05

Single-feature patch release closing Item F from the v0.4.0
real-world testing report: PG CDC streams whose replication slot
was dropped (typically after `wal_status='lost'`) now recover via
auto-fall-through to cold-start instead of erroring out with no
flag to bypass.

### Added

- **Slot-missing fall-through to cold-start (Item F).** When a
  Postgres CDC stream's persisted position references a replication
  slot that no longer exists on the source — typically because the
  operator dropped it after sluice surfaced `wal_status='lost'` —
  the streamer now logs a loud WARN naming the slot + persisted LSN,
  then falls through to the cold-start path automatically. No flag
  required; no manual `DELETE FROM sluice_cdc_state` step. Bug 9's
  pre-flight refusal still gates populated-dest operations, so
  operators who want a fresh bulk-copy still pass `--force-cold-start`
  or drop dest tables manually. The fall-through is engine-neutral:
  CDC readers signal the condition via `ir.ErrPositionInvalid`
  (wrapped on their specific diagnostic via `%w`); the pipeline
  detects it via `errors.Is`. PG slot-missing is the only emitter
  in this release; MySQL binlog-purged is queued as a follow-up.
  See ADR-0022.

  Recovery flow before this fix: drop slot → DELETE cdc_state row
  → drop publication → drop dest tables (or `--force-cold-start`)
  → re-run sluice. With this fix: drop slot → drop dest tables
  (or `--force-cold-start`) → re-run sluice. The two manual SQL
  steps disappear.

  Integration test:
  `TestStreamer_PostgresToPostgres_SlotMissingFallsThroughToColdStart`.

## [0.5.1] - 2026-05-05

Single-issue patch release fixing a misleading flag name in the
Postgres `wal_status='unreserved'`/`'lost'` recovery hint. No
behavioural change.

### Fixed

- **`wal_status` recovery hint named `--target` instead of
  `--source` (Item F).** When sluice refused to start CDC against an
  invalidated slot, the error message pointed operators at
  `sluice slot drop <name> --target ...`. The slot lives on the
  *source* database and `slot drop`'s actual flag is `--source` —
  operators following the hint hit a flag-not-found error and had
  to consult `slot drop --help` to recover. Both the `unreserved`
  and `lost` branches of `checkSlotUsable` now emit
  `--source-driver=postgres --source ...`. `docs/postgres-source-prep.md`
  is corrected in lockstep. Real-world testing surfaced this as the
  one polish item against an otherwise gold-standard error message.
  Test coverage extended to assert the recovery hint references
  `--source` so the regression doesn't return.

## [0.5.0] - 2026-05-05

Reliability + performance release. Headline feature is parallel
within-table bulk copy (the pgcopydb-class signature win for multi-TB
migrations), throughput metrics extended to MB/s + ETA, plus four
fixes uncovered during real-world v0.4.0 soak testing — one of which
(Bug 15) was a CRITICAL silent-data-loss path on Postgres CDC. Three
new ADRs (0019, 0020, 0021).

### Added — performance

- **Parallel within-table bulk copy.** Tables above
  `--bulk-parallel-min-rows` (default 100k) with a single integer PK
  are now split into N PK ranges and copied concurrently, with per-
  chunk cursor checkpoints in `sluice_migrate_state`. Tables below
  the threshold, with composite PKs, or without a PK fall through to
  the v0.4.x single-reader behaviour. Postgres readers share a single
  exported snapshot via `SET TRANSACTION SNAPSHOT` (`SnapshotImporter`
  optional engine surface) so all chunks see a consistent view; MySQL
  uses per-chunk `REPEATABLE READ` transactions because per-session
  REPEATABLE-READ snapshots have no shareable name. Boundaries are
  computed once via `MIN`/`MAX` on the PK and persisted, so a resume
  run aligns exactly with completed chunks rather than recomputing
  ranges (which would shift if rows landed concurrently). New flags:
  `--bulk-parallelism` (default `min(8, NumCPU)`) and
  `--bulk-parallel-min-rows`. See ADR-0019.
- **Throughput metrics: MB/s + ETA.** The bulk-copy progress ticker
  now emits `total_rows`, `bytes`, `rate_mb_per_sec`, and
  `eta_seconds` alongside the existing `rows`/`rate` attributes;
  per-chunk progress lines carry a `chunk=` attribute so operators
  can see which range is in flight. Row-byte estimation walks the
  `ir.Row` value-side: string/`[]byte` by length, fixed-width
  numerics by Go size, `time.Time` as 24, bool as 1, recursive on
  `[]any`/`[]string`. Approximate but stable enough that MB/s tracks
  observed network throughput within a few percent.
- **`CountRows` / `RangeBounds` optional engine surfaces.** Postgres
  estimates row counts via `pg_class.reltuples` (autovacuum-
  maintained); MySQL via `information_schema.TABLE_ROWS`. Both short-
  circuit when called against a snapshot-pinned reader where a
  concurrent query would deadlock the single shared connection. The
  ETA computation falls back gracefully when the surface isn't
  available.

### Fixed

- **Postgres CDC: slot ack advanced before apply commit (Bug 15,
  CRITICAL — silent data loss on crash).** The PG CDC reader was
  sending the *streamed* LSN in `StandbyStatusUpdate`, so a crash
  between `Send` and `tx.Commit` advanced `confirmed_flush_lsn` past
  events that were never applied — and a warm resume started at the
  acked position, dropping the in-flight batch on the floor. Real-
  world soak observed silent row drift after a clean stop/restart
  cycle when the streamer happened to interrupt a partial batch.

  Fix: a single-producer/single-consumer `lsnTracker` plumbed
  engine-neutrally via `lsnTrackerProvider`/`lsnTrackerAttacher`
  structural interfaces. The applier reports `appliedLSN` after
  `tx.Commit()`; the reader sends `min(streamed, applied)` in the
  next status update. Trailing-row latency under `--apply-batch-size
  > 1` is bounded by the batch interval since the LSN only advances
  on commit boundaries — acceptable today; idle-flush is on the
  roadmap. See ADR-0020.

  Integration test: `TestStreamer_PostgresToPostgres_StopRestartNoLoss`
  exercises a stop in the middle of a batched apply and asserts
  every source change lands on the target after warm resume.

- **Postgres CDC: publication scope was `FOR ALL TABLES` (Bug 13).**
  The v0.4.0 publication was created `FOR ALL TABLES`, so a brand-
  new unrelated table on the source — created after sluice started
  streaming — would land in the pgoutput stream. The applier either
  crashed on the unknown table OID or, worse, silently dropped the
  events.

  Fix: `Engine.EnsurePublication(ctx, dsn, tables)` now creates
  `FOR TABLE <list>` from the resolved migration set after
  `applyTableFilter`. Existing v0.4.0 `FOR ALL TABLES` publications
  are migrated by drop-and-recreate during cold start (the slot is
  unaffected; only the publication is replaced). The applier now
  has defence-in-depth: an unknown table OID is logged at WARN and
  the change is skipped rather than crashing the stream. See
  ADR-0021.

  Integration test: `TestStreamer_PostgresToPostgres_NewTableOnSourceIgnored`
  creates a fresh table on the source mid-stream and asserts the
  applier ignores it.

- **PG array → MySQL JSON conversion (Bug 14).** A PG source column
  of array type (e.g. `text[]`, `int[]`) migrating to a MySQL JSON
  target arrived at the writer as `[]any`, a PG-array literal string
  (`{a,b,c}`), or `[]byte` holding the same — none of which MySQL's
  driver knows how to bind to a JSON column. `prepareValue` now
  branches `convertArrayLikeToJSON` for all three shapes. Empty
  arrays serialize as `[]` (disambiguated from `{}`, which would be a
  JSON object). Integration test:
  `TestMigrate_PostgresToMySQL_ArrayToJSONOverride`.

- **MySQL CDC: silent stalls on quiet upstream (Bug 12).**
  go-mysql's binlog syncer can hang silently if the upstream goes
  quiet for long enough that the TCP keepalive doesn't fire — the
  reader has no signal to distinguish "no events" from "connection
  dead". v0.5.0 sets `defaultBinlogHeartbeatPeriod = 10s` on the
  syncer so the upstream emits keep-alive heartbeats, and adds a
  30s no-events watchdog that surfaces a stalled-stream error if no
  row-relevant event arrives in that window (filtered by
  `isRowRelevantEvent` so heartbeat and rotation events don't reset
  the timer indefinitely, which would mask a real stall). Not
  reproducible in CI without a multi-minute idle, so manually
  validated against real PlanetScale/vanilla MySQL streams.

### Added — architecture documentation

Three new ADRs in `docs/adr/`:

- **ADR-0019**: Parallel within-table bulk copy — chunk-boundary
  computation, snapshot-import strategy per engine, boundary
  stability invariant, fallback matrix.
- **ADR-0020**: Slot-ack-after-apply — LSN tracker design, SPSC
  contract, why `min(streamed, applied)` instead of just `applied`,
  trailing-row latency tradeoff.
- **ADR-0021**: Publication scope by table — `FOR TABLE <list>`
  rationale, drop-and-recreate migration from v0.4.0 publications,
  applier defence-in-depth on unknown OIDs.

## [0.4.0] - 2026-05-04

Feature release with four substantive responses to measured production
concerns from the v0.3.x robustness testing rounds, plus three new
ADRs (0016, 0017, 0018) documenting the design decisions.

### Added — performance

- **`--apply-batch-size N`** on `sluice sync start` (and
  `Streamer.ApplyBatchSize` for programmatic callers) batches up to N
  CDC changes per target transaction with the position write of the
  last change in the batch. Default 1 keeps v0.3.x conservative
  one-change-per-tx behaviour; production tuning is 100–500. v0.3.0
  testing measured the per-change applier at ~6.5 rows/sec on
  PG→MySQL CDC with a 5000-row source transaction; batched mode
  amortises commit overhead 50–100× on production hardware (3.5×
  observed locally without fsync). Idempotency preserved via the
  existing ON CONFLICT / ON DUPLICATE KEY UPDATE semantics on
  Insert. Schema-change events (Truncate, DDL) flush the in-flight
  batch before applying. See ADR-0017.
- **`--bulk-batch-size N`** on `sluice migrate` (default 5000)
  controls the per-batch checkpointing size for resume. Cold-start
  migrations continue to use the faster plain-INSERT (and PG COPY)
  path with no per-batch overhead.

### Added — operability

- **Per-batch checkpointing for `sluice migrate --resume`.**
  Previously, resume on an in-progress table truncated and re-copied
  from row 0. v0.4.0 tracks a per-table PK cursor in
  `sluice_migrate_state.table_progress`, reads the source via
  `WHERE pk > cursor ORDER BY pk LIMIT batch_size`, and applies
  rows with `ON CONFLICT` / `ON DUPLICATE KEY UPDATE` so the brief
  replay window between batch commit and cursor write is tolerated
  cleanly. Multi-hour copies of 100M+ row tables can resume mid-
  table. Composite PKs descend via row-comparison cursors
  (`(a,b) > ($1,$2) ORDER BY a,b`). Tables without a PK fall back
  to the v0.3.0 truncate-and-redo behaviour with a clear log line.
  v0.3.0-shape state rows are read backward-compatibly. See
  ADR-0018.
- **Cross-engine expression translation for generated columns and
  CHECK constraints.** v0.3.2's verbatim-passthrough policy held
  the fail-loud claim (no silent corruption), but the set of
  "non-portable" expressions included very common idioms.
  Bidirectional translation pass at the writer boundary now covers:
  - **MySQL → Postgres**: `CONCAT(a,b)` → `(a || b)`, `IFNULL` →
    `COALESCE`, `IF(cond,a,b)` → `CASE WHEN cond THEN a ELSE b END`,
    `JSON_UNQUOTE(JSON_EXTRACT(j,'$.k'))` → `(j->>'k')`,
    `JSON_EXTRACT(j,'$.k')` → `(j->'k')`.
  - **Postgres → MySQL**: `(expr)::type` → `CAST(expr AS …)`,
    `a || b` → `CONCAT(a, b)`, `~~`/`~~*` → `LIKE`/case-insensitive
    `LIKE`, `= ANY(ARRAY[…])` → `IN (…)`.

  Unrecognized constructs still pass through verbatim and rely on
  the loud-failure-on-target fallback. Translator uses a string-
  literal-aware walker that respects single-quoted literals and
  balanced parens — no full SQL parser. See ADR-0016.

### Fixed

- **Cold-start hangs when dest tables have pre-existing data
  (Bug 9, open since v0.3.0).** Three-part fix:
  1. **Pre-flight refusal**: cold-start now checks each source
     table for non-empty dest data and refuses with a clear error
     pointing at recovery commands. Skipped on `--resume` (resume
     expects partial state).
  2. **Goroutine-leak fix**: `copyTable` now derives a child
     context and cancels it on every return path. Previously, when
     `WriteRows` errored mid-stream, the row-reader goroutine
     blocked forever on `out <- row` against an abandoned
     consumer, holding the snapshot transaction open and surfacing
     as PG's "idle in transaction" sessions.
  3. **Clearer log shape**: progress ticker's Stop now takes the
     writer error and logs `bulk copy aborted table=foo rows=N
     err="…"` on failure instead of the misleading `bulk copy
     complete rows=N`. New `--force-cold-start` flag bypasses the
     pre-flight refusal for the rare legitimate "bulk-copy into a
     populated target" case.
- **`stop_requested_at` not cleared after consumption (Bug 11,
  open since v0.3.2).** A `sluice sync stop` left the timestamp
  set after the streamer drained and exited; the next
  `sluice sync start` would see the stale signal and exit within
  the first poll interval. The streamer now clears the flag at
  startup (after `EnsureControlTable`, before reading the persisted
  position). Idempotent and tolerant of a missing row. New
  `ChangeApplier.ClearStopRequested` interface method on the
  applier.

### Changed

- **`docs/type-mapping.md` corrected for PG→MySQL `Inet`/`Cidr`/
  `Macaddr`/`Array` types.** The doc previously claimed auto-emit
  as `VARCHAR(N) CHECK (format)`; v0.3.x and v0.4.x actually refuse
  loudly with a copy-paste-ready `mappings:` YAML snippet pointing
  at the `--type-override` CLI flag. Auto-emit is queued as a
  future enhancement; manual override is the supported path today.

## [0.3.2] - 2026-05-04

Patch release adding CHECK constraint support, a CLI form of the
type-override YAML config, and an opportunistic improvement to
the generated-column expression normalizer that the CHECK work
surfaced.

### Added

- **CHECK constraint support across both engines.** Source schemas
  declared with `CHECK (qty >= 0)` or `CHECK (status IN ('open',
  'closed'))` now round-trip cleanly: the schema readers capture
  the expression on `Table.CheckConstraints`, the DDL writers
  emit `CONSTRAINT name CHECK (expr)` inline in CREATE TABLE,
  and the constraint is enforced on the target.

  Translation policy is verbatim passthrough — non-portable
  expressions fail loudly on the target rather than be guessed
  at. Identifier and string-literal decoration is normalized at
  the read boundary (see below).

  Integration coverage: MySQL→MySQL, PG→PG, and MySQL→PG cross-
  engine snapshot migrations each verify (1) the CHECK lands on
  the target's `information_schema.check_constraints`, (2)
  bulk-copied rows survived, (3) a violating INSERT is rejected
  by the target, and (4) a satisfying INSERT is accepted.

- **`--type-override TABLE.COLUMN=TYPE` CLI flag** on `sluice
  migrate` and `sluice sync start`. Repeatable; format mirrors
  the YAML `mappings:` shape but in a single string. Wholesale
  CLI-over-YAML precedence (matches the existing `--include-table`
  / `--exclude-table` precedence policy). For target-type options
  (e.g. `jsonb` with `binary=true`) operators still need the YAML
  form — the CLI deliberately doesn't try to encode key/value
  options in a single string.

### Fixed

- **Generated-column cross-engine expressions with string
  literals**. The v0.3.1 generated-column work normalized MySQL's
  backtick identifier quotes but missed two more layers of
  decoration MySQL applies to the stored expression text:

  - **Charset introducers** — every string literal is wrapped as
    `_<charset>'literal'` (e.g. `_utf8mb4'open'`). PG rejects this
    as a syntax error.
  - **Delimiter-escape form** — every string literal's apostrophes
    are stored as `\'`. PG with `standard_conforming_strings=on`
    (the default since 9.1) rejects `\'` outright.

  v0.3.1 didn't catch these because the test fixtures used
  `qty * price` — no string literals. The CHECK constraint work
  in this release surfaced both immediately (via `status IN
  ('open', ...)`) and the new `normalizeMySQLExpressionText`
  helper now strips all three layers. **Generated columns benefit
  from the same fix**: a column declared as `CONCAT(name, ' ')`
  cross-engine that would have silently failed on v0.3.1 now
  works.

## [0.3.1] - 2026-05-04

Patch release — adds first-class generated-column support and
includes the CI-pipeline fixes that surfaced during the v0.3.0
release rebuild.

### Added

- **Generated column support across both engines.** Source columns
  declared as `GENERATED ALWAYS AS (expr) STORED` (or `VIRTUAL` on
  MySQL) now round-trip cleanly: the schema readers capture the
  expression on `ir.Column.GeneratedExpr`, the DDL writers emit
  the corresponding `GENERATED ALWAYS AS (...)` clause, and the
  bulk-copy / CDC paths skip the column from INSERT/UPDATE column
  lists so the target re-computes via its own GENERATED clause.

  Translation policy is verbatim passthrough — non-portable
  expressions (e.g. MySQL `CONCAT(a, b)` vs PG `a || b`) fail
  loudly on the target rather than be guessed at. Identifier
  quoting *is* normalized at the read boundary (MySQL's stored
  expression text uses backticks that PG can't parse), since
  that's a mechanical dialect-quoting issue rather than a
  function/operator translation. Cross-engine sources with
  VIRTUAL columns are silently promoted to STORED on PG (which
  doesn't support VIRTUAL) with a `slog.Warn` documenting the
  shift.

  Integration coverage on MySQL→MySQL, PG→PG, and MySQL→PG
  (cross-engine) for both the migrate and streamer paths.

### Fixed

- **CI pipeline fixes uncovered during the v0.3.0 release rebuild**:
  - Migrated `.golangci.yml` to v2 schema (top-level `version: "2"`,
    `linters.default: none`, formatters split into the new
    top-level `formatters:` section, drop deprecated `gosimple`
    which is merged into `staticcheck`).
  - Bumped `golangci/golangci-lint-action` to `@v8` so `version:
    latest` resolves to the v2 module path.
  - Re-enabled `install-mode: goinstall` so the linter compiles
    with our Go 1.26 toolchain rather than the prebuilt-binary's
    older Go (which couldn't typecheck stdlib `chacha20poly1305`'s
    Go-1.26-only file).
  - **MySQL binlog composite-PK test**: corrected `int32` type
    assertions to `int64`. The binlog reader's `decodeInteger`
    widens every integer to `int64`, so the v0.3.0 test asserted
    a type that doesn't exist in the row map.
  - Five new lint findings v1 missed (caught by v2): `any`
    variable shadowing the builtin, an embedded-field selector
    simplification, a capitalised error string, two De-Morgan'd
    conditional reads.

### Changed

- **Schema readers exclude `sluice_*_state` tables**. Already done
  in v0.3.0 for the migrate-state table; this release extends the
  list to fully cover both bookkeeping tables on re-migrations.

## [0.3.0] - 2026-05-04

Feature release. Three substantial additions to the operator surface
(`sluice migrate --resume`, `sluice sync stop`, `--include-table` /
`--exclude-table`), one silent-data-loss fix on Postgres CDC, and
five new ADRs documenting the v0.2.x and v0.3.0 design decisions.

### Added — resumable simple-mode migrations

- **`sluice migrate --resume --migration-id ID`** picks up a failed
  migration where it left off rather than forcing a drop-and-redo.
  Per-target `sluice_migrate_state` row tracks phase
  (`tables`/`bulk_copy`/`identity_sync`/`indexes`/`constraints`/
  `complete`) and per-table bulk-copy progress as a JSON map.
  In-progress tables are TRUNCATEd before re-copy. Failure paths
  persist the in-flight phase plus a 1KB-truncated error message;
  a state-write failure during cleanup is joined with the primary
  error via `errors.Join` so the operator never loses the root
  cause.
- **Behavior matrix** is conservative for non-resume runs: existing
  state row + no `--resume` errors out (no silent overwrites), and
  `--resume` against a `complete` row exits cleanly with an
  "already complete" log. New `MigrationStateStore` and
  `TableTruncator` are optional engine surfaces (type-assertion
  pattern, mirroring `SlotManagerOpener`); engines without the
  primitives error clearly when `--resume` is requested.
- **`CREATE TABLE IF NOT EXISTS`** is now universal in the DDL
  emitters on both engines, so the resume tables-phase is a clean
  no-op on re-run. Schema readers exclude `sluice_*_state` so
  re-migrations don't propagate sluice's bookkeeping as user data.

### Added — selective table inclusion / exclusion

- **`--include-table TABLE,...`** and **`--exclude-table TABLE,...`**
  on `sluice migrate` and `sluice sync start`. Comma-separated,
  repeatable, glob patterns supported via stdlib `path.Match`
  (`audit_*`, `tmp_*`). Mutually exclusive at the CLI parse layer.
  Same fields available in YAML config as `include_tables` /
  `exclude_tables`; CLI takes precedence wholesale (no merge).
- **Filtering happens at the orchestrator boundary**: schema
  pruning after `ReadSchema` and a CDC dispatch wrapper that drops
  events for excluded tables before the applier sees them. Engines
  remain agnostic to the spec, so behaviour is identical across
  MySQL/Postgres/future engines.
- **Position-advancement caveat**: positions only commit when an
  event applies, so a stream that consists entirely of dropped
  events lags within the source-side WAL/binlog retention window.
  Documented on the `Streamer.Filter` field.

### Added — graceful stream stop

- **`sluice sync stop --target-driver X --target DSN --stream-id ID`**
  asks a running sync stream to drain in-flight changes, persist
  the final position, and exit cleanly. Mechanism is a control-
  table flag (`stop_requested_at` column on `sluice_cdc_state`)
  polled by the running streamer every 5s. Survives operator
  machine boundaries, container lifecycles, and process restarts —
  the flag persists; a restarted streamer sees it on next poll.
- **Additive to `Ctrl-C` / `SIGTERM`** which still work via the
  existing signal path. The new mechanism fits Kubernetes lifecycle
  hooks, systemd `ExecStop`, and remote orchestrators that can't
  send signals to a different machine.
- **Idempotent schema migration**: existing v0.2.x deployments pick
  up the new column on next `EnsureControlTable` call without
  losing data. PG uses `ADD COLUMN IF NOT EXISTS`; MySQL uses
  detect-then-ALTER for portability across all 8.x versions.

### Added — observability

- **Structured logging via `log/slog`** (replacing
  `fmt.Fprintf`-to-stdout). `--log-level` is now wired into the
  default handler; `debug` / `info` / `warn` / `error` actually
  change verbosity. Pipeline records emit as
  `time=... level=INFO msg="..." key=value` to stderr; CLI table
  outputs (`engines`, `sync status`, `slot list`) keep using stdout
  unchanged — they're table renders, not log streams.
- **Bulk-copy progress reporting**: a per-table `progressTicker`
  emits `bulk copy progress table=foo rows=N rate=R` every 2s
  while a copy is in flight, plus a final `bulk copy complete`
  line on table completion. Long migrations are no longer 30
  minutes of silence.
- **Phase-aware error hints**: wrapped pipeline errors gain an
  optional one-line `hint:` suffix for common operator-facing
  failures (missing target table, bad DSN host, auth failures,
  missing `REPLICATION` grant, missing `CREATE` on schema).
  Registry is intentionally tiny (7 entries, scoped by phase);
  hints are appended via `fmt.Errorf("%w\nhint: %s")` so
  `errors.Is`/`As` traversal is unaffected.

### Added — architecture documentation

Five new ADRs in `docs/adr/`:

- **ADR-0011**: `SlotManager` as an optional engine surface.
- **ADR-0012**: Bypass `pglogrepl` to send raw
  `CREATE_REPLICATION_SLOT FAILOVER true` for PG 17+.
- **ADR-0013**: Applier value-shaping via column-type cache and
  `CAST(? AS JSON)` (the Bug 6 fix shape).
- **ADR-0014**: Phase-aware error-hint registry (substring + phase
  matching, deliberately tiny).
- **ADR-0015**: Migration resume design — per-target state table,
  truncate-and-redo for in-progress tables, `errors.Join` on
  state-write-during-failure paths.

### Fixed

- **Postgres CDC: composite-PK DELETE silently lost (Bug 8)**.
  pgoutput's `DeleteMessage` with `REPLICA IDENTITY DEFAULT`
  carries an `OldTuple` whose `ColumnNum` equals the relation's
  full column count, with `'n'` (null) markers for non-key
  columns. `decodeTuple` translated those into present-but-nil
  entries on the row map; the applier's `WHERE` then emitted
  `non_key IS NULL` predicates that matched zero rows on the
  destination. The applier's resume-idempotency tolerance for
  zero-rows-affected (ADR-0010) absorbed the silence; the
  position advanced; `DELETE`s disappeared. Real-world soak
  testing observed a 30-row drift on a composite-PK
  `order_items` table.

  Fix: `filterDeleteBefore` narrows the emitted Before to columns
  flagged `KeyColumn=true` on the relation cache. Correct under
  every `REPLICA IDENTITY` mode (DEFAULT drops `'n'` entries; FULL
  drops non-identity columns; USING INDEX is a no-op on the
  already-narrow OldTuple; PK-less FULL falls back to the full row
  to honour the operator's deliberate setting). `REPLICA IDENTITY
  NOTHING` is rejected loudly — DELETE is unreplicatable in that
  mode.

  MySQL is unaffected: `binlog_row_image=FULL` (the default)
  carries every column with real values, so the WHERE matches
  exactly. The user's PG→MySQL drift was the PG source-side bug
  propagating through.

### Test gap closed

- **Composite-PK CDC coverage on MySQL paths**. Bug 8 reached
  real-world soak because no existing CDC integration test
  exercised composite-PK tables across any direction. Added
  `TestCDCReader_CompositePK` (MySQL binlog, asserts both PK
  columns survive INSERT/UPDATE/DELETE) and
  `TestStreamer_MySQLToPostgres_CompositePKDelete` (cross-engine,
  asserts row-count drop on the target). VStream coverage punted
  to a follow-up — the test infrastructure (vtgate setup) is
  heavier and the protocol surface differs enough to warrant its
  own pass.

## [0.2.2] - 2026-05-04

Patch release closing a CDC-applier JSON-encoding bug that surfaced
during v0.2.1 revalidation testing — affecting both PG→MySQL (loud
crash) and MySQL→MySQL (silent data divergence). Plus a small
dry-run output clarification and a debug-level zero-rows-affected
log so the silent class of bug is one filter away from being
spotted in the future.

### Fixed

- **MySQL applier: shape JSON column values for the wire on CDC
  Insert/Update/Delete**. The MySQL `ChangeApplier` bound row values
  straight from `ir.Row` to the parameterised SQL, bypassing the
  `prepareValue` used by the bulk-copy path. Two production failures
  shared the same root cause:

  - **Loud (PG → MySQL CDC on Vitess/PlanetScale)**: `[]byte` JSON
    values arrived `_binary`-tagged on the wire and Vitess rejected
    them with "Cannot create a JSON value from a string with
    CHARACTER SET 'binary'". Sluice exited.
  - **Silent (MySQL → MySQL CDC, vanilla MySQL included)**: `WHERE`
    on a JSON column with a bare `?` placeholder never matched —
    MySQL's `=` operator does not implicitly cast a bound parameter
    to JSON regardless of whether it's `[]byte` or `string`. The
    applier (which tolerates zero-rows-affected for resume
    idempotency) silently advanced past UPDATEs and DELETEs that
    should have matched. The destination row stayed stale forever
    with no error signal — data divergence with no observability.

  The fix has two parts: (1) a per-table column-type cache lets every
  bound value go through `prepareValue` (so JSON `[]byte` → `string`,
  Set `[]string` → comma-joined, Geometry gets the SRID prefix); and
  (2) `WHERE` placeholders on JSON-typed columns are wrapped in
  `CAST(? AS JSON)` so the comparison is JSON-vs-JSON rather than
  JSON-vs-text. The Postgres applier got the parallel cleanup for
  symmetry and for Array/Geometry shaping (its WHERE didn't need a
  CAST equivalent — pgx inspects per-column type metadata natively).

  A new `TestChangeApplier_JSONColumn` integration test on each
  engine exercises the silent path end-to-end; without the fix it
  fails loudly in PG→MySQL and quietly in MySQL→MySQL.

### Added

- **Debug-level zero-rows-affected log on Update/Delete**. The
  applier still tolerates zero-rows-affected (resume idempotency
  depends on it), but a `slog.Debug` line now fires when it
  happens — a single observability footprint that lets future
  silent-divergence bugs be one log filter away from being spotted.

### Changed

- **Dry-run table output: split `indexes` into `primary_key` +
  `secondary_indexes`**. The IR stores the primary key on a separate
  field from secondary indexes, so the v0.2.0 `indexes=N` field
  silently excluded PK and confused operators comparing against
  psql / SHOW INDEX output. The new shape (`primary_key=true
  secondary_indexes=1 foreign_keys=2`) is explicit from the field
  names alone.

## [0.2.1] - 2026-05-03

Single-issue patch release fixing a regression introduced in v0.2.0:
PG-source CDC is unblocked on PlanetScale Postgres (and any other
PG 17+ deployment whose option-list parser is strict).

### Fixed

- **PG 17+ slot creation: use named `SNAPSHOT 'export'` option**.
  v0.2.0 sent `CREATE_REPLICATION_SLOT ... (EXPORT_SNAPSHOT,
  FAILOVER true)` on PG 17+, which is a syntax mismatch — the bare
  `EXPORT_SNAPSHOT` keyword is the *pre-PG-17* form. Inside the new
  parenthesised option-list grammar the snapshot option must be the
  named form `SNAPSHOT 'export'`. PlanetScale Postgres rejected the
  v0.2.0 form with `ERROR: unrecognized option: export_snapshot`,
  blocking every `sluice sync start` against a PG source. Cold-start
  CDC (without snapshot export) was unaffected; snapshot+CDC handoff
  is the path that hit it.

## [0.2.0] - 2026-05-03

Bug-fix and operator-UX release driven by real-world v0.1.0
testing against PlanetScale Postgres + MySQL. Four target-side
data-correctness bugs fixed; the slot lifecycle on PG sources
gets a first-class CLI plus auto-drop on failed setup; logical
slots now opt into PG 17 `FAILOVER`; CLI output moves to
structured logging with bulk-copy progress lines and phase-aware
error hints.

### Added — operator surface

- **`sluice slot list` / `sluice slot drop`**: source-side
  replication-slot management for Postgres CDC. List shows
  every slot's plugin, active flag, `wal_status`, `restart_lsn`,
  and `confirmed_flush_lsn`; drop is destructive and prompts
  for confirmation by default (`--yes` skips, `--force` allows
  dropping an active slot, `--if-exists` swallows the not-found
  error). Engines without slot management (MySQL today) surface
  a clear error rather than silently no-op. Backed by a new
  `ir.SlotManager` interface that engines opt into via
  `OpenSlotManager`.
- **Auto-drop slot on failed cold-start**: when sluice creates a
  fresh slot in `StreamChanges` and any later setup step fails
  (IDENTIFY_SYSTEM, START_REPLICATION, ctx cancellation), the
  slot is dropped before `StreamChanges` returns. Slots that
  already existed when the call started are never touched. Once
  the channel is in the caller's hands the auto-drop is
  suppressed: emitted change positions reference the slot, and
  that's user data we don't auto-clean.
- **Refuse to start on invalidated slots**: `pg_replication_slots
  .wal_status` of `unreserved` or `lost` (the latter caused by a
  slow consumer falling behind `max_slot_wal_keep_size`) now
  surfaces a clear, actionable error pointing at
  `sluice slot drop` and `max_slot_wal_keep_size` for prevention,
  instead of letting `START_REPLICATION` fail mid-stream with
  "requested WAL segment has already been removed".
- **Structured logging via `log/slog`**: `--log-level` is now
  wired into the slog default handler (stderr text format), so
  `debug`/`info`/`warn`/`error` actually changes verbosity. The
  pipeline's `Migrator` and `Streamer` types drop their `Stdout`
  fields and emit structured records (`migration complete
  tables=N`, `bulk copy complete table=foo rows=N`, etc.).
  Operator-facing CLI tables (`engines`, `sync status`,
  `slot list`) keep using stdout — they're table renders, not
  log streams.
- **Bulk-copy progress reporting**: a new `progressTicker` sits
  in the row pipe between `RowReader` and `RowWriter` for each
  bulk-copied table. It atomically counts rows, emits
  `bulk copy progress` every 2s while rows are advancing, and a
  final `bulk copy complete` line on Stop. Counting at the
  pipeline layer keeps engines unchanged.
- **Phase-aware error hints**: wrapped pipeline errors get an
  optional one-line `hint:` suffix for common operator-facing
  failures — missing target table, bad DSN host, auth failures,
  missing REPLICATION grant, missing CREATE on schema. Hints are
  appended via `fmt.Errorf("%w\nhint: %s")` so `errors.Is`/`As`
  traversal is unaffected. Registry is intentionally tiny (7
  entries) and scoped by phase.

### Added — Postgres slot HA

- **`FAILOVER true` on PG 17+ slot creation**: both slot-creation
  sites — the cold-start path in the CDC reader and the
  snapshot+CDC handoff — now go through a version-aware helper.
  PG 17+ sends a raw `CREATE_REPLICATION_SLOT ... (FAILOVER true)`
  protocol command via `pgconn.Exec` (pglogrepl's options struct
  doesn't yet expose the flag); PG ≤ 16 falls back to the
  FAILOVER-less path and emits a one-time stderr warning naming
  the slot and pointing at the manual workaround. Closes the
  silent slot-loss-on-failover gotcha for PlanetScale and any
  Patroni-fronted PG 17+ deployment.

### Added — orchestration

- **`sluice sync start --dry-run`** (`-n`): symmetric with the
  existing `migrate --dry-run` flag. Reads the source schema,
  looks up the persisted position on the target, and prints the
  plan (cold-start vs warm-resume; source schema summary or
  position token) without modifying the target or starting the
  stream. The position lookup is tolerant of the control table
  being absent — both engines' `readPosition` helpers now fall
  through "missing relation" errors as "no row".

### Added — managed-service support

- **Multi-shard Vitess snapshot+CDC handoff**: the snapshot path
  (`Engine.OpenSnapshotStream` on the `planetscale` flavor) now
  fans out to every shard in a sharded keyspace, buffers rows
  from all shards into a unified per-table view, and uses the
  global `COPY_COMPLETED` event (both `Keyspace` and `Shard`
  empty) as the snapshot→CDC handoff boundary. The captured
  `ir.Position` carries one `shardGtid` entry per shard. Pairs
  with `vstream_auto_discover_shards=true` for shard discovery
  via `SHOW VITESS_SHARDS`. Validated against
  `vitess/vttestserver` with `NUM_SHARDS=2`.
- **Reshard-during-COPY signalling**: a `JOURNAL` event during
  the snapshot path's COPY phase now surfaces the typed
  `ShardLayoutChangedError`, matching the standalone CDC reader.
  v1 of the multi-shard snapshot does not recover in place — the
  caller drops the snapshot stream and reopens against the new
  layout.

### Fixed

- **MySQL target rejects JSON values labelled `_binary`**: PG
  source columns of type JSONB arriving through a MySQL writer
  were being sent over the wire with the `_binary` charset
  prefix, which Vitess (and MySQL strict mode) reject with
  "Cannot create a JSON value from a string with CHARACTER SET
  'binary'". `prepareValue` now converts `[]byte` to `string`
  for `ir.JSON` columns. Surfaced during PlanetScale-target
  testing.
- **Warm-resume engine alias**: `ChangeApplier.ReadPosition`
  stamps every recovered position with the applier's engine
  name (always `mysql` for the MySQL applier) regardless of
  which reader produced the original. Strict engine-name checks
  in `decodeBinlogPos` / `decodeVStreamPos` rejected warm-resume
  on PlanetScale streams with `wrong engine "mysql"; want
  "planetscale"`. Both decoders now accept the mysql-family
  aliases (`mysql` or `planetscale`); the cross-engine guard
  still rejects `postgres` positions.
- **Postgres UPDATE empty-WHERE under REPLICA IDENTITY DEFAULT**:
  pgoutput omits `OldTuple` on UPDATEs that don't modify the
  identity-key columns (the common case under the server-default
  identity). The CDC reader previously left `Before` nil, and
  the applier built `UPDATE t SET ... WHERE` with an empty
  predicate that Postgres rejects with "syntax error at end of
  input". The reader now synthesises a key-only `Before` from
  the after-tuple's identity columns. REPLICA IDENTITY NOTHING
  and tables without identity columns surface a clear error
  instead of a malformed statement.
- **MySQL `CURRENT_TIMESTAMP` default precision mismatch**: MySQL
  rejects `TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP` because the
  function-call precision must equal the column's. The most
  common path that hit this was a PG `TIMESTAMPTZ DEFAULT now()`
  migrating to MySQL — PG reports `Precision=6`, the translator
  turned `now()` into bare `CURRENT_TIMESTAMP`, leaving
  precisions mismatched. `emitDefault` now promotes a bare
  `CURRENT_TIMESTAMP` to `CURRENT_TIMESTAMP(N)` on a
  `TIMESTAMP`/`DATETIME`/`TIME` column with non-zero precision.
  Expressions that already carry an explicit precision pass
  through unchanged.

### Added — docs

- **`docs/postgres-source-prep.md`**: operator checklist for
  running sluice CDC against a Postgres source — required GUCs,
  connecting role attributes, slot lifecycle, `wal_status`
  recovery workflow, and the failover-survival mechanisms
  (Patroni `slots:`, PlanetScale "Logical slot name" UI,
  PG 17 `sync_replication_slots`). The PlanetScale section is
  load-bearing: slot loss on failover is silent without proper
  permanent-slots config.
- **README hero example** showing `migrate` / `sync start` /
  `sync status` end-to-end against the same DSN pair.
- **CONTRIBUTING test-tag layering**: documents the four build
  tags (default, integration, integration+postgis,
  integration+vstream, psverify) and which container images each
  pulls.

## [0.1.0] - 2026-05-03

The initial tagged release. Captures everything from the design
pass through the multi-shard Vitess + `sluice sync status`
chunks. Entries are grouped by capability rather than
chronologically; `git log` is the source of truth for commit-
level history.

### Added — orchestration

- **Simple-mode `Migrator`**: one-shot schema-and-data migration
  with three-phase apply (tables-without-constraints → bulk row
  copy → identity-sequence sync → indexes → foreign keys). Wired
  into the kong `migrate` subcommand. CLI signals (Ctrl-C) cancel
  cleanly via context.
- **Continuous-sync `Streamer`**: long-running snapshot+CDC
  orchestrator. Cold start captures a consistent snapshot, runs
  the bulk-copy phase, then tails CDC events through to a target
  `ChangeApplier`. Warm resume reads the persisted position from
  the target's control table and skips the snapshot phase
  entirely. Wired into the `sluice sync start` subcommand.
- **Translation layer (`internal/translate`)**: per-column
  type-override layer that consumes the `mappings:` block from
  `sluice.yaml` and rewrites column types in the IR before the
  schema-write phase sees them. Strict on missing tables/columns
  (typos surface as startup errors). Initial alias set covers
  `text`, `text_array`, `jsonb`, `json`, `bytea`, `varchar`
  (with optional `length` option), and the eight `postgis_*`
  geometry shapes (with optional `srid`).
- **`sluice sync status`** subcommand: prints every continuous-
  sync stream the target database has been the destination for
  (one row per `sluice_cdc_state` entry) with stream-id, last-
  updated wall-clock, human "5m ago" age, and a truncated
  position token. Filterable to a single stream via
  `--stream-id`. Tolerant of the target's control table being
  absent — operators querying status against a fresh target see
  "no streams recorded" rather than an error. Backed by a new
  `ChangeApplier.ListStreams` interface method, implemented on
  both MySQL and Postgres.

### Added — engines

- **MySQL engine** (vanilla, `mysql:` driver): SchemaReader,
  SchemaWriter, RowReader, RowWriter (LOAD DATA INFILE),
  CDCReader (row-based binlog via go-mysql), ChangeApplier,
  SnapshotStream (REPEATABLE READ + WITH CONSISTENT SNAPSHOT
  pinned to the binlog position).
- **PlanetScale MySQL flavor** (`planetscale:` driver): same code
  paths as vanilla, with a capability declaration that disables
  `LOAD DATA INFILE` (uses BatchedInsert), turns off
  user-defined partitioning, and selects the VStream gRPC
  protocol for CDC.
- **Postgres engine** (`postgres:` driver): SchemaReader,
  SchemaWriter (with three-phase apply, identity-sequence sync,
  PostGIS-aware geometry emission, MySQL SET → TEXT[] with a
  CHECK constraint), RowReader, RowWriter (COPY FROM STDIN),
  CDCReader (pgoutput logical replication via pglogrepl),
  ChangeApplier, SnapshotStream (CREATE_REPLICATION_SLOT +
  EXPORT_SNAPSHOT + SET TRANSACTION SNAPSHOT for atomic
  snapshot-to-CDC handoff).

### Added — managed-service support

- **PlanetScale Postgres** (PS-PG): the vanilla `postgres` engine
  works against PS-PG without code changes. All six verification
  phases pass against a real PS-PG account: connectivity, schema
  reader, simple-mode migration, CDC reader, snapshot+CDC
  streamer, and cross-engine PS-MySQL → PS-PG. See
  [docs/managed-services.md](docs/managed-services.md).
- **PlanetScale MySQL via VStream**: Vitess's gRPC streaming
  protocol is now sluice's CDC path for the PlanetScale flavor.
  Capability declaration declares `CDCVStream` so the streamer
  accepts the flavor. Position encoding is JSON `[]shardGtid`
  matching Debezium's persistence shape, future-proofing for
  multi-keyspace migrations.
- **Vanilla Vitess deployments**: the same `planetscale` flavor
  covers self-hosted Vitess, with DSN flags to opt out of
  PlanetScale-specific defaults: `vstream_transport=plaintext`,
  `vstream_auth=none`, `vstream_shards=<custom>`,
  `vstream_endpoint=<host:port>`. Verified against
  `vitess/vttestserver` via testcontainers.
- **Sharded Vitess keyspaces** are now supported: the VStream
  reader streams from N shards concurrently (per-shard cursor
  tracking is built into the `[]shardGtid` position), and the
  new `vstream_auto_discover_shards=true` DSN flag asks the
  reader to populate the layout via `SHOW VITESS_SHARDS LIKE
  '<keyspace>/%'` at Open time. Reshards are detected via the
  typed `ShardLayoutChangedError` (matchable with `errors.Is`
  against `ErrShardLayoutChanged`); callers resume on the new
  layout via `vstreamCDCReader.Reopen`. Validated against
  `vttestserver` with `NUM_SHARDS=2` (`-80,80-`).

### Added — types and translation policies

- **MySQL SET → PostgreSQL TEXT[]** (default policy): SET columns
  emerge on the target as `TEXT[]` with a table-level
  `CONSTRAINT <table>_<column>_set CHECK (... <@ ARRAY[...])`
  enforcing membership. Comma-separated MySQL DEFAULTs translate
  to PG array literals so the source default survives the
  rewrite.
- **PostGIS-aware GEOMETRY emission**: PG engine detects PostGIS
  at writer-open time. With the extension installed, ir.Geometry
  columns emit as `geometry(<subtype>, <srid>)`; without it the
  existing loud rejection persists (sluice doesn't auto-install
  extensions). MySQL SRID-prefixed WKB → PostGIS EWKB framing
  via `wkbToEWKB`. Per-column SRID flows through the translate
  layer's `postgis_*` aliases. The PG schema reader queries
  PostGIS's `geometry_columns` view at read time so geometry
  columns surface in the IR with their precise subtype + SRID
  (cleanly degrades to `GeometryUnspecified+SRID=0` when PostGIS
  isn't installed).
- **TRUNCATE detection in CDC** for both binlog and VStream
  paths. The narrow `parseTruncateTable` parser recognises
  `TRUNCATE [TABLE] [<schema>.]<table>` shapes and emits
  `ir.Truncate`; out-of-shape statements fall through to the
  cache-invalidation path.
- **MySQL TINYINT(1) → PG BOOLEAN** through both the snapshot
  bulk-copy path and the CDC stream, validated by the
  cross-engine integration test.
- **MySQL UNSIGNED BIGINT → PG NUMERIC(20,0)**, with auto-
  increment widening to BIGINT IDENTITY when applicable.
- **MySQL ENUM → PG enum type** with per-column generated type
  names, default-value casting handled inline.
- **MySQL JSON → PG JSONB** by default (canonical fast path);
  override to `json` (text) via mappings if needed.

### Added — testing

- **Integration suite** (`integration` build tag): testcontainers
  pairs cover MySQL→MySQL, PG→PG, MySQL→PG, PG→MySQL one-shot
  migrations, plus PG→PG and MySQL→PG continuous-sync streaming
  with restart-resume. The cross-engine seed exercises every
  type-translation policy in one fixture.
- **PostGIS suite** (`integration && postgis` build tag): boots
  `postgis/postgis:16-3.4`, exercises end-to-end MySQL → PG
  geometry round-trip with `ST_AsText` verification.
- **PlanetScale verification suite** (`psverify` build tag):
  exercises sluice's PG and MySQL paths against a real
  PlanetScale account using credentials from
  `PLANETSCALE_CREDENTIALS.env` or env vars. Includes
  connectivity probe (logs version, wal_level, REPLICATION
  attribute, PostGIS state), schema reader round-trip, simple-
  mode migration, CDC reader, continuous-sync streamer, and
  cross-engine verification. CI workflow at
  `.github/workflows/psverify.yml` (manual-trigger only).
- **VStream suite** (`integration && vstream` build tag):
  testcontainers-based against `vitess/vttestserver:mysql80`,
  exercises the FlavorPlanetScale CDC path against vanilla
  Vitess (plaintext + no-auth) including INSERT/UPDATE/DELETE
  and TRUNCATE.

### Added — CI

- Four-job CI workflow: cross-platform unit Test (Linux, macOS,
  Windows), Integration on Linux, Lint, and cross-platform
  Build smoke-test. Required for branch protection on main.
- Manual-trigger PlanetScale verification workflow with
  per-environment secrets for the four PS DSNs.

### Architecture and process

- 10 ADRs in [docs/adr/](docs/adr/) capture the load-bearing
  design decisions: IR-first translation, sealed interfaces,
  kong+koanf, three-phase schema apply, MySQL flavors, pgoutput
  over wal2json, position persistence on the target, go-mysql
  for binlog parsing, Streamer as separate orchestrator, and
  idempotent applier semantics.
- Documentation under [docs/](docs/): architecture overview,
  type-mapping policies, runtime value contract, testing guide,
  managed-services compatibility matrix, and a sakila-based
  end-to-end walkthrough.

### Removed

- The pre-translate placeholder mappings handling in `Migrator`
  and `Streamer`. Replaced by `translate.ApplyMappings` between
  schema-read and schema-write.

### Known limitations

(none currently — see the closed entries above.)

[Unreleased]: https://github.com/orware/sluice/compare/v0.7.0...HEAD
[0.7.0]: https://github.com/orware/sluice/releases/tag/v0.7.0
[0.6.0]: https://github.com/orware/sluice/releases/tag/v0.6.0
[0.5.2]: https://github.com/orware/sluice/releases/tag/v0.5.2
[0.5.1]: https://github.com/orware/sluice/releases/tag/v0.5.1
[0.5.0]: https://github.com/orware/sluice/releases/tag/v0.5.0
[0.4.0]: https://github.com/orware/sluice/releases/tag/v0.4.0
[0.3.2]: https://github.com/orware/sluice/releases/tag/v0.3.2
[0.3.1]: https://github.com/orware/sluice/releases/tag/v0.3.1
[0.3.0]: https://github.com/orware/sluice/releases/tag/v0.3.0
[0.2.2]: https://github.com/orware/sluice/releases/tag/v0.2.2
[0.2.1]: https://github.com/orware/sluice/releases/tag/v0.2.1
[0.2.0]: https://github.com/orware/sluice/releases/tag/v0.2.0
[0.1.0]: https://github.com/orware/sluice/releases/tag/v0.1.0
