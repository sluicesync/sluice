# ADR-0169: `mariadb` flavor — Phase-2 type fidelity (JSON identity, native uuid/inet, geometry SRID, invisible-object census)

## Status

**Accepted (2026-07-17).** Roadmap item 73 Phase 2, building on ADR-0168 (Phase 1). Every convention this ADR encodes was re-ground-truthed on live `mariadb:11.4` and `mariadb:10.11` containers during implementation and is pinned by unit family-matrices plus an integration matrix on both LTS lines (same-engine reads) and a mariadb→PG cross-engine round-trip.

## Context

Phase 1 (ADR-0168) shipped bulk migrate/backup/verify with honest, deliberately narrow capabilities: `JSONSupport: JSONText` but JSON columns read as plain `longtext`; `ExtGeometry` excluded; native uuid/inet types loud-failing; system-versioned tables and sequences refused as a census. Phase 2 closes the MariaDB-specific *type fidelity* gaps behind those Phase-1 placeholders. None of them was a silent-loss path in Phase 1 (each failed loudly or degraded visibly) — Phase 2 is about faithfulness and reach, not fixing corruption.

## Decision

Four items, each MariaDB-flavor-gated (the MySQL-8 paths stay byte-identical).

### 1. JSON identity via the auto `json_valid` CHECK

MariaDB's `JSON` type is a `LONGTEXT` alias: `information_schema.columns.data_type` reports `longtext` and MariaDB auto-generates a CHECK constraint `json_valid(<col>)`. Both facts are what distinguish a JSON column from a plain LONGTEXT, so — after BOTH columns and checks are populated — the mariadb reader (`recoverMariaDBJSONColumns`) keys on the pair: a `longtext` column whose **only** CHECK is **exactly** `json_valid(<that column>)` is

- remapped to `ir.JSON{Binary: false}` — honest, because MariaDB JSON is textual (matches the flavor's `JSONText` capability and PG `json`, not `jsonb`), and
- has that auto-CHECK **stripped** from `ir.Table.CheckConstraints`. It is MariaDB-internal metadata: re-emitting it would land an invalid `json_valid()` CHECK on a PG target (PG has no such function — Phase 1's exact loud-failure) and a redundant one on a MySQL/MariaDB JSON column.

The match is precise (`isMariaDBAutoJSONValidCheck`): a single `json_valid(<bare identifier>)` call with no trailing content — a user's `json_valid(js) AND length(js) > 2`, or a `json_valid(other_col)`, or a non-longtext column is deliberately **not** detected and keeps both its type and its CHECK. Because MariaDB's `JSON` and a hand-written `LONGTEXT CHECK (json_valid(x))` are byte-identical in the catalog, treating the latter as JSON too is the faithful reading, not a guess.

**`Binary: false` rationale.** MariaDB has no binary JSON storage — it is text end to end. `false` is honest about the source, keeps the value contract textual, and lands PG `json` (not `jsonb`); a MySQL-8 or MariaDB target still emits `JSON` (the emitter ignores `Binary` on MySQL-family targets). Declaring `Binary: true` would over-claim a binary representation the source doesn't have.

### 2. Native uuid / inet6 / inet4

`translateType` gains `uuid`→`ir.UUID{}` and `inet4`/`inet6`→`ir.Inet{}`. INET4 and INET6 both collapse to `ir.Inet{}`: the IR has no IPv4-only variant, and MariaDB's INET4 is a storage optimisation over INET6, not a distinct value space — the address round-trips losslessly as canonical text. These data_type strings are MariaDB-only (MySQL 8 never emits them), so the additions sit safely in the shared switch. The flavor declares `ExtUUID` and `ExtInet`.

Cross-engine emit is the established v0.7.0 auto-emit policy, verified: a PG target lands native `uuid`/`inet`; a MySQL-family target auto-emits `CHAR(36)`/`VARCHAR(45)` (surfaced by `schema preview`, not silent). A **same-engine mariadb** target currently also lands `CHAR(36)`/`VARCHAR(45)` — values round-trip losslessly, but the native type name is not preserved; native-type emit on a mariadb target is a deliberate future refinement, noted here so the `SupportedTypes` claim is read as "reads natively + carries losslessly", not "emits the native MariaDB type on a mariadb target".

### 3. System-versioned tables + SEQUENCEs — census stays a LOUD refusal

Both remain a loud, coded read-time refusal (`refuseInvisibleMariaDBTables`), upgraded to a proper census with **per-class remedies**:

- **SYSTEM VERSIONED** tables carry temporal history (period columns + historical row-versions). Carrying one as a plain current-state table would silently drop that history — the first-tenet silent-loss class. Remedy named: `ALTER TABLE <t> DROP SYSTEM VERSIONING` (keeps current rows) or exclude from scope.
- **SEQUENCE** objects have no representation on any MySQL-family target. The IR *does* have a first-class `Sequence` surface (`Schema.Sequences`) — but it is **PG-only**: the MySQL/MariaDB writer refuses a non-empty `Schema.Sequences`, and the cross-engine PG-sequence→MySQL path refuses too (`migcore.CheckCrossEngineSupportable`). So mapping a MariaDB sequence into that surface would only relocate the loud failure from read time to emit time on the common target, while adding an unverified MariaDB-sequence-semantics read. Half-mapping is worse UX than one clear upfront refusal. Kept a loud read-time refusal; remedy named (drop the sequence + the referencing columns, or migrate the sequence-dependent portion to a PG target).

**Why no partial support:** silently dropping temporal history or a sequence's topology is exactly the class this refusal exists to prevent. "Do not half-map" was the explicit instruction, and the IR/target-writer reality confirms it.

### 4. POINT SRID — read from `GEOMETRY_COLUMNS`, write as `REF_SYSTEM_ID`

`ir.ExtGeometry` is now declared for the flavor, with the SRID round-trip closed on BOTH read and write:

- **Read.** Recovering the per-column SRID required a **ground-truth-corrected deviation from the roadmap draft** (see below): the mariadb reader reads SRIDs from `information_schema.GEOMETRY_COLUMNS` (the OGC-standard view; `SRID` keyed by `G_TABLE_SCHEMA` / `G_TABLE_NAME` / `G_GEOMETRY_COLUMN`) and backfills `ir.Geometry.SRID` after `populateColumns` (`populateMariaDBGeometrySRID`). One query covers the whole schema; a column with no entry keeps SRID 0 ("no spatial reference declared").
- **Write (F3, review follow-up).** A mariadb-flavor emitter renders a non-zero SRID as the `REF_SYSTEM_ID=<n>` TYPE attribute — which the MariaDB grammar requires BEFORE `NOT NULL` (it is a syntax error after it) — and skips the MySQL-8 `SRID <n>` clause (which MariaDB rejects outright). The emitter carries the target flavor (`mysqlEmitter.flavor`, set by `newMySQLEmitterForFlavor`); the zero value is `FlavorVanilla`, so `stdEmitter` and every unit/preview construction keep the MySQL-8 `SRID <n>` form byte-identically. This closes the round-trip: a PG (PostGIS) `geometry(POINT, 4326)` lands on a MariaDB target with the SRID preserved (`GEOMETRY_COLUMNS.SRID` and each value's `ST_SRID` both 4326), and a MariaDB geometry column feeds its SRID into a PG target. Preferred emit-branch path taken (well under the ~1hr fallback threshold; the emitter already had a flavor-derived field for the collation remap, so seeing the target flavor was clean). Pinned live both directions (`postgis` build tag).

This closes the Phase-1 silent-SRID-drop class that motivated excluding geometry — on read AND write.

### Bug 198 — MariaDB check-constraint join fan-out (bundled into P2)

`populateCheckConstraints` joins `information_schema.check_constraints` ↔ `table_constraints` on `(constraint_schema, constraint_name)`. MySQL-8 constraint names are unique per schema (1:1), but **MariaDB names are unique only per table**, so two tables in one database sharing a check-constraint name fan the join out — each table captured every same-named CHECK once per sharing table, and could cross-contaminate (table `a` capturing table `b`'s CHECK). Because MariaDB spells `JSON` as `longtext CHECK(json_valid(<col>))` **named after the column**, this fired for the ubiquitous case of two tables with a same-named JSON column (`meta`/`data`/`payload`). CREATE TABLE then emitted duplicate CHECKs and the target refused loudly (Error 1826 on MySQL/MariaDB, SQLSTATE 42710 on PG) — loud, not silent, but a real HIGH.

The fix is **flavor-gated** (`checkConstraintsQuery(flavor)`, mirroring `columnsQuery`/`indexesQuery`): MariaDB's `check_constraints` DOES carry `TABLE_NAME`, so its variant adds `AND cc.table_name = tc.table_name` to the join, restoring the 1:1. MySQL 8's `check_constraints` has **no `TABLE_NAME` column** (verified live) — referencing it there is a hard SQL error — so the MySQL-8 query is left byte-identical to the historical constant (unit-pinned). The fix runs BEFORE `recoverMariaDBJSONColumns`, so the JSON strip operates on a clean 1:1 CHECK set. Pinned on 11.4 + 10.11: two tables sharing a non-JSON CHECK → each captured exactly once (2 without the fix); two tables sharing a JSON `meta` column → each detected as JSON with the auto-CHECK stripped, no cross-contamination; same-engine and MySQL-8-target migrates succeed.

## Deviation from the roadmap draft (ground truth)

Roadmap item 73 P2 and ADR-0168 both anticipated recovering the geometry SRID by parsing `REF_SYSTEM_ID=n` out of `SHOW CREATE TABLE`. **Live probing (mariadb:11.4 and 10.11) proved that wrong on two counts:**

1. MariaDB **does not echo `REF_SYSTEM_ID` in `SHOW CREATE TABLE`** — a `POINT REF_SYSTEM_ID=4326` column shows as bare `` `p` point DEFAULT NULL ``. Parsing SHOW CREATE would always recover SRID 0.
2. MariaDB's SRID is not enforced as a MySQL-8-style per-column constraint (a wrong-SRID value inserts fine; ST_SRID is per-value), but the declared column SRID **is** recorded — in `information_schema.GEOMETRY_COLUMNS.SRID`, present and correct on both LTS lines.

The implementation therefore uses `GEOMETRY_COLUMNS` (also cleaner: one schema-wide query, no per-table SHOW CREATE). This was caught by the mandatory integration ground-truth (a POINT with SRID 4326 read back as 0 under the SHOW-CREATE approach) — exactly the "must be integration-proven, not unit-mocked" discipline the task required.

## Alternatives considered

- **Parse `REF_SYSTEM_ID` from SHOW CREATE (roadmap draft).** Rejected on ground truth — MariaDB does not emit it (see above).
- **Map MariaDB sequences into `ir.Schema.Sequences`.** Rejected — the surface is PG-only; the common MySQL-family target refuses it, so this relocates the failure and adds unverified read semantics. Loud refusal with a remedy is the proportional close.
- **`ir.JSON{Binary: true}` for MariaDB JSON.** Rejected — MariaDB JSON is textual; `true` over-claims binary storage and would land PG `jsonb`.
- **Native uuid/inet emit on a mariadb target.** Deferred — the type emitter now carries the target flavor (added for the geometry `REF_SYSTEM_ID` spelling), so this is newly feasible, but native UUID/INET6/INET4 emit on a mariadb target is out of Phase-2 scope; values already round-trip losslessly as CHAR(36)/VARCHAR(45). Filed as a future refinement.
- **Coded loud-refusal fallback for non-zero-SRID geometry on a mariadb target (F3 fallback).** Not needed — the `REF_SYSTEM_ID` emit branch was clean and cheap (the emitter already had a flavor field), so the round-trip is closed rather than refused.
- **Adding `mariadb` to `migcore.IsMySQLFamilyEngine` as a hand-kept list entry (F1).** Rejected in favour of DELEGATING to `translate.IsMySQLFamily` (the single, registry-parity-tested source of truth) — a hand-kept second list is exactly what drifted and caused the silent-loss. A registry-parity test now fails CI if any MySQL-dialect engine misses it.

### F1 — CRITICAL silent-loss closed: PG→mariadb cross-engine refusals

`migcore.IsMySQLFamilyEngine` (the target-family gate in `CheckCrossEngineSupportable`) had EXCLUDED `mariadb` while `translate.IsMySQLFamily` included it. A PG→mariadb `migrate` therefore computed `pgToMySQL = isPGSource && IsMySQLFamilyEngine(target) = true && false = false`, skipped every PG-native refusal, and the mariadb writer then **silently dropped an EXCLUDE constraint** (CreateConstraints iterates only ForeignKeys; count-based verify is blind to it) — the exact vitess-precedent silent-loss class. Fixed by **delegating** `IsMySQLFamilyEngine` to `translate.IsMySQLFamily`, the single registry-parity-tested source of truth, and converting the hand-kept `TestIsMySQLFamilyEngine` to a registry-parity test in the external `migcore_test` package (mirroring `translate`'s). A future MySQL-dialect engine that misses the branch now fails CI instead of reopening the vector. This also resolves F2 (a standalone PG sequence was silently dropped on the same path — `PrimeSequences` runs only on cutover). Pinned: unit (EXCLUDE + sequence, mariadb + postgres-trigger sources) and integration (PG→mariadb `migrate` refuses loudly for both).

## Consequences

- MariaDB JSON columns migrate faithfully to PG `json` (the auto-CHECK no longer breaks the migrate) and to MySQL/MariaDB `JSON`; the Phase-1 "no JSON column in the mariadb→PG corpus" caveat is lifted.
- Native `uuid`/`inet6`/`inet4` columns read and cross-engine-migrate (PG native uuid/inet — with NULLs and the PG full-host mask pinned; MySQL-family CHAR(36)/VARCHAR(45)).
- Geometry columns carry their SRID cross-engine BOTH ways (`geometry(POINT, 4326)` ↔ MariaDB `REF_SYSTEM_ID=4326`) — value + SRID pinned live in both directions plus the read-side family × {SRID 0, 4326} on both LTS lines.
- PG→mariadb migrate refuses loudly for PG-only shapes (EXCLUDE, standalone sequence, PostGIS opclasses) instead of silently dropping them (F1/F2).
- The MariaDB check-constraint fan-out is fixed (Bug 198), flavor-gated so the MySQL-8 path is byte-identical.
- System-versioned tables and sequences still refuse loudly, now with per-class actionable remedies.
- Phase 3 (domain-GTID CDC) remains roadmap item 73. Native-type emit on a mariadb target is a filed refinement (the emitter now carries the target flavor, so it is newly feasible).
