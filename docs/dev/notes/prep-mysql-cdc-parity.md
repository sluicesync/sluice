# Prep: MySQL CDC parity gaps

> **Status: SHIPPED.** All three items (TRUNCATE detection, MySQL restart-resume integration test, MySQL → PG cross-engine continuous-sync test) landed and are referenced as canonical across the roadmap.

Roadmap reference: not in the original roadmap. Surfaces from the post-§8 audit of MySQL CDC completeness — three known punts left from §2 (MySQL CDC reader) and §4 (snapshot+CDC handoff) where Postgres got coverage and MySQL didn't.

## Goal

Close three asymmetries between the MySQL and Postgres CDC paths that have accumulated through §2-§7. Each is small on its own; bundled because they share a context (MySQL CDC is feature-complete but the test coverage and the TRUNCATE handling lag PG's symmetry).

The three gaps:

1. **`ir.Truncate` emission for MySQL.** PG's pgoutput protocol emits a typed `TruncateMessage` and the §3 reader translates it to `ir.Truncate`. MySQL embeds `TRUNCATE TABLE foo` inside a generic `QUERY_EVENT` carrying the SQL text; the §2 reader treats every non-BEGIN/COMMIT QUERY_EVENT as schema-cache-invalidation only and emits no `ir.Change`. Result: MySQL→PG continuous-sync silently drops TRUNCATE events. The applier (§5) handles `ir.Truncate` correctly when it arrives; the gap is purely on the read side.

2. **Restart-resume integration test for MySQL.** The §5 test (`TestStreamer_RestartResume_PostgresToPostgres`) proves the warm-resume path works against PG. The same orchestrator code paths run for MySQL, but we have no integration test that proves it.

3. **Cross-engine continuous-sync test (MySQL → PG).** §4 shipped same-engine snapshot+CDC tests for both engines, plus a same-engine PG→PG end-to-end Streamer test. Cross-engine snapshot+CDC (snapshot from MySQL, stream MySQL CDC, apply to PG) hasn't been exercised end-to-end.

Out of scope:

- **DDL replay generally.** Translating arbitrary DDL across engines (CREATE TABLE, ALTER TABLE, DROP INDEX) is post-v1. This chunk only handles TRUNCATE because it's a row-level state change masquerading as DDL — the IR already models it as `ir.Truncate`, the applier already knows how to apply it, and the only missing piece is the source-side detection.
- **MySQL CDC against PlanetScale.** That's §3b (VStream); the binlog-based reader can't be made to work against PlanetScale because PlanetScale doesn't expose binlog.

## Item 1: TRUNCATE detection in MySQL CDC

The §2 prep doc punted this with the rationale "MySQL's binlog represents `TRUNCATE TABLE foo` as a `QUERY_EVENT` containing the SQL text. Recognising it would require parsing the DDL string, which the project tenets reject."

That's broadly right — but TRUNCATE is the narrow exception worth making. The prep doc itself flagged it: "If a future use case needs `ir.Truncate` we can add a narrow string-prefix check (`strings.HasPrefix(strings.TrimSpace(strings.ToUpper(q)), "TRUNCATE")`) — that's bounded and tenet-compatible."

Implementation:

- In `internal/engines/mysql/cdc_reader.go`'s QUERY_EVENT dispatch (currently `case *replication.QueryEvent` in `dispatch`):
  - Extract the query text.
  - Trim leading whitespace, uppercase the first token.
  - If it starts with `TRUNCATE TABLE` (with optional schema-qualified table reference), parse out the table name with a small bounded helper (split on whitespace, strip backticks, handle `schema.table` form), emit `ir.Truncate{Position, Schema, Table}`.
  - All other QUERY_EVENTs continue to be treated as DDL → schema cache invalidation, no event emitted.
- The string parsing is **not** general DDL parsing. It's the same shape as `strings.HasPrefix(strings.ToUpper(strings.TrimSpace(q)), "TRUNCATE TABLE ")`. Tenet-compatible because it's bounded, well-documented, and unambiguous.

Tests:
- Unit test for the parser: `TRUNCATE TABLE foo`, `truncate table  schema.foo`, `TRUNCATE TABLE \`foo\``, `TRUNCATE foo` (the optional-TABLE form), and negative cases (`CREATE TABLE`, `DROP TABLE`).
- Integration test: append to the existing CDC integration test or add a new one that issues `TRUNCATE TABLE users` and asserts `ir.Truncate` arrives.

## Item 2: MySQL restart-resume integration test

Mirror `internal/pipeline/streamer_resume_integration_test.go` (PG-only) with a MySQL counterpart. Same flow:

```
Phase A: Setup MySQL container with binlog enabled
Phase B: Cold-start streamer; bulk-copy R1; CDC delivers R2
Phase C: Cancel ctx → simulated crash
Phase D: Verify control table has a position for the StreamID
Phase E: Restart streamer with same StreamID; row count stays stable for 3s
Phase F: Insert R3; verify it flows through CDC
Phase G: Final state has {R1, R2, R3} exactly once
```

Same-engine MySQL → MySQL. The streamer code is engine-agnostic so this should pass without code changes. If it doesn't, the surprise is the deliverable — we'd find an engine-specific issue we missed.

File: `internal/pipeline/streamer_resume_mysql_integration_test.go`. Helpers (`waitForRowCount`, `pollRowCount`, etc.) currently live in the PG-flavored test file; either generalise them to a shared helper file or duplicate (small enough to be fine).

## Item 3: MySQL → PG cross-engine continuous-sync test

Existing: `TestMigrate_MySQLToPostgres` (simple-mode bulk migration). We don't have a continuous-sync analogue.

The shape:

```
Phase A: Boot MySQL with binlog + Postgres with wal_level=logical
Phase B: Seed MySQL with R1
Phase C: Streamer{Source: mysqlEng, Target: pgEng} runs
   - Bulk-copies R1 from MySQL to PG (verifies cross-engine bulk path)
   - Starts CDC from MySQL binlog
   - Applies events to PG via PG ChangeApplier (verifies cross-engine apply)
Phase D: Insert R2 on MySQL → verify it arrives on PG
Phase E: Update R1 on MySQL → verify it lands on PG
Phase F: Delete R2 on MySQL → verify it's gone from PG
Phase G: Cancel ctx; verify clean shutdown
```

Single test file, ~250 lines. Highest-value test we can write: it validates the entire continuous-sync stack (snapshot capture, bulk copy, CDC stream, applier) across the most common cross-engine direction.

File: `internal/pipeline/streamer_cross_integration_test.go`.

## Files to add / touch

- `internal/engines/mysql/cdc_reader.go` — TRUNCATE detection in QueryEvent dispatch (~30 lines added).
- `internal/engines/mysql/cdc_reader_test.go` — TRUNCATE parser unit tests (~60 lines added).
- `internal/engines/mysql/cdc_reader_integration_test.go` — TRUNCATE end-to-end test (~40 lines added).
- `internal/pipeline/streamer_resume_mysql_integration_test.go` — new (~200 lines).
- `internal/pipeline/streamer_cross_integration_test.go` — new (~250 lines).

~580 lines net. Mostly tests.

## Anticipated rough edges

- **Quoted identifiers in TRUNCATE.** `TRUNCATE TABLE \`foo\`` and `TRUNCATE TABLE \`schema\`.\`foo\`` need the backtick stripping to work right. Easy to test, easy to get wrong.
- **MySQL binlog format dependency.** TRUNCATE only appears as `QUERY_EVENT` with text in `binlog_format=ROW` (which we already require for CDC). The integration test should confirm with `SHOW VARIABLES LIKE 'binlog_format'` if it needs to.
- **Cross-engine MySQL → PG identity columns.** The applier-side test needs to handle the MySQL `BIGINT AUTO_INCREMENT` → PG `BIGINT IDENTITY` translation in the bulk-copy phase. §7's identity-sync phase 3.5 ensures the PG sequence advances past the bulk-copied IDs; the cross-engine test exercises this end-to-end.
- **Replication slot persistence between cold-start and warm-resume in MySQL.** Unlike PG (which has slots), MySQL's binlog position is just a file/offset or GTID — no server-side state to clean up between test runs. Containers are fresh per-test so this is moot, but worth a comment.
- **TRUNCATE on a table the source schema-cache doesn't know about.** Edge case: TRUNCATE arrives before any DML touched the table, so the cache has no entry. The existing dispatch falls through to schema invalidation; with TRUNCATE detection, we emit `ir.Truncate` directly without a cache lookup (the schema/table name comes from the parsed SQL, not from the binlog table_id).

## Open questions for the user

1. **TRUNCATE parser scope.** Just `TRUNCATE TABLE` and `TRUNCATE` (the optional-TABLE form), or also handle `TRUNCATE schema.table` cross-database TRUNCATEs? *Recommendation:* support `TRUNCATE [TABLE] [schema.]table` with backtick handling. Confirm?
2. **MySQL → PG cross-engine continuous-sync test scope.** Same conservative seed as the simple-mode cross-engine test (one table, BIGINT auto-increment, VARCHAR, BOOLEAN, FK), or a shorter shape just for the streaming spine? *Recommendation:* shorter shape — single `users` table with `(id, email, active)`, no FK. The simple-mode test already covers the FK translation; the streamer test focuses on the streaming spine. Confirm?
3. **Helper file layout.** Generalise `waitForRowCount`, `pollRowCount` etc. into a shared `internal/pipeline/streamer_test_helpers.go` (with the integration build tag), or duplicate per test file? *Recommendation:* generalise — the helpers are short and reuse is honest. Confirm?

## Suggested first-cut prompt

> "Read CLAUDE.md, docs/dev/notes/prep-mysql-cdc-parity.md, and internal/engines/mysql/cdc_reader.go. Propose the design before writing: (1) the exact TRUNCATE-parser helper signature and unit-test shape, (2) the dispatch insertion point in cdc_reader.go's QueryEvent branch, (3) the restart-resume MySQL test structure, (4) the MySQL → PG cross-engine streaming test structure. Note any deviation from the prep doc with a why. Stop after the design for review."
