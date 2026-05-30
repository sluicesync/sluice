# Broader test-gap mining — pg_dump / Debezium / wal2json / pgcopydb+Bucardo deeper

Author: research scratch, 2026-05-29.

Third upstream-research pass; the first two live at
`docs/dev/notes/pgcopydb-planetscale-fork-review.md` and
`docs/dev/notes/upstream-pgcopydb-bucardo-review.md`. Those notes surfaced
adoptions (a) `json` equality cast **[MERGED, PR #92]**, (b) NOT VALID FK
retry **[MERGED, PR #93]**, (c) XID-wraparound preflight **[queued]**, plus
three test-coverage gaps (multi-byte UTF-8 identifiers, REPLICA IDENTITY
USING INDEX UPDATE, GENERATED ALWAYS AS IDENTITY apply) which are queued
and **not** re-recommended.

Sources for this pass:

- **PostgreSQL `src/bin/pg_dump/t/`** — listing read via
  `gh api repos/postgres/postgres/contents/src/bin/pg_dump/t`. Read in
  catalog-summary mode: `001_basic.pl`, `002_pg_dump.pl` (the long one),
  `005_pg_dump_filterfile.pl`. Other files (`003_…with_server.pl`,
  `004_…parallel.pl`, `006_…compress.pl`, `007_…dumpall.pl`,
  `010_dump_connstr.pl`) listed only — they pin plumbing concerns
  (transport / compression / connstr parsing) that don't translate to
  sluice's IR-first model.
- **Debezium PG connector** — listing read via
  `gh api repos/debezium/debezium/contents/debezium-connector-postgres/src/test/java/io/debezium/connector/postgresql`.
  Read in detail: `PostgresDefaultValueConverterIT`, `PostgresEnumIT`,
  `PostgresTemporalPrecisionHandlingIT`, `TablesWithoutPrimaryKeyIT`,
  `DomainTypesIT`, `PostgresMoneyIT`, `PostgresReselectColumnsProcessorIT`,
  `SnapshotIsolationIT`, `PostgresSkipMessagesWithoutChangeConfigIT`,
  `LogicalDecodingMessageIT`. Listed only (no read): the giant ITs
  (`PostgresConnectorIT` 223k bytes, `RecordsStreamProducerIT` 252k bytes),
  Java-only concerns (`OpenLineageIT`, `DebeziumEngineIT`,
  `IncrementalSnapshotIT`, vector DBs, outbox-router, custom-snapshotter
  SPI) — Java threading models and Kafka Connect plumbing don't translate.
- **wal2json `sql/`** — listing read via
  `gh api repos/eulerto/wal2json/contents/sql`. Read in full:
  `toast.sql`, `specialvalue.sql`, `typmod.sql`, `savepoint.sql`,
  `rename_column.sql`, `numeric_data_types_as_string.sql`,
  `include_domain_data_type.sql`, `message.sql`, `update2.sql`,
  `update3.sql`, `update4.sql`, `delete2.sql`, `delete3.sql`,
  `insert1.sql`, `include_lsn.sql`, `bytea.sql`, `actions.sql`. Listed
  only: `default.sql`, `delete1.sql`, `delete4.sql`, `filtertable.sql`,
  `selecttable.sql`, `pk.sql`, `position.sql`, `type_oid.sql`,
  `include_xids.sql`, `include_timestamp.sql`, `cmdline.sql`,
  `truncate.sql`, `update1.sql` — covered structurally by what we read.
- **pgcopydb deeper** — re-read `cdc-low-level/{ddl,dml}.sql`,
  `follow-data-only/multi-wal-txn.sql`,
  `cdc-endpos-between-transaction/dml1.sql`. The prior pass had skimmed
  the LO-level `ddl.sql` for the 32 KB-NOTICE-payload trick.
- **Bucardo deeper** — read `30-delta.t`, `30-crash.t`, `50-star.t`,
  `10-makedelta.t` in full. Skip on `20-{drizzle,firebird,mariadb,mongo,
  mysql,oracle,redis,sqlite}.t` (per prior-pass ruling) and on
  `40-{customcode-exception,conflict,serializable}.t` (custom Perl callback
  model + multi-master + already covered by the prior pass on `40-serializable.t`).

## 1. TL;DR

- **The richest seam is wal2json + Debezium temporal/special-value testing**,
  not pg_dump. pg_dump's tests pin DDL *shapes* that sluice doesn't aspire
  to (event triggers, large objects, extensions-as-first-class) — most of
  its corpus is out-of-scope. wal2json and Debezium pin **per-value
  semantics on the CDC wire** which is exactly where silent-loss bugs land.
- **Top three verified gaps** (each ranked by silent-loss weight × likelihood):
  (1) `'infinity'` / `'-infinity'` / `'NaN'` on `timestamp`, `timestamptz`,
  `numeric`, `float8` end-to-end (init load *and* CDC apply), (2) Large
  TOAST round-trip through CDC where the unchanged-TOAST omit semantics
  are correct in sluice's decoder but only unit-tested — there is no
  integration test that proves a multi-KB TOASTed column survives an UPDATE
  to a sibling column with the target's TOAST preserved, (3) `ALTER TYPE
  … ADD VALUE` ENUM-value drift mid-stream (snapshot reads enum values,
  source adds new label, CDC delivers it before target schema catches up
  → silent insert of unknown enum on a PG target with a CHECK-style
  enforcement or a hard fail on enum-as-bytes path).
- **Smaller but real gaps:** PG `MONEY` type round-trip
  (locale-dependent parsing — neither PG nor MySQL drivers ground-truth
  this), DOMAIN-type-as-array round-trip (Debezium hit this in DBZ-3657),
  `pg_logical_emit_message` non-transactional records appearing on the
  wire (sluice's stream filter), bytea hex vs escape format on the source
  side, idempotent UPDATE under REPLICA IDENTITY FULL (`SKIP_MESSAGES_WITHOUT_CHANGE`
  test — sluice currently re-applies them, may be correct but unverified).
- **Comparison vs prior two passes:** prior passes surfaced 3 high-leverage
  adoptions + 3 test-coverage gaps. This pass surfaces 0 code adoptions
  (none of the projects do anything structurally sluice doesn't) and
  ~8 test-coverage gaps, of which 5 are high-conviction and 3 are
  belt-and-suspenders confirmations. The volume reflects that **silent-loss
  hides in the value layer**, where wal2json/Debezium have decades more
  ground-truth than pgcopydb's per-row CSV path or Bucardo's PG-only delta
  trigger path.
- **Honesty note:** pg_dump's tests are well-engineered but most of their
  shapes (event triggers, FDW, casts, transforms, language handlers, base
  types with custom I/O) sit far outside sluice's IR translation policy.
  The pg_dump pass is mostly a confirmation that sluice's IR is correctly
  scoped, not a source of new pins.

## 2. Per-project findings

### 2a. PostgreSQL `pg_dump` test suite

Bottom line: **out-of-scope dominates**. pg_dump's `002_pg_dump.pl` covers
roughly 40 distinct DDL shape categories (composite/range/enum/domain
types, partitioned tables, inheritance, identity columns + cycle/cache,
exclusion constraints, expression indexes with opclass + INCLUDE,
publications/subscriptions, large objects, FDW, transforms, text-search,
ACLs, RLS, comments-on-everything). About half don't intersect sluice's
translation policy at all (FDW, transforms, language handlers, custom
casts, text-search configs, large objects, custom base types, event
triggers, ACLs/role tree — sluice's IR explicitly excludes these).

The shapes that **do** intersect sluice's surface and are worth pinning:

| Shape | URL | Sluice coverage | Verdict |
|---|---|---|---|
| Composite types (`CREATE TYPE … AS (a int, b text)`) | https://github.com/postgres/postgres/blob/master/src/bin/pg_dump/t/002_pg_dump.pl | Not covered. `Grep` for `composite type\|CREATE TYPE.*AS \(` → no hits in `internal/`. | **Gap candidate** — see §3 |
| Range types (`int4range`, `tstzrange`, custom range type) | same | Not covered. `Grep` for `tstzrange\|int4range\|daterange` → no hits. | **Gap candidate** — see §3 |
| `GENERATED ALWAYS AS IDENTITY` with custom start / cycle / cache | same | `migrate_generated_integration_test.go` covers STORED; identity-on-apply queued by the prior pass. The **start-value / cycle / cache** axis (PG sequence options) is independently relevant — does sluice preserve `START 1000000000000`? | **Verify-and-pin** — see §3 |
| Partial / expression indexes with `INCLUDE` columns | same | `ddl_emit.go` emits these; corpus tests exercise common shapes. INCLUDE + WHERE + opclass combo unclear. | **Already-have (probably)** — see §4 |
| `DEFAULT` containing function calls (`now()`, `gen_random_uuid()`) — versus literal-string defaults (the famous `DEFAULT 'NULL'` vs `DEFAULT NULL::text` trap) | same; Debezium `PostgresDefaultValueConverterIT` echoes this | `Grep` for `gen_random_uuid` → hits in `migrate_uuidossp_pgcrypto_integration_test.go` — covered for that family. Literal-string `'NULL'` vs typed `NULL` cast is not pinned. | **Gap candidate (low priority)** — see §4 |
| Inheritance (`CREATE TABLE child () INHERITS (parent)`) | same | Not exercised; sluice's schema reader is `relkind='r'` flat. | **Out of scope** (sluice declines to model PG inheritance) |
| Filter-file syntax with quoted multi-byte identifiers (`005_pg_dump_filterfile.pl`) | https://github.com/postgres/postgres/blob/master/src/bin/pg_dump/t/005_pg_dump_filterfile.pl | sluice has filter machinery (`internal/pipeline/filter.go`); prior-pass identified UTF-8 identifier round-trip as a gap, this confirms it. | **Already in queue** (gap #1 from prior pass) |

### 2b. Debezium PG connector tests

Bottom line: **the strongest signal of the four projects** for sluice's
silent-loss surface. Debezium is the largest CDC-on-PG codebase by usage
and their per-test scenarios are tightly scoped to value-layer edge cases.

| Test | URL | Scenario | Sluice coverage | Verdict |
|---|---|---|---|---|
| `PostgresTemporalPrecisionHandlingIT` | https://github.com/debezium/debezium/blob/main/debezium-connector-postgres/src/test/java/io/debezium/connector/postgresql/PostgresTemporalPrecisionHandlingIT.java | `'infinity'` / `'-infinity'` on timestamp, `'NaN'` on numeric, microsecond precision across `TIMESTAMP(0..6)`, timezone conversion, DELETE under REPLICA IDENTITY FULL with infinity values | sluice's `migrate_temporal_precision_integration_test.go` covers `TIMESTAMP(0..6)` clean precision; `Grep` for `infinity\|'-infinity'\|'NaN'` → **no hits**. | **High-priority gap** — see §3 #1 |
| `PostgresReselectColumnsProcessorIT` (TOAST) | https://github.com/debezium/debezium/blob/main/debezium-connector-postgres/src/test/java/io/debezium/connector/postgresql/PostgresReselectColumnsProcessorIT.java | UPDATE one column on a row with a 10 KB+ TOASTed sibling column. The TOAST marker arrives as "unchanged toast"; the connector reselects from source. sluice doesn't reselect — it relies on "absent key in row = preserve target value", which is the right semantics, but unverified at integration level | `Grep` for `'u'` byte handling: present in `cdc_reader.go:1454` and unit-tested in `cdc_reader_test.go:384`. **No integration test** exercising actual multi-KB TOAST + sibling UPDATE end-to-end. | **High-priority gap** — see §3 #2 |
| `PostgresEnumIT` | https://github.com/debezium/debezium/blob/main/debezium-connector-postgres/src/test/java/io/debezium/connector/postgresql/PostgresEnumIT.java | ENUM type resolution across schemas; same name in `public` and `test`; ENUM vs DOMAIN of same name. Notably does **not** test `ALTER TYPE … ADD VALUE` mid-stream | `migrate_pg_fidelity_integration_test.go` covers enum migration; **no test** for `ALTER TYPE … ADD VALUE` mid-stream drift (snapshot reads `{draft,published}`, source adds `archived`, CDC delivers a row with `archived` before target's schema catches up). | **High-priority gap** — see §3 #3 |
| `DomainTypesIT` | https://github.com/debezium/debezium/blob/main/debezium-connector-postgres/src/test/java/io/debezium/connector/postgresql/DomainTypesIT.java | DOMAIN inside array (the DBZ-3657 case): `nmtoken[]`. Debezium silently dropped these from the schema | `Grep` for `DOMAIN` in `internal/engines/postgres` → no hits. sluice's schema reader likely resolves DOMAINs to their base type via `pg_type.typbasetype`, but this is unverified for the **array-of-domain** case. | **Verified gap (medium)** — see §3 #4 |
| `PostgresMoneyIT` | https://github.com/debezium/debezium/blob/main/debezium-connector-postgres/src/test/java/io/debezium/connector/postgresql/PostgresMoneyIT.java | `money` type: locale-dependent format, min/max values, NULL, three decoder modes. Notably does *not* cover currency-symbol parsing (suggests they punted) | `Grep` for `money\|MONEY` → no hits in `internal/`. Whether sluice handles PG `money` at all is unclear — likely it falls through `verbatim` or refuses-loudly. Should explicitly declare a stance. | **Verified gap (medium-low)** — see §3 #5 |
| `LogicalDecodingMessageIT` | https://github.com/debezium/debezium/blob/main/debezium-connector-postgres/src/test/java/io/debezium/connector/postgresql/LogicalDecodingMessageIT.java | `pg_logical_emit_message('prefix', 'payload', txn)`: transactional + non-transactional; rollback of txn message; prefix filtering. Wire-level: pgoutput emits `Message` type 'M' | `Grep` for `pg_logical_emit_message\|logical.*message` → one hit in `cdc_reader.go`. Need to confirm sluice ignores these (right behaviour) and doesn't crash on them | **Verify-and-pin (low)** — see §4 |
| `TablesWithoutPrimaryKeyIT` | https://github.com/debezium/debezium/blob/main/debezium-connector-postgres/src/test/java/io/debezium/connector/postgresql/TablesWithoutPrimaryKeyIT.java | No-PK tables with REPLICA IDENTITY FULL handling for UPDATE/DELETE | Prior pass gap #2 (REPLICA IDENTITY USING INDEX + UPDATE) covers the UPDATE shape. The **no-PK + no unique + REPLICA IDENTITY FULL** case is the most-permissive variant. Likely covered by `cdc_update_full_family_integration_test.go` but worth confirming the no-PK-at-all axis is in there | **Probably already-have** — see §4 |
| `PostgresSkipMessagesWithoutChangeConfigIT` | https://github.com/debezium/debezium/blob/main/debezium-connector-postgres/src/test/java/io/debezium/connector/postgresql/PostgresSkipMessagesWithoutChangeConfigIT.java | Idempotent UPDATE (SET x = x): does the connector skip the no-op message? Only works under REPLICA IDENTITY FULL | sluice currently re-applies every UPDATE; the no-op case yields a target-side no-op write which is **correct** but uses bandwidth + slot lag. Not a silent-loss gap; an efficiency note. | **Already-have / out of scope** |
| `SnapshotIsolationIT` | https://github.com/debezium/debezium/blob/main/debezium-connector-postgres/src/test/java/io/debezium/connector/postgresql/SnapshotIsolationIT.java | Snapshot isolation modes: SERIALIZABLE / REPEATABLE_READ / READ_COMMITTED / READ_UNCOMMITTED | sluice uses REPEATABLE READ explicitly in `cdc_snapshot.go`; the choice is correct for snapshot consistency. The pin shape (read same row at two timestamps and assert one snapshot value) might be useful for the snapshot→CDC handoff invariants. | **Already-have at code level** |
| `PostgresDefaultValueConverterIT` | https://github.com/debezium/debezium/blob/main/debezium-connector-postgres/src/test/java/io/debezium/connector/postgresql/PostgresDefaultValueConverterIT.java | `DEFAULT 'NULL'` (literal three-letter string) vs `DEFAULT NULL::text` (typed null) — the snapshot reader can confuse these | `Grep` for `'NULL'::text\|literal_null` → no hits. Probably fine because sluice reads `pg_attrdef.adbin` parsed via `pg_get_expr`, not unrolled bytes — but unverified | **Verify-and-pin (low)** — see §4 |

### 2c. wal2json SQL test cases

Bottom line: **closest analogue to sluice's pgoutput path** because both
are PG-native logical-decoding plugins. The SQL test files are small and
self-contained; each is the SQL setup + a `pg_logical_slot_peek_changes`
assertion. Several pin shapes that map cleanly into sluice integration
tests.

| Test | URL | Scenario | Sluice coverage | Verdict |
|---|---|---|---|---|
| `specialvalue.sql` | https://github.com/eulerto/wal2json/blob/master/sql/specialvalue.sql | INSERT rows with `+inf`, `-inf`, `nan` on `real`; embedded quotes/backslashes/unicode in `varchar` | `Grep` for `+inf\|-inf\|NaN\|infinity` → none. **Same gap as Debezium's temporal/precision test, extended to `float4`/`float8`.** | **High-priority gap** — see §3 #1 (covered by the same pin matrix) |
| `toast.sql` | https://github.com/eulerto/wal2json/blob/master/sql/toast.sql | TOAST columns: uncompressed external (`generate_series(1, 2000)` → ~2 KB) and compressed external (`repeat(...)`). INSERT, UPDATE (changing sibling vs TOAST), DELETE | Same as Debezium TOAST gap. | **High-priority gap** — see §3 #2 |
| `savepoint.sql` | https://github.com/eulerto/wal2json/blob/master/sql/savepoint.sql | Transaction with `SAVEPOINT sp1 / sp2 / sp3 / ROLLBACK TO sp2` — partial rollbacks within a transaction | `Grep` for `SAVEPOINT\|ROLLBACK TO` → only in cutover test, unrelated. **sluice's pgoutput path should already filter out the rolled-back changes** (PG's logical decoder drops them before they reach the slot) — but unverified at integration level. | **Verify-and-pin (medium)** — see §3 #6 |
| `rename_column.sql` | https://github.com/eulerto/wal2json/blob/master/sql/rename_column.sql | `ALTER TABLE … RENAME COLUMN d TO f` mid-stream, then DML on the renamed column. wal2json correctly emits the new name | sluice has schema-drift detection (`schema_drift_pg_integration_test.go:86`) for RENAME COLUMN, but this detects drift between source and target. The **CDC mid-stream rename + DML** case — where the relation message arrives with the new column name and the apply WHERE matches by name — is the streaming-protocol-level pin. | **Probably already-have** (Relation messages re-arrive after schema change) — see §4 |
| `update3.sql` | https://github.com/eulerto/wal2json/blob/master/sql/update3.sql | UPDATE under `REPLICA IDENTITY USING INDEX` on a non-PK unique constraint, modifying a column in the unique index | Prior-pass gap (REPLICA IDENTITY USING INDEX + UPDATE) covers the un-modified case. Modifying the identity column itself (key migration) is a separate variant — comment in the file says `FIXME não apresenta valor correto de g`, suggesting wal2json itself had a bug here | **Adjacent to queued gap** — recommend bundling into prior gap #2's pin matrix |
| `delete3.sql` | https://github.com/eulerto/wal2json/blob/master/sql/delete3.sql | DELETE under REPLICA IDENTITY FULL with table-without-PK and table-with-unique | Already covered by `cdc_delete_matrix_pg_integration_test.go` and `cdc_update_full_family_integration_test.go`. | **Already-have** |
| `bytea.sql` | https://github.com/eulerto/wal2json/blob/master/sql/bytea.sql | Bytea hex-decoded values, ~2 KB. Notably **does not** test embedded NUL bytes (`\x00`) — wal2json punt | `Grep` for `\\\\x\|bytea` → covered in type-breadth tests. Embedded NULs unverified — likely a non-issue because pgx COPY-binary handles bytes as bytes, but a one-shot pin removes the class | **Probably already-have** — see §4 |
| `message.sql` | https://github.com/eulerto/wal2json/blob/master/sql/message.sql | `pg_logical_emit_message(true/false, 'prefix', payload)` — including a payload after `ROLLBACK` (non-transactional persists). Tests prefix include/exclude filtering | Same scenario as Debezium's `LogicalDecodingMessageIT`. sluice should drop these. **Worth a one-shot pin** ensuring a flood of these doesn't wedge the apply loop or get misclassified | **Verify-and-pin (low)** — see §4 |
| `actions.sql` | https://github.com/eulerto/wal2json/blob/master/sql/actions.sql | TRUNCATE through CDC (filter: `insert,update,delete,truncate`) | `Grep` for `TRUNCATE.*replica\|TRUNCATE TABLE` → hits in pipeline/IR but no CDC-replay-of-TRUNCATE integration test. PG's pgoutput emits a `T` (truncate) message; sluice's reader ignores it? Filters it? Applies it? **Not pinned** | **Gap candidate (medium)** — see §3 #7 |
| `typmod.sql` | https://github.com/eulerto/wal2json/blob/master/sql/typmod.sql | DOMAIN types over `bigint`, `numeric(5,3)`, `varchar(30)`, `bit varying(20)` in CDC stream | Confirms the DOMAIN gap (§3 #4) is wire-level too |
| `numeric_data_types_as_string.sql` | https://github.com/eulerto/wal2json/blob/master/sql/numeric_data_types_as_string.sql | smallint/int/bigint MIN/MAX boundary values + `Infinity`/`-Infinity`/`NaN` on numeric | Same gap as §3 #1 — boundary integer values worth bundling: `smallint=-32768/32767`, `bigint=-2^63/2^63-1`. sluice's bulk path likely handles these; CDC apply is the place to verify |
| `include_lsn.sql` | https://github.com/eulerto/wal2json/blob/master/sql/include_lsn.sql | LSN-per-change vs LSN-per-transaction. wal2json pins that a batch INSERT in one transaction gets one nextlsn | Architectural to wal2json, not sluice. Out of scope |

### 2d. pgcopydb deeper

Re-read four files the prior pass had skimmed:

| Test | URL | Finding | Sluice coverage |
|---|---|---|---|
| `cdc-low-level/ddl.sql` | https://github.com/dimitri/pgcopydb/blob/main/tests/cdc-low-level/ddl.sql | Trigger function `RAISE NOTICE 'A new record has been inserted into the metrics table! %'` with **32 KB padding** — explicitly to overflow the libpq 16 KB read buffer. Also tests `ENABLE REPLICA TRIGGER`. | sluice uses pgx not libpq directly. `Grep` for `ENABLE REPLICA TRIGGER\|session_replication_role` → **no hits**. The 32-KB-NOTICE case is a curiosity for sluice; the `session_replication_role = 'replica'` semantics (whether sluice sets this on the applier connection so the target's normal triggers don't fire during apply) is the relevant question and is **unverified**. |
| `follow-data-only/multi-wal-txn.sql` | https://github.com/dimitri/pgcopydb/blob/main/tests/follow-data-only/multi-wal-txn.sql | Transaction with `pg_switch_wal()` between inserts — txn spans multiple WAL segments | `Grep` for `pg_switch_wal\|MultiWAL` → no hits. sluice's reader is byte-stream from pglogrepl; WAL-segment boundaries are transparent at this layer, so the bug class is *structurally* absent. But the **interaction with sluice's chunk rotation** (`stream_rotation.go`) is worth a one-paragraph verify: does a transaction that straddles a rotation boundary get a clean commit? |
| `cdc-endpos-between-transaction/dml1.sql` | https://github.com/dimitri/pgcopydb/blob/main/tests/cdc-endpos-between-transaction/dml1.sql | All-or-nothing: a 5-row transaction must be emitted atomically, not partially | sluice's streamer applies per-transaction via the change-applier batch model; structurally correct. Likely covered by `streamer_batched_integration_test.go` but the specific "stop-LSN-falls-mid-transaction" case isn't an explicit pin |
| `cdc-low-level/dml.sql` | https://github.com/dimitri/pgcopydb/blob/main/tests/cdc-low-level/dml.sql | Just sample DML, no edge case | n/a |

### 2e. Bucardo deeper

Re-read four files:

| Test | URL | Finding | Sluice coverage |
|---|---|---|---|
| `t/30-delta.t` | https://github.com/bucardo/bucardo/blob/master/t/30-delta.t | Delta capture mechanics: empty state, single-row, **"doubled up entries" with identical timestamps + PK**, quoted identifiers like `"id space"`. Bucardo's delta-trigger-table layer differs from sluice's pgoutput | The doubled-up-with-identical-timestamps case is a Bucardo-specific trigger-table concern. Quoted identifiers with embedded space — `Grep` for reserved_idents covers it, but the space-as-character case worth confirming explicitly |
| `t/30-crash.t` | https://github.com/bucardo/bucardo/blob/master/t/30-crash.t | Bucardo's multi-source kill-one-DB test (already referenced in prior pass §5 item 7). Worth lifting for multi-source Shape A | Prior pass already queued this for `prep-multi-source-shape-a.md`. No new action |
| `t/50-star.t` | https://github.com/bucardo/bucardo/blob/master/t/50-star.t | Star topology: hub + many leaves, convergence pin (poll until all DBs match) | Out of scope for sluice (we're not multi-target; even if we add multi-source Shape A, it's not star). No action |
| `t/10-makedelta.t` | https://github.com/bucardo/bucardo/blob/master/t/10-makedelta.t | Cascaded sync: A→B→C. "Makedelta" flag controls whether B re-emits A's writes onward to C. Bidirectional sync semantics | Bucardo-specific multi-master / re-emit semantics. Not applicable to sluice |

### 2f. Bucardo cross-engine target tests — what they imply about type-fidelity

The maintainer flagged Bucardo's `t/20-<engine>.t` family — nine target-engine tests (drizzle/firebird/mariadb/mongo/mysql/oracle/postgres/redis/sqlite) hooked through Perl DBI. Bucardo's source is always PG (triggers + delta table); these tests are the closest Bucardo gets to publishing what they consider not-portable to non-PG targets. Each test's `delete` / `next if` / regex workarounds are de-facto documentation of the type-fidelity gaps Bucardo accepted rather than solved. Cross-checked against sluice's actual coverage to ground-truth the implication.

| Source test | URL | What Bucardo papered over | Sluice coverage |
|---|---|---|---|
| `t/20-mysql.t` line 48-49 | https://github.com/bucardo/bucardo/blob/master/t/20-mysql.t | `delete $tabletypemysql{bucardo_test8}` — the bytea table dropped wholesale: "we don't have full support yet". Binary data is "stored in escaped form" (line 256) and skipped on assertion. | **Covered + first-class.** `internal/pipeline/migrate_pg_to_mysql_type_breadth_integration_test.go:76` ships PG `BYTEA NULL → MySQL BLOB-family` end-to-end, including a `'\x68656c6c6f'::bytea` round-trip (line 111). Sluice's BYTEA→MySQL is a real feature, not a documented gap. |
| `t/20-mysql.t` line 68 | (same URL) | `$dbh->do("SET sql_mode='ANSI_QUOTES'");` — Bucardo MUST coerce MySQL into PG-style double-quote identifier quoting because their DDL emitter is PG-centric. | **Not a gap for sluice; avoided by design.** Sluice's MySQL writer uses backticks natively (see `internal/engines/mysql/exprident_shared.go:28+`, `internal/engines/mysql/ddl_emit.go:462`) and never emits double-quoted identifiers to MySQL targets. The Bucardo workaround is irrelevant because sluice's IR-first translation chooses the target-native quote per engine. |
| `t/20-mysql.t` line 257-261 | (same URL) | `$tabletypemysql{$table} =~ /DATETIME/ and $id =~ s/\+.*//;` — the test strips PG's `+00` timezone offset from the expected value because Bucardo writes a PG `TIMESTAMPTZ` to a MySQL `DATETIME` and loses the offset silently. | **Covered + policy-driven.** `internal/engines/mysql/row_writer.go:542-550` explicitly handles `ir.Time{WithTimeZone:true}` and `timestamptz` (`expr_walk.go:265`) under the "zone-flatten cross-engine policy" — the conversion is deliberate and documented in `docs/type-mapping.md`. Sluice's policy is the same flatten Bucardo enacts implicitly, but as a first-class typed translation, not a regex workaround in the test. |
| `t/20-mariadb.t` (full file) | https://github.com/bucardo/bucardo/blob/master/t/20-mariadb.t | Comment line 5: "It should be a dropin for MySQL, but we break it out just in case." Same bytea drop + ANSI_QUOTES dance. | **Not a gap.** Sluice's MySQL engine already covers MariaDB-as-target through the same writer (MySQL and MariaDB share binlog + DBT). The "dropin but separate just in case" posture matches sluice's stance — MariaDB ships under the MySQL engine code, deviations would be loud refusals. |
| `t/20-oracle.t` line 47-49 | https://github.com/bucardo/bucardo/blob/master/t/20-oracle.t | `for my $num (3,5,6,8,10) { delete $tabletype{"bucardo_test$num"}; }` — **5 of 10** standard table types dropped on Oracle. Half the PG type matrix doesn't survive a PG→Oracle Bucardo sync. | **N/A — Oracle isn't on sluice's roadmap.** Oracle is rejected per `CLAUDE.md` cross-engine scope (PG ↔ MySQL only). Worth noting as a data point: even Bucardo at its peak had to accept ~50% type-matrix loss to claim Oracle support, which validates sluice's deliberate scope-narrowness. |

**Takeaway.** The Bucardo cross-engine target tests don't surface NEW gaps in sluice — they validate that sluice's IR-first cross-engine model handles cases Bucardo could only skip with `delete` / `next if` / regex workarounds. The maintainer's observation ("interesting that Bucardo has 9 target engines") inverts to a confidence-builder: even the legacy multi-target tool published their type-fidelity gaps as in-test workarounds, and sluice covers the two on sluice's roadmap (PG↔MySQL) end-to-end with first-class typed translation. Out-of-scope targets (Oracle, MongoDB, Redis, etc.) stay rejected per `CLAUDE.md` cross-engine scope; this is a deliberate narrowness, not a coverage gap.

No new entries for §3 — the gaps Bucardo identified through cross-engine target tests are either already covered by sluice's typed translation or out-of-scope by design.

## 3. Verified gaps, ranked by silent-loss weight

The shape + the sluice code path + a minimal Go test sketch + sizing.
Top-7 sized by `silent_loss_weight × likelihood × catch_value`.

### Gap 1: PG special float / numeric / temporal values through CDC apply

**Silent-loss weight: HIGH.** A `'infinity'::timestamptz` on the source
that decodes to a zero-valued Go `time.Time` on apply and writes
`2000-01-01 00:00:00` to the target is a textbook silent corruption. PG's
`Infinity`/`-Infinity` on timestamps and `NaN` on `numeric` are valid
values pgx exposes but our `value_decode.go` may not round-trip without
ground truth.

**Shape:** `CREATE TABLE t (id int pk, ts_inf timestamptz, ts_neginf
timestamptz, ts_normal timestamptz, num_nan numeric, num_inf numeric,
f4_nan real, f4_inf real, f8_normal float8)`. INSERT a row with `'infinity'`,
`'-infinity'`, `'NaN'`, `'Infinity'`. Migrate source→target end-to-end
(snapshot + CDC INSERT + CDC UPDATE), assert byte-equal on the target.
Cross-engine variant (PG→MySQL): refuse-loudly is the right behaviour
since MySQL has no `infinity` representation; pin the refusal.

**Sluice code path:** `internal/engines/postgres/value_decode.go`
(`decodeValue`), `cdc_normalize.go`, `change_applier.go` UPDATE WHERE +
SET shape. The IR `ir.Row` value is `any`; the pgx-driver-decoded value
arrives as `time.Time` (zero for infinity? or `time.Time` with a special
sentinel?). The risk is in the pgx → IR boundary.

**Test sketch (~150 LOC):**

```go
// internal/engines/postgres/cdc_special_values_integration_test.go
//go:build integration

func TestPostgresCDCSpecialFloatTemporalRoundTrip(t *testing.T) {
    src := newPGContainer(t); defer src.cleanup()
    dst := newPGContainer(t); defer dst.cleanup()
    mustExec(src, `CREATE TABLE t (id int PRIMARY KEY,
        ts_inf timestamptz, ts_neginf timestamptz, num_nan numeric, f4_inf real)`)
    mustExec(src, `INSERT INTO t VALUES (1, 'infinity', '-infinity', 'NaN', 'Infinity')`)
    runMigrate(t, src, dst, "t")
    // Snapshot survived
    assertRow(t, dst, "SELECT ts_inf::text, ts_neginf::text, num_nan::text, f4_inf::text FROM t WHERE id=1",
        []any{"infinity", "-infinity", "NaN", "Infinity"})
    // CDC INSERT survives
    runStreamerOnce(t, src, dst, func() {
        mustExec(src, `INSERT INTO t VALUES (2, 'infinity', 'infinity', 'NaN', '-Infinity')`)
    })
    assertRow(t, dst, "...", []any{"infinity", "infinity", "NaN", "-Infinity"})
}
```

**Sizing:** ~150 LOC PG-only; +~100 LOC cross-engine refuse-loudly variant.
One file each.

### Gap 2: TOAST round-trip through CDC apply (multi-KB unchanged-TOAST column)

**Silent-loss weight: HIGH-CRITICAL.** sluice's decoder correctly omits
unchanged-TOAST columns ("data byte 'u'") and the applier's "absent key
= preserve target value" semantics are correct in theory
(`cdc_reader.go:1454`, `change_applier.go:1612`). But **there is no
integration test that actually exercises this end-to-end with a real
multi-KB TOASTed value**. The unit test in `cdc_reader_test.go:384` only
exercises the decoder. A regression in `buildUpdateSQL` that mis-handles
the absent key (e.g. interpolates an empty string) silently wipes the
TOASTed column on the target — and the existing pins won't catch it.

**Shape:** `CREATE TABLE t (id int PRIMARY KEY, label text, blob text)`.
INSERT a row with `blob = repeat('x', 10000)` (forces external storage).
UPDATE only `label` — pgoutput emits a Relation message with `blob`'s
TupleData byte = 'u'. sluice's apply must skip `blob` in the SET clause.
Assert the target's `blob` is still 10000 chars of 'x' after apply.

**Sluice code path:** `internal/engines/postgres/cdc_reader.go:1454`
(decoder), `change_applier.go:buildUpdateSQL` (apply).

**Test sketch (~120 LOC):**

```go
// internal/engines/postgres/change_applier_toast_integration_test.go
//go:build integration

func TestPostgresCDCToastUnchangedColumnPreserved(t *testing.T) {
    src, dst := newPGPair(t)
    mustExec(src, `CREATE TABLE t (id int PRIMARY KEY, label text, blob text)`)
    mustExec(src, `ALTER TABLE t ALTER COLUMN blob SET STORAGE EXTERNAL`)
    blob := strings.Repeat("x", 10_000)
    mustExec(src, `INSERT INTO t VALUES (1, 'before', $1)`, blob)
    runMigrate(t, src, dst, "t")
    // Sanity: target has full blob
    assertScalar(t, dst, "SELECT length(blob) FROM t WHERE id=1", 10_000)
    // Now UPDATE only label; TOAST column unchanged → comes through as 'u'
    runStreamerOnce(t, src, dst, func() {
        mustExec(src, `UPDATE t SET label = 'after' WHERE id = 1`)
    })
    // CRITICAL: blob preserved, not emptied
    assertScalar(t, dst, "SELECT length(blob) FROM t WHERE id=1", 10_000)
    assertScalar(t, dst, "SELECT label FROM t WHERE id=1", "after")
}
```

Add matrix variant under REPLICA IDENTITY FULL (where every column,
including unchanged-TOAST, appears in OldTuple — the `'u'` byte path in
the *Before* tuple is a separate code path).

**Sizing:** ~120 LOC; one file.

### Gap 3: `ALTER TYPE … ADD VALUE` ENUM mid-stream drift

**Silent-loss weight: HIGH.** Snapshot reads enum values
`{draft, published}`. After snapshot but before CDC catches up, an
operator runs `ALTER TYPE post_status ADD VALUE 'archived'` on the
source. A subsequent INSERT on the source uses `'archived'`. The CDC
stream delivers the row. Target's enum type doesn't include `'archived'`
— PG fails the INSERT with `22P02 invalid input value for enum`. **This
should be loud-fail**; the risk is whether sluice's apply path turns it
into a silent skip or whether the schema-drift detector catches it
explicitly. Either way, no test.

**Shape:** Create source ENUM `{a, b}` + table; migrate to target (target
gets identical enum). Apply `ALTER TYPE … ADD VALUE 'c'` on source.
INSERT with `'c'` on source. Run streamer. Either: target loud-fails (and
the test asserts the loud failure shape), or sluice's drift detector
catches the ALTER TYPE upstream and emits a refuse-loudly. Test pins
which.

**Sluice code path:** Schema-drift detector
(`internal/pipeline/schema_drift_pg_integration_test.go` covers RENAME
COLUMN, not ALTER TYPE), `internal/engines/postgres/cdc_relations.go`
(enum value resolution), applier error classification
(`applier_errors.go`).

**Test sketch (~100 LOC):**

```go
// internal/engines/postgres/cdc_enum_drift_integration_test.go
//go:build integration

func TestPostgresCDCEnumAddValueLoudFail(t *testing.T) {
    src, dst := newPGPair(t)
    mustExec(src, `CREATE TYPE status AS ENUM ('a', 'b')`)
    mustExec(src, `CREATE TABLE t (id int PRIMARY KEY, s status)`)
    runMigrate(t, src, dst, "t")
    // Drift: source adds new enum value
    mustExec(src, `ALTER TYPE status ADD VALUE 'c'`)
    mustExec(src, `INSERT INTO t VALUES (1, 'c')`)
    // Streamer should refuse-loudly, not silently skip
    err := runStreamerOnce(t, src, dst, nil)
    require.Error(t, err)
    require.Contains(t, err.Error(), "enum") // or whatever the refuse shape is
    // And the target should NOT have row id=1
    assertCount(t, dst, "SELECT count(*) FROM t WHERE id=1", 0)
}
```

**Sizing:** ~100 LOC; one file. Plus possibly a CHANGELOG/ADR clarifying
the "sluice refuses ENUM drift mid-stream" stance.

### Gap 4: DOMAIN-type-as-array round-trip

**Silent-loss weight: MEDIUM.** Debezium hit this in DBZ-3657 (their
`DomainTypesIT` exists because they silently dropped these columns from
the schema). sluice's schema reader almost certainly resolves DOMAINs to
their base type via `pg_type.typbasetype`, but the **array of DOMAIN**
case is a separate code path (pg_type's `typarray` for the domain points
to a domain-array OID, not the base-array OID). If pgx's array codec
keys off the domain-array OID and sluice's value decoder doesn't, the
result is silent type confusion or a panic.

**Shape:** `CREATE DOMAIN nmtoken AS text`. `CREATE TABLE t (tags
nmtoken[])`. INSERT row with array of values. Migrate source→target.
Assert array survives.

**Sluice code path:**
`internal/engines/postgres/schema_reader.go` (type resolution),
`value_decode.go` (array codec dispatch).

**Test sketch (~80 LOC):**

```go
func TestPostgresDomainArrayRoundTrip(t *testing.T) {
    src, dst := newPGPair(t)
    mustExec(src, `CREATE DOMAIN nmtoken AS text NOT NULL`)
    mustExec(src, `CREATE TABLE t (id int PRIMARY KEY, tags nmtoken[])`)
    mustExec(src, `INSERT INTO t VALUES (1, ARRAY['x','y','z']::nmtoken[])`)
    runMigrate(t, src, dst, "t")
    assertRow(t, dst, "SELECT tags FROM t WHERE id=1", []string{"x", "y", "z"})
}
```

**Sizing:** ~80 LOC. Plus mirror with `nmtoken[][]` (multi-dim) per the
Bug 74 lesson in `CLAUDE.md` (the family-dispatched-codec corollary).

### Gap 5: PG `money` type explicit stance

**Silent-loss weight: LOW-MEDIUM.** PG's `money` type is locale-dependent
in its text representation but locale-independent in its binary
representation (it's an `int64` of cents). pgx exposes it as a `string`
(with `$` prefix in default locale) or as `pgtype.Money`. sluice's
current behaviour: not pinned; likely either falls through `verbatim`
tier (treat as opaque bytes) or silently mis-maps to text. Worst case:
PG→MySQL silently writes `'$1,234.56'` as a string when the operator
expected a numeric column on the target.

**Recommended stance:** refuse-loudly on `money` for cross-engine (it
has no MySQL equivalent), explicit verbatim/numeric handling for PG→PG.
The test pins which path sluice takes.

**Shape:** `CREATE TABLE t (amount money)`. INSERT `'$1,234.56'::money`,
`'-92233720368547758.08'::money` (PG money min), `'92233720368547758.07'::money`
(max). Migrate PG→PG → assert exact preservation. Migrate PG→MySQL →
assert refuse-loudly.

**Sluice code path:** `internal/engines/postgres/types.go` type mapping,
`value_decode.go`, `cross_engine_supportable.go`.

**Sizing:** ~80 LOC for PG-only; +~50 LOC for refuse-loudly cross-engine.

### Gap 6: SAVEPOINT / ROLLBACK TO during a captured transaction

**Silent-loss weight: MEDIUM.** PG's logical decoder is supposed to drop
sub-transaction changes that get rolled back to a savepoint before final
commit. But sluice's streaming-protocol handling for streamed (in-progress)
transactions (`cdc_reader_streaming_protocol_integration_test.go`) is
where this matters: if a streamed transaction's sub-changes are
prematurely applied to the target before the COMMIT message indicates
which sub-tree was rolled back, we'd silently write rolled-back rows.

**Sluice code path:** `internal/engines/postgres/cdc_reader.go`
streaming-protocol message handling.

**Test sketch (~120 LOC):**

```go
func TestPostgresCDCSavepointRollbackDropped(t *testing.T) {
    src, dst := newPGPair(t)
    mustExec(src, `CREATE TABLE t (id int PRIMARY KEY, name text)`)
    runMigrate(t, src, dst, "t")
    runStreamerOnce(t, src, dst, func() {
        mustExec(src, `
            BEGIN;
              INSERT INTO t VALUES (1, 'alice');
              SAVEPOINT sp1;
              INSERT INTO t VALUES (2, 'bob');
              SAVEPOINT sp2;
              INSERT INTO t VALUES (3, 'carol');
              ROLLBACK TO sp2;  -- drop carol
              INSERT INTO t VALUES (4, 'dave');
              ROLLBACK TO sp1;  -- drop bob and dave
              INSERT INTO t VALUES (5, 'eve');
            COMMIT;
        `)
    })
    // target should have only alice and eve; bob, carol, dave must be absent
    assertSet(t, dst, "SELECT id FROM t ORDER BY id", []int{1, 5})
}
```

**Sizing:** ~120 LOC. Recommend a streamed-protocol variant
(`logical_decoding_work_mem` very low to force in-progress streaming) so
both paths are pinned.

### Gap 7: `TRUNCATE` event in CDC stream

**Silent-loss weight: MEDIUM.** pgoutput emits a `T` (TRUNCATE) message
for `TRUNCATE TABLE`. If a source operator runs `TRUNCATE t` and sluice
silently ignores it, the source has 0 rows but the target still has all
the rows — silent divergence. If sluice applies it, the target becomes
empty — desired in some cases, catastrophic in others (a one-key-mistake
TRUNCATE that the operator wanted to roll back).

**Recommended stance:** refuse-loudly (TRUNCATE is destructive and
operators rarely want it mirrored), with an opt-in flag. Pin the
refusal.

**Sluice code path:** `internal/engines/postgres/cdc_reader.go` and
`change_applier.go`. Currently unclear what happens to a TRUNCATE
message — `Grep` for `'TRUNCATE'\|truncate.*cascade` shows hits but none
in CDC apply context.

**Test sketch (~80 LOC):**

```go
func TestPostgresCDCTruncateRefuseLoudly(t *testing.T) {
    src, dst := newPGPair(t)
    mustExec(src, `CREATE TABLE t (id int PRIMARY KEY)`)
    mustExec(src, `INSERT INTO t VALUES (1), (2), (3)`)
    runMigrate(t, src, dst, "t")
    err := runStreamerOnce(t, src, dst, func() {
        mustExec(src, `TRUNCATE TABLE t`)
    })
    require.Error(t, err) // or assert structured refuse signal
    assertCount(t, dst, "SELECT count(*) FROM t", 3) // target unchanged
}
```

**Sizing:** ~80 LOC. Plus a CHANGELOG/ADR explaining the refuse stance.

## 4. Already-covered checks (confidence-builders)

These would be gaps if not for X. We looked and confirmed coverage.

- **`unchanged TOAST` decoding** — covered at unit-test level
  (`cdc_reader_test.go:384`). Integration coverage is the new gap (§3 #2).
- **REPLICA IDENTITY FULL DELETE on tables with/without PK/unique** — covered
  by `cdc_delete_matrix_pg_integration_test.go` +
  `cdc_update_full_family_integration_test.go`.
- **MySQL `TINYINT(1) → BOOLEAN` cross-engine** — covered by value-types
  doc and the cross-engine tests (`migrate_cross_integration_test.go`).
- **`gen_random_uuid()` / `uuid-ossp`-style defaults** —
  `migrate_uuidossp_pgcrypto_integration_test.go`.
- **Reserved identifier quoting** — `reserved_idents_test.go` covers PG
  keyword set comprehensively. The non-ASCII identifier gap is queued
  from the prior pass; the ASCII keyword case is solved.
- **Exclusion constraints (EXCLUDE USING gist)** — covered by
  `migrate_exclude_constraint_integration_test.go` and
  `internal/ir/exclude_constraint_test.go`. Includes deferrable variant.
- **FK referential actions (CASCADE / SET NULL / SET DEFAULT / RESTRICT /
  NO ACTION)** — covered by `ddl_emit_test.go` + schema_reader tests.
- **Composite primary key + CDC DELETE** — `streamer_composite_pk_delete_integration_test.go`.
- **Snapshot isolation level (REPEATABLE READ)** — encoded in
  `cdc_snapshot.go` and the snapshot integration tests.
- **`mid-stream RENAME COLUMN` Relation message republish** — pgoutput
  re-emits a Relation message after schema changes, sluice
  re-registers via `cdc_relations.go`. Schema-drift detector covers
  the source→target drift case (`schema_drift_pg_integration_test.go:86`).
  An explicit RENAME-during-CDC-streaming test would be belt-and-suspenders,
  not a gap — sizing ~60 LOC if maintainer wants it pinned.
- **GENERATED ALWAYS AS IDENTITY (apply-side)** — already queued (prior
  pass).
- **Statement-timeout on source connection** — already queued (prior
  pass).
- **`pg_logical_emit_message` (logical decoding message)** — `Grep`
  shows `cdc_reader.go` has one reference; likely a guarded ignore but
  worth a verify-and-pin. Low priority.
- **Bytea hex format with `\x` prefix** — covered by
  `migrate_pg_to_mysql_type_breadth_integration_test.go`. Embedded NUL
  bytes not explicitly tested but pgx COPY-binary handles bytes as bytes
  by construction.

## 5. Out of scope / rejected

- **pg_dump's event trigger / FDW / transform / language handler / custom
  cast / custom base type / text-search-configuration shapes.** Sluice's
  IR explicitly excludes these. Adding pins would imply expanding
  translation policy.
- **Debezium's Kafka Connect plumbing (offset commits, schema registry,
  cloud events).** Architectural mismatch — sluice isn't a Connect
  source.
- **Bucardo's multi-master / NOTIFY-kick / custom Perl callback / pgservice
  plumbing.** Architectural mismatch; the prior pass rejected these.
- **Bucardo's `t/40-customcode-exception.t`.** Custom in-DB Perl exception
  handlers — the canonical Bucardo wart sluice is correct to reject.
- **wal2json's `actions` filter modes (filter-by-action-kind).** Filter
  granularity at the action level is not part of sluice's surface and
  would conflict with the bulk-semantics tenet.
- **wal2json's `include-lsn` / `include-xids` / `include-timestamp`
  output knobs.** These are presentation-layer concerns specific to
  wal2json's JSON output; sluice's wire-level pgoutput consumer doesn't
  expose them as user-facing knobs.
- **pgcopydb's catalog-fork SQLite corruption / libpq EPIPE / catalog
  semaphore / per-process pre-fork DB handle hygiene.** C/SQLite/libpq
  process-model artifacts.
- **Idempotent-UPDATE skip (`SKIP_MESSAGES_WITHOUT_CHANGE`).** Not a
  silent-loss class; an efficiency-knob debate. Out of scope of this
  pass.
- **Inheritance partitioning (legacy PG inheritance).** Sluice models
  declarative partitions only when added; legacy inheritance is
  explicitly out of scope.
- **Large objects (`pg_largeobject`).** sluice does not replicate large
  objects; pg_dump's LO tests + pgcopydb's LO bug fixes (#957) don't apply.

## 6. Open questions for the maintainer

1. **Gap #1 (special floats/temporals) — is `infinity` on `timestamptz`
   in-scope for sluice's PG→PG path?** If yes, the pin is a no-brainer.
   If you'd rather refuse-loudly (because cross-engine has no equivalent),
   say so and the gap pivots from "round-trip pin" to "refuse-loudly
   preflight pin".
2. **Gap #2 (TOAST integration pin) — is there an existing helper that
   forces TOAST storage in integration tests?** I didn't find one. If
   not, the pin includes a `ALTER TABLE … ALTER COLUMN … SET STORAGE
   EXTERNAL` + a big-enough value to trigger it. Cheap; just calling
   it out.
3. **Gap #3 (ENUM ADD VALUE drift) — is sluice's preferred stance
   "refuse-loudly when source enum drifts" or "auto-replay the ALTER
   TYPE on target"?** The former is on-tenet; the latter is convenient
   for operators. The test shape changes meaningfully between the two.
4. **Gap #5 (`money`) — is `money` already declared out of scope
   somewhere I missed?** If not, an explicit refuse-loudly stance for
   cross-engine and a verbatim/numeric stance for PG→PG should land
   together.
5. **Gap #6 (SAVEPOINT) — sluice's streaming protocol handles
   in-progress transactions; is the savepoint rollback in the streamed
   path covered by `cdc_reader_streaming_protocol_integration_test.go`?**
   I checked and didn't find an explicit savepoint case (only stream
   start/stop framing). If you want it pinned, the streamed variant is
   the harder one and the pin should cover both `logical_decoding_work_mem`
   low (forces streaming) and high (buffered).
6. **Gap #7 (TRUNCATE in CDC) — refuse-loudly, opt-in apply, or
   default-apply?** Opinion-territory. The pin shape follows the call.
7. **Test infrastructure — for any of these pins, would you rather they
   land as one omnibus integration test file or one per gap?** Smaller
   files match sluice's existing `migrate_bug*_integration_test.go`
   convention; recommendation is one file per gap unless they share
   significant fixture setup.

## 7. Sources index (URLs to read in full if/when implementing)

- pg_dump:
  https://github.com/postgres/postgres/tree/master/src/bin/pg_dump/t
- Debezium PG IT directory:
  https://github.com/debezium/debezium/tree/main/debezium-connector-postgres/src/test/java/io/debezium/connector/postgresql
- wal2json SQL tests:
  https://github.com/eulerto/wal2json/tree/master/sql
- pgcopydb tests:
  https://github.com/dimitri/pgcopydb/tree/main/tests
- Bucardo tests:
  https://github.com/bucardo/bucardo/tree/master/t
