# Upstream `dimitri/pgcopydb` + `bucardo/bucardo` — review for sluice

Author: research scratch, 2026-05-29.

This is the **second** upstream-research pass. The first reviewed PlanetScale's
fork of pgcopydb and lives at
`docs/dev/notes/pgcopydb-planetscale-fork-review.md`. That note surfaced three
adoptions — (a) `json`-equality cast under REPLICA IDENTITY FULL **[MERGED, PR
#92]**, (b) NOT VALID FK retry **[in flight]**, (c) XID-wraparound preflight
**[queued]** — which are **not** re-recommended here.

Sources reviewed in this pass:

- `dimitri/pgcopydb` upstream (NOT the fork): PRs / issues / commits over the
  last ~18 months and a sample of the `tests/` tree.
  - PRs enumerated via `gh api repos/dimitri/pgcopydb/pulls?state=all`; sampled
    in detail: #946 (replica identity USING INDEX), #949 (partitioned target
    asymmetry), #950 (test_decoding perf), #933 (quoted null), #957 (PG17 LO
    fix), #956 (snapshot-state guard), #954 (extension OID collision), #937
    (tolerate restore errors), #936 (intra-table split), #940 (CDC apply-phase
    filter), #927/#926/#925/#924 (the same fixes that landed as 9xx after
    cherry-pick).
  - Issues: #943 (KEEPALIVE crash), #942 (uppercase schema), #941 (GENERATED
    ALWAYS column on CDC), #931 (`null`-as-text deserialized as NULL), #930
    (non-unique OIDs in filter), #928 (PG17/18 verify ask).
  - Commits: `since=2025-11-01`, all roll up to merged PRs.
  - Tests: enumerated via `gh api … contents/tests`. Read in full:
    `cdc-replica-identity-index/{ddl,dml}.sql`, `cdc-partitioned-target/{source,target,dml}.sql`,
    `cdc-endpos-between-transaction/dml{1,2}.sql`,
    `cdc-low-level/{ddl,dml}.sql`, `follow-data-only/{ddl,dml,multi-wal-txn,dml-bufsize}.sql`,
    `unit/script/{3-collations,4-list-table-split}.sh`. Skim only on
    `blobs/`, `extensions/`, `pagila/`, `timescaledb/`.
- `bucardo/bucardo` upstream: PRs / issues / commits and the `t/` tree.
  - PRs: #269–#273 (the only post-2021 PRs). All open. Read in full.
  - Issues: #261–#268 (post-2024). Skim — all are user-support tickets,
    not bug reports with reproducers, with the exception of #267/#268.
  - Commits: last ~5y. **Bucardo upstream is effectively dormant** —
    only test-infra commits in 2025; the last meaningful behaviour change
    landed in **April 2021**. The body of evidence is the open PR queue.
  - Tests (`t/`): read in full `30-crash.t`, `40-conflict.t`,
    `40-serializable.t`, `40-customcode-exception.t` (skim), `10-makedelta.t`,
    `10-object-names.t`, `30-delta.t`. Skim only on the bctl-* files,
    20-{drizzle,firebird,mariadb,mongo,mysql,oracle,redis,sqlite}.t (not on
    sluice's roadmap), and 99-{lint,perlcritic,signature,spellcheck,yaml}.t.

## 1. TL;DR

- **pgcopydb upstream is converging with the PlanetScale fork**, by the way of
  upstream cherry-picking the same fixes ~6 months later. Most of the
  high-signal PRs in this window — partitioned-target asymmetry (#949), the
  test_decoding REPLICA IDENTITY USING INDEX parser fix (#946), PG17 LO copy
  (#957), extension-OID-collision (#954) — are upstream landings of work that
  was already on the fork's tree. Net new adoption candidates from upstream
  alone are slim; the most valuable contribution of this pass is **pinning the
  bug *classes* in sluice's own test suite**, not stealing C code.
- **The single net-new test pin worth lifting (sluice does not currently cover
  this):** REPLICA IDENTITY USING INDEX on a **non-PK unique index**, with
  UPDATEs whose identity columns are NOT changed (the case pgcopydb PR #946
  was filed against). sluice's CDC reader path is pgoutput, not test_decoding,
  so the literal bug doesn't apply — but sluice **does** have an apply-side
  WHERE-clause builder, and the **same class** of "is this column part of the
  replica identity?" question is implicit in the pgoutput Relation message's
  `flags` bit. We already have a related delete-matrix integration test
  (`internal/pipeline/cdc_delete_matrix_pg_integration_test.go`) that exercises
  `REPLICA IDENTITY USING INDEX`, but the UPDATE-where-identity-columns-are-
  unchanged variant doesn't appear to be pinned. **Recommend adding it.**
- **Bucardo upstream's only modern signal worth lifting** is the
  `t/10-object-names.t` pattern: **non-ASCII table/column names** (Greek,
  Cyrillic, emoji, Unicode-character-class identifiers like `pkey_⚕`) wired
  through the entire copy + sync path with `pg_enable_utf8` deliberately
  disabled to exercise the byte-level encode path. sluice's reserved-idents
  testing covers quoting but doesn't appear to exercise multi-byte UTF-8
  identifiers through CDC apply. This is a cheap test-coverage gap to close.
- **Two pgcopydb issues describe sluice-relevant production bugs that we
  should pre-empt by pinning:** (i) #941 — GENERATED ALWAYS identity column
  during CDC replay (sluice handles this in the bulk path and at apply build
  time; need an integration pin that demonstrates idempotent replay on a table
  with `GENERATED ALWAYS AS IDENTITY` doesn't emit the column at all on UPDATE);
  (ii) #931 — `null` as a text literal deserialized as actual NULL (sluice
  uses pgoutput which doesn't have this string-vs-typed-NULL ambiguity, but
  the **mirror case** at the bulk-load CSV-encoder layer is worth verifying).
- **What's deliberately not recommended:** Bucardo's multi-master, NOTIFY-kick
  control-DB design, Perl pgservice plumbing, custom-code exception handlers,
  conflict-resolution strategies, and the fix-the-relation-cache-memory-blowup
  PR (#270) are all rejected because they describe a fundamentally different
  product. The Bucardo "20-year wear" payoff in this review is the test
  *patterns* (multi-byte names, conflict pinning, crash semantics), not the
  Perl code.

## 2. pgcopydb upstream — categorized

### 2a. Correctness / silent-loss

| PR / Issue | What it does | Sluice verdict | Rationale |
|---|---|---|---|
| [#946](https://github.com/dimitri/pgcopydb/pull/946) | test_decoding parser learns `pg_index.indisreplident` (was reading only `indisprimary`); UPDATE on `REPLICA IDENTITY USING INDEX <non-PK-unique>` now parses. Adds `tests/cdc-replica-identity-index/`. | **partial overlap — adopt the test pattern** | sluice uses pgoutput, not test_decoding, so the parser bug doesn't apply to us directly. But the **shape** of the test case (UPDATE where identity columns are unchanged, table with no PK, identity is a non-PK unique index) is exactly the kind of edge case that exercises sluice's apply-side WHERE builder + relation-message decode. sluice already has `cdc_delete_matrix_pg_integration_test.go` for DELETE; UPDATE-with-unchanged-identity-cols is the gap. |
| [#949](https://github.com/dimitri/pgcopydb/pull/949) | Source flat table → target partitioned table: rewrites `TRUNCATE ONLY` → `TRUNCATE` and skips `COPY FREEZE` when target's `relkind = 'p'`. CDC `ld_apply.c` does the rewrite at apply time. | **partial — note for if/when sluice supports declarative partitions** | sluice's schema_reader filters `relkind = 'r'` only (no partition handling); the cross-engine PG↔MySQL story doesn't need partition awareness today. **The relevant lesson** is the apply-time `pg_class.relkind` lookup pattern — when sluice does add partition support, the source-flat / target-partitioned asymmetry is the case to remember. |
| [#933](https://github.com/dimitri/pgcopydb/pull/933) + issue [#931](https://github.com/dimitri/pgcopydb/issues/931) | test_decoding emits `c[text]:'null'` for a literal string `"null"`; pgcopydb's parser was deserializing this as actual SQL NULL → silent data corruption. | **not applicable — but mirror-case worth verifying** | sluice's pgoutput path receives typed tuples (the wire format distinguishes the null bit from the text bytes), so the literal "null"-as-text case is structurally not at risk on CDC. **The mirror case to verify** is the bulk-load CSV encoder for sources that emit text-formatted COPY (csv `\N` vs text `\N` vs literal `\N`). Worth a one-shot integration pin: `t (c text)` with rows `'null'`, `'NULL'`, `''`, actual NULL — assert all four survive a PG→PG migrate end-to-end. |
| [#957](https://github.com/dimitri/pgcopydb/pull/957) | PG17 changed `pg_dump` LO behaviour: large object metadata is no longer in pre-data. Fix: on `lo_open` failure, fall back to `lo_create(blobOid)`. | **not applicable** | sluice doesn't shell out to pg_dump and doesn't replicate large objects. The PG17 regression class is real but doesn't intersect sluice's surface. |
| [#956](https://github.com/dimitri/pgcopydb/pull/956) | `clone --follow --snapshot` with a pre-exported snapshot was missing a guard: the second `copydb_fetch_schema_and_prepare_specs` saw `SNAPSHOT_STATE_CLOSED` instead of `SNAPSHOT_STATE_UNKNOWN`, so no transaction was opened, then commit failed. | **not applicable** | sluice's snapshot lifecycle is owned by the orchestrator, not a fork's protocol-state machine. The C state machine has no Go analogue. |
| [#940](https://github.com/dimitri/pgcopydb/pull/940) | CDC apply phase was ignoring configured table/schema **filters** — filterOut was set during streaming but never during apply (PREPARE/EXECUTE codepath). Closes "ERROR: relation x does not exist" for filtered schemas. | **already have** | sluice's CDC reader filters by relation OID against the snapshot catalog; apply only sees changes for tables the reader admitted. The bug class doesn't exist for us. |
| [#954](https://github.com/dimitri/pgcopydb/pull/954) | `--skip-extensions`: when an extension's contained-object OID collides with an unrelated table's OID, the filter was hiding the unrelated table. | **not applicable** | sluice's filter is by `schema.table` qualified name, not OID. |
| [#950](https://github.com/dimitri/pgcopydb/pull/950) | Per-row SQLite catalog lookups in the test_decoding transform hot path → 100% CPU + slot lag unbounded. Fix: per-process `(nspname, relname) → attr-map` cache. | **already have (structurally)** | sluice's `change_applier_schema_cache.go` already caches column-type and PK info per relation; the parallel structure is in place. Worth noting the symptom — "transform process pegs CPU and slot lag grows" — as a regression-classifier signal for sluice's own apply throughput. |
| Issue [#943](https://github.com/dimitri/pgcopydb/issues/943) | Synthetic KEEPALIVE message with empty timestamp crashes the apply process (test_decoding + wal2json), blocking cutover; broken `.sql` file on disk means restarts keep crashing on the same line. | **not applicable — but the "poison-pill on disk" pattern is a useful design check** | sluice's keepalive path goes through `pglogrepl.ParsePrimaryKeepaliveMessage` directly on the wire — there's no JSON intermediary, no on-disk staging. **The structural lesson:** any future on-disk WAL-staging codepath (the snapshot→CDC handoff manifest already touches disk) needs a "poison message is logged & quarantined, not fatal" recovery posture. Worth referencing in the snapshot→CDC prep note. |
| Issue [#942](https://github.com/dimitri/pgcopydb/issues/942) | Uppercase / mixed-case schema names in `--filters` not propagated through quoting; pg_dump's schema arg ends up unquoted. | **partial — verify** | sluice's schema_reader and DDL emitter use `quoteIdent`; the question is whether sluice's filter machinery (`internal/pipeline/filter_test.go`) preserves the case-as-typed-by-the-operator vs lowercasing somewhere. Worth a one-shot integration test: source schema named `"UPPER-SCHEMA"`, filter inclusion, end-to-end migrate. |
| Issue [#941](https://github.com/dimitri/pgcopydb/issues/941) | CDC apply on a table with `GENERATED ALWAYS AS IDENTITY` column hits SQLSTATE 428C9 "can only be updated to DEFAULT" because the replay statement includes the identity column. | **already have (apply-side)** | sluice's `generatedColCache` (`change_applier.go:156`) filters generated columns out of INSERT lists, UPDATE SET, and UPDATE/DELETE WHERE. The pgcopydb bug doesn't fire for us. **Worth pinning** that this works for `GENERATED ALWAYS AS IDENTITY` specifically (not just `GENERATED ... STORED`) with an integration test — the existing tests target STORED; identity-column-on-replay is the variant pgcopydb just got bit by. |
| Issue [#930](https://github.com/dimitri/pgcopydb/issues/930) | Non-unique OIDs across databases break pgcopydb's filter table (its SQLite mirror of pg_class). | **not applicable** | sluice doesn't mirror pg_class to a side store; relation lookups go directly against the source on every reader connection. |

### 2b. Operator UX / managed-source accommodation

| PR / Issue | What it does | Sluice verdict | Rationale |
|---|---|---|---|
| [#937](https://github.com/dimitri/pgcopydb/pull/937) | `MAX_TOLERATED_RESTORE_ERRORS` (default 10): parse pg_restore's "errors ignored on restore: N" and continue if `N ≤ 10`. | **rejected (same reasoning as fork PR #18/#6)** | Tolerating N silent target-side schema errors is anti-tenet for sluice ("loud failure over silent degrade"). The IR is the source of truth; if a target object can't be created we surface the failure, we don't trade the migration's correctness for fewer support tickets. |
| [#936](https://github.com/dimitri/pgcopydb/pull/936) | Adds intra-table parallel COPY (table-splitting) to `pgcopydb copy data`. Wires existing split-logic into the standalone `copy` command. | **already have** | sluice has chunked parallel COPY (`internal/pipeline/migrate_parallel.go`); not a port candidate. |
| [#929](https://github.com/dimitri/pgcopydb/pull/929) | CI: test against PG17/PG18 explicitly. | **already have** | sluice's CI matrix is broad. |

### 2c. Performance / pipeline orchestration

| PR | What it does | Sluice verdict | Rationale |
|---|---|---|---|
| [#950](https://github.com/dimitri/pgcopydb/pull/950) | Per-process catalog cache for `(nspname, relname) → attr-map`, eliminates per-row SQLite roundtrip in the transform hot path. | **already have (structurally)** | See §2a row above — `change_applier_schema_cache.go` is the parallel. |

### 2d. Build / CI / governance

| PR | What it does | Sluice verdict | Rationale |
|---|---|---|---|
| [#955](https://github.com/dimitri/pgcopydb/pull/955) | Test-infra: collation comparison is OID-dependent (fix unit test). | **not applicable** | pgcopydb test internal. |
| [#948](https://github.com/dimitri/pgcopydb/pull/948) | Add maintenance/support section to README. | **not applicable** | OSS governance. |
| [#935](https://github.com/dimitri/pgcopydb/pull/935) | SAST: bad copy-paste in compare.c. | **not applicable** | C-internal. |

### 2e. PRs / issues skipped intentionally

`#925, #927`, `#902, #903, #904` (libpq error-string memory ownership, segfault on NULL SQLSTATE, idle_in_transaction_session_timeout) — all C/libpq-specific, no Go analogue. `#911` (arm64 docker image), `#909` (typo), `#908` (double-free), `#907` (.env support), `#891` (zero alloc), `#890` (pipe-done condition) — small fixes / dev hygiene, not on-tenet for sluice.

## 3. Bucardo upstream — categorized

Bucardo is in **maintenance-only mode**: the last meaningful behaviour change
landed April 2021 (PR #232, fullcopy role docs). 2024–2025 commits are
copyright-bumps and test-infra. The five open PRs are the only signal.

### 3a. Bug reports we can learn from

| PR / Issue | What it does | Sluice verdict | Rationale |
|---|---|---|---|
| [#271](https://github.com/bucardo/bucardo/pull/271) | "Race condition when running bucardo commands": parallel `bucardo add table` calls from a thread pool corrupt the control DB. Adds a reproducer test, then fixes with locking. | **not applicable** | Bucardo's race is its control-DB-state-machine concurrency; sluice has no shared mutable control DB. The *test pattern* — "fan out N concurrent `add table`-style operations and assert post-condition" — is generic and we already practice it (e.g. `internal/engines/postgres/change_applier_catalog_race_test.go`). |
| [#270](https://github.com/bucardo/bucardo/pull/270) | "Only cache the relations we care about" — Bucardo was caching all pg_class rows on a 100k-relation source → 40 GB RSS for 3 syncs; after the patch 400 MB. | **already have (structurally)** | sluice's schema reader returns only the relations explicitly requested; we don't slurp all of pg_class. Worth keeping the **symptom signature** in mind as a regression-classifier: if sluice's memory ever balloons on a wide-schema source, the cause class is "scope-creep on caching". |
| [#269](https://github.com/bucardo/bucardo/pull/269) | "Lost DB connection in master-master setup": detect a stalled DB after ping+settled, auto-restart sync. Fixes Bucardo's [#213]. | **partial — sluice already has TCP-keepalive (#77)** | sluice landed `internal/netkeepalive` to keep dial paths alive; the **adjacent question** Bucardo's PR raises is what sluice's posture is when a connection is alive at the TCP layer but the DB-side has gone quiet (e.g. failover mid-stream, network blackhole that survives keepalives). Today sluice's streamer relies on libpq read-timeouts and PG keepalives. **Worth a roadmap note** to design an explicit "connection-health probe + sync auto-restart" path before we hit a customer who needs it. |
| [#272](https://github.com/bucardo/bucardo/pull/272) | pgservice env-var support: the CLI user and the pl/Perl function user might be different. | **not applicable** | sluice has no in-DB Perl functions; pgservice file resolution is libpq's, which sluice already uses. |
| [#273](https://github.com/bucardo/bucardo/pull/273) | Allow hyphens in schema/table names for `bucardo remove`. Regex was `\w` only; hyphens are valid PG identifiers when quoted. | **partial — verify** | sluice's identifier handling uses `quoteIdent` end-to-end and `internal/engines/postgres/reserved_idents.go` covers the reserved word path. Whether `internal/pipeline/filter_test.go` and friends accept hyphens-in-quoted-identifier filter rules is worth checking. Cheap to verify with one test. |

### 3b. Issues — user-support tickets

Bucardo's open issues are overwhelmingly **user-support tickets**, not bug
reports with reproducers (e.g. #245 "Sync shows No records found but source
table has 1286452 rows", no follow-up). The two with actionable detail are:

- [#268](https://github.com/bucardo/bucardo/issues/268) — "cannot delete from
  table `bucardo_delta_targets` because it does not have a replica identity
  and publishes deletes": a Bucardo internal table runs afoul of PG's REPLICA
  IDENTITY publication rule. **Sluice verdict: not applicable**, but it's a
  good reminder that any in-DB control table sluice ever introduces (e.g. the
  sentinel/state tables `sluice_migrate_state.*`) must declare a PK or REPLICA
  IDENTITY explicitly — otherwise it can't itself be the source of a CDC
  publication. Worth a note in the relevant ADR.
- [#262](https://github.com/bucardo/bucardo/issues/262) — Global statement
  timeout on source breaks `bucardo add sync` (lock acquisition exceeds
  timeout). **Sluice verdict: partial.** sluice's reader/writer paths set
  `statement_timeout = 0` explicitly on the connections that own
  long-running work? Worth verifying. If not, this is a class of
  silent-failure-on-managed-source we'd inherit.

### 3c. Bucardo tests — what's worth lifting as a *pattern*

`t/30-crash.t` — kills one of N databases mid-sync, asserts the other syncs
keep moving; restarts the dead DB, asserts the killed sync catches up. **The
relevant test-pattern for sluice:** the loud-failure tenet says we surface
problems, but the **adjacent property** is that a failure on one stream
shouldn't poison adjacent streams. The multi-source Shape A roadmap item
(`prep-multi-source-shape-a.md`) will eventually need a pin like this.

`t/40-conflict.t` — exercises Bucardo's conflict strategies (`latest`, named
DB priority, etc.) against deliberate write-write conflicts. **Not applicable
to sluice** — we're not multi-master, no conflict resolution surface — but
the *systemic shape* of "inject a conflict, kick the sync, assert the chosen
winner" is the pattern sluice's CDC apply-during-snapshot tests already
mirror.

`t/40-serializable.t` — mocks a serialization failure on the target by
installing a rule that raises `40001` and asserts Bucardo retries. **The
direct lesson for sluice:** our applier already handles serialization-class
errors (`internal/engines/postgres/applier_errors.go`), but I don't see an
integration pin that *injects* a 40001 on the target mid-stream and asserts
the retry+convergence. **Worth adding** as a "loud-failure discipline +
backoff" pin.

`t/10-makedelta.t` — tests cascaded sync chains (A→B→C, with B as a relay)
and asserts that data INSERTed at A propagates through B to C. **Not on
sluice's surface** (we're not a multi-hop replication mesh) but is a useful
shape for future multi-target.

`t/10-object-names.t` — **the gold pattern**: creates a table named
`test_büçárđo` with a column `pkey_⚕` (note the Unicode-character-class
non-ASCII codepoint) and asserts it survives end-to-end migrate + sync. **The
deliberate `pg_enable_utf8 = 0`** at the DBI level forces the byte-level path
to round-trip the UTF-8 bytes correctly — Bucardo found bugs at that boundary.
sluice's identifier coverage (`reserved_idents_test.go`) is rich on PG keyword
quoting but I don't see a multi-byte-identifier integration pin. **High-value
test-coverage gap, see §4.**

`t/40-customcode-exception.t` — Bucardo's user-defined exception handler
mechanism (you write a Perl callback that decides how to resolve a conflict).
**Not applicable** — sluice is opinionated about not exposing in-DB code
extensibility, and this is the canonical Bucardo wart we're correct to reject.

## 4. Test-coverage gaps in sluice

Ranked by "would catch a plausible-to-introduce silent-loss bug in sluice".

1. **Multi-byte UTF-8 identifier round-trip through CDC.** Bucardo's
   `t/10-object-names.t` pattern. Create `CREATE TABLE "tëst_šluicé" (pkey_⚕
   text)` on the PG source, run a clone + a few CDC INSERTs/UPDATEs/DELETEs,
   assert the target sees the same bytes. sluice's `quoteIdent` is correct in
   theory but a regression in any encode path (bulk-load CSV header, COPY
   FROM column list, pgoutput Relation message decode) could silently truncate
   or mojibake the identifier — and the existing reserved-idents tests use
   ASCII-only "bad" names. **Where it goes:** `internal/engines/postgres/`
   integration test; mirror for PG→MySQL in `internal/pipeline/`.
   - **Why this is the highest-leverage gap:** silent identifier corruption
     fails the loud-failure tenet in the worst way — the migration "succeeds"
     and the operator finds a missing or differently-named column at cutover.
   - **Sizing:** ~150 LOC for the PG-only test; another ~100 for the
     cross-engine variant.

2. **REPLICA IDENTITY USING INDEX on a no-PK table, UPDATE where identity
   columns are NOT changed.** pgcopydb PR #946's pin shape. sluice's
   `cdc_delete_matrix_pg_integration_test.go` covers DELETE under this
   identity configuration but the UPDATE variant — specifically when the
   replica-identity columns themselves stay unchanged and pgoutput emits the
   row's old-key-implicit form — is not pinned. The fix-the-class corollary
   from the Bug 74 lesson in `CLAUDE.md` (test every shape variant, not the
   representative): we have DELETE; UPDATE and UPSERT-via-CDC are the
   remaining shapes for the same identity configuration.
   - **Where it goes:** `internal/engines/postgres/cdc_update_using_index_integration_test.go`.
   - **Sizing:** ~120 LOC, one new file.

3. **CSV / COPY-format `null`-as-text vs actual NULL.** pgcopydb issue #931
   mirror. Create `t (c text)`, insert four rows: `'null'`, `'NULL'`, `''`,
   actual NULL. Migrate PG → PG (and PG → MySQL, where `\N` / empty-string
   semantics also vary). Assert all four byte-survive. sluice's bulk-load
   path likely handles this correctly via pgx COPY-binary, but a one-shot
   pin removes the class entirely.
   - **Where it goes:** `internal/pipeline/migrate_null_literal_integration_test.go`.
   - **Sizing:** ~80 LOC.

4. **CDC replay against a target table with `GENERATED ALWAYS AS IDENTITY`.**
   pgcopydb issue #941 mirror. sluice already filters STORED-generated columns
   via `generatedColCache`; the identity-generation variant (`pg_attribute.attidentity = 'a'`)
   is a separate but adjacent rule. The existing tests target STORED. Pin
   identity-column on UPDATE replay specifically.
   - **Where it goes:** `internal/engines/postgres/change_applier_identity_integration_test.go`
     or extend the existing `migrate_generated_integration_test.go`.
   - **Sizing:** ~80 LOC; mostly DDL + assertion plumbing.

5. **Mid-stream injected 40001 (serialization failure) on the applier.**
   Bucardo `t/40-serializable.t` mirror. sluice handles `40001` in
   `applier_errors.go` retry classification but a fuzz-style integration test
   that installs a target-side trigger raising `40001` on the first N
   attempts then succeeds confirms the retry-budget + final-convergence
   behaviour. Loud-failure tenet — we want to be sure the retry path commits
   exactly once.
   - **Where it goes:** `internal/engines/postgres/change_applier_serialization_integration_test.go`.
   - **Sizing:** ~120 LOC.

6. **Statement-timeout-non-zero on the source connection.** Bucardo issue #262
   mirror. Source DB has `statement_timeout = 5s` globally; sluice opens a
   reader connection and tries to do a long bulk-COPY. Either we override the
   timeout on our reader connection (and need a test pinning that) or we
   refuse-loudly with a structured error (same shape as the #61 Heroku
   preflight). Today this is unverified.
   - **Where it goes:** `internal/engines/postgres/connect.go` or
     `internal/engines/postgres/replication_preflight.go`; integration test.
   - **Sizing:** ~60 LOC for the preflight check; another ~80 LOC for the
     integration test.

Not on the gap list but noted: pgcopydb's `cdc-low-level/ddl.sql` exercises
the **libpq 16 KB read buffer overflow** path by installing a trigger that
raises a NOTICE with 32 KB of payload during apply. sluice uses pgx not
libpq directly, but the corollary — what happens when the source-side
trigger raises a huge NOTICE during pgoutput streaming — is not exercised in
our integration suite. Low priority; mostly a curiosity unless a customer
hits it.

## 5. Recommended adoptions, ranked

Note: (a)/(b)/(c) from the fork review (`json` equality, NOT VALID FK,
XID-wraparound preflight) are already adopted / in flight / queued and are
**not** repeated here.

1. **Multi-byte UTF-8 identifier round-trip pin** (gap #1 above). Highest
   confidence-to-leverage ratio: cheap test, catches the worst class of
   bug (silent identifier corruption). **Sizing:** ~250 LOC across two files.
   **Where:** `internal/engines/postgres/` + `internal/pipeline/`.

2. **REPLICA IDENTITY USING INDEX + UPDATE pin** (gap #2 above). Closes the
   shape-variant gap from Bug 74's lesson. **Sizing:** ~120 LOC.
   **Where:** `internal/engines/postgres/cdc_update_using_index_integration_test.go`.

3. **GENERATED ALWAYS AS IDENTITY apply pin** (gap #4 above). Confirms a
   class sluice already handles structurally but doesn't pin. **Sizing:**
   ~80 LOC. **Where:** extend `migrate_generated_integration_test.go`.

4. **Source statement-timeout preflight + override** (gap #6 above). New
   preflight check next to the #61 Heroku one; refuse-loudly if the source
   has a non-zero global `statement_timeout` and we can't override it on our
   reader connection. **Sizing:** ~60 LOC code + ~80 LOC test. **Where:**
   `internal/engines/postgres/replication_preflight.go`.

5. **Serialization-failure retry pin** (gap #5 above). Catches a class
   sluice handles in `applier_errors.go` but doesn't have an end-to-end pin
   for. **Sizing:** ~120 LOC. **Where:** `internal/engines/postgres/`.

6. **CSV `null`-as-text pin** (gap #3 above). One-shot test; closes the
   class. **Sizing:** ~80 LOC. **Where:** `internal/pipeline/`.

7. **(Note, not an adoption)** Add a paragraph to
   `prep-multi-source-shape-a.md` lifting the Bucardo `t/30-crash.t`
   pattern: "kill one stream's source mid-flight, assert the other streams
   keep running and the killed one catches up on restart". When Shape A is
   built, the pin shape is ready.

8. **(Note, not an adoption)** Add a paragraph to
   `prep-snapshot-cdc-handoff.md` referencing pgcopydb issue #943: any
   on-disk WAL-staging codepath needs a "poison message logged and
   quarantined, not fatal" recovery posture so a single bad record doesn't
   wedge the apply loop forever.

## 6. Explicitly rejected — with reasons

- **`MAX_TOLERATED_RESTORE_ERRORS` and the "tolerate N pg_restore errors"
  family (pgcopydb #937).** Same anti-tenet position as the fork review:
  silently dropping target objects is the silent-loss class sluice exists
  to prevent. We refuse-loudly with a structured report instead.
- **Bucardo's multi-master design + conflict resolution surface
  (`40-conflict.t`, `40-customcode-exception.t`).** Bucardo is PG-only,
  multi-master, and Perl-callback-based — three properties that conflict
  with sluice's tenets (IR-first, cross-engine, no in-DB code surface).
  Rejected wholesale.
- **Bucardo's pgservice env-var plumbing (PR #272).** Not applicable — sluice
  has no in-DB Perl functions; libpq + DSN already covers the use case.
- **Bucardo's per-relation cache scoping fix (PR #270).** Not applicable —
  sluice already scopes its relation queries; we don't have the bug.
- **pgcopydb's PG17 large-object fallback (PR #957).** Out of surface — sluice
  doesn't replicate large objects.
- **pgcopydb's catalog-mirror OID dedup (issue #930, PR #954).** Out of
  surface — sluice doesn't mirror pg_class to SQLite.
- **pgcopydb's snapshot-state-machine guard (PR #956).** C-only; the Go
  orchestrator owns snapshot lifecycle directly.
- **pgcopydb's KEEPALIVE-with-empty-timestamp crash (issue #943).** Not
  applicable directly — sluice's keepalive path is `pglogrepl` on the wire,
  no JSON intermediary. The pattern is referenced for snapshot→CDC handoff
  design (see §5 item 8), not adopted as code.
- **Bucardo upstream as a maintenance signal.** The repo is effectively
  dormant; we're not waiting on upstream for anything actionable. Any future
  reference is to the *test patterns*, not the Perl code.

## 7. Open questions for the maintainer

1. **#1 above (UTF-8 identifiers) — is there a known design ruling that
   non-ASCII identifiers are out of scope for sluice?** I didn't find one;
   the `quoteIdent` path looks correct, but the absence of a pin is the
   risk. If you'd rather declare them out-of-scope, the alternative is a
   refuse-loudly preflight that rejects non-ASCII identifiers in the source
   schema (cheap; same shape as #61).
2. **#4 above (source statement-timeout) — is the operator expected to set
   `statement_timeout = 0` on their session before running sluice, or should
   sluice override it on its own reader connection?** Override is friendlier
   but requires the role to have permission. The preflight + refuse-loudly
   path is the most-tenet-aligned answer but a connection-level
   `SET LOCAL statement_timeout = 0` on our own session is the cheaper
   correctness win.
3. **#5 above (serialization-failure pin) — is the existing
   `applier_errors.go` retry budget visible to an integration test, or does
   the test need to inspect counters?** The pin is much cleaner if the
   applier exposes a "retries observed" counter; if not, the test has to
   reason about wall-clock retries which is flakier.
4. **Bucardo's `t/30-crash.t` pattern for the multi-source roadmap item — is
   that the right pin shape for Shape A, or do you want a separate prep
   note?** I added a paragraph reference in §5 item 7; tell me if you'd
   rather a dedicated prep note edit.
5. **Reviewer corollary on the fork-review's NOT VALID FK in-flight item —
   does that change's pin matrix cover (a) PG target, (b) MySQL target via
   `SET FOREIGN_KEY_CHECKS=0` analogue, and (c) the dirty-source report
   surface ordering? Worth re-checking before merge, given the Bug 74
   "family-dispatched change" lesson.**
