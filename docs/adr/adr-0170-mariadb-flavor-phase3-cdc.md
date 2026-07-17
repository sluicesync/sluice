# ADR-0170: `mariadb` flavor — Phase-3 CDC with domain GTIDs

## Status

**Accepted (2026-07-17).** Roadmap item 73 Phase 3, building on ADR-0168 (Phase 1) and ADR-0169 (Phase 2). Every MariaDB-specific behavior this ADR encodes was **ground-truthed live** on `mariadb:11.4` and `mariadb:10.11` during implementation — including two premises from the roadmap draft that the ground truth **falsified** (§ "Ground-truth corrections"). Pinned by unit tests plus a CDC integration matrix on both LTS lines (basic convergence, resume-after-kill exactly-once, no-per-transaction schema-cache churn, purged-position loud refusal).

**Concurrency note:** this chunk touches the binlog CDC streaming path (a new `MariadbGTIDEvent` case on the pump goroutine, GTID-set accumulation, reactive position-loss classification). The `-race` Integration job is CI-only (the dev box is CGO=0), so it **must pass `-race` before any tag**.

## Context

Phase 1 (ADR-0168) shipped bulk migrate/backup/verify with `CDC: CDCNone`, refused loudly with the coded `SLUICE-E-CDC-MARIADB-UNSUPPORTED` on every CDC surface. The vendored go-mysql v1.15.0 already carries complete MariaDB GTID support (`MariadbGTIDSet`, `ParseGTIDSet(MariaDBFlavor, …)`, `StartSyncGTID`), so Phase 3 is plumbing over an existing library, not new protocol work. MariaDB replicates with **domain GTIDs** (`domain-server-sequence`, e.g. `0-1-38`) — one position per replication domain — which the MySQL GTID parser and several MySQL-only catalog queries can't handle.

The vanilla MySQL binlog reader (`internal/engines/mysql/cdc_reader.go`) is the path; VStream (`cdc_vstream*.go`) is a separate reader and is untouched. The `binlogPos` JSON codec (`cdc_position.go`) needs **no** format change — it stores the GTID string opaquely; only the go-mysql parse and the verify/status SQL are format-sensitive.

## Decision

Flip the flavor to `ir.CDCBinlog` and flavor-branch every format-sensitive surface off `FlavorMariaDB`. **The MySQL-8 / vanilla path stays byte-identical** — `FlavorVanilla` is the zero value, so every branch's default is the pre-existing MySQL behavior.

The reader gains a `flavor Flavor` field (threaded from `Engine.Flavor` through `openBinlogCDCReaderShared`). The flavor-aware SQL helpers (`gtidModeOnFor`, `coldStartGTIDSetFor`) are free functions over a shared `rowQuerier` (both `*sql.DB` and `*sql.Conn`) so the CDC reader **and** the snapshot-pinned backup-position path (`captureBackupPositionInTx`, `CaptureBackupPosition`) share one MariaDB branch.

### The nine format-sensitive edit points

1. **Capability.** `FlavorMariaDB.CDC: CDCNone → CDCBinlog`.
2. **go-mysql syncer flavor** (`BinlogSyncerConfig.Flavor`) and **GTID-set parser** (`ParseGTIDSet`) → `r.goMySQLFlavor()` = `mysql.MariaDBFlavor` for MariaDB (a `MariadbGTIDSet`: one `domain-server-seq` GTID per domain), `mysql.MySQLFlavor` otherwise.
3. **GTID-mode auto-detect** (`gtidModeOnFor`). MariaDB has **no `gtid_mode` variable** (the `SHOW VARIABLES LIKE 'gtid_mode'` probe returns zero rows, which would wrongly fall the reader into file/pos mode). MariaDB is always GTID-capable on the 10.11+ floor, so the branch returns `true` unconditionally.
4. **Cold-start position** (`coldStartGTIDSetFor`). MySQL reads `@@global.gtid_executed`; MariaDB reads **`@@gtid_binlog_pos`** — the GTID of the last event group *written to the binlog*. Deliberately **not** `@@gtid_current_pos`, which folds in `@@gtid_slave_pos` (transactions applied but not necessarily re-logged) and can sit ahead of the binlog, asking `StartSyncGTID` to resume at a position not yet in any binlog file. On a pure primary the two are equal (ground-truthed both `0-1-N`).
5. **Per-event GTID accumulation + transaction boundary** (pump `case *replication.MariadbGTIDEvent`). See § "The load-bearing correctness fix".
6. **Reachability / purged-position refusal.** See § "Reachability".
7. **Master-status statement spelling.** See § "Ground-truth corrections" (2).
8. **`@@server_uuid` skip.** MariaDB has no `@@server_uuid`; the read errors 1193 and MariaDB always streams in GTID mode where `serverUUID` (a file/pos node-replace guard) is never consulted — so the probe is skipped on MariaDB to avoid a misleading "degraded" WARN on every stream open.
9. **Phase-1 refusal removed.** `mariadbCDCUnsupportedError` and the `ir.CDCUnsupportedExplainer` implementation are deleted — every mysql-family flavor now supports CDC, so no flavor needs the "CDC unsupported" hook. `SLUICE-E-CDC-MARIADB-UNSUPPORTED` stays defined in the `sluicecode` catalog (stable public code registry) but is no longer emitted.

### The load-bearing correctness fix: `MariadbGTIDEvent` on the pump

MariaDB opens each transaction with its **domain-GTID event**; there is **no separate `BEGIN` QueryEvent** as on MySQL. A plain-DML transaction is exactly `MARIADB_GTID → TABLE_MAP → ROWS → XID`. The vanilla pump handles only `*replication.GTIDEvent` (MySQL) and keys `TxBegin` off the `BEGIN` QueryEvent — **neither fires on MariaDB.** Without a new case, two silent failures result:

- **Positions would stall.** `r.gtidSet` is accumulated by applying each GTID event; if the MariaDB event is ignored, the set never advances past the cold-start point, so every emitted position is stale — a **silent wrong-resume-position gap** on the next restart. The new case calls `r.gtidSet.Update(e.GTID.String())` (`domain-server-seq`; `ServerID` is stamped from the event header by go-mysql's parser, so `String()` is correct).
- **No transaction boundary.** The batched applier and the backup/stream window logic (`stream.go` straddle) use `TxBegin`/`TxCommit` to avoid ending a window mid-transaction. The new case emits `TxBegin` for a **transactional (non-standalone)** group — the MySQL `BEGIN` analogue — and emits nothing for a **standalone** group (DDL; no XID follows), matching the MySQL DDL path (GTID → QueryEvent, no BEGIN). `TxCommit` still rides the `XIDEvent`, unchanged.

`CDCPositionCommitsAfterRows` stays **false** for MariaDB: like MySQL binlog, the GTID event precedes the transaction's rows, so positions do not commit after rows (this is the Bug-184 anchor-forge capability; MariaDB is in the MySQL camp, not the VStream camp).

### Reachability — the highest-risk silent-gap surface

**MariaDB exposes no reliable SQL surface to pre-check GTID reachability.** Ground truth:

- There is **no `GTID_SUBSET` function and no `@@gtid_purged`** (both MySQL-only) — the MySQL proactive check (`verifyGTIDSetReachable`) is inapplicable and would error.
- `@@gtid_binlog_state` reports the **newest** GTID per domain, **not a purged floor** — it is **UNCHANGED across a `PURGE BINARY LOGS`** (ground-truthed: `0-1-15` before and after purging every prior file). So a resume position below the purged floor is **indistinguishable from a live one** via SQL. A naive `Contain`-style check against `gtid_binlog_state` would falsely report "reachable" → the exact silent gap the loud-failure tenet forbids.

So the authoritative check is the **stream itself**: `StartSyncGTID` on a purged position surfaces MariaDB **error 1236** on the pump's first `GetEvent` (ground-truthed verbatim):

> `ERROR 1236 (HY000): Could not find GTID state requested by slave in any binlog files. Probably the slave state is too old and required binlog files have been purged.`

This wording shares **no substring** with the MySQL/Vitess file-pos 1236 (`"the master has purged required binary logs"`, matched by `isVStreamPurgedGTIDError`), so it gets its own matcher `isMariaDBPurgedGTIDError` keyed on the distinct phrase `"could not find gtid state requested"`. `classifyReaderError` wraps it as `ir.ErrPositionInvalid` (before the retriable classifier, so a purged position is never retried forever) → the streamer routes it to an ADR-0022/ADR-0093 cold-start re-snapshot, exactly as the VStream purged path does.

`verifyPositionResumableInner`'s MariaDB GTID branch is therefore a **minimal parse-validation that returns nil (defers)** — a warm resume from a *valid* position proceeds; a resume from a *purged* position is refused loudly by the reactive 1236 classification. Never a silent start-from-wrong-position. Pinned on both LTS lines (`TestMariaDB_CDCReader_PurgedPosition_LoudRefusal`).

Note: an **empty** MariaDB GTID position passed to `StartSyncGTID` also errors 1236 (it means "from the very beginning", which is purged on a server with history), so cold-start must never pass empty — it uses `@@gtid_binlog_pos`, which is populated whenever any transaction has committed.

## Ground-truth corrections (roadmap draft premises the live probe falsified)

1. **There is NO per-transaction dummy QueryEvent** (the roadmap draft's "perf trap … needs a cheap filter"). Streaming a live MariaDB with go-mysql's default flags, a plain-DML transaction produces `MARIADB_GTID → TABLE_MAP → ROWS → XID` — **no `BEGIN`, no dummy QueryEvent.** The only QueryEvents are real DDL (`ALTER`/`CREATE`, carrying the standalone+DDL GTID flag) and the session-variable SET preamble MySQL also emits for DDL. So the blanket `clear(r.schemaCache)` is **not** tripped per transaction, and **no dummy-event filter was added** — adding an over-broad one would have risked skipping a real DDL, the silent-DDL-drop the task explicitly warns against. This is pinned as an invariant: a `schemaCacheClears` counter (atomic, incremented at the clear site) asserts **zero** clears across N plain-DML transactions and **exactly one** across a real `ALTER` (whose new column is then decoded on the next row). `TestMariaDB_CDCReader_SchemaCacheChurn`, both LTS lines.

2. **`SHOW BINLOG STATUS` is accepted on 10.11 AND 11.4 already** (the draft implied it was a MariaDB-12-only rename). Ground truth on both LTS lines: `SHOW MASTER STATUS` **works**, `SHOW BINLOG STATUS` **works**, and `SHOW BINARY LOG STATUS` (the MySQL 8.4 spelling) **fails with error 1064**. The fallback list is now `{SHOW BINARY LOG STATUS, SHOW MASTER STATUS, SHOW BINLOG STATUS}` in all three call sites (`masterStatus`, `snapshotMasterStatus`, `probeMasterStatus` via the shared `masterStatusSpellings`) — ordered so that **no currently-supported server pays an extra round-trip** (MySQL 8.4 hits the first, MySQL 8.0 / MariaDB 10.11+ hit the second), with `SHOW BINLOG STATUS` present purely as forward-compat for MariaDB 12, which removes `SHOW MASTER STATUS`.

| statement | MySQL 8.4 | MySQL 8.0 | MariaDB 10.11 | MariaDB 11.4 | MariaDB 12 |
|---|---|---|---|---|---|
| `SHOW BINARY LOG STATUS` | ✅ | ❌ | ❌ (1064) | ❌ (1064) | ❌ |
| `SHOW MASTER STATUS` | (dep.) | ✅ | ✅ | ✅ | ❌ (removed) |
| `SHOW BINLOG STATUS` | ❌ | ❌ | ✅ | ✅ | ✅ |

## Consequences

- **MariaDB continuous sync, incremental backup, and add-table now work** — all route through the engine-neutral CDC orchestrator, which now sees MariaDB as a `CDCBinlog` engine like vanilla MySQL. Backup snapshot + position capture for MariaDB now take the consistent-snapshot + GTID-position path (`captureBackupPositionInTx` flavor-threaded), an intended consequence of the capability flip (previously the `CDCNone` guard routed backup differently).
- **`@@gtid_binlog_pos` empty-set edge:** a brand-new source with zero committed transactions yields an empty cold-start set, the same empty-`@@gtid_executed` edge MySQL has; integration setup always commits DDL first, so the streamed position is non-empty in practice.
- **Cross-engine `sync start` end-to-end validated (`mariadb → postgres`).** `TestStreamer_MariaDBToPostgres` drives the full pipeline: cold-start snapshot → CDC handoff → INSERT/UPDATE/DELETE convergence on a real PG target → stop → warm-resume of a while-down change. Asserts value fidelity through BOTH the bulk-copy AND CDC paths for int / text / **JSON identity** (carried verbatim — MariaDB JSON is textual, `ir.JSON{Binary:false}`, PG `json` preserves exact bytes) / **temporal** `DATETIME(3)`. Two defects the reader-level pins could not catch surfaced here and were fixed in this chunk:
  - **Warm-resume Bug-142 shape (fixed):** the streamer's retag-for-source stamps a resumed position with the source engine's `Name()` = `"mariadb"`, which `isMySQLFamilyEngine` did not accept → warm-resume crash-looped on `decode binlog position: engine = "mariadb"`. Added `engineNameMariaDB` to the decode-accept family (mirrors the vitess entry); pinned by `TestDecodeBinlogPosAcceptsMariaDBEngine`.
  - **Native uuid / inet6 / inet4 through CDC — now a LOUD PRE-FLIGHT REFUSAL on ALL targets (decode itself deferred).** Phase 2 (ADR-0169) added these for schema + bulk copy, where the driver hands back formatted TEXT. The binlog carries the RAW storage bytes (16 for uuid/inet6, 4 for inet4), and `decodeValue`'s `ir.UUID`/`ir.Inet` handler assumes the MySQL "stored in a VARCHAR → text" shape and stringifies the raw bytes → a **wrong value**. **The loudness was target-dependent and therefore insufficient:** a Postgres target rejects the bad string (22P02), but a **MySQL-family target lands these columns as `CHAR(36)`/`VARCHAR(45)`, which SILENTLY ACCEPT the garbage** — a reachable silent corruption on `mariadb → mysql / mariadb / planetscale` CDC, exactly the class Phase 3 makes reachable (Phase 1 refused MariaDB CDC entirely). The first tenet requires loud on ALL targets, so this is closed by a **flavor-gated, source-side, coded refusal — `SLUICE-E-CDC-MARIADB-NATIVE-TYPE-UNSUPPORTED`** — that fires BEFORE any data moves and is target-independent by construction (the stream/copy never starts):
    - at **CDC stream start** (`CDCReader.preflightMariaDBNativeUUIDInet`, an `information_schema` scan of in-scope tables — covers warm resume, backup incremental, and the multi-database fan-out),
    - at **cold-start snapshot open** (both the serial and concurrent openers call the shared `scanMariaDBNativeUUIDInet` before the FTWRL/bulk-copy — so a cold-start sync refuses pre-data, **zero rows** written to the target), and
    - at **mid-stream `add-table`** (`Engine.PreflightCDCScope` / `ir.CDCScopePreflighter`, since add-table extends a live stream's scope without re-running the stream-start preflight).

    The refusal names the offending `schema.table.column` and steers to bulk `migrate` (uuid/inet bulk copy is unaffected) or excluding the column. Pinned: reader-level refusal on both LTS lines (uuid AND inet6 named); the add-table hook as a unit family-matrix (uuid, inet, non-mariadb no-op); and an end-to-end `mariadb → mysql` sync asserting the coded refusal + **0 rows on the MySQL target** (the silent path, directly closed). The remaining follow-up is the **decode itself** — a flavor+type-aware binlog decode that FIRST ground-truths MariaDB's uuid byte ORDER (MariaDB reorders the time fields, so a straight big-endian format would produce a *valid-but-wrong* uuid, a Bug-74-class SILENT corruption if done without a live pin) and pins the uuid/inet4/inet6 family on both LTS lines. Filed (roadmap item 73 P3 follow-up).

## Alternatives considered

- **A synthetic proactive reachability check against `@@gtid_binlog_state`.** Rejected: ground truth proved it cannot detect a purged floor (unchanged across PURGE), so it would give false "reachable" verdicts — a silent gap. The reactive 1236 classification is the only faithful signal.
- **Parsing the oldest binlog's `Gtid_list` event to compute a purged floor.** No clean SQL surface; would require reading raw binlog files. Not worth it when the reactive 1236 already gives a loud, correct answer.
- **Adding a dummy-event filter "to be safe".** Rejected: ground truth shows no dummy event exists, and an over-broad filter before `clear(r.schemaCache)` is precisely how a real DDL would be silently skipped.
