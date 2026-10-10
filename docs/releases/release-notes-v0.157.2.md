# sluice v0.157.2

**A stream over one database on a busy shared server no longer falls behind without limit (Bug 300; MEDIUM, throughput, no data loss).** sluice sees a transaction marker pair for transactions it does not copy: on MySQL, every transaction on the server; on Postgres 14, every transaction that touched no published table. Each such empty transaction was recorded in backups and, on the serial apply paths, cost a target commit of its own. Above roughly 80 transactions a second elsewhere on the source, a serial `sync start`, `sync from-backup` broker or chain restore fell further behind indefinitely, with no error. Runs of empty transactions are now collapsed everywhere a serial path could see them, and chains written before this release are collapsed as they replay. No format or flag changes.

<!-- notes-claims-exempt: SAME-SERVER-POSITION-ECHO -- audit-backlog item name for a filed residual; it lives only in docs/dev/audit-backlog.md -->

## Fixed

**A source transaction that changed nothing in scope no longer costs a backup two records and every serial apply path a commit of its own (Bug 300; MEDIUM, throughput, no data loss).** Where these empty transactions come from:
- **MySQL:** the binlog is server-wide, so a stream over one database sees a BEGIN/XID pair for every transaction of every other database.
- **Postgres 14:** emits a pair for every transaction that touched no published table. Postgres 15 and later skip those.
- **VStream:** read from the code, not measured, to do the same for tables outside its filter.
- **Any source:** a client-side table or `--where` filter empties more.

`backup incremental` and `backup stream` recorded each one as an empty `tx_begin`/`tx_commit` pair. The serial apply paths committed each pair as its own position write: `sync start`, `sync from-backup` and chain restore under `--apply-concurrency 1`, `--apply-batch-size 1`, or the auto lane count degrading to serial. The default concurrent-apply lanes checkpoint on a cadence and were not affected.

The regression cycle measured about 12 ms per foreign transaction. A stream that falls behind also costs the source: a Postgres 14 slot retains WAL, and a MySQL stream that falls further behind than the binlog's retention can no longer resume.

A run of empty transactions now collapses to its last one at every point where a serial path could see it:
- **Backup capture:** both lanes record only the last empty transaction of each run, so a window's end position still advances past the foreign traffic and still ends on its recorded tail.
- **Replay of older chains:** the broker and chain restore collapse the runs of chains written before this release as they replay them. The broker does this after counting every event, so a position persisted inside an incremental means what it meant before (ADR-0191).
- **Live `sync start`:** collapses a run whose transactions arrive within 100 ms of each other. It holds a run for at most 1 s, the lanes' own checkpoint period, so a source busy elsewhere still has its position persisted every second. A change that carries data is never delayed.

Measured with 900 foreign transactions:
- **MySQL 8:** an incremental's chunks fall from 1,808 records to 14; a broker re-run from 460 position writes to 10; a `sync start` catch-up from 908 position writes to 2.
- **Postgres 14:** a `sync start` catch-up falls from 326 position writes to 2–3. The 900 empty pairs were confirmed in the slot by a server-side peek, independently of sluice's reader.

The live tests also assert that the persisted position reaches the source's own head (`@@GLOBAL.gtid_executed`, `pg_current_wal_lsn()`), read independently on a separate target server. Pinned by `TestEmptyTxRuns_MySQLGTID`, `TestEmptyTxRuns_MySQLFilePos`, `TestEmptyTxRuns_Postgres14`, `TestEmptyTxRuns_LiveSyncCatchUp`, `TestEmptyTxRuns_LiveSyncReachesSourceHead_MySQLGTID`, `TestEmptyTxRuns_LiveSyncReachesSourceHead_Postgres14` and `TestLiveEmptyTxStage_HoldCapBoundsARunTheLingerKeepsResetting`.

**Affected releases:** the serial batched path has written one position per empty transaction since v0.99.89 on a MySQL target and v0.113.0 on a Postgres target. It was measured on v0.156.12, v0.157.0 and v0.157.1.

## Compatibility

No format, flag or default changes. A manifest's `row_count` still counts records, transaction markers included. `backup incremental --max-changes` and `backup stream`'s change count now count recorded events, so a window that sees only foreign traffic closes on its timer rather than on its count.

**Known residuals (filed in `docs/dev/audit-backlog.md`):**
- `SAME-SERVER-POSITION-ECHO` (LOW): when the target is on the source's own server, each position write is itself an out-of-scope source transaction. Measured at about 13 position writes per 2 s on an idle MySQL stream; this release does not remove it.
- VStream sources are covered by the same downstream fix, but this release did not measure them.

**Not run for this release:** the dispatch-only `psverify` (live PlanetScale), `d1verify` (live Cloudflare D1) and `nekiverify` (live Neki) suites. They need live, billable infrastructure and run only on the operator's call. A live VStream source is the main path left unmeasured.

## Who needs this

- **Anyone running a serial apply path** (`--apply-concurrency 1`, `--apply-batch-size 1`, or a target whose auto lane count degrades to serial) over a MySQL source that shares its server with other busy databases, or over a Postgres 14 source with traffic outside the publication: upgrade. A stream that was slowly falling behind will catch up.
- **Anyone whose backups over such a source grew mostly empty transaction records:** new incrementals are smaller, and existing chains replay faster.
- **Everyone else:** upgrade at leisure; nothing to check.

---

**Install:** `brew install sluicesync/tap/sluice` · `go install sluicesync.dev/sluice/cmd/sluice@v0.157.2` · **Container:** `ghcr.io/sluicesync/sluice:0.157.2`

**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
