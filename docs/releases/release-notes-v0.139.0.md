# sluice v0.139.0

**The 2026-09-01 audit's remaining tail, plus the three defects v0.138.0's own regression cycle found — every one of them measured on a real server before it was fixed, and two of them in code v0.138.0 had just shipped.** If you run multi-schema Postgres sync, a MySQL sync against a source that can fail over, backups against a replica, Cloudflare D1 tables with wide rows, `--inject-shard-column`, or MariaDB backup chains, one of the entries below names you.

## Fixed

**A forwarded `TIMESTAMP` cast to or from another type is refused, not only the swap between the two timestamp types.** A mid-stream `ALTER` from `TIMESTAMP` to `VARCHAR`, `DATE`, `TIME` or `BIGINT`, and each of those back, resolves through the executing session's time zone exactly as the `TIMESTAMP`-to-`DATETIME` swap already refused for. Measured on MySQL 8.0.46 and Postgres 16: a value stored at 20:00 UTC came back as 05:00 the next day under a `+09:00` session in every one of those shapes, and a date crossed midnight. Postgres behaves the same for `timestamptz` to text or date. The measurement also settled a case that could not be reasoned out: `timetz` is unaffected, because it stores its own offset per value rather than being normalised to UTC, so its casts measured byte-identical under both zones, while `time` to `timetz` invents an offset from the session and is refused. The widened rule contains the old one by construction, so nothing that refused before is allowed through now.

**The Postgres CDC reader refuses a `timestamp` to `timestamptz` swap performed while the stream was stopped.** Its prior shape for a table came from the current process's relation cache, so after a clean stop the source's `ALTER` was accepted on resume as the table's first known shape. Measured in four cells on Postgres 16 with the `ALTER` run under `Asia/Tokyo`: no refusal, the target column type unchanged, and every pre-existing row nine hours apart between source and target at exit 0. The reader now takes the same seed the MySQL lanes take, the target's current column types on a warm resume, and checks a table's first relation message against it.

**A MySQL `sync` cold start on a GTID-mode source hands the CDC stream a GTID position, so a failover to a promoted replica resumes instead of re-copying.** Both snapshot openers stamped a file-and-offset position with the server's identity regardless of `gtid_mode`; only the backup doors and the from-now open picked the GTID arm. On a real promoted replica the resume refused on server identity, fell through, and re-ran the cold start, dropping a target-only row on the way.

**A multi-schema Postgres `sync start` checks source replica identity before it publishes anything.** The fan-out ran the partitioning and inheritance preflights and not this one, so a run over a table with no usable replica identity streamed it, and the source application's own `UPDATE` and `DELETE` on that table began failing. The refusal carries `SLUICE-E-SOURCE-REPLICA-IDENTITY` and now runs per selected schema ahead of the spanning snapshot, which is the fix rather than a detail: that snapshot is what creates the database-wide publication, so a refusal arriving later has already let sluice break the operator's writes.

**A multi-schema Postgres `sync start` honours `--slot-name`.** The flag reached the single-schema paths and nothing on the fan-out, so a multi-schema run created and resumed the default slot whatever was asked for. Two such streams against one database could not coexist, and the slot name recorded in the CDC state row named a slot nobody had chosen. Both the cold start and the warm resume are fixed together, because a cold start that creates a named slot paired with a warm resume that opens the default one resumes from whatever position that other slot sits at.

**`backup full` against a Postgres hot standby refuses before it reads a row.** The standby preflight was reached on the backup path, but its refusal was treated as just another reason the snapshot view was unavailable, so the run copied every row and then died with a raw `SQLSTATE 55000` at the position capture, which cannot run on a standby either. What was left behind was an uncommitted manifest that `restore` refuses. Recording no position and copying anyway was considered and rejected: a positionless backup is extended from the source's current position with only a warning, so it would have traded this loud failure for a silent gap in the chain.

**Cloudflare D1 pages are sized in bytes, so wide-row tables migrate.** The reader fetched a fixed 1,000-row page behind an 8 MiB limit, on the premise that D1 caps a response near 1 MiB. Measured on real D1, there is no such cap: a 1,000-row page of 16 KiB values returned whole at 32.8 MB and failed with an unhelpful decoding error, so any table averaging more than about 8 KB per row could not be migrated at all. Pages now size against a byte budget and halve on overflow, and a single row that cannot fit refuses by name with the new `SLUICE-E-BULKCOPY-ROW-TOO-LARGE`.

**`--inject-shard-column` forwards DDL into the target's own schema instead of the source's database name.** Under that flag a MySQL to Postgres sync died on its first forwardable DDL, looking for a schema named after the MySQL database. Rows and the initial table creation had always used the bound schema, so the stream reached the DDL with a healthy target and stopped there. Postgres to Postgres had the identical defect whenever the target schema differed from the source's, which every earlier test missed by running one schema to the same name.

**A MariaDB resume whose lineage anchor was purged says it could not verify the instance, instead of claiming it did.** v0.138.0 confirmed the lineage from the oldest retained binlog's start state. That evidence is consistent with a routine purge and is equally produced by a rebuilt instance whose numbering rolled past the anchor and whose transaction identifiers collide, which the regression cycle built and which recorded a foreign instance's rows as the chain's delta at exit 0. MariaDB carries no instance identity to settle it, so the resume proceeds under the `UNVERIFIED-INSTANCE-IDENTITY` warning with the evidence in the message.

## Compatibility

Drop-in from v0.138.0. No flag change, no format change. One new error code, `SLUICE-E-BULKCOPY-ROW-TOO-LARGE`.

New refusals on configurations that previously ran, each one a case that was losing or corrupting data quietly: a forwarded `ALTER` casting to or from `TIMESTAMP`; a `timestamp` to `timestamptz` swap made while a Postgres stream was stopped; a multi-schema `sync start` over a table with no usable replica identity; and `backup full` against a Postgres standby (`SLUICE-E-CDC-STANDBY-SOURCE`). A MariaDB resume whose anchor was purged now warns `UNVERIFIED-INSTANCE-IDENTITY` where it used to log an informational line, and proceeds as before.

On a `gtid_mode=ON` MySQL source, a `sync` cold start now records a GTID position where it previously recorded a file and offset. Existing positions keep resuming on their original arm, so an upgrade needs nothing; the change takes effect at the next cold start. One corner is worth naming because it is a routine operator action: a GTID-mode source that has executed nothing, either a fresh server or one that has just had `RESET MASTER` run against it with its data intact, is anchored in file-and-offset mode with a warning. An empty GTID set is not something sluice can read back, and the file-and-offset anchor is exactly what earlier releases recorded there. The source moves onto the GTID arm by itself at the first capture after its next transaction.

## Who needs this

Anyone running multi-schema Postgres sync, MySQL sync against a source that can fail over or be rebuilt, backups from a replica, Cloudflare D1 with rows larger than a few kilobytes, `--inject-shard-column`, or MariaDB backup chains. Everyone else can upgrade at leisure.

## Install

```
brew install sluicesync/tap/sluice
scoop bucket add sluicesync https://github.com/sluicesync/scoop-bucket && scoop install sluice
go install sluicesync.dev/sluice/cmd/sluice@v0.139.0
docker pull ghcr.io/sluicesync/sluice:0.139.0
```
