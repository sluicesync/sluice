# sluice v0.99.37

**Three correctness fixes, all sourced by the post-release battle-test cycles: shard-consolidation leases are now host-timezone-independent, hard-killed backups no longer leak WAL-pinning replication slots, and a PG→MySQL migration with indexed `text`/`bytea`/`json` columns now refuses loudly BEFORE copying data instead of failing at index-build time after it.**

## Fixed

- **ADR-0054 shard-consolidation leases are host-TZ-independent (Postgres targets).** `lease_expires_at` is a naive `TIMESTAMP` column and the driver encodes `time.Time` parameters as the host's local wall-clock digits — so a sluice process on a TZ-behind-UTC host wrote expiries hours in the past, making a just-acquired lease instantly stealable by a peer shard (the lease's DDL-apply serialization guarantee was void); TZ-ahead hosts got stuck leases outliving their TTL. Lease times are now bound in UTC and the SQL guards compare against `timezone('utc', now())`. MySQL was verified already-correct by construction (the connector pins `Loc=UTC` + session `time_zone='+00:00'`) and gained the same pin. Both engines now carry an explicit host-TZ-independence test (UTC-7 and UTC+9 directions) so UTC-only CI exercises the class. Rows written by prior versions on UTC hosts are fully compatible.
- **Hard-killed `backup full` runs no longer leak a WAL-pinning replication slot per kill (Bug 137).** The snapshot anchor slot was persistent at protocol level and only dropped by a clean Close — a SIGKILL'd backup left `sluice_backup_anchor_<timestamp>` orphans that silently pinned WAL forever (cumulative source disk pressure). The anchor is now created protocol-`TEMPORARY` (the server drops it automatically when the owning connection dies, including on process death), and resuming a backup sweeps inactive anchor orphans left by OLDER binaries (1-hour safety margin; each drop WARN-logged; younger suspects get a WARN naming the manual drop command). `--chain-slot` anchors stay persistent + FAILOVER by design.
- **PG→MySQL-family migrations with indexed TEXT/BLOB/JSON-landing columns refuse EARLY with the workaround named (Bug 136).** A PG `text` (or `bytea`, `json`/`jsonb`, array, hstore, wide-varchar-downmap) column carrying a UNIQUE or secondary index translated to MySQL index DDL that fails (Error 1170/3152) — loudly, but only at the index phase AFTER all rows had copied; `schema preview` showed the invalid DDL with no warning. The shared cross-engine preflight now refuses before any DDL or rows move, naming the table.column, the index, and the `--type-override <col>=varchar(N)` workaround; `schema preview` carries the same advisory in a dedicated section (and `text_index_refusals` in JSON output). sluice deliberately does NOT auto-emit prefix-length index parts — a prefix UNIQUE index silently changes uniqueness semantics. MySQL→MySQL prefix-indexed sources are unaffected (prefix lengths round-trip as before). Also closes a gap where `vitess`-flavor targets skipped this family of translation notices.

## Compatibility

- A PG→MySQL/Vitess migration that previously copied everything and then failed at create-indexes now refuses up front — same outcome, hours earlier, with the fix named. Use `--type-override` exactly as before to proceed.
- Backup anchor slots are temporary; observers of `pg_replication_slots` during a backup will see `temporary=t` on the anchor. No format changes.

## Who needs this

- Anyone running sluice on hosts not set to UTC (the lease fix).
- Anyone whose backups can be killed/crash (the slot-leak fix — and resume cleans up after older binaries).
- Anyone migrating PG schemas with indexed text/bytea/json columns to MySQL-family targets.

## Install

Binaries for Linux/macOS/Windows (x86_64 + arm64) attached; container image `ghcr.io/sluicesync/sluice:0.99.37`. Verify with `checksums.txt`.
