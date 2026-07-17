# sluice v0.99.267

Provider-advisory hardening from the Vultr, Azure, and Supabase probes, the confirming audit's small-leftovers tail, and the read-replica finding.

## Added

- **Vultr Managed MySQL retention advisory.** A `*.vultrdb.com` source WARNs that the platform purges binlogs ~10–16 minutes after creation regardless of what `@@binlog_expire_logs_seconds` reports — and, unique among the managed-MySQL providers, that no retention knob exists at all (API, CLI, and SQL paths all reject it). CDC there is migrate-and-cutover-shaped.
- **Platform-internal slot roster.** `sluice slot list` labels Neon's `wal_proposer_slot` and Vultr's `pghoard_local` as platform-internal, and `slot drop` refuses them without `--force`.
- **Standby/read-replica CDC sources refuse with the coded `SLUICE-E-CDC-STANDBY-SOURCE`** — pointing `sync start` at a Supabase `-rr-` replica (or any streaming standby) now names the standby and steers to the primary instead of dying at a raw SQLSTATE 25006. A replica remains a fine bulk-`migrate` source: the parallel snapshot-pinned copy works unreduced on PG 16+ standbys (live-validated). New live-validated managed-services sections for Azure Flexible Server, Vultr, and Supabase read replicas; the managed-MySQL retention comparison now spans five providers.

## Changed

- **"A pooler cannot proxy replication" is corrected to provider-dependent** — Vultr's managed pgbouncer carried replication end to end (live-verified). The docs and hint texts now say most poolers strip the replication parameter while some forward it; the coded refusal still fires only on the observed strip signature.
- **PG 15+ negative numeric scale → MySQL refuses upfront by name** (previously a raw Error 1064 at CREATE), with the lossless `--type-override` recovery; `verify`/`slot list`/`slot drop` gained the IPv6-only connect hint (Bug 196 residual).

## Fixed

- Single-full restores now run the BackupID recompute check (previously chain-only); `--stage-dir` governs the sqlite `.sql`-dump materialize; the mydumper zero-chunk WARNs dedupe per table per run.

## Compatibility

- **No breaking changes.** Advisories WARN; the new refusals convert raw driver errors into coded, remedied ones.

## Who needs this

Anyone running sluice against Vultr Managed Databases, Azure Flexible Server, or Supabase read replicas — each now has live-validated guidance and the right preflight behavior. Anyone who hit a raw 25006 pointing CDC at a replica, or a raw 1064 on a negative-scale numeric, gets a named remedy instead.

## Install

- Binaries: https://github.com/sluicesync/sluice/releases/tag/v0.99.267
- Homebrew: `brew install sluicesync/tap/sluice`
- Scoop: `scoop bucket add sluicesync https://github.com/sluicesync/scoop-bucket; scoop install sluice`
- winget: `winget install sluicesync.sluice`
- Docker: `docker pull ghcr.io/sluicesync/sluice:v0.99.267`
