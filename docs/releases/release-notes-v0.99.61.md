# sluice v0.99.61

**A multi-shard Vitess/PlanetScale source with no shard discriminator is now refused before any data moves, instead of silently overwriting rows across shards (Bug 152).** A sharded keyspace fronted by vtgate merges every shard into one logical stream; copying that into a single keyed target table without a discriminator could silently drop rows when keys collide across shards. sluice now catches that configuration at preflight and tells you exactly how to proceed.

## Added

- **Cross-shard-collision preflight (Bug 152).** When the source is a multi-shard Vitess/PlanetScale keyspace (detected via `SHOW VITESS_SHARDS` on the `planetscale`/`vitess` source flavors) and `--inject-shard-column` is not set, sluice refuses — before any data moves — to copy into a target table that has a primary key or a UNIQUE constraint. Every shard's rows would otherwise merge into that one table through vtgate, and rows from different shards that share a key value (per-shard auto-increment ranges, tenant-local ids) would silently overwrite each other: an exit-0, fewer-rows-than-source data-loss class. The refusal names the two ways forward.
- **`--allow-cross-shard-merge`** on `migrate` and `sync start` — the explicit opt-out, for when the key is globally unique across shards (e.g. Vitess sequences or UUID keys) so no overwrite can occur. The structural alternative is `--inject-shard-column NAME=VALUE` (ADR-0048), which adds a per-shard discriminator and a composite PK.

## Compatibility / notes

- **No effect on single-shard or non-sharded sources** (an unsharded keyspace reports one shard; vanilla MySQL and Postgres aren't sharded and the check is skipped without even connecting for shard discovery).
- **No effect when `--inject-shard-column` is set** — that path already keeps per-shard rows disjoint and is governed by its own preflight (ADR-0048).
- **Keyless tables are never refused** — they're already at-least-once with no overwrite, so merging shards into them loses nothing.
- If the shard layout can't be determined (e.g. the discovery query fails), the preflight **fails closed** (refuses, naming the opt-out) rather than risk a silent overwrite — a safety-over-convenience choice for a data-loss guard.
- This is a *new refusal* for one specific, already-unsafe configuration; if you were running a multi-shard source into a single keyed target without a discriminator, you were silently losing rows, and sluice now stops and tells you. Add `--inject-shard-column` (recommended) or `--allow-cross-shard-merge` (if your keys are globally unique) to proceed.

## Who needs this

- Anyone migrating or syncing from a **sharded** PlanetScale/Vitess keyspace into a single consolidated MySQL/Postgres table.

## Install

```
# Linux/macOS/Windows binaries + checksums on the release page.
docker pull ghcr.io/sluicesync/sluice:v0.99.61
```
