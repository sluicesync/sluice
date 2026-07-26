# sluice v0.102.0

**`sluice metrics-watch` grows from a per-database alerter into an org-wide fleet collector: point it at an organization and it watches every database and branch, with a persistent sample sink so keeping the history no longer means standing up Prometheus.** Drop-in upgrade, no breaking changes. Single-database mode is untouched — the org-wide path is a separate branch taken only when you omit `--planetscale-metrics-db`, and every existing flag behaves exactly as before.

## Features

**Org-wide auto-discovery.** `metrics-watch` previously required naming one database (and optionally one branch). Omit `--planetscale-metrics-db` and it now discovers every database+branch the organization exposes and fans the poll across all of them, with bounded concurrency so a large org does not hammer the control-plane API, and `--include-database` / `--exclude-database` glob filters (matched against both `database` and `database/branch`; exclude wins on a conflict) to scope the sweep. Exported series carry `database` and `branch` labels, so one Grafana dashboard covers the whole org instead of one scrape target per database.

Discovery reuses the per-org service-discovery document sluice already calls rather than enumerating databases and branches through separate API calls: that document returns one element per database+branch for the entire org in a single authenticated request, lists only branches that actually have a metrics endpoint, and carries each one's pre-signed scrape URL — so a poll cycle is one authenticated call plus N unauthenticated scrapes regardless of org size.

One wrinkle that shaped the design and is worth knowing if you run a mixed org: **the service-discovery document carries no engine label**, and a real organization mixes Vitess/MySQL and Postgres databases. A single declared engine would therefore read the wrong metric-name table for half the fleet. sluice picks each target's table from marker-metric presence in that target's own exposition — the two engines' storage metric names are disjoint, so a wrong pick can only fail to *find* a series (leaving the honest `unobserved` degrade) and can never attribute one engine's number to the other's signal. An ambiguous exposition falls back to the declared engine. Single-database mode does not use this path at all.

**A persistent sample sink.** Until now the only durable output was an external Prometheus scraping `--metrics-listen`. `--sink-file` writes each polled sample as one JSON object per line, with size-based rotation (`--sink-file-max-bytes`, default 64 MiB; `--sink-file-max-files`, default 5 generations; a negative size disables rotation if you would rather own the file's growth), and `--sink-http` POSTs the same records to an endpoint of your choosing. Both are advisory and failure-isolated exactly like the existing `--notify-*` sinks: a dead sink is logged and swallowed, never stalling or failing a poll.

The record schema is deliberately explicit rather than compact — every metric is a nullable field with no `omitempty`, so an unobserved reading is a literal `null` and every key is present in every row. That matters for a file you intend to query later: a missing key and a zero are indistinguishable once a metric is absent, and a storage utilisation that reads `0` instead of `null` says "the volume is empty" rather than "we could not see it". The encoder refuses invalid UTF-8 and non-finite floats by field name and target rather than letting a JSON encoder silently substitute a replacement character, and integers ride as exact digits so a 64-bit value cannot round-trip through a float.

## Fixed

**The fleet exporter and the sink now carry the worst-pod storage family.** v0.101.0 added `sluice_target_storage_util_worst` and its two companions — the fullest pod of a branch, which on managed Postgres is routinely a replica rather than the primary, and therefore the pod a storage alert should actually watch. That work landed while the fleet and sink code was being written separately, so merging the two left fleet mode and the persisted records silently missing a metric the single-database exporter emits. A signal present in one mode and absent in another is exactly the "fixed the representative, missed the sibling" shape, and neither existing gate caught it: the fleet doc-sync ratchet's fixture left the worst-pod flag false so the exporter never emitted the family, and the sink's reflection ratchet proves every *record* field is validated, not that the record covers every snapshot field. Both are fixed, both fixtures now exercise the family, and the sink's mapping is guarded by its own flag rather than the primary's — the two are independently observable, so gating both on one flag would drop a reading that was actually taken.

## Compatibility

No breaking changes. Single-database `metrics-watch` is byte-for-byte unchanged: the org-wide path is a distinct loop entered only when `--planetscale-metrics-db` is omitted. All new flags are additive and default off; with no sink flags set, nothing is written anywhere. Two flag behaviours are worth noting because they differ from their nearest cousins elsewhere in the CLI: `--include-database` and `--exclude-database` may be **combined** here (exclude wins), unlike the mutually-exclusive table filters on `migrate`/`sync start`; and `--planetscale-metrics-branch`, when unset, means *every* branch in org-wide mode versus `main` in single-database mode. Both are deliberate and tested.

## Who needs this — action required

- **Anyone running more than a handful of PlanetScale databases** — org-wide mode replaces one `metrics-watch` process per database with one per organization, and the `database`/`branch` labels make a single dashboard cover the fleet.
- **Anyone who wanted the samples kept but did not want to run Prometheus** — `--sink-file` is the whole answer; the JSONL reads directly in DuckDB via `read_json_auto`.
- **Existing single-database users** — no action. Nothing about your invocation changes.
- Everyone else: no action needed beyond upgrading.

## Install

- Binaries: https://github.com/sluicesync/sluice/releases/tag/v0.102.0
- Homebrew: `brew install sluicesync/tap/sluice`
- Scoop: `scoop bucket add sluicesync https://github.com/sluicesync/scoop-bucket; scoop install sluice`
- winget: `winget install sluicesync.sluice`
- Docker: `docker pull ghcr.io/sluicesync/sluice:0.102.0`

**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
