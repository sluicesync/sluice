# sluice v0.129.0

**Three changes that make a large PlanetScale migration less of a gauntlet.** Prompted by a real user's 30 GB PlanetScale region-to-region move that took 7–8 hours and 4–5 restarts because the gotchas surfaced one at a time, at the end, instead of once, up front: a pre-copy plan/gotcha report, a PlanetScale foreign-key pre-flight, and — the big one — large index builds that no longer wall on PlanetScale's statement-time limit. **Drop-in upgrade from v0.128.x — no schema or format change; one new refusal code and one behavior change, both scoped to PlanetScale/Vitess targets.**

## Added

**A pre-copy migration plan + gotcha report for `migrate`.** Before the bulk copy starts, sluice logs a concise plan — table count, the largest table by row estimate, foreign-key count — and, for a PlanetScale target, a nudge toward the levers that matter on a large table (`--planetscale-raise-query-timeout`, `--upfront-indexes`). It stays quiet on a small, unremarkable run. The point is to surface the large-table and foreign-key gotchas *before* an operator commits hours to a copy, not to discover them one failure at a time.

**A PlanetScale foreign-key pre-flight (`SLUICE-E-PS-FK-NOT-ENABLED`).** When the target is PlanetScale, the source schema carries foreign keys, and (with a service token) the target database has FK support disabled, `migrate` now refuses loudly *before* the copy — a fresh PlanetScale database ships with foreign-key constraints off, so otherwise the whole copy runs and then the constraints phase fails at the very end. The remedy is named: enable FK support on the target, or re-run with `--skip-foreign-keys` (each FK's referencing columns stay indexed, so the constraints can be added out-of-band). When FK support is on but the branch has safe-migrations enabled — which blocks the direct DDL sluice's FK add uses, with no deploy-request fallback for constraints — it WARNs rather than refuses, since that configuration may still work or the operator may disable safe migrations for the window. Without a service token it degrades to an advisory warning; it never silently proceeds.

## Changed

**Large index builds on a PlanetScale target now split into one `ALTER` per index (ADR-0184).** sluice defers secondary-index creation to a post-copy phase and, per ADR-0080, combines all of a table's indexes into one `ALTER TABLE … ADD INDEX …, ADD INDEX …` so InnoDB scans the table once — a real win on ordinary tables. On a large table on PlanetScale that single statement blows past the ~900 s statement-time limit (errno 3024), and `--resume` re-issues the identical walling statement so it never converges. This was the core of the reported ordeal: the 12.8 GB `audio_plays_daily` table's four-index `ALTER` hit the wall at exactly 900,004 ms, and each restart re-copied 30 GB. For a large table (source `DATA_LENGTH` ≥ 8 GiB) on a Vitess/PlanetScale target, sluice now emits one `ALTER` per index: each is a fraction of the combined statement's time (measured ~1/N, no total-time penalty at buffer-pool scale), stays under the wall, and — because each index is its own statement — `--resume` finishes only the indexes still missing instead of re-hitting a monolith. Every other target and every ordinary table keeps the combined `ALTER` unchanged.

The size decision is taken from the *source* table's stats, not the freshly-copied target's — a live PlanetScale test caught that the target's `information_schema` reports stale near-zero stats right after a copy (16 KB for a 36 MB table), which would have left the split inert exactly where it is needed. (The idea of splitting a table into partitions and recombining with `EXCHANGE PARTITION` was tested too — it recombines in 0.6 s but is blocked for a surrogate-PK-plus-independent-unique table like this one by MySQL's partitioning rules; it is documented in ADR-0184 as considered-but-deferred.)

## Compatibility

Drop-in from v0.128.3 — no schema or format change. `migrate` into a PlanetScale target with foreign keys may now refuse or warn at pre-flight where it previously failed (or would have failed) mid-run — the guidance is actionable and fires in seconds. The per-index index split only changes the DDL *shape* of the deferred index phase on a large table on a Vitess/PlanetScale target; the resulting indexes are identical.

**Anyone migrating a sizable database to PlanetScale** wants this release — the index-wall fix and the pre-flight guidance together are aimed squarely at the multi-hour, multi-restart experience this came from. **Everyone else: no action — this is a drop-in upgrade** (every non-PlanetScale target and every small table is byte-for-byte unchanged).

## Install

```
# Homebrew
brew upgrade sluice

# Scoop (Windows)
scoop update sluice

# Go
go install sluicesync.dev/sluice/cmd/sluice@v0.129.0
```

Container images: `ghcr.io/sluicesync/sluice:0.129.0` (multi-arch; the image tag carries no `v` prefix).
