# sluice v0.106.0

**One opt-in feature: sluice can raise PlanetScale's per-query statement timeout for the duration of a migration, then put it back.** It came out of the same 122 GB / 153-million-row scale work as v0.105.0 — the ~900-second statement wall is the root of the index-build and foreign-key-validation failures, and it turns out to be a *configurable* keyspace setting rather than a fixed limit. This release lets you opt into raising it to its 3600-second maximum for a migration, with a strong "leave it as you found it" guarantee.

### Added

**`migrate --planetscale-raise-query-timeout`** raises the target keyspace's `queryserver-config-query-timeout` to 3600 seconds before the copy begins, runs the whole migration under the raised limit, and reverts it afterward. It gives the direct index/FK paths headroom on tiers where an operation fits 3600s but not the default 900s.

It is **off by default and tightly gated** — it fires only on a PlanetScale/Vitess target with `--planetscale-org` and the service token present (the same credentials the deploy-request fallback uses), refuses loudly if set without them, and skips the raise entirely when the migration's largest table is under a million rows. It raises **once up front and reverts once at the end** (or on abort), never per table.

**The revert is the careful part.** The previous value is recorded and restored — an operator who had customized the timeout gets that value back, not a hardcoded default. The record is persisted to `sluice_migrate_state` *before* the change takes effect, so a crash mid-run is recoverable: a `--resume`, or even a bare re-run, detects and reverts a dangling raise before it touches anything.

Validated end-to-end against a real PlanetScale database: the keyspace timeout rose to 3600, the copy ran under it, and it reverted to the default when the run completed, with the data landed exactly.

### Compatibility

Drop-in. The flag is new and off by default; a migration that does not pass it behaves exactly as before. No format changes, no other flag changes.

### Who needs this

**Operators doing large PlanetScale-MySQL migrations who want the direct index/FK path to have more headroom** — specifically on faster (local-NVMe / Metal) tiers, where a deferred index build fits 3600 seconds but not 900. It is **not** a universal fix: on network-attached storage a large index build can exceed even 3600 seconds, so raising the timeout does not help there, and `--upfront-indexes` remains the cleaner path on that tier. And it has a real cost — the change is keyspace-wide and its rollout is a rolling tablet restart, several minutes each way, which is why it is opt-in and size-gated rather than automatic.

**Worth knowing:** the 3600-second maximum is PlanetScale's own product limit, not a Vitess constraint — Vitess itself places no upper bound on this setting. So an even higher ceiling, which would let longer index builds complete, is a question for PlanetScale rather than a limit in the engine.

### Install

- Binaries: https://github.com/sluicesync/sluice/releases/tag/v0.106.0
- Homebrew: `brew install sluicesync/tap/sluice`
- Scoop: `scoop bucket add sluicesync https://github.com/sluicesync/scoop-bucket; scoop install sluice`
- winget: `winget install sluicesync.sluice`
- Docker: `docker pull ghcr.io/sluicesync/sluice:0.106.0`

**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
