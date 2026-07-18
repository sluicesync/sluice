# sluice v0.99.277

**Row-level filtering follow-ons** — a small release that finishes the `--where` rollout from v0.99.276 (ADR-0173): the FK-orphan safety flag now parses under the spelling every doc recommends, and there's a dedicated operator guide. No change to any successful path.

## Fixed

### `--allow-degraded-fks` parses under its documented spelling

kong auto-kebabs the `AllowDegradedFKs` field name to `--allow-degraded-f-ks` (it splits the `FKs` capital run), but the `--help` prose, the `SLUICE-E-WHERE-FK-ORPHAN` error hint, and the v0.99.276 release notes all recommend `--allow-degraded-fks`. So the documented spelling was rejected — with a self-correcting kong "did you mean `--allow-degraded-f-ks`?" suggestion, but rejected all the same. This is the flag you reach for when a filtered `migrate --where` orphans a foreign key, so it's exactly the moment you don't want a papercut.

An explicit `name:"allow-degraded-fks"` tag (the same treatment `--mysql-sql-mode`, `--zero-date`, and the other capital-run fields already get) binds the spelling the docs use. It's pinned in a CLI-parse test so a future dropped tag re-breaks loudly rather than silently. The v0.99.276 post-release regression cycle caught the mismatch.

## Added

### Filtered / subset-migration operator guide

A new `docs/operator/filtered-subset-migration.md` walks through row-level `--where` filtering end to end: the per-table flag surface, the `migrate` source-side push-down and matching `verify --where`, the referential-integrity gotcha (filtering a parent orphans its children → `SLUICE-E-WHERE-FK-ORPHAN`, and the `--allow-degraded-fks` escape), and the continuous-sync path — the row-move truth table (a row updated out of scope becomes a target `DELETE`, not a leak), the restricted client-side CDC grammar, and the full-before-image preflight. It's wired into the README operator-guide list.

## Compatibility

**No behavior change.** The flag fix makes the already-documented `--allow-degraded-fks` spelling work; the guide is documentation. The behavior of row-level `--where` filtering itself is unchanged from v0.99.276.

## Who needs this

Anyone using (or about to use) `migrate --where` / `sync --where` for a filtered / subset migration — especially if you hit the FK-orphan refusal and typed the flag exactly as the error told you to. Everyone else can skip this one; it's a documentation-and-papercut release on top of v0.99.276.

## Install

- Binaries: https://github.com/sluicesync/sluice/releases/tag/v0.99.277
- Homebrew: `brew install sluicesync/tap/sluice`
- Scoop: `scoop bucket add sluicesync https://github.com/sluicesync/scoop-bucket; scoop install sluice`
- winget: `winget install sluicesync.sluice`
- Docker: `docker pull ghcr.io/sluicesync/sluice:v0.99.277`
