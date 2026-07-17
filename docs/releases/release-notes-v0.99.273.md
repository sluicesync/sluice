# sluice v0.99.273

A CRITICAL silent-loss fix surfaced by the 2026-07-17 confirming audit — the VStream partial-row-image belt (v0.99.272, ADR-0172) was wired into only one of its two dispatch paths.

## Fixed

- **CRITICAL: the item-74 NOBLOB partial-row-image belt was missing from the cold-start CDC dispatch path, so a self-hosted Vitess `binlog_row_image=NOBLOB` source could silently overwrite unchanged BLOB/TEXT columns with NULL on the default first sync.** The belt refuses a partial VStream row image before decode — but v0.99.272 wired it only into `vstreamCDCReader.dispatchRow` (the warm-resume path), not its hand-mirrored twin `vstreamSnapshotStream.dispatchCDCRow`, which serves the cold-start snapshot→CDC catch-up. A cold-start sync (the common case) therefore reached the unguarded path: a NOBLOB UPDATE omits an unchanged BLOB/TEXT column as a −1-length NULL cell, `decodeVStreamRow` read it as a genuine NULL, and the applied UPDATE silently wrote NULL over the column's real value — stream green, row counts equal. The belt now fires on **both** dispatch paths, pinned by a wiring test that drives `dispatchCDCRow` directly with a partial `DataColumns` bitmap and asserts the coded `SLUICE-E-CDC-ROW-IMAGE-PARTIAL` refusal (proven non-vacuous: it fails "want refusal; got nil" without the belt call). This is the "fix landed in one sibling path but not its mirror" class — a mirror method is a mirror obligation.

## Compatibility

- **A refusal, not a behavior change for correct configs.** The belt only ever fires on a genuinely-partial self-hosted-Vitess row image; a FULL stream — including all of PlanetScale (which pins `binlog_row_image=FULL`) — is unaffected on both dispatch paths. Exposure was narrow (self-hosted Vitess with the `AllowNoBlobBinlogRowImage` experimental flag), but a silent-loss on a default path is fixed regardless of blast radius, per the project's correctness-gates-throughput tenet.

## Who needs this

Anyone running continuous CDC from a self-hosted Vitess cluster whose underlying mysqlds use a non-FULL `binlog_row_image` (NOBLOB with the experimental flag). PlanetScale and vanilla-MySQL/MariaDB sources are unaffected.

## Install

- Binaries: https://github.com/sluicesync/sluice/releases/tag/v0.99.273
- Homebrew: `brew install sluicesync/tap/sluice`
- Scoop: `scoop bucket add sluicesync https://github.com/sluicesync/scoop-bucket; scoop install sluice`
- winget: `winget install sluicesync.sluice`
- Docker: `docker pull ghcr.io/sluicesync/sluice:v0.99.273`
