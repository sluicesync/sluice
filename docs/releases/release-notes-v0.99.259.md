# sluice v0.99.259

The audit's performance batch plus its two remaining leverage items: fallback mode-parity and an independent-reader gate for the parquet boundary.

## Added

- **`--stage-dir` (env `SLUICE_STAGE_DIR`)** relocates sluice's large scratch files — the flat-file staged copy, the D1 `--stage-local` replica, and the export-as-parquet scratch — off the system temp dir (the tmpfs `/tmp` hazard); a missing directory refuses loudly.
- **The deploy-request index-build fallback now arms on `restore` and `sync start`, not just `migrate`.** The same walled PlanetScale index build in those modes previously always ended at the hint even with credentials available. Same flags as migrate; on restore/sync the existing `--planetscale-org` serves both the telemetry opt-in and the fallback, each arming on its own token pair. Unarmed runs are byte-identical.
- **A `DuckDB parquet compat` CI workflow** — a family×shape matrix export (uint64 max, −0.0 signbit, NaN/±Inf, denormals, DECIMAL tiers, temporals, JSON, empty-vs-NULL, arrays, row-group placement, GeoParquet CRS) generated through the real export codec and read back with real DuckDB, values compared exactly. A future parquet-go upgrade that breaks external readability now fails a gate instead of shipping silently. Plus response-shape pins for every PlanetScale API endpoint sluice reads.

## Performance

- **The mydumper statement stream is now linear in statement size** — one 64 MiB statement read in 128ms instead of 2.23s (~17×, measured; dumps taken with `--statement-size 64M` were ~15× slower per byte). The incremental lexer's boundary-spanning token semantics are pinned byte-identical against the original splitter as oracle.
- **csv/tsv/ndjson migrates stage into temp SQLite once, not twice** — schema and row readers share one staged copy, halving staging writes and peak temp space.
- **`backup export-as-parquet` bounds writer memory on BLOB-heavy tables** by rolling row groups at a 128 MiB byte target inside oversized chunks (row groups still never span chunks).
- **mydumper `.gz` chunks decompress ~1.5× faster** via klauspost gzip.

## Compatibility

- **No breaking changes.** One new optional flag; the fallback arming is opportunistic and WARN-at-most with unarmed runs byte-identical; typical parquet exports are shape-identical; the statement-stream rewrite is oracle-pinned byte-identical.

## Who needs this

Anyone reading large-statement mydumper dumps (the 17× fix), staging big flat files on small-temp hosts (`--stage-dir` + stage-once), exporting BLOB-heavy tables to parquet, or running restore/sync against PlanetScale safe-migrations branches (the fallback now reaches those modes).

## Install

- Binaries: https://github.com/sluicesync/sluice/releases/tag/v0.99.259
- Homebrew: `brew install sluicesync/tap/sluice`
- Scoop: `scoop bucket add sluicesync https://github.com/sluicesync/scoop-bucket; scoop install sluice`
- winget: `winget install sluicesync.sluice`
- Docker: `docker pull ghcr.io/sluicesync/sluice:v0.99.259`
