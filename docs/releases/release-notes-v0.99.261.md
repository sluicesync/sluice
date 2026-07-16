# sluice v0.99.261

Two same-day fixes for defects the v0.99.259 regression cycle filed: the giant-statement read path is now linear end-to-end, and the off-PlanetScale fallback arming contract holds as documented.

## Fixed

- **mydumper giant-statement reads are now linear end-to-end (Bug 191).** v0.99.259 fixed the statement splitter, but every quoted value's decode buffer below it was still sized to the remaining statement tail — keeping the pipeline quadratic (a 16 MiB single-statement chunk cost ~1.8s and 34 GB of allocation churn; the cycle's 49 MiB shape took ~350s). The decoder now pre-scans to the closing quote and sizes the buffer to the value: ~75ms / 139 MB for the same chunk, benchmarked at the pipeline level. The fix also covers a worse same-class sibling found during implementation — the double-quote path, mydumper's default emit shape, copied the entire tail per value. Byte-exactness is pinned across both quote delimiters × the full escape matrix plus a million-input differential fuzz against the previous decoder. The decode-stall class that starved `LOAD DATA` past `net_read_timeout` on earlier gz runs is plausibly closed.
- **`restore`/`sync start` with `--planetscale-org` + the service-token pair on a non-planetscale target no longer trip the telemetry refusal (Bug 192)** — fallback intent is now keyed on the supplied token pair, so the arming is inert-with-WARN off-planetscale exactly as the v0.99.259 notes promised. The org-alone and partial-pair typo-catch refusals are unchanged.

## Compatibility

- **No breaking changes; no new flags or codes.** Decode output is byte-identical (differential-fuzz-pinned); one refusal that contradicted documented behavior now proceeds with the documented WARN.

## Who needs this

Anyone reading mydumper dumps taken with a large `--statement-size` (the end-to-end fix this time — v0.99.259 was necessary but not sufficient), and anyone arming the index-build fallback on `restore`/`sync start` in scripts that also run against non-PlanetScale targets.

## Install

- Binaries: https://github.com/sluicesync/sluice/releases/tag/v0.99.261
- Homebrew: `brew install sluicesync/tap/sluice`
- Scoop: `scoop bucket add sluicesync https://github.com/sluicesync/scoop-bucket; scoop install sluice`
- winget: `winget install sluicesync.sluice`
- Docker: `docker pull ghcr.io/sluicesync/sluice:v0.99.261`
