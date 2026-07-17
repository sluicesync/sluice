# sluice v0.99.272

A post-MariaDB-arc batch: native MariaDB `uuid`/`inet` now stream through CDC faithfully, a silent-loss belt for self-hosted Vitess partial row images, and two roadmap-leftover closures.

## Added

- **MariaDB native `uuid`/`inet6`/`inet4` columns now decode faithfully through CDC** (roadmap item 73 Phase 3 follow-up, ADR-0171) — lifting the loud pre-flight refusal v0.99.271 shipped for them. The correct decode required ground-truthing MariaDB's actual binlog byte layout, and the anticipated hazard was wrong: contrary to the expected `UUID_TO_BIN` time-field reordering, MariaDB stores UUID **canonical big-endian** (no swap). The real trap is that MariaDB frames these types as length-prefixed and **strips trailing `0x00` bytes** on the wire (a nil uuid / `0.0.0.0` / `::` arrive empty; a zero-suffixed value arrives short), so the decoder right-pads to the fixed width and takes the width from the declared `data_type` rather than the received length — and `inet6` text is rendered with MariaDB's BSD `inet_ntop6`, which diverges from Go's `net/netip` on IPv4-compatible `::a.b.c.d` forms. Every one of those was verified byte-exact against a live MariaDB `SELECT` on 11.4 and 10.11. A naive implementation would have silently corrupted zero-suffixed values on a MySQL-family target — so this is pinned by a full byte→text family matrix plus live same-engine and cross-engine CDC value-fidelity tests.
- **PlanetScale index-build fallback arming completed across all modes** (audit gap #12). The ADR-0148 safe-migrations index-build fallback (auto-retry on `errno 3024`/`1105`) — already wired through `migrate`, `restore`, and sync cold-start — is now also armed on the fleet `sync run` per-sync YAML specs and the `sync from-backup` broker `--reset-target-data` reset-leg, so no PlanetScale index-build path is left without the fallback. Unarmed it is a clean no-op; the shared `--planetscale-org` reconciles with the telemetry opt-in (a fallback-only arming turns telemetry off with a WARN, never tripping the all-or-nothing refusal).

## Fixed

- **Self-hosted Vitess with `binlog_row_image=NOBLOB` no longer risks silently writing NULL over unchanged BLOB/TEXT columns through the VStream CDC path** (roadmap item 74, ADR-0172). Under NOBLOB (Vitess 16+ with the `AllowNoBlobBinlogRowImage` flag), an UPDATE's after-image omits unchanged BLOB/TEXT columns, which Vitess encodes as a NULL cell that the reader would apply as a genuine NULL — silently overwriting the real value. sluice now checks the `RowChange.DataColumns`/`JsonPartialValues` bitmaps per row-change and refuses loudly with the coded `SLUICE-E-CDC-ROW-IMAGE-PARTIAL` (naming the offending column) rather than mis-carrying — the Bug-193 lesson applied through the Vitess door. No preflight is possible (sluice talks to a vtgate, not the tablets' mysqlds), so the bitmap is the authoritative signal. PlanetScale pins `binlog_row_image=FULL`, so the managed flavor never trips it and is unaffected.

## Changed

- **The migrate cross-table copy pool is confirmed to reach flat-file/mydumper sources** (audit gap #13) — the source-agnostic pool already opens a dedicated reader per concurrent table, so a multi-table `mydumper`/`pscale-dump` directory copies through the same bounded parallelism the database engines use. This is now pinned end-to-end (a multi-table, multi-chunk-file dump → Postgres, byte-identical between parallel and serial); within-table parallelism for these sources stays a deliberate absence (dump chunk files carry no primary-key addressing). No behavior change — a verification-and-documentation closure.

## Compatibility

- **Additive.** MariaDB CDC of `uuid`/`inet` columns now succeeds where it previously refused loudly; the VStream belt only ever refuses a genuinely-partial self-hosted-Vitess row image (never a FULL stream — PlanetScale is unaffected); the index-fallback arming is opt-in and a no-op when unarmed. No change to any existing successful path.

## Who needs this

Anyone running continuous CDC from a MariaDB source that uses native `uuid`/`inet` columns (previously refused, now supported); anyone running self-hosted Vitess with a non-FULL `binlog_row_image` (now guarded against silent BLOB/TEXT loss); and PlanetScale users who reach index builds through fleet `sync run` or the from-backup broker (now covered by the safe-migrations fallback).

## Install

- Binaries: https://github.com/sluicesync/sluice/releases/tag/v0.99.272
- Homebrew: `brew install sluicesync/tap/sluice`
- Scoop: `scoop bucket add sluicesync https://github.com/sluicesync/scoop-bucket; scoop install sluice`
- winget: `winget install sluicesync.sluice`
- Docker: `docker pull ghcr.io/sluicesync/sluice:v0.99.272`
