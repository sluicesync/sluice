# sluice v0.99.257

The 2026-07-15 repo audit's safety-net release: fixes for both OBSERVED CRITICAL silent-loss defects the audit found in the newest surfaces, the HIGH silent UTC shift in flat-file type inference, and two gate-integrity fixes so the checks that should have caught them can't quietly not run.

## Fixed

- **CRITICAL (mydumper): silent statement drop on torn or re-encoded dumps.** The chunk reader skipped any statement whose first token didn't lex to a SQL keyword — a UTF-8 BOM at chunk start (a file re-saved by a Windows editor) silently dropped the whole first INSERT, and a severed INSERT fragment in a torn dump vanished with exit 0. Worse, `verify --depth count` rode the same reader, so verification confirmed the loss. A leading BOM is now stripped losslessly with a WARN; any other keyword-less non-comment fragment refuses loudly naming the file and the offending bytes, on both the migrate and verify paths; schema files get the same treatment.
- **CRITICAL (resume cursors): lossless typed envelope for the resume-PK store.** Binary PK cursors were mangled by the JSON control-table round-trip (invalid-UTF-8 bytes → U+FFFD; the migrate path additionally re-bound base64 text) and integer cursors above 2⁵³ drifted through float64 — a resumed `sluice backfill` or interrupted-`migrate` resume could silently skip, or replay past the documented one-chunk bound, arbitrary PK ranges. BINARY(16) UUID keys and snowflake-magnitude ids are exactly the mainstream shapes affected. Cursors now persist in a lossless tagged envelope; legacy plain cursors keep resuming exactly, and provably-mangled legacy cursors refuse loudly with the new coded `SLUICE-E-BACKFILL-CORRUPT-CURSOR` (re-run with `--restart`) on backfill, or self-heal via truncate-and-redo on migrate. Resume fidelity is now integration-pinned per orderable PK family on real MySQL and Postgres.
- **`--infer-types` no longer silently UTC-shifts zoned timestamps carrying stray whitespace.** A CSV column of `2024-01-15T10:30:00+05:00␠` values (trailing space) classified as naive while the decoder parsed the offset — every wall clock silently shifted 5 hours, exit 0. Zone classification now runs over the whitespace-trimmed value, trimming exactly the character set the decoder trims (test-pinned equal), so padded zoned values resolve `timestamptz` with instants intact.

## CI / developer gate

- The Windows pre-commit mirror no longer soft-skips the CI coverage guards when `sh` isn't on PATH (it resolves Git for Windows' bundled shell and hard-fails with instructions otherwise).
- The psverify live-verification workflow now receives the full `PLANETSCALE_*` env surface its suites read and fails the job if any test skips — a live run that skipped anything did not verify what it claims.

## Compatibility

- **No breaking changes.** The cursor store change is one-way forward-compatible: new binaries read all legacy rows (including bare unsigned-bigint cursors above MaxInt64); an old binary reading new envelope rows fails loudly at bind, never silently wrong. One new error code; the mydumper refusals convert silent losses into loud errors on inputs that were already losing data.

## Who needs this

**Anyone using the flat-file import surfaces (v0.99.247+) or resume on `backfill`/`migrate` (any version) should upgrade before their next run.** The mydumper fix matters for any dump that passed through an editor, transfer tool, or partial copy; the cursor fix matters for any interrupted run on binary or large-integer primary keys; the infer-types fix matters for CSV/TSV/NDJSON columns with padded timestamps. All three defects exited 0.

## Install

- Binaries: https://github.com/sluicesync/sluice/releases/tag/v0.99.257
- Homebrew: `brew install sluicesync/tap/sluice`
- Scoop: `scoop bucket add sluicesync https://github.com/sluicesync/scoop-bucket; scoop install sluice`
- winget: `winget install sluicesync.sluice`
- Docker: `docker pull ghcr.io/sluicesync/sluice:v0.99.257`
