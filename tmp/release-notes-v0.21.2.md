# sluice v0.21.2

Single-bug patch from the v0.21.0 cycle. v0.21.0 / v0.21.1 left a pre-existing CDC value-decoder bug intact; the v0.21 cycle's expanded UUID coverage surfaced it. Stream-mode CDC against any Postgres source carrying a `UUID`-typed column crashed on the first INSERT/UPDATE. Fix is local, targeted, and pinned by a new integration regression.

## Fixed

- **Bug 41 — PG CDC decode of UUID columns crashes the stream with `UUID byte slice has length 36; want 16`.** Pgoutput's TupleData carries every column value with format byte `'t'` (text) — the `'b'` (binary) branch in the CDC tuple-data switch is already a hard refusal, so for the CDC path UUID values arrive at `decodeUUID` as the 36-byte ASCII canonical hyphenated string. The previous code path required `len([]byte) == 16` (the binary shape pgx returns for non-CDC reads) and bailed loudly on anything else — including the CDC text-format payload. Net effect: the stream exited with the catalog error message on the first INSERT against any UUID-bearing CDC-streamed table. Pre-existing — affects same-engine PG → PG and cross-engine PG → MySQL alike. Bulk-copy `sluice migrate` of UUID-bearing tables was unaffected (it uses `TableReader`, a different code path that already handled the binary shape). Fix: `decodeUUID`'s `[]byte` branch switches on length — 16 routes to the existing binary path, 36 routes through a new `canonicalizeUUIDText` helper that validates the 8-4-4-4-12 hyphenated shape and lowercases to the IR's UUID-as-string contract; any other length surfaces a clear error naming both the length and the supported alternatives so a future protocol surprise (e.g. PG changing wire encoding) is easy to triage. String-passthrough case routes through the same canonicalisation so the IR contract holds whichever shape pgx returns.

## Compatibility

- **No format changes.** Manifest schema, control-table schema, change-chunk format, CLI surface — all unchanged.
- **No CLI breaking changes.** All existing `sluice` subcommands keep their flag surfaces verbatim.
- **Drop-in upgrade from v0.21.0 / v0.21.1.** No DDL migration on `sluice_cdc_state`; no operator action required.
- **Bulk-copy `sluice migrate` paths regression-clean.** Existing UUID-bearing-table migrate behaviour was already correct (binary `[16]byte` → IR-canonical string) and is not touched by this fix. The fix surface is the CDC text path that was broken pre-v0.21.2.

## Who needs this

- **Anyone running `sluice backup stream run` or `sluice sync start` against a Postgres source with a `UUID`-typed column** — Bug 41 affects you. UUID primary keys are extremely common in modern PG schemas (Rails, Django, Hasura, Supabase, and most newer ORMs prefer them over integer surrogates), so any team running CDC against an app that adopted UUID PKs was on the hook before this release. v0.21.0 / v0.21.1 are crashing-on-first-INSERT for these schemas; v0.21.2 streams cleanly.
- **Anyone running cross-engine PG → MySQL CDC restore** — same surface. The Phase 5 cross-engine test cycle that surfaced this was driven by exactly this configuration; v0.21.0's `UUID → CHAR(36)` translation lands the data correctly on bulk-copy, but the CDC apply path crashed on the first incremental row carrying a UUID. v0.21.2 closes this end-to-end.
- **Operators who worked around it with `--exclude-table` or by dropping UUID columns** — you can drop the workaround on v0.21.2 upgrade.

## What's next

Roadmap unchanged from v0.21.1: Phase 6 (KMS encryption) stays unimplemented; the next minor focuses on remaining cross-engine type-translation polish + the tasks listed in `docs/dev/roadmap.md`. v0.21.2 closes the v0.21.0 cycle's only operational regression catalog entry.
