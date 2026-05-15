# ADR-0040: dictionary-strategy determinism — PK-keyed vs input-value-keyed

## Status

Accepted. Implemented in v0.61.0 (PII Phase 3; GitHub issue #24).

## Context

PII Phase 3 (the dictionary section of `docs/dev/notes/prep-pii-redaction-phase-2-strategy-catalog.md`) introduces two dictionary-based redaction strategies:

- `randomize:dict:<name>` — selects a dictionary entry per row.
- `tokenize:dict:<name>` — selects a dictionary entry per input value.

Both strategies look superficially identical (input column → dictionary entry), but their determinism contracts must differ to support real-world operator workflows. ADR-0039 documented the per-row replay-stable contract for `randomize:*`; this ADR documents how `tokenize:dict` deliberately deviates.

## The two contracts

### `randomize:dict` — PK-keyed (inherits ADR-0039)

Same source row (identified by stream-id + table + column + primary-key values) always selects the same dictionary entry. Two rows with different PKs but identical column values will (usually) map to different dictionary entries.

Strategy `Name()` starts with `randomize:`, so the v0.59.0 no-PK preflight refuses `randomize:dict` rules against tables without a primary key. Replay-stability across CDC resume / cold-start re-apply / backup→restore depends on the seed being derivable from row identity; no PK → no row identity → no replay-stability.

Selection algorithm: `dict[seedToIndex(seed, len(dict))]` where `seedToIndex` reads the first 8 bytes of the SHA-256 row seed as little-endian uint64 and reduces modulo `len(dict)`.

### `tokenize:dict` — input-value-keyed (new contract)

Same input value always selects the same dictionary entry, **regardless of which row it came from, regardless of which table or column carries it, regardless of whether the table has a PK at all**. Every occurrence of "Alice" anywhere in the database maps to the same dictionary entry; every occurrence of "Bob" maps to (likely) a different entry but consistently the same one across the database.

Strategy `Name()` starts with `tokenize:`, **not** `randomize:`, so the no-PK preflight does NOT refuse it. This is intentional: the whole point of `tokenize:dict` is that it works on tables without a PK — its output is keyed by the value, not by the row.

Selection algorithm: `dict[seedToIndex(HMAC_SHA256(key, streamID || ":" || dictName || ":" || input), len(dict))]` where:

- `key` is a constant byte string (`"sluice-tokenize-dict-v1"`). Security model is "stable hashing", not secrecy — operators wanting a separate keyset story will get one in PII Phase 4.
- `streamID` is the active stream identifier (`""` for migrate / no-stream contexts; still produces a deterministic mapping within that empty-streamID space).
- `dictName` is the operator-supplied dictionary name. Mixed in so two dicts with overlapping content still produce different tokenizations.
- `input` is the source value (canonicalized via `fmt.Sprintf("%v", val)` for non-string inputs; `[]byte` is treated as its string form).
- NULL input passes through as NULL (no tokenization).

## Why the two contracts differ

The differing contracts trace to the differing operator workflows each strategy is built for.

`randomize:dict` is the operator's request "give me a synthetic-but-stable surrogate for this column; I don't care that two rows with the same value map to different entries — I just want each row's entry to be predictable across replays". Like `randomize:email` and `randomize:us-phone`, this is fundamentally about replacing one row's value with a synthetic one for that row.

`tokenize:dict` is the operator's request "I have a name like 'Alice' that appears in 12 tables; I want it tokenized to the SAME surrogate in every place so referential analytics still work". Pure-random per-row would scramble the relationships. Per-PK would still scramble cross-table relationships (the same name in different rows would get different tokenizations). The only workable contract for "cross-database stable surrogate" is **keyed by the input value itself**.

This is the FIRST sluice strategy whose output depends on the input value rather than the row's PK. The `Strategy.Redact(col, val, seed)` interface already gave us this — `val` was always available — but no prior strategy used it for output derivation. `tokenize:dict` is the first.

## Why HMAC and not plain SHA-256

A direct `SHA256(streamID || ":" || dictName || ":" || input) mod len(dict)` would be functionally equivalent for this use case (the goal isn't secrecy, it's stable mapping). HMAC with a fixed key is chosen because:

1. **Future-compatible.** When PII Phase 4 ships an operator-keyset story, switching the HMAC's key from the fixed constant to an operator-keyed value is a one-line change; the determinism contract stays the same. With plain SHA-256, that switch would silently change every tokenization output (because the input bytes would change).
2. **Idiomatic.** HMAC is the Go-stdlib's "keyed hash with stability across future-mixing" primitive. Using it signals intent more clearly than `sha256.New()` + manual byte concatenation.

The fixed key (`"sluice-tokenize-dict-v1"`) is intentionally unusual-looking so a future audit grep finds it instantly. The `-v1` suffix leaves room for a key-rotation story that preserves Tokenize output across operator key changes — Phase 4's concern, not Phase 3's.

## Migrate vs sync semantics

Same as ADR-0039:

- **Migrate (no stream-id):** `streamID = ""`. The HMAC still computes deterministically. Re-running migrate against the same source produces identical tokenizations.
- **Sync:** `streamID` is the active stream's identifier. Two streams with different IDs produce different tokenizations for the same input value. Operators wanting cross-stream determinism declare the same `--stream-id`.

The streamID prefix is a small but real safety: a tenant-isolated multi-stream operator running staging + production streams against the same source doesn't want trivial cross-stream value-inversion (knowing a row's value in staging would imply its tokenization in production).

## Empty-dictionary refusal

Both strategies refuse at `Redact` time if `len(Entries) == 0`. The loader (`redact.LoadDictionaries`) refuses empty dictionaries at config-load time too, so this is defense-in-depth — a direct API user constructing a strategy by hand wouldn't have gone through the loader and could otherwise hit a `mod-by-zero` panic.

## Alternatives considered

- **Single strategy with a flag.** `randomize:dict` with a `--cross-row-stable` flag that switches between per-row and per-value semantics. Rejected: the two contracts are semantically distinct enough that a flag would obscure rather than clarify. Different `Name()` outputs (`randomize:dict:foo` vs `tokenize:dict:foo`) also lets the no-PK preflight gate just one of them naturally.
- **Operator-supplied key for tokenize.** A `--tokenize-key-source` flag mirroring `--redact-key-source`. Deferred to Phase 4: Phase 3's fixed key suffices for the "stable per-value surrogate" use case, and Phase 4's keyset story is the right place for operator-controlled secret material.
- **Different reduction (e.g. consistent hashing) instead of modulo.** Rejected as overkill: dictionary sizes are bounded by operator declaration (10s-thousands of entries); the modulo bias is negligible at those sizes. A future Phase 4 dictionary-versioning story (operator adds an entry mid-stream) would need a different reduction, but that's a Phase 4 problem.
- **Compute the HMAC once per (dict, streamID) and cache.** The HMAC is recomputed per row, which is ~microseconds per row at most — entirely negligible vs sluice's bulk-copy bandwidth. Caching adds complexity without measurable win.

## Consequences

- **Cross-table stable tokenization works.** Operators get the "every Alice maps to the same surrogate everywhere" property that drives the strategy's design.
- **No-PK tables are supported by tokenize:dict but not randomize:dict.** The preflight's prefix match on `randomize:` does the right thing automatically; no special-case code.
- **First sluice strategy with input-value-keyed output.** The `Strategy.Redact(col, val, seed)` interface tolerates this without changes — `val` was always available. Future strategies wanting per-value determinism follow the same pattern.
- **Empty dictionaries refused at load time.** Loud-fail-loudly trumps mysterious-mod-by-zero.
- **PII Phase 4 will need to revisit the constant HMAC key.** When operator-keyset persistence lands, the key becomes operator-controllable; the determinism contract stays the same but cross-version compatibility needs explicit migration notes. The `v1` suffix on the current constant is the placeholder for that future.

## Reference

- `docs/dev/notes/prep-pii-redaction-phase-2-strategy-catalog.md` — Phase 3 section
- `internal/redact/strategies_dict.go` — strategy implementations
- `internal/redact/dictionary.go` — `LoadDictionaries` helper (YAML inline + file form)
- ADR-0039 — per-row seed contract for `randomize:*`
