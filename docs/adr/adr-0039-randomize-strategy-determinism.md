# ADR-0039: randomize:* strategy determinism via per-row seed

## Status

Accepted. Implemented in v0.59.0 (PII Phase 2.c first wave; GitHub issue #24).

## Context

PII Phase 2.c (the third tranche of the strategy catalog in `docs/dev/notes/prep-pii-redaction-phase-2-strategy-catalog.md`) introduces four randomizing redaction strategies:

- `randomize:int:<min>,<max>` — integer in [min, max] inclusive
- `randomize:email` — random `<local>@<domain>.test` shape
- `randomize:us-phone` — NANP-valid `XXX-XXX-XXXX`
- `randomize:uuid` — random UUIDv4

The headline reference shape is MySQL Enterprise's data-masking-component (`gen_rnd_pan`, `gen_rnd_us_phone`, `gen_rnd_canada_sin`, etc.), which is pure-random per-call: every invocation against the same input draws an unrelated new value.

Sluice deviates: **each randomize:* strategy is deterministic per (table, column, primary-key values)**. The same source row always produces the same target value, across runs and across machines.

## Why deviate

Sluice's operator-facing contract is dominated by **continuous-sync semantics**, not one-shot anonymization. Pure-random per-call breaks every continuous-sync user story:

1. **CDC resume after crash.** The streamer warm-resumes from a persisted position. With pure-random redaction, a row replayed during overlap (between checkpoint and crash) would be redacted to a new random value, overwriting the prior value on the target. Operators would see fields silently change on every restart even when source data hasn't moved.

2. **Backup → restore round-trip.** `sluice backup full` writes redacted rows to disk; `sluice restore` reads them back. With pure-random redaction, the act of restoring would generate yet another set of random values (because the restorer would have to re-redact in the absence of stable seeding) — defeating the purpose of restoring a redacted backup as a faithful reproduction of the redacted source.

3. **Idempotent target writes.** PG's `ON CONFLICT (pk) DO UPDATE` and MySQL's `INSERT … ON DUPLICATE KEY UPDATE` are sluice's idempotency primitives. The applier relies on re-applying the same change producing the same row. Pure-random redaction breaks this — the same source row maps to different target rows on every retry.

4. **Diff / verification.** `sluice verify` and `sluice diff` compare source to target row-by-row. Pure-random redaction makes any source row look like a target divergence on every comparison; operators would have no way to distinguish "your data changed" from "your randomizer drew a new value."

The cost of deviating is small: replay-stable randomization is what MySQL Enterprise users actually expected (and the `gen_rnd_*` pure-random shape is mostly an artefact of the function-per-row execution model, not a deliberate design choice). The benefit is large: every continuous-sync user story keeps its idempotency contract.

## Decision

Each randomize:* strategy's `Redact` method requires a per-row seed and refuses if one isn't supplied. The seed is derived deterministically:

```
seed = SHA256(streamID || "|" || table || "|" || column || "|"
              || pkCol1 || "=" || canonical(pkVal1) || "|"
              || pkCol2 || "=" || canonical(pkVal2) || ...)
```

- `streamID` — the active stream's identifier (or `""` for migrate / chain-handoff paths)
- `table`, `column` — the IR-side names
- `pkCol_i`, `pkVal_i` — the table's primary-key column names + the row's PK values in declaration order
- `canonical(v)` is `fmt.Sprintf("%v", v)` — operator's data is what it is; we just need stable input → stable hash

The seed is 32 bytes (SHA-256 output), used to construct a `math/rand/v2.ChaCha8` source. ChaCha8 is fast and produces a stable byte sequence for a given seed across Go versions; cryptographic strength isn't needed because the output is a placeholder, not a secret.

### Refusal contract

Tables without a primary key cannot produce stable seeds (there's no row-identity input). A randomize:* rule registered against a no-PK table is refused at startup by `pipeline.preflightRedactTypes`. Operators see an error naming the rule + a hint to either add a PK on the source or pick a non-random strategy (`hash:sha256`, `mask:*`, `static:`).

Tables with a primary key but where the rule is configured before the PK can be loaded (e.g., direct API users skipping preflight) get a defense-in-depth refusal at the strategy's `Redact` site, surfacing the same operator-actionable error.

### Migrate vs sync semantics

- **Migrate**: `streamID = ""`. The seed is fully determined by (table, column, PK). Re-running migrate against the same source produces the same target values. Cross-migrate determinism is preserved.
- **Sync**: `streamID = <operator-supplied or auto-generated>`. The seed depends on streamID, so two streams with different IDs (e.g., a staging stream vs a production stream sharing a source) produce different randomized values for the same row. Within one stream, replay stability is exact.

This is intentional: cross-stream determinism is *not* a default contract because operators routinely run staging streams against production sources for testing, and we don't want test data leaking the same randomized values as production data. Operators wanting cross-stream determinism set the same `--stream-id` across runs.

## Alternatives considered

- **Pure-random per call (MySQL Enterprise's shape)**: rejected for the four reasons above.
- **Operator-supplied seed material**: an additional `--randomize-key-source` flag mirroring `--redact-key-source`. Rejected for now: the per-row seed derivation already includes stream-id + table + column, which is enough operator-controlled material to provide cross-stream separation (operators wanting cross-stream determinism just use the same stream-id). Adding another flag would multiply the configuration surface without a clear win.
- **Seed the column-wide RNG once at startup**: i.e., the same RNG drives every row in a column. Rejected — that's just a way to make the strategy stateful, which breaks goroutine-safe iteration in the bulk-copy reader's parallel-chunk path.
- **Crypto-strength source (e.g., HMAC-SHA256 stream cipher)**: rejected as overkill. The output is not a secret; the goal is "looks random to a human" + "stable across runs," both of which ChaCha8 satisfies at lower cost.

## Consequences

- **Replay-safe**: CDC resume + cold-start re-apply + backup→restore all produce identical target values for a given source row. Idempotency is preserved.
- **PK requirement is explicit**: operators using randomize:* see a startup-time refusal on no-PK tables rather than a silent surprise at row-process time.
- **Streamer wiring**: the `ChangeApplier`'s redactor call sites need streamID + per-table PK column lists. The applier caches PK columns per table (one info_schema round-trip on first sight, reused thereafter); the streamer plumbs streamID via `ir.StreamIDSetter` (mirrors `RedactorSetter`'s shape).
- **Future randomize:* strategies inherit the seed contract automatically**: any new strategy whose `Name()` starts with `randomize:` is detected by the pipeline + ApplyRow seed-derivation gate. No interface marker required.

## Reference

- `docs/dev/notes/prep-pii-redaction-phase-2-strategy-catalog.md` — Phase 2 catalog
- `internal/redact/strategies_randomize.go` — generator implementations
- `internal/redact/redact.go::DeriveRowSeed` — seed derivation helper
- `internal/pipeline/redact_preflight.go::preflightRedactTypes` — randomize-on-no-PK refusal
