# sluice v0.74.2 — F1 protocol audit + F3 invariant pin

**Headline:** Closes the two remaining PG-internals research findings (F1 + F3, both severity-c). F1 converts a latent silent-corruption-class behavior in the pgoutput receiver into a refuse-loud + new ADR-0055 documenting the protocol audit; F3 lands a durable regression-catcher for the ADR-0007 / ADR-0020 confirmed_flush_lsn invariant. Behavior-additive on top of v0.74.1; drop-in upgrade.

## What this closes

Both findings come from the 2026-05-22 PG-internals research (`sluice-pg-internals-research-2026-05-22.md`):

- **F1 (severity c)** — pgoutput protocol-version audit. sluice's `START_REPLICATION` passes `proto_version=2` but never sets `streaming='on'`, so PG should never emit streaming-chunk messages. The receiver's previous `default:` switch arm silently skipped `StreamAbortMessageV2` alongside benign skips for type/origin/logical-decoding messages. The StreamAbort silent-skip is **latent silent-corruption-class** if streaming is ever enabled (PG config drift or future sluice change): per ADR-0027 each chunk is committed as its own target transaction; silently swallowing the abort leaves the target carrying rows the source rolled back. v0.74.2 converts the silent-skip to refuse-loudly with a self-describing error (xid + sub-xid + recovery hint) so the latent failure mode can't materialize undetected.

- **F3 (severity c)** — confirmed_flush_lsn invariant pin. ADR-0007's "position + data lands durably together" guarantee + ADR-0020's slot-ack-after-apply machinery together enforce that `pg_replication_slots.confirmed_flush_lsn <= max(target's persisted source_position LSN)` at all times. Pre-finding this invariant was implicit; a future refactor could regress it without any pin catching the regression. v0.74.2 adds an integration test that drives 25 transactions through a real CDC stream and continuously samples both LSNs every 100 ms. Local + CI runs show the invariant holding cleanly — F3 is pin-only, no production code change.

## Added

- **`feat(engines/postgres): F1 — refuse loudly on pgoutput StreamAbortMessageV2`** *(severity c)*. New explicit `case *pglogrepl.StreamAbortMessageV2:` arm in `dispatchWAL`'s message-type switch returns an error naming the message type, the abort's xid + sub-xid (for operator correlation against source-side logs), the recovery path (drop the slot + re-snapshot — same shape as ADR-0022's slot-missing fall-through), and a reference to ADR-0055. The other previously-silent skips (TypeMessage, OriginMessage, LogicalDecodingMessage, StreamCommitMessageV2) stay silent — they're benign in the current config.

## Docs

- **`docs(adr-0055): pgoutput streaming-protocol audit`** *(new ADR for F1)*. Documents the pgoutput v1 vs v2 protocol distinction (parsing capability via `proto_version >= 2` vs emitting capability via `streaming='on'`), sluice's current `proto_version=2`-without-streaming config, why the defensive StreamStart/StreamStop handlers exist (against config drift), and the F1 decision to refuse loudly on StreamAbort. Cross-references ADR-0027 (chunk-as-tx batching), ADR-0028 (memory-bounded streaming), ADR-0007 (position-durability invariant), ADR-0020 (slot-ack-after-apply, related family of silent-loss closures), and ADR-0010 (idempotent applier convergence assumption).

## Tests

- **`test(engines/postgres): cdc_reader_streaming_protocol_test.go`** — F1 unit pin. Constructs synthetic StreamAbortMessageV2 wire-format bytes (`'A'` + xid + sub-xid big-endian uint32s), drives them through `dispatchWAL`, and asserts the returned error names the message type, includes the xid + sub-xid for operator correlation, carries the recovery hint, references ADR-0055, and emits no `ir.Change` before refusing. A second pin asserts the error does NOT wrap `ir.ErrPositionInvalid` (which would incorrectly route through the ADR-0022 cold-start fall-through instead of forcing the operator to drop + re-snapshot).

- **`test(engines/postgres): cdc_reader_streaming_protocol_integration_test.go`** — F1 integration pin (receiver-side empirical confirmation). Boots PG with `logical_decoding_work_mem=64kB`, runs a single ~1000-row INSERT transaction that comfortably exceeds the cap, drains the changes channel, and asserts EXACTLY ONE `TxBegin` / 1000 `Insert` / EXACTLY ONE `TxCommit` triplet arrives — proving streaming chunks are not being emitted under sluice's default plugin args even when the source spills to disk. A streaming-enabled stream would produce ≥2 `TxBegin` / `TxCommit` pairs (one per chunk).

- **`test(engines/postgres): confirmed_flush_invariant_integration_test.go`** — F3 pin. Asserts the load-bearing ADR-0007 / ADR-0020 invariant continuously during a real CDC stream. Wires CDCReader + ChangeApplier with the LSN tracker, drives 25 distinct insert transactions, and a polling goroutine samples both LSNs every 100 ms — any violation is captured at the time it happens. Also asserts the slot's `confirmed_flush_lsn` advanced strictly above 0 (rules out the trivially-passing case where both sides stayed at zero) and re-checks the invariant post-stop. Teardown wired with a separate `applyCtx` so `ApplyBatch` returns on context-cancel rather than waiting for the async pump-shutdown channel close (same async-Close pattern that bit the F5 tests; symmetric fix on the apply side).

## Compatibility

- **Drop-in upgrade from v0.74.1.** No CLI surface change. No storage shape change. No behavior change outside the F1 refuse-loud (which cannot fire in any current sluice configuration — sluice doesn't enable streaming).
- **The F1 refusal triggers ONLY** if pgoutput emits `StreamAbortMessageV2`, which requires `streaming='on'` to be passed to `START_REPLICATION`. Sluice doesn't do this. The refusal is defensive coverage for hypothetical future config drift or sluice changes.
- **MySQL paths** unchanged — F1/F3 are PG-only.

## Cross-references

- [ADR-0055 — pgoutput streaming-protocol audit](https://github.com/sluicesync/sluice/blob/main/docs/adr/adr-0055-pgoutput-streaming-protocol-audit.md) *(new)*
- [ADR-0027 — Source-transaction-boundary CDC batching](https://github.com/sluicesync/sluice/blob/main/docs/adr/adr-0027-source-transaction-boundary-cdc-batching.md)
- [ADR-0007 — Position persistence](https://github.com/sluicesync/sluice/blob/main/docs/adr/adr-0007-position-persistence.md)
- [ADR-0020 — Slot ack after apply](https://github.com/sluicesync/sluice/blob/main/docs/adr/adr-0020-slot-ack-after-apply.md)
- Durable research artifact: `sluice-pg-internals-research-2026-05-22.md` (Ch 12)
