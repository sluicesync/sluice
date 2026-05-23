# ADR-0055: pgoutput streaming-protocol audit — refuse loudly on StreamAbort

## Status

Accepted. Implemented in `internal/engines/postgres/cdc_reader.go` (the new `case *pglogrepl.StreamAbortMessageV2:` arm of `dispatchWAL`). Pinned by `internal/engines/postgres/cdc_reader_streaming_protocol_test.go` (unit) and `internal/engines/postgres/cdc_reader_streaming_protocol_integration_test.go` (integration receiver-side observation).

## Context

[Research finding F1](#references) of the 2026-05-22 PG-internals audit identified a latent silent-loss-class behaviour in the Postgres CDC reader's WAL dispatch path. The issue is a subtle interaction between the pgoutput protocol's two-sided streaming negotiation and one silent `default:` branch in `dispatchWAL`.

### The pgoutput v1 vs v2 protocol distinction

pgoutput negotiates the protocol via the `proto_version` plugin argument on `START_REPLICATION`. Two related but independent capabilities matter:

- **`proto_version` ≥ 2 enables the receiver to parse streaming messages.** The wire format gains `StreamStartMessageV2` / `StreamStopMessageV2` / `StreamCommitMessageV2` / `StreamAbortMessageV2` framing for transactions that exceed `logical_decoding_work_mem` (default 64 MB) at the source.
- **The publisher only EMITS streaming messages when `streaming = 'on'` (PG 14+) or `streaming = 'parallel'` (PG 16+) is ALSO passed as a plugin argument.** Without that flag, even a `proto_version = 2` stream delivers oversized transactions as a single `BeginMessage` / row events / `CommitMessage` triplet, after the source has finished decoding (and after it has spilled to disk; see F2 spill-counter surfacing in v0.74.1).

sluice's current configuration is **`proto_version = 2` WITHOUT `streaming` argument** (`internal/engines/postgres/engine.go:184` and `cdc_reader.go:250-253`):

```go
pluginArgs := []string{
    fmt.Sprintf("proto_version '%d'", r.protoVersion),  // r.protoVersion == 2
    fmt.Sprintf("publication_names '%s'", r.publication),
}
```

This is a deliberate design choice. Disabling streaming trades source-side memory pressure on huge transactions (PG buffers the whole transaction in `logical_decoding_work_mem` plus the on-disk spill in `pg_replslot/<slot>/snap/`) for the simpler "one source tx → one target tx" alignment that [ADR-0027](adr-0027-source-transaction-boundary-cdc-batching.md)'s chunk-as-tx semantics depend on.

### The defensive handlers

`dispatchWAL` already had defensive arms for `StreamStartMessageV2` and `StreamStopMessageV2`. The original justification (per the in-code comment quoting ADR-0027) was that *if* streaming ever fires, treating each chunk as its own boundary is the least-bad mapping under the existing IR.

The hazard is that these defensive arms exist alongside a `default:` arm that **silently skipped `StreamAbortMessageV2`** with the comment:

```go
default:
    // TypeMessage, OriginMessage, LogicalDecodingMessage,
    // StreamCommit/Abort: not in v1 scope. Silent skip.
    return nil
```

### Why silently skipping StreamAbort is silent-loss-class

Consider the protocol semantics under hypothetical streaming activation (operator-driven config drift, or a future sluice change):

1. Source begins a large transaction. PG starts streaming chunks as the in-flight transaction crosses `logical_decoding_work_mem`.
2. For each chunk, sluice receives `StreamStartMessageV2` → emits `ir.TxBegin`. Row events follow. `StreamStopMessageV2` → emits `ir.TxCommit`.
3. Per [ADR-0027](adr-0027-source-transaction-boundary-cdc-batching.md) and [ADR-0017](adr-0017-batched-cdc-apply.md), the batched applier commits each `TxBegin / ... / TxCommit` triplet as its own target transaction. Chunk 1 lands durably on the target. Chunk 2 lands durably. Chunk N lands durably.
4. Source rolls the transaction back. PG emits `StreamAbortMessageV2`.
5. Pre-fix: sluice silently dropped the abort. The N target transactions remain committed. The source has no record of the rolled-back transaction. The target now carries rows the source rolled back — **silent unrecoverable divergence**.

This is the same shape as Bug 15 ([ADR-0020](adr-0020-slot-ack-after-apply.md)) — a window where the target's durable state outruns the source's source-of-truth — but the asymmetry is harder to detect because the divergence isn't a missing-rows gap (which a row-count or checksum diff would catch). The diverged rows are *extra* rows compared to the source post-abort, and there's no upstream signal anyone is looking at.

The class is silent-loss-by-extra-rows. The "zero users is the current reality" tenet in `CLAUDE.md` makes this exactly the kind of latent failure mode the project must close before users arrive — the first silently-corrupted dataset ends the project's credibility permanently.

## Decision

Three pieces:

### 1. Convert `StreamAbortMessageV2` silent-skip to refuse-loudly

Add an explicit `case *pglogrepl.StreamAbortMessageV2:` to `dispatchWAL`'s type switch that returns a structured error wrapping the source's xid + sub-xid plus a self-describing message:

> postgres: cdc: pgoutput StreamAbortMessageV2 received (xid=N sub_xid=M) but sluice does not enable streaming (proto_version=2 without streaming='on'). This message indicates a source-side rolled-back transaction whose pre-abort chunks may have already been committed on the target — data divergence is possible. Either: (a) PG config drift enabled streaming externally; (b) a future sluice change enabled it without wiring StreamAbort rollback. Refusing loudly per the loud-failure tenet. To recover, drop the slot, re-snapshot. See ADR-0055.

The error propagates through `dispatchWAL` → `pump`'s `r.setErr(err)` → the streamer-side error classification → loud CLI failure. The channel closes; the operator sees the message; the slot is held at `confirmed_flush_lsn` (ADR-0020) so re-snapshot has a clean recovery point.

The error is not a sentinel — wrapping the message and letting the standard streamer error path handle it is enough. A future revision that wants to make StreamAbort routine (by also wiring streaming chunk rollback in the IR / applier) will revisit this branch deliberately.

### 2. Leave the other silent skips alone

`TypeMessage`, `OriginMessage`, `LogicalDecodingMessage`, and `StreamCommitMessageV2` continue to fall through `default:` with no error. The audit confirms:

- `TypeMessage` carries OID → type-mapping hints the receiver can choose to consume. sluice already builds its own type maps from `pg_attribute` via the schema reader and doesn't depend on pgoutput's type catalogue — silent skip is correct.
- `OriginMessage` carries origin-routing metadata used by multi-master setups (e.g. BDR). sluice's reader doesn't participate in origin routing — silent skip is correct.
- `LogicalDecodingMessage` is the `pg_logical_emit_message()` mechanism for application-level signalling embedded in WAL. sluice's IR doesn't model this — silent skip is correct.
- `StreamCommitMessageV2` would fire on a streaming end-of-transaction *if* streaming were enabled. The per-chunk `StreamStopMessageV2` handler already emits `TxCommit` for the last chunk, so a separate `StreamCommit` would be redundant signal — silent skip is safe.

The `StreamAbort` case is genuinely different: it carries the only piece of information (rollback) that the receiver MUST act on to maintain correctness, and the receiver cannot reconstruct it from the chunk sequence alone.

### 3. Pin the audit shape

Two tests pin the decision so a future refactor cannot regress the audit's correctness:

- **Unit pin** (`cdc_reader_streaming_protocol_test.go`): construct the wire-format bytes for a `StreamAbortMessageV2` (the leading byte `'A'` followed by xid + sub-xid as big-endian uint32s — the pgoutput wire format) and drive them through `dispatchWAL`. Assert that the returned error mentions the `StreamAbortMessageV2` token, the xid, and the recovery hint.
- **Integration pin** (`cdc_reader_streaming_protocol_integration_test.go`): start a real PG 16 container, run a deliberately oversized transaction (50k rows >> 64 MB `logical_decoding_work_mem`) under the sluice CDC reader, and observe that the receiver sees a SINGLE `TxBegin` / N inserts / `TxCommit` triplet — not chunks. This is the empirical confirmation that sluice's current `START_REPLICATION` plugin args do not enable streaming on a stock PG configuration.

## Why not the alternatives

### "Just enable streaming and handle abort properly"

Tempting because it would let large source transactions stream rather than spill. But:

1. **The IR doesn't model streaming-chunk rollback.** Today's IR is `Insert / Update / Delete / Truncate / TxBegin / TxCommit / SchemaSnapshot`. There's no `TxAbort` variant, and adding one means teaching every applier (PG + MySQL) and the batched-apply flush logic how to roll back already-committed target transactions. That's a multi-engine, multi-ADR change touching ADR-0017 + ADR-0027 + ADR-0010 (idempotency assumes target rows always converge to source rows; rollback violates "convergence" because the rolled-back rows must be deleted, not converged).
2. **Throughput parity.** Even with streaming enabled, the rolled-back transaction's chunks were already wire-shipped to the target and committed there. Rolling them back requires reverse DML on the target — for a 50k-row aborted transaction, that's 50k DELETEs. The "savings" from not spilling on the source are paid for in extra target-side work on every rollback.
3. **F1 is severity-c.** The probability of an active operator enabling `streaming='on'` externally without coordinating with sluice is low; the probability of a future sluice change accidentally enabling it without wiring the IR is also low. The cost of the loud-failure guard is trivial (one switch case) compared to the cost of the IR refactor.

The loud refusal closes the silent-loss class without committing the project to a stream-aware IR.

### "Lower proto_version to 1 to make StreamAbort impossible at the protocol level"

Would remove the `*V2` cases from the parse output entirely. Considered and rejected because:

1. PG-internals research expects the `proto_version` ≥ 2 baseline for future enhancements (replication of generated columns at v3, binary value formats, etc.). Backing off to v1 forecloses those without a clear benefit.
2. The defensive handlers for `StreamStartMessageV2` / `StreamStopMessageV2` are correct under ADR-0027's chunk-as-tx semantics regardless of whether they ever fire. Removing v2 protocol support removes the defensive value too.
3. The loud refusal is the same outcome for the same source-of-truth signal.

## Consequences

- **Source-side rolled-back streaming transactions now refuse loudly.** Operators see a self-describing error pointing at the recovery procedure (drop slot, re-snapshot). No silent target divergence.
- **No source-side change required.** Operators don't need to disable streaming explicitly; sluice's `START_REPLICATION` already does that by not passing the flag. The audit just hardens the receiver against the case where streaming gets enabled by some other path.
- **The receiver-side test confirms the negative.** A 50k-row transaction lands as a single `TxBegin / ... / TxCommit` triplet under the default config — proof that streaming is OFF without depending on pg_replication_slots metadata.
- **Future work.** If a real operator hits this loud-refusal in production, that's a signal that either (1) their PG ops team enabled streaming externally (the audit doc points at this as configuration drift), or (2) sluice needs streaming-aware IR. The second case would warrant a new ADR superseding this one.

## References

- **F1 — pgoutput streaming-protocol audit:** finding 1 from the 2026-05-22 PG-internals research run (chapters covering logical-replication protocol mechanics). Severity-c; latent silent-loss-class if streaming activated.
- **ADR-0027** — source-transaction-boundary CDC batching (the chunk-as-tx semantics this audit interacts with).
- **ADR-0028** — memory-bounded streaming (the orthogonal "receiver-side memory cap" concern; not source-side `logical_decoding_work_mem`).
- **ADR-0007** — position persistence guarantee (the durability invariant that streaming-chunk-as-tx would compromise on abort).
- **ADR-0020** — slot-ack-after-apply (the related Bug-15-shape gap closure on the apply path; same family of silent-loss).
- **ADR-0010** — idempotent applier (convergence assumption that streaming abort would violate).
- PostgreSQL documentation: [logical streaming replication protocol](https://www.postgresql.org/docs/current/protocol-logical-replication.html) — wire format for `Stream Start` (`'S'`), `Stream Stop` (`'E'`), `Stream Commit` (`'c'`), `Stream Abort` (`'A'`).
