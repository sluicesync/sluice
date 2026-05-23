# ADR-0051: Postgres CDC source-identity pinning

## Status

Accepted. Implemented in:

- `internal/engines/postgres/cdc_position.go` — `pgPos` extended with optional `SystemID` (string) and `Timeline` (int32) JSON fields; older tokens without them decode cleanly.
- `internal/engines/postgres/cdc_reader.go::resolveStartPosition` — issues `IDENTIFY_SYSTEM` on every `StreamChanges` call (both cold-start and resume paths); captures `SystemID` / `Timeline` onto the reader and compares against the persisted pin on resume via `checkSourceIdentity`.
- `internal/engines/postgres/cdc_reader.go::checkSourceIdentity` — pure comparator; returns nil on match or pre-ADR-0051 sentinel (lazy install + INFO log); returns `fmt.Errorf("...: %w", ir.ErrPositionInvalid)` on divergence.
- `internal/engines/postgres/cdc_reader.go::positionAt` — every emitted change's `ir.Position` now carries the reader's pinned `(SystemID, Timeline)` so subsequent reconnects have the pin to compare against.

Closes severity-A finding F5 from `2026-05-22 PG-internals research chapters 9–10–11` (durable findings doc).

## Context

Postgres's logical-replication LSN reference frame is **timeline-scoped**. The (sysid, timeline) tuple identifies a specific cluster on a specific timeline; LSN values from one timeline are not comparable to LSN values from a different timeline. After a source-side PITR, a standby promotion, a cluster restore-from-backup, or an operator pointing sluice at a different instance with the same DSN host:port shape, the new source's IDENTIFY_SYSTEM reply changes — and any LSN sluice had persisted from the old source lives in a different reference frame than the new source's WAL.

References from the PG-internals book:

- **Ch 10.3 (Streaming Replication)** — sysid + timeline form the cluster-identity tuple every replica negotiates against.
- **Ch 10.4 (Switchover and Failover)** — promotion increments the timeline; PITR can produce a new timeline within the same cluster (same sysid, new timeline) or a fresh cluster from base backup (new sysid).
- **Ch 11.1 (Logical Replication)** — `IDENTIFY_SYSTEM` is the replication-protocol command that returns `(systemid, timeline, xlogpos, dbname)` to the replica before `START_REPLICATION`. Pre-ADR-0051, sluice called `IDENTIFY_SYSTEM` only on the cold-start path (purely to read `XLogPos`) and discarded `systemid` / `timeline`.

The pre-ADR-0051 surface was a silent-loss class: on a post-PITR / post-promotion reconnect, sluice's `resolveStartPosition` saw the persisted `(slot, lsn)` token and started `START_REPLICATION` from that LSN. The new source happily streamed WAL from that LSN — but in the new timeline's reference frame. Events that landed at "the same LSN" on the old timeline were silently skipped or replayed depending on the timeline geometry, with no operator-visible signal. Per the project's loud-failure tenet ("validate end-to-end before building more" / "the first real migration that silently corrupts data ends the project's credibility permanently"), this is exactly the class of bug the codebase exists to refuse.

A subset of the surface is recoverable today by accident: if the new timeline didn't have sluice's replication slot, the pre-existing slot-missing branch of `resolveStartPosition` would trip and fall through to cold-start via ADR-0022's `ir.ErrPositionInvalid` path. But a same-cluster PITR or a promotion that preserved the slot definitions in pg_replication_slots would slip past entirely — silent-loss territory.

## Decision

Pin the source's identity. Three layered design choices:

1. **Capture on every StreamChanges, both cold-start and resume.** Pre-ADR-0051, `IDENTIFY_SYSTEM` ran only on the cold-start path purely to read `XLogPos`. ADR-0051 hoists the call to the top of `resolveStartPosition` so it runs unconditionally; the reply's `SystemID` and `Timeline` are stashed on the reader (`r.systemID`, `r.timeline`). The check fires BEFORE the slot existence query so a diverged source surfaces "source identity has changed" rather than the misleading "replication slot missing" error (the slot may genuinely exist on the new source — its name is the same; its meaning is not).

2. **Persist the pin on the position token, additively.** `pgPos` gains `SystemID` and `Timeline` as JSON fields with `omitempty`. Positions persisted by pre-ADR-0051 sluice decode cleanly with both fields zero-valued. On the first reconnect with a legacy token, `checkSourceIdentity` engages **lazy install**: emit a one-time INFO log noting the pin is being installed, accept the legacy position, and let subsequent reconnects engage the strict comparison (the next emitted change carries the now-installed pin). This preserves drop-in upgrade from prior versions.

3. **Refuse loudly on divergence, wrap `ir.ErrPositionInvalid`.** When persisted `(SystemID, Timeline)` disagree with the live IDENTIFY_SYSTEM reply, return an error that:
   - Names both the OLD `(systemid, timeline)` (from the persisted token) and the NEW `(systemid, timeline)` (from the live reply), so the operator can confirm whether the divergence matches their intended PITR / promotion event.
   - Spells out the three plausible operator-side causes: source-side PITR, standby promotion, or pointing sluice at the wrong instance.
   - Mirrors ADR-0022's slot-missing recovery-hint shape — name the slot, point at `sluice slot drop`, point at the cold-start fall-through ("restart with empty position (forces a fresh snapshot)").
   - **Wraps `ir.ErrPositionInvalid`** via `%w`. This routes the divergence through the existing ADR-0022 streamer fall-through — the same code path that handles slot-missing on the warm-resume side. The operator-experience semantics are identical: "the persisted position is no longer valid; cold-start is the only recovery path."

There is no `--ignore-source-identity-change` flag. The persisted LSN is *by definition* meaningless against a source whose identity has changed; "stay strict" is the only semantic — a flag would only let the operator opt into the silent-loss class the ADR closes. The ADR-0022 logic ("loud WARN + cold-start fall-through" — non-destructive on its own, ADR-0009's `--reset-target-data` still gates destructive dest-data operations) is the right shape for this class too.

## Consequences

- **Closes a silent-loss class.** A post-PITR / post-promotion reconnect now refuses loudly with operator-actionable diagnostics, instead of silently streaming WAL from a different timeline's LSN reference frame. The refusal is the same shape as ADR-0022's slot-missing one; operators familiar with the slot-drop recovery flow see the same recovery shape here.

- **Position token grows by ~30 bytes on average.** A typical IDENTIFY_SYSTEM reply has a 19-digit `SystemID` (uint64 as decimal) and a small `Timeline` (single or double digits). The `omitempty` tags mean the wire size grows only for positions emitted post-ADR-0051; pre-existing persisted positions stay byte-identical.

- **Pre-ADR-0051 persisted positions accepted on first reconnect.** Lazy install — accept once, log once at INFO, write the pin onto subsequent emitted positions. Operators upgrading sluice across the ADR-0051 boundary see no disruption; the only operator-visible change is one INFO line in the log on the first post-upgrade reconnect per stream.

- **`IDENTIFY_SYSTEM` now runs on every StreamChanges call.** It already ran on cold-start; the change is that it also runs on warm-resume. The command is a single round-trip and is cheap relative to slot-state validation (`slotInfo` queries `pg_replication_slots` via the *sql.DB pool, which is also a round-trip). No measurable cost.

- **Other engines: out of scope.** This is a PG-specific finding (logical replication's protocol command is what gives sluice the identity tuple). MySQL's `verifyPositionResumable` already covers the equivalent class via `GTID_SUBSET` (a GTID set from a different server-uuid would simply not be a subset of the new source's executed GTIDs). VStream / future engines should be evaluated when their CDC readers land — the engine-neutral sentinel (`ir.ErrPositionInvalid`) already exists; each engine's reader is responsible for surfacing the engine-specific divergence shape.

- **Reviewer corollary (CLAUDE.md "pin the class, not the representative").** The test matrix exercises all three branches of `checkSourceIdentity`: exact-match (silent pass), lazy-install sentinel (silent pass + INFO log), and divergence (refuse + ErrPositionInvalid wrap). The integration test exercises the end-to-end shape against a real PG container via a tampered persisted token — a stand-in for a real PITR / promotion event, since wiring `pg_promote` into testcontainers is heavier without exercising additional surface (the comparator is the load-bearing logic).

## Why a tampered-token integration test rather than a real `pg_basebackup` + promote

A live promotion or `pg_basebackup`-cloned-cluster test would be the gold-standard end-to-end exercise. It was considered and rejected for the v1 of this ADR's verification:

- The load-bearing logic is the **comparison** in `checkSourceIdentity`. Whether `persisted != live` came from a real PITR or from a hand-doctored persisted token, the divergence-detection code path is byte-identical — the comparator doesn't know (or care) where the divergence came from.
- Spinning a second testcontainer, configuring `primary_conninfo` / replication slots between them, and triggering promotion adds significant test wall-time and flake surface without exercising any additional code in the reader.
- The integration test does exercise the **full** PG CDC reader stack against a real PG: cold-start, position capture, position decode, resume against a tampered identity, resume against the un-tampered identity. The only thing it doesn't do is generate the divergence via a real promotion event. If a future PG patch ever broke `IDENTIFY_SYSTEM`'s `SystemID` / `Timeline` parsing on the pglogrepl side, the happy-path resume test would catch it (the captured pin would no longer round-trip via `decodePGPos`).

Operators or future-us who want a real-promotion test can layer it on top — the comparator and the position-token format are stable.

## Verification

Unit tests in `internal/engines/postgres/cdc_reader_test.go`:

- `TestEncodeDecodePGPos` extended with two cases pinning the `SystemID` / `Timeline` round-trip.
- `TestDecodePGPosPreADR0051CompatibleToken` — pre-ADR-0051 JSON shape (no systemid / timeline keys) decodes cleanly with zero-value pin fields.
- `TestEncodePGPosOmitsZeroIdentityFields` — wire-format invariant: zero-value pin fields are NOT emitted to JSON (omitempty).
- `TestCheckSourceIdentity` — every branch of the comparator: exact match (silent pass), pre-ADR-0051 sentinel (lazy install — silent pass), timeline diverges (refuse), sysid diverges (refuse), both diverge (refuse). Each refusal case asserts `errors.Is(err, ir.ErrPositionInvalid)` so the ADR-0022 fall-through engages, AND asserts both old and new (sysid, timeline) pairs appear in the message AND asserts the recovery hint names the slot and `sluice slot drop`.

Integration tests in `internal/engines/postgres/cdc_reader_source_identity_integration_test.go` (build tag `integration`):

- `TestCDCReader_SourceIdentityPin_HappyPathResume` — positive control. Cold-start captures a position with the live pin; resume with the SAME position succeeds and the next emitted position still carries the pin.
- `TestCDCReader_SourceIdentityPin_DivergenceRefusesLoud` — tamper the captured position's `SystemID`; resume must fail with `ir.ErrPositionInvalid`-wrapped error naming both old and new sysids; the un-tampered position must still succeed (regression guard against over-strict refusal).
- `TestCDCReader_SourceIdentityPin_LegacyPositionLazyInstalls` — re-serialize a captured position WITHOUT the pin fields (mimicking pre-ADR-0051 persisted state); resume must succeed; the next emitted position must carry the now-installed pin so future reconnects engage divergence detection.
