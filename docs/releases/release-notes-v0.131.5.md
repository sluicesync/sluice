# sluice v0.131.5

The remediation of a fresh full audit (grade A−; the prior audit's scorecard came back 13 fixed / 0 regressed), shipped the same day it ran. Three genuine correctness fixes, two new loud-failure doors on the `postgres-trigger` CDC engine, and two new permanent gates. Drop-in from v0.131.4 — no schema, format, flag, or command change.

## Fixed

**`--redact` `randomize` could silently give every row one identical "randomized" value.** The per-row seed derivation looked up primary-key values with a case-exact key and no missing-key check — and on the CDC path PK *names* come from the target catalog while row *keys* carry the source's spelling, so a case divergence silently derived every row's seed from nil: one identical output value for the whole table, exit 0. The lookup now resolves exact-first, falls back to a case-insensitive match only when it is unambiguous, and refuses loudly (naming the row's actual columns and the consequence) on ambiguity or a genuine miss. Relatedly, **`randomize:int` with a range like `0,MaxInt64` returned the constant minimum for every row** (the span computation wrapped negative); the span is now computed overflow-safe and the full range works. A new randomize family-matrix pins both.

**A mistyped `--encrypt` passphrase on a no-op `backup compact`/`prune` no longer silently re-keys a signed chain.** The crash-stale signature heal (v0.131.3) re-signed on *any* verification failure at the no-op maintenance doors — including "the supplied key is not the chain's key" — so a wrong passphrase on a routine no-op run re-keyed every signature at exit 0 and the chain then reported `SLUICE-E-BACKUP-SIGNATURE-INVALID` under the *correct* key. The heal now compares the recorded signature's key fingerprint against the supplied key's and refuses with a coded error on a mismatch (failing closed when either fingerprint is absent); a genuinely crash-stale chain under the same key still heals.

**A stale liveness-watchdog fire can no longer silently end a Vitess/PlanetScale sync mid-stream.** The snapshot CDC pump's watchdog cancelled via a dynamic field rather than the cancel captured when it was armed, so a timer fire racing a reshard reopen could cancel the freshly-reopened stream — in the worst interleaving the sync exited 0 with no error recorded. The watchdog now joins on stop and can only ever cancel the stream it was armed for, pinned by ordering tests.

## Changed

**`postgres-trigger` CDC now refuses loudly at stream open when its capture machinery has been tampered with or lost.** A dropped or disabled capture trigger (including `ENABLE REPLICA` state) or a dropped DDL event trigger previously meant silently uncaptured changes; a new capture-shape door — the sibling of the SQLite/D1 door from v0.131.2 — verifies every expected trigger's presence, enabled state, bound function, and shape at CDC open and refuses with a re-setup remedy, verified against real PostgreSQL including the remedy actually repairing.

**`postgres-trigger` CDC now warns loudly about replica-role capture blindness.** PostgreSQL triggers without `ENABLE ALWAYS` do not fire for DML applied under `session_replication_role='replica'` — which is how logical-replication subscriptions apply rows, and how sluice's own Postgres applier applies them when privileged. A pgtrigger source in either shape (an active subscription, or a sluice relay target) silently captures nothing for those writes. Setup and stream open now probe both shapes and emit an unmissable warning naming the risk (a probe error also warns — never a silent skip), with the limitation documented in the operator docs. The full `ENABLE ALWAYS` fix has echo-loop implications and is tracked as its own ADR-gated change.

## Internal

The audit itself and its ratchet: a per-PR fake-VStream harness finally gives the v0.131.4 reshard ride-out loop running test coverage (both reopen lanes, the standalone lane's recovery contract pinned honestly); a scalar type-registry parity gate closes the schema-reader↔CDC drift class that has recurred six times (and surfaced a new `timetz` metadata divergence on first contact, filed); an ADR file→index completeness gate closes the missing-index-row class; plus a documentation batch fixing every stale claim the audit found, and the audit's findings ratcheted into the tracked backlog.

## Compatibility

Drop-in from v0.131.4 — no schema, format, error-code, flag, or command change. Every new refusal or warning fires only on a genuinely broken, tampered, or at-risk configuration: a dropped/disabled capture trigger, a wrong signing key, a missing/ambiguous PK spelling, or a replica-role-blind source. The `randomize` fixes change output only where the previous output was degenerate (constant or identical values — the bug being fixed).

**Who needs this:** anyone running **`postgres-trigger` CDC** (two new integrity doors), **signed encrypted backup chains** (the wrong-key guard), or **`--redact` with `randomize`** (genuine silent-value fixes). Everyone else: no action.

## Install

```
# Homebrew
brew upgrade sluice

# Scoop (Windows)
scoop update sluice

# Go
go install sluicesync.dev/sluice/cmd/sluice@v0.131.5
```

Container images: `ghcr.io/sluicesync/sluice:0.131.5` (multi-arch; the image tag carries no `v` prefix).
