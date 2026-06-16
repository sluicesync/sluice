# sluice v0.99.53

**The keyless-table CDC warning now tells the truth about delivery semantics (Bug 143).** No behaviour change — a truth-in-logging fix. If you sync a table with no PRIMARY KEY (and no usable unique index) over CDC, the operator WARN previously over-promised; it now states the at-least-once guarantee plainly.

## Fixed

- **Keyless-table CDC is at-least-once, and the WARN now says so (Bug 143).** sluice's adaptive apply default (ADR-0089) holds a keyless table at one change per transaction so it is never *worse* than `--apply-batch-size=1`. The accompanying WARN claimed that meant "crash-replay cannot duplicate rows" — which is not correct:
  - A keyless `INSERT` has no key to upsert on, so it is **not idempotent**.
  - Crash-resume granularity is the **source transaction**, not the row: the source position (a GTID for MySQL/Vitess, an LSN for Postgres) only advances at the source transaction's *commit*. For PlanetScale/Vitess VStream specifically, the position-bearing `VGTID` event arrives *after* every row event of the transaction, so all of a transaction's keyless rows carry the same pre-transaction resume position.

  So a hard kill before the in-flight source transaction commits re-streams that **entire** transaction on resume, re-inserting **every keyless row in it** — not one. Keyless CDC is at-least-once; per-row checkpointing cannot fix this (you cannot resume from the middle of a source transaction). The WARN, the ADR-0089 text, and the in-code comments now state this honestly and point at the durable fix: add a PRIMARY KEY (or a NOT NULL UNIQUE index) for exactly-once, batched throughput.

## Compatibility / notes

- **No flag, config, or behaviour change.** Apply throughput, batch sizing, and the keyless single-row guard are all exactly as in v0.99.52. Only the WARN wording and documentation changed.
- This matches sluice's long-standing position (ADR-0010): tables without a usable key are **not recommended for continuous sync**. If a keyless table must be synced continuously, deduplicate downstream or add a key.

## Who needs this

- Anyone running continuous CDC on a table **without a PRIMARY KEY or usable unique index**, who reasonably read the old WARN as a no-duplicate-on-crash guarantee. Your data path is unchanged; the guarantee is now stated correctly so you can plan for it.

## Install

```
# Linux/macOS/Windows binaries + checksums on the release page.
docker pull ghcr.io/sluicesync/sluice:v0.99.53
```
