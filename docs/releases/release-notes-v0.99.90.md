# sluice v0.99.90

**CRITICAL fix: concurrent CDC apply (`--apply-concurrency > 1`) could persist a mid-transaction resume position on a native-MySQL file/pos source, crash-looping every warm-resume.** If you run native MySQL CDC with `gtid_mode=OFF` and `--apply-concurrency > 1`, upgrade. Vitess/PlanetScale (VStream) and GTID-mode sources were never affected.

## Fixed

**Concurrent CDC apply now checkpoints only at real transaction boundaries on a file/pos source.** The concurrent key-hash apply orchestrator recorded a checkpoint boundary whenever the source position *token* changed between consecutive events. That is correct only when every event within a source transaction shares one position token:

- **Vitess/PlanetScale VStream** — the VGTID token is stable within a transaction and changes only at the boundary, so the heuristic correctly finds the last change of each transaction. **Never affected.**
- **Postgres** — resume is by LSN and a mid-transaction LSN is independently resumable. **Never affected.**
- **Native MySQL in file/pos mode (`gtid_mode=OFF`)** — every binlog event has a distinct `LogPos`, so the heuristic recorded *mid-transaction row positions* as boundaries, and the persisted checkpoint could land **inside** a transaction.

go-mysql cannot warm-resume from a mid-transaction position — it reads a `ROWS` event whose `TABLE_MAP` appeared earlier in the same transaction and fails fatally with **`no corresponding table map event`**. The stream then crash-loops on every restart (watchdog bounce, crash, or a concurrency change) until a fresh resnapshot.

This is the **concurrent-apply counterpart of the serial-path fix in v0.99.89** (item 29): the same symptom from a different root cause. v0.99.89 fixed the serial applier; it did not cover the concurrent (`--apply-concurrency`) path, and that release's regression cycle exercised serial-vs-concurrent *convergence*, not *warm-resume from a position the concurrent path persisted* — which is exactly the gap this edge lived in. It was found on a live native-MySQL `--apply-concurrency` run (a non-Metal PlanetScale resilience test), when a deliberate restart surfaced a mid-transaction checkpoint the run had already persisted.

The fix selects the checkpoint-boundary strategy by stream shape:

- **Marker streams** (the binlog-MySQL and Postgres CDC readers, which emit transaction BEGIN/COMMIT) record a checkpoint boundary **only at a real transaction boundary** — `TxCommit`, plus the DDL-statement boundary `Truncate` — never at a mid-transaction row position. An interrupted transaction is re-read and idempotently re-applied from the prior boundary on resume (ADR-0010; at-least-once for the interrupted transaction, exactly as the keyless guard and serial path already are).
- **Marker-less streams** (VStream, whose VGTID token is transaction-stable) keep the existing position-run heuristic **byte-for-byte**.

The selection is purely dynamic — it latches on the first transaction marker observed — so there is no new configuration knob and no default to mis-set. The `SchemaSnapshot` first-touch exclusion (Bug 158) is preserved.

Validated by unit pins: a marker stream **interrupted mid-transaction** persists only the prior `TxCommit` boundary, never a mid-transaction row position (verified to fail without the fix — it persisted the mid-transaction position); a `Truncate` is correctly recorded as a boundary; and a marker-less (VStream) token-run stream is unchanged. The `-race` integration gate ran before tagging (CDC/exactly-once chunk).

## Compatibility

No interface, flag, or default-behavior changes. Vitess/PlanetScale VStream and GTID-mode MySQL sources, and all Postgres sources, are byte-for-byte unchanged. The serial apply path (the default, `--apply-concurrency` unset or 1) is unaffected — it was fixed separately in v0.99.89.

## Who needs this

Anyone running **native MySQL CDC with `gtid_mode=OFF` (file/pos resume) and `--apply-concurrency > 1`**. On that configuration a warm-resume could crash-loop with `no corresponding table map event` after the concurrent applier persisted a mid-transaction checkpoint. If you use Vitess/PlanetScale (VStream), GTID-mode MySQL, Postgres, or the default serial apply path, this release changes nothing for you.

## Install

```
go install sluicesync.dev/sluice/cmd/sluice@v0.99.90
```

Container image:

```
docker pull ghcr.io/sluicesync/sluice:0.99.90
```
