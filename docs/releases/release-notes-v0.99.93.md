# sluice v0.99.93

**Cold-copy now rides through a transient source-read drop instead of aborting.** Roadmap item 34 (ADR-0109) closes the second half of the PlanetScale storage-auto-grow resilience story that v0.99.92's ADR-0108 started: a large migration into a non-Metal PlanetScale target no longer dies when the target's storage grows underneath it and the resulting stall backpressures the source read connection to death.

## Added

**Cold-copy source-read resilience (ADR-0109, item 34).** Live finding while migrating into a non-Metal PlanetScale target: during a storage auto-grow the target's *binlog* volume hit `errno 28 — No space left on device` (a fast bulk load generates enormous binlog churn that fills the volume before it grows). The target replica's SQL thread failed and, under semi-sync, the target primary's writes **stalled** — they *blocked* rather than returning an error. sluice's reader/writer pipeline backpressured: the writer couldn't drain, so the reader stopped consuming from the source, the source connection went idle past the source server's `net_write_timeout` (default 60 s), and the source closed the read connection (`unexpected EOF` → `invalid connection`) → the whole cold-copy aborted. v0.99.92's ADR-0108 (target-write reparent-retry) can't catch this — the write *blocked*, so the first error appeared on the **source-read** side.

The fix is three-pronged:

- **(A) Raise the source read session timeouts — the primary fix.** sluice now sets `net_write_timeout` / `net_read_timeout` to a bounded ~10-minute default on every MySQL source read session it opens (injected as DSN params at handshake, so it covers the full-scan reader, the keyset-paged chunk reads, and the consistent-snapshot reader). A transient target stall (a storage grow takes seconds to minutes) no longer causes the source to drop sluice's idle read connection; when the target recovers, the writer drains, the reader resumes, and the copy continues — no reconnect, no re-snapshot, no consistency concern. The bound stays finite so a genuinely-dead target still surfaces. An operator's own DSN override still wins.

- **(B) Bounded auto-restart of the cold-start — the backstop.** If a source read still drops during the cold-copy phase, it is no longer a fatal exit: it triggers a bounded, backed-off auto-restart of the cold-start. On the consistent-snapshot `sync` path, MySQL cannot re-establish the frozen snapshot on a fresh connection, so a clean re-copy is the only consistency-preserving recovery. The discriminator is whether a CDC anchor exists yet: no anchor means the drop happened in the cold-copy phase, so the re-run forces a clean re-establishment (the v0.99.73 force-fresh path — a native plain-INSERT target drops+recreates rather than dup-keying, an idempotent target re-copies with UPSERT); once a CDC anchor exists the retry warm-resumes from the durable position, exactly as before. It is bounded by the existing apply-retry budget — never an infinite loop, loud on exhaustion — replacing the old fatal-exit (and the external-watchdog crash-loop it invited).

- **(C) Per-table reconnect + resume — the `migrate` path.** Where independent per-table readers exist (no shared consistent snapshot), a source-read drop reconnects a fresh per-table reader and resumes from a dup/loss-safe position: keyset-chunked tables resume from the persisted `chunk.LastPK` (`WHERE pk > LastPK` — dup-free and loss-free), non-chunkable tables truncate the target and restart that table. A transient on one table no longer aborts its sibling table copies.

Together with v0.99.92's ADR-0108, the cold-copy now rides through **both faces** of a PlanetScale storage-auto-grow: the reparent-*error* face (ADR-0108, target write) and the disk-full-*stall* face (ADR-0109, source read). A genuinely terminal source error (a decode fault, a real query error) still fails loudly and unchanged.

## Compatibility

No configuration changes and no behaviour change for an untroubled migration. The only observable differences engage on a transient target stall that previously killed the copy: the source read now tolerates the stall (a larger source-session `net_write_timeout`, which an operator's own DSN value still overrides), and a drop during cold-copy now bounded-auto-restarts instead of exiting fatally. No resume-format, wire, or result-state changes; final target state is unchanged.

## Who needs this

Anyone running a large `sluice` migration or cold-copy into a **non-Metal PlanetScale MySQL** target (or any managed-MySQL target whose storage auto-grows mid-copy) — the copy now survives the storage-grow stall instead of dying at the boundary. No action required; it is automatic. Scoped to MySQL sources; the Postgres `COPY`-source path is a noted follow-up.

## Install

```
go install sluicesync.dev/sluice/cmd/sluice@v0.99.93
```

Container image:

```
docker pull ghcr.io/sluicesync/sluice:0.99.93
```
