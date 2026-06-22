# sluice v0.99.104

**A source-read drop during a native-MySQL concurrent cold-copy now resumes from where it left off instead of restarting from row 0 — the read-side companion to the v0.99.100–v0.99.103 storage-grow arc.** This is the last piece of the "don't get stuck on a storage threshold and have to start over" guarantee for native MySQL → PlanetScale migrations.

## Fixed

**Native-MySQL concurrent cold-copy survives a transient source-read drop by re-snapshotting and resuming incomplete tables from their cursors (ADR-0111).** The v0.99.103 live validation rode a full 12→39→62→214 GB storage grow on the write side (the ADR-0110 grow-gate + wall-clock retry held cleanly), but a transient source-read connection drop during the grow was terminal on the native concurrent cold-copy path: it aborted the copy and auto-resnapshotted, re-copying everything from row 0 — the exact restart-from-scratch the grow-window work exists to eliminate.

The fix could not simply reuse the migrate path's source-read retry. The sync native path pins N reader connections in one FTWRL-captured `START TRANSACTION WITH CONSISTENT SNAPSHOT` — that single position is the CDC handoff anchor — and InnoDB cannot recreate that consistent read-view once a connection drops, so a fresh reader would read at a different position and silently mix snapshot points. So this brings the native path to the resilience parity that ADR-0072 already gave the VStream (PlanetScale/Vitess) source, with a re-snapshot variant suited to the non-re-observable native snapshot:

- On a classified source-read drop, the reader re-establishes a fresh consistent snapshot (a new FTWRL at a new position P′) **transparently** — the copy continues on one uninterrupted row stream rather than aborting.
- It **skips tables already fully copied**, **resumes each incomplete keyed table from its PK cursor** (`WHERE pk > lastpk`), and re-reads keyless tables from the start (at-least-once, the existing Bug-143 contract).
- It **keeps the CDC anchor at the original/earliest position**, never advancing it to P′. This is the load-bearing value-fidelity invariant: the idempotent CDC applier then replays from the original position and converges the keyed tables to the exact source state, whereas anchoring at the new position would skip changes on the already-completed tables — silent loss. A runtime guard refuses any anchor drift.
- A single grow window triggers **one** coordinated re-snapshot across the W read lanes (peers coalesce on the first lane's recovery), not W. The recovery is bounded and loud on exhaustion, and falls back to the existing full restart if the source binlog at the original position has been purged (so adequate `binlog_expire_logs_seconds`, ≥48 h per the PlanetScale import guidance, remains a prerequisite).

The per-table cursors are held in memory, so this covers the in-process source-read-drop recovery (the case the live run demonstrated); durable cross-process-restart cursor persistence is a tracked follow-up — it needs a durable-write watermark to avoid its own silent-gap risk and is deliberately deferred.

This change was value-fidelity-reviewed end to end (all four silent-loss invariants — anchor-at-earliest, keyed exactly-once, keyless at-least-once, no-premature-complete — verified against the code) and is pinned by unit tests (anchor immutability, peer-coalescing, the PK-family classification matrix) plus `-race` integration tests that inject a real mid-copy drop and assert byte-identical convergence across a genuine FTWRL re-snapshot for integer, temporal (DATETIME), and composite primary keys, keyed and keyless.

## Compatibility

No configuration changes and no behaviour change for an untroubled migration — the recovery only engages on a classified source-read drop during a native concurrent cold-copy, and the keyed read becomes cursor-paginated (a small read-side cost for resumability). No resume-format, wire, or result-state changes; the exactly-once CDC contract is unchanged. Non-MySQL sources and the VStream path are untouched (VStream already had ADR-0072).

## Who needs this

Anyone running a `sync` cold-start from a self-managed (non-Vitess) **MySQL** source into a **non-Metal PlanetScale** target across storage auto-grow steps — a transient source-read drop during a grow now resumes the in-progress tables instead of re-copying the whole dataset. With v0.99.100–v0.99.103 (the write-side grow-window resilience) this completes the end-to-end ride-through. Automatic; no action required.

## Install

```
go install sluicesync.dev/sluice/cmd/sluice@v0.99.104
```

Container image:

```
docker pull ghcr.io/sluicesync/sluice:0.99.104
```
