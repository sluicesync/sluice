# sluice v0.99.160

**CRITICAL fix: `sluice migrate` into a non-Metal PlanetScale MySQL target could silently under-copy rows (drop whole batches with `rc=0` "migration complete") when the bulk-copy crossed a storage-grow reparent. If you migrate large datasets into a non-Metal PlanetScale MySQL target, upgrade. Migrations into Postgres targets, and into pre-sized / Metal MySQL targets that never reparent mid-copy, were not affected. This closes the `migrate`-path counterpart of the v0.99.x ADR-0113 restore fix.**

## Fixed

**CRITICAL — `migrate` into a non-Metal PlanetScale MySQL target silently under-copied rows across a storage-grow reparent (Bug 175).** This is the same silent-loss class ADR-0113 fixed for `restore`, now closed on the `migrate` path. When a non-Metal PlanetScale MySQL volume fills during a fast bulk-copy (`Error 1114 "table is full"`), semi-sync replication falls back to async and the storage grow **reparents** — the new primary is promoted from a replica behind the async-acked window, so rows that were committed-and-acked are dropped. A live 75 GB regression migrate reproduced it precisely:

- the target finished `rc=0` "migration complete" but landed **5,496,003 of 5,500,003 rows — 4,000 silently lost**, in whole ~500-row batches scattered across the table (confirmed against the primary, so not replica lag); and
- there was **no error or warning** — the only log lines were the benign `local_infile=OFF → batched INSERT` notices.

ADR-0113 had closed this exact class for `restore` (re-derive every reparent-touched table from its immutable chunks), but it wired the reparent observer and the reconciliation phase into the **restore path only**. The `migrate` orchestrator never set the observer and had no reconciliation phase, so the rows a reparent dropped stayed lost. The coordinated grow-gate (ADR-0110) is already wired into `migrate`, but it is reactive — it calms the target and reduces loss, yet cannot recover rows lost in the window *before* the first transient is observed.

**Fix (ADR-0141).** Reparent-reconciliation now covers `migrate`. The run wires the reparent observer onto every cold-copy writer, and after the MySQL bulk-copy completes — before indexes/constraints — it re-derives each reparent-touched table from the **source**: `TRUNCATE` the target table, then re-copy it serially (the single-stream pace that never outruns replication), looping until a full pass observes no new reparent touches. A target that reparents on every redo surfaces a **loud** non-convergence error (naming the still-touched tables and the `--bulk-parallelism 1` / pre-sized-target remedy) rather than looping forever. The key insight: unlike a CDC stream (whose source has moved on), `migrate`'s source is replayable — so it can reconcile the same way `restore` does from chunks. The reconciliation *is* the guarantee — no impractical full `COUNT(*)` scan needed.

## How it was found

The live 75 GB PlanetScale regression sweep (the cross-engine / from-backup / PlanetScale program). The Postgres `--all-schemas` leg of the same source landed exactly; only the non-Metal PlanetScale MySQL leg, which crossed real storage-grow reparents mid-copy, lost rows — pinpointing the reparent window as the loss vector and the missing `migrate`-path reconciliation as the gap.

## Compatibility

No format or flag changes. Migrations into Postgres targets are untouched (the PG overlapped copy+index path does not reparent into this class; the reconciliation machinery is engine-neutral so PG can adopt it if a PG reparent-loss is ever demonstrated). On a clean run with no reparent, the reconciliation phase is a zero-cost no-op. The redo re-reads the live source, which is sound under `migrate`'s existing static-source precondition (a plain migrate already captures no cross-table snapshot consistency) and threads the same redaction + shard-stamp path, so the re-copy is value-fidelity-identical to the initial copy. The `-race` integration gate passed before tagging (concurrency change).

## Who needs this

Anyone running `sluice migrate` into a **non-Metal PlanetScale MySQL** target (or any MySQL target that can reparent mid-copy during a storage auto-grow) with a dataset large enough to trigger a grow during the copy. Upgrade before relying on such a migration. Postgres-target migrations, and MySQL targets pre-sized so no grow/reparent occurs during the copy, are unaffected — but upgrading is recommended regardless. (Continuous-sync and `restore` users were already protected by ADR-0113.)

---

**Install:** brew install sluicesync/tap/sluice · go install sluicesync.dev/sluice/cmd/sluice@v0.99.160 · **Container:** ghcr.io/sluicesync/sluice:0.99.160
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
