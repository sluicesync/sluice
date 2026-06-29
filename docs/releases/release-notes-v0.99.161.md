# sluice v0.99.161

**CRITICAL follow-up: the v0.99.160 fix for Bug 175 (migrate → PlanetScale-MySQL silent under-copy across a storage-grow reparent) was *inert* — it never actually ran for a real MySQL target. v0.99.161 makes the reconciliation engage. If you took v0.99.160 for this fix, upgrade to v0.99.161. No release ever corrupted data from this defect (v0.99.160's fix was dormant, not wrong); the gap is that Bug 175's protection wasn't active until now.**

## Fixed

**CRITICAL — the v0.99.160 migrate-path reparent-reconciliation (Bug 175 / ADR-0141) was dead code for real targets.** v0.99.160 placed the reconciliation call inside the *non-`IncrementalIndexBuilder`* fallback branch of the bulk-copy orchestrator, on the mistaken premise — from a stale interface comment — that the MySQL target takes that branch. It does not: **both the PostgreSQL and the MySQL targets implement `ir.IncrementalIndexBuilder`** and take the *overlapped* copy+index branch. So the reconcile call sat in a branch no production engine executes — it never ran for a real PlanetScale-MySQL migrate, and Bug 175's silent under-copy remained unprotected despite the v0.99.160 release.

This was caught by **live PlanetScale re-validation**, not by tests: a real migrate into a reparenting non-Metal PlanetScale MySQL crossed **81 storage-grow-gate windows / 112 reparent-retries yet logged 0 reconciliation rounds**. (No rows were lost on that run — the reactive grow-gate happened to ride every reparent — but a run that drops committed-and-acked rows in the pre-transient window, the exact failure mode Bug 175 is about, would not have been recovered.)

**Fix.** The reconciliation now runs **after the copy+index block, covering both branches** (the overlapped PG/MySQL path and the non-IIB fallback), before the constraints phase. Running it there keeps the `TRUNCATE` + serial re-copy of a reparent-touched table free of FK dependencies, and the table's already-built secondary indexes are maintained by the re-`INSERT`. The reconciliation now actually engages for a reparenting PlanetScale-MySQL migrate target, exactly as ADR-0141 intended.

## Why a test didn't catch it

The v0.99.160 unit test used a plain fake `SchemaWriter` that is **not** an `IncrementalIndexBuilder`, so `Migrator.Run` took the dead fallback branch — the test validated the wrong (unreachable) path and passed. v0.99.161's test drives the **real overlapped branch** via an `IncrementalIndexBuilder` fake, so the coverage gap that let the inert fix ship is closed. This is the "validate end-to-end before building more" tenet doing its job — the inert fix surfaced only because the fix was re-validated against a real reparenting target end to end, watching for the reconciliation to actually fire.

## Compatibility

No format or flag changes. On a clean run with no reparent, the reconciliation phase is a zero-cost no-op (the touched-set drains empty). Only the MySQL target marks reparent-touched tables today, so a PostgreSQL target reconciles nothing. The `-race` integration gate passed before tagging (concurrency change).

## Who needs this

Anyone running `sluice migrate` into a **non-Metal PlanetScale MySQL** target (or any MySQL target that can reparent mid-copy during a storage auto-grow) with a dataset large enough to trigger a grow during the copy — i.e. exactly the audience of v0.99.160. v0.99.160 advertised this fix but it was inert; v0.99.161 is the build where it actually works. Postgres-target migrations, and MySQL targets pre-sized so no grow/reparent occurs during the copy, are unaffected.

---

**Install:** brew install sluicesync/tap/sluice · go install sluicesync.dev/sluice/cmd/sluice@v0.99.161 · **Container:** ghcr.io/sluicesync/sluice:0.99.161
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
