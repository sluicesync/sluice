# sluice v0.99.122

Two fixes, both surfaced by the v0.99.118 fresh-database re-validation arc — one a real bug it caught live, one a regression in the immediately-preceding release.

## Fixed

**HIGH — a PostgreSQL index build during a storage-grow reparent could abort a restore/migration with a spurious `pg_class` duplicate-key error (`23505`), even though all data had copied correctly (Bug #114).** A parallel cross-engine MySQL→PlanetScale-Postgres restore correctly rode 16 storage-grow reparents through the post-copy DDL phase (the v0.99.118/ADR-0114 fix working), then aborted at `create index … on inventory: duplicate key value violates unique constraint "pg_class_relname_nsp_index" (23505)` — with every row already landed byte-perfect against the manifest (loud, never silent). Root cause (confirmed by a three-phase investigation that conclusively ruled out the more-serious cross-engine index-name-collision hypothesis for this path): `CREATE INDEX IF NOT EXISTS` is not atomic against an overlapping same-name creation — its `pg_class` existence pre-check and the catalog insert race. Under ADR-0114's whole-phase retry over the concurrent index pool, a reparent makes a retry's `CREATE INDEX` overlap the prior attempt's just-committed (replicated-to-the-new-primary) build, and the resulting `pg_class` unique-violation is — correctly — classified non-transient (a user-table duplicate key must always stay loud), so the reparent-retry layers didn't catch it. The fix wraps the single index-build chokepoint (`buildOneIndex`, through which the serial, concurrent-pool, and overlap index-build paths all route) in the **same** narrow catalog-race retry the CDC schema-apply path already uses: it retries only the `pg_class` / `pg_type` constraint-name `23505` shape (the race resolves in milliseconds) and keeps every user-table `23505` loud. Inner layer — a genuine connection/reparent transient still propagates to the outer reparent-retry. Pinned by unit tests that fail if the wrap is removed.

**Regression fix — v0.99.121's MySQL connection-budget prober (ADR-0116) wrongly applied its PlanetScale buffer-pool tier cap to *every* MySQL flavor, collapsing parallel backup/restore on vanilla MySQL and self-hosted Vitess.** The Part-B tier cap buckets `@@innodb_buffer_pool_size` into a parallelism ceiling, calibrated to PlanetScale's fixed plan tiers (PS-10 → PS-160) where the buffer pool genuinely proxies the instance's CPU tier. v0.99.121 folded that cap in for all MySQL flavors — so a vanilla MySQL or a self-hosted Vitess with the common 128 MB default buffer pool was throttled to parallelism 2, collapsing the automatic parallelism of `migrate`, `backup`, and `restore` to (near-)serial. Loud-safe (output was always correct — just slower), but wrong: a self-hosted box sizes its buffer pool to its own hardware. The tier cap is now gated to the **PlanetScale flavor only** (`--target-driver`/`--source-driver planetscale`); vanilla MySQL (`mysql`) and self-hosted Vitess (`vitess`) get the real connection-slot budget with no tier cap, restoring their prior parallelism. Pinned by a unit test asserting the cap binds for PlanetScale and is a no-op for the non-PlanetScale flavors.

## Compatibility

No flag or API changes. **If you ran a `migrate` / `backup` / `restore` against a non-PlanetScale MySQL on v0.99.121, it produced correct results but ran more serially than intended — upgrade to v0.99.122 to restore full parallelism.** The Bug #114 fix has no value-fidelity surface; data was never at risk in either case.

## Install

```
go install sluicesync.dev/sluice/cmd/sluice@v0.99.122
```

Container image:

```
docker pull ghcr.io/sluicesync/sluice:0.99.122
```
