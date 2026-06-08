# sluice v0.99.18

**CRITICAL fix — a PlanetScale/Vitess `migrate` of a large table could silently copy only a fraction of its rows.** If you ran `sluice migrate` with `--source-driver=planetscale` or `--source-driver=vitess` on **v0.99.14–v0.99.17**, against a table big enough to be chunked (≥100k rows, default parallelism), the copy could land only a small fraction of the rows and **still exit 0 with `migration complete`**. This release fixes it. **Drop-in from v0.99.17.** No source data was ever touched — the target simply received a partial copy — so re-running on v0.99.18 fully resolves it.

## Fixed

- **`workload=olap` is no longer a session-wide setting on the PlanetScale/Vitess source reader (Bug 132, CRITICAL silent loss).** v0.99.14 set vtgate's `workload=olap` as a session-wide DSN parameter to lift vtgate's ~100k OLTP result cap on a **no-PK** full-table scan. But that session setting also covered the `LIMIT`-paged reads the **parallel chunked bulk-copy** uses (the default for tables at/above `--bulk-parallel-min-rows`), and under olap *streaming* mode each concurrently-read chunk's page was truncated — so a large PK table was copied only in part (e.g. ~7.5k of 1.5M rows) while `migrate` still reported success. Single-stream copies (`--bulk-parallelism=1`) and vanilla MySQL sources were unaffected, and it only appeared above the chunk threshold — which is why the existing VStream tests (sub-threshold tables) didn't catch it.

  `workload=olap` is now scoped to **just** the unbounded no-PK full scan, applied on a dedicated connection — never session-wide. The `LIMIT`-paged chunked reads are olap-free again, exactly as before v0.99.14, so the parallel copy reads **every** row, while the no-PK 100k-cap lift the olap change was added for is preserved. An operator-supplied `workload` DSN parameter is left as your explicit session choice. Pinned by a deterministic engine-level regression test (the source reader's pooled session must not be globally olap — verified to fail if the session-wide setting is reintroduced) and an end-to-end chunked-PlanetScale migrate test (the path the prior pins never exercised).

## Compatibility

- No breaking API or CLI changes. Drop-in from v0.99.17. Affects MySQL/PlanetScale/Vitess **sources** only; Postgres sources and `sync start` (cold-start + CDC) were never on this path.

## Who needs this — action required

- **Anyone who ran a PlanetScale or Vitess `migrate` of a table with ≥100k rows at the default parallelism on v0.99.14, v0.99.15, v0.99.16, or v0.99.17.** Re-verify your target row counts and re-run the migration on v0.99.18. The fix-up is just a re-run — the source was never modified.

---

**Install:** `brew install sluicesync/tap/sluice`  ·  `go install sluicesync.dev/sluice/cmd/sluice@v0.99.18`  ·  **Container:** `ghcr.io/sluicesync/sluice:0.99.18`
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
