# sluice v0.99.173

**When a large PlanetScale-MySQL migrate's deferred index build hits the statement-time wall (errno 3024), the error now tells the operator the useful thing: the data is already copied, so `--resume` finishes just the indexes with no re-copy — no more cryptic vttablet error or "start over" dead-end.**

## Added

**Actionable hints for the PlanetScale index-build statement-time limit.** On a large PlanetScale-MySQL target, a deferred `ALTER … ADD INDEX` can run past PlanetScale's max-statement-execution-time limit and fail with MySQL errno 3024 ("Query execution was interrupted, maximum statement execution time exceeded"). Previously that surfaced as a raw vttablet error at the end of a long migration. Now the index-phase error carries a hint:

- **The data is already copied — `--resume` finishes just the indexes with no re-copy.** This is the important part: if a big migration fails on the *last* table's index build, every table's data is already in the target. sluice's `--resume` skips completed tables (`classifyTableForResume`), so it retries only the index build — you do **not** restart the whole migration. Bumping the PlanetScale resource size first helps (a larger cluster builds the index faster, more likely under the limit).
- `--upfront-indexes` remains the fresh-start alternative (build the indexes during the copy so no post-copy `ALTER` is issued).

A sibling hint covers the case where PlanetScale **safe-migrations** is enabled on the target branch and blocks direct DDL (errno 1105, "direct DDL is disabled") — with the correct fix (disable safe-migrations for the migration), since `--upfront-indexes` does not help there (its `ALTER` is also direct DDL).

## Compatibility

Pure diagnostic hints appended to the existing wrapped error text — no behavior change, no flags added or removed. The underlying error chain is preserved (`errors.Is` / `errors.As` unaffected). No-op on non-PlanetScale targets and on any error that doesn't match.

## Who needs this

Operators running large `sluice migrate` loads into PlanetScale-MySQL, where a deferred index build can cross PlanetScale's ~900 s statement-time limit. The hint turns a late-stage failure into a clear, cheap recovery (`--resume`, optionally after a resize) instead of a cryptic error or an unnecessary full re-run. Everyone else is unaffected.

---

**Install:** brew install sluicesync/tap/sluice · go install sluicesync.dev/sluice/cmd/sluice@v0.99.173 · **Container:** ghcr.io/sluicesync/sluice:0.99.173
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
